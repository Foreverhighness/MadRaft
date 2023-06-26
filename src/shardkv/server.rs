use super::{
    key2shard,
    msg::{DeleteShards, InstallShards, Op, Reply},
};
use crate::{
    kvraft::{
        client::ClerkCore,
        server::{Seen, Server, State},
    },
    shard_ctrler::{
        client::Clerk as CtrlerClerk,
        msg::{Config, ConfigId, Gid},
    },
};
use madsim::{
    task,
    time::{error::Elapsed, sleep, timeout},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

static GARBAGE_COLLECT: bool = true;
static HANDLE_REQUEST_DURING_MIGRATION: bool = true;

const QUERY_TIMEOUT: Duration = Duration::from_secs(2);

pub struct ShardKvServer {
    inner: Arc<Server<ShardKv>>,
    ctrl_ck: CtrlerClerk,
    self_ck: ClerkCore<Op, Reply>,
    me: u64,
}

impl ShardKvServer {
    pub async fn new(
        ctrl_ck: CtrlerClerk,
        servers: Vec<SocketAddr>,
        gid: u64,
        me: usize,
        max_raft_state: Option<usize>,
    ) -> Arc<Self> {
        let self_ck = ClerkCore::new(servers.clone());
        let inner = Server::new(servers, me, max_raft_state).await;
        let this = Arc::new(ShardKvServer {
            inner,
            ctrl_ck,
            self_ck,
            me: gid,
        });
        this.inner.state().lock().unwrap().me = gid;
        this.spawn_fetcher();
        this.spawn_pusher();
        this
    }

    fn spawn_fetcher(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        task::spawn(async move {
            loop {
                let Some(this) = weak.upgrade() else { return };
                this.spawn_fetch_config();
                sleep(Duration::from_millis(100)).await;
            }
        })
        .detach();
    }

    fn spawn_fetch_config(self: Arc<Self>) {
        task::spawn(async move { self.fetch_config().await }).detach();
    }

    async fn fetch_config(self: &Arc<Self>) {
        let num = self.inner.state().lock().unwrap().config.num + 1;
        let fetch_config = self.ctrl_ck.query_at(num);
        let fetch_config = timeout(QUERY_TIMEOUT, fetch_config);
        let Ok(config) = fetch_config.await else { return };
        // if config.num != num {
        //     return;
        // }
        self.spawn_self_op(Op::NewConfig { config });
    }

    fn spawn_self_op(self: &Arc<Self>, op: Op) {
        assert!(matches!(op, Op::NewConfig { .. } | Op::DeleteShards { .. }));
        let weak = Arc::downgrade(self);
        task::spawn(async move {
            let Some(this) = weak.upgrade() else { return };
            let Err(_) = this.inner.apply(op.clone()).await else { return };
            let request = this.self_ck.call(op);
            let request = timeout(QUERY_TIMEOUT, request);
            let Ok(Reply::Ok) = request.await else { return };
        })
        .detach();
    }

    fn spawn_pusher(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        task::spawn(async move {
            loop {
                let Some(this) = weak.upgrade() else { return };
                this.spawn_push_shards();
                sleep(Duration::from_millis(300)).await;
            }
        })
        .detach();
    }

    fn spawn_push_shards(self: Arc<Self>) {
        let args = {
            let state = self.inner.state().lock().unwrap();
            let me = self.me;
            info!(
                "CONFIG G{me} now push:{:?}, pull:{:?} at T{}",
                state.push.keys().collect::<BTreeSet<_>>(),
                state.pull,
                state.config.num
            );

            let need_push = !state.push.is_empty();
            if !need_push {
                return;
            }
            state.push_args()
        };
        // trace!("PUSH G{me} send push args: {args:?}");

        for (servers, config_id, shards, kv, seen) in args {
            let op = InstallShards {
                config_id,
                shards,
                kv,
                seen,
            };
            self.spawn_install_shards(servers, op);
        }
    }

    fn spawn_install_shards(self: &Arc<Self>, servers: Vec<SocketAddr>, push_op: InstallShards) {
        let me = self.me;

        let weak = Arc::downgrade(self);
        let delete_op = DeleteShards {
            config_id: push_op.config_id,
            shards: push_op.shards.clone(),
        };
        task::spawn(async move {
            let Ok(reply) = send_operation(servers, Op::InstallShards(push_op)).await else { return };
            trace!("PUSH_REPLY {reply:?}");

            if let Reply::Ok = reply {
                let Some(this) = weak.upgrade() else { return };
                trace!("DELETE G{me} send delete {delete_op:?}");
                this.spawn_self_op(Op::DeleteShards(delete_op));
            }
        })
        .detach();
    }
}

async fn send_operation(servers: Vec<SocketAddr>, op: Op) -> Result<Reply, Elapsed> {
    let clerk = ClerkCore::<Op, Reply>::new(servers);
    let request = clerk.call(op);
    timeout(QUERY_TIMEOUT, request).await
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ShardKv {
    kv: HashMap<String, String>,
    seen: Seen,

    me: Gid,
    config: Config,

    pull: BTreeSet<usize>,
    push: HashMap<Vec<usize>, Vec<SocketAddr>>,
}

impl ShardKv {
    fn is_pulling(&self, shard: usize) -> bool {
        self.pull.contains(&shard)
    }

    fn can_serve(&self, key: &str) -> bool {
        let me = self.me;
        if me == 0 {
            return false;
        }
        let shard = key2shard(key);
        let group_ok = self.config.shards[shard] == self.me;
        if !group_ok {
            return false;
        }

        let not_in_migration = self.pull.is_empty() && self.push.is_empty();
        if not_in_migration {
            return true;
        }

        if !HANDLE_REQUEST_DURING_MIGRATION {
            return false;
        }

        !self.is_pulling(shard)
    }

    fn config_migrate(&mut self, new_config: &Config) {
        let me = self.me;
        info!("CONFIG G{me} {:?} -> {new_config:?}", self.config);
        if self.config.groups.is_empty() || new_config.groups.is_empty() {
            return;
        }
        let old_shards = self.config.shards_by_gid(self.me);
        let new_shards = new_config.shards_by_gid(self.me);
        let diff = old_shards
            .symmetric_difference(&new_shards)
            .copied()
            .collect::<BTreeSet<_>>();
        assert_eq!(diff.len(), old_shards.len().abs_diff(new_shards.len()));

        match old_shards.len().cmp(&new_shards.len()) {
            std::cmp::Ordering::Less => {
                // pull
                assert!(self.pull.is_empty());
                self.pull = diff;
            }
            std::cmp::Ordering::Greater => {
                // push
                let mut gid2shards: HashMap<Gid, Vec<_>> = HashMap::new();
                for shard in diff {
                    let gid = new_config.shards[shard];
                    gid2shards.entry(gid).or_default().push(shard);
                }
                assert!(self.push.is_empty());
                self.push = gid2shards
                    .into_iter()
                    .map(|(gid, shards)| (shards, new_config.groups[&gid].clone()))
                    .collect();
            }
            std::cmp::Ordering::Equal => assert_eq!(old_shards, new_shards),
        }
    }

    fn push_args(&self) -> Vec<Ret> {
        // construct
        let mut ret = self
            .push
            .iter()
            .map(|(shards, servers)| {
                (
                    servers.clone(),
                    self.config.num,
                    shards.clone(),
                    HashMap::new(),
                    self.seen.clone(),
                )
            })
            .collect::<Vec<Ret>>();
        #[allow(clippy::pattern_type_mismatch)]
        ret.sort_by_key(|(.., shards, _, _)| shards[0]);

        // insert kvs
        for (k, v) in &self.kv {
            let shard = key2shard(k);
            for &mut (.., ref shards, ref mut kv, _) in &mut ret {
                if shards.contains(&shard) {
                    assert!(kv.insert(k.clone(), v.clone()).is_none());
                    break;
                }
            }
        }

        ret
    }
}

impl Config {
    fn shards_by_gid(&self, gid: Gid) -> BTreeSet<usize> {
        self.shards
            .iter()
            .enumerate()
            .filter(|&(_, &g)| gid == g)
            .map(|(s, _)| s)
            .collect()
    }
}

type Ret = (
    Vec<SocketAddr>,
    ConfigId,
    Vec<usize>,
    HashMap<String, String>,
    Seen,
);

impl State for ShardKv {
    type Command = Op;
    type Output = Reply;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
        let me = self.me;
        let num = self.config.num;
        trace!("APPLY G{me} apply {cmd:?} at T{num}");
        match cmd {
            Op::Get { key } => {
                if !self.can_serve(&key) {
                    return Reply::WrongGroup;
                }
                let value = self.kv.get(&key).cloned();
                return Reply::Get { value };
            }
            Op::Put { key, value, id } => {
                if !self.can_serve(&key) {
                    return Reply::WrongGroup;
                }
                if !self.seen.is_duplicate(id) {
                    self.seen.update(id);

                    *self.kv.entry(key).or_default() = value;
                }
            }
            Op::Append { key, value, id } => {
                if !self.can_serve(&key) {
                    return Reply::WrongGroup;
                }
                if !self.seen.is_duplicate(id) {
                    self.seen.update(id);

                    self.kv.entry(key).or_default().push_str(&value);
                }
            }
            Op::NewConfig { config } => {
                match config.num.cmp(&(self.config.num + 1)) {
                    std::cmp::Ordering::Less => return Reply::Ok,
                    std::cmp::Ordering::Greater => return Reply::WrongConfig,
                    std::cmp::Ordering::Equal => (),
                }

                let can_advance = self.pull.is_empty() && self.push.is_empty();
                if can_advance {
                    self.config_migrate(&config);
                    self.config = config;
                    info!(
                        "CONFIG G{me} now push:{:?}, pull:{:?} at T{}",
                        self.push.keys().collect::<BTreeSet<_>>(),
                        self.pull,
                        self.config.num
                    );
                }
            }
            Op::InstallShards(InstallShards {
                config_id,
                shards,
                kv,
                seen,
            }) => {
                match config_id.cmp(&self.config.num) {
                    std::cmp::Ordering::Less => return Reply::Ok,
                    std::cmp::Ordering::Greater => return Reply::WrongConfig,
                    std::cmp::Ordering::Equal => (),
                }
                let ret = shards
                    .iter()
                    .map(|shard| self.pull.remove(shard))
                    .collect::<Vec<_>>();
                let keep = ret.iter().all(|&r| r);
                let removed = ret.iter().all(|&r| !r);
                assert!(keep || removed);
                if keep {
                    self.kv.extend(kv);
                    self.seen.install(seen);
                }
                info!(
                    "INSTALL G{me} install {shards:?}, now pull: {:?} at T{config_id}",
                    self.pull
                );
            }
            Op::DeleteShards(DeleteShards { config_id, shards }) => {
                match config_id.cmp(&self.config.num) {
                    std::cmp::Ordering::Less => return Reply::Ok,
                    std::cmp::Ordering::Greater => return Reply::WrongConfig,
                    std::cmp::Ordering::Equal => (),
                }
                let ret = self.push.remove(&shards);
                if GARBAGE_COLLECT {
                    let removed_keys = self
                        .kv
                        .keys()
                        .filter(|&key| shards.contains(&key2shard(key)))
                        .cloned()
                        .collect::<Vec<_>>();
                    assert!(ret.is_some() || removed_keys.is_empty());

                    for key in removed_keys {
                        assert!(self.kv.remove(&key).is_some());
                    }
                }
                info!(
                    "DELETE G{me} delete {shards:?}, now push: {:?} at T{config_id}",
                    self.push.keys().collect::<BTreeSet<_>>()
                );
            }
        }
        Reply::Ok
    }
}
