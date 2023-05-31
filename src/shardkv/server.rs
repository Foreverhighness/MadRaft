use super::{key2shard, msg::*};
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
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

static USE_PULL: bool = false;
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
        if USE_PULL {
            this.spawn_puller();
        } else {
            this.spawn_pusher();
        }
        this
    }

    fn spawn_fetcher(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        task::spawn(async move {
            loop {
                let Some(this) = weak.upgrade() else { return };
                this.fetch_config().await;
                drop(this);
                sleep(Duration::from_millis(100)).await;
            }
        })
        .detach();
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
        if USE_PULL {
            assert!(matches!(
                op,
                Op::NewConfig { .. } | Op::InstallShards { .. }
            ));
        } else {
            assert!(matches!(op, Op::NewConfig { .. } | Op::DeleteShards { .. }));
        }
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
                this.do_push().await;
                {
                    let state = this.inner.state().lock().unwrap();
                    info!(
                        "CONFIG G{} now push:{:?}, pull:{:?} at T{}",
                        this.me,
                        state.push.keys(),
                        state.pull.keys(),
                        state.config.num
                    );
                }
                drop(this);
                sleep(Duration::from_millis(300)).await;
            }
        })
        .detach();
    }

    async fn do_push(self: &Arc<Self>) {
        let need_push = !self.inner.state().lock().unwrap().push.is_empty();
        if !need_push {
            return;
        }
        let args = self.inner.state().lock().unwrap().push_args();
        let me = self.me;
        trace!("PUSH G{me} send push args: {args:?}");

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
        assert!(!USE_PULL);

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

    fn spawn_puller(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        task::spawn(async move {
            loop {
                let Some(this) = weak.upgrade() else { return };
                this.do_pull().await;
                drop(this);
                sleep(Duration::from_millis(300)).await;
            }
        })
        .detach();
    }

    async fn do_pull(self: &Arc<Self>) {
        let need_pull = !self.inner.state().lock().unwrap().pull.is_empty();
        if !need_pull {
            return;
        }
        let state = self.inner.state().lock().unwrap();
        let config_id = state.config.num;
        for (shards, servers) in &state.pull {
            let (shards, servers) = (shards.clone(), servers.clone());
            let weak = Arc::downgrade(self);
            task::spawn(async move {
                let pull_op = Op::PullShards { config_id, shards: shards.clone() };
                let Ok(Reply::PullShards { kv, seen }) = send_operation(servers.clone(), pull_op).await else { return };

                let Some(this) = weak.upgrade() else { return };
                let push_op = Op::InstallShards(InstallShards{ config_id, shards: shards.clone(), kv, seen });
                let request = this.self_ck.call(push_op);
                let request = timeout(QUERY_TIMEOUT, request);
                let Ok(Reply::Ok) = request.await else { return };

                let del_op = Op::DeleteShards(DeleteShards { config_id, shards });
                let Ok(Reply::Ok) = send_operation(servers, del_op).await else { return };
            }).detach();
        }
    }
}

async fn send_operation(servers: Vec<SocketAddr>, op: Op) -> Result<Reply, Elapsed> {
    let push_ck = ClerkCore::<Op, Reply>::new(servers);
    let request = push_ck.call(op);
    timeout(QUERY_TIMEOUT, request).await
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ShardKv {
    kv: HashMap<String, String>,
    seen: Seen,

    me: Gid,
    config: Config,

    pull: HashMap<Vec<usize>, Vec<SocketAddr>>,
    push: HashMap<Vec<usize>, Vec<SocketAddr>>,
}

impl ShardKv {
    fn is_pulling(&self, shard: usize) -> bool {
        self.pull.keys().any(|shards| shards.contains(&shard))
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
        if self.config.groups.is_empty() || new_config.groups.is_empty() {
            return;
        }
        let old_shards = self.config.shards_by_gid(self.me);
        let new_shards = new_config.shards_by_gid(self.me);
        let diff = old_shards
            .symmetric_difference(&new_shards)
            .copied()
            .collect::<HashSet<_>>();
        assert_eq!(diff.len(), old_shards.len().abs_diff(new_shards.len()));

        match old_shards.len().cmp(&new_shards.len()) {
            std::cmp::Ordering::Less => {
                // pull
                // TODO: group by gid
                let config = &self.config;
                for shard in diff {
                    let gid = config.shards[shard];
                    self.pull.insert(vec![shard], config.groups[&gid].clone());
                }
            }
            std::cmp::Ordering::Greater => {
                // push
                // TODO: group by gid
                let config = new_config;
                for shard in diff {
                    let gid = config.shards[shard];
                    self.push.insert(vec![shard], config.groups[&gid].clone());
                }
            }
            std::cmp::Ordering::Equal => assert_eq!(old_shards, new_shards),
        }
    }

    fn push_args(&self) -> Vec<Ret> {
        // TODO: rewrite with for loop
        let config_id = self.config.num;
        self.push
            .iter()
            .map(move |(shards, servers)| {
                let (shards, servers) = (shards.clone(), servers.clone());
                let kv = self
                    .kv
                    .iter()
                    .filter(|&(key, value)| shards.contains(&key2shard(key)))
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect();
                let seen = self.seen.clone();
                (servers, config_id, shards, kv, seen)
            })
            .collect()
    }
}

impl Config {
    fn shards_by_gid(&self, gid: Gid) -> HashSet<usize> {
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
                return Reply::Get {
                    value: self.kv.get(&key).cloned(),
                };
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
                    info!("CONFIG G{me} {:?} -> {config:?}", self.config);
                    self.config_migrate(&config);
                    self.config = config;
                    info!(
                        "CONFIG G{me} now push:{:?}, pull:{:?} at T{}",
                        self.push.keys(),
                        self.pull.keys(),
                        self.config.num
                    );
                }
            }
            Op::PullShards { config_id, shards } => {
                assert!(USE_PULL);
                todo!();
                match config_id.cmp(&self.config.num) {
                    std::cmp::Ordering::Less => return Reply::Ok,
                    std::cmp::Ordering::Greater => return Reply::WrongConfig,
                    std::cmp::Ordering::Equal => (),
                }
                let kv = self
                    .kv
                    .iter()
                    .filter(|&(key, _)| shards.contains(&key2shard(key)))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let seen = self.seen.clone();
                return Reply::PullShards { kv, seen };
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
                let ret = self.pull.remove(&shards);
                if ret.is_some() {
                    self.kv.extend(kv);
                    self.seen.install(seen);
                }
                info!(
                    "INSTALL G{me} install {shards:?}, now pull: {:?} at T{config_id}",
                    self.pull.keys()
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
                    self.push.keys()
                );
            }
        }
        Reply::Ok
    }
}
