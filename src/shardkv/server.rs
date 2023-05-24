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
    time::{sleep, timeout},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

static USE_PULL: bool = false;
static DELETE_AFTER_INSTALL: bool = false;
static HANDLE_REQUEST_DURING_MIGRATION: bool = false;

const QUERY_TIMEOUT: Duration = Duration::from_millis(500);

pub struct ShardKvServer {
    inner: Arc<Server<ShardKv>>,
    ctrl_ck: CtrlerClerk,
    self_ck: ClerkCore<Op, Reply>,
    gid: u64,
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
            gid,
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
                sleep(Duration::from_millis(300)).await;
            }
        })
        .detach();
    }

    async fn fetch_config(self: &Arc<Self>) {
        let num = self.inner.state().lock().unwrap().config.num + 1;
        let fetch_config = self.ctrl_ck.query_at(num);
        let fetch_config = timeout(QUERY_TIMEOUT, fetch_config);
        let Ok(config) = fetch_config.await else { return };
        if config.num != num {
            return;
        }
        self.spawn_new_config(config);
    }

    fn spawn_new_config(self: &Arc<Self>, config: Config) {
        let weak = Arc::downgrade(self);
        task::spawn(async move {
            let Some(this) = weak.upgrade() else { return };
            let apply_config = this.self_ck.call(Op::NewConfig { config });
            let apply_config = timeout(QUERY_TIMEOUT, apply_config);
            let Ok(reply) = apply_config.await else { return };
            assert!(matches!(reply, Reply::Ok));
        })
        .detach();
    }

    fn spawn_pusher(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        task::spawn(async move {
            loop {
                let Some(this) = weak.upgrade() else { return };
                this.do_push().await;
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
        let push_value = {
            let state = self.inner.state().lock().unwrap();
            let num = state.config.num;
            state.push.clone()
        };
    }

    fn spawn_puller(self: &Arc<Self>) {
        todo!()
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ShardKv {
    kv: HashMap<String, String>,
    seen: Seen,

    me: Gid,
    config: Config,

    pull: HashMap<ConfigId, (Vec<usize>, Vec<SocketAddr>)>,
    push: HashMap<ConfigId, (Vec<usize>, Vec<SocketAddr>)>,
}

impl ShardKv {
    fn is_pulling(&self, shard: usize) -> bool {
        todo!()
    }

    fn can_serve(&self, key: &str) -> bool {
        if self.me == 0 {
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

    fn start_pull(&mut self, config: &Config) {
        let me = self.me;
        let mut config = config.clone();
        task::spawn(async move {
            let self_ck = ClerkCore::<Op, Reply>::new(config.groups.remove(&me).unwrap());
            let pull = vec![0];

            for shard in pull {}
        })
        .detach();
    }

    fn start_push(&mut self, config: &Config) {}

    fn config_migrate(&mut self, config: &Config) {
        if USE_PULL {
            self.start_pull(config);
        } else {
            self.start_push(config);
        }
    }
}

impl State for ShardKv {
    type Command = Op;
    type Output = Reply;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
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
                let config_ok = config.num == self.config.num + 1;
                let can_advance = self.pull.is_empty();
                if config_ok && can_advance {
                    info!("CONFIG {:?} -> {:?}", self.config, config);
                    self.config_migrate(&config);
                    self.config = config;
                }
            }
            Op::PullShards { shard, config_id } => {
                assert!(USE_PULL);
                if config_id == self.config.num {
                    let shards = self
                        .kv
                        .iter()
                        .filter(|&(key, _)| key2shard(key) == shard)
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<HashMap<_, _>>();
                    let seen = self.seen.clone();
                    return Reply::PullShards { shards, seen };
                }
            }
            Op::InstallShards {
                config_id,
                shards,
                seen,
            } => {
                if config_id == self.config.num {
                    self.kv.extend(shards);
                    self.seen.install(seen);
                }
            }
            Op::DelShards { shard, config_id } => {
                assert!(DELETE_AFTER_INSTALL);
            }
        }
        Reply::Ok
    }
}
