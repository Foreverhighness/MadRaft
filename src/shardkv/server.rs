use super::{key2shard, msg::*};
use crate::kvraft::client::ClerkCore;
use crate::kvraft::server::{Seen, Server, State};
use crate::shard_ctrler::client::Clerk as CtrlerClerk;
use crate::shard_ctrler::msg::{Config, Gid};
use madsim::{task, time::sleep};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

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
        this.start_fetcher();
        this
    }

    fn start_fetcher(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        task::spawn(async move {
            loop {
                let Some(this) = weak.upgrade() else { return };
                this.fetch_config().await;
                sleep(Duration::from_millis(500)).await;
            }
        })
        .detach();
    }

    async fn fetch_config(self: &Arc<Self>) {
        let num = self.inner.state().lock().unwrap().config.num + 1;
        let config = self.ctrl_ck.query_at(num).await;
        if config.num != num {
            return;
        }
        let weak = Arc::downgrade(self);
        task::spawn(async move {
            let Some(this) = weak.upgrade() else { return };
            this.self_ck.call(Op::NewConfig { config }).await;
        })
        .detach();
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ShardKv {
    kv: HashMap<String, String>,
    seen: Seen,

    me: Gid,
    config: Config,
}

impl ShardKv {
    fn can_serve(&self, key: &str) -> bool {
        if self.me == 0 {
            return false;
        }
        let shard = key2shard(key);
        self.config.shards[shard] == self.me
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
                if config.num > self.config.num {
                    info!("CONFIG {:?} -> {:?}", self.config, config);
                    self.config = config;
                }
            }
        }
        Reply::Ok
    }
}
