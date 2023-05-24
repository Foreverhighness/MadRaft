use super::{key2shard, msg::*};
use crate::{
    kvraft::{client::ClerkCoreRef, msg::OpId},
    shard_ctrler::{
        client::Clerk as CtrlerClerk,
        msg::{Config, Gid},
    },
};
use futures::lock::Mutex;
use madsim::time::{sleep, timeout, Duration};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering::Relaxed},
};

pub struct Clerk {
    ctrl_ck: CtrlerClerk,

    config: Mutex<Config>,
    seq: AtomicUsize,
    leaders: Mutex<HashMap<Gid, AtomicUsize>>,
}

impl Clerk {
    pub fn new(servers: Vec<SocketAddr>) -> Clerk {
        Clerk {
            ctrl_ck: CtrlerClerk::new(servers),
            config: Mutex::default(),
            seq: AtomicUsize::default(),
            leaders: Mutex::default(),
        }
    }

    const fn me(&self) -> usize {
        self.ctrl_ck.me()
    }

    fn op_id(&self) -> OpId {
        OpId {
            client_id: self.me(),
            seq: self.seq.load(Relaxed),
        }
    }

    pub async fn get(&self, key: String) -> String {
        let Reply::Get { value } = self.call(Op::Get { key }).await else { unreachable!() };
        value.unwrap_or_default()
    }

    pub async fn put(&self, key: String, value: String) {
        let id = self.op_id();
        let Reply::Ok = self.call(Op::Put { key, value, id }).await else { unreachable!() };
        self.seq.fetch_add(1, Relaxed);
    }

    pub async fn append(&self, key: String, value: String) {
        let id = self.op_id();
        let Reply::Ok = self.call(Op::Append { key, value, id }).await else { unreachable!() };
        self.seq.fetch_add(1, Relaxed);
    }

    async fn call(&self, args: Op) -> Reply {
        let me = self.me();
        let shard = key2shard(args.key());
        trace!("CLIENT C{me} call args {args:?}");

        let mut config = self.config.lock().await;
        loop {
            let gid = config.shards[shard];
            'outer: {
                if gid == 0 {
                    break 'outer;
                }
                let servers = &config.groups[&gid];
                let mut leaders = self.leaders.lock().await;
                let leader = leaders.entry(gid).or_default();

                let core = ClerkCoreRef::<Op, Reply>::new(servers, leader, me);

                match timeout(Duration::from_secs(1), core.call(args.clone())).await {
                    Ok(reply) => match reply {
                        Reply::Get { .. } | Reply::Ok => return reply,
                        Reply::WrongGroup => sleep(Duration::from_millis(100)).await,
                        Reply::PullShards { .. } => unreachable!(),
                    },
                    Err(e) => trace!("CLIENT C{me} got error from G{gid} {e:?}"),
                }
            }
            *config = self.ctrl_ck.query().await;
        }
    }
}

impl Op {
    fn key(&self) -> &str {
        match *self {
            Op::Get { ref key } | Op::Put { ref key, .. } | Op::Append { ref key, .. } => key,
            _ => unreachable!(),
        }
    }
}
