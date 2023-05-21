use super::msg::*;
use crate::kvraft::{client::ClerkCore, msg::OpId};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering::Relaxed},
};

pub struct Clerk {
    core: ClerkCore<Op, Option<Config>>,

    seq: AtomicUsize,
}

impl Clerk {
    pub fn new(servers: Vec<SocketAddr>) -> Clerk {
        Clerk {
            core: ClerkCore::new(servers),
            seq: AtomicUsize::default(),
        }
    }

    fn op_id(&self) -> OpId {
        OpId {
            client_id: self.core.me,
            seq: self.seq.load(Relaxed),
        }
    }

    pub async fn query(&self) -> Config {
        self.core.call(Op::Query { num: u64::MAX }).await.unwrap()
    }

    pub async fn query_at(&self, num: u64) -> Config {
        self.core.call(Op::Query { num }).await.unwrap()
    }

    pub async fn join(&self, groups: HashMap<Gid, Vec<SocketAddr>>) {
        let id = self.op_id();
        let ret = self.core.call(Op::Join { groups, id }).await;
        assert!(ret.is_none());
        self.seq.fetch_add(1, Relaxed);
    }

    pub async fn leave(&self, gids: &[u64]) {
        let gids = gids.to_owned();
        let id = self.op_id();
        let ret = self.core.call(Op::Leave { gids, id }).await;
        assert!(ret.is_none());
        self.seq.fetch_add(1, Relaxed);
    }

    pub async fn move_(&self, shard: usize, gid: u64) {
        let id = self.op_id();
        let ret = self.core.call(Op::Move { shard, gid, id }).await;
        assert!(ret.is_none());
        self.seq.fetch_add(1, Relaxed);
    }
}
