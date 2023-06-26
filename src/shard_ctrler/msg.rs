use super::N_SHARDS;
use crate::kvraft::msg::OpId;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr};

pub type Gid = u64;
pub type ConfigId = u64;

// A configuration -- an assignment of shards to groups.
// Please don't change this.
#[derive(Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Config {
    /// config number
    pub num: ConfigId,
    /// shard -> gid
    pub shards: [Gid; N_SHARDS],
    /// gid -> servers[]
    pub groups: HashMap<Gid, Vec<SocketAddr>>,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let groups = self
            .groups
            .iter()
            .collect::<std::collections::BTreeMap<_, _>>();
        f.debug_struct("Config")
            .field("num", &self.num)
            .field("shards", &self.shards)
            .field("groups", &groups)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    Query {
        /// desired config number
        num: ConfigId,
    },
    Join {
        /// new GID -> servers mappings
        groups: HashMap<Gid, Vec<SocketAddr>>,
        id: OpId,
    },
    Leave {
        gids: Vec<Gid>,
        id: OpId,
    },
    Move {
        shard: usize,
        gid: Gid,
        id: OpId,
    },
}
