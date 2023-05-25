use crate::{
    kvraft::{msg::OpId, server::Seen},
    shard_ctrler::msg::{Config, ConfigId},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    Get {
        key: String,
    },
    Put {
        key: String,
        value: String,
        id: OpId,
    },
    Append {
        key: String,
        value: String,
        id: OpId,
    },
    NewConfig {
        config: Config,
    },
    PullShards {
        config_id: ConfigId,
        shards: Vec<usize>,
    },
    InstallShards {
        config_id: ConfigId,
        shards: Vec<usize>,
        kv: HashMap<String, String>,
        seen: Seen,
    },
    DelShards {
        config_id: ConfigId,
        shards: Vec<usize>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Reply {
    Get {
        value: Option<String>,
    },
    PullShards {
        shards: HashMap<String, String>,
        seen: Seen,
    },
    Ok,
    WrongGroup,
    WrongConfig,
}
