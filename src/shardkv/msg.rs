use crate::{kvraft::msg::OpId, shard_ctrler::msg::Config};
use serde::{Deserialize, Serialize};

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Reply {
    Get { value: Option<String> },
    Ok,
    WrongGroup,
}
