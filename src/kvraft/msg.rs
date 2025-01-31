use serde::{Deserialize, Serialize};

pub type ClientId = usize;
pub type SequenceNumber = usize;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpId {
    pub client_id: ClientId,
    pub seq: SequenceNumber,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
}

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Error {
    #[error("not leader, hint: {hint}")]
    NotLeader { hint: usize },
    #[error("timeout")]
    Timeout,
    #[error("failed to reach consensus")]
    Failed,
}
