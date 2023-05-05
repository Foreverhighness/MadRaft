use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct Logs {}

impl Logs {
    pub fn new() -> Logs {
        Self {}
    }
}
