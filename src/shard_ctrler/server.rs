use super::msg::*;
use crate::kvraft::server::{Seen, Server, State};
use serde::{Deserialize, Serialize};

pub type ShardCtrler = Server<ShardInfo>;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ShardInfo {
    configs: Vec<Config>,
    seen: Seen,
}

impl ShardInfo {
    fn check_invariant(&self) {
        todo!()
    }
}

impl Config {
    fn rebalance(&mut self) {
        todo!()
    }
}

impl State for ShardInfo {
    type Command = Op;
    type Output = Option<Config>;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
        match cmd {
            Op::Query { num } => {
                let num = (num as usize).min(self.configs.len() - 1);
                return Some(self.configs[num].clone());
            }
            Op::Join { groups, id } => {
                if !self.seen.is_duplicate(id) {
                    self.seen.update(id);

                    let mut new_config = self.configs.last().unwrap().clone();
                    new_config.num += 1;

                    assert!(groups.keys().all(|k| !new_config.groups.contains_key(k)));
                    new_config.groups.extend(groups);

                    new_config.rebalance();
                    self.configs.push(new_config);
                }
            }
            Op::Leave { gids, id } => {
                if !self.seen.is_duplicate(id) {
                    self.seen.update(id);

                    let mut new_config = self.configs.last().unwrap().clone();
                    new_config.num += 1;

                    gids.into_iter()
                        .for_each(|gid| assert!(new_config.groups.remove(&gid).is_some()));

                    new_config.rebalance();
                    self.configs.push(new_config);
                }
            }
            Op::Move { shard, gid, id } => {
                if !self.seen.is_duplicate(id) {
                    self.seen.update(id);

                    let mut new_config = self.configs.last().unwrap().clone();
                    new_config.num += 1;

                    assert_ne!(new_config.shards[shard], gid);
                    new_config.shards[shard] = gid;

                    new_config.rebalance();
                    self.configs.push(new_config);
                }
            }
        }
        self.check_invariant();
        None
    }
}
