use super::msg::{Config, Op};
use crate::kvraft::server::{Seen, Server, State};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type ShardCtrler = Server<ShardInfo>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ShardInfo {
    configs: Vec<Config>,
    seen: Seen,
}

impl Default for ShardInfo {
    fn default() -> Self {
        Self {
            configs: vec![Config::default()],
            seen: Seen::default(),
        }
    }
}

impl ShardInfo {
    fn check_invariant(&self) {
        let not_empty = !self.configs.is_empty();
        assert!(not_empty);
        let monotonic_increase = self.configs.windows(2).all(|s| s[0].num + 1 == s[1].num);
        assert!(monotonic_increase);
    }
}

impl Config {
    fn rebalance(&mut self) {
        if self.groups.is_empty() {
            self.shards.iter_mut().for_each(|gid| *gid = 0);
            return;
        }

        let mut gid_shard_count = self
            .groups
            .keys()
            .map(|&gid| (gid, 0))
            .collect::<BTreeMap<_, _>>();
        let mut not_allocated_shards = Vec::new();
        for (shard, gid) in self.shards.iter().enumerate() {
            if let Some(count) = gid_shard_count.get_mut(gid) {
                *count += 1;
            } else {
                not_allocated_shards.push(shard);
            }
        }

        for shard in not_allocated_shards {
            let (&gid, count) = gid_shard_count
                .iter_mut()
                .min_by_key(|&(_, &mut count)| count)
                .unwrap();
            self.shards[shard] = gid;
            *count += 1;
        }

        loop {
            let more = gid_shard_count
                .iter()
                .max_by_key(|&(_, &count)| count)
                .map(|(&gid, ..)| gid)
                .unwrap();
            let less = gid_shard_count
                .iter()
                .min_by_key(|&(_, &count)| count)
                .map(|(&gid, ..)| gid)
                .unwrap();

            assert!(gid_shard_count[&more] >= gid_shard_count[&less]);
            if gid_shard_count[&more] - gid_shard_count[&less] <= 1 {
                break;
            }
            *self
                .shards
                .iter_mut()
                .find(|&&mut gid| gid == more)
                .unwrap() = less;
            *gid_shard_count.get_mut(&more).unwrap() -= 1;
            *gid_shard_count.get_mut(&less).unwrap() += 1;
        }
    }
}

impl State for ShardInfo {
    type Command = Op;
    type Output = Option<Config>;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
        match cmd {
            Op::Query { num } => {
                #[allow(clippy::cast_possible_truncation)]
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

                    new_config.shards[shard] = gid;

                    self.configs.push(new_config);
                }
            }
        }
        self.check_invariant();
        None
    }
}
