use super::{logs::Logs, Raft, RaftHandle, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use madsim::{net, task};
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct RequestVoteArgs {
    term: u64,
    candidate_id: usize,
    last_log_term: u64,
    last_log_index: usize,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct RequestVoteReply {
    term: u64,
    vote_granted: bool,
}

macro_rules! function {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        type_name_of(f)
            .rsplit("::")
            .find(|&part| part != "f" && part != "{{closure}}")
            .expect("Short function name")
    }};
}

impl RaftHandle {
    pub async fn request_vote(&self, args: RequestVoteArgs) -> Result<RequestVoteReply> {
        let reply = {
            let mut this = self.inner.lock().unwrap();
            trace!(
                "RPC S{} receive {} call at T{}",
                this.me,
                function!(),
                this.state.term,
            );
            this.request_vote(args)
        };
        // if you need to persist or call async functions here,
        // make sure the lock is scoped and dropped.
        self.persist().await.expect("failed to persist");
        Ok(reply)
    }
}

impl Raft {
    pub fn request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        todo!("handle RequestVote RPC");
    }

    // Here is an example to send RPC and manage concurrent tasks.
    pub fn send_vote_request(&mut self) {
        let args: RequestVoteArgs = todo!("construct RPC request");
        let timeout = Self::generate_election_timeout();
        let net = net::NetLocalHandle::current();

        let mut rpcs = FuturesUnordered::new();
        for (i, &peer) in self.peers.iter().enumerate() {
            if i == self.me {
                continue;
            }
            // NOTE: `call` function takes ownerships
            let net = net.clone();
            let args = args.clone();
            rpcs.push(async move {
                net.call_timeout::<RequestVoteArgs, RequestVoteReply>(peer, args, timeout)
                    .await
            });
        }

        // spawn a concurrent task
        task::spawn(async move {
            // handle RPC tasks in completion order
            while let Some(res) = rpcs.next().await {
                todo!("handle RPC results");
            }
        })
        .detach(); // NOTE: you need to detach a task explicitly, or it will be cancelled on drop
    }
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct AppendEntriesArgs {
    term: u64,
    leader_id: usize,
    prev_log_term: u64,
    prev_log_index: usize,
    entries: Option<Logs>,
    leader_commit: usize,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct AppendEntriesReply {
    term: u64,
    success: bool,

    conflict_term: u64,
    conflict_index: usize,
}

impl RaftHandle {
    pub async fn append_entries(&self, args: AppendEntriesArgs) -> Result<AppendEntriesReply> {
        let reply = {
            let mut this = self.inner.lock().unwrap();
            trace!(
                "RPC S{} receive {} call at T{}",
                this.me,
                function!(),
                this.state.term,
            );
            this.append_entries(args)
        };
        // if you need to persist or call async functions here,
        // make sure the lock is scoped and dropped.
        self.persist().await.expect("failed to persist");
        Ok(reply)
    }
}

impl Raft {
    pub fn append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        todo!("handle AppendEntires RPC");
    }

    // Here is an example to send RPC and manage concurrent tasks.
    pub fn send_append_entries(&mut self) {
        let args: AppendEntriesArgs = todo!("construct RPC request");
        let timeout = Self::generate_election_timeout();
        let net = net::NetLocalHandle::current();

        let mut rpcs = FuturesUnordered::new();
        for (i, &peer) in self.peers.iter().enumerate() {
            if i == self.me {
                continue;
            }
            // NOTE: `call` function takes ownerships
            let net = net.clone();
            let args = args.clone();
            rpcs.push(async move {
                net.call_timeout::<AppendEntriesArgs, AppendEntriesReply>(peer, args, timeout)
                    .await
            });
        }

        // spawn a concurrent task
        task::spawn(async move {
            // handle RPC tasks in completion order
            while let Some(res) = rpcs.next().await {
                todo!("handle RPC results");
            }
        })
        .detach(); // NOTE: you need to detach a task explicitly, or it will be cancelled on drop
    }
}
