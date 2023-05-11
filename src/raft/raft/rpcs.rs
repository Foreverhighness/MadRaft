use super::{
    logs::Logs,
    Raft, RaftHandle, Result,
    Role::{self, Candidate, Follower, Killed, Leader},
    State,
};
use futures::{stream::FuturesUnordered, StreamExt};
use madsim::{net, task};
use serde::{Deserialize, Serialize};
use std::sync::Weak;

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

impl Raft {
    fn request_vote_args(&self) -> RequestVoteArgs {
        let term = self.state.term;
        let candidate_id = self.me;
        let (last_log_term, last_log_index) = self.logs.last().info();
        RequestVoteArgs {
            term,
            candidate_id,
            last_log_term,
            last_log_index,
        }
    }
}

macro_rules! function {
    () => {{
        const fn f() {}
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
    pub async fn request_vote(&self, args: &RequestVoteArgs) -> Result<RequestVoteReply> {
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
    fn update_term(&mut self, term: u64) {
        info!("TERM S{} T{} -> T{}", self.me, self.state.term, term);

        self.state.term = term;
        self.vote_for = None;
        self.persist();
        self.leader_id = None;
    }
    fn check_term(&mut self, term: u64) -> bool {
        if self.state.term < term {
            self.update_term(term);
            self.transform(Follower);
        }
        self.state.term == term
    }
    fn init_candidate(&mut self) {
        assert!(matches!(self.state.role, Follower | Candidate));
        self.update_term(self.state.term + 1);
        self.vote(self.me);

        let me = self.me;
        let threshold = self.peers.len() / 2 + 1;
        let term = self.state.term;
        info!("VOTE S{me} get vote 1/{threshold} from S{me} at T{term}");
    }
    fn init_leader(&mut self) {
        let me = self.me;
        self.leader_id = Some(me);
        self.match_index.fill(self.logs.last().index + 1);
        self.next_index.fill(0);
    }
    fn transform(&mut self, role: Role) {
        match role {
            Follower => (),
            Candidate => self.init_candidate(),
            Leader => self.init_leader(),
            Killed => todo!(),
        }
        if !(matches!(self.state.role, Follower) && matches!(role, Follower)) {
            info!(
                "ROLE S{} {:?} => {role:?} at {}",
                self.me, self.state.role, self.state.term
            );
        }
        self.state.role = role;
        self.state_tx.unbounded_send(self.state).unwrap();
    }
    fn vote(&mut self, candidate_id: usize) {
        assert!(self.vote_for.is_none() || self.vote_for == Some(candidate_id));
        info!(
            "VOTE S{} => S{} vote at T{}",
            self.me, candidate_id, self.state.term
        );

        self.vote_for = Some(candidate_id);
        self.persist();
    }
}

impl Raft {
    pub fn request_vote(&mut self, args: &RequestVoteArgs) -> RequestVoteReply {
        let me = self.me;
        let term = self.state.term;
        trace!("VOTE S{me} handle request vote {args:?} at T{term}",);

        let RequestVoteArgs {
            term,
            candidate_id,
            last_log_term,
            last_log_index,
        } = *args;

        // 1.  Reply false if term < currentTerm (§5.1)
        if self.check_term(term) {
            // 2. If votedFor is null or candidateId
            if self.vote_for.is_none() || self.vote_for == Some(candidate_id) {
                let (term, index) = self.logs.last().info();

                // and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
                if (term, index) <= (last_log_term, last_log_index) {
                    assert_eq!(
                        self.state,
                        State {
                            term: args.term,
                            role: Follower
                        }
                    );
                    // self.transform(Follower);
                    self.vote(candidate_id);
                    return RequestVoteReply {
                        term: self.state.term,
                        vote_granted: true,
                    };
                }
            }
        }
        RequestVoteReply {
            term: self.state.term,
            vote_granted: false,
        }
    }

    // Here is an example to send RPC and manage concurrent tasks.
    pub fn send_vote_request(&mut self) {
        // prepare args
        let args: RequestVoteArgs = self.request_vote_args();
        let old_state = self.state;

        assert_eq!(
            old_state,
            State {
                term: args.term,
                role: Candidate,
            }
        );

        let me = self.me;
        let old_term = old_state.term;
        trace!("VOTE S{me} send vote with args: {args:?} at T{old_term}",);

        // prepare futures
        let timeout = Raft::VOTE_TIMEOUT_MAX;
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
                let res = net
                    .call_timeout::<RequestVoteArgs, RequestVoteReply>(peer, args, timeout)
                    .await;
                (res, i)
            });
        }

        // spawn a concurrent task
        let weak = Weak::clone(&self.weak);
        let threshold = self.peers.len() / 2 + 1;
        task::spawn(async move {
            // handle RPC tasks in completion order
            let mut vote_count = 1;
            while let Some((res, i)) = rpcs.next().await {
                match res {
                    Ok(reply) => {
                        let Some(this) = weak.upgrade() else {return};
                        let mut raft = this.lock().unwrap();
                        if raft.state != old_state {
                            return;
                        }

                        assert!(reply.term >= old_term);
                        raft.check_term(reply.term);
                        if raft.state != old_state {
                            return;
                        }

                        if reply.vote_granted {
                            vote_count += 1;
                            info!(
                                "VOTE S{me} get vote {vote_count}/{threshold} from {i} at T{old_term}"
                            );
                            if vote_count >= threshold {
                                raft.transform(Leader);
                                return;
                            }
                        }
                    }
                    Err(e) => trace!("VOTE S{me} got RPC error {e:?} from {i} with T{old_term}"),
                }
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
    pub async fn append_entries(&self, args: &AppendEntriesArgs) -> Result<AppendEntriesReply> {
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
    pub fn append_entries(&mut self, args: &AppendEntriesArgs) -> AppendEntriesReply {
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
