use super::{
    logs::LogEntry,
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
        self.match_index.resize(self.peers.len(), 0);
        self.next_index.resize(self.peers.len(), 0);

        self.match_index.fill(0);
        self.next_index.fill(self.logs.last().index + 1);
    }
    fn transform(&mut self, role: Role) {
        match role {
            Follower => (),
            Candidate => self.init_candidate(),
            Leader => self.init_leader(),
            Killed => unreachable!(),
        }
        if !(matches!(self.state.role, Follower) && matches!(role, Follower)) {
            info!(
                "ROLE S{} {:?} => {role:?} at T{}",
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
    fn set_commit_index(&mut self, new_commit_index: usize) {
        let me = self.me;
        let commit_index = self.commit_index;
        let term = self.state.term;
        info!("COMMIT S{me} C{commit_index} -> C{new_commit_index} at T{term}");
        assert!(commit_index < new_commit_index);

        self.commit_index = new_commit_index;
        self.apply();
    }
    fn update_leader_commit_index(&mut self) {
        assert!(self.state.is_leader());

        let n = {
            let num = self.peers.len();
            self.match_index[self.me] = self.logs.last().index;

            // Find the median
            *self.match_index.clone().select_nth_unstable(num / 2).1
        };
        // TODO: remove assert
        let m = {
            self.match_index[self.me] = self.logs.last().index;
            let mut v = self.match_index.clone();
            v.sort_unstable();
            v[v.len() / 2]
        };
        assert_eq!(n, m);

        // If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
        if n > self.commit_index {
            // and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
            if self.logs[n].term == self.state.term {
                self.set_commit_index(n);
            }
        }
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
        // prepare reply
        let mut vote_granted = false;

        // 1.  Reply false if term < currentTerm (§5.1)
        'deny: {
            let term_ok = self.check_term(term);
            if !term_ok {
                break 'deny;
            }
            assert_eq!(self.state.term, term);

            // 2. If votedFor is null or candidateId
            let can_vote = self.vote_for.is_none() || self.vote_for == Some(candidate_id);
            if !can_vote {
                break 'deny;
            }

            let (term, index) = self.logs.last().info();
            // and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
            let log_up_to_date = (term, index) <= (last_log_term, last_log_index);
            if !log_up_to_date {
                break 'deny;
            }

            assert_eq!(self.state.term, args.term);
            assert!(matches!(self.state.role, Follower));
            // self.transform(Follower);
            self.vote(candidate_id);

            vote_granted = true;
        }

        RequestVoteReply {
            term: self.state.term,
            vote_granted,
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
                        assert_eq!(reply.term, old_term);

                        let RequestVoteReply { vote_granted , ..} = reply;

                        // handle vote request reply
                        if vote_granted {
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

#[derive(Clone, Deserialize, Serialize, Debug, Default)]
pub struct AppendEntriesArgs {
    term: u64,
    leader_id: usize,
    prev_log_term: u64,
    prev_log_index: usize,
    entries: Vec<LogEntry>,
    leader_commit: usize,
}

#[derive(Clone, Deserialize, Serialize, Debug, Default)]
pub struct AppendEntriesReply {
    term: u64,
    success: bool,

    conflict_term: u64,
    conflict_index: usize,
}

impl Raft {
    fn append_entries_args(&self, i: usize) -> AppendEntriesArgs {
        let (prev_log_term, prev_log_index) = self.logs[self.next_index[i] - 1].info();
        let entries = self.logs[self.next_index[i]..].to_owned();
        AppendEntriesArgs {
            term: self.state.term,
            leader_id: self.me,
            prev_log_term,
            prev_log_index,
            entries,
            leader_commit: self.commit_index,
        }
    }
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
        let me = self.me;
        let term = self.state.term;
        trace!("HEART S{me} handle append entries {args:?} at T{term}");

        let AppendEntriesArgs {
            term,
            leader_id,
            prev_log_term,
            prev_log_index,
            entries,
            leader_commit,
        } = args;
        // prepare reply
        let mut success = false;
        let (mut conflict_term, mut conflict_index) = (0, 0);

        'deny: {
            // 1. Reply false if term < currentTerm (§5.1)
            let term_ok = self.check_term(term);
            if !term_ok {
                break 'deny;
            }
            assert_eq!(self.state.term, term);

            // If the leader’s term is at least as large as the candidate’s current term,
            // then the candidate recognizes the leader as legitimate and returns to follower state.
            self.transform(Follower);
            self.leader_id = Some(leader_id);
            trace!("S{me} get heartbeat from L{leader_id} at T{term}");

            let entry = self.logs.get(prev_log_index);
            // 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
            let log_matched = entry.map(|e| e.term) == Some(prev_log_term);
            if !log_matched {
                // entry term miss match
                if let Some(entry) = entry {
                    conflict_term = entry.term;
                    conflict_index = self.logs.find_first(entry.term);
                } else {
                    conflict_index = self.logs.len();
                }
                break 'deny;
            }

            let entries_matched = self.logs.matches(&entries, prev_log_index + 1);
            // 3. If an existing entry conflicts with a new one (same index but different terms),
            let entry_conflicted = entries_matched != entries.len();
            if entry_conflicted {
                // delete the existing entry and all that follow it (§5.3)
                self.logs.truncate(prev_log_index + entries_matched + 1);
            }

            // 4. Append any new entries not already in the log
            let has_new_entries = entries_matched != entries.len();
            if has_new_entries {
                self.logs.extend_from_slice(&entries[entries_matched..]);
                self.persist();
            }

            // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if leader_commit > self.commit_index {
                let commit_index = self.commit_index;
                let new_commit_index = leader_commit.min(self.logs.last().index);

                self.set_commit_index(new_commit_index);
            }

            success = true;
        }

        AppendEntriesReply {
            term: self.state.term,
            success,
            conflict_term,
            conflict_index,
        }
    }

    fn handle_append_entries_reply(
        &mut self,
        reply: &AppendEntriesReply,
        new_match_index: usize,
        i: usize,
    ) {
        let me = self.me;
        let old_term = self.state.term;

        let AppendEntriesReply {
            term,
            success,
            conflict_term,
            conflict_index,
        } = *reply;

        if success {
            // the correct thing to do is update matchIndex to be prevLogIndex + len(entries[]) from the arguments you sent in the RPC originally.
            // https://thesquareplanet.com/blog/students-guide-to-raft/#term-confusion
            let new_match_index = new_match_index;

            // If successful: update nextIndex and matchIndex for follower (§5.3)
            if new_match_index > self.match_index[i] {
                info!(
                    "COMMIT S{i} M({}) -> M({}) with L{me} at T{old_term}",
                    self.match_index[i], new_match_index
                );
                self.match_index[i] = new_match_index;

                info!(
                    "COMMIT S{i} N({}) -> N({}) with L{me} at T{old_term}",
                    self.next_index[i],
                    new_match_index + 1
                );
                self.next_index[i] = new_match_index + 1;

                self.update_leader_commit_index();
            }
        } else {
            let next_index = self.logs.find_last(conflict_term).unwrap_or(conflict_index);
            // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
            info!(
                "COMMIT S{i} N({}) -> N({}) with L{me} at T{old_term}",
                self.next_index[i], next_index
            );
            self.next_index[i] = next_index;
        }
    }

    // Here is an example to send RPC and manage concurrent tasks.
    pub fn send_append_entries(&mut self) {
        let me = self.me;
        let old_state = self.state;
        let old_term = old_state.term;

        // prepare args
        let timeout = Raft::generate_heartbeat_interval();
        let net = net::NetLocalHandle::current();

        // prepare futures
        let mut rpcs = FuturesUnordered::new();
        for (i, &peer) in self.peers.iter().enumerate() {
            if i == self.me {
                continue;
            }
            // NOTE: `call` function takes ownerships
            let net = net.clone();
            let args = self.append_entries_args(i);
            rpcs.push(async move {
                let new_match_index = args.prev_log_index + args.entries.len();
                let res = net
                    .call_timeout::<AppendEntriesArgs, AppendEntriesReply>(peer, args, timeout)
                    .await;
                (res, new_match_index, i)
            });
        }

        // spawn a concurrent task
        let weak = Weak::clone(&self.weak);
        task::spawn(async move {
            // handle RPC tasks in completion order
            while let Some((res, new_match_index, i)) = rpcs.next().await {
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
                        assert_eq!(reply.term, old_term);

                        raft.handle_append_entries_reply(&reply, new_match_index, i);
                    }
                    Err(e) => trace!("HEART S{me} got RPC error {e:?} from S{i} at T{old_term}"),
                }
            }
        })
        .detach(); // NOTE: you need to detach a task explicitly, or it will be cancelled on drop
    }
}
