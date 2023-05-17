use super::{
    Raft, RaftHandle,
    Role::{Candidate, Follower, Leader},
    State, StateReceiver,
};
use futures::{select_biased, FutureExt, StreamExt};
use madsim::{
    rand::{self, Rng},
    task,
    time::{sleep_until, Duration, Instant},
};
use std::sync::{Arc, Mutex, Weak};

impl Raft {
    // Here is an example to generate random number.
    pub fn generate_election_timeout() -> Duration {
        // see rand crate for more details
        Duration::from_millis(rand::rng().gen_range(150..300))
    }
    pub fn generate_vote_timeout() -> Duration {
        Duration::from_millis(rand::rng().gen_range(300..450))
    }
    pub const fn generate_heartbeat_interval() -> Duration {
        Duration::from_millis(50)
    }
}

pub struct Ticker {
    weak: Weak<Mutex<Raft>>,
    me: usize,

    state: State,
    state_rx: StateReceiver,
}

impl Ticker {
    pub(super) fn start(handler: &RaftHandle, state_rx: StateReceiver) {
        let mut raft = handler.inner.lock().unwrap();

        let mut ticker = Ticker {
            weak: Arc::downgrade(&handler.inner),
            me: raft.me,
            state: raft.state,
            state_rx,
        };

        raft.tasks.push(task::spawn(async move {
            ticker.run().await;
        }));
    }

    async fn run(&mut self) {
        let me = self.me;
        let term = self.state.term;
        trace!("TICKER S{me} start ticker at T{term}");

        loop {
            let State { term, role } = self.state;
            trace!("TICKER S{me} handler {role:?} at T{term}");

            match role {
                Follower => self.handle_follower().await,
                Candidate => self.handle_candidate().await,
                Leader => self.handle_leader().await,
            };
        }
    }

    async fn handle_follower(&mut self) {
        let me = self.me;
        let term = self.state.term;
        let timeout = Raft::generate_election_timeout();
        trace!("TIMER S{me} generate election timeout {timeout:?} at T{term}");

        let mut timeout = sleep_until(Instant::now() + timeout).fuse();
        select_biased! {
            // timeout => start new election
            _ = timeout => self.spawn_change_to_candidate(term),
            // state changed => continue
            state = self.state_rx.select_next_some() => self.state = state,
        };
    }
    async fn handle_candidate(&mut self) {
        let me = self.me;
        let term = self.state.term;
        let timeout = Raft::generate_vote_timeout();
        trace!("TIMER S{me} generate vote timeout {timeout:?} at T{term}");

        self.spawn_request_votes(term);

        let mut timeout = sleep_until(Instant::now() + timeout).fuse();
        select_biased! {
            _ = timeout => self.spawn_change_to_candidate(term),
            // state changed => continue
            state = self.state_rx.select_next_some() => self.state = state,
        };
    }
    async fn handle_leader(&mut self) {
        let me = self.me;
        let term = self.state.term;
        let timeout = Raft::generate_heartbeat_interval();
        trace!("TIMER S{me} generate heartbeat timeout {timeout:?} at T{term}");

        self.spawn_append_entries(term);

        let mut timeout = sleep_until(Instant::now() + timeout).fuse();
        select_biased! {
            _ = timeout => (),
            // state changed => continue
            state = self.state_rx.select_next_some() => self.state = state,
        };
    }

    fn spawn_change_to_candidate(&self, term: u64) {
        let weak = Weak::clone(&self.weak);
        task::spawn(async move {
            let Some(this) = Weak::upgrade(&weak) else {return};
            let mut raft = this.lock().unwrap();
            raft.change_to_candidate(term);
        })
        .detach();
    }
    fn spawn_request_votes(&self, term: u64) {
        let weak = Weak::clone(&self.weak);
        task::spawn(async move {
            let Some(this) = Weak::upgrade(&weak) else {return};
            let mut raft = this.lock().unwrap();
            raft.start_request_votes(term);
        })
        .detach();
    }
    fn spawn_append_entries(&self, term: u64) {
        let weak = Weak::clone(&self.weak);
        task::spawn(async move {
            let Some(this) = Weak::upgrade(&weak) else {return};
            let mut raft = this.lock().unwrap();
            raft.start_append_entries(term);
        })
        .detach();
    }
}

impl Raft {
    fn state_change(&mut self, new_state: State) {
        trace!(
            "STATE S{} {:?} -> {:?} at T{}",
            self.me,
            self.state,
            new_state,
            self.state.term
        );
        self.state = new_state;
        self.state_tx.unbounded_send(self.state).unwrap();
    }
    pub fn change_to_candidate(&mut self, term: u64) {
        if self.state.term != term || !matches!(self.state.role, Follower | Candidate) {
            return;
        }
        trace!(
            "ELECT S{} change to Candidate at T{}",
            self.me,
            self.state.term
        );

        self.state_change(State {
            term: term + 1,
            role: Candidate,
        });

        self.vote_for = Some(self.me);
        self.persist();
    }
    pub fn start_request_votes(&mut self, term: u64) {
        if self.state.term != term || !matches!(self.state.role, Candidate) {
            return;
        }
        self.send_vote_request();
    }
    pub fn start_append_entries(&mut self, term: u64) {
        if self.state.term != term || !matches!(self.state.role, Leader) {
            return;
        }
        self.send_append_entries();
    }
}
