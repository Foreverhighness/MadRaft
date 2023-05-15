use super::{
    Raft, RaftHandle,
    Role::{Candidate, Follower, Killed, Leader},
    State, StateReceiver,
};
use futures::{select_biased, FutureExt, StreamExt};
use madsim::{
    rand::{self, Rng},
    task,
    time::{sleep_until, Duration, Instant},
};

impl Raft {
    pub const VOTE_TIMEOUT_MAX: Duration = Duration::from_millis(300);
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

// Ticker
impl RaftHandle {
    // TODO: replace with weak pointer
    pub(super) fn start_ticker(&self, state_rx: StateReceiver) {
        let init_term = self.inner.lock().unwrap().state.term;
        let this = self.clone();
        task::spawn(async move {
            this.ticker(init_term, state_rx).await;
        })
        .detach();
    }

    // TODO: replace with weak pointer
    async fn ticker(&self, init_term: u64, mut state_rx: StateReceiver) {
        let me = self.me;
        trace!("TICKER S{me} start ticker at T{init_term}");

        let mut state = State {
            term: init_term,
            role: Follower,
        };
        loop {
            let State { term, role } = state;
            trace!("TICKER S{me} handler {role:?} at T{term}");

            state = match state.role {
                Follower => self.handle_follower(state, &mut state_rx).await,
                Candidate => self.handle_candidate(state, &mut state_rx).await,
                Leader => self.handle_leader(state, &mut state_rx).await,
                Killed => unreachable!(),
            };
        }

        let State { term, role } = state;
        assert!(matches!(role, Killed));
        trace!("TICKER S{me} end {role:?} at T{term}");
    }

    async fn handle_follower(&self, state: State, state_rx: &mut StateReceiver) -> State {
        let me = self.me;
        let term = state.term;
        let timeout = Raft::generate_election_timeout();
        trace!("TIMER S{me} generate election timeout {timeout:?} at T{term}");

        let mut timeout = sleep_until(Instant::now() + timeout).fuse();
        select_biased! {
            // timeout => start new election
            _ = timeout => self.spawn_change_to_candidate(term),
            // state changed => continue
            new_state = state_rx.select_next_some() => return new_state,
        };
        state
    }
    async fn handle_candidate(&self, state: State, state_rx: &mut StateReceiver) -> State {
        let me = self.me;
        let term = state.term;
        let timeout = Raft::generate_vote_timeout();
        trace!("TIMER S{me} generate vote timeout {timeout:?} at T{term}");

        self.spawn_request_votes(term);

        let mut timeout = sleep_until(Instant::now() + timeout).fuse();
        select_biased! {
            _ = timeout => self.spawn_change_to_candidate(term),
            // state changed => continue
            new_state = state_rx.select_next_some() => return new_state,
        };
        state
    }
    async fn handle_leader(&self, state: State, state_rx: &mut StateReceiver) -> State {
        let me = self.me;
        let term = state.term;
        let timeout = Raft::generate_heartbeat_interval();
        trace!("TIMER S{me} generate heartbeat timeout {timeout:?} at T{term}");

        self.spawn_append_entries(term);

        let mut timeout = sleep_until(Instant::now() + timeout).fuse();
        select_biased! {
            _ = timeout => (),
            // state changed => continue
            new_state = state_rx.select_next_some() => return new_state,
        };
        state
    }

    fn spawn_change_to_candidate(&self, term: u64) {
        let this = self.clone();
        task::spawn(async move {
            let mut raft = this.inner.lock().unwrap();
            raft.change_to_candidate(term);
        })
        .detach();
    }
    fn spawn_request_votes(&self, term: u64) {
        let this = self.clone();
        task::spawn(async move {
            let mut raft = this.inner.lock().unwrap();
            raft.start_request_votes(term);
        })
        .detach();
    }
    fn spawn_append_entries(&self, term: u64) {
        let this = self.clone();
        task::spawn(async move {
            let mut raft = this.inner.lock().unwrap();
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
