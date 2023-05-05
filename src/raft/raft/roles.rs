use super::{Raft, RaftHandle, Role::*, State};
use futures::{select_biased, FutureExt};
use futures_timer::Delay;

impl RaftHandle {
    async fn ticker(&self, mut state: State) {
        loop {
            state = match state.role {
                Follower => self.handle_follower(state).await,
                Candidate => todo!(),
                Leader => todo!(),
                Killed => todo!(),
            };
        }
    }
    async fn handle_follower(&self, old_state: State) -> State {
        let mut timeout = Delay::new(Raft::generate_election_timeout()).fuse();
        select_biased! {
            _ = timeout => ()
        };
        todo!()
    }
}
