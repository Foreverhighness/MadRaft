#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::diverging_sub_expression)]
#![allow(unreachable_code)]
use futures::channel::mpsc;
use madsim::{fs, net};
use serde::{Deserialize, Serialize};
use std::{
    fmt, io,
    net::SocketAddr,
    sync::{Arc, Mutex, Weak},
};

mod logs; // indicates that the logs module is in a different file
use logs::*;

mod rpcs;
use rpcs::*;

mod roles;

#[derive(Clone)]
pub struct RaftHandle {
    inner: Arc<Mutex<Raft>>,
    me: usize,
}

type MsgSender = mpsc::UnboundedSender<ApplyMsg>;
pub type MsgRecver = mpsc::UnboundedReceiver<ApplyMsg>;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

#[derive(Debug)]
pub struct Start {
    /// The index that the command will appear at if it's ever committed.
    pub index: u64,
    /// The current term.
    pub term: u64,
}

type StateSender = mpsc::UnboundedSender<State>;
type StateReceiver = mpsc::UnboundedReceiver<State>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("this node is not a leader, next leader: {0}")]
    NotLeader(usize),
    #[error("IO error")]
    IO(#[from] io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

struct Raft {
    peers: Vec<SocketAddr>,
    me: usize,
    apply_ch: MsgSender,

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    state: State,

    vote_for: Option<usize>,
    logs: Logs,

    // volatile state on all servers
    commit_index: usize,
    last_applied: usize,

    // volatile state on leader
    // (reinitialized after election)
    next_index: Vec<usize>,
    match_index: Vec<usize>,

    // TODO: remove Option wrapper
    leader_id: Option<usize>,

    state_tx: StateSender,

    weak: Weak<Mutex<Raft>>,
}

/// State of a raft peer.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct State {
    term: u64,
    role: Role,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
    Killed,
}

impl State {
    const fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader)
    }
}

/// Data needs to be persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Persist {
    // Your data here.
}

impl fmt::Debug for Raft {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Raft({})", self.me)
    }
}

// HINT: put async functions here
impl RaftHandle {
    pub async fn new(peers: Vec<SocketAddr>, me: usize) -> (Self, MsgRecver) {
        let (apply_ch, recver) = mpsc::unbounded();
        let (state_tx, state_rx) = mpsc::unbounded();
        let inner = Arc::new(Mutex::new(Raft {
            peers,
            me,
            apply_ch,
            state: State::default(),
            vote_for: None,
            logs: Logs::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: Vec::new(),
            match_index: Vec::new(),
            leader_id: None,
            state_tx,
            weak: Weak::default(),
        }));
        inner.lock().unwrap().weak = Arc::downgrade(&inner);

        let handle = RaftHandle { inner, me };
        // initialize from state persisted before a crash
        handle.restore().await.expect("failed to restore");
        handle.start_rpc_server();

        handle.start_ticker(state_rx);

        (handle, recver)
    }

    /// Start agreement on the next command to be appended to Raft's log.
    ///
    /// If this server isn't the leader, returns [`Error::NotLeader`].
    /// Otherwise start the agreement and return immediately.
    ///
    /// There is no guarantee that this command will ever be committed to the
    /// Raft log, since the leader may fail or lose an election.
    pub async fn start(&self, cmd: &[u8]) -> Result<Start> {
        let mut raft = self.inner.lock().unwrap();
        info!("{:?} start", *raft);
        raft.start(cmd)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        let raft = self.inner.lock().unwrap();
        raft.state.term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        let raft = self.inner.lock().unwrap();
        raft.state.is_leader()
    }

    /// A service wants to switch to snapshot.
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub async fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        true
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub async fn snapshot(&self, index: u64, snapshot: &[u8]) -> Result<()> {
        todo!()
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    async fn persist(&self) -> io::Result<()> {
        return Ok(());
        let persist: Persist = todo!("persist state");
        let snapshot: Vec<u8> = todo!("persist snapshot");
        let state = bincode::serialize(&persist).unwrap();

        // you need to store persistent state in file "state"
        // and store snapshot in file "snapshot".
        // DO NOT change the file names.
        let file = fs::File::create("state").await?;
        file.write_all_at(&state, 0).await?;
        // make sure data is flushed to the disk,
        // otherwise data will be lost on power fail.
        file.sync_all().await?;

        let file = fs::File::create("snapshot").await?;
        file.write_all_at(&snapshot, 0).await?;
        file.sync_all().await?;
        Ok(())
    }

    /// Restore previously persisted state.
    async fn restore(&self) -> io::Result<()> {
        match fs::read("snapshot").await {
            Ok(snapshot) => {
                todo!("restore snapshot");
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        match fs::read("state").await {
            Ok(state) => {
                let persist: Persist = bincode::deserialize(&state).unwrap();
                todo!("restore state");
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        Ok(())
    }

    fn start_rpc_server(&self) {
        let net = net::NetLocalHandle::current();

        let this = self.clone();
        net.add_rpc_handler(move |args: RequestVoteArgs| {
            let this = this.clone();
            async move { this.request_vote(&args).await.unwrap() }
        });
        // add more RPC handlers here
        let this = self.clone();
        net.add_rpc_handler(move |args: AppendEntriesArgs| {
            let this = this.clone();
            async move { this.append_entries(args).await.unwrap() }
        });
    }
}

// HINT: put mutable non-async functions here
impl Raft {
    fn start(&mut self, data: &[u8]) -> Result<Start> {
        if !self.state.is_leader() {
            let leader_id = (self.me + 1) % self.peers.len();
            return Err(Error::NotLeader(self.leader_id.unwrap_or(leader_id)));
        }
        let me = self.me;
        let index = self.logs.len();
        let term = self.state.term;

        // If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
        debug!("CLIENT S{me} start D({data:?}) at I{index} at T{term}");
        self.logs.push(LogEntry {
            data: data.to_owned(),
            term,
            index,
        });

        Ok(Start {
            index: index as u64,
            term,
        })
    }

    // Here is an example to apply committed message.
    fn apply(&mut self) {
        while self.last_applied < self.commit_index {
            let index = self.last_applied + 1;
            let data = self.logs[index].data.clone();

            let msg = ApplyMsg::Command {
                data,
                index: index as u64,
            };
            self.apply_ch.unbounded_send(msg).unwrap();
            self.last_applied += 1;
        }
    }

    /// persist Raft state (not used because we don't use blocked io)
    #[allow(clippy::unused_self)]
    const fn persist(&self) {}
}
