use futures::channel::mpsc;
use madsim::{fs, net, task::Task};
use serde::{Deserialize, Serialize};
use std::{
    fmt, io,
    net::SocketAddr,
    sync::{Arc, Mutex, Weak},
};

mod logs; // indicates that the logs module is in a different file
use logs::{LogEntry, Logs};

mod rpcs;
use rpcs::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply,
    RequestVoteArgs, RequestVoteReply,
};

mod roles;
use roles::Ticker;

#[derive(Clone)]
pub struct RaftHandle {
    inner: Arc<Mutex<Raft>>,
}

#[derive(Clone)]
struct WeakHandle {
    inner: Weak<Mutex<Raft>>,
}

type MsgSender = mpsc::UnboundedSender<ApplyMsg>;
pub type MsgRecver = mpsc::UnboundedReceiver<ApplyMsg>;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        term: u64,
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

    leader_id: usize,

    state_tx: StateSender,

    weak: Weak<Mutex<Raft>>,
    tasks: Vec<Task<()>>,

    snapshot: Vec<u8>,
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
}

impl State {
    const fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader)
    }
}

/// Data needs to be persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Persist {
    term: u64,
    vote_for: Option<usize>,
    logs: Logs,
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
        let n = peers.len();
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
            leader_id: (me + 1) % n,
            state_tx,
            weak: Weak::default(),
            tasks: Vec::new(),
            snapshot: Vec::new(),
        }));
        inner.lock().unwrap().weak = Arc::downgrade(&inner);

        let handle = RaftHandle { inner };
        // initialize from state persisted before a crash
        handle.restore().await.expect("failed to restore");
        handle.start_rpc_server();

        Ticker::start(&handle, state_rx);

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
        let (res, persist) = {
            let mut raft = self.inner.lock().unwrap();

            (raft.start(cmd), raft.get_persist())
        };

        self.persist(persist, None)
            .await
            .expect("failed to persist");

        res
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
    #[allow(clippy::unused_async)]
    pub async fn cond_install_snapshot(
        &self,
        _last_included_term: u64,
        _last_included_index: u64,
        _snapshot: &[u8],
    ) -> bool {
        true
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub async fn snapshot(&self, index: u64, snapshot: &[u8]) -> Result<()> {
        let (persist, snapshot) = {
            let mut raft = self.inner.lock().unwrap();
            #[allow(clippy::cast_possible_truncation)]
            let index = index as usize;
            assert!(0 < index && index <= raft.last_applied + 1);
            let last_include_index = raft.logs.snapshot().index;
            // assert!(last_include_index < index, "{last_include_index} < {index}");
            let snapshot_outdate = index <= last_include_index;
            if snapshot_outdate {
                return Ok(());
            }

            debug!(
                "CLIENT S{} snapshot S({}) at I{index} at T{}",
                raft.me,
                bincode::deserialize::<u64>(snapshot).unwrap(),
                raft.state.term
            );

            let term = raft.logs[index].term;
            raft.update_snapshot(snapshot.to_owned(), term, index);
            (raft.get_persist(), raft.get_snapshot())
        };
        self.persist(persist, Some(snapshot))
            .await
            .expect("failed to persist");

        Ok(())
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    async fn persist(&self, persist: Persist, snapshot: Option<Vec<u8>>) -> io::Result<()> {
        let state = bincode::serialize(&persist).unwrap();

        // you need to store persistent state in file "state"
        // and store snapshot in file "snapshot".
        // DO NOT change the file names.
        let file = fs::File::create("state").await?;
        file.write_all_at(&state, 0).await?;
        // make sure data is flushed to the disk,
        // otherwise data will be lost on power fail.
        file.sync_all().await?;

        if let Some(snapshot) = snapshot {
            let file = fs::File::create("snapshot").await?;
            file.write_all_at(&snapshot, 0).await?;
            file.sync_all().await?;
        }
        Ok(())
    }

    /// Restore previously persisted state.
    async fn restore(&self) -> io::Result<()> {
        match fs::read("snapshot").await {
            Ok(snapshot) => {
                let mut raft = self.inner.lock().unwrap();
                raft.restore_snapshot(snapshot);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        match fs::read("state").await {
            Ok(state) => {
                let persist: Persist = bincode::deserialize(&state).unwrap();
                let mut raft = self.inner.lock().unwrap();
                raft.restore_state(persist);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        Ok(())
    }

    fn start_rpc_server(&self) {
        let net = net::NetLocalHandle::current();

        let weak = self.weak();
        net.add_rpc_handler(move |args: RequestVoteArgs| {
            let weak = weak.clone();
            async move {
                if let Some(this) = weak.upgrade() {
                    this.request_vote(&args).await.unwrap()
                } else {
                    RequestVoteReply::default()
                }
            }
        });
        // add more RPC handlers here
        let weak = self.weak();
        net.add_rpc_handler(move |args: AppendEntriesArgs| {
            let weak = weak.clone();
            async move {
                if let Some(this) = weak.upgrade() {
                    this.append_entries(args).await.unwrap()
                } else {
                    AppendEntriesReply::default()
                }
            }
        });
        let weak = self.weak();
        net.add_rpc_handler(move |args: InstallSnapshotArgs| {
            let weak = weak.clone();
            async move {
                if let Some(this) = weak.upgrade() {
                    this.install_snapshot(args).await.unwrap()
                } else {
                    InstallSnapshotReply::default()
                }
            }
        });
    }
}

// HINT: put mutable non-async functions here
impl Raft {
    fn start(&mut self, data: &[u8]) -> Result<Start> {
        if !self.state.is_leader() {
            return Err(Error::NotLeader(self.leader_id));
        }
        let me = self.me;
        let index = self.logs.len();
        let term = self.state.term;

        // If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
        debug!(
            "CLIENT S{me} start D({}) at I{index} at T{term}",
            bincode::deserialize::<u64>(data).unwrap()
        );
        self.logs.push(LogEntry {
            data: data.to_owned(),
            term,
            index,
        });

        self.start_append_entries(term);

        Ok(Start {
            index: index as u64,
            term,
        })
    }

    // Here is an example to apply committed message.
    fn apply(&mut self) {
        assert!(self.last_applied <= self.commit_index);

        while self.last_applied < self.commit_index {
            let mut applied_index = self.last_applied + 1;
            let need_snapshot = applied_index <= self.logs.snapshot().index;
            let msg = if need_snapshot {
                let (term, index) = self.logs.snapshot().info();
                applied_index = index;
                ApplyMsg::Snapshot {
                    data: self.snapshot.clone(),
                    term,
                    index: index as u64,
                }
            } else {
                let LogEntry {
                    ref data,
                    term,
                    index,
                } = self.logs[applied_index];
                assert_eq!(index, applied_index);
                ApplyMsg::Command {
                    data: data.clone(),
                    term,
                    index: index as u64,
                }
            };

            self.apply_ch.unbounded_send(msg).unwrap();

            info!(
                "APPLY S{} apply A{} -> A{} at T{}",
                self.me, self.last_applied, applied_index, self.state.term
            );
            self.last_applied = applied_index;
        }
    }

    fn update_snapshot(&mut self, snapshot: Vec<u8>, term: u64, index: usize) {
        let old_size = self.logs.size();

        self.snapshot = snapshot;
        self.logs.install_snapshot(term, index);

        let new_size = self.logs.size();
        info!(
            "SNAPSHOT S{} S(T{term}, I{index}) L{old_size} => L{new_size} at T{}",
            self.me, self.state.term
        );
    }

    fn restore_snapshot(&mut self, snapshot: Vec<u8>) {
        self.snapshot = snapshot;
    }

    fn restore_state(&mut self, persist: Persist) {
        let Persist {
            term,
            vote_for,
            logs,
        } = persist;

        self.state.term = term;
        self.vote_for = vote_for;
        self.logs = logs;

        let new_commit_index = self.logs.snapshot().index;
        self.update_commit_index(new_commit_index);
    }

    fn get_persist(&self) -> Persist {
        let term = self.state.term;
        let vote_for = self.vote_for;
        let logs = self.logs.clone();

        Persist {
            term,
            vote_for,
            logs,
        }
    }

    fn get_snapshot(&mut self) -> Vec<u8> {
        self.snapshot.clone()
    }

    /// persist Raft state (not used because we don't use blocked io)
    #[allow(clippy::unused_self)]
    const fn persist(&self) {}
}

impl RaftHandle {
    fn weak(&self) -> WeakHandle {
        WeakHandle {
            inner: Arc::downgrade(&self.inner),
        }
    }
}

impl WeakHandle {
    fn upgrade(&self) -> Option<RaftHandle> {
        let inner = self.inner.upgrade()?;
        Some(RaftHandle { inner })
    }
}
