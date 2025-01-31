use super::msg::{ClientId, Error, Op, OpId, SequenceNumber};
use crate::raft::{self, ApplyMsg, Start};
use futures::{
    channel::{mpsc::UnboundedReceiver, oneshot},
    StreamExt,
};
use madsim::{fs, net, task, time::timeout};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{self, Debug},
    net::SocketAddr,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

pub trait State: net::Message + Default {
    type Command: net::Message + Clone;
    type Output: net::Message;
    fn apply(&mut self, cmd: Self::Command) -> Self::Output;
}

type NotifyChannels<T> = HashMap<u64, (u64, oneshot::Sender<T>)>;

pub struct Server<S: State> {
    rf: raft::RaftHandle,
    me: usize,
    state: Mutex<S>,
    max_raft_state: Option<usize>,

    notify_channels: Mutex<NotifyChannels<S::Output>>,
}

impl<S: State> fmt::Debug for Server<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Server({})", self.me)
    }
}

impl<S: State> Server<S> {
    pub async fn new(
        servers: Vec<SocketAddr>,
        me: usize,
        max_raft_state: Option<usize>,
    ) -> Arc<Self> {
        // You may need initialization code here.
        let (rf, apply_ch) = raft::RaftHandle::new(servers, me).await;

        let this = Arc::new(Server {
            rf,
            me,
            state: Mutex::default(),
            max_raft_state,
            notify_channels: Mutex::default(),
        });
        this.start_rpc_server();
        this.start_applier(apply_ch);
        this
    }

    fn start_applier(self: &Arc<Self>, mut rx: UnboundedReceiver<ApplyMsg>) {
        let weak = Arc::downgrade(self);
        task::spawn(async move {
            while let Some(msg) = rx.next().await {
                let Some(this) = weak.upgrade() else {return};
                match msg {
                    ApplyMsg::Command { data, term, index } => {
                        let cmd = bincode::deserialize(&data).unwrap();
                        let reply = this.state.lock().unwrap().apply(cmd);
                        this.notify(term, index, reply);
                        if this.need_snapshot().await {
                            this.snapshot(index);
                        }
                    }
                    ApplyMsg::Snapshot { data, term, index } => {
                        if this.rf.cond_install_snapshot(term, index, &data).await {
                            let state = bincode::deserialize(&data).unwrap();
                            *this.state.lock().unwrap() = state;
                        }
                    }
                }
            }
        })
        .detach();
    }

    fn start_rpc_server(self: &Arc<Self>) {
        let net = net::NetLocalHandle::current();

        let weak = Arc::downgrade(self);
        net.add_rpc_handler(move |cmd: S::Command| {
            let weak = Weak::clone(&weak);
            async move {
                if let Some(this) = weak.upgrade() {
                    this.apply(cmd).await
                } else {
                    Err(Error::Failed)
                }
            }
        });
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.rf.term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.rf.is_leader()
    }

    pub const fn state(&self) -> &Mutex<S> {
        &self.state
    }

    /// Whether the server need snapshot
    async fn need_snapshot(&self) -> bool {
        if let Some(max) = self.max_raft_state {
            let max = max as u64;
            let size = fs::metadata("state")
                .await
                .map(|metadata| metadata.len())
                .unwrap();
            return size >= max;
        }
        false
    }

    fn snapshot(&self, index: u64) {
        let snapshot = bincode::serialize(&*self.state.lock().unwrap()).unwrap();
        let raft = self.rf.clone();
        task::spawn(async move { raft.snapshot(index, &snapshot).await }).detach();
    }

    pub async fn apply(&self, cmd: S::Command) -> Result<S::Output, Error> {
        let cmd = bincode::serialize(&cmd).unwrap();

        let res = self.rf.start(&cmd).await;
        if let Err(raft::Error::NotLeader(hint)) = res {
            return Err(Error::NotLeader { hint });
        }

        let Start { index, term } = res.unwrap();
        let rx = self.register(term, index).ok_or(Error::Failed)?;

        match timeout(Duration::from_millis(500), rx).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_)) => Err(Error::Failed),
            Err(_) => Err(Error::Timeout),
        }
    }

    fn register(&self, term: u64, index: u64) -> Option<oneshot::Receiver<S::Output>> {
        trace!("CHANNEL S{} register {index} with {term}", self.me);
        let mut notify_channels = self.notify_channels.lock().unwrap();

        let old_term = notify_channels.get(&index).map(|&(term, ..)| term);
        assert_ne!(old_term, Some(term));
        if old_term.map_or(true, |old_term| old_term < term) {
            let (tx, rx) = oneshot::channel();
            notify_channels.insert(index, (term, tx));
            return Some(rx);
        }
        None
    }

    fn notify(&self, term: u64, index: u64, reply: S::Output) {
        let mut notify_channels = self.notify_channels.lock().unwrap();
        trace!("Channel S{} before notify {:?}", self.me, notify_channels);

        if let Some((ch_term, tx)) = notify_channels.remove(&index) {
            if ch_term == term {
                std::mem::drop(tx.send(reply));
            }
        }

        let remove_keys = notify_channels
            .keys()
            .filter(|&&k| k < index)
            .copied()
            .collect::<Vec<_>>();
        let removed = remove_keys
            .into_iter()
            .map(|k| notify_channels.remove(&k).unwrap())
            .collect::<Vec<_>>();
        if !removed.is_empty() {
            trace!("Channel S{} delete obsolete {:?}", self.me, removed);
        }

        trace!("Channel S{} after notify {:?}", self.me, notify_channels);
    }
}

pub type KvServer = Server<Kv>;

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Kv {
    kv: HashMap<String, String>,
    seen: Seen,
}

impl Kv {
    pub const fn new(kv: HashMap<String, String>, seen: Seen) -> Self {
        Self { kv, seen }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Seen {
    inner: HashMap<ClientId, SequenceNumber>,
}

impl Seen {
    pub fn is_duplicate(&self, OpId { client_id, seq }: OpId) -> bool {
        self.inner.get(&client_id).map_or(false, |&old_seq| {
            assert!(old_seq <= seq);
            seq == old_seq
        })
    }

    pub fn update(&mut self, OpId { client_id, seq }: OpId) {
        trace!("STATE before update {:?}", self.inner);
        let old = self.inner.insert(client_id, seq);
        if let Some(old_seq) = old {
            assert!(old_seq < seq);
        }
        trace!("STATE after update {:?}", self.inner);
    }

    pub fn install(&mut self, seen: Seen) {
        for (id, seq) in seen.inner {
            self.inner
                .entry(id)
                .and_modify(|s| *s = seq.max(*s))
                .or_insert(seq);
        }
    }
}

impl State for Kv {
    type Command = Op;
    type Output = String;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
        match cmd {
            Op::Get { key } => return self.kv.get(&key).cloned().unwrap_or_default(),
            Op::Put { key, value, id } => {
                if !self.seen.is_duplicate(id) {
                    self.seen.update(id);

                    *self.kv.entry(key).or_default() = value;
                }
            }
            Op::Append { key, value, id } => {
                if !self.seen.is_duplicate(id) {
                    self.seen.update(id);

                    self.kv.entry(key).or_default().push_str(&value);
                }
            }
        };
        String::new()
    }
}
