use super::msg::*;
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
    sync::{atomic::AtomicU64, Arc, Mutex},
    time::Duration,
};

pub trait State: net::Message + Default {
    type Command: net::Message + Clone;
    type Output: net::Message;
    fn apply(&mut self, cmd: Self::Command) -> Self::Output;
}

type NotifyChannels<T> = HashMap<u64, HashMap<ChannelId, oneshot::Sender<T>>>;

pub struct Server<S: State> {
    rf: raft::RaftHandle,
    me: usize,
    state: Mutex<S>,
    max_raft_state: Option<usize>,

    notify_channels: Mutex<NotifyChannels<S::Output>>,
}

type ChannelId = u64;

/// For debugging purposes, this function does not return a random value.
fn generate_channel_id() -> ChannelId {
    static COUNT: AtomicU64 = AtomicU64::new(0);
    COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
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
                    ApplyMsg::Command { data, index } => {
                        let (channel_id, cmd) = bincode::deserialize(&data).unwrap();
                        let reply = this.state.lock().unwrap().apply(cmd);
                        this.notify(index, channel_id, reply);
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

        // TODO: lab3 replace with weak pointer
        let this = Arc::clone(self);
        net.add_rpc_handler(move |cmd: S::Command| {
            let this = Arc::clone(&this);
            async move { this.apply(cmd).await }
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

    async fn apply(&self, cmd: S::Command) -> Result<S::Output, Error> {
        let channel_id = generate_channel_id();
        let cmd = bincode::serialize(&(channel_id, cmd)).unwrap();

        let res = self.rf.start(&cmd).await;
        if let Err(raft::Error::NotLeader(hint)) = res {
            return Err(Error::NotLeader { hint });
        }

        // TODO: use term to simplify channels
        let Start { index, term } = res.unwrap();

        let rx = self.register(index, channel_id);
        match timeout(Duration::from_millis(500), rx).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(e)) => Err(Error::Failed),
            Err(e) => Err(Error::Timeout),
        }
    }

    fn register(&self, index: u64, channel_id: ChannelId) -> oneshot::Receiver<S::Output> {
        trace!("CHANNEL S{} register {index} with {channel_id}", self.me);
        let mut notify_channels = self.notify_channels.lock().unwrap();
        let channels_with_index = notify_channels.entry(index).or_default();

        let (tx, rx) = oneshot::channel();
        let old = channels_with_index.insert(channel_id, tx);
        assert!(old.is_none());

        rx
    }

    fn notify(&self, index: u64, channel_id: ChannelId, reply: S::Output) {
        let mut notify_channels = self.notify_channels.lock().unwrap();
        if self.is_leader() {
            trace!("CHANNEL S{} notify {index} with {channel_id}", self.me);
            let Some(channels_with_index) = notify_channels.get_mut(&index) else { return };

            let Some(tx) = channels_with_index.remove(&channel_id) else { return };
            std::mem::drop(tx.send(reply));

            // remove old notify channels
            notify_channels.remove(&index).unwrap();
        } else {
            notify_channels.clear();
        }

        assert!(notify_channels.keys().all(|&key| key > index));
    }
}

pub type KvServer = Server<Kv>;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Kv {
    kv: HashMap<String, String>,
    // TODO: lab3 remove Vec wrapper
    seen: HashMap<ClientId, Vec<(SequenceNumber, Op, Reply)>>,
}
impl Kv {
    fn check_duplicate(&self, OpId { client_id, seq }: OpId, cmd: &Op) -> Option<Reply> {
        let vec = self.seen.get(&client_id)?;
        let (_, ref op, ref reply) = *vec.iter().find(|&&(s, _, _)| s == seq)?;
        assert_eq!(op, cmd);
        Some(reply.clone())
    }

    fn update_seen(&mut self, OpId { client_id, seq }: OpId, cmd: Op, reply: Reply) {
        // TODO: lab3 remove old seen
        self.seen
            .entry(client_id)
            .or_default()
            .push((seq, cmd, reply));
    }
}

impl State for Kv {
    type Command = Op;
    type Output = String;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
        // TODO: lab3 remove clone
        match cmd.clone() {
            Op::Get { key } => return self.kv.get(&key).cloned().unwrap_or_default(),
            Op::Put { key, value, id } => {
                if let Some(reply) = self.check_duplicate(id, &cmd) {
                    return reply;
                }
                *self.kv.entry(key).or_default() = value;
                self.update_seen(id, cmd, String::new());
            }
            Op::Append { key, value, id } => {
                if let Some(reply) = self.check_duplicate(id, &cmd) {
                    return reply;
                }
                self.kv.entry(key).or_default().push_str(&value);
                self.update_seen(id, cmd, String::new());
            }
        };
        String::new()
    }
}
