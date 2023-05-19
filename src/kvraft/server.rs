use super::msg::*;
use crate::raft::{self, ApplyMsg};
use futures::{channel::mpsc::UnboundedReceiver, StreamExt};
use madsim::{fs, net, task};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{self, Debug},
    net::SocketAddr,
    sync::{Arc, Mutex},
};

pub trait State: net::Message + Default {
    type Command: net::Message + Clone;
    type Output: net::Message;
    fn apply(&mut self, cmd: Self::Command) -> Self::Output;
}

pub struct Server<S: State> {
    rf: raft::RaftHandle,
    me: usize,
    state: Mutex<S>,
    max_raft_state: Option<usize>,
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
                        let cmd = bincode::deserialize(&data).unwrap();
                        let reply = this.state.lock().unwrap().apply(cmd);
                        this.notify(index);
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

    fn notify(&self, index: u64) {
        todo!()
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
        todo!("apply command");
    }
}

pub type KvServer = Server<Kv>;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Kv {
    kv: HashMap<String, String>,
    // TODO: lab3 remove Vec wrapper
    seen: HashMap<ClientId, Vec<(SequenceNumber, Op, Reply)>>,
}

impl State for Kv {
    type Command = Op;
    type Output = String;

    fn apply(&mut self, cmd: Self::Command) -> Self::Output {
        todo!("apply command");
    }
}
