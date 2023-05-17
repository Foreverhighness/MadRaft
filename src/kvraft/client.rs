use super::msg::*;
use madsim::{net, time::*};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc,
    },
};

pub struct Clerk {
    core: ClerkCore<Op, String>,
}

impl Clerk {
    pub fn new(servers: Vec<SocketAddr>) -> Clerk {
        Clerk {
            core: ClerkCore::new(servers),
        }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    pub async fn get(&self, key: String) -> String {
        self.core.call(Op::Get { key }).await
    }

    pub async fn put(&self, key: String, value: String) {
        self.core.call(Op::Put { key, value }).await;
    }

    pub async fn append(&self, key: String, value: String) {
        self.core.call(Op::Append { key, value }).await;
    }
}

pub struct ClerkCore<Req, Rsp> {
    servers: Vec<SocketAddr>,
    _mark: std::marker::PhantomData<(Req, Rsp)>,

    me: usize,
    seq: Arc<AtomicUsize>,
    leader: Arc<AtomicUsize>,
}

/// For debugging purposes, this function does not return a random value.
pub fn generate_client_id() -> usize {
    static COUNT: AtomicUsize = AtomicUsize::new(0);
    COUNT.fetch_add(1, Relaxed)
}

impl<Req, Rsp> ClerkCore<Req, Rsp>
where
    Req: net::Message + Clone,
    Rsp: net::Message,
{
    pub fn new(servers: Vec<SocketAddr>) -> Self {
        Self::with_state(
            servers,
            generate_client_id(),
            Arc::new(AtomicUsize::default()),
            Arc::new(AtomicUsize::default()),
        )
    }

    pub fn with_state(
        servers: Vec<SocketAddr>,
        me: usize,
        seq: Arc<AtomicUsize>,
        leader: Arc<AtomicUsize>,
    ) -> Self {
        ClerkCore {
            servers,
            _mark: std::marker::PhantomData,
            me,
            seq,
            leader,
        }
    }

    pub async fn call(&self, args: Req) -> Rsp {
        let net = net::NetLocalHandle::current();

        // found leader
        let mut leader = self.leader.load(Relaxed);
        let len = self.servers.len();

        // prepare args
        let me = self.me;
        let seq = self.seq.load(Relaxed);
        trace!("CLIENT C{me} call ({seq}) args {args:?}");

        loop {
            match net
                .call_timeout::<((usize, usize), Req), Result<Rsp, Error>>(
                    *self.servers.get(leader).expect("{i} out of bound"),
                    ((me, seq), args.clone()),
                    Duration::from_millis(500),
                )
                .await
            {
                Ok(Ok(reply)) => {
                    trace!("CLIENT C{me} get ({seq}) reply {reply:?}");
                    self.seq.fetch_add(1, Relaxed);
                    self.leader.store(leader, Relaxed);
                    return reply;
                }
                Ok(Err(e)) => {
                    trace!("CLIENT C{me} get ({seq}) error {e:?}");
                    if let Error::NotLeader { hint } = e {
                        leader = hint;
                        continue;
                    }
                }
                Err(e) => trace!("CLIENT C{me} get ({seq}) error {e:?}"),
            }
            leader = (leader + 1) % len;
        }
    }
}
