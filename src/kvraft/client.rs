use super::msg::{ClientId, Error, Op, OpId, Reply};
use madsim::{net, time::Duration};
use std::{
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering::Relaxed},
};

pub struct Clerk {
    core: ClerkCore<Op, Reply>,

    seq: AtomicUsize,
}

impl Clerk {
    pub fn new(servers: Vec<SocketAddr>) -> Clerk {
        Clerk {
            core: ClerkCore::new(servers),
            seq: AtomicUsize::default(),
        }
    }

    fn op_id(&self) -> OpId {
        OpId {
            client_id: self.core.me,
            seq: self.seq.load(Relaxed),
        }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    pub async fn get(&self, key: String) -> Reply {
        self.core.call(Op::Get { key }).await
    }

    pub async fn put(&self, key: String, value: String) {
        let id = self.op_id();
        let ret = self.core.call(Op::Put { key, value, id }).await;
        assert_eq!(ret, String::default());
        self.seq.fetch_add(1, Relaxed);
    }

    pub async fn append(&self, key: String, value: String) {
        let id = self.op_id();
        let ret = self.core.call(Op::Append { key, value, id }).await;
        assert_eq!(ret, String::default());
        self.seq.fetch_add(1, Relaxed);
    }
}

pub struct ClerkCore<Req, Rsp> {
    servers: Vec<SocketAddr>,
    _mark: std::marker::PhantomData<(Req, Rsp)>,

    leader: AtomicUsize,
    pub me: ClientId,
}

pub struct ClerkCoreRef<'a, Req, Rsp> {
    servers: &'a [SocketAddr],
    leader: &'a AtomicUsize,
    me: ClientId,

    _mark: std::marker::PhantomData<(Req, Rsp)>,
}

/// For debugging purposes, this function does not return a random value.
pub fn generate_client_id() -> ClientId {
    static COUNT: AtomicUsize = AtomicUsize::new(0);
    COUNT.fetch_add(1, Relaxed)
}

impl<Req, Rsp> ClerkCore<Req, Rsp>
where
    Req: net::Message + Clone,
    Rsp: net::Message,
{
    pub fn new(servers: Vec<SocketAddr>) -> Self {
        ClerkCore {
            servers,
            _mark: std::marker::PhantomData,
            leader: AtomicUsize::default(),
            me: generate_client_id(),
        }
    }

    fn as_ref(&self) -> ClerkCoreRef<'_, Req, Rsp> {
        ClerkCoreRef::new(&self.servers, &self.leader, self.me)
    }

    pub async fn call(&self, args: Req) -> Rsp {
        self.as_ref().call(args).await
    }
}

impl<'a, Req, Rsp> ClerkCoreRef<'a, Req, Rsp>
where
    Req: net::Message + Clone,
    Rsp: net::Message,
{
    pub const fn new(servers: &'a [SocketAddr], leader: &'a AtomicUsize, me: ClientId) -> Self {
        ClerkCoreRef {
            servers,
            _mark: std::marker::PhantomData,
            leader,
            me,
        }
    }

    pub async fn call(&self, args: Req) -> Rsp {
        // TODO: lab3 remove limit
        const LIMIT: usize = 100;

        let net = net::NetLocalHandle::current();
        let me = self.me;
        trace!("CLIENT C{me} call args {args:?}");

        // found leader
        let mut leader = self.leader.load(Relaxed);
        let len = self.servers.len();

        // loop {
        for _ in 0..LIMIT {
            match net
                .call_timeout::<Req, Result<Rsp, Error>>(
                    *self.servers.get(leader).expect("{i} out of bound"),
                    args.clone(),
                    Duration::from_millis(500),
                )
                .await
            {
                Ok(Ok(reply)) => {
                    trace!("CLIENT C{me} get reply from S{leader} {reply:?}");
                    self.leader.store(leader, Relaxed);
                    return reply;
                }
                Ok(Err(e)) => {
                    trace!("CLIENT C{me} get error from S{leader} {e:?}");
                    if let Error::NotLeader { hint } = e {
                        leader = hint;
                        continue;
                    }
                }
                Err(e) => trace!("CLIENT C{me} get error from S{leader} {e:?}"),
            }
            leader = (leader + 1) % len;
        }
        unreachable!();
    }
}
