use std::str;
use std::io;
use std::cell::RefCell;
use std::rc::Rc;
use std::net::SocketAddr;
use std::sync::{Mutex, Arc};
use std::collections::{VecDeque, HashMap};
use database::{Database, Token};
use workers::{WorkerMsg, WorkerSender};
use futures::{self, Future};
use futures::stream::{self, Stream};
use my_futures;
use tokio_core as tokio;
use tokio_core::io::Io;
use resp::{self, RespValue};
use config::Config;
use utils::IdHashMap;

struct RespConnection {
    addr: SocketAddr,
    token: Token,
}

struct LocalContext {
    context: Rc<GlobalContext>,
    token: Token,
    requests: VecDeque<RespValue>,
    inflight: bool,
}

struct GlobalContext {
    database: Arc<Database>,
    db_sender: RefCell<WorkerSender>,
    token_chans: Arc<Mutex<IdHashMap<Token, tokio::channel::Sender<RespValue>>>>,
}

pub struct Server {
    config: Config,
}

impl LocalContext {
    fn dispatch(&mut self, req: RespValue) {
        if self.inflight {
            self.requests.push_back(req);
        } else {
            self.context.db_sender.borrow_mut().send(WorkerMsg::Command(self.token, req));
            self.inflight = true;
        }
    }

    fn dispatch_next(&mut self) {
        assert!(self.inflight, "can't cycle if there's nothing inflight");
        if let Some(req) = self.requests.pop_front() {
            self.context.db_sender.borrow_mut().send(WorkerMsg::Command(self.token, req));
        } else {
            self.inflight = false;
        }
    }
}

impl RespConnection {
    fn new(addr: SocketAddr, token: Token) -> Self {
        RespConnection {
            addr: addr,
            token: token,
        }
    }

    fn run(self, handle: tokio::reactor::Handle, socket: tokio::net::TcpStream,
           context: Rc<GlobalContext>)
           -> Box<Future<Item = (), Error = ()>> {
        debug!("run token {}", self.token);
        let (pipe_tx, pipe_rx) = tokio::channel::channel(&handle).unwrap();
        context.token_chans.lock().unwrap().insert(self.token, pipe_tx);
        let (sock_rx, sock_tx) = socket.split();
        let ctx_rx = Rc::new(RefCell::new(LocalContext {
            token: self.token,
            context: context,
            inflight: false,
            requests: VecDeque::new(),
        }));
        let ctx_tx = ctx_rx.clone();

        let read_fut = stream::iter((0..).map(|_| -> Result<(), io::Error> { Ok(()) }))
            .fold((ctx_rx, sock_rx, resp::ByteTendril::new(), 0), |(ctx, s, mut b, p), _| {
                if b.len() - p < 4 * 1024 {
                    unsafe {
                        b.push_uninitialized(4 * 1024);
                    }
                }

                my_futures::read_at(s, b, p).and_then(|(s, mut b, p, r)| {
                    debug!("read {} bytes at [{}..{}]", r, p, b.len());
                    let mut parser = resp::Parser::new(b.subtendril(0, (p + r) as u32));
                    loop {
                        match parser.parse() {
                            Ok(req) => {
                                debug!("Parsed request {:?}", req);
                                ctx.borrow_mut().dispatch(req);
                            }
                            Err(resp::RespError::Incomplete) => break,
                            Err(resp::RespError::Invalid(e)) => {
                                debug!("Parser error {:?}", e);
                                return Err(io::Error::new(io::ErrorKind::Other, e));
                            }
                        }
                    }
                    b.pop_front(parser.bytes_consumed() as u32);
                    Ok((ctx, s, b, p + r - parser.bytes_consumed()))
                })
            })
            .map(|_| ());

        let write_fut = pipe_rx.fold((ctx_tx, sock_tx, Vec::new()), |(ctx, s, mut b), resp| {
                ctx.borrow_mut().dispatch_next();
                b.clear();
                resp.serialize_to(&mut b);
                tokio::io::write_all(s, b).map(move |(s, b)| (ctx, s, b))
            })
            .map(|_| ());

        Box::new(read_fut.select(write_fut).then(move |_| {
            debug!("finished token {}", self.token);
            futures::finished::<(), ()>(())
        }))
    }
}

impl Server {
    pub fn new(config: Config) -> Server {
        Server { config: config }
    }

    pub fn run(self) {
        let mut core = tokio::reactor::Core::new().unwrap();
        let listener = tokio::net::TcpListener::bind(&self.config.listen_addr, &core.handle())
            .unwrap();

        let token_chans: Arc<Mutex<IdHashMap<Token, tokio::channel::Sender<RespValue>>>> =
            Default::default();
        let token_chans_cloned = token_chans.clone();
        let response_fn = Box::new(move |token, resp| {
            if let Some(chan) = token_chans_cloned.lock().unwrap().get(&token) {
                let _ = chan.send(resp);
            } else {
                debug!("Can't find response channel for token {:?}", token);
            }
        });

        let database = Database::new(&self.config, true, response_fn);

        let context = Rc::new(GlobalContext {
            db_sender: RefCell::new(database.sender()),
            database: database,
            token_chans: token_chans,
        });

        let mut next_token = 0;
        let handle = core.handle();
        let listener_fut = listener.incoming()
            .and_then(|(socket, addr)| {
                let conn = RespConnection::new(addr, next_token);
                let conn_handle = handle.clone();
                let conn_context = context.clone();
                handle.spawn_fn(move || conn.run(conn_handle, socket, conn_context));
                next_token = next_token.wrapping_add(1);
                Ok(())
            })
            .for_each(|_| Ok(()));

        core.run(listener_fut).unwrap();
    }
}
