use std::str;
use std::io;
use std::cell::RefCell;
use std::rc::Rc;
use std::net::SocketAddr;
use std::sync::{Mutex, Arc};
use std::collections::VecDeque;
use database::{Database, Token};
use workers::{WorkerMsg, WorkerSender};
use futures::{self, Future};
use futures::stream::{self, Stream};
use futures::sync::mpsc as fmpsc;
use extra_futures::read_at;
use tokio_core as tokio;
use tokio_io::{io as tokio_io, AsyncRead};
use resp::{self, RespValue};
use config::Config;
use metrics::{self, Gauge};
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
    token_chans: Arc<Mutex<IdHashMap<Token, fmpsc::UnboundedSender<RespValue>>>>,
}

pub struct Server {
    config: Config,
}

impl LocalContext {
    fn dispatch(&mut self, req: RespValue) {
        if self.inflight {
            debug!("Enqueued request ({}) {:?}", self.token, req);
            self.requests.push_back(req);
        } else {
            debug!("Dispatched request ({}) {:?}", self.token, req);
            self.context
                .db_sender
                .borrow_mut()
                .send(WorkerMsg::Command(self.token, req));
            self.inflight = true;
        }
    }

    fn dispatch_next(&mut self) {
        assert!(self.inflight, "can't cycle if there's nothing inflight");
        if let Some(req) = self.requests.pop_front() {
            debug!("Dispatched request ({}) {:?}", self.token, req);
            self.context
                .db_sender
                .borrow_mut()
                .send(WorkerMsg::Command(self.token, req));
        } else {
            self.inflight = false;
        }
    }
}

impl RespConnection {
    fn new(addr: SocketAddr, token: Token) -> Self {
        metrics::CLIENT_CONNECTION.inc();
        RespConnection {
            addr: addr,
            token: token,
        }
    }

    fn run(
        self, socket: tokio::net::TcpStream, context: Rc<GlobalContext>,
        _handle: tokio::reactor::Handle
    ) -> Box<Future<Item = (), Error = ()>> {
        debug!("run token {}", self.token);
        let _ = socket.set_nodelay(true);
        let (sock_rx, sock_tx) = socket.split();
        let (pipe_tx, pipe_rx) = fmpsc::unbounded();
        context.token_chans.lock().unwrap().insert(self.token, pipe_tx);
        let ctx_rx = Rc::new(
            RefCell::new(
                LocalContext {
                    token: self.token,
                    context: context,
                    inflight: false,
                    requests: VecDeque::new(),
                },
            ),
        );
        let ctx_tx = ctx_rx.clone();

        let read_fut = stream::iter((0..).map(|_| -> Result<(), io::Error> { Ok(()) }))
            .fold(
                (ctx_rx, sock_rx, Vec::new(), 0), |(ctx, s, mut b, p), _| {
                    if b.len() - p < 4 * 1024 {
                        unsafe {
                            b.reserve(16 * 1024);
                            let cap = b.capacity();
                            b.set_len(cap);
                        }
                    }

                    read_at(s, b, p).and_then(
                        |(s, b, p, r)| {
                            trace!("read {} bytes at [{}..{}]", r, p, b.len());
                            let end = p + r;
                            let mut parser = match resp::Parser::new(&b[..end]) {
                                Ok(parser) => parser,
                                Err(resp::RespError::Incomplete) => return Ok((ctx, s, b, end)),
                                Err(resp::RespError::Invalid(e)) => {
                                    return Err(io::Error::new(io::ErrorKind::Other, e))
                                }
                            };
                            loop {
                                match parser.parse() {
                                    Ok(req) => ctx.borrow_mut().dispatch(req),
                                    Err(resp::RespError::Incomplete) => break,
                                    Err(resp::RespError::Invalid(e)) => {
                                        return Err(io::Error::new(io::ErrorKind::Other, e))
                                    }
                                }
                            }
                            if end != parser.consumed() {
                                // TODO: this should be abstracted away
                                // copy remaining to start of buffer
                                unsafe {
                                    ::std::ptr::copy(
                                        b.as_ptr().offset(parser.consumed() as isize),
                                        b.as_ptr() as *mut _,
                                        end - parser.consumed(),
                                    );
                                }
                            }
                            Ok((ctx, s, b, end - parser.consumed()))
                        },
                    )
                }
            )
            .map(|_| ());

        let write_fut = pipe_rx
            .map_err(|_| io::ErrorKind::Other.into())
            .fold(
                (ctx_tx, sock_tx, Vec::new()), |(ctx, s, mut b), resp| {
                    ctx.borrow_mut().dispatch_next();
                    b.clear();
                    resp.serialize_to(&mut b);
                    tokio_io::write_all(s, b).map(move |(s, b)| (ctx, s, b))
                }
            )
            .map(|_| ());

        Box::new(
            read_fut
                .select(write_fut)
                .then(
                    move |_| {
                        debug!("finished token {}", self.token);
                        futures::finished::<(), ()>(())
                    },
                ),
        )
    }
}

impl Drop for RespConnection {
    fn drop(&mut self) {
        metrics::CLIENT_CONNECTION.dec();
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

        let token_chans: Arc<Mutex<IdHashMap<Token, fmpsc::UnboundedSender<_>>>> =
            Default::default();
        let token_chans_cloned = token_chans.clone();
        let response_fn = Box::new(
            move |token, resp| if let Some(chan) = token_chans_cloned
                   .lock()
                   .unwrap()
                   .get_mut(&token) {
                let _ = chan.send(resp);
            } else {
                debug!("Can't find response channel for token {:?}", token);
            },
        );

        let database = Database::new(&self.config, response_fn);

        let context = Rc::new(
            GlobalContext {
                db_sender: RefCell::new(database.sender()),
                database: database,
                token_chans: token_chans,
            },
        );

        let mut next_token = 0;
        let handle = core.handle();
        let listener_fut = listener
            .incoming()
            .and_then(
                |(socket, addr)| {
                    let conn = RespConnection::new(addr, next_token);
                    let conn_handle = handle.clone();
                    let conn_context = context.clone();
                    handle.spawn_fn(move || conn.run(socket, conn_context, conn_handle));
                    next_token = next_token.wrapping_add(1);
                    Ok(())
                },
            )
            .for_each(|_| Ok(()));

        core.run(listener_fut).unwrap();
    }
}
