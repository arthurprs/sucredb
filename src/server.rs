use std::str;
use std::io;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Mutex, Arc};
use std::collections::VecDeque;

use database::{Database, Token};
use workers::{WorkerMsg, WorkerSender};
use futures::{Future, Stream, Sink};
use futures::sync::mpsc as fmpsc;
use tokio_core as tokio;
use tokio_io::{codec, AsyncRead};
use bytes::{BufMut, BytesMut};

use resp::{self, RespValue};
use config::Config;
use metrics::{self, Gauge};
use utils::IdHashMap;

struct RespCodec;

impl codec::Decoder for RespCodec {
    type Item = RespValue;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        let (consumed, result) = resp::Parser::new(&*src)
            .and_then(|mut p| match p.parse() {
                Ok(v) => Ok((p.consumed(), Ok(Some(v)))),
                Err(e) => Err(e),
            })
            .unwrap_or_else(|e| match e {
                resp::RespError::Incomplete => (0, Ok(None)),
                _ => (0, Err(io::ErrorKind::InvalidData.into())),
            });
        src.split_to(consumed);
        result
    }
}

impl codec::Encoder for RespCodec {
    type Item = RespValue;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> io::Result<()> {
        dst.reserve(item.serialized_size());
        item.serialize_into(&mut dst.writer()).expect(
            "Failed to serialize into reserved space",
        );
        Ok(())
    }
}

struct Context {
    context: Rc<SharedContext>,
    token: Token,
    requests: VecDeque<RespValue>,
    inflight: bool,
}

struct SharedContext {
    database: Arc<Database>,
    db_sender: RefCell<WorkerSender>,
    token_chans: Arc<Mutex<IdHashMap<Token, fmpsc::UnboundedSender<RespValue>>>>,
}

pub struct Server {
    config: Config,
}

impl Context {
    fn new(
        context: Rc<SharedContext>,
        token: Token,
        chan_tx: fmpsc::UnboundedSender<RespValue>,
    ) -> Self {
        metrics::CLIENT_CONNECTION.inc();
        context.token_chans.lock().unwrap().insert(token, chan_tx);
        Context {
            context: context,
            token: token,
            inflight: false,
            requests: VecDeque::new(),
        }
    }

    fn dispatch(&mut self, req: RespValue) {
        if self.inflight {
            debug!("Enqueued request ({}) {:?}", self.token, req);
            self.requests.push_back(req);
        } else {
            debug!("Dispatched request ({}) {:?}", self.token, req);
            self.context.db_sender.borrow_mut().send(
                WorkerMsg::Command(
                    self.token,
                    req,
                ),
            );
            self.inflight = true;
        }
    }

    fn dispatch_next(&mut self) {
        assert!(self.inflight, "can't cycle if there's nothing inflight");
        if let Some(req) = self.requests.pop_front() {
            debug!("Dispatched request ({}) {:?}", self.token, req);
            self.context.db_sender.borrow_mut().send(
                WorkerMsg::Command(
                    self.token,
                    req,
                ),
            );
        } else {
            self.inflight = false;
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        self.context.token_chans.lock().unwrap().remove(&self.token);
        metrics::CLIENT_CONNECTION.dec();
    }
}

impl Server {
    pub fn new(config: Config) -> Server {
        Server { config: config }
    }

    fn connection(
        context: Rc<SharedContext>,
        token: Token,
        socket: tokio::net::TcpStream,
    ) -> Box<Future<Item = (), Error = io::Error>> {
        socket.set_nodelay(true).expect("Failed to set nodelay");
        let (sock_rx, sock_tx) = socket.split();
        let sock_tx = codec::FramedWrite::new(sock_tx, RespCodec);
        let sock_rx = codec::FramedRead::new(sock_rx, RespCodec);
        let (chan_tx, chan_rx) = fmpsc::unbounded();
        let ctx_rx = Rc::new(RefCell::new(Context::new(context, token, chan_tx)));
        let ctx_tx = ctx_rx.clone();

        let fut_rx = sock_rx.for_each(move |request| {
            ctx_rx.borrow_mut().dispatch(request);
            Ok(())
        });

        let fut_tx = sock_tx
            .send_all(
                chan_rx
                    .map(move |response| {
                        ctx_tx.borrow_mut().dispatch_next();
                        response
                    })
                    .map_err(|_| io::Error::from(io::ErrorKind::Other)),
            )
            .map(|_| ());

        Box::new(fut_rx.select(fut_tx).map(|_| ()).map_err(|(e, _)| e))
    }

    pub fn run(self) {
        let mut core = tokio::reactor::Core::new().unwrap();
        let listener = tokio::net::TcpListener::bind(&self.config.listen_addr, &core.handle())
            .unwrap();

        let token_chans: Arc<Mutex<IdHashMap<Token, fmpsc::UnboundedSender<_>>>> =
            Default::default();
        let token_chans_cloned = token_chans.clone();
        let response_fn = Box::new(move |token, resp| if let Some(chan) =
            token_chans_cloned.lock().unwrap().get_mut(&token)
        {
            if let Err(e) = chan.unbounded_send(resp) {
                warn!("Can't send to token {} chan: {:?}", token, e);
            }
        } else {
            debug!("Can't find response channel for token {:?}", token);
        });

        let database = Database::new(&self.config, response_fn);

        let context = Rc::new(SharedContext {
            db_sender: RefCell::new(database.sender()),
            database: database,
            token_chans: token_chans,
        });

        let mut next_token = 0;
        let handle = core.handle();
        let listener_fut = listener.incoming().for_each(|(socket, addr)| {
            if context.token_chans.lock().unwrap().len() >=
                context.database.config.client_connection_max as usize
            {
                info!(
                    "Refusing connection from {:?}, connection limit reached",
                    addr
                );
                return Ok(());
            }
            info!("Token {} accepting connection from {:?}", next_token, addr);
            let conn_ctx = context.clone();
            handle.spawn(Self::connection(conn_ctx, next_token, socket).then(
                move |r| {
                    info!("Token {} disconnected {:?}", next_token, r);
                    Ok(())
                },
            ));
            next_token = next_token.wrapping_add(1);
            Ok(())
        });

        core.run(listener_fut).unwrap();
    }
}
