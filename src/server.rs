use std::str;
use std::error::Error;
use std::sync::{RwLock, Arc};
use std::collections::{VecDeque, HashMap};
use database::{Database, Token};
use workers::{WorkerMsg, WorkerSender};
use rotor::{self, Scope};
use rotor::mio::util::BoundedQueue;
use rotor::mio::tcp::{TcpListener, TcpStream};
use rotor_stream::{Accept, Stream, Protocol, Intent, Transport, Exception};
use resp::{self, RespValue};
use utils;

struct RespConnection {
    token: Token,
    inflight: bool,
    requests: VecDeque<RespValue>,
}

struct SharedContext {
    queues: RwLock<HashMap<Token, (BoundedQueue<RespValue>, rotor::Notifier)>>,
}

struct Context {
    next_token: Token,
    database: Arc<Database>,
    sender: WorkerSender,
    shared: Arc<SharedContext>,
}

pub struct Server;

impl Context {
    fn gen_token(&mut self) -> Token {
        let token = self.next_token;
        self.next_token = self.next_token.wrapping_add(1);
        token
    }
}

impl RespConnection {
    fn cleanup(&self, scope: &mut Scope<Context>) {
        debug!("cleaning up");
        scope.shared.queues.write().unwrap().remove(&self.token).unwrap();
    }

    fn dispatch(&self, scope: &mut Scope<Context>, req: RespValue) {
        // scope.shared.database.
    }
}

impl Protocol for RespConnection {
    type Context = Context;
    type Socket = TcpStream;
    type Seed = ();

    fn create(_seed: (), _sock: &mut TcpStream, scope: &mut Scope<Context>) -> Intent<Self> {
        debug!("incomming connection from {:?}", _sock.local_addr().unwrap());
        let token = scope.gen_token();
        let notifier = scope.notifier();
        let r = scope.shared
            .queues
            .write()
            .unwrap()
            .insert(token, (BoundedQueue::with_capacity(1), notifier));
        assert!(r.is_none());
        let con = RespConnection {
            token: token,
            inflight: true,
            requests: Default::default(),
        };
        Intent::of(con).expect_delimiter(b"\r\n", 1024)
    }

    fn bytes_read(mut self, transport: &mut Transport<TcpStream>, _end: usize,
                  scope: &mut Scope<Context>)
                  -> Intent<Self> {
        let mut parser = resp::Parser::new(&transport.input()[..]);
        match parser.parse() {
            Ok(req) => {
                transport.input().consume(parser.bytes_consumed());
                // parse it and send to correct worker thread?
                debug!("received request {:?}", req);
                if self.inflight {
                    self.requests.push_back(req);
                } else {
                    self.inflight = true;
                    self.dispatch(scope, req);
                }
                Intent::of(self).expect_delimiter(b"\r\n", 1024)
            }
            Err(resp::RespError::Incomplete) => {
                let buf_len = transport.input().len();
                Intent::of(self).expect_delimiter_after(buf_len - 1, b"\r\n", 64 * 1024)
            }
            Err(resp::RespError::Invalid(err_str)) => {
                warn!("protocol error {}", err_str);
                self.cleanup(scope);
                Intent::done()
            }
        }
    }

    fn bytes_flushed(self, _transport: &mut Transport<TcpStream>, _scope: &mut Scope<Context>)
                     -> Intent<Self> {
        debug!("bytes_flushed");
        Intent::done()
    }

    fn timeout(self, _transport: &mut Transport<TcpStream>, scope: &mut Scope<Context>)
               -> Intent<Self> {
        debug!("Timeout happened");
        self.cleanup(scope);
        Intent::done()
    }

    fn wakeup(mut self, transport: &mut Transport<TcpStream>, scope: &mut Scope<Context>) -> Intent<Self> {
        debug_assert!(self.inflight);
        let rv = scope.shared
            .queues
            .read()
            .unwrap()
            .get(&self.token)
            .unwrap()
            .0
            .pop()
            .unwrap();
        rv.serialize_to(transport.output());
        // maybe dispatch next
        if let Some(req) = self.requests.pop_back() {
            self.dispatch(scope, req);
        } else {
            self.inflight = false;
        }
        let buf_len = transport.input().len();
        Intent::of(self).expect_delimiter_after(buf_len - 1, b"\r\n", 64 * 1024)
    }

    fn exception(self, _transport: &mut Transport<Self::Socket>, reason: Exception,
                 scope: &mut Scope<Self::Context>)
                 -> Intent<Self> {
        error!("exception: {}", reason);
        self.cleanup(scope);
        Intent::done()
    }

    fn fatal(self, reason: Exception, scope: &mut Scope<Self::Context>) -> Option<Box<Error>> {
        error!("fatal: {}", reason);
        self.cleanup(scope);
        None
    }
}

impl Server {
    pub fn new() -> Server {
        Server {}
    }

    pub fn run(&self) {
        let mut event_loop = rotor::Loop::new(&rotor::Config::new()).unwrap();
        let lst = TcpListener::bind(&"127.0.0.1:6379".parse().unwrap()).unwrap();
        event_loop
            .add_machine_with(|scope| Accept::<Stream<RespConnection>, _>::new(lst, (), scope))
            .unwrap();

        let shared = Arc::new(SharedContext { queues: Default::default() });
        let shared_cloned = shared.clone();

        let response_fn = Box::new(move |t, rv| {
            if let Some(queue) = shared.queues.read().unwrap().get(&t) {
                queue.0.push(rv).unwrap();
                queue.1.wakeup().unwrap();
            } else {
                debug!("couldn't find token {} to write response", t);
            }
        });

        let database = Database::new(utils::get_or_gen_node_id(),
                                     "127.0.0.1:6379".parse().unwrap(),
                                     "./run/",
                                     true,
                                     response_fn);

        event_loop.run(Context {
                next_token: 0,
                sender: database.sender(),
                database: database,
                shared: shared_cloned,
            })
            .unwrap();
    }
}
