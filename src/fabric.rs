use rotor::{self, Scope, Response, Void};
use rotor::mio::{EventSet, PollOpt};
use rotor::mio::tcp::{TcpListener, TcpStream};
use rotor_tools::uniform::{Uniform, Action};
use rotor_tools::loop_ext::LoopExt;
use rotor_stream::{Accept, Stream, Protocol, Intent, Transport, Exception};
use std::thread;
use std::net;
use std::fmt;
use std::error::Error;
use std::sync::{Arc, RwLock, mpsc};
use std::collections::HashMap;

pub enum FabricMsgType {
    Get,
    GetAck,
    Put,
    PutAck,
    PutRemote,
    PutRemoteAck,
    Bootstrap,
    BootstrapStream,
    BootstrapFin,
    SyncStart,
    SyncStream,
    SyncAck,
    SyncFin,
}

pub type CallbackFn = Box<Fn()>;

pub struct Fabric {
    loop_thread: thread::JoinHandle<()>,
    shared: Arc<SharedContext>,
}

struct SharedContext {
    nodes: RwLock<HashMap<net::SocketAddr, (mpsc::Sender<()>, rotor::Notifier)>>,
    msg_callbacks: RwLock<HashMap<u8, CallbackFn>>,
}

struct Context {
    shared: Arc<SharedContext>,
    nodes: HashMap<net::SocketAddr, mpsc::Receiver<()>>,
}

struct OutConnector;

struct OutConnection;

struct InConnection;

rotor_compose! {
    enum Fsm/Seed<Context> {
        Out(Stream<OutConnection>),
        In(Stream<InConnection>),
        OutConnector(Uniform<OutConnector>),
        InAcceptor(Accept<Stream<InConnection>, TcpListener>),
    }
}

type FabricResult<T> = Result<T, ()>;

enum Message {

}

impl Context {
    fn new() -> Self {
        unimplemented!()
    }
}

impl Action for OutConnector {
    type Context = Context;
    type Seed = ();

    fn create(_seed: Self::Seed, _scope: &mut Scope<Self::Context>) -> Response<Self, Void> {
        unreachable!()
    }

    fn action(self, _scope: &mut Scope<Self::Context>) -> Response<Self, Self::Seed> {
        unreachable!()
        // Response::spawn(Fsm::OutConnection)
    }
}

impl Protocol for InConnection {
    type Context = Context;
    type Socket = TcpStream;
    type Seed = ();

    fn create(_seed: (), _sock: &mut TcpStream, _scope: &mut Scope<Context>) -> Intent<Self> {
        debug!("incomming connection from {:?}",
               _sock.local_addr().unwrap());
        Intent::of(InConnection {}).expect_delimiter(b"\r\n", 1024)
    }

    fn bytes_read(self, transport: &mut Transport<TcpStream>, _end: usize,
                  _scope: &mut Scope<Context>)
                  -> Intent<Self> {
        let buf_len = transport.input().len();

        Intent::done()
    }

    fn bytes_flushed(self, _transport: &mut Transport<TcpStream>, _scope: &mut Scope<Context>)
                     -> Intent<Self> {
        debug!("bytes_flushed");
        Intent::done()
    }
    fn timeout(self, _transport: &mut Transport<TcpStream>, _scope: &mut Scope<Context>)
               -> Intent<Self> {
        debug!("Timeout happened");
        Intent::done()
    }

    fn wakeup(self, _transport: &mut Transport<TcpStream>, _scope: &mut Scope<Context>)
              -> Intent<Self> {
        unreachable!();
    }
    fn exception(self, _transport: &mut Transport<Self::Socket>, reason: Exception,
                 _scope: &mut Scope<Self::Context>)
                 -> Intent<Self> {
        debug!("Error: {}", reason);
        Intent::done()
    }
    fn fatal(self, reason: Exception, _scope: &mut Scope<Self::Context>) -> Option<Box<Error>> {
        debug!("Error: {}", reason);
        None
    }
}

impl Fabric {
    fn new() -> Fabric {
        unimplemented!()
    }

    fn run() {
        let mut event_loop = rotor::Loop::new(&rotor::Config::new()).unwrap();
        let lst = TcpListener::bind(&"127.0.0.1:6479".parse().unwrap()).unwrap();
        let oc_notifier = event_loop.add_and_fetch(Fsm::OutConnector, |scope| {
                let oc = Uniform(OutConnector);
                Response::ok((oc, scope.notifier()))
            })
            .unwrap();
        event_loop.add_machine_with(|scope| {
                match scope.register(&lst, EventSet::readable(), PollOpt::edge()) {
                    Ok(()) => {}
                    Err(e) => return Response::error(Box::new(e)),
                }
                Response::ok(Fsm::InAcceptor(Accept::Server(lst, ())))
            })
            .unwrap();
        event_loop.run(Context::new()).unwrap();
    }

    fn send_message(&self, recipient: net::SocketAddr, msg: Message) -> FabricResult<()> {
        unimplemented!()
    }

    fn register_msg_handler(&self, msg_type: FabricMsgType, callback: CallbackFn) {
        unimplemented!()
    }

    fn function_name(&self) {
        unimplemented!()
    }
}
