use std::{fmt, thread, net};
use std::error::Error;
use std::sync::{Arc, RwLock, Mutex, atomic};
use std::collections::HashMap;
use linear_map::LinearMap;
use rotor::{self, Machine, Scope, Response, Void};
use rotor::mio::{EventSet, PollOpt};
use rotor::mio::util::BoundedQueue;
use rotor::mio::tcp::{TcpListener, TcpStream};
use rotor_tools::loop_ext::LoopExt;
use rotor_stream::{Accept, Stream, Protocol, Intent, Transport, Exception};
use rand::{thread_rng, Rng};
use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};
use serde::{Serialize, Deserialize};
use bincode::{self, serde as bincode_serde};
pub use fabric_msg::*;
use utils::GenericError;
use database::NodeId;

pub type FabricHandlerFn = Box<FnMut(NodeId, FabricMsg) + Send>;

pub type FabricResult<T> = Result<T, GenericError>;

type FabricId = (NodeId, net::SocketAddr);

pub struct Fabric {
    loop_thread: Option<thread::JoinHandle<()>>,
    shared_context: Arc<SharedContext>,
}

struct SharedContext {
    node: NodeId,
    outgoing: RwLock<HashMap<NodeId, (BoundedQueue<FabricMsg>, Vec<rotor::Notifier>)>>,
    nodes_addr: Mutex<HashMap<NodeId, net::SocketAddr>>,
    msg_handlers: Mutex<LinearMap<u8, FabricHandlerFn>>,
    connector_notifier: rotor::Notifier,
    connector_queue: BoundedQueue<FabricId>,
    running: atomic::AtomicBool,
}

struct Context {
    shared: Arc<SharedContext>,
}

enum OutMachine {
    Connector(FabricId, BoundedQueue<FabricId>),
    Connection(Stream<OutConnection>),
}

struct OutConnection {
    identified: bool,
    this: FabricId,
    other: FabricId,
    notifier: rotor::Notifier,
}

struct InConnection {
    this: FabricId,
    other: Option<FabricId>,
}

rotor_compose! {
    enum Fsm/Seed<Context> {
        Out(OutMachine),
        In(Accept<Stream<InConnection>, TcpListener>),
    }
}

impl Context {
    fn new(node: NodeId, connector_notifier: rotor::Notifier,
           connector_queue: BoundedQueue<FabricId>)
           -> Self {
        Context { shared: Arc::new(SharedContext::new(node, connector_notifier, connector_queue)) }
    }
}

impl SharedContext {
    fn new(node: NodeId, connector_notifier: rotor::Notifier,
           connector_queue: BoundedQueue<FabricId>)
           -> Self {
        SharedContext {
            node: node,
            outgoing: Default::default(),
            nodes_addr: Default::default(),
            msg_handlers: Default::default(),
            connector_notifier: connector_notifier,
            connector_queue: connector_queue,
            running: atomic::AtomicBool::new(false),
        }
    }
}

impl Machine for OutMachine {
    type Context = Context;
    type Seed = (FabricId, FabricId);

    fn create((this, other): Self::Seed, scope: &mut Scope<Self::Context>) -> Response<Self, Void> {
        let stream = TcpStream::connect(&other.1).unwrap();
        Stream::new(stream, (this, other), scope).wrap(OutMachine::Connection)
    }

    fn ready(self, events: EventSet, scope: &mut Scope<Self::Context>) -> Response<Self, Self::Seed> {
        match self {
            OutMachine::Connector(..) => unreachable!(),
            OutMachine::Connection(m) => {
                m.ready(events, scope)
                    .map(OutMachine::Connection, |_| unreachable!())
            }
        }
    }

    fn spawned(self, _scope: &mut Scope<Self::Context>) -> Response<Self, Self::Seed> {
        match self {
            OutMachine::Connector(..) => Response::ok(self),
            OutMachine::Connection(..) => unreachable!(),
        }
    }

    fn timeout(self, scope: &mut Scope<Self::Context>) -> Response<Self, Self::Seed> {
        match self {
            OutMachine::Connector(..) => unreachable!(),
            OutMachine::Connection(m) => {
                m.timeout(scope).map(OutMachine::Connection, |_| unreachable!())
            }
        }
    }

    fn wakeup(self, scope: &mut Scope<Self::Context>) -> Response<Self, Self::Seed> {
        match self {
            OutMachine::Connector(this, q) => {
                if !scope.shared.running.load(atomic::Ordering::Relaxed) {
                    debug!("shuting down loop");
                    scope.shutdown_loop();
                    return Response::done();
                }
                let other = q.pop().unwrap();
                debug!("connector wake up with: {:?}", other);
                Response::spawn(OutMachine::Connector(this, q), (this, other))
            }
            OutMachine::Connection(m) => {
                m.wakeup(scope).map(OutMachine::Connection, |_| unreachable!())
            }
        }
    }
}

fn write_msg<T: Serialize + fmt::Debug>(transport: &mut Transport<TcpStream>, msg: &T) {
    let msg_len = bincode_serde::serialized_size(msg);
    trace!("writing msg {:?} ({} bytes)", msg, msg_len);
    transport.output().write_u32::<LittleEndian>(msg_len as u32).unwrap();
    bincode_serde::serialize_into(transport.output(), msg, bincode::SizeLimit::Infinite).unwrap();
}

fn read_msg<T: Deserialize + fmt::Debug>(transport: &mut Transport<TcpStream>) -> (Option<T>, usize) {
    let mut consumed = 0;
    let mut needed = 4;
    let mut de_msg = None;
    {
        let mut buf = &transport.input()[..];
        if let Ok(msg_len) = buf.read_u32::<LittleEndian>().map(|l| l as usize) {
            if buf.len() >= msg_len {
                let mut msg_buf = &buf[..msg_len];
                let de_result = bincode_serde::deserialize_from(&mut msg_buf,
                                                                bincode::SizeLimit::Infinite);
                match de_result {
                    Ok(msg) => {
                        trace!("read msg {:?} ({} bytes)", msg, msg_len);
                        de_msg = Some(msg);
                    }
                    Err(e) => {
                        panic!("Error deserializing msg: {:?} from: {:?}", e, &buf[..msg_len])
                    }
                }
                consumed += 4 + msg_len;
            } else {
                needed += msg_len;
            }
        }
    }
    transport.input().consume(consumed);
    (de_msg, needed)
}

impl OutConnection {
    fn pull_msgs(self, transport: &mut Transport<TcpStream>, scope: &mut Scope<Context>)
                 -> Intent<Self> {
        let outgoing = scope.shared.outgoing.read().unwrap();
        let node = outgoing.get(&self.other.0).unwrap();
        if let Some(msg) = node.0.pop() {
            debug!("sending to node {:?} msg {:?}", self.other.0, msg);
            write_msg(transport, &msg);
            Intent::of(self).expect_flush()
        } else {
            trace!("no message, will sleep");
            Intent::of(self).sleep()
        }
    }

    fn cleanup(&self, scope: &mut Scope<Context>) {
        debug!("cleaning up");
        // TODO create another connection if queue still has items
        scope.shared.outgoing.write().unwrap().get_mut(&self.other.0).map(|n| {
            n.1.retain(|a| unsafe {
                use std::mem;
                mem::transmute_copy::<_, (usize, usize)>(a) !=
                mem::transmute_copy::<_, (usize, usize)>(&self.notifier)
            });
        });
    }
}

impl Protocol for OutConnection {
    type Context = Context;
    type Socket = TcpStream;
    type Seed = (FabricId, FabricId);

    fn create((this, other): Self::Seed, sock: &mut TcpStream, scope: &mut Scope<Context>)
              -> Intent<Self> {
        debug!("outgoing connection to {:?}", other);
        let _ = sock.set_nodelay(true);
        let _ = sock.set_keepalive(Some(1));
        let notifier = scope.notifier();
        let mut outgoing = scope.shared.outgoing.write().unwrap();
        outgoing.get_mut(&other.0).unwrap().1.push(notifier.clone());
        Intent::of(OutConnection {
                identified: false,
                this: this,
                other: other,
                notifier: notifier,
            })
            .expect_flush()
    }

    fn bytes_read(self, _transport: &mut Transport<TcpStream>, _end: usize,
                  _scope: &mut Scope<Context>)
                  -> Intent<Self> {
        unreachable!();
    }

    fn bytes_flushed(mut self, transport: &mut Transport<TcpStream>, scope: &mut Scope<Context>)
                     -> Intent<Self> {
        if self.identified {
            self.pull_msgs(transport, scope)
        } else {
            write_msg(transport, &self.this);
            self.identified = true;
            Intent::of(self).expect_flush()
        }
    }

    fn timeout(self, _transport: &mut Transport<TcpStream>, scope: &mut Scope<Context>)
               -> Intent<Self> {
        debug!("Out: Timeout happened");
        self.cleanup(scope);
        Intent::done()
    }

    fn wakeup(self, transport: &mut Transport<TcpStream>, scope: &mut Scope<Context>) -> Intent<Self> {
        self.pull_msgs(transport, scope)
    }

    fn exception(self, _transport: &mut Transport<Self::Socket>, reason: Exception,
                 scope: &mut Scope<Self::Context>)
                 -> Intent<Self> {
        debug!("Out: Error: {}", reason);
        self.cleanup(scope);
        Intent::done()
    }

    fn fatal(self, reason: Exception, scope: &mut Scope<Self::Context>) -> Option<Box<Error>> {
        debug!("Out: Error: {}", reason);
        self.cleanup(scope);
        None
    }
}

impl Protocol for InConnection {
    type Context = Context;
    type Socket = TcpStream;
    type Seed = FabricId;

    fn create(this: Self::Seed, sock: &mut TcpStream, _scope: &mut Scope<Context>) -> Intent<Self> {
        debug!("incomming connection from {:?}", sock.peer_addr().unwrap());
        let _ = sock.set_nodelay(true);
        let _ = sock.set_keepalive(Some(1));
        Intent::of(InConnection {
                this: this,
                other: None,
            })
            .expect_bytes(4)
    }

    fn bytes_read(mut self, transport: &mut Transport<TcpStream>, _end: usize,
                  scope: &mut Scope<Context>)
                  -> Intent<Self> {
        // first message is always the node addr
        if self.other.is_none() {
            let (fabricid_opt, needed) = read_msg::<FabricId>(transport);
            if let Some(fabricid) = fabricid_opt {
                self.other = Some(fabricid);
                debug!("identified connection from {:?}", fabricid);
            }
            return Intent::of(self).expect_bytes(needed);
        }

        loop {
            let (msg_opt, needed) = read_msg::<FabricMsg>(transport);
            if let Some(msg) = msg_opt {
                debug!("received from node {:?} msg {:?}", self.other.unwrap().0, msg);
                let msg_type = msg.get_type();
                if let Some(handler) = scope.shared
                    .msg_handlers
                    .lock()
                    .unwrap()
                    .get_mut(&(msg_type as u8)) {
                    handler(self.other.unwrap().0, msg);
                } else {
                    error!("No handler for msg type {:?}", msg_type);
                }
            }

            if transport.input().len() < needed {
                return Intent::of(self).expect_bytes(needed);
            }
        }
    }

    fn bytes_flushed(self, _transport: &mut Transport<TcpStream>, _scope: &mut Scope<Context>)
                     -> Intent<Self> {
        unreachable!();
    }

    fn timeout(self, _transport: &mut Transport<TcpStream>, _scope: &mut Scope<Context>)
               -> Intent<Self> {
        debug!("In: Timeout happened");
        Intent::done()
    }

    fn wakeup(self, _transport: &mut Transport<TcpStream>, _scope: &mut Scope<Context>)
              -> Intent<Self> {
        unreachable!();
    }

    fn exception(self, _transport: &mut Transport<Self::Socket>, reason: Exception,
                 _scope: &mut Scope<Self::Context>)
                 -> Intent<Self> {
        debug!("In: Error: {}", reason);
        Intent::done()
    }

    fn fatal(self, reason: Exception, _scope: &mut Scope<Self::Context>) -> Option<Box<Error>> {
        debug!("In: Fatal: {}", reason);
        None
    }
}

impl Fabric {
    pub fn new(node: NodeId, bind_addr: net::SocketAddr) -> FabricResult<Self> {
        // rotor sorcery
        let mut event_loop = try!(rotor::Loop::new(&rotor::Config::new()));
        let lst = try!(TcpListener::bind(&bind_addr));
        let connector_queue = BoundedQueue::with_capacity(128);
        let connector_notifier = event_loop.add_and_fetch(Fsm::Out, |scope| {
                let connector = OutMachine::Connector((node, bind_addr), connector_queue.clone());
                Response::ok((connector, scope.notifier()))
            })
            .unwrap();
        event_loop.add_machine_with(|scope| {
                if let Err(e) = scope.register(&lst, EventSet::readable(), PollOpt::edge()) {
                    return Response::error(Box::new(e));
                }
                Response::ok(Fsm::In(Accept::Server(lst, (node, bind_addr))))
            })
            .unwrap();
        // create context and mark as running
        let context = Context::new(node, connector_notifier, connector_queue);
        let shared_context = context.shared.clone();
        shared_context.running.store(true, atomic::Ordering::Relaxed);
        // start event loop thread
        let thread = thread::spawn(move || {
            event_loop.run(context).unwrap();
        });
        Ok(Fabric {
            loop_thread: Some(thread),
            shared_context: shared_context,
        })
    }

    pub fn send_message<T: Into<FabricMsg>>(&self, recipient: NodeId, msg: T) -> FabricResult<()> {
        // fast path if connection already available
        if let Some(node) = self.shared_context.outgoing.read().unwrap().get(&recipient) {
            if let Some(n) = thread_rng().choose(&node.1) {
                node.0.push(msg.into()).unwrap();
                n.wakeup().unwrap();
                return Ok(());
            };
        }
        let addr = self.shared_context.nodes_addr.lock().unwrap().get(&recipient).cloned().unwrap();
        let mut outgoing = self.shared_context.outgoing.write().unwrap();
        let node = outgoing.entry(recipient)
            .or_insert_with(|| (BoundedQueue::with_capacity(1024), Vec::new()));
        // push the message
        node.0.push(msg.into()).unwrap();
        // FIXME: we might have raced, so only create new conn if necessary
        if let Some(n) = thread_rng().choose(&node.1) {
            n.wakeup().unwrap();
        } else {
            self.shared_context.connector_queue.push((recipient, addr)).unwrap();
            self.shared_context.connector_notifier.wakeup().unwrap();
        }

        Ok(())
    }

    pub fn register_msg_handler(&self, msg_type: FabricMsgType, handler: FabricHandlerFn) {
        self.shared_context.msg_handlers.lock().unwrap().insert(msg_type as u8, handler);
    }

    pub fn register_node(&self, node: NodeId, addr: net::SocketAddr) {
        self.shared_context.nodes_addr.lock().unwrap().insert(node, addr);
    }
}

impl Drop for Fabric {
    fn drop(&mut self) {
        warn!("droping fabric");
        // mark as not running and wakeup connector as it knows how to shutdown the loop
        self.shared_context.running.store(false, atomic::Ordering::Relaxed);
        self.shared_context.connector_notifier.wakeup().unwrap();
        // join the loop thread
        if let Some(t) = self.loop_thread.take() {
            t.join().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread, net};
    use env_logger;
    use std::sync::{Arc, atomic};

    #[test]
    fn test() {
        let _ = env_logger::init();
        let fabric = Fabric::new(0, "127.0.0.1:6479".parse().unwrap()).unwrap();
        fabric.register_node(0, "127.0.0.1:6479".parse().unwrap());
        let counter = Arc::new(atomic::AtomicUsize::new(0));
        let counter_ = counter.clone();
        fabric.register_msg_handler(FabricMsgType::Crud,
                                    Box::new(move |_, m| {
                                        counter_.fetch_add(1, atomic::Ordering::Relaxed);
                                    }));
        for _ in 0..3 {
            fabric.send_message(0,
                              MsgSetRemoteAck {
                                  cookie: Default::default(),
                                  vnode: Default::default(),
                                  result: Ok(()),
                              })
                .unwrap();
        }
        thread::sleep_ms(10);
        assert_eq!(counter.load(atomic::Ordering::Relaxed), 3);
    }
}
