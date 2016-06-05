use std::{fmt, thread, net};
use std::error::Error;
use std::sync::{Arc, RwLock, atomic};
use std::collections::HashMap;
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

pub type HandlerFn = Box<Fn(net::SocketAddr, FabricMsg) + Send + Sync>;

pub type FabricResult<T> = Result<T, GenericError>;

pub struct Fabric {
    loop_thread: Option<thread::JoinHandle<()>>,
    shared_context: Arc<SharedContext>,
}

struct SharedContext {
    nodes: RwLock<HashMap<net::SocketAddr, (BoundedQueue<FabricMsg>, Vec<rotor::Notifier>)>>,
    msg_handlers: RwLock<HashMap<u8, HandlerFn>>,
    connector_notifier: rotor::Notifier,
    connector_queue: BoundedQueue<net::SocketAddr>,
    running: atomic::AtomicBool,
}

struct Context {
    shared: Arc<SharedContext>,
}

enum OutMachine {
    Connector(net::SocketAddr, BoundedQueue<net::SocketAddr>),
    Connection(Stream<OutConnection>),
}

struct OutConnection {
    identified: bool,
    this: net::SocketAddr,
    other: net::SocketAddr,
    notifier: rotor::Notifier,
}

struct InConnection {
    this: net::SocketAddr,
    other: Option<net::SocketAddr>,
}

rotor_compose! {
    enum Fsm/Seed<Context> {
        Out(OutMachine),
        In(Accept<Stream<InConnection>, TcpListener>),
    }
}

impl Context {
    fn new(connector_notifier: rotor::Notifier, connector_queue: BoundedQueue<net::SocketAddr>)
           -> Self {
        Context { shared: Arc::new(SharedContext::new(connector_notifier, connector_queue)) }
    }
}

impl SharedContext {
    fn new(connector_notifier: rotor::Notifier, connector_queue: BoundedQueue<net::SocketAddr>)
           -> Self {
        SharedContext {
            nodes: RwLock::new(Default::default()),
            msg_handlers: RwLock::new(Default::default()),
            connector_notifier: connector_notifier,
            connector_queue: connector_queue,
            running: atomic::AtomicBool::new(false),
        }
    }
}

impl Machine for OutMachine {
    type Context = Context;
    type Seed = (net::SocketAddr, net::SocketAddr);

    fn create((this, other): Self::Seed, scope: &mut Scope<Self::Context>) -> Response<Self, Void> {
        let stream = TcpStream::connect(&other).unwrap();
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
                    scope.shutdown_loop();
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
    debug!("writing msg {:?} ({} bytes)", msg, msg_len);
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
                        debug!("read msg {:?} ({} bytes)", msg, msg_len);
                        de_msg = Some(msg);
                    }
                    Err(e) => error!("Error deserializing msg: {:?}", e),
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
        let nodes = scope.shared.nodes.read().unwrap();
        let node = nodes.get(&self.other).unwrap();
        if let Some(msg) = node.0.pop() {
            debug!("pull message from queue {:?}", msg);
            write_msg(transport, &msg);
            Intent::of(self).expect_flush()
        } else {
            debug!("no message, will sleep");
            Intent::of(self).sleep()
        }
    }

    fn cleanup(&self, scope: &mut Scope<Context>) {
        debug!("cleaning up");
        // TODO create another connection if queue still has items
        scope.shared.nodes.write().unwrap().get_mut(&self.other).map(|n| {
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
    type Seed = (net::SocketAddr, net::SocketAddr);

    fn create((this, other): Self::Seed, sock: &mut TcpStream, scope: &mut Scope<Context>)
              -> Intent<Self> {
        debug!("outgoing connection to {:?}", other);
        let _ = sock.set_nodelay(true);
        let _ = sock.set_keepalive(Some(1));
        let notifier = scope.notifier();
        let mut nodes = scope.shared.nodes.write().unwrap();
        nodes.get_mut(&other).unwrap().1.push(notifier.clone());
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
    type Seed = net::SocketAddr;

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
            let (addr_opt, needed) = read_msg::<net::SocketAddr>(transport);
            if let Some(addr) = addr_opt {
                self.other = Some(addr);
                debug!("identified connection from {:?}", addr);
            }
            return Intent::of(self).expect_bytes(needed);
        }

        loop {
            let (msg_opt, needed) = read_msg::<FabricMsg>(transport);
            if let Some(msg) = msg_opt {
                let msg_type = msg.get_type();
                if let Some(handler) = scope.shared
                    .msg_handlers
                    .read()
                    .unwrap()
                    .get(&(msg_type as u8)) {
                    handler(self.other.unwrap(), msg);
                } else {
                    warn!("No handler for msg type {:?}", msg_type);
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
    pub fn new(bind_addr: net::SocketAddr) -> FabricResult<Self> {
        let mut event_loop = try!(rotor::Loop::new(&rotor::Config::new()));
        let lst = try!(TcpListener::bind(&bind_addr));
        let connector_queue = BoundedQueue::with_capacity(128);
        let connector_notifier = event_loop.add_and_fetch(Fsm::Out, |scope| {
                let connector = OutMachine::Connector(bind_addr, connector_queue.clone());
                Response::ok((connector, scope.notifier()))
            })
            .unwrap();
        event_loop.add_machine_with(|scope| {
                match scope.register(&lst, EventSet::readable(), PollOpt::edge()) {
                    Ok(()) => {}
                    Err(e) => return Response::error(Box::new(e)),
                }
                Response::ok(Fsm::In(Accept::Server(lst, bind_addr)))
            })
            .unwrap();
        // create context and mark as running
        let context = Context::new(connector_notifier, connector_queue);
        let shared_context = context.shared.clone();
        shared_context.running.store(true, atomic::Ordering::Relaxed);
        // start event loop thread
        let thread = thread::spawn(move || event_loop.run(context).unwrap());
        Ok(Fabric {
            loop_thread: Some(thread),
            shared_context: shared_context,
        })
    }

    pub fn send_message(&self, recipient: &net::SocketAddr, msg: FabricMsg) -> FabricResult<()> {
        // fast path if connection already available
        if let Some(node) = self.shared_context.nodes.read().unwrap().get(recipient) {
            if let Some(n) = thread_rng().choose(&node.1) {
                node.0.push(msg).unwrap();
                n.wakeup().unwrap();
                return Ok(());
            };
        }
        let mut nodes = self.shared_context.nodes.write().unwrap();
        let node = nodes.entry(*recipient)
            .or_insert_with(|| (BoundedQueue::with_capacity(1024), Vec::new()));
        // push the message
        node.0.push(msg).unwrap();
        // we might have raced, so only create new conn if necessary
        if let Some(n) = thread_rng().choose(&node.1) {
            n.wakeup().unwrap()
        } else {
            self.shared_context.connector_queue.push(*recipient).unwrap();
            self.shared_context.connector_notifier.wakeup().unwrap();
        }

        Ok(())
    }

    pub fn register_msg_handler(&self, msg_type: FabricMsgType, handler: HandlerFn) {
        self.shared_context.msg_handlers.write().unwrap().insert(msg_type as u8, handler);
    }
}

impl Drop for Fabric {
    fn drop(&mut self) {
        self.shared_context.running.store(false, atomic::Ordering::Relaxed);
        self.shared_context.connector_notifier.wakeup().unwrap();
        self.loop_thread.take().map(|t| t.join());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread, net};
    use env_logger;

    #[test]
    fn test() {
        let _ = env_logger::init();
        let fabric = Fabric::new("127.0.0.1:6479".parse().unwrap()).unwrap();
        fabric.send_message(&"127.0.0.1:6479".parse().unwrap(), FabricMsg::Bootstrap).unwrap();
        thread::sleep_ms(3000);
        fabric.register_msg_handler(FabricMsgType::Bootstrap,
                                    Box::new(move |_, m| {
                                        info!("yay, handler received {:?}", m);
                                    }));
        fabric.send_message(&"127.0.0.1:6479".parse().unwrap(), FabricMsg::Bootstrap).unwrap();
        thread::sleep_ms(3000);
    }
}
