use std::{str, io, thread};
use std::net::SocketAddr;
use std::time::Duration;
use std::sync::{Mutex, Arc};
use std::collections::HashMap;
use std::collections::hash_map::Entry as HMEntry;

use rand::{thread_rng, Rng};
use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};
use bincode::{self, serde as bincode_serde};

use futures::{self, Future, IntoFuture};
use futures::stream::{self, Stream};
use my_futures::{read_at, SignaledChan, ShortCircuit};
use tokio_core as tokio;
use tokio_core::io::Io;

pub use fabric_msg::*;
use utils::{GenericError, IdHashMap};
use database::NodeId;

pub type FabricHandlerFn = Box<FnMut(NodeId, FabricMsg) + Send>;
pub type FabricResult<T> = Result<T, GenericError>;
const RECONNECT_INTERVAL_MS: u64 = 1000;

struct ReaderContext {
    context: Arc<GlobalContext>,
    peer: NodeId,
    peer_addr: SocketAddr,
}

struct WriterContext {
    context: Arc<GlobalContext>,
    peer: NodeId,
    peer_addr: SocketAddr,
    sender: tokio::channel::Sender<FabricMsg>,
}

struct GlobalContext {
    node: NodeId,
    addr: SocketAddr,
    loop_remote: tokio::reactor::Remote,
    msg_handlers: Mutex<IdHashMap<u8, FabricHandlerFn>>,
    nodes_addr: Mutex<IdHashMap<NodeId, SocketAddr>>,
    // FIXME: remove mutex or at least change to RwLock
    writer_chans: Arc<Mutex<IdHashMap<NodeId, Vec<tokio::channel::Sender<FabricMsg>>>>>,
}

pub struct Fabric {
    context: Arc<GlobalContext>,
    loop_thread: Option<(futures::Complete<()>, thread::JoinHandle<()>)>,
}

type InitType = Result<(Arc<GlobalContext>, futures::Complete<()>), io::Error>;

impl ReaderContext {
    fn dispatch(&self, msg: FabricMsg) {
        let msg_type = msg.get_type();
        if let Some(handler) = self.context
            .msg_handlers
            .lock()
            .unwrap()
            .get_mut(&(msg_type as u8)) {
            handler(self.peer, msg);
        } else {
            error!("No handler for msg type {:?}", msg_type);
        }
    }
}

impl WriterContext {
    fn add_writer_chan(&self, sender: tokio::channel::Sender<FabricMsg>) {
        let mut locked = self.context.writer_chans.lock().unwrap();
        locked.entry(self.peer).or_insert(Default::default()).push(sender);
    }

    fn remove_writer_chan(&self, sender: &tokio::channel::Sender<FabricMsg>) {
        let mut locked = self.context.writer_chans.lock().unwrap();
        if let HMEntry::Occupied(mut o) = locked.entry(self.peer) {
            o.get_mut().retain(|i| unsafe {
                use ::std::mem::transmute_copy;
                transmute_copy::<_, usize>(i) != transmute_copy::<_, usize>(sender)
            });
            if o.get().is_empty() {
                o.remove();
            }
        }
    }
}

impl Drop for WriterContext {
    fn drop(&mut self) {
        self.remove_writer_chan(&self.sender);
    }
}

impl Fabric {
    fn listen(listener: tokio::net::TcpListener, context: Arc<GlobalContext>,
              handle: tokio::reactor::Handle)
              -> Box<Future<Item = (), Error = ()>> {
        debug!("Starting fabric listener");
        let fut = listener.incoming()
            .and_then(move |(socket, addr)| {
                debug!("Accepting fabric connection from {:?}", addr);
                let handle_cloned = handle.clone();
                let context_cloned = context.clone();
                handle.spawn_fn(move || {
                    Self::connection(socket, None, addr, context_cloned, handle_cloned)
                        .map_err(|_| ())
                });
                Ok(())
            })
            .map_err(|_| ())
            .for_each(|_| Ok(()));
        Box::new(fut)
    }

    fn connect(node: NodeId, addr: SocketAddr, context: Arc<GlobalContext>,
               handle: tokio::reactor::Handle)
               -> Box<Future<Item = (), Error = ()>> {
        debug!("Opening fabric connection to node {:?}", addr);
        let context_cloned = context.clone();
        let handle_cloned = handle.clone();
        let fut = tokio::net::TcpStream::connect(&addr, &handle)
            .and_then(move |s| {
                debug!("Stablished fabric connection with {:?}", addr);
                Self::connection(s, Some(node), addr, context_cloned, handle_cloned)
            })
            .then(move |_| {
                tokio::reactor::Timeout::new(Duration::from_millis(RECONNECT_INTERVAL_MS), &handle)
                    .into_future()
                    .and_then(|t| t)
                    .and_then(move |_| {
                        let still_valid = {
                            let locked = context.nodes_addr.lock().unwrap();
                            locked.get(&node).map_or(false, |&a| a == addr)
                        };
                        if still_valid {
                            debug!("Reconnecting fabric connection to node {:?}", addr);
                            handle.spawn(Self::connect(node, addr, context, handle.clone()));
                        }
                        Ok(())
                    })
            })
            .map_err(|_| ());
        Box::new(fut)
    }

    fn connection(socket: tokio::net::TcpStream, expected_node: Option<NodeId>, addr: SocketAddr,
                  context: Arc<GlobalContext>, handle: tokio::reactor::Handle)
                  -> Box<Future<Item = (), Error = io::Error>> {
        let _ = socket.set_nodelay(true);
        let _ = socket.set_keepalive_ms(Some(2000));
        let mut buffer = [0u8; 8];
        (&mut buffer[..]).write_u64::<LittleEndian>(context.node).unwrap();
        let fut = tokio::io::write_all(socket, buffer)
            .and_then(|(s, b)| tokio::io::read_exact(s, b))
            .and_then(move |(s, b)| {
                let peer_id = (&b[..]).read_u64::<LittleEndian>().unwrap();
                if expected_node.unwrap_or(peer_id) == peer_id {
                    Ok((s, peer_id))
                } else {
                    Err(io::Error::new(io::ErrorKind::Other, "Unexpected NodeId"))
                }
            })
            .and_then(move |(s, peer_id)| {
                debug!("Identified fabric connection to node {:?}", peer_id);
                Self::steady_connection(s, peer_id, addr, context, handle)
            })
            .then(|r| {
                debug!("Fabric connection exit {:?}", r);
                r
            });

        Box::new(fut)
    }

    fn steady_connection(socket: tokio::net::TcpStream, peer: NodeId, peer_addr: SocketAddr,
                         context: Arc<GlobalContext>, handle: tokio::reactor::Handle)
                         -> Box<Future<Item = (), Error = io::Error>> {
        let (sock_rx, sock_tx) = socket.split();
        let (chan_tx, chan_rx) = tokio::channel::channel::<FabricMsg>(&handle).unwrap();
        let ctx_rx = ReaderContext {
            context: context.clone(),
            peer: peer,
            peer_addr: peer_addr,
        };
        let ctx_tx = WriterContext {
            context: context,
            peer: peer,
            peer_addr: peer_addr,
            sender: chan_tx.clone(),
        };
        ctx_tx.add_writer_chan(chan_tx);

        let rx_fut = stream::iter((0..).map(|_| -> Result<(), io::Error> { Ok(()) }))
            .fold((ctx_rx, sock_rx, Vec::<u8>::new(), 0), |(ctx, s, mut b, p), _| {
                if b.len() - p < 4 * 1024 {
                    unsafe {
                        b.reserve(4 * 1024);
                        let cap = b.capacity();
                        b.set_len(cap);
                    }
                }

                read_at(s, b, p).and_then(|(s, b, p, r)| {
                    let mut end = p + r;
                    let mut start = 0;
                    while end - start > 4 {
                        let mut slice = &b[start..end];
                        let msg_len = slice.read_u32::<LittleEndian>().unwrap() as usize;
                        if slice.len() < msg_len {
                            break;
                        }

                        let de_result =
                            bincode_serde::deserialize_from(&mut slice,
                                                            bincode::SizeLimit::Infinite);

                        match de_result {
                            Ok(msg) => {
                                debug!("Received from node {} msg {:?}", ctx.peer, msg);
                                ctx.dispatch(msg);
                                start += 4 + msg_len;
                            }
                            Err(e) => {
                                return Err(io::Error::new(io::ErrorKind::Other, e));
                            }
                        }
                    }

                    if start != 0 {
                        unsafe {
                            ::std::ptr::copy(b.as_ptr().offset(start as isize),
                                             b.as_ptr() as *mut _,
                                             end - start);
                        }
                        end -= start;
                    }
                    Ok((ctx, s, b, end))
                })
            })
            .map(|_| ());

        let tx_fut = SignaledChan::new(chan_rx)
            .fold((ctx_tx, sock_tx, Vec::new()), move |(ctx, s, mut b), msg_opt| {
                let flush = if let Some(msg) = msg_opt {
                    debug!("Sending to node {} msg {:?}", ctx.peer, msg);
                    let offset = b.len();
                    b.write_u32::<LittleEndian>(0).unwrap();
                    bincode_serde::serialize_into(&mut b, &msg, bincode::SizeLimit::Infinite)
                        .unwrap();
                    let msg_len = (b.len() - offset - 4) as u32;
                    (&mut b[offset..offset + 4]).write_u32::<LittleEndian>(msg_len).unwrap();
                    b.len() >= 4 * 1024
                } else {
                    true
                };
                if flush {
                    trace!("flushing msgs to node {:?}", ctx.peer);
                    ShortCircuit::from_future(tokio::io::write_all(s, b).map(|(s, mut b)| {
                        b.clear();
                        (ctx, s, b)
                    }))
                } else {
                    ShortCircuit::from_item((ctx, s, b))
                }
            })
            .map(|_| ());
        Box::new(rx_fut.select(tx_fut).map(|_| ()).map_err(|(e, _)| e))
    }

    fn run(node: NodeId, addr: SocketAddr, init_tx: ::std::sync::mpsc::Sender<InitType>) {
        let mut core = tokio::reactor::Core::new().unwrap();
        let handle = core.handle();
        let remote = core.remote();
        let context = Arc::new(GlobalContext {
            node: node,
            addr: addr,
            loop_remote: remote,
            nodes_addr: Default::default(),
            msg_handlers: Default::default(),
            writer_chans: Default::default(),
        });
        let init_result = tokio::net::TcpListener::bind(&context.addr, &handle)
            .map(|listener| {
                core.handle().spawn(Self::listen(listener, context.clone(), core.handle()))
            });

        match init_result {
            Ok(_) => {
                let (completer_tx, completer_rx) = futures::oneshot();
                let _ = init_tx.send(Ok((context, completer_tx)));
                core.run(completer_rx).unwrap();
            }
            Err(e) => {
                let _ = init_tx.send(Err(e));
            }
        }
    }

    pub fn new(node: NodeId, addr: SocketAddr) -> FabricResult<Self> {
        // start event loop thread
        let (init_tx, init_rx) = ::std::sync::mpsc::channel();
        let thread = thread::Builder::new()
            .name(format!("Fabric:{}", node))
            .spawn(move || Self::run(node, addr, init_tx))
            .unwrap();
        let (context, completer) = try!(try!(init_rx.recv()));
        Ok(Fabric {
            context: context,
            loop_thread: Some((completer, thread)),
        })
    }

    pub fn register_msg_handler(&self, msg_type: FabricMsgType, handler: FabricHandlerFn) {
        self.context.msg_handlers.lock().unwrap().insert(msg_type as u8, handler);
    }

    pub fn register_node(&self, node: NodeId, addr: SocketAddr) {
        if node == self.context.node {
            return;
        }
        let prev = self.context.nodes_addr.lock().unwrap().insert(node, addr);
        if prev.is_none() || prev.unwrap() != addr {
            self.start_connect(node, addr);
        }
    }

    fn start_connect(&self, node: NodeId, addr: SocketAddr) {
        let context = self.context.clone();
        {
            let mut writers = context.writer_chans.lock().unwrap();
            writers.entry(node).or_insert(Default::default());
        }
        let context_cloned = context.clone();
        context.loop_remote.spawn(move |h| Self::connect(node, addr, context_cloned, h.clone()));
        ::std::thread::sleep(::std::time::Duration::from_millis(500));
    }

    pub fn send_msg<T: Into<FabricMsg>>(&self, node: NodeId, msg: T) -> FabricResult<()> {
        if node == self.context.node {
            panic!("Can't send message to self");
        }
        let msg = msg.into();
        if cfg!(test) {
            let droppable = match msg.get_type() {
                FabricMsgType::Crud => false,
                _ => true,
            };
            if droppable {
                let fabric_drop = ::std::env::var("FABRIC_DROP")
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                if fabric_drop > 0.0 && thread_rng().gen::<f64>() < fabric_drop {
                    warn!("Fabric msg droped due to FABRIC_DROP: {:?}", msg);
                    return Ok(());
                }
            }
        }
        let mut writers = self.context.writer_chans.lock().unwrap();
        match writers.entry(node) {
            HMEntry::Occupied(o) => {
                let chans = o.get();
                match chans.len() {
                    0 => {
                        // FIXME: super broken
                        error!("DROPING MSG - No writers");
                    }
                    1 => {
                        let _ = chans[0].send(msg);
                    }
                    _ => {
                        let _ = thread_rng().choose(&chans).unwrap().send(msg);
                    }
                }
            }
            HMEntry::Vacant(_v) => {
                // FIXME: super broken
                error!("DROPING MSG - No Channels entry");
            }
        }
        Ok(())
    }
}

impl Drop for Fabric {
    fn drop(&mut self) {
        warn!("droping fabric");
        if let Some((c, t)) = self.loop_thread.take() {
            c.complete(());
            t.join().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread, net};
    use std::time::Duration;
    use env_logger;
    use std::sync::{Arc, atomic};

    #[test]
    fn test() {
        let _ = env_logger::init();
        let fabric1 = Fabric::new(1, "127.0.0.1:6481".parse().unwrap()).unwrap();
        let fabric2 = Fabric::new(2, "127.0.0.1:6482".parse().unwrap()).unwrap();
        fabric1.register_node(2, "127.0.0.1:6482".parse().unwrap());
        fabric2.register_node(1, "127.0.0.1:6481".parse().unwrap());
        thread::sleep(Duration::from_millis(10));

        let counter = Arc::new(atomic::AtomicUsize::new(0));
        let counter_ = counter.clone();
        fabric2.register_msg_handler(FabricMsgType::Crud,
                                     Box::new(move |_, m| {
                                         counter_.fetch_add(1, atomic::Ordering::Relaxed);
                                     }));
        for _ in 0..3 {
            fabric1.send_msg(2,
                          MsgRemoteSetAck {
                              cookie: Default::default(),
                              vnode: Default::default(),
                              result: Ok(()),
                          })
                .unwrap();
        }
        thread::sleep(Duration::from_millis(10));
        assert_eq!(counter.load(atomic::Ordering::Relaxed), 3);
    }
}
