use std::{str, io, thread};
use std::net::SocketAddr;
use std::time::Duration;
use std::sync::{Mutex, Arc};
use std::sync::atomic::{Ordering, AtomicUsize};
use std::collections::hash_map::Entry as HMEntry;

use rand::{thread_rng, Rng};
use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};
use bincode;

use futures;
use futures::future::{self, Future, IntoFuture, Either};
use futures::stream::{self, Stream};
use futures::sync::mpsc as fmpsc;
use extra_futures::{read_at, SignaledChan};
use tokio_core as tokio;
use tokio_io::{io as tokio_io, AsyncRead};

pub use fabric_msg::*;
use utils::{GenericError, IdHashMap};
use database::NodeId;

const RECONNECT_INTERVAL_MS: u64 = 1000;

pub type FabricHandlerFn = Box<FnMut(NodeId, FabricMsg) + Send>;
type SenderChan = fmpsc::UnboundedSender<FabricMsg>;
type InitType = Result<(Arc<GlobalContext>, futures::sync::oneshot::Sender<()>), io::Error>;

/// The messasing network that encompasses all nodes of the cluster
/// using the fabric you can send messages (best-effort delivery)
/// to any registered node.
/// Currently each node keeps a connection to every other node. Due to the
/// full-duplex nature of tcp this gives 2 pipes to each server, both are
/// used to make better use of the socket buffers (is this a good idea though?).
pub struct Fabric {
    context: Arc<GlobalContext>,
    loop_thread: Option<(futures::sync::oneshot::Sender<()>, thread::JoinHandle<()>)>,
}

#[derive(Debug)]
pub enum FabricError {
    NoRoute,
}

struct ReaderContext {
    context: Arc<GlobalContext>,
    peer: NodeId,
    peer_addr: SocketAddr,
}

struct WriterContext {
    context: Arc<GlobalContext>,
    peer: NodeId,
    peer_addr: SocketAddr,
    sender: SenderChan,
    sender_chan_id: usize,
}

struct GlobalContext {
    node: NodeId,
    addr: SocketAddr,
    loop_remote: tokio::reactor::Remote,
    msg_handlers: Mutex<IdHashMap<u8, FabricHandlerFn>>,
    nodes_addr: Mutex<IdHashMap<NodeId, SocketAddr>>,
    // FIXME: remove mutex or at least change to RwLock
    writer_chans: Arc<Mutex<IdHashMap<NodeId, Vec<(usize, SenderChan)>>>>,
    chan_id_gen: AtomicUsize,
}

impl GlobalContext {
    fn add_writer_chan(&self, peer: NodeId, sender: SenderChan) -> usize {
        let chan_id = self.chan_id_gen.fetch_add(1, Ordering::Relaxed);
        let mut locked = self.writer_chans.lock().unwrap();
        locked
            .entry(peer)
            .or_insert(Default::default())
            .push((chan_id, sender.clone()));
        chan_id
    }

    fn remove_writer_chan(&self, peer: NodeId, sender_chan_id: usize) {
        let mut locked = self.writer_chans.lock().unwrap();
        if let HMEntry::Occupied(mut o) = locked.entry(peer) {
            o.get_mut().retain(|&(i, _)| i != sender_chan_id);
            // cleanup entry if empty
            if o.get().is_empty() {
                o.remove();
            }
        } else {
            panic!("peer not found in writer_chans");
        }
    }
}

impl ReaderContext {
    fn new(context: Arc<GlobalContext>, peer: NodeId, peer_addr: SocketAddr) -> Self {
        ReaderContext {
            context: context,
            peer: peer,
            peer_addr: peer_addr,
        }
    }

    fn dispatch(&self, msg: FabricMsg) {
        let msg_type = msg.get_type();
        if let Some(handler) =
            self.context.msg_handlers.lock().unwrap().get_mut(&(msg_type as u8)) {
            handler(self.peer, msg);
        } else {
            panic!("No handler for msg type {:?}", msg_type);
        }
    }
}

impl WriterContext {
    fn new(context: Arc<GlobalContext>, peer: NodeId, peer_addr: SocketAddr, sender: SenderChan)
           -> Self {
        let chan_id = context.add_writer_chan(peer, sender.clone());
        WriterContext {
            context: context,
            peer: peer,
            peer_addr: peer_addr,
            sender_chan_id: chan_id,
            sender: sender,
        }
    }
}

impl Drop for WriterContext {
    fn drop(&mut self) {
        self.context.remove_writer_chan(self.peer, self.sender_chan_id);
    }
}

impl Fabric {
    fn listen(listener: tokio::net::TcpListener, context: Arc<GlobalContext>,
              handle: tokio::reactor::Handle)
              -> Box<Future<Item = (), Error = ()>> {
        debug!("Starting fabric listener");
        let fut = listener
            .incoming()
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
                        let addr_opt = {
                            let locked = context.nodes_addr.lock().unwrap();
                            locked.get(&node).cloned()
                        };
                        if let Some(addr) = addr_opt {
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
        let fut = tokio_io::write_all(socket, buffer)
            .and_then(|(s, b)| tokio_io::read_exact(s, b))
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
                         context: Arc<GlobalContext>, _handle: tokio::reactor::Handle)
                         -> Box<Future<Item = (), Error = io::Error>> {
        let (sock_rx, sock_tx) = socket.split();
        let (chan_tx, chan_rx) = fmpsc::unbounded();
        let ctx_rx = ReaderContext::new(context.clone(), peer, peer_addr);
        let ctx_tx = WriterContext::new(context, peer, peer_addr, chan_tx);

        let rx_fut = stream::iter((0..).map(|_| -> Result<(), io::Error> { Ok(()) }))
            .fold((ctx_rx, sock_rx, Vec::new(), 0), |(ctx, s, mut b, p), _| {
                if b.len() - p < 4 * 1024 {
                    unsafe {
                        b.reserve(16 * 1024);
                        let cap = b.capacity();
                        b.set_len(cap);
                    }
                }

                read_at(s, b, p).and_then(|(s, b, p, r)| {
                    let end = p + r;
                    let mut consumed = 0;
                    while end - consumed > 4 {
                        let mut slice = &b[consumed..end];
                        let msg_len = slice.read_u32::<LittleEndian>().unwrap() as usize;
                        // TODO: sanity check msg_len
                        if slice.len() < msg_len {
                            break;
                        }

                        let de_result = bincode::deserialize_from(&mut slice, bincode::Infinite);

                        match de_result {
                            Ok(msg) => {
                                debug!("Received from node {} msg {:?}", ctx.peer, msg);
                                ctx.dispatch(msg);
                                consumed += 4 + msg_len;
                            }
                            Err(e) => {
                                return Err(io::Error::new(io::ErrorKind::Other, e));
                            }
                        }
                    }

                    if end != consumed {
                        // TODO: this should be abstracted away
                        // copy remaining to start of buffer
                        unsafe {
                            ::std::ptr::copy(b.as_ptr().offset(consumed as isize),
                                             b.as_ptr() as *mut _,
                                             end - consumed);
                        }
                    }
                    Ok((ctx, s, b, end - consumed))
                })
            })
            .map(|_| ());

        let tx_fut = SignaledChan::new(chan_rx)
            .map_err(|_| io::ErrorKind::Other.into())
            .fold((ctx_tx, sock_tx, Vec::new()), move |(ctx, s, mut b), msg_opt| {
                let flush = if let Some(msg) = msg_opt {
                    debug!("Sending to node {} msg {:?}", ctx.peer, msg);
                    let offset = b.len();
                    b.write_u32::<LittleEndian>(0).unwrap();
                    bincode::serialize_into(&mut b, &msg, bincode::Infinite).unwrap();
                    let msg_len = (b.len() - offset - 4) as u32;
                    (&mut b[offset..offset + 4]).write_u32::<LittleEndian>(msg_len).unwrap();
                    b.len() >= 4 * 1024
                } else {
                    true
                };
                if flush {
                    trace!("flushing msgs to node {:?}", ctx.peer);
                    Either::A(tokio_io::write_all(s, b).map(|(s, mut b)| {
                        b.clear();
                        (ctx, s, b)
                    }))
                } else {
                    Either::B(future::ok((ctx, s, b)))
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
                                   chan_id_gen: Default::default(),
                               });
        let init_result = tokio::net::TcpListener::bind(&context.addr, &handle)
            .map(|listener| {
                     core.handle().spawn(Self::listen(listener, context.clone(), core.handle()))
                 });

        match init_result {
            Ok(_) => {
                let (completer_tx, completer_rx) = futures::sync::oneshot::channel();
                let _ = init_tx.send(Ok((context, completer_tx)));
                core.run(completer_rx).unwrap();
            }
            Err(e) => {
                let _ = init_tx.send(Err(e));
            }
        }
    }

    pub fn new(node: NodeId, addr: SocketAddr) -> Result<Self, GenericError> {
        // start event loop thread
        let (init_tx, init_rx) = ::std::sync::mpsc::channel();
        let thread = thread::Builder::new()
            .name(format!("Fabric:{}", node))
            .spawn(move || Self::run(node, addr, init_tx))
            .unwrap();
        let (context, completer) = init_rx.recv()??;
        Ok(Fabric {
               context: context,
               loop_thread: Some((completer, thread)),
           })
    }

    pub fn register_msg_handler(&self, msg_type: FabricMsgType, handler: FabricHandlerFn) {
        self.context.msg_handlers.lock().unwrap().insert(msg_type as u8, handler);
    }

    pub fn register_node(&self, node: NodeId, addr: SocketAddr) {
        let prev = self.context.nodes_addr.lock().unwrap().insert(node, addr);
        if prev.is_none() || prev.unwrap() != addr {
            self.start_connect(node, addr);
        }
    }

    pub fn remove_node(&self, node: NodeId) {
        self.context.nodes_addr.lock().unwrap().remove(&node);
    }

    pub fn set_nodes<I>(&self, it: I) where I: Iterator<Item = (NodeId, SocketAddr)> {
        let mut nodes = self.context.nodes_addr.lock().unwrap();
        let mut x_nodes = nodes.clone();
        for (node, addr) in it {
            if node != self.context.node {
                x_nodes.remove(&node);
                let prev = nodes.insert(node, addr);
                if prev.is_none() || prev.unwrap() != addr {
                    self.start_connect(node, addr);
                }
            }
        }
        for (node, _) in x_nodes {
            nodes.remove(&node);
        }
    }

    fn start_connect(&self, node: NodeId, addr: SocketAddr) {
        let context = self.context.clone();
        {
            let mut writers = context.writer_chans.lock().unwrap();
            writers.entry(node).or_insert(Default::default());
        }
        let context_cloned = context.clone();
        context
            .loop_remote
            .spawn(move |h| Self::connect(node, addr, context_cloned, h.clone()));
    }

    // TODO: take msgs as references and buffer serialized bytes instead
    pub fn send_msg<T: Into<FabricMsg>>(&self, node: NodeId, msg: T) -> Result<(), FabricError> {
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
            HMEntry::Occupied(mut o) => {
                if let Some(&mut (_, ref mut chan)) = thread_rng().choose_mut(o.get_mut()) {
                    let _ = chan.send(msg);
                    Ok(())
                } else {
                    warn!("DROPING MSG - No channel available for {:?}", node);
                    Err(FabricError::NoRoute)
                }
            }
            HMEntry::Vacant(_v) => {
                warn!("DROPING MSG - No entry for node {:?}", node);
                Err(FabricError::NoRoute)
            }
        }
    }
}

impl Drop for Fabric {
    fn drop(&mut self) {
        warn!("droping fabric");
        if let Some((c, t)) = self.loop_thread.take() {
            let _ = c.send(());
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
            fabric1
                .send_msg(2,
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
