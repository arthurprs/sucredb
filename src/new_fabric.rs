use std::{str, io, net, thread};
use std::sync::{Mutex, Arc};
use std::collections::HashMap;
use std::collections::hash_map::Entry as HMEntry;

use rand::{thread_rng, Rng};
use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};
use bincode::{self, serde as bincode_serde};

use futures::{self, Future, IntoFuture};
use futures::stream::{self, Stream};
use my_futures;
use tokio_core as tokio;
use tokio_core::io::Io;

pub use fabric_msg::*;
use utils::GenericError;
use database::NodeId;

pub type FabricHandlerFn = Box<FnMut(NodeId, FabricMsg) + Send>;
pub type FabricResult<T> = Result<T, GenericError>;
type FabricId = (NodeId, net::SocketAddr);

struct ReaderContext {
    context: Arc<GlobalContext>,
    peer: FabricId,
}

impl ReaderContext {
    fn dispatch(&self, msg: FabricMsg) {
        let msg_type = msg.get_type();
        if let Some(handler) = self.context
            .msg_handlers
            .lock()
            .unwrap()
            .get_mut(&(msg_type as u8)) {
            handler(self.peer.0, msg);
        } else {
            error!("No handler for msg type {:?}", msg_type);
        }
    }
}

struct WriterContext {
    context: Arc<GlobalContext>,
    peer: FabricId,
}

struct GlobalContext {
    node: FabricId,
    msg_handlers: Mutex<HashMap<u8, FabricHandlerFn>>,
    nodes_addr: Mutex<HashMap<NodeId, net::SocketAddr>>,
    writer_chans: Arc<Mutex<HashMap<NodeId, Vec<tokio::channel::Sender<FabricMsg>>>>>,
}

pub struct Fabric {
    context: Arc<GlobalContext>,
    loop_remote: tokio::reactor::Remote,
    loop_thread: Option<(thread::JoinHandle<()>, futures::Complete<()>)>,
}

type InitType = Result<(tokio::reactor::Remote, futures::Complete<()>), io::Error>;

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
                    Self::connection(socket, None, context_cloned, handle_cloned).map_err(|_| ())
                });
                Ok(())
            })
            .map_err(|_| ())
            .for_each(|_| Ok(()));
        Box::new(fut)
    }

    fn connect(node: NodeId, addr: net::SocketAddr, context: Arc<GlobalContext>,
               handle: tokio::reactor::Handle)
               -> Box<Future<Item = (), Error = ()>> {
        debug!("Opening fabric connecting to node {:?}", addr);
        let fut = tokio::net::TcpStream::connect(&addr, &handle)
            .and_then(move |s| {
                debug!("Stablished fabric connection with {:?}", addr);
                Self::connection(s, Some(node), context, handle)
            })
            .map_err(|_| ());
        Box::new(fut)
    }

    fn connection(socket: tokio::net::TcpStream, expected_node: Option<NodeId>,
                  context: Arc<GlobalContext>, handle: tokio::reactor::Handle)
                  -> Box<Future<Item = (), Error = io::Error>> {
        let _ = socket.set_nodelay(true);
        let _ = socket.set_keepalive_ms(Some(2000));
        let mut buffer = [0u8; 8];
        (&mut &mut buffer[..]).write_u64::<LittleEndian>(context.node.0).unwrap();
        let fut = tokio::io::write_all(socket, buffer)
            .and_then(|(s, buffer)| tokio::io::read_exact(s, buffer))
            .and_then(move |(s, buffer)| {
                let peer_id = (&buffer[..]).read_u64::<LittleEndian>().unwrap();
                if expected_node.unwrap_or(peer_id) == peer_id {
                    Ok((s, peer_id))
                } else {
                    Err(io::Error::new(io::ErrorKind::Other, "Unexpected NodeId"))
                }
            })
            .and_then(move |(s, peer_id)| {
                debug!("Identified fabric connection to node {:?}", peer_id);
                Self::steady_connection(s, peer_id, context, handle)
            })
            .then(|r| {
                debug!("Fabric connection exit {:?}", r);
                r
            });

        Box::new(fut)
    }

    fn steady_connection(socket: tokio::net::TcpStream, peer_id: NodeId,
                         context: Arc<GlobalContext>, handle: tokio::reactor::Handle)
                         -> Box<Future<Item = (), Error = io::Error>> {
        let ctx_rx = ReaderContext {
            context: context.clone(),
            peer: (peer_id, socket.peer_addr().unwrap()),
        };
        let ctx_tx = WriterContext {
            context: context.clone(),
            peer: (peer_id, socket.peer_addr().unwrap()),
        };
        let (pipe_tx, pipe_rx) = tokio::channel::channel::<FabricMsg>(&handle).unwrap();
        context.writer_chans
            .lock()
            .unwrap()
            .entry(peer_id)
            .or_insert_with(|| Default::default())
            .push(pipe_tx);

        let (sock_rx, sock_tx) = socket.split();
        let rx_fut = stream::iter((0..).map(|_| -> Result<(), io::Error> { Ok(()) }))
            .fold((ctx_rx, sock_rx, Vec::<u8>::new(), 0), |(ctx, s, mut b, p), _| {
                if b.len() - p < 4 * 1024 {
                    unsafe {
                        b.reserve(4 * 1024);
                        let cap = b.capacity();
                        b.set_len(cap);
                    }
                }

                my_futures::read_at(s, b, p).and_then(|(s, b, p, r)| {
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
                                debug!("Received from node {} msg {:?}", ctx.peer.0, msg);
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
            .into_future()
            .map(|_| ());
        let tx_fut = pipe_rx.fold((ctx_tx, sock_tx, Vec::new()), |(ctx, s, mut b), msg| {
                debug!("Sending to node {} msg {:?}", ctx.peer.0, msg);
                unsafe {
                    b.clear();
                    b.reserve(4);
                    b.set_len(4);
                }
                bincode_serde::serialize_into(&mut b, &msg, bincode::SizeLimit::Infinite).unwrap();
                let msg_len = b.len() as u32 - 4;
                (&mut b[0..4]).write_u32::<LittleEndian>(msg_len).unwrap();
                tokio::io::write_all(s, b).map(|(s, b)| (ctx, s, b))
            })
            .into_future()
            .map(|_| ());
        Box::new(rx_fut.select(tx_fut).map(|_| ()).map_err(|(e, _)| e))
    }

    fn run(context: Arc<GlobalContext>, init_tx: ::std::sync::mpsc::Sender<InitType>) {
        let mut core = tokio::reactor::Core::new().unwrap();
        let handle = core.handle();
        let init_result = tokio::net::TcpListener::bind(&context.node.1, &handle)
            .map(|listener| core.handle().spawn(Self::listen(listener, context, core.handle())));

        match init_result {
            Ok(_) => {
                let (completer_tx, completer_rx) = futures::oneshot();
                let _ = init_tx.send(Ok((core.remote(), completer_tx)));
                core.run(completer_rx).unwrap();
            }
            Err(e) => {
                let _ = init_tx.send(Err(e));
            }
        }
    }

    pub fn new(node: NodeId, addr: net::SocketAddr) -> FabricResult<Self> {
        // create context and mark as running
        let context = Arc::new(GlobalContext {
            node: (node, addr),
            nodes_addr: Default::default(),
            msg_handlers: Default::default(),
            writer_chans: Default::default(),
        });
        // start event loop thread
        let context_cloned = context.clone();
        let (init_tx, init_rx) = ::std::sync::mpsc::channel();
        let thread = thread::Builder::new()
            .name(format!("Fabric:{}", node))
            .spawn(move || Self::run(context_cloned, init_tx))
            .unwrap();
        let (remote, completer) = try!(try!(init_rx.recv()));
        Ok(Fabric {
            context: context,
            loop_remote: remote,
            loop_thread: Some((thread, completer)),
        })
    }

    pub fn register_msg_handler(&self, msg_type: FabricMsgType, handler: FabricHandlerFn) {
        self.context.msg_handlers.lock().unwrap().insert(msg_type as u8, handler);
    }

    pub fn register_node(&self, node: NodeId, addr: net::SocketAddr) {
        self.context.nodes_addr.lock().unwrap().insert(node, addr);
        let context = self.context.clone();
        let mut writers = self.context.writer_chans.lock().unwrap();
        writers.insert(node, Default::default());
        self.loop_remote.spawn(move |handle| Self::connect(node, addr, context, handle.clone()))
    }

    pub fn send_msg<T: Into<FabricMsg>>(&self, node_id: NodeId, msg: T) -> FabricResult<()> {
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
                if thread_rng().gen::<f64>() < fabric_drop {
                    warn!("Fabric msg droped due to FABRIC_DROP: {:?}", msg);
                    return Ok(());
                }
            }
        }
        let mut writers = self.context.writer_chans.lock().unwrap();
        match writers.entry(node_id) {
            HMEntry::Occupied(o) => {
                let chans = o.get();
                match chans.len() {
                    0 => {
                        warn!("DROPING MSG - No writers");
                    }
                    1 => {
                        chans[0].send(msg).unwrap();
                    }
                    _ => {
                        thread_rng().choose(&chans).unwrap().send(msg).unwrap();
                    }
                }
            }
            HMEntry::Vacant(_v) => {
                warn!("DROPING MSG - No Channels entry");
            }
        }
        Ok(())
    }
}

impl Drop for Fabric {
    fn drop(&mut self) {
        warn!("droping fabric");
        if let Some((t, c)) = self.loop_thread.take() {
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
        let fabric = Fabric::new(0, "127.0.0.1:6479".parse().unwrap()).unwrap();
        fabric.register_node(0, "127.0.0.1:6479".parse().unwrap());
        let counter = Arc::new(atomic::AtomicUsize::new(0));
        let counter_ = counter.clone();
        fabric.register_msg_handler(FabricMsgType::Crud,
                                    Box::new(move |_, m| {
                                        counter_.fetch_add(1, atomic::Ordering::Relaxed);
                                    }));
        thread::sleep(Duration::from_millis(10));
        for _ in 0..3 {
            fabric.send_msg(0,
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
