use std::{str, io, net, thread};
use std::cell::RefCell;
use std::rc::Rc;
use std::net::SocketAddr;
use std::sync::{Mutex, Arc};
use std::collections::HashMap;

use rand::{thread_rng, Rng};
use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};
use serde::{Serialize, Deserialize};
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
    peer: FabricId,
}

struct WriterContext {
    peer: FabricId,
}

struct GlobalContext {
    node: FabricId,
    msg_handlers: Mutex<HashMap<u8, FabricHandlerFn>>,
    nodes_addr: Mutex<HashMap<NodeId, net::SocketAddr>>,
    writer_chans: Arc<Mutex<HashMap<NodeId, tokio::channel::Sender<FabricMsg>>>>,
}

pub struct Fabric {
    loop_thread: Option<(thread::JoinHandle<()>, futures::Complete<()>)>,
    context: Arc<GlobalContext>,
}

impl Fabric {
    pub fn new(node: NodeId, bind_addr: net::SocketAddr) -> FabricResult<Self> {
        let core = tokio::reactor::Core::new().unwrap();
        // create context and mark as running
        let context = GlobalContext {

        };
        // start event loop thread
        let (completer, oneshot) = futures::oneshot();
        let thread = thread::Builder::new()
            .name(format!("Fabric:{}", node))
            .spawn(move || core.run(oneshot).unwrap())
            .unwrap();
        Ok(Fabric {
            loop_thread: Some((thread, completer)),
            context: context,
        })
    }

    pub fn register_msg_handler(&self, msg_type: FabricMsgType, handler: FabricHandlerFn) {
        self.context.msg_handlers.lock().unwrap().insert(msg_type as u8, handler);
    }

    pub fn register_node(&self, node: NodeId, addr: net::SocketAddr) {
        self.context.nodes_addr.lock().unwrap().insert(node, addr);
    }

    pub fn send_msg<T: Into<FabricMsg>>(&self, recipient: NodeId, msg: T) -> FabricResult<()> {
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
                if fabric_drop > 0.0 && ::rand::thread_rng().gen::<f64>() < fabric_drop {
                    warn!("Fabric msg droped due to FABRIC_DROP: {:?}", msg);
                    return Ok(());
                }
            }
        }
        unimplemented!()
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
