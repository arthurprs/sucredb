use std::{net, path};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use dht::DHT;
use version_vector::*;
use fabric::*;
use rand::{Rng, thread_rng};
use vnode::*;


pub struct Database {
    pub dht: DHT<()>,
    pub fabric: Fabric,
    pub replication_factor: usize,
    storage_dir: path::PathBuf,
    vnodes: RwLock<HashMap<u16, Mutex<VNode>>>,
    // TODO: move this inside vnode?
    // TODO: create a thread to walk inflight and handle timeouts
    inflight: Mutex<HashMap<u64, ProxyReqState>>,
    // FIXME: here just to add dev/debug
    responses: Mutex<HashMap<u64, DottedCausalContainer<Vec<u8>>>>,
}

struct ProxyReqState {
    from: NodeId,
    cookie: u64,
}

macro_rules! vnode {
    ($s: expr, k: $k: expr, $ok: expr) => ({
        vnode!($s, $s.dht.key_vnode(&$k), $ok);
    });
    ($s: expr, k: $k: expr, $ok: expr, $el: expr) => ({
        vnode!($s, $s.dht.key_vnode(&$k), $ok, $el);
    });
    ($s: expr, $k: expr, $ok: expr) => ({
        let vnodes = $s.vnodes.read().unwrap();
        vnodes.get(&$k).map(|vn| vn.lock().unwrap()).map($ok);
    });
    ($s: expr, $k: expr, $ok: expr, $el: expr) => ({
        let vnodes = $s.vnodes.read().unwrap();
        vnodes.get(&$k).ok_or($k).map(|vn| vn.lock().unwrap()).map_or_else($el, $ok);
    });
}

impl Database {
    pub fn new(bind_addr: net::SocketAddr, storage_dir: &str, create: bool) -> Arc<Database> {
        // FIXME: save to storage
        let node = thread_rng().gen::<NodeId>();
        let db = Arc::new(Database {
            replication_factor: 3,
            fabric: Fabric::new(node, bind_addr).unwrap(),
            dht: DHT::new(node,
                          bind_addr,
                          if create {
                              Some(((), 64))
                          } else {
                              None
                          }),
            storage_dir: storage_dir.into(),
            inflight: Mutex::new(Default::default()),
            responses: Mutex::new(Default::default()),
            vnodes: RwLock::new(Default::default()),
        });

        db.dht.members().into_iter().map(|(n, a)| db.fabric.register_node(n, a)).count();

        let cdb = Arc::downgrade(&db);
        db.dht.set_callback(Box::new(move || {
            cdb.upgrade().map(|db| {
                db.dht.members().into_iter().map(|(n, a)| db.fabric.register_node(n, a)).count()
            });
        }));

        let cdb = Arc::downgrade(&db);
        db.fabric.register_msg_handler(FabricMsgType::Crud,
                                       Box::new(move |f, m| {
                                           cdb.upgrade().map(|db| db.handler_fabric_crud(f, m));
                                       }));
        let cdb = Arc::downgrade(&db);
        db.fabric.register_msg_handler(FabricMsgType::Bootstrap,
                                       Box::new(move |f, m| {
                                           cdb.upgrade().map(|db| db.handler_fabric_boostrap(f, m));
                                       }));
        db
    }

    pub fn handler_fabric_crud(&self, from: NodeId, msg: FabricMsg) {
        match msg {
            FabricMsg::GetRemote(m) => self.handler_get_remote(from, m),
            FabricMsg::GetRemoteAck(m) => self.handler_get_remote_ack(from, m),
            FabricMsg::Set(m) => self.handler_set(from, m),
            FabricMsg::SetAck(m) => self.handler_set_ack(from, m),
            FabricMsg::SetRemote(m) => self.handler_set_remote(from, m),
            FabricMsg::SetRemoteAck(m) => self.handler_set_remote_ack(from, m),
            _ => unreachable!("Can't handle {:?}", msg),
        }
    }

    pub fn handler_fabric_boostrap(&self, from: NodeId, msg: FabricMsg) {
        let vnodes = self.vnodes.read().unwrap();
        match msg {
            FabricMsg::BootstrapStart(m) => {
                vnodes.get(&m.vnode)
                    .map(|vn| vn.lock().unwrap().handler_bootstrap_start(self, from, m))
            }
            FabricMsg::BootstrapSend(m) => {
                vnodes.get(&m.vnode)
                    .map(|vn| vn.lock().unwrap().handler_bootstrap_send(self, from, m))
            }
            FabricMsg::BootstrapAck(m) => {
                vnodes.get(&m.vnode)
                    .map(|vn| vn.lock().unwrap().handler_bootstrap_ack(self, from, m))
            }
            FabricMsg::BootstrapFin(m) => {
                vnodes.get(&m.vnode)
                    .map(|vn| vn.lock().unwrap().handler_bootstrap_fin(self, from, m))
            }
            _ => unreachable!("Can't handle {:?}", msg),
        };
    }

    pub fn init_vnode(&self, vnode_n: u16) {
        self.vnodes
            .write()
            .unwrap()
            .entry(vnode_n)
            .or_insert_with(|| Mutex::new(VNode::new(&self.storage_dir, vnode_n)));
    }

    pub fn remove_vnode(&self, vnode_n: u16) {
        self.vnodes.write().unwrap().remove(&vnode_n);
    }

    fn start_migration(&self, vnode: u16) {
        let vnodes = self.vnodes.read().unwrap();
        vnodes.get(&vnode).unwrap().lock().unwrap().start_migration(self);
    }

    pub fn send_set(&self, addr: NodeId, vnode: u16, cookie: u64, key: &[u8],
                    value_opt: Option<&[u8]>, vv: VersionVector) {
        self.fabric
            .send_message(addr,
                          FabricMsg::Set(MsgSet {
                              cookie: cookie,
                              vnode: vnode,
                              key: key.into(),
                              value: value_opt.map(|x| x.into()),
                              version_vector: vv,
                          }))
            .unwrap();
    }


    // CLIENT CRUD
    fn set(&self, token: u64, key: &[u8], value: Option<&[u8]>, vv: VersionVector) {
        vnode!(self, k: key, |mut vn| {
            vn.do_set(self, token, key, value, vv);
        });
    }

    fn get(&self, token: u64, key: &[u8]) {
        vnode!(self, k: key, |mut vn| {
            vn.do_get(self, token, key);
        });
    }

    // CRUD HANDLERS
    fn handler_set(&self, from: NodeId, msg: MsgSet) {
        vnode!(self, msg.vnode, |mut vn| {
            vn.handler_set(self, from, msg);
        });
    }

    fn handler_set_ack(&self, from: NodeId, msg: MsgSetAck) {

    }

    fn handler_set_remote(&self, from: NodeId, msg: MsgSetRemote) {
        vnode!(self, msg.vnode, |mut vn| {
            vn.handler_set_remote(self, from, msg);
        });
    }

    fn handler_set_remote_ack(&self, from: NodeId, msg: MsgSetRemoteAck) {
        vnode!(self, msg.vnode, |mut vn| {
            vn.handler_set_remote_ack(self, from, msg);
        });
    }

    fn handler_get_remote(&self, from: NodeId, msg: MsgGetRemote) {
        vnode!(self, msg.vnode, |mut vn| {
            vn.handler_get_remote(self, from, msg);
        });
    }

    fn handler_get_remote_ack(&self, from: NodeId, msg: MsgGetRemoteAck) {
        vnode!(self, msg.vnode, |mut vn| {
            vn.handler_get_remote_ack(self, from, msg);
        });
    }

    // UTILITIES

    fn inflight(&self, cookie: u64) -> Option<ProxyReqState> {
        self.inflight.lock().unwrap().remove(&cookie)
    }

    pub fn set_response(&self, cookie: u64, response: DottedCausalContainer<Vec<u8>>) {
        self.responses.lock().unwrap().insert(cookie, response);
    }

    fn response(&self, cookie: u64) -> Option<DottedCausalContainer<Vec<u8>>> {
        self.responses.lock().unwrap().remove(&cookie)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::thread;
    use env_logger;
    use version_vector::VersionVector;

    #[test]
    fn test() {
        let _ = fs::remove_dir_all("./t");
        let _ = env_logger::init();
        let db = Database::new("127.0.0.1:9000".parse().unwrap(), "t/db", true);
        for i in 0u16..64 {
            db.init_vnode(i);
        }
        db.get(1, b"test");
        assert!(db.response(1).unwrap().is_empty());

        db.set(1, b"test", Some(b"value1"), VersionVector::new());
        assert!(db.response(1).unwrap().is_empty());

        db.get(1, b"test");
        assert!(db.response(1).unwrap().values().eq(vec![b"value1"]));

        db.set(1, b"test", Some(b"value2"), VersionVector::new());
        assert!(db.response(1).unwrap().is_empty());

        db.get(1, b"test");
        let state = db.response(1).unwrap();
        assert!(state.values().eq(vec![b"value1", b"value2"]));

        db.set(1, b"test", Some(b"value12"), state.version_vector().clone());
        assert!(db.response(1).unwrap().is_empty());

        db.get(1, b"test");
        let state = db.response(1).unwrap();
        assert!(state.values().eq(vec![b"value12"]));

        db.set(1, b"test", None, state.version_vector().clone());
        assert!(db.response(1).unwrap().is_empty());

        db.get(1, b"test");
        assert!(db.response(1).unwrap().is_empty());
    }

    #[test]
    fn test_two() {
        let _ = fs::remove_dir_all("./t");
        let _ = env_logger::init();
        let db1 = Database::new("127.0.0.1:9000".parse().unwrap(), "t/db1", true);
        let db2 = Database::new("127.0.0.1:9001".parse().unwrap(), "t/db2", false);
        for i in 0u16..64 {
            db1.init_vnode(i);
            db2.init_vnode(i);
        }
        for i in 0u16..32 {
            db1.dht.add_pending_node(i * 2 + 1, db2.dht.node(), ());
            db1.dht.promote_pending_node(i * 2 + 1, db2.dht.node());
        }

        db1.get(1, b"test");
        thread::sleep_ms(100);
        assert!(db1.response(1).unwrap().is_empty());

        db1.set(1, b"test", Some(b"value1"), VersionVector::new());
        thread::sleep_ms(100);
        assert!(db1.response(1).unwrap().is_empty());

        for &db in &[&db1, &db2] {
            db.get(1, b"test");
            thread::sleep_ms(100);
            assert!(db.response(1).unwrap().values().eq(vec![b"value1"]));
        }
        thread::sleep_ms(100);

        for i in 0u16..32 {
            db1.start_migration(i * 2 + 1);
            db2.start_migration(i * 2);
        }

        thread::sleep_ms(1000);

    }
}
