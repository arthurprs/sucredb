use std::{net, path};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use dht::DHT;
use hash::hash;
use version_vector::*;
use fabric::Fabric;
use fabric_msg::*;
use rand::{Rng, thread_rng};
use vnode::VNode;

pub struct ReqState {
    replies: u8,
    succesfull: u8,
    required: u8,
    total: u8,
    from: net::SocketAddr,
    // only used for get
    container: DottedCausalContainer<Vec<u8>>,
}

pub struct Database {
    pub dht: DHT<()>,
    pub fabric: Fabric,
    pub replication_factor: usize,
    storage_dir: path::PathBuf,
    vnodes: RwLock<HashMap<u16, Mutex<VNode>>>,
    // TODO: move this inside vnode?
    // TODO: create a thread to walk inflight and handle timeouts
    inflight: Mutex<HashMap<u64, ReqState>>,
    // FIXME: here just to add dev/debug
    responses: Mutex<HashMap<u64, DottedCausalContainer<Vec<u8>>>>,
}


impl Database {
    pub fn new(node: net::SocketAddr, storage_dir: &str, create: bool) -> Arc<Database> {
        let db = Arc::new(Database {
            replication_factor: 3,
            fabric: Fabric::new(node).unwrap(),
            dht: DHT::new(node,
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
        let cdb = Arc::downgrade(&db);
        db.fabric.register_msg_handler(FabricMsgType::Crud,
                                       Box::new(move |f, m| {
                                           cdb.upgrade().unwrap().fabric_crud_handler(f, m)
                                       }));
        let cdb = Arc::downgrade(&db);
        db.fabric.register_msg_handler(FabricMsgType::Bootstrap,
                                       Box::new(move |f, m| {
                                           cdb.upgrade().unwrap().fabric_boostrap_handler(f, m)
                                       }));
        db
    }

    pub fn fabric_crud_handler(&self, from: &net::SocketAddr, msg: FabricMsg) {
        match msg {
            FabricMsg::GetRemote(m) => self.get_remote_handler(from, m),
            FabricMsg::GetRemoteAck(m) => self.get_remote_ack_handler(from, m),
            FabricMsg::Set(m) => self.set_handler(from, m),
            FabricMsg::SetAck(m) => self.set_ack_handler(from, m),
            FabricMsg::SetRemote(m) => self.set_remote_handler(from, m),
            FabricMsg::SetRemoteAck(m) => self.set_remote_ack_handler(from, m),
            _ => unreachable!("Can't handle {:?}", msg),
        }
    }

    pub fn fabric_boostrap_handler(&self, from: &net::SocketAddr, msg: FabricMsg) {
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

    fn new_state_from(&self, token: usize, nodes: u8, from: net::SocketAddr) -> u64 {
        let cookie = token as u64;
        let _a = self.inflight
            .lock()
            .unwrap()
            .insert(cookie,
                    ReqState {
                        from: from,
                        required: nodes / 2 + 1,
                        total: nodes,
                        replies: 0,
                        succesfull: 0,
                        container: DottedCausalContainer::new(),
                    });
        debug_assert!(_a.is_none());
        cookie
    }

    fn new_state(&self, token: usize, nodes: u8) -> u64 {
        self.new_state_from(token, nodes, self.dht.node())
    }

    pub fn get(&self, token: usize, key: &[u8]) {
        let (vnode_n, nodes) = self.dht.nodes_for_key(key, self.replication_factor, false);
        let cookie = self.new_state(token, nodes.len() as u8);

        for node in nodes {
            if node == self.dht.node() {
                self.get_local(vnode_n, cookie, key);
            } else {
                self.send_get_remote(&node, vnode_n, cookie, key);
            }
        }
    }

    fn get_callback(&self, cookie: u64, container_opt: Option<DottedCausalContainer<Vec<u8>>>) {
        let mut inflight = self.inflight.lock().unwrap();
        if {
            let state = inflight.get_mut(&cookie).unwrap();
            state.replies += 1;
            if let Some(container) = container_opt {
                state.container.sync(container);
                state.succesfull += 1;
            }
            if state.succesfull == state.required {
                // return to client & remove state
                true
            } else {
                false
            }
        } {
            let state = inflight.remove(&cookie).unwrap();
            self.responses.lock().unwrap().insert(cookie, state.container);
        }
    }

    fn get_local(&self, vnode: u16, cookie: u64, key: &[u8]) {
        let container = self.vnodes
            .read()
            .unwrap()
            .get(&vnode)
            .map(|vn| vn.lock().unwrap().storage_get(self, key));
        self.get_callback(cookie, container);
    }

    fn send_get_remote(&self, addr: &net::SocketAddr, vnode: u16, cookie: u64, key: &[u8]) {
        self.fabric
            .send_message(addr,
                          FabricMsg::GetRemote(FabricMsgGetRemote {
                              cookie: cookie,
                              vnode: vnode,
                              key: key.into(),
                          }))
            .unwrap();
    }

    pub fn get_remote_handler(&self, from: &net::SocketAddr, msg: FabricMsgGetRemote) {
        let result = self.vnodes
            .read()
            .unwrap()
            .get(&msg.vnode)
            .map(|vn| vn.lock().unwrap().storage_get(self, &msg.key))
            .ok_or(FabricMsgError::VNodeNotFound);
        self.fabric
            .send_message(&from,
                          FabricMsg::GetRemoteAck(FabricMsgGetRemoteAck {
                              cookie: msg.cookie,
                              vnode: msg.vnode,
                              result: result,
                          }))
            .unwrap();
    }

    fn get_remote_ack_handler(&self, _from: &net::SocketAddr, msg: FabricMsgGetRemoteAck) {
        self.get_callback(msg.cookie, msg.result.ok());
    }

    pub fn set(&self, token: usize, key: &[u8], value_opt: Option<&[u8]>, vv: VersionVector) {
        self.set_(self.dht.node(), token, key, value_opt, vv)
    }

    pub fn set_(&self, from: net::SocketAddr, token: usize, key: &[u8], value_opt: Option<&[u8]>,
                vv: VersionVector) {
        let (vnode_n, nodes) = self.dht.nodes_for_key(key, self.replication_factor, true);
        let cookie = self.new_state_from(token, nodes.len() as u8, from);

        if nodes.iter().position(|n| n == &self.dht.node()).is_none() {
            // forward if we can't coordinate it
            self.send_set(thread_rng().choose(&nodes).unwrap(),
                          vnode_n,
                          cookie,
                          key,
                          value_opt,
                          vv);
        } else {
            // otherwise proceed normally
            let dcc = self.set_local(cookie, key, value_opt, &vv);
            for node in nodes {
                if node != self.dht.node() {
                    self.send_set_remote(&node, vnode_n, cookie, key, dcc.clone());
                }
            }
        }
    }

    fn set_callback(&self, cookie: u64, succesfull: bool) {
        let mut inflight = self.inflight.lock().unwrap();
        if {
            let state = inflight.get_mut(&cookie).unwrap();
            state.replies += 1;
            if succesfull {
                state.succesfull += 1;
            }
            if state.succesfull == state.required {
                // remove state
                true
            } else {
                false
            }
        } {
            let state = inflight.remove(&cookie).unwrap();
            if state.from == self.dht.node() {
                // return to proxy
            } else {
                // return to client
            }
            self.responses.lock().unwrap().insert(cookie, state.container);
        }
    }

    fn set_local(&self, cookie: u64, key: &[u8], value_opt: Option<&[u8]>, vv: &VersionVector)
                 -> DottedCausalContainer<Vec<u8>> {
        let vnode_n = self.dht.key_vnode(key);
        debug!("[{}] set_local {:?}", self.dht.node(), vnode_n);
        let dcc = self.vnodes
            .read()
            .unwrap()
            .get(&vnode_n)
            .map(|vn| {
                vn.lock()
                    .unwrap()
                    .storage_set_local(self, hash(self.dht.node()), key, value_opt, vv)
            });
        self.set_callback(cookie, true);
        dcc.unwrap()
    }

    fn send_set(&self, addr: &net::SocketAddr, vnode: u16, cookie: u64, key: &[u8],
                value_opt: Option<&[u8]>, vv: VersionVector) {
        self.fabric
            .send_message(addr,
                          FabricMsg::Set(FabricMsgSet {
                              cookie: cookie,
                              vnode: vnode,
                              key: key.into(),
                              value: value_opt.map(|x| x.into()),
                              version_vector: vv,
                          }))
            .unwrap();
    }

    fn send_set_remote(&self, addr: &net::SocketAddr, vnode: u16, cookie: u64, key: &[u8],
                       dcc: DottedCausalContainer<Vec<u8>>) {
        self.fabric
            .send_message(addr,
                          FabricMsg::SetRemote(FabricMsgSetRemote {
                              cookie: cookie,
                              vnode: vnode,
                              key: key.into(),
                              container: dcc.clone(),
                          }))
            .unwrap();
    }

    fn set_handler(&self, from: &net::SocketAddr, msg: FabricMsgSet) {
        // self.set_(*from, msg.cookie, &msg.key, msg.value.map(|v| &v[..]), msg.version_vector);
        unimplemented!()
    }

    fn set_ack_handler(&self, _from: &net::SocketAddr, msg: FabricMsgSetAck) {
        self.set_callback(msg.cookie, msg.result.is_ok());
    }

    fn set_remote_handler(&self, from: &net::SocketAddr, msg: FabricMsgSetRemote) {
        let FabricMsgSetRemote { key, container, vnode, cookie } = msg;
        let result = self.vnodes
            .read()
            .unwrap()
            .get(&msg.vnode)
            .map(|vn| vn.lock().unwrap().storage_set_remote(self, hash(from), &key, container))
            .ok_or(FabricMsgError::VNodeNotFound);
        self.fabric
            .send_message(from,
                          FabricMsg::SetRemoteAck(FabricMsgSetRemoteAck {
                              vnode: vnode,
                              cookie: cookie,
                              result: result,
                          }))
            .unwrap();
    }

    fn set_remote_ack_handler(&self, _from: &net::SocketAddr, msg: FabricMsgSetRemoteAck) {
        self.set_callback(msg.cookie, msg.result.is_ok());
    }

    fn inflight(&self, cookie: u64) -> Option<ReqState> {
        self.inflight.lock().unwrap().remove(&cookie)
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
            thread::sleep_ms(20);
            db1.dht.promote_pending_node(i * 2 + 1, db2.dht.node());
            thread::sleep_ms(20);
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
            db2.start_migration(i * 2);
        }

        thread::sleep_ms(1000);

    }
}
