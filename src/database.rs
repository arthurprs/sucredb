use std::net;
use std::path::Path;
use std::collections::{HashMap, BTreeMap};
use std::sync::{Arc, Mutex, RwLock};
use bincode::{self, serde as bincode_serde};
use linear_map::LinearMap;
use dht::DHT;
use hash::hash;
use version_vector::*;
use storage::Storage;
use fabric::Fabric;
use fabric_msg::*;
use rand::{Rng, thread_rng};

const PEER_LOG_SIZE: usize = 1000;

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
    dht: DHT<()>,
    fabric: Fabric,
    vnodes: RwLock<HashMap<u16, Mutex<VNode>>>,
    replication_factor: usize,
    // TODO: move this inside vnode?
    // TODO: create a thread to walk inflight and handle timeouts
    inflight: Mutex<HashMap<u64, ReqState>>,
    // FIXME: here just to add dev/debug
    responses: Mutex<HashMap<u64, DottedCausalContainer<Vec<u8>>>>,
}

struct VNodePeer {
    knowledge: u64,
    log: BTreeMap<u64, Vec<u8>>,
}

struct VNode {
    num: u16,
    clock: BitmappedVersionVector,
    log: BTreeMap<u64, Vec<u8>>,
    peers: LinearMap<u64, VNodePeer>,
    storage: Storage,
}

impl Database {
    pub fn new(node: net::SocketAddr) -> Arc<Database> {
        let db = Arc::new(Database {
            replication_factor: 3,
            fabric: Fabric::new(node).unwrap(),
            dht: DHT::new(node, (), 64),
            inflight: Mutex::new(Default::default()),
            responses: Mutex::new(Default::default()),
            vnodes: RwLock::new(Default::default()),
        });
        let cdb = Arc::downgrade(&db);
        db.fabric.register_msg_handler(FabricMsgType::Crud,
                                       Box::new(move |f, m| {
                                           cdb.upgrade().unwrap().fabric_crud_handler(f, m)
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

    pub fn init_vnode(&self, vnode_n: u16) {
        self.vnodes
            .write()
            .unwrap()
            .entry(vnode_n)
            .or_insert_with(|| Mutex::new(VNode::new(vnode_n)));
    }

    pub fn remove_vnode(&self, vnode_n: u16) {
        self.vnodes.write().unwrap().remove(&vnode_n);
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
        let (vnode_n, nodes) = self.dht.nodes_for_key(key, self.replication_factor);
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
            if let Some(container) = container_opt {
                state.container.sync(container);
                state.succesfull += 1;
            }
            state.replies += 1;
            // TODO: only consider succesfull
            if state.succesfull == state.required {
                // return to client & remove state
                true
            } else if state.replies == state.total {
                // remove state
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
            .map(|vn| vn.lock().unwrap().get(self, key));
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
            .map(|vn| vn.lock().unwrap().get(self, &msg.key))
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
        let (vnode_n, nodes) = self.dht.nodes_for_key(key, self.replication_factor);
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
        debug!("set_local {:?}", vnode_n);
        let dcc = self.vnodes
            .read()
            .unwrap()
            .get(&vnode_n)
            .map(|vn| {
                vn.lock().unwrap().set_local(self, hash(self.dht.node()), key, value_opt, vv)
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
            .map(|vn| vn.lock().unwrap().set_remote(self, hash(from), &key, container))
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

impl VNodePeer {
    fn new() -> VNodePeer {
        VNodePeer {
            knowledge: 0,
            log: Default::default(),
        }
    }

    fn advance_knowledge(&mut self, until: u64) {
        debug_assert!(until > self.knowledge);
        self.knowledge = until;
    }

    fn log(&mut self, dot: u64, key: Vec<u8>) {
        let prev = self.log.insert(dot, key);
        debug_assert!(prev.is_none());
        while self.log.len() > PEER_LOG_SIZE {
            let min = self.log.keys().next().cloned().unwrap();
            self.log.remove(&min).unwrap();
        }
    }
}

impl VNode {
    fn new(num: u16) -> VNode {
        VNode {
            num: num,
            clock: BitmappedVersionVector::new(),
            peers: Default::default(),
            storage: Storage::open(Path::new("./vnode_t"), num as i32, true).unwrap(),
            log: Default::default(),
        }
    }

    fn get(&self, _db: &Database, key: &[u8]) -> DottedCausalContainer<Vec<u8>> {
        let mut dcc = if let Some(bytes) = self.storage.get_vec(key) {
            bincode_serde::deserialize(&bytes).unwrap()
        } else {
            DottedCausalContainer::new()
        };
        dcc.fill(&self.clock);
        dcc
    }

    fn set_local(&mut self, db: &Database, id: u64, key: &[u8], value_opt: Option<&[u8]>,
                 vv: &VersionVector)
                 -> DottedCausalContainer<Vec<u8>> {
        let mut dcc = self.get(db, key);
        dcc.discard(vv);
        let dot = self.clock.event(id);
        if let Some(value) = value_opt {
            dcc.add(id, dot, value.into());
        }
        dcc.strip(&self.clock);

        if dcc.is_empty() {
            self.storage.del(key);
        } else {
            let mut bytes = Vec::new();
            bincode_serde::serialize_into(&mut bytes, &dcc, bincode::SizeLimit::Infinite).unwrap();
            self.storage.set(key, &bytes);
        }

        self.log.insert(dot, key.into());
        dcc
    }

    fn set_remote(&mut self, db: &Database, peer_id: u64, key: &[u8],
                  mut new_dcc: DottedCausalContainer<Vec<u8>>) {
        let old_dcc = self.get(db, key);
        new_dcc.add_to_bvv(&mut self.clock);
        new_dcc.sync(old_dcc);
        new_dcc.strip(&self.clock);

        if new_dcc.is_empty() {
            self.storage.del(key);
        } else {
            let mut bytes = Vec::new();
            bincode_serde::serialize_into(&mut bytes, &new_dcc, bincode::SizeLimit::Infinite)
                .unwrap();
            self.storage.set(key, &bytes);
        }

        self.peers
            .entry(peer_id)
            .or_insert_with(|| VNodePeer::new())
            .log(self.clock.get(peer_id).unwrap().base(), key.into());
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
        let _ = fs::remove_dir_all("./vnode_t");
        let _ = env_logger::init();
        let db = Database::new("127.0.0.1:9000".parse().unwrap());
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

        db.set(1,
               b"test",
               Some(b"value12"),
               state.version_vector().clone());
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
        let _ = fs::remove_dir_all("./vnode_t");
        let _ = env_logger::init();
        let db1 = Database::new("127.0.0.1:9000".parse().unwrap());
        let db2 = Database::new("127.0.0.1:9001".parse().unwrap());
        for i in 0u16..64 {
            db1.init_vnode(i);
            db2.init_vnode(i);
        }
        for i in 0u16..32 {
            db1.dht.add_pending_node(i * 2 + 1, db2.dht.node(), ());
            db1.dht.promote_pending_node(i * 2 + 1, db2.dht.node());
            db2.dht.add_pending_node(i * 2 + 1, db1.dht.node(), ());
            db2.dht.promote_pending_node(i * 2 + 1, db1.dht.node());
        }

        db1.get(1, b"test");
        thread::sleep_ms(1000);
        assert!(db1.response(1).unwrap().is_empty());

        db1.set(1, b"test", Some(b"value1"), VersionVector::new());
        thread::sleep_ms(1000);
        assert!(db1.response(1).unwrap().is_empty());

        for &db in &[&db1, &db2] {
            db.get(1, b"test");
            thread::sleep_ms(1000);
            assert!(db.response(1).unwrap().values().eq(vec![b"value1"]));
        }

        thread::sleep_ms(1000);

    }
}
