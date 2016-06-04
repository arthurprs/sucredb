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
    replies: usize,
    required: usize,
    total: usize,
    from: net::SocketAddr,
    // only used for get
    container: DottedCausalContainer<Vec<u8>>,
}

pub struct Database {
    dht: DHT<()>,
    fabric: Fabric,
    vnodes: RwLock<HashMap<u16, Mutex<VNode>>>,
    replication_factor: usize,
    //TODO: move this inside vnode?
    //TODO: create a thread to walk inflight and handle timeouts
    inflight: Mutex<HashMap<u64, ReqState>>,
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
            vnodes: RwLock::new(Default::default()),
        });
        register_handlers(db.clone());
        db
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

    fn new_state_from(&self, token: usize, from: net::SocketAddr) -> u64 {
        let cookie = token as u64;
        let _a = self.inflight
            .lock()
            .unwrap()
            .insert(cookie,
                    ReqState {
                        from: from,
                        required: self.replication_factor / 2 + 1,
                        total: self.replication_factor,
                        replies: 0,
                        container: DottedCausalContainer::new(),
                    });
        debug_assert!(_a.is_none());
        cookie
    }


    fn new_state(&self, token: usize) -> u64 {
        self.new_state_from(token, self.dht.node())
    }

    pub fn get(&self, token: usize, key: &[u8]) {
        let vnode_n = self.dht.key_vnode(key);
        let nodes = self.dht.nodes_for_key(key, self.replication_factor);
        let cookie = self.new_state(token);

        for node in nodes {
            if node == self.dht.node() {
                self.get_local(cookie, key);
            } else {
                self.send_get_remote(&node, vnode_n, cookie, key);
            }
        }
    }

    fn get_callback(&self, cookie: u64, container: Option<DottedCausalContainer<Vec<u8>>>) {
        let mut inflight = self.inflight.lock().unwrap();
        if {
            let state = inflight.get_mut(&cookie).unwrap();
            container.map(|dcc| state.container.sync(dcc));
            state.replies += 1;
            if state.replies == state.required {
                // return to client
            }
            if state.replies == state.total {
                // remove state
                true
            } else {
                false
            }
        } {
            inflight.remove(&cookie).unwrap();
        }
    }

    fn get_local(&self, cookie: u64, key: &[u8]) {
        let vnode_n = self.dht.key_vnode(key);
        let container = self.vnodes
            .read()
            .unwrap()
            .get(&vnode_n)
            .map(|vn| vn.lock().unwrap().get(self, key));
        self.get_callback(cookie, container);
    }

    fn send_get_remote(&self, addr: &net::SocketAddr, vnode: u16, cookie: u64, key: &[u8]) {
        self.fabric
            .send_message(addr,
                          FabricMsg::Get(FabricMsgGet {
                              cookie: cookie,
                              vnode: vnode,
                              key: key.into(),
                          }))
            .unwrap();
    }

    pub fn get_remote_handler(&self, from: &net::SocketAddr, msg: FabricMsgGet) {
        let container = self.vnodes
            .read()
            .unwrap()
            .get(&msg.vnode)
            .map(|vn| vn.lock().unwrap().get(self, &msg.key));
        self.fabric
            .send_message(&from,
                          FabricMsg::GetAck(FabricMsgGetAck {
                              cookie: msg.cookie,
                              vnode: msg.vnode,
                              container: container.unwrap(),
                          }))
            .unwrap();
    }

    pub fn get_remote_ack_handler(&self, _from: &net::SocketAddr, msg: FabricMsgGetAck) {
        self.get_callback(msg.cookie, Some(msg.container));
    }

    pub fn set(&self, token: usize, key: &[u8], value_opt: Option<&[u8]>, vv: VersionVector) {
        self.set_(self.dht.node(), token, key, value_opt, vv)
    }

    pub fn set_(&self, from: net::SocketAddr, token: usize, key: &[u8], value_opt: Option<&[u8]>,
                vv: VersionVector) {
        let vnode_n = self.dht.key_vnode(key);
        let nodes = self.dht.nodes_for_key(key, self.replication_factor);
        let cookie = self.new_state_from(token, from);

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

    fn set_callback(&self, cookie: u64) {
        let mut inflight = self.inflight.lock().unwrap();
        if {
            let state = inflight.get_mut(&cookie).unwrap();
            state.replies += 1;
            if state.replies == state.required {
                if state.from == self.dht.node() {
                    // return to proxy
                } else {
                    // return to client
                }
            }
            if state.replies == state.total {
                // remove state
                true
            } else {
                false
            }
        } {
            inflight.remove(&cookie).unwrap();
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
        self.set_callback(cookie);
        dcc.unwrap()
    }

    pub fn send_set(&self, addr: &net::SocketAddr, vnode: u16, cookie: u64, key: &[u8],
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

    pub fn send_set_remote(&self, addr: &net::SocketAddr, vnode: u16, cookie: u64, key: &[u8],
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

    pub fn set_handler(&self, from: &net::SocketAddr, msg: FabricMsgSet) {
        unimplemented!()
    }

    pub fn set_ack_handler(&self, from: &net::SocketAddr, msg: FabricMsgSetAck) {
        unimplemented!()
    }

    pub fn set_remote_handler(&self, from: &net::SocketAddr, msg: FabricMsgSetRemote) {
        self.vnodes
            .read()
            .unwrap()
            .get(&msg.vnode)
            .map(move |vn| {
                vn.lock().unwrap().set_remote(self, hash(from), &msg.key, msg.container)
            });
    }

    pub fn set_remote_ack_handler(&self, _from: &net::SocketAddr, msg: FabricMsgSetRemoteAck) {
        self.set_callback(msg.cookie);
    }

    fn inflight(&self, cookie: u64) -> Option<ReqState> {
        self.inflight.lock().unwrap().remove(&cookie)
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

// boilerplate
fn register_handlers(db: Arc<Database>) {
    // FIXME: these create a circular ref :/
    let cdb = db.clone();
    db.fabric.register_msg_handler(FabricMsgType::Get,
                                   Box::new(move |f, m| {
                                       cdb.get_remote_handler(&f, fmsg!(m, FabricMsg::Get))
                                   }));
    let cdb = db.clone();
    db.fabric.register_msg_handler(FabricMsgType::GetAck,
                                   Box::new(move |f, m| {
                                       cdb.get_remote_ack_handler(&f, fmsg!(m, FabricMsg::GetAck))
                                   }));
    let cdb = db.clone();
    db.fabric.register_msg_handler(FabricMsgType::Set,
                                   Box::new(move |f, m| {
                                       cdb.set_handler(&f, fmsg!(m, FabricMsg::Set))
                                   }));
    let cdb = db.clone();
    db.fabric.register_msg_handler(FabricMsgType::SetAck,
                                   Box::new(move |f, m| {
                                       cdb.set_ack_handler(&f, fmsg!(m, FabricMsg::SetAck))
                                   }));
    let cdb = db.clone();
    db.fabric.register_msg_handler(FabricMsgType::SetRemote,
                                   Box::new(move |f, m| {
                                       cdb.set_remote_handler(&f, fmsg!(m, FabricMsg::SetRemote))
                                   }));
    let cdb = db.clone();
    db.fabric.register_msg_handler(FabricMsgType::SetRemoteAck,
                                   Box::new(move |f, m| {
                                       cdb.set_remote_ack_handler(&f,
                                                                  fmsg!(m, FabricMsg::SetRemoteAck))
                                   }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
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
        assert!(db.inflight(1).unwrap().container.is_empty());

        db.set(1, b"test", Some(b"value1"), VersionVector::new());
        assert!(db.inflight(1).unwrap().container.is_empty());

        db.get(1, b"test");
        assert!(db.inflight(1).unwrap().container.values().eq(vec![b"value1"]));

        db.set(1, b"test", Some(b"value2"), VersionVector::new());
        assert!(db.inflight(1).unwrap().container.is_empty());

        db.get(1, b"test");
        let state = db.inflight(1).unwrap();
        assert!(state.container.values().eq(vec![b"value1", b"value2"]));

        db.set(1,
               b"test",
               Some(b"value12"),
               state.container.version_vector().clone());
        assert!(db.inflight(1).unwrap().container.is_empty());

        db.get(1, b"test");
        let state = db.inflight(1).unwrap();
        assert!(state.container.values().eq(vec![b"value12"]));

        db.set(1, b"test", None, state.container.version_vector().clone());
        assert!(db.inflight(1).unwrap().container.is_empty());

        db.get(1, b"test");
        assert!(db.inflight(1).unwrap().container.is_empty());
    }
}
