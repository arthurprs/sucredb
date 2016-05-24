use std::net;
use std::collections::{HashMap, BTreeMap};
use std::sync::{Mutex, RwLock};
use bincode::{self, serde as bincode_serde};
use linear_map::LinearMap;
use dht::DHT;
use hash::hash;
use version_vector::*;
use storage::Storage;

const PEER_LOG_SIZE: usize = 1000;

pub struct ReqState {
    replies: usize,
    required: usize,
    total: usize,
    // only used for get
    container: DottedCausalContainer<Vec<u8>>,
}

pub struct Database {
    dht: DHT<()>,
    vnodes: RwLock<HashMap<usize, Mutex<VNode>>>,
    replication_factor: usize,
    inflight: Mutex<HashMap<usize, ReqState>>,
}

struct VNodePeer {
    knowledge: u64,
    log: BTreeMap<u64, Vec<u8>>,
}

struct VNode {
    clock: BitmappedVersionVector,
    log: BTreeMap<u64, Vec<u8>>,
    peers: LinearMap<u64, VNodePeer>,
    storage: Storage,
}

impl Database {
    pub fn new() -> Database {
        let node = net::lookup_host("127.0.0.1").unwrap().next().unwrap().unwrap();
        Database {
            replication_factor: 1,
            dht: DHT::new(node, (), 64),
            inflight: Mutex::new(Default::default()),
            vnodes: RwLock::new(Default::default()),
        }
    }

    pub fn init_vnode(&self, vnode_n: usize) {
        self.vnodes.write().unwrap().entry(vnode_n).or_insert_with(|| Mutex::new(VNode::new()));
    }

    pub fn remove_vnode(vnode_n: usize) {
        unimplemented!()
    }

    pub fn get(&self, token: usize, key: &[u8]) {
        let nodes = self.dht.nodes_for_key(key, self.replication_factor);
        let _a = self.inflight
            .lock()
            .unwrap()
            .insert(token,
                    ReqState {
                        required: self.replication_factor / 2 + 1,
                        total: self.replication_factor,
                        replies: 0,
                        container: DottedCausalContainer::new(),
                    });
        debug_assert!(_a.is_none());
        for node in nodes {
            if node == self.dht.node() {
                self.get_local(token, key);
            } else {
                // self.get_remote(token, key);
            }
        }
    }

    fn process_get(&self, token: usize, container: Option<DottedCausalContainer<Vec<u8>>>) {
        let mut inflight = self.inflight.lock().unwrap();
        let state = inflight.get_mut(&token).unwrap();
        container.map(|dcc| state.container.sync(dcc));
        state.replies += 1;
        if state.replies == state.required {
            // return to client
        } else if state.replies == state.total {
            // remove state
        }
    }

    fn get_local(&self, token: usize, key: &[u8]) {
        let vnode_n = self.dht.key_vnode(key);
        let container = self.vnodes
            .read()
            .unwrap()
            .get(&vnode_n)
            .map(|vn| vn.lock().unwrap().get(key));
        self.process_get(token, container);
    }

    fn get_remote(&self, from: net::SocketAddr, key: &[u8]) {
        unimplemented!();
    }

    pub fn get_remote_callback(&self) {
        unimplemented!()
    }

    pub fn set(&self, token: usize, key: &[u8], value_opt: Option<&[u8]>, vv: VersionVector) {
        let nodes = self.dht.nodes_for_key(key, self.replication_factor);
        let _a = self.inflight
            .lock()
            .unwrap()
            .insert(token,
                    ReqState {
                        required: self.replication_factor / 2 + 1,
                        total: self.replication_factor,
                        replies: 0,
                        container: DottedCausalContainer::new(),
                    });
        debug_assert!(_a.is_none());

        for node in nodes {
            if node == self.dht.node() {
                self.set_local(token, key, value_opt, &vv);
            } else {
                // self.set_remote(token, key, value_opt, &vv);
            }
        }
    }

    fn set_callback(&self, token: usize) {
        let mut inflight = self.inflight.lock().unwrap();
        let state = inflight.get_mut(&token).unwrap();
        state.replies += 1;
        if state.replies == state.required {
            // return to client
        } else if state.replies == state.total {
            // remove state
        }
    }

    fn set_local(&self, token: usize, key: &[u8], value_opt: Option<&[u8]>, vv: &VersionVector) {
        let vnode_n = self.dht.key_vnode(key);
        debug!("set_local {:?}", vnode_n);
        self.vnodes
            .read()
            .unwrap()
            .get(&vnode_n)
            .map(|vn| vn.lock().unwrap().set_local(hash(self.dht.node()), key, value_opt, vv));
        self.set_callback(token);
    }

    pub fn set_remote(&self,
                      from: net::SocketAddr,
                      key: &[u8],
                      dcc: DottedCausalContainer<Vec<u8>>) {
        let vnode_n = self.dht.key_vnode(key);
        self.vnodes
            .read()
            .unwrap()
            .get(&vnode_n)
            .map(move |vn| vn.lock().unwrap().set_remote(hash(from), key, dcc));
    }

    pub fn set_remote_callback(&self, from: net::SocketAddr) {
        unimplemented!()
    }

    pub fn inflight(&self, token: usize) -> Option<ReqState> {
        self.inflight.lock().unwrap().remove(&token)
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
    fn new() -> VNode {
        use std::path::Path;
        use std::fs;
        let _ = fs::remove_dir_all("./vnode_t");
        VNode {
            clock: BitmappedVersionVector::new(),
            peers: Default::default(),
            storage: Storage::open(Path::new("./vnode_t"), true).unwrap(),
            log: Default::default(),
        }
    }

    fn get(&self, key: &[u8]) -> DottedCausalContainer<Vec<u8>> {
        let mut dcc = if let Some(bytes) = self.storage.get_vec(key) {
            bincode_serde::deserialize(&bytes).unwrap()
        } else {
            DottedCausalContainer::new()
        };
        dcc.fill(&self.clock);
        dcc
    }

    fn set_local(&mut self,
                 id: u64,
                 key: &[u8],
                 value_opt: Option<&[u8]>,
                 vv: &VersionVector)
                 -> DottedCausalContainer<Vec<u8>> {
        let mut dcc = self.get(key);
        dcc.discard(vv);
        let dot = self.clock.event(id);
        if let Some(value) = value_opt {
            dcc.add(id, dot, value.into());
        }
        dcc.strip(&self.clock);
        let mut bytes = Vec::new();
        bincode_serde::serialize_into(&mut bytes, &dcc, bincode::SizeLimit::Infinite).unwrap();
        self.storage.set(key, &bytes);
        self.log.insert(dot, key.into());
        dcc
    }

    fn set_remote(&mut self,
                  peer_id: u64,
                  key: &[u8],
                  mut new_dcc: DottedCausalContainer<Vec<u8>>) {
        let old_dcc = self.get(key);
        new_dcc.add_to_bvv(&mut self.clock);
        new_dcc.sync(old_dcc);
        new_dcc.strip(&self.clock);
        let mut bytes = Vec::new();
        bincode_serde::serialize_into(&mut bytes, &new_dcc, bincode::SizeLimit::Infinite).unwrap();
        self.storage.set(key, &bytes);
        self.peers
            .entry(peer_id)
            .or_insert_with(|| VNodePeer::new())
            .log(self.clock.get(peer_id).unwrap().base(), key.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use env_logger;
    use version_vector::VersionVector;

    #[test]
    fn test() {
        let _ = env_logger::init();
        let db = Database::new();
        for i in 0usize..64 {
            db.init_vnode(i);
        }
        db.get(1, b"test");
        assert!(db.inflight(1).unwrap().container.values().next().is_none());

        db.set(1, b"test", Some(b"value1"), VersionVector::new());
        assert!(db.inflight(1).unwrap().container.values().next().is_none());

        db.get(1, b"test");
        assert!(db.inflight(1).unwrap().container.values().eq(vec![b"value1"]));

        db.set(1, b"test", Some(b"value2"), VersionVector::new());
        assert!(db.inflight(1).unwrap().container.values().next().is_none());

        db.get(1, b"test");
        let state = db.inflight(1).unwrap();
        assert!(state.container.values().eq(vec![b"value1", b"value2"]));

        db.set(1,
               b"test",
               Some(b"value12"),
               state.container.version_vector().clone());
        assert!(db.inflight(1).unwrap().container.values().next().is_none());

        db.get(1, b"test");
        assert!(db.inflight(1).unwrap().container.values().eq(vec![b"value12"]));
    }
}
