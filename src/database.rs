use version_vector;
use storage::Storage;
use std::net;
use linear_map::LinearMap;
use std::collections::{HashMap, BTreeMap};

struct Database {
    vnodes: HashMap<usize, VNode>,
}

struct VNodePeer {
    knowledge: u64,
    log: BTreeMap<u64, Vec<u8>>,
}

struct VNode {
    clock: version_vector::BitmappedVersionVector,
    log: BTreeMap<u64, Vec<u8>>,
    peers: LinearMap<net::SocketAddr, VNodePeer>,
    storage: Storage,
}

impl Database {}

impl VNodePeer {
    fn advance_knowledge(&mut self, until: u64) {
        debug_assert!(until > self.knowledge);
        self.knowledge = until;
    }

    fn log(&mut self, dot: u64, key: Vec<u8>) {
        let prev = self.log.insert(dot, key);
        debug_assert!(prev.is_none());
    }
}

impl VNode {}
