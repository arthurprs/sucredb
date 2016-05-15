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
    storage: Storage,
    peers: LinearMap<net::SocketAddr, VNodePeer>,
}

impl Database {

}

impl VNodePeer {
    
}

impl VNode {

}
