use std::{thread, net, mem};
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use linear_map::set::LinearSet;
use rand::{self, Rng};
use serde::{Serialize, Deserialize};
use serde_json;
use hash::hash;
use fabric::NodeId;
use etcd;
use utils::{self, GenericError};

pub type DHTChangeFn = Box<Fn() + Send + Sync>;

pub struct DHT<T: Clone + Serialize + Deserialize + Sync + Send + 'static> {
    node: NodeId,
    addr: net::SocketAddr,
    inner: Arc<RwLock<Inner<T>>>,
    thread: Option<thread::JoinHandle<()>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Ring<T: Clone + Serialize + Deserialize + Sync + Send + 'static> {
    vnodes: Vec<(NodeId, T)>,
    #[serde(serialize_with="utils::json_serialize_map", deserialize_with="utils::json_deserialize_map")]
    nodes: HashMap<NodeId, net::SocketAddr>,
    #[serde(serialize_with="utils::json_serialize_map", deserialize_with="utils::json_deserialize_map")]
    pending: HashMap<u16, (NodeId, T)>,
    #[serde(serialize_with="utils::json_serialize_map", deserialize_with="utils::json_deserialize_map")]
    zombie: HashMap<u16, (NodeId, T)>,
}

struct Inner<T: Clone + Serialize + Deserialize + Sync + Send + 'static> {
    ring: Ring<T>,
    ring_version: u64,
    etcd: etcd::Client,
    callback: Option<DHTChangeFn>,
}

impl<T: Clone + Serialize + Deserialize + Sync + Send + 'static> DHT<T> {
    pub fn new(node: NodeId, addr: net::SocketAddr, initial: Option<(T, usize)>) -> DHT<T> {
        let etcd1 = Default::default();
        let etcd2 = Default::default();
        let inner = Arc::new(RwLock::new(Inner {
            ring: Ring {
                vnodes: Default::default(),
                nodes: Default::default(),
                pending: Default::default(),
                zombie: Default::default(),
            },
            ring_version: 0,
            etcd: etcd1,
            callback: None,
        }));
        let mut dht = DHT {
            node: node,
            addr: addr,
            inner: inner.clone(),
            thread: None,
        };
        if let Some((meta, partitions)) = initial {
            dht.reset(meta, partitions);
        } else {
            dht.join();
        }
        dht.thread = Some(thread::spawn(move || Self::run(inner, etcd2)));
        dht
    }

    fn run(inner: Arc<RwLock<Inner<T>>>, etcd: etcd::Client) {
        loop {
            let watch_version = inner.read().unwrap().ring_version + 1;
            // listen for changes
            let r = match etcd.watch("/dht", Some(watch_version), false) {
                Ok(r) => r,
                Err(e) => {
                    warn!("etcd err: {:?}", e);
                    continue;
                }
            };

            // deserialize
            let node = r.node.unwrap();
            let ring = Self::deserialize(&node.value.unwrap()).unwrap();
            let ring_version = node.modified_index.unwrap();
            debug!("new ring version {}", ring_version);
            // update state
            {
                let mut inner = inner.write().unwrap();
                if ring_version > inner.ring_version {
                    inner.ring = ring;
                    inner.ring_version = ring_version;
                }
            }
            // call callback
            inner.read().unwrap().callback.as_ref().map(|f| f());
            trace!("dht callback called");
        }
    }

    fn join(&self) {
        loop {
            let r = self.inner.read().unwrap().etcd.get("/dht", false, false, false).unwrap();
            let node = r.node.unwrap();
            let ring = Self::deserialize(&node.value.unwrap()).unwrap();
            let ring_version = node.modified_index.unwrap();
            let mut new_ring = ring.clone();
            new_ring.nodes.insert(self.node, self.addr);
            if self.propose(ring_version, new_ring).is_ok() {
                break;
            }
        }
    }

    fn reset(&self, meta: T, partitions: usize) {
        let mut inner = self.inner.write().unwrap();
        inner.ring = Ring {
            vnodes: vec![(self.node, meta); partitions],
            nodes: vec![(self.node, self.addr)].into_iter().collect(),
            pending: Default::default(),
            zombie: Default::default(),
        };
        let new = Self::serialize(&inner.ring).unwrap();
        let r = inner.etcd.set("/dht", &new, None).unwrap();
        inner.ring_version = r.node.unwrap().modified_index.unwrap();
    }

    pub fn set_callback(&self, callback: DHTChangeFn) {
        self.inner.write().unwrap().callback = Some(callback);
    }

    pub fn node(&self) -> NodeId {
        self.node
    }

    pub fn key_vnode(&self, key: &[u8]) -> u16 {
        let inner = self.inner.read().unwrap();
        (hash(key) % inner.ring.vnodes.len() as u64) as u16
    }

    pub fn nodes_for_key(&self, key: &[u8], n: usize, include_pending: bool) -> (u16, Vec<NodeId>) {
        let vnode = self.key_vnode(key);
        (vnode, self.nodes_for_vnode(vnode, n, include_pending))
    }

    pub fn nodes_for_vnode(&self, vnode: u16, n: usize, include_pending: bool) -> Vec<NodeId> {
        let mut result = LinearSet::new();
        let inner = self.inner.read().unwrap();
        let ring_len = inner.ring.vnodes.len();
        for i in 0..n {
            let vnode_i = (vnode as usize + i) % ring_len;
            if let Some(p) = inner.ring.vnodes.get(vnode_i) {
                result.insert(p.0);
            }
            if include_pending {
                if let Some(p) = inner.ring.pending.get(&(vnode_i as u16)) {
                    result.insert(p.0);
                }
            }
        }
        result.into()
    }

    pub fn members(&self) -> Vec<(NodeId, net::SocketAddr)> {
        let inner = self.inner.read().unwrap();
        inner.ring.nodes.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    fn ring_clone(&self) -> (Ring<T>, u64) {
        let inner = self.inner.read().unwrap();
        (inner.ring.clone(), inner.ring_version)
    }

    pub fn claim(&self, node: NodeId, meta: T) -> Vec<u16> {
        let (mut ring, ring_version) = self.ring_clone();
        let mut moves = Vec::new();
        let members = self.members().len() + 1;
        let partitions = ring.vnodes.len();
        for _ in 0..(members / partitions) {
            let r = (rand::thread_rng().gen::<usize>() % partitions) as u16;
            ring.pending.insert(r, (node, meta.clone()));
            moves.push(r);
        }
        self.propose(ring_version, ring).unwrap();
        moves
    }

    pub fn add_pending_node(&self, vnode: u16, node: NodeId, meta: T) {
        let (mut ring, ring_version) = self.ring_clone();
        assert!(ring.pending.insert(vnode, (node, meta)).is_none());
        self.propose(ring_version, ring).unwrap();
    }

    pub fn remove_pending_node(&self, vnode: u16, node: NodeId) {
        let (mut ring, ring_version) = self.ring_clone();
        assert!(ring.pending.remove(&vnode).unwrap().0 == node);
        self.propose(ring_version, ring).unwrap();
    }

    pub fn promote_pending_node(&self, vnode: u16, node: NodeId) {
        let (mut ring, ring_version) = self.ring_clone();

        let mut p = ring.pending.remove(&vnode).unwrap();
        assert!(p.0 == node);
        mem::swap(&mut p, &mut ring.vnodes[vnode as usize]);

        self.propose(ring_version, ring).unwrap();
    }

    pub fn get_vnode(&self, vnode: u16) -> (NodeId, Option<NodeId>) {
        let inner = self.inner.read().unwrap();
        (inner.ring.vnodes.get(vnode as usize).map(|a| a.0).unwrap(),
         inner.ring.pending.get(&vnode).map(|a| a.0))
    }

    fn propose(&self, old_version: u64, new_ring: Ring<T>) -> Result<(), GenericError> {
        // self.inner.write().unwrap().ring = new_ring;
        debug!("proposing new ring");
        let new = Self::serialize(&new_ring).unwrap();
        let r = try!(self.inner
            .read()
            .unwrap()
            .etcd
            .compare_and_swap("/dht", &new, None, None, Some(old_version))
            .map_err(|mut e| e.pop().unwrap()));
        let mut inner = self.inner.write().unwrap();
        inner.ring = new_ring;
        inner.ring_version = r.node.unwrap().modified_index.unwrap();
        debug!("proposed new ring version {}", inner.ring_version);
        Ok(())
    }

    fn serialize(ring: &Ring<T>) -> serde_json::Result<String> {
        serde_json::to_string(&ring)
    }

    fn deserialize(json_ring: &str) -> serde_json::Result<Ring<T>> {
        serde_json::from_str(json_ring)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net;

    #[test]
    fn test_new() {
        let node = 0;
        let addr = "127.0.0.1:9000".parse().unwrap();
        let dht = DHT::new(node, addr, Some(((), 256)));
        assert_eq!(dht.nodes_for_key(b"abc", 1, true).1, &[node]);
        assert_eq!(dht.nodes_for_key(b"abc", 3, true).1, &[node]);
        assert_eq!(dht.members(), &[(node, addr)]);
    }
}
