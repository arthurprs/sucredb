use std::{thread, net, cmp, time};
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use linear_map::LinearMap;
use rand::{self, Rng};
use serde::{Serialize, Deserialize};
use serde_yaml;
use hash::hash;
use fabric::NodeId;
use etcd;
use utils::GenericError;

pub type DHTChangeFn = Box<Fn() + Send + Sync>;

pub struct DHT<T: Clone + Serialize + Deserialize + Sync + Send + 'static> {
    node: NodeId,
    addr: net::SocketAddr,
    inner: Arc<RwLock<Inner<T>>>,
    thread: Option<thread::JoinHandle<()>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Ring<T: Clone + Serialize + Deserialize + Sync + Send + 'static> {
    replication_factor: usize,
    vnodes: Vec<LinearMap<NodeId, T>>,
    nodes: HashMap<NodeId, net::SocketAddr>,
    pending: HashMap<u16, LinearMap<NodeId, T>>,
    zombie: HashMap<u16, (NodeId, T)>,
}

struct Inner<T: Clone + Serialize + Deserialize + Sync + Send + 'static> {
    ring: Ring<T>,
    ring_version: u64,
    etcd: etcd::Client,
    callback: Option<DHTChangeFn>,
    running: bool,
}

macro_rules! try_cas {
    ($s: ident, $e: block) => (
        loop {
            let (ring_version, new_ring) = $e;
            match $s.propose(ring_version, new_ring){
                Ok(()) => break,
                Err(e) => {
                    error!("Proposing new ring failed with: {}", e);
                    $s.wait_new_version(ring_version);
                }
            }
        }
    );
}

impl<T: Clone + Serialize + Deserialize + Sync + Send + 'static> DHT<T> {
    pub fn new(replication_factor: usize, node: NodeId, addr: net::SocketAddr,
               initial: Option<(T, usize)>)
               -> DHT<T> {
        let etcd1 = Default::default();
        let etcd2 = Default::default();
        let inner = Arc::new(RwLock::new(Inner {
            ring: Ring {
                replication_factor: replication_factor,
                vnodes: Default::default(),
                nodes: Default::default(),
                pending: Default::default(),
                zombie: Default::default(),
            },
            ring_version: 0,
            etcd: etcd1,
            callback: None,
            running: true,
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
            let watch_version = {
                let inner = inner.read().unwrap();
                if !inner.running {
                    break;
                }
                inner.ring_version + 1
            };
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
            // update state
            {
                let mut inner = inner.write().unwrap();
                if !inner.running {
                    break;
                }
                if ring_version > inner.ring_version {
                    inner.ring = ring;
                    inner.ring_version = ring_version;
                }
            }

            debug!("new ring version {}", ring_version);
            // call callback
            inner.read().unwrap().callback.as_ref().map(|f| f());
            trace!("dht callback called");
        }
        debug!("exiting dht thread");
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
            replication_factor: inner.ring.replication_factor,
            vnodes: vec![[(self.node, meta)].iter().cloned().collect(); partitions],
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

    fn wait_new_version(&self, old_version: u64) {
        while self.inner.read().unwrap().ring_version <= old_version {
            thread::sleep(time::Duration::from_millis(1));
        }
    }

    pub fn key_vnode(&self, key: &[u8]) -> u16 {
        // FIXME: this should be lock free
        let inner = self.inner.read().unwrap();
        (hash(key) % inner.ring.vnodes.len() as u64) as u16
    }

    pub fn nodes_for_vnode(&self, vnode: u16, include_pending: bool) -> Vec<NodeId> {
        let mut result = Vec::new();
        let inner = self.inner.read().unwrap();
        result.extend(inner.ring.vnodes[vnode as usize].iter().map(|(&n, _)| n));
        if include_pending {
            if let Some(pending) = inner.ring.pending.get(&vnode) {
                result.extend(pending.iter().map(|(&n, _)| n));
            }
        }
        result
    }

    pub fn members(&self) -> Vec<(NodeId, net::SocketAddr)> {
        let inner = self.inner.read().unwrap();
        inner.ring.nodes.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    fn ring_clone(&self) -> (Ring<T>, u64) {
        let inner = self.inner.read().unwrap();
        (inner.ring.clone(), inner.ring_version)
    }

    pub fn claim(&self, node: NodeId, meta: T) -> Result<(), GenericError> {
        try_cas!(self, {
            let (mut ring, ring_version) = self.ring_clone();
            let members = self.members().len() + 1;
            let partitions = ring.vnodes.len();
            if members <= ring.replication_factor {
                for i in 0..ring.vnodes.len() as u16 {
                    ring.pending.entry(i).or_insert(Default::default()).insert(node, meta.clone());
                }
            } else {
                for _ in 0..partitions * ring.replication_factor / members {
                    loop {
                        let r = (rand::thread_rng().gen::<usize>() % partitions) as u16;
                        if let Some(pending) = ring.pending.get(&r) {
                            if pending.contains_key(&node) {
                                continue;
                            }
                        }
                        ring.pending
                            .entry(r)
                            .or_insert(Default::default())
                            .insert(node, meta.clone());
                        break;
                    }
                }
            }
            (ring_version, ring)
        });
        Ok(())
    }

    pub fn add_pending_node(&self, vnode: u16, node: NodeId, meta: T) -> Result<(), GenericError> {
        try_cas!(self, {
            let (mut ring, ring_version) = self.ring_clone();
            {
                let pending = ring.pending.entry(vnode).or_insert(Default::default());
                assert!(pending.insert(node, meta.clone()).is_none());
            }
            (ring_version, ring)
        });
        Ok(())
    }

    pub fn remove_pending_node(&self, vnode: u16, node: NodeId) -> Result<(), GenericError> {
        try_cas!(self, {
            let (mut ring, ring_version) = self.ring_clone();
            {
                let pending = ring.pending.entry(vnode).or_insert(Default::default());
                pending.remove(&node).unwrap();
            }
            (ring_version, ring)
        });
        Ok(())
    }

    pub fn promote_pending_node(&self, vnode: u16, node: NodeId) -> Result<(), GenericError> {
        try_cas!(self, {
            let (mut ring, ring_version) = self.ring_clone();
            let new_meta = {
                let pending = ring.pending.entry(vnode).or_insert(Default::default());
                pending.remove(&node).unwrap()
            };

            assert!(ring.vnodes[vnode as usize].insert(node, new_meta).is_none());
            (ring_version, ring)
        });
        Ok(())
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

    fn serialize(ring: &Ring<T>) -> serde_yaml::Result<String> {
        serde_yaml::to_string(&ring)
    }

    fn deserialize(json_ring: &str) -> serde_yaml::Result<Ring<T>> {
        serde_yaml::from_str(json_ring)
    }
}

impl<T: Clone + Serialize + Deserialize + Sync + Send + 'static> Drop for DHT<T> {
    fn drop(&mut self) {
        let _ = self.inner.write().map(|mut inner| inner.running = false);
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
        for rf in 0..3 {
            let dht = DHT::new(rf, node, addr, Some(((), 256)));
            assert_eq!(dht.nodes_for_vnode(0, true), &[node]);
            assert_eq!(dht.nodes_for_vnode(0, true), &[node]);
            assert_eq!(dht.members(), &[(node, addr)]);
        }
    }

    #[test]
    fn test_claim() {}
}
