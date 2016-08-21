use std::{thread, net, time};
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use linear_map::LinearMap;
use rand::{thread_rng, Rng};
use serde::{Serialize, Deserialize};
use serde_yaml;
use hash::hash;
use database::{NodeId, VNodeId};
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
    pending: Vec<LinearMap<NodeId, T>>,
    zombie: Vec<LinearMap<NodeId, T>>,
    nodes: HashMap<NodeId, net::SocketAddr>,
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
            match $s.propose(ring_version, new_ring, false) {
                Ok(()) => break,
                Err(etcd::Error::Api(ref e)) if e.error_code == 101 => {
                    warn!("Proposing new ring conflicted at version {}", ring_version);
                    $s.wait_new_version(ring_version);
                }
                Err(e) => {
                    error!("Proposing new ring failed with: {}", e);
                    return Err(e.into());
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
        dht.thread = Some(thread::Builder::new()
            .name(format!("DHT:{}", node))
            .spawn(move || Self::run(inner, etcd2))
            .unwrap());
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

            debug!("Callback with new ring version {}", ring_version);
            // call callback

            let callback = inner.write().unwrap().callback.take().unwrap();
            callback();
            inner.write().unwrap().callback = Some(callback);
            trace!("dht callback returned");
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
            if self.propose(ring_version, new_ring, true).is_ok() {
                break;
            }
        }
    }

    fn reset(&self, meta: T, partitions: usize) {
        let mut inner = self.inner.write().unwrap();
        inner.ring = Ring {
            replication_factor: inner.ring.replication_factor,
            vnodes: vec![[(self.node, meta)].iter().cloned().collect(); partitions],
            pending: vec![Default::default(); partitions],
            zombie: vec![Default::default(); partitions],
            nodes: vec![(self.node, self.addr)].into_iter().collect(),
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

    pub fn partitions(&self) -> usize {
        self.inner.read().unwrap().ring.vnodes.len()
    }

    pub fn replication_factor(&self) -> usize {
        self.inner.read().unwrap().ring.replication_factor
    }

    fn wait_new_version(&self, old_version: u64) {
        while self.inner.read().unwrap().ring_version <= old_version {
            thread::sleep(time::Duration::from_millis(1));
        }
    }

    pub fn key_vnode(&self, key: &[u8]) -> VNodeId {
        // FIXME: this should be lock free
        let inner = self.inner.read().unwrap();
        (hash(key) % inner.ring.vnodes.len() as u64) as VNodeId
    }

    pub fn vnodes_for_node(&self, node: NodeId) -> (Vec<VNodeId>, Vec<VNodeId>) {
        let mut insync = Vec::new();
        let mut pending = Vec::new();
        let inner = self.inner.read().unwrap();
        for (i, (v, p)) in inner.ring.vnodes.iter().zip(inner.ring.pending.iter()).enumerate() {
            if v.contains_key(&node) {
                insync.push(i as VNodeId);
            }
            if p.contains_key(&node) {
                pending.push(i as VNodeId);
            }
        }
        (insync, pending)
    }

    pub fn nodes_for_vnode(&self, vnode: VNodeId, include_pending: bool) -> Vec<NodeId> {
        // FIXME: this shouldn't alloc
        let mut result = Vec::new();
        let inner = self.inner.read().unwrap();
        result.extend(inner.ring.vnodes[vnode as usize].keys());
        if include_pending {
            result.extend(inner.ring.pending[vnode as usize].keys());
        }
        result
    }

    pub fn members(&self) -> HashMap<NodeId, net::SocketAddr> {
        let inner = self.inner.read().unwrap();
        inner.ring.nodes.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    fn ring_clone(&self) -> (Ring<T>, u64) {
        let inner = self.inner.read().unwrap();
        (inner.ring.clone(), inner.ring_version)
    }

    pub fn claim(&self, node: NodeId, meta: T) -> Result<Vec<VNodeId>, GenericError> {
        let mut changes = Vec::new();
        try_cas!(self, {
            changes.clear();
            let (mut ring, ring_version) = self.ring_clone();
            let members = self.members().len() + 1;
            let partitions = ring.vnodes.len();
            if members <= ring.replication_factor {
                for i in 0..ring.vnodes.len() {
                    ring.pending[i].insert(node, meta.clone());
                    changes.push(i as VNodeId);
                }
            } else {
                for _ in 0..partitions * ring.replication_factor / members {
                    loop {
                        let r = thread_rng().gen::<usize>() % partitions;
                        if let Some(pending) = ring.pending.get(r) {
                            if pending.contains_key(&node) {
                                continue;
                            }
                        }
                        ring.pending[r].insert(node, meta.clone());
                        changes.push(r as VNodeId);
                        break;
                    }
                }
            }
            (ring_version, ring)
        });
        Ok(changes)
    }

    pub fn add_pending_node(&self, vnode: VNodeId, node: NodeId, meta: T) -> Result<(), GenericError> {
        try_cas!(self, {
            let (mut ring, ring_version) = self.ring_clone();
            {
                let pending = &mut ring.pending[vnode as usize];
                assert!(pending.insert(node, meta.clone()).is_none());
            }
            (ring_version, ring)
        });
        Ok(())
    }

    pub fn remove_pending_node(&self, vnode: VNodeId, node: NodeId) -> Result<(), GenericError> {
        try_cas!(self, {
            let (mut ring, ring_version) = self.ring_clone();
            {
                let pending = &mut ring.pending[vnode as usize];
                pending.remove(&node).unwrap();
            }
            (ring_version, ring)
        });
        Ok(())
    }

    pub fn promote_pending_node(&self, vnode: VNodeId, node: NodeId) -> Result<(), GenericError> {
        try_cas!(self, {
            let (mut ring, ring_version) = self.ring_clone();
            let new_meta = {
                let pending = &mut ring.pending[vnode as usize];
                pending.remove(&node).unwrap()
            };

            assert!(ring.vnodes[vnode as usize].insert(node, new_meta).is_none());
            (ring_version, ring)
        });
        Ok(())
    }

    fn propose(&self, old_version: u64, new_ring: Ring<T>, update: bool) -> Result<(), etcd::Error> {
        // self.inner.write().unwrap().ring = new_ring;
        debug!("Proposing new ring against version {}", old_version);
        let new = Self::serialize(&new_ring).unwrap();
        let r = try!(self.inner
            .read()
            .unwrap()
            .etcd
            .compare_and_swap("/dht", &new, None, None, Some(old_version))
            .map_err(|mut e| e.pop().unwrap()));
        if update {
            let mut inner = self.inner.write().unwrap();
            inner.ring = new_ring;
            inner.ring_version = r.node.unwrap().modified_index.unwrap();
            debug!("Updated ring to version {}", inner.ring_version);
        }
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
    use std::collections::HashMap;
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
            assert_eq!(dht.members(), [(node, addr)].iter().cloned().collect::<HashMap<_, _>>());
        }
    }

    #[test]
    fn test_claim() {}
}
