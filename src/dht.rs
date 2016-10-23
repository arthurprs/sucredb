use std::{thread, net};
use std::time::Duration;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use linear_map::set::LinearSet;
use rand::{thread_rng, Rng};
use serde::{Serialize, Deserialize};
use serde_yaml;
use hash::hash;
use database::{NodeId, VNodeId};
use etcd;
use utils::{IdHashMap, GenericError};

pub type DHTChangeFn = Box<Fn() + Send + Sync>;

pub struct DHT<T: Clone + Serialize + Deserialize + Sync + Send + 'static> {
    node: NodeId,
    cluster: String,
    addr: net::SocketAddr,
    inner: Arc<RwLock<Inner<T>>>,
    thread: Option<thread::JoinHandle<()>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Ring<T: Clone + Serialize + Deserialize + Sync + Send + 'static> {
    replication_factor: usize,
    vnodes: Vec<LinearSet<NodeId>>,
    pending: Vec<LinearSet<NodeId>>,
    zombie: Vec<LinearSet<NodeId>>,
    nodes: IdHashMap<NodeId, (net::SocketAddr, T)>,
}

pub struct RingDescription {
    pub replication_factor: u8,
    pub partitions: u16,
}

impl RingDescription {
    pub fn new(replication_factor: u8, partitions: u16) -> Self {
        RingDescription {
            replication_factor: replication_factor,
            partitions: partitions,
        }
    }
}

impl<T: Clone + Serialize + Deserialize + Sync + Send + 'static> Ring<T> {
    fn leave_node(&mut self, node: NodeId) -> Result<(), GenericError> {
        for i in 0..self.vnodes.len() {
            if self.vnodes[i].remove(&node) {
                self.zombie[i].insert(node);
            }
            self.pending[i].remove(&node);
        }
        Ok(())
    }

    fn join_node(&mut self, node: NodeId, addr: net::SocketAddr,  meta: T) -> Result<(), GenericError> {
        self.nodes.insert(node, (addr, meta));
        Ok(())
    }

    fn remove_node(&mut self, node: NodeId) -> Result<(), GenericError> {
        self.nodes.remove(&node);
        for i in 0..self.vnodes.len() {
            self.vnodes[i].remove(&node);
            self.zombie[i].remove(&node);
            self.pending[i].remove(&node);
        }
        Ok(())
    }

    fn promote_pending_node(&mut self, node: NodeId, vn: VNodeId) -> Result<(), GenericError> {
        self.pending[vn as usize].remove(&node);
        self.vnodes[vn as usize].insert(node);
        Ok(())
    }

    fn stepdown_zombie_node(&mut self, node: NodeId, vn: VNodeId) -> Result<(), GenericError> {
        self.zombie[vn as usize].remove(&node);
        Ok(())
    }

    fn rebalance(&mut self) -> Result<(), GenericError> {
        // fast path when #nodes <= replication factor
        if self.nodes.len() <= self.replication_factor {
            for (vn_num, vn_replicas) in self.vnodes.iter_mut().enumerate() {
                for node in self.nodes.keys().cloned() {
                    if vn_replicas.insert(node) {
                        self.pending[vn_num].insert(node);
                    }
                }
            }
            return Ok(());
        }

        // simple 2-steps eager rebalancing
        // 1. take from who is doing too much work and give to who is doing little
        // 2. assing replicas for under replicated based on who is doing less work
        let mut node_partitions = HashMap::new();
        for (v_num, vn) in self.vnodes.iter().enumerate() {
            for &node in vn {
                node_partitions.entry(node).or_insert(Vec::new()).push(v_num);
            }
        }

        let ppn = self.vnodes.len() * self.replication_factor / self.nodes.len();
        loop {
            let mut with_little: Vec<NodeId> =
                node_partitions.iter().filter(|kv| kv.1.len() < ppn).map(|kv| *kv.0).collect();
            let mut with_much: Vec<NodeId> =
                node_partitions.iter().filter(|kv| kv.1.len() > ppn).map(|kv| *kv.0).collect();
            if with_little.is_empty() || with_much.is_empty() {
                break;
            }
            thread_rng().shuffle(&mut with_little);
            thread_rng().shuffle(&mut with_much);
            while !with_little.is_empty() && !with_much.is_empty() {
                let from = with_much.pop().unwrap();
                let vn = node_partitions.get_mut(&from).unwrap().pop().unwrap();
                self.vnodes[vn].remove(&from);
                self.zombie[vn].insert(from);
                let to = with_little.pop().unwrap();
                node_partitions.entry(to).or_insert(Vec::new()).push(vn);
                self.vnodes[vn].insert(to);
                self.pending[vn].insert(to);
            }
        }

        loop {
            let under_replicated: Vec<usize> = self.vnodes
                .iter()
                .enumerate()
                .filter(|kv| kv.1.len() < self.replication_factor)
                .map(|kv| kv.0)
                .collect();
            if under_replicated.is_empty() {
                break;
            }
            let mut with_little: Vec<NodeId> =
                node_partitions.iter().filter(|kv| kv.1.len() < ppn).map(|kv| *kv.0).collect();
            let mut with_normal: Vec<NodeId> =
                node_partitions.iter().filter(|kv| kv.1.len() == ppn).map(|kv| *kv.0).collect();
            thread_rng().shuffle(&mut with_little);
            thread_rng().shuffle(&mut with_normal);
            for vn in under_replicated {
                let taker = with_little.pop().or_else(|| with_normal.pop()).unwrap();
                node_partitions.get_mut(&taker).unwrap().push(vn);
                self.vnodes[vn].insert(taker);
                self.pending[vn].insert(taker);
            }
        }
        Ok(())
    }
}

struct Inner<T: Clone + Serialize + Deserialize + Sync + Send + 'static> {
    ring: Ring<T>,
    ring_version: u64,
    cluster: String,
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
    pub fn new(node: NodeId, fabric_addr: net::SocketAddr, cluster: &str, etcd: &str, meta: T,
               initial: Option<RingDescription>)
               -> DHT<T> {
        let etcd1 = etcd::Client::new(&[etcd]).unwrap();
        let etcd2 = etcd::Client::new(&[etcd]).unwrap();
        let inner = Arc::new(RwLock::new(Inner {
            ring: Ring {
                replication_factor: Default::default(),
                vnodes: Default::default(),
                nodes: Default::default(),
                pending: Default::default(),
                zombie: Default::default(),
            },
            ring_version: 0,
            cluster: cluster.into(),
            etcd: etcd1,
            callback: None,
            running: true,
        }));
        let mut dht = DHT {
            node: node,
            addr: fabric_addr,
            cluster: cluster.into(),
            inner: inner.clone(),
            thread: None,
        };
        if let Some(description) = initial {
            dht.reset(meta, description.replication_factor, description.partitions);
        } else {
            dht.join(meta);
        }
        dht.thread = Some(thread::Builder::new()
            .name(format!("DHT:{}", node))
            .spawn(move || Self::run(inner, etcd2))
            .unwrap());
        dht
    }

    fn run(inner: Arc<RwLock<Inner<T>>>, etcd: etcd::Client) {
        let cluster_key = format!("/{}/dht", inner.read().unwrap().cluster);
        loop {
            let watch_version = {
                let inner = inner.read().unwrap();
                if !inner.running {
                    break;
                }
                inner.ring_version + 1
            };
            // listen for changes
            let r = match etcd.watch(&cluster_key, Some(watch_version), false) {
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

    fn join(&self, meta: T) {
        let cluster_key = format!("/{}/dht", self.inner.read().unwrap().cluster);
        loop {
            let r = self.inner.read().unwrap().etcd.get(&cluster_key, false, false, false).unwrap();
            let node = r.node.unwrap();
            let ring = Self::deserialize(&node.value.unwrap()).unwrap();
            let ring_version = node.modified_index.unwrap();
            assert!(ring.vnodes.len().is_power_of_two());
            let mut new_ring = ring.clone();
            new_ring.nodes.insert(self.node, (self.addr, meta.clone()));
            if self.propose(ring_version, new_ring, true).is_ok() {
                break;
            }
        }
    }

    fn reset(&self, meta: T, replication_factor: u8, partitions: u16) {
        assert!(partitions.is_power_of_two());
        assert!(replication_factor > 0);
        let mut inner = self.inner.write().unwrap();
        let cluster_key = format!("/{}/dht", inner.cluster);
        inner.ring = Ring {
            replication_factor: replication_factor as usize,
            vnodes: vec![[self.node].iter().cloned().collect(); partitions as usize],
            pending: vec![Default::default(); partitions as usize],
            zombie: vec![Default::default(); partitions as usize],
            nodes: vec![(self.node, (self.addr, meta))].into_iter().collect(),
        };
        let new = Self::serialize(&inner.ring).unwrap();
        let r = inner.etcd.set(&cluster_key, &new, None).unwrap();
        inner.ring_version = r.node.unwrap().modified_index.unwrap();
    }

    pub fn set_callback(&self, callback: DHTChangeFn) {
        self.inner.write().unwrap().callback = Some(callback);
    }

    fn wait_new_version(&self, old_version: u64) {
        while self.inner.read().unwrap().ring_version <= old_version {
            thread::sleep(Duration::from_millis(1));
        }
    }

    pub fn node(&self) -> NodeId {
        self.node
    }

    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    pub fn partitions(&self) -> usize {
        self.inner.read().unwrap().ring.vnodes.len()
    }

    pub fn replication_factor(&self) -> usize {
        self.inner.read().unwrap().ring.replication_factor as usize
    }

    pub fn key_vnode(&self, key: &[u8]) -> VNodeId {
        // FIXME: this should be lock free
        let inner = self.inner.read().unwrap();
        debug!("h: {} vnode: {}", hash(key), (hash(key) % inner.ring.vnodes.len() as u64) as VNodeId);
        (hash(key) % inner.ring.vnodes.len() as u64) as VNodeId
    }

    pub fn vnodes_for_node(&self, node: NodeId) -> (Vec<VNodeId>, Vec<VNodeId>) {
        let mut insync = Vec::new();
        let mut pending = Vec::new();
        let inner = self.inner.read().unwrap();
        for (i, (v, p)) in inner.ring.vnodes.iter().zip(inner.ring.pending.iter()).enumerate() {
            if v.contains(&node) {
                insync.push(i as VNodeId);
            }
            if p.contains(&node) {
                pending.push(i as VNodeId);
            }
        }
        (insync, pending)
    }

    // TODO: split into read_ and write_
    pub fn nodes_for_vnode(&self, vnode: VNodeId, include_pending: bool) -> Vec<NodeId> {
        // FIXME: this shouldn't alloc
        let mut result = Vec::new();
        let inner = self.inner.read().unwrap();
        result.extend(&inner.ring.vnodes[vnode as usize]);
        result.extend(&inner.ring.zombie[vnode as usize]);
        if include_pending {
            result.extend(&inner.ring.pending[vnode as usize]);
        }
        result
    }

    pub fn members(&self) -> HashMap<NodeId, net::SocketAddr> {
        let inner = self.inner.read().unwrap();
        inner.ring.nodes.iter().map(|(k, v)| (k.clone(), v.0.clone())).collect()
    }

    fn ring_clone(&self) -> (Ring<T>, u64) {
        let inner = self.inner.read().unwrap();
        (inner.ring.clone(), inner.ring_version)
    }

    pub fn rebalance(&self) -> Result<(), GenericError> {
        try_cas!(self, {
            let (mut ring, ring_version) = self.ring_clone();
            try!(ring.rebalance());
            (ring_version, ring)
        });
        Ok(())
    }

    pub fn add_pending_node(&self, vnode: VNodeId, node: NodeId) -> Result<(), GenericError> {
        try_cas!(self, {
            let (mut ring, ring_version) = self.ring_clone();
            {
                let pending = &mut ring.pending[vnode as usize];
                assert!(pending.insert(node));
            }
            (ring_version, ring)
        });
        Ok(())
    }

    pub fn remove_node(&self, node: NodeId) -> Result<(), GenericError> {
        try_cas!(self, {
            let (mut ring, ring_version) = self.ring_clone();
            try!(ring.remove_node(node));
            (ring_version, ring)
        });
        Ok(())
    }

    pub fn leave_node(&self, node: NodeId) -> Result<(), GenericError> {
        try_cas!(self, {
            let (mut ring, ring_version) = self.ring_clone();
            try!(ring.leave_node(node));
            (ring_version, ring)
        });
        Ok(())
    }

    pub fn promote_pending_node(&self, node: NodeId, vnode: VNodeId) -> Result<(), GenericError> {
        try_cas!(self, {
            let (mut ring, ring_version) = self.ring_clone();
            try!(ring.promote_pending_node(node, vnode));
            (ring_version, ring)
        });
        Ok(())
    }

    pub fn stepdown_zombie_node(&self, node: NodeId, vnode: VNodeId) -> Result<(), GenericError> {
        try_cas!(self, {
            let (mut ring, ring_version) = self.ring_clone();
            try!(ring.stepdown_zombie_node(node, vnode));
            (ring_version, ring)
        });
        Ok(())
    }

    fn propose(&self, old_version: u64, new_ring: Ring<T>, update: bool)
               -> Result<(), etcd::Error> {
        debug!("Proposing new ring against version {}", old_version);
        let cluster_key = format!("/{}/dht", self.inner.read().unwrap().cluster);
        let new = Self::serialize(&new_ring).unwrap();
        let r = try!(self.inner
            .read()
            .unwrap()
            .etcd
            .compare_and_swap(&cluster_key, &new, None, None, Some(old_version))
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
    use config;

    #[test]
    fn test_new() {
        let node = 0;
        let addr = "127.0.0.1:9000".parse().unwrap();
        for rf in 1..4 {
            let dht = DHT::new(node,
                               addr,
                               "test",
                               config::DEFAULT_ETCD_ADDR,
                               (),
                               Some(RingDescription::new(rf, 256)));
            assert_eq!(dht.nodes_for_vnode(0, true), &[node]);
            assert_eq!(dht.nodes_for_vnode(0, true), &[node]);
            assert_eq!(dht.members(), [(node, addr)].iter().cloned().collect::<HashMap<_, _>>());
        }
    }

    #[test]
    fn test_claim() {}
}
