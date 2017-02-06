use std::{fmt, thread, net, time};
use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, BTreeMap};
use linear_map::set::LinearSet;
use rand::{thread_rng, Rng};
use serde::{Serialize, Deserialize};
use serde_yaml;
use hash::{hash_slot, HASH_SLOTS};
use database::{NodeId, VNodeId};
use etcd;
use utils::{IdHashMap, GenericError};

pub type DHTChangeFn = Box<FnMut() + Send>;
pub trait Metadata
    : Clone + Serialize + Deserialize + Send + fmt::Debug + 'static {
}

impl<T: Clone + Serialize + Deserialize + Send + fmt::Debug + 'static> Metadata for T {}

pub struct DHT<T: Metadata> {
    node: NodeId,
    addr: net::SocketAddr,
    slots_per_partition: u16,
    partitions: usize,
    replication_factor: usize,
    cluster: String,
    inner: Arc<Mutex<Inner<T>>>,
    etcd_client: etcd::Client,
    thread: Option<thread::JoinHandle<()>>,
}

struct Inner<T: Metadata> {
    ring: Ring<T>,
    ring_version: u64,
    cluster: String,
    callback: Option<DHTChangeFn>,
    running: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Ring<T: Metadata> {
    replication_factor: usize,
    vnodes: Vec<LinearSet<NodeId>>,
    pending: Vec<LinearSet<NodeId>>,
    retiring: Vec<LinearSet<NodeId>>,
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

impl<T: Metadata> Ring<T> {
    fn new(node: NodeId, addr: net::SocketAddr, meta: T, partitions: u16, replication_factor: u8)
           -> Self {
        Ring {
            replication_factor: replication_factor as usize,
            vnodes: vec![[node].iter().cloned().collect(); partitions as usize],
            pending: vec![Default::default(); partitions as usize],
            retiring: vec![Default::default(); partitions as usize],
            nodes: vec![(node, (addr, meta))].into_iter().collect(),
        }
    }

    fn leave_node(&mut self, node: NodeId) -> Result<(), GenericError> {
        for i in 0..self.vnodes.len() {
            if self.vnodes[i].remove(&node) {
                assert!(self.retiring[i].insert(node));
                assert!(!self.pending[i].remove(&node));
            } else {
                assert!(self.pending[i].remove(&node));
            }
        }
        Ok(())
    }

    fn join_node(&mut self, node: NodeId, addr: net::SocketAddr, meta: T)
                 -> Result<(), GenericError> {
        self.nodes.insert(node, (addr, meta));
        Ok(())
    }

    fn remove_node(&mut self, node: NodeId) -> Result<(), GenericError> {
        self.nodes.remove(&node);
        for i in 0..self.vnodes.len() {
            self.vnodes[i].remove(&node);
            self.retiring[i].remove(&node);
            self.pending[i].remove(&node);
        }
        Ok(())
    }

    fn promote_pending_node(&mut self, node: NodeId, vn: VNodeId) -> Result<(), GenericError> {
        if !self.pending[vn as usize].remove(&node) {
            return Err(format!("{} is not in pending[{}]", node, vn).into());
        }
        if !self.vnodes[vn as usize].insert(node) {
            return Err(format!("{} is already in vnodes[{}]", node, vn).into());
        }
        if self.pending[vn as usize].is_empty() {
            self.retiring[vn as usize].clear();
        }
        Ok(())
    }

    fn replace(&mut self, old: NodeId, node: NodeId, addr: net::SocketAddr, meta: T)
               -> Result<(), GenericError> {
        if self.nodes.insert(node, (addr, meta)).is_none() {
            return Err(format!("{} is already in the cluster", node).into());
        }
        if self.nodes.remove(&old).is_some() {
            let iter = self.vnodes
                .iter_mut()
                .chain(self.pending.iter_mut().chain(self.retiring.iter_mut()));
            for v in iter {
                if v.remove(&old) {
                    assert!(v.insert(node));
                }
            }
        } else {
            return Err(format!("{} is not in the cluster", node).into());
        }
        Ok(())
    }

    fn rebalance(&mut self) -> Result<(), GenericError> {
        let simulations = 100;
        let start = time::Instant::now();
        for retry in 0..simulations {
            let mut cloned = self.clone();
            let result = cloned.try_rebalance();
            if let Ok(cost) = result {
                debug!("found a rebalance solution in simulation #{} with cost {} took: {:?}",
                       retry,
                       cost,
                       time::Instant::elapsed(&start));
                *self = cloned;
                return Ok(());
            }
        }
        panic!("Can't find a solution after {} simulations: {:?}", simulations, self);
    }

    fn try_rebalance(&mut self) -> Result<usize, GenericError> {
        // TODO: Vec -> VecDeque to avoid the O(N) insert(0, ..)
        // TODO: return a cost so caller can select the best plan out of N
        let mut rng = thread_rng();
        // special case when #nodes <= replication factor
        if self.nodes.len() <= self.replication_factor {
            for (vn_num, vn_replicas) in self.vnodes.iter().enumerate() {
                for node in self.nodes.keys().cloned() {
                    if !vn_replicas.contains(&node) {
                        self.pending[vn_num].insert(node);
                    }
                }
            }
            return Ok(0);
        }

        // simple 2-steps eager rebalancing
        // 1. take from who is doing too much work and give to who is doing little
        // 2. assing replicas for under replicated vnodes based on who is doing less work
        let mut node_partitions = HashMap::new();
        for &node in self.nodes.keys() {
            node_partitions.insert(node, Vec::new());
        }
        for (v_num, vn) in self.vnodes.iter().enumerate() {
            for node in vn {
                node_partitions.get_mut(node).unwrap().push(v_num);
            }
        }
        for partitions in node_partitions.values_mut() {
            rng.shuffle(partitions);
        }

        // partitions per node
        let ppn = self.vnodes.len() * self.replication_factor / self.nodes.len();
        let ppn_rest = self.vnodes.len() * self.replication_factor % self.nodes.len();
        if ppn == 0 {
            panic!("partitions per node is 0!");
        }

        let mut with_much: Vec<NodeId> = Vec::new();
        let mut with_little: Vec<NodeId> = Vec::new();
        loop {
            if with_much.is_empty() {
                with_much.extend(node_partitions.iter()
                    .filter(|&(_, p)| p.len() > ppn)
                    .map(|(&n, _)| n));
                if with_much.is_empty() {
                    break;
                }
                rng.shuffle(&mut with_much);
            }
            if with_little.is_empty() {
                with_little.extend(node_partitions.iter()
                    .filter(|&(_, p)| p.len() < ppn)
                    .map(|(&n, _)| n));
                if with_little.is_empty() {
                    break;
                }
                rng.shuffle(&mut with_little);
            }

            while !with_little.is_empty() && !with_much.is_empty() {
                let from = with_much.pop().unwrap();
                let to = with_little.pop().unwrap();
                let vn = node_partitions.get_mut(&from).unwrap().pop().unwrap();
                if !self.vnodes[vn].contains(&to) && self.pending[vn].insert(to) {
                    assert!(self.vnodes[vn].remove(&from));
                    assert!(self.retiring[vn].insert(from));
                    node_partitions.get_mut(&to).unwrap().push(vn);
                } else {
                    with_much.insert(0, from);
                    with_little.insert(0, to);
                    node_partitions.get_mut(&from).unwrap().insert(0, vn);
                }
            }
        }

        with_little.clear();
        with_little.extend(node_partitions.iter()
            .filter(|&(_, p)| p.len() < ppn)
            .flat_map(|(&n, p)| (p.len()..ppn).map(move |_| n)));
        rng.shuffle(&mut with_little);

        with_much.clear();
        with_much.extend(node_partitions.iter()
            .filter(|&(_, p)| p.len() >= ppn)
            .map(|(&n, _)| n));
        rng.shuffle(&mut with_much);

        let mut extra_nodes: Vec<NodeId> = Vec::new();
        extra_nodes.extend(self.nodes.keys().cloned());
        rng.shuffle(&mut extra_nodes);
        extra_nodes.truncate(ppn_rest);

        let mut under_replicated: Vec<usize> = self.vnodes
            .iter()
            .zip(self.pending.iter())
            .enumerate()
            .filter(|&(_, (v, p))| self.replication_factor > v.len() + p.len())
            .flat_map(|(vn, (v, p))| {
                (0..self.replication_factor - v.len() - p.len()).map(move |_| vn)
            })
            .collect();
        rng.shuffle(&mut under_replicated);

        'next: while let Some(vn) = under_replicated.pop() {
            for _ in 0..with_little.len() {
                if let Some(taker) = with_little.pop() {
                    assert!(!self.retiring[vn].contains(&taker));
                    if !self.vnodes[vn].contains(&taker) && self.pending[vn].insert(taker) {
                        node_partitions.get_mut(&taker).unwrap().push(vn);
                        continue 'next;
                    } else {
                        with_little.insert(0, taker);
                    }
                }
            }
            for _ in 0..with_much.len() {
                if let Some(taker) = with_much.pop() {
                    if self.retiring[vn].remove(&taker) {
                        assert!(!self.pending[vn].contains(&taker));
                        assert!(self.vnodes[vn].insert(taker));
                        node_partitions.get_mut(&taker).unwrap().push(vn);
                        continue 'next;
                    } else if !self.vnodes[vn].contains(&taker) &&
                              !self.retiring[vn].contains(&taker) &&
                              self.pending[vn].insert(taker) {
                        node_partitions.get_mut(&taker).unwrap().push(vn);
                        continue 'next;
                    } else {
                        with_much.insert(0, taker);
                    }
                }
            }
            for _ in 0..extra_nodes.len() {
                if let Some(taker) = extra_nodes.pop() {
                    if self.retiring[vn].remove(&taker) {
                        assert!(!self.pending[vn].contains(&taker));
                        assert!(self.vnodes[vn].insert(taker));
                        node_partitions.get_mut(&taker).unwrap().push(vn);
                        continue 'next;
                    } else if !self.vnodes[vn].contains(&taker) &&
                              !self.retiring[vn].contains(&taker) &&
                              self.pending[vn].insert(taker) {
                        node_partitions.get_mut(&taker).unwrap().push(vn);
                        continue 'next;
                    } else {
                        extra_nodes.insert(0, taker);
                    }
                }
            }

            return Err(format!("Cant find replica for vnode {:?}", vn).into());
        }
        try!(self.is_valid());

        let mut cost = 0;
        cost += node_partitions.values()
            .map(|p| (ppn as isize - p.len() as isize).abs() as usize)
            .sum::<usize>();
        cost += self.pending.iter().map(|p| p.len()).sum::<usize>();

        Ok(cost)
    }

    #[cfg(test)]
    fn finish_rebalance(&mut self) {
        self.is_valid().unwrap();
        for retiring in &mut self.retiring {
            retiring.clear();
        }
        for (vn, pendings) in self.pending.iter_mut().enumerate() {
            for pending in pendings.drain() {
                assert!(self.vnodes[vn].insert(pending));
            }
        }
        self.is_valid().unwrap();
    }

    fn is_valid(&self) -> Result<(), GenericError> {
        if self.nodes.len() <= self.replication_factor {
            for (vn, (v, p)) in self.vnodes.iter().zip(self.pending.iter()).enumerate() {
                if v.len() + p.len() != self.nodes.len() {
                    return Err(format!("vn {} has {:?}{:?} replicas", vn, v, p).into());
                }
            }
        } else {
            for (vn, (v, p)) in self.vnodes.iter().zip(self.pending.iter()).enumerate() {
                if v.len() + p.len() != self.replication_factor {
                    return Err(format!("vn {} has {:?}{:?} replicas", vn, v, p).into());
                }
            }
        }
        Ok(())
    }
}

// TODO: move to a member fn on DHT
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

impl<T: Metadata> DHT<T> {
    pub fn new(node: NodeId, fabric_addr: net::SocketAddr, cluster: &str, etcd: &str, meta: T,
               initial: Option<RingDescription>, old_node: Option<NodeId>)
               -> DHT<T> {
        let etcd_client1 = etcd::Client::new(&[etcd]).unwrap();
        let etcd_client2 = etcd::Client::new(&[etcd]).unwrap();
        let inner = Arc::new(Mutex::new(Inner {
            ring: Ring {
                replication_factor: Default::default(),
                vnodes: Default::default(),
                nodes: Default::default(),
                pending: Default::default(),
                retiring: Default::default(),
            },
            ring_version: 0,
            cluster: cluster.into(),
            callback: None,
            running: true,
        }));
        let mut dht = DHT {
            node: node,
            addr: fabric_addr,
            cluster: cluster.into(),
            inner: inner.clone(),
            thread: None,
            slots_per_partition: 0,
            partitions: 0,
            replication_factor: 0,
            etcd_client: etcd_client1,
        };
        if let Some(description) = initial {
            dht.reset(meta, description.replication_factor, description.partitions);
        } else if let Some(old) = old_node {
            dht.replace(old, meta);
        } else {
            dht.join(meta);
        }
        dht.partitions = inner.lock().unwrap().ring.vnodes.len();
        dht.slots_per_partition = HASH_SLOTS / dht.partitions as u16;
        dht.replication_factor = inner.lock().unwrap().ring.replication_factor;
        dht.thread = Some(thread::Builder::new()
            .name(format!("DHT:{}", node))
            .spawn(move || Self::run(inner, etcd_client2))
            .unwrap());
        dht
    }

    fn run(inner: Arc<Mutex<Inner<T>>>, etcd: etcd::Client) {
        let cluster_key = format!("/{}/dht", inner.lock().unwrap().cluster);
        loop {
            let watch_version = {
                let inner = inner.lock().unwrap();
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
            debug!("Callback with new ring {:?} version {}", ring, ring_version);
            // update state
            {
                let mut inner = inner.lock().unwrap();
                if !inner.running {
                    break;
                }
                if ring_version > inner.ring_version {
                    inner.ring = ring;
                    inner.ring_version = ring_version;
                }
            }

            // call callback
            inner.lock().unwrap().callback.as_mut().unwrap()();
            trace!("dht callback returned");
        }
        debug!("exiting dht thread");
    }

    fn refresh_ring(&self) {
        let cluster_key = format!("/{}/dht", self.cluster);
        let r = self.etcd_client.get(&cluster_key, false, false, false).unwrap();
        let node = r.node.unwrap();
        let mut inner = self.inner.lock().unwrap();
        inner.ring = Self::deserialize(&node.value.unwrap()).unwrap();
        inner.ring_version = node.modified_index.unwrap();
    }

    fn join(&self, meta: T) {
        self.refresh_ring();
        let f = move || -> Result<(), GenericError> {
            try_cas!(self, {
                let (mut ring, ring_version) = self.ring_clone();
                try!(ring.join_node(self.node, self.addr, meta.clone()));
                (ring_version, ring)
            });
            Ok(())
        };
        f().unwrap();
    }

    pub fn replace(&self, old: NodeId, meta: T) {
        self.refresh_ring();
        let f = move || -> Result<(), GenericError> {
            try_cas!(self, {
                let (mut ring, ring_version) = self.ring_clone();
                try!(ring.replace(old, self.node, self.addr, meta.clone()));
                (ring_version, ring)
            });
            Ok(())
        };
        f().unwrap();
    }

    fn reset(&self, meta: T, replication_factor: u8, partitions: u16) {
        assert!(partitions.is_power_of_two());
        assert!(replication_factor > 0);
        let cluster_key = format!("/{}/dht", self.cluster);
        let mut inner = self.inner.lock().unwrap();
        inner.ring = Ring::new(self.node, self.addr, meta, partitions, replication_factor);
        let new = Self::serialize(&inner.ring).unwrap();
        let r = self.etcd_client.set(&cluster_key, &new, None).unwrap();
        inner.ring_version = r.node.unwrap().modified_index.unwrap();
    }

    pub fn set_callback(&self, callback: DHTChangeFn) {
        self.inner.lock().unwrap().callback = Some(callback);
    }

    fn wait_new_version(&self, old_version: u64) {
        while self.inner.lock().unwrap().ring_version <= old_version {
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
        self.partitions
    }

    pub fn replication_factor(&self) -> usize {
        self.replication_factor
    }

    pub fn key_vnode(&self, key: &[u8]) -> VNodeId {
        // use / instead of % to get continuous hash slots
        // for each vnode
        (hash_slot(key) / self.slots_per_partition) as VNodeId
    }

    pub fn vnodes_for_node(&self, node: NodeId) -> (Vec<VNodeId>, Vec<VNodeId>) {
        let mut insync = Vec::new();
        let mut pending = Vec::new();
        let inner = self.inner.lock().unwrap();
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
    pub fn nodes_for_vnode(&self, vnode: VNodeId, include_pending: bool, include_retiring: bool)
                           -> Vec<NodeId> {
        // FIXME: this shouldn't alloc
        let mut result = Vec::new();
        let inner = self.inner.lock().unwrap();
        result.extend(&inner.ring.vnodes[vnode as usize]);
        if include_pending {
            result.extend(&inner.ring.pending[vnode as usize]);
        }
        if include_retiring {
            result.extend(&inner.ring.retiring[vnode as usize]);
        }
        result
    }

    pub fn write_members_for_vnode(&self, vnode: VNodeId) -> Vec<(NodeId, (net::SocketAddr, T))> {
        // FIXME: this shouldn't alloc
        let mut result = Vec::new();
        let inner = self.inner.lock().unwrap();
        result.extend(inner.ring.vnodes[vnode as usize]
            .iter()
            .chain(inner.ring.retiring[vnode as usize].iter())
            .map(|n| (*n, inner.ring.nodes.get(n).cloned().unwrap())));
        result
    }

    pub fn members(&self) -> HashMap<NodeId, net::SocketAddr> {
        let inner = self.inner.lock().unwrap();
        inner.ring.nodes.iter().map(|(k, v)| (k.clone(), v.0.clone())).collect()
    }

    pub fn slots(&self) -> BTreeMap<(u16, u16), Vec<(NodeId, (net::SocketAddr, T))>> {
        let slots_per_partition = HASH_SLOTS / self.partitions() as u16;
        let mut result = BTreeMap::new();
        let inner = self.inner.lock().unwrap();
        for (hi, ((v, p), r)) in
            inner.ring
                .vnodes
                .iter()
                .zip(inner.ring.pending.iter())
                .zip(inner.ring.retiring.iter())
                .enumerate() {
            let members: Vec<_> = v.iter()
                .chain(p.iter())
                .chain(r.iter())
                .map(|n| (*n, inner.ring.nodes.get(n).cloned().unwrap()))
                .collect();
            let hi = hi as u16;
            result.insert((hi * slots_per_partition, (hi + 1) * slots_per_partition - 1), members);
        }
        result
    }

    fn ring_clone(&self) -> (Ring<T>, u64) {
        let inner = self.inner.lock().unwrap();
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

    fn propose(&self, old_version: u64, new_ring: Ring<T>, update: bool)
               -> Result<(), etcd::Error> {
        debug!("Proposing new ring against version {}", old_version);
        let cluster_key = format!("/{}/dht", self.cluster);
        let new = Self::serialize(&new_ring).unwrap();
        let r = try!(self.etcd_client
            .compare_and_swap(&cluster_key, &new, None, None, Some(old_version))
            .map_err(|mut e| e.pop().unwrap()));
        if update {
            let mut inner = self.inner.lock().unwrap();
            inner.ring = new_ring;
            inner.ring_version = r.node.unwrap().modified_index.unwrap();
            debug!("Updated ring to {:?} version {}", inner.ring, inner.ring_version);
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

impl<T: Metadata> Drop for DHT<T> {
    fn drop(&mut self) {
        let _ = self.inner.lock().map(|mut inner| inner.running = false);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use super::*;
    use std::net;
    use config;
    use rand::{self, Rng, thread_rng};
    use env_logger;

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
                               Some(RingDescription::new(rf, 256)),
                               None);
            assert_eq!(dht.nodes_for_vnode(0, true, false), &[node]);
            assert_eq!(dht.nodes_for_vnode(0, false, false), &[node]);
            assert_eq!(dht.nodes_for_vnode(0, false, false), &[node]);
            assert_eq!(dht.nodes_for_vnode(0, true, true), &[node]);
            assert_eq!(dht.members(), [(node, addr)].iter().cloned().collect::<HashMap<_, _>>());
        }
    }

    #[test]
    fn test_rebalance() {
        let _ = env_logger::init();
        let addr = "0.0.0.0:0".parse().unwrap();
        for i in 0..100_000 {
            let mut ring = Ring::new(0, addr, (), 64, 1 + thread_rng().gen::<u8>() % 4);
            for i in 0..thread_rng().gen::<u64>() % 64 {
                ring.join_node(i, addr, ());
            }
            ring.rebalance();
            if let Err(e) = ring.is_valid() {
                panic!("{:?}: Invalid Ring {:?}", e, ring);
            }
            ring.finish_rebalance();

            for i in 0..thread_rng().gen::<u64>() % ring.nodes.len() as u64 {
                ring.remove_node(i);
            }
            ring.rebalance();
            if let Err(e) = ring.is_valid() {
                panic!("{:?}: Invalid Ring {:?}", e, ring);
            }
            ring.finish_rebalance();
        }
    }
}
