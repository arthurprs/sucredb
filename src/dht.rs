use std::{fmt, thread, net};
use std::cmp::min;
use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::collections::BTreeMap;
use linear_map::set::LinearSet;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json;
use hash::{hash_slot, HASH_SLOTS};
use database::{NodeId, VNodeId};
use etcd;
use utils::{IdHashMap, IdHashSet, GenericError};

pub type DHTChangeFn = Box<FnMut() + Send>;
pub trait Metadata
    : Clone + Serialize + DeserializeOwned + Send + fmt::Debug + 'static {
}

impl<T: Clone + Serialize + DeserializeOwned + Send + fmt::Debug + 'static> Metadata for T {}

/// The Cluster controller, it knows how to map keys to their vnodes and
//// whose nodes hold data for each vnodes.
/// Calls a callback on cluster changes so the database can execute logic to converge
/// it's state to match the DHT.
/// Consistency is achieved by using etcd in the background but it's only required for
/// database startup and cluster changes.
/// Only knows about NodeIds (and their fabric addresses), Vnodes and other high level info,
/// Extra (static) information is attached to NodeId through a Metadata type.
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
#[serde(bound="T: DeserializeOwned")]
pub struct Ring<T: Metadata> {
    replication_factor: usize,
    vnodes: Vec<LinearSet<NodeId>>,
    pending: Vec<LinearSet<NodeId>>,
    retiring: Vec<LinearSet<NodeId>>,
    // map of nodes to (fabric_addr, leaving_flag, meta)
    nodes: IdHashMap<NodeId, (net::SocketAddr, bool, T)>,
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
            nodes: vec![(node, (addr, false, meta))].into_iter().collect(),
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
        self.nodes.get_mut(&node).unwrap().1 = true;
        Ok(())
    }

    fn join_node(&mut self, node: NodeId, addr: net::SocketAddr, meta: T)
        -> Result<(), GenericError> {
        self.nodes.insert(node, (addr, false, meta));
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
        let (_, leaving, _) = if let Some(removed) = self.nodes.remove(&old) {
            let iter = self.vnodes
                .iter_mut()
                .chain(self.pending.iter_mut().chain(self.retiring.iter_mut()));
            for v in iter {
                if v.remove(&old) {
                    assert!(v.insert(node));
                }
            }
            removed
        } else {
            return Err(format!("{} is not in the cluster", node).into());
        };
        if self.nodes.insert(node, (addr, leaving, meta)).is_some() {
            return Err(format!("{} is already in the cluster", node).into());
        }
        Ok(())
    }

    fn rebalance(&mut self) -> Result<(), GenericError> {
        // special case when #nodes <= replication factor
        if self.nodes.len() <= self.replication_factor {
            for (vn_num, vn_replicas) in self.vnodes.iter().enumerate() {
                for &node in self.nodes.keys() {
                    if !vn_replicas.contains(&node) {
                        self.pending[vn_num].insert(node);
                    }
                }
            }
            return Ok(());
        }

        // simple 2-steps eager rebalancing
        // 1. take from who is doing too much work
        // 2. assing replicas for under replicated vnodes

        // map of active nodes to vnodes
        let mut node_map = IdHashMap::default();
        for (&node, &(_, leaving, _)) in &self.nodes {
            if !leaving {
                node_map.insert(node, IdHashSet::default());
            }
        }
        for vn in 0..self.vnodes.len() {
            let vnodes = &mut self.vnodes[vn];
            let pending = &mut self.pending[vn];
            // let retiring = &mut self.retiring[vn];
            for node in vnodes.iter().chain(pending.iter()) {
                node_map.get_mut(node).unwrap().insert(vn);
            }
        }

        // partitions per node
        let vnpn = self.vnodes.len() * self.replication_factor / node_map.len();

        // 1. robin-hood
        for vn in 0..self.vnodes.len() {
            let vnodes = &mut self.vnodes[vn];
            let pending = &mut self.pending[vn];
            let retiring = &mut self.retiring[vn];
            let doing_much: IdHashSet<_> = vnodes
                .iter()
                .chain(pending.iter())
                .filter(|n| node_map.get(n).unwrap().len() > vnpn)
                .cloned()
                .collect();
            let candidates: IdHashSet<_> = node_map
                .keys()
                .filter(
                    |n| {
                        node_map.get(n).unwrap().len() < vnpn && !vnodes.contains(n) &&
                        !pending.contains(n) && !retiring.contains(n)
                    },
                )
                .cloned()
                .collect();
            for (from, to) in doing_much.into_iter().zip(candidates.into_iter()) {
                assert!(vnodes.remove(&from) || pending.remove(&from));
                assert!(pending.insert(to));
                assert!(retiring.insert(from));
                assert!(node_map.get_mut(&from).unwrap().remove(&vn));
                assert!(node_map.get_mut(&to).unwrap().insert(vn));
            }
        }

        // 2. complete replicas
        for vn in 0..self.vnodes.len() {
            let vnodes = &mut self.vnodes[vn];
            let pending = &mut self.pending[vn];
            let retiring = &mut self.retiring[vn];
            'outer: while vnodes.len() + pending.len() < self.replication_factor {
                // try to find a candidate that is doing less work
                if let Some((&node, partitions)) =
                    node_map
                        .iter_mut()
                        .filter(
                            |&(&node, _)| {
                                !vnodes.contains(&node) && !pending.contains(&node) &&
                                !retiring.contains(&node)
                            },
                        )
                        .min_by_key(|&(_, ref p)| p.len()) {
                    assert!(partitions.insert(vn));
                    assert!(pending.insert(node));
                    continue 'outer;
                }
                // try to find candidate that was retiring from that vnode
                if let Some(&node) =
                    retiring.iter().min_by_key(|n| node_map.get(n).unwrap().len()) {
                    assert!(node_map.get_mut(&node).unwrap().insert(vn));
                    assert!(retiring.remove(&node));
                    assert!(vnodes.insert(node));
                    continue 'outer;
                }
                unreachable!(
                    "cant find replica for vnode {} v{:?} p{:?} r{:?} rf:{}",
                    vn,
                    vnodes,
                    pending,
                    retiring,
                    self.replication_factor
                );
            }
        }

        self.is_valid()?;

        Ok(())
    }

    #[cfg(test)]
    fn finish_rebalance(&mut self) -> Result<(), GenericError> {
        self.is_valid().unwrap();
        for retiring in &mut self.retiring {
            retiring.clear();
        }
        for (vn, pendings) in self.pending.iter_mut().enumerate() {
            for pending in pendings.drain() {
                assert!(self.vnodes[vn].insert(pending));
            }
        }
        self.is_valid()
    }

    fn is_valid(&self) -> Result<(), GenericError> {
        let replicas = min(self.nodes.len(), self.replication_factor);
        let vnpn = self.vnodes.len() * self.replication_factor / self.nodes.len();
        let vnpn_rest = self.vnodes.len() * self.replication_factor % self.nodes.len();
        let mut node_map = IdHashMap::default();
        node_map.reserve(self.nodes.len());

        for vn in 0..self.vnodes.len() {
            let vnodes = &self.vnodes[vn];
            let pending = &self.pending[vn];
            // let retiring = &self.retiring[vn];
            if vnodes.len() + pending.len() != replicas {
                return Err(format!("vnode {} has {:?}{:?} replicas", vn, vnodes, pending).into());
            }
            for &n in vnodes.iter().chain(pending) {
                *node_map.entry(n).or_insert(0usize) += 1;
            }
        }

        for (&n, &count) in &node_map {
            if count > vnpn + vnpn_rest + 1 {
                return Err(
                    format!(
                        "node {} is a replica for {} vnodes, expected {} max, {:?}",
                        n,
                        count,
                        vnpn + vnpn_rest + 1,
                        node_map
                    )
                            .into(),
                );
            }
        }

        Ok(())
    }
}

impl<T: Metadata> DHT<T> {
    pub fn new(
        node: NodeId, fabric_addr: net::SocketAddr, cluster: &str, etcd: &str, meta: T,
        initial: Option<RingDescription>, old_node: Option<NodeId>
    ) -> DHT<T> {
        let etcd_client1 = etcd::Client::new(&[etcd]).unwrap();
        let etcd_client2 = etcd::Client::new(&[etcd]).unwrap();
        let inner = Arc::new(
            Mutex::new(
                Inner {
                    ring: Ring {
                        replication_factor: Default::default(),
                        vnodes: Default::default(),
                        pending: Default::default(),
                        retiring: Default::default(),
                        nodes: Default::default(),
                    },
                    ring_version: 0,
                    cluster: cluster.into(),
                    callback: None,
                    running: true,
                },
            ),
        );
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
            assert!(description.partitions <= 4 * 1024);
            dht.reset(meta, description.replication_factor, description.partitions);
        } else if let Some(old) = old_node {
            dht.replace(old, meta);
        } else {
            dht.join(meta);
        }
        dht.partitions = inner.lock().unwrap().ring.vnodes.len();
        dht.slots_per_partition = HASH_SLOTS / dht.partitions as u16;
        dht.replication_factor = inner.lock().unwrap().ring.replication_factor;
        dht.thread = Some(
            thread::Builder::new()
                .name(format!("DHT:{}", node))
                .spawn(move || Self::run(inner, etcd_client2))
                .unwrap(),
        );
        dht
    }

    fn run(inner: Arc<Mutex<Inner<T>>>, etcd: etcd::Client) {
        let cluster_key = format!("/sucredb/{}/dht", inner.lock().unwrap().cluster);
        loop {
            let watch_version = {
                let inner = inner.lock().unwrap();
                if !inner.running {
                    break;
                }
                inner.ring_version + 1
            };
            // listen for changes
            let watch_r = etcd.watch(&cluster_key, Some(watch_version), false);
            // FIXME: we have no way of canceling the watch right now
            // so before anything make sure the dht is still marked as running
            if !inner.lock().unwrap().running {
                return;
            }
            let node = match watch_r {
                Ok(r) => r.node.unwrap(),
                Err(e) => {
                    warn!("etcd.watch error: {:?}", e);
                    continue;
                }
            };

            // deserialize
            let ring = Self::deserialize(&node.value.unwrap()).unwrap();
            let ring_version = node.modified_index.unwrap();
            info!("New ring version {}", ring_version);
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
        let cluster_key = format!("/sucredb/{}/dht", self.cluster);
        let r = self.etcd_client.get(&cluster_key, false, false, false).unwrap();
        let node = r.node.unwrap();
        let mut inner = self.inner.lock().unwrap();
        inner.ring = Self::deserialize(&node.value.unwrap()).unwrap();
        inner.ring_version = node.modified_index.unwrap();
    }

    fn join(&self, meta: T) {
        self.refresh_ring();
        self.try_cas(
                || {
                    let (mut ring, ring_version) = self.ring_clone();
                    ring.join_node(self.node, self.addr, meta.clone())?;
                    Ok((ring_version, ring))
                },
                true,
            )
            .unwrap();
    }

    fn replace(&self, old: NodeId, meta: T) {
        self.refresh_ring();
        self.try_cas(
                || {
                    let (mut ring, ring_version) = self.ring_clone();
                    ring.replace(old, self.node, self.addr, meta.clone())?;
                    Ok((ring_version, ring))
                },
                true,
            )
            .unwrap();
    }

    fn reset(&self, meta: T, replication_factor: u8, partitions: u16) {
        assert!(partitions.is_power_of_two(), "Number of partitions must be power of 2");
        assert!(replication_factor >= 1, "Replication factor must be >= 1");
        let cluster_key = format!("/sucredb/{}/dht", self.cluster);
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
        // use / instead of % to get continuous hash slots for each vnode
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
        result.extend(
            inner.ring.vnodes[vnode as usize]
                .iter()
                .chain(inner.ring.pending[vnode as usize].iter())
                .map(
                    |n| {
                        let (addr, _, meta) = inner.ring.nodes.get(n).cloned().unwrap();
                        (*n, (addr, meta))
                    },
                ),
        );
        result
    }

    pub fn members(&self) -> IdHashMap<NodeId, net::SocketAddr> {
        let inner = self.inner.lock().unwrap();
        inner.ring.nodes.iter().map(|(k, v)| (k.clone(), v.0.clone())).collect()
    }

    pub fn slots(&self) -> BTreeMap<(u16, u16), Vec<(NodeId, (net::SocketAddr, T))>> {
        let slots_per_partition = HASH_SLOTS / self.partitions() as u16;
        let mut result = BTreeMap::new();
        let inner = self.inner.lock().unwrap();
        for (hi, ((v, p), r)) in
            inner
                .ring
                .vnodes
                .iter()
                .zip(inner.ring.pending.iter())
                .zip(inner.ring.retiring.iter())
                .enumerate() {
            let members: Vec<_> = v.iter()
                .chain(p.iter())
                .chain(r.iter())
                .map(
                    |n| {
                        let (addr, _, meta) = inner.ring.nodes.get(n).cloned().unwrap();
                        (*n, (addr, meta))
                    },
                )
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
        self.try_cas(
            || {
                let (mut ring, ring_version) = self.ring_clone();
                ring.rebalance()?;
                Ok((ring_version, ring))
            },
            false,
        )
    }

    pub fn remove_node(&self, node: NodeId) -> Result<(), GenericError> {
        self.try_cas(
            || {
                let (mut ring, ring_version) = self.ring_clone();
                ring.remove_node(node)?;
                Ok((ring_version, ring))
            },
            false,
        )
    }

    pub fn leave_node(&self, node: NodeId) -> Result<(), GenericError> {
        self.try_cas(
            || {
                let (mut ring, ring_version) = self.ring_clone();
                ring.leave_node(node)?;
                Ok((ring_version, ring))
            },
            false,
        )
    }

    pub fn promote_pending_node(&self, node: NodeId, vnode: VNodeId) -> Result<(), GenericError> {
        self.try_cas(
            || {
                let (mut ring, ring_version) = self.ring_clone();
                ring.promote_pending_node(node, vnode)?;
                Ok((ring_version, ring))
            },
            false,
        )
    }

    fn propose(&self, old_version: u64, new_ring: Ring<T>, update: bool)
        -> Result<(), etcd::Error> {
        debug!("Proposing new ring against version {}", old_version);
        let cluster_key = format!("/sucredb/{}/dht", self.cluster);
        let new = Self::serialize(&new_ring).unwrap();
        let r = self.etcd_client
            .compare_and_swap(&cluster_key, &new, None, None, Some(old_version))
            .map_err(|mut e| e.pop().unwrap())?;
        if update {
            let mut inner = self.inner.lock().unwrap();
            inner.ring = new_ring;
            inner.ring_version = r.node.unwrap().modified_index.unwrap();
            debug!("Updated ring to {:?} version {}", inner.ring, inner.ring_version);
        }
        Ok(())
    }

    fn try_cas<C>(&self, callback: C, update: bool) -> Result<(), GenericError>
    where
        C: Fn() -> Result<(u64, Ring<T>), GenericError>,
    {
        loop {
            let (ring_version, new_ring) = callback()?;
            info!("Proposing new ring using version {}", ring_version);
            match self.propose(ring_version, new_ring, update) {
                Ok(()) => break,
                Err(etcd::Error::Api(ref e)) if e.error_code == 101 => {
                    warn!("Proposing new ring conflicted at version {}", ring_version);
                    self.wait_new_version(ring_version);
                }
                Err(e) => {
                    error!("Proposing new ring failed with: {}", e);
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    fn serialize(ring: &Ring<T>) -> serde_json::Result<String> {
        serde_json::to_string_pretty(&ring)
    }

    fn deserialize(json_ring: &str) -> serde_json::Result<Ring<T>> {
        serde_json::from_str(json_ring)
    }
}

impl<T: Metadata> Drop for DHT<T> {
    fn drop(&mut self) {
        let _ = self.inner.lock().map(|mut inner| inner.running = false);
    }
}

#[cfg(test)]
mod tests {
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
            let dht = DHT::new(
                node,
                addr,
                "test",
                config::DEFAULT_ETCD_ADDR,
                (),
                Some(RingDescription::new(rf, 256)),
                None,
            );
            assert_eq!(dht.nodes_for_vnode(0, true, false), &[node]);
            assert_eq!(dht.nodes_for_vnode(0, false, false), &[node]);
            assert_eq!(dht.nodes_for_vnode(0, false, false), &[node]);
            assert_eq!(dht.nodes_for_vnode(0, true, true), &[node]);
            assert_eq!(dht.members(), [(node, addr)].iter().cloned().collect());
        }
    }

    #[test]
    fn test_rebalance_rf1() {
        let _ = env_logger::init();
        let addr = "0.0.0.0:0".parse().unwrap();
        let mut ring = Ring::new(0, addr, (), 64, 1);
        ring.join_node(1, addr, ()).unwrap();
        ring.rebalance().unwrap();
        ring.finish_rebalance().unwrap();
    }

    #[test]
    // #[ignore]
    fn test_rebalance() {
        let _ = env_logger::init();
        let addr = "0.0.0.0:0".parse().unwrap();
        for i in 0..1_000 {
            let mut ring = Ring::new(0, addr, (), 64, 1 + thread_rng().gen::<u8>() % 4);
            for i in 0..thread_rng().gen::<u64>() % 64 {
                ring.join_node(i, addr, ()).unwrap();
            }
            ring.rebalance().unwrap();
            ring.finish_rebalance().unwrap();

            for i in 0..thread_rng().gen::<u64>() % ring.nodes.len() as u64 {
                ring.remove_node(i).unwrap();
            }
            ring.rebalance().unwrap();
            ring.finish_rebalance().unwrap();
        }
    }
}
