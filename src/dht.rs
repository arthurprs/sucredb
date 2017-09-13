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
use version_vector::VersionVector;
use fabric::{Fabric, FabricMsg, FabricMsgType};
use utils::{IdHashMap, IdHashSet, GenericError};

pub type DHTChangeFn = Box<FnMut() + Send>;
pub trait Metadata
    : Clone + Serialize + DeserializeOwned + Send + fmt::Debug + 'static {
}

impl<T: Clone + Serialize + DeserializeOwned + Send + fmt::Debug + 'static> Metadata for T {}

/// The Cluster controller, it knows how to map keys to their vnodes and
/// whose nodes hold data for each vnodes.
/// Calls a callback on cluster changes so the database can execute logic to converge
/// it's state to match the DHT.
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
}

struct Inner<T: Metadata> {
    ring: Ring<T>,
    ring_version: u64,
    cluster: String,
    callback: Option<DHTChangeFn>,
    fabric: Arc<Fabric>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(bound = "T: DeserializeOwned")]
struct Node<T: Metadata> {
    addr: net::SocketAddr,
    leaving: bool, // if true, shouldn't be considered to own anything
    meta: T,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
struct VNode {
    // nodes with ownership
    owners: LinearSet<NodeId>,
    // nodes taking ownership
    pending: LinearSet<NodeId>,
    // nodes giving up ownership, may include retiring nodes
    retiring: LinearSet<NodeId>,
    // vv
    version: VersionVector,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(bound = "T: DeserializeOwned")]
pub struct Ring<T: Metadata> {
    vnodes: Vec<VNode>,
    nodes: IdHashMap<NodeId, Node<T>>,
    replication_factor: usize,
    version: VersionVector,
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
    fn new(
        node: NodeId,
        addr: net::SocketAddr,
        meta: T,
        partitions: u16,
        replication_factor: u8,
    ) -> Self {

        let vn = VNode {
            owners: [node].iter().cloned().collect(),
            ..Default::default()
        };
        Ring {
            version: Default::default(),
            replication_factor: replication_factor as usize,
            vnodes: vec![vn; partitions as usize],
            nodes: vec![
                (
                    node,
                    Node {
                        addr,
                        leaving: false,
                        meta,
                    }
                ),
            ].into_iter()
                .collect(),
        }
    }

    fn valid_nodes_count(&self) -> usize {
        self.nodes.values().filter(|n| !n.leaving).count()
    }

    fn leave_node(&mut self, node: NodeId) -> Result<(), GenericError> {
        for vn in &mut self.vnodes {
            if vn.owners.remove(&node) {
                assert!(vn.retiring.insert(node));
                assert!(!vn.pending.remove(&node));
            } else {
                vn.pending.remove(&node);
            }
        }
        self.nodes.get_mut(&node).unwrap().leaving = true;
        Ok(())
    }

    fn join_node(
        &mut self,
        node: NodeId,
        addr: net::SocketAddr,
        meta: T,
    ) -> Result<(), GenericError> {
        self.nodes.insert(
            node,
            Node {
                addr,
                leaving: false,
                meta,
            },
        );
        Ok(())
    }

    fn remove_node(&mut self, node: NodeId) -> Result<(), GenericError> {
        self.nodes.remove(&node);
        for vn in &mut self.vnodes {
            vn.owners.remove(&node);
            vn.retiring.remove(&node);
            vn.pending.remove(&node);
        }
        Ok(())
    }

    fn promote_pending_node(&mut self, node: NodeId, vn_no: VNodeId) -> Result<(), GenericError> {
        let vn = &mut self.vnodes[vn_no as usize];
        if !vn.pending.remove(&node) {
            return Err(format!("{} is not in pending[{}]", node, vn_no).into());
        }
        if !vn.owners.insert(node) {
            return Err(format!("{} is already in vnodes[{}]", node, vn_no).into());
        }
        if vn.pending.is_empty() {
            vn.retiring.clear();
        }
        Ok(())
    }

    fn replace(
        &mut self,
        old: NodeId,
        new: NodeId,
        addr: net::SocketAddr,
        meta: T,
    ) -> Result<(), GenericError> {
        let Node { leaving, .. } = if let Some(removed) = self.nodes.remove(&old) {
            for vn in &mut self.vnodes {
                for v in &mut [&mut vn.owners, &mut vn.pending, &mut vn.retiring] {
                    if v.remove(&old) {
                        assert!(v.insert(new));
                    }
                }
            }
            removed
        } else {
            return Err(format!("{} is not in the cluster", new).into());
        };
        let node = Node {
            addr,
            leaving,
            meta,
        };
        if self.nodes.insert(new, node).is_some() {
            return Err(format!("{} is already in the cluster", new).into());
        }
        Ok(())
    }

    fn rebalance(&mut self) -> Result<(), GenericError> {
        // special case when #nodes <= replication factor
        if self.valid_nodes_count() <= self.replication_factor {
            for vn in &mut self.vnodes {
                for (&node, &Node { leaving, .. }) in &self.nodes {
                    if !leaving && !vn.owners.contains(&node) {
                        assert!(vn.pending.insert(node));
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
        for (&node, &Node { leaving, .. }) in &self.nodes {
            if !leaving {
                node_map.insert(node, IdHashSet::default());
            }
        }
        for (vn_no, vn) in self.vnodes.iter().enumerate() {
            for node in vn.owners.iter().chain(&vn.pending) {
                node_map.get_mut(node).unwrap().insert(vn_no);
            }
        }

        // partitions per node
        let vnpn = self.vnodes.len() * self.replication_factor / node_map.len();

        // 1. robin-hood
        for (vn_no, vn) in self.vnodes.iter_mut().enumerate() {
            let owners = &mut vn.owners;
            let pending = &mut vn.pending;
            let retiring = &mut vn.retiring;
            let doing_much: IdHashSet<_> = owners
                .iter()
                .chain(pending.iter())
                .filter(|n| node_map.get(n).unwrap().len() > vnpn)
                .cloned()
                .collect();
            let candidates: IdHashSet<_> = node_map
                .keys()
                .filter(|n| {
                    node_map.get(n).unwrap().len() < vnpn && !owners.contains(n) &&
                        !pending.contains(n) && !retiring.contains(n)
                })
                .cloned()
                .collect();
            for (from, to) in doing_much.into_iter().zip(candidates) {
                assert!(owners.remove(&from) || pending.remove(&from));
                assert!(pending.insert(to));
                assert!(retiring.insert(from));
                assert!(node_map.get_mut(&from).unwrap().remove(&vn_no));
                assert!(node_map.get_mut(&to).unwrap().insert(vn_no));
            }
        }

        // 2. complete replicas
        for (vn_no, vn) in self.vnodes.iter_mut().enumerate() {
            let owners = &mut vn.owners;
            let pending = &mut vn.pending;
            let retiring = &mut vn.retiring;
            while owners.len() + pending.len() < self.replication_factor {
                // try to find a candidate that is doing less work
                if let Some((&node, partitions)) =
                    node_map
                        .iter_mut()
                        .filter(|&(&node, _)| {
                            !owners.contains(&node) && !pending.contains(&node) &&
                                !retiring.contains(&node)
                        })
                        .min_by_key(|&(_, ref p)| p.len())
                {
                    assert!(partitions.insert(vn_no));
                    assert!(pending.insert(node));
                    continue;
                }
                // try to find candidate that was retiring from that vnode
                // only consider nodes in node_map (others could be leaving)
                if let Some((_, node)) = retiring
                    .iter()
                    .filter_map(|&n| node_map.get(&n).map(|p| (p.len(), n)))
                    .min()
                {
                    assert!(node_map.get_mut(&node).unwrap().insert(vn_no));
                    assert!(retiring.remove(&node));
                    assert!(owners.insert(node));
                    continue;
                }
                unreachable!(
                    "cant find replica for vnode {} v{:?} p{:?} r{:?} rf:{}",
                    vn_no,
                    owners,
                    pending,
                    retiring,
                    self.replication_factor
                );
            }
        }

        self.is_valid()
    }

    #[cfg(test)]
    fn finish_rebalance(&mut self) -> Result<(), GenericError> {
        self.is_valid().unwrap();
        for (vn_no, vn) in self.vnodes.iter_mut().enumerate() {
            vn.retiring.clear();
            for pending in vn.pending.drain() {
                assert!(vn.owners.insert(pending));
            }
        }
        self.is_valid()
    }

    fn is_valid(&self) -> Result<(), GenericError> {
        let valid_nodes_count = self.valid_nodes_count();
        let replicas = min(valid_nodes_count, self.replication_factor);
        let vnpn = self.vnodes.len() * self.replication_factor / valid_nodes_count;
        let vnpn_rest = self.vnodes.len() * self.replication_factor % valid_nodes_count;
        let mut node_map = IdHashMap::default();
        node_map.reserve(valid_nodes_count);

        for (vn_no, vn) in self.vnodes.iter().enumerate() {
            let owners = &vn.owners;
            let pending = &vn.pending;
            if owners.len() + pending.len() != replicas {
                return Err(
                    format!("vnode {} has {:?}{:?} replicas", vn_no, owners, pending).into(),
                );
            }
            for &n in owners.iter().chain(pending) {
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
                    ).into(),
                );
            }
        }

        Ok(())
    }
}

impl<T: Metadata> DHT<T> {
    pub fn new(
        fabric: Arc<Fabric>,
        cluster: &str,
        meta: T,
        initial: Option<RingDescription>,
        old_node: Option<NodeId>,
    ) -> DHT<T> {
        let ring = if let Some(d) = initial {
            Ring::new(
                fabric.node(),
                fabric.addr(),
                meta,
                d.partitions,
                d.replication_factor,
            )
        } else {
            Ring::new(fabric.node(), fabric.addr(), meta, 0, 0)
        };

        let inner = Arc::new(Mutex::new(Inner {
            ring: ring,
            ring_version: 0,
            cluster: cluster.into(),
            callback: None,
            fabric: fabric.clone(),
        }));

        let w_inner = Arc::downgrade(&inner);
        let fabric_cb = move |from, msg| if let Some(inner) = w_inner.upgrade() {
            Self::on_message(&mut *inner.lock().unwrap(), from, msg);
        };
        fabric.register_msg_handler(FabricMsgType::DHT, Box::new(fabric_cb));

        let mut dht = DHT {
            node: fabric.node(),
            addr: fabric.addr(),
            cluster: cluster.into(),
            inner: inner.clone(),
            slots_per_partition: 0,
            partitions: 0,
            replication_factor: 0,
        };
        // if let Some(d) = initial {
        //     dht.reset(meta, d.replication_factor, d.partitions).expect(
        //         "Failed to init cluster",
        //     );
        // } else if let Some(old) = old_node {
        //     dht.replace(old, meta).expect("Failed to replace node");
        // } else {
        //     dht.join(meta).expect("Failed join cluster");
        // }
        dht.partitions = inner.lock().unwrap().ring.vnodes.len();
        dht.slots_per_partition = HASH_SLOTS / dht.partitions as u16;
        dht.replication_factor = inner.lock().unwrap().ring.replication_factor;
        dht
    }

    fn on_message(inner: &mut Inner<T>, from: NodeId, msg: FabricMsg) {
        unimplemented!()
    }

    fn refresh_ring(&self) -> Result<(), GenericError> {
        // let r = self.etcd_client
        //     .get(&cluster_key, false, false, false)
        //     .map_err(squash_etcd_errors)?;
        // let node = r.node.unwrap();
        // let mut inner = self.inner.lock().unwrap();
        // inner.ring = Self::deserialize(&node.value.unwrap()).unwrap();
        // inner.ring_version = node.modified_index.unwrap();
        Ok(())
    }

    fn join(&self, meta: T) -> Result<(), GenericError> {
        self.refresh_ring()?;
        self.try_cas(
            || {
                let (mut ring, ring_version) = self.ring_clone();
                ring.join_node(self.node, self.addr, meta.clone())?;
                Ok((ring_version, ring))
            },
            true,
        )
    }

    fn replace(&self, old: NodeId, meta: T) -> Result<(), GenericError> {
        self.refresh_ring()?;
        self.try_cas(
            || {
                let (mut ring, ring_version) = self.ring_clone();
                ring.replace(old, self.node, self.addr, meta.clone())?;
                Ok((ring_version, ring))
            },
            true,
        )
    }

    fn reset(&self, meta: T, replication_factor: u8, partitions: u16) -> Result<(), GenericError> {
        assert!(
            partitions.is_power_of_two(),
            "Partition count must be a power of 2"
        );
        assert!(partitions >= 32, "Partition count must be >= 32");
        assert!(partitions <= 1024, "Partition count must be <= 1024");
        assert!(replication_factor >= 1, "Replication factor must be >= 1");
        assert!(replication_factor <= 6, "Replication factor must be <= 6");

        let mut inner = self.inner.lock().unwrap();
        inner.ring = Ring::new(self.node, self.addr, meta, partitions, replication_factor);
        let new = Self::serialize(&inner.ring).unwrap();
        // let r = self.etcd_client.set(&cluster_key, &new, None).map_err(
        //     squash_etcd_errors,
        // )?;
        // inner.ring_version = r.node.unwrap().modified_index.unwrap();
        Ok(())
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
        for (vn_no,
             &VNode {
                 owners: ref o,
                 pending: ref p,
                 ..
             }) in inner.ring.vnodes.iter().enumerate()
        {
            if o.contains(&node) {
                insync.push(vn_no as VNodeId);
            }
            if p.contains(&node) {
                pending.push(vn_no as VNodeId);
            }
        }
        (insync, pending)
    }

    // TODO: split into read_ and write_
    pub fn nodes_for_vnode(
        &self,
        vn_no: VNodeId,
        include_pending: bool,
        include_retiring: bool,
    ) -> Vec<NodeId> {
        // FIXME: this shouldn't alloc
        let mut result = Vec::with_capacity(self.replication_factor + 1);
        let inner = self.inner.lock().unwrap();
        let vn = &inner.ring.vnodes[vn_no as usize];
        result.extend(&vn.owners);
        if include_pending {
            result.extend(&vn.pending);
        }
        if include_retiring {
            result.extend(&vn.retiring);
        }
        result
    }

    pub fn nodes_for_vnode_ex(
        &self,
        vn_no: VNodeId,
        include_pending: bool,
        include_retiring: bool,
    ) -> Vec<(NodeId, (net::SocketAddr, T))> {
        // FIXME: this shouldn't alloc
        let mut result = Vec::with_capacity(self.replication_factor + 1);
        let inner = self.inner.lock().unwrap();

        let filter_map = |n| {
            let node = inner.ring.nodes.get(n).unwrap();
            if node.leaving {
                None
            } else {
                Some((*n, (node.addr, node.meta.clone())))
            }
        };

        let vn = &inner.ring.vnodes[vn_no as usize];
        result.extend(vn.owners.iter().filter_map(&filter_map));
        if include_pending {
            result.extend(vn.pending.iter().filter_map(&filter_map));
        }
        if include_retiring {
            result.extend(vn.retiring.iter().filter_map(&filter_map));
        }
        result
    }

    pub fn members(&self) -> IdHashMap<NodeId, net::SocketAddr> {
        let inner = self.inner.lock().unwrap();
        inner
            .ring
            .nodes
            .iter()
            .map(|(k, v)| (k.clone(), v.addr))
            .collect()
    }

    pub fn slots(&self) -> BTreeMap<(u16, u16), Vec<(NodeId, (net::SocketAddr, T))>> {
        let slots_per_partition = HASH_SLOTS / self.partitions() as u16;
        let mut result = BTreeMap::new();
        let inner = self.inner.lock().unwrap();
        for (vn_no,
             &VNode {
                 owners: ref o,
                 pending: ref p,
                 retiring: ref r,
                 ..
             }) in inner.ring.vnodes.iter().enumerate()
        {
            let members: Vec<_> = o.iter()
                .chain(p)
                .chain(r)
                .filter_map(|n| {
                    let node = inner.ring.nodes.get(n).unwrap();
                    if node.leaving {
                        None
                    } else {
                        Some((*n, (node.addr, node.meta.clone())))
                    }
                })
                .collect();
            let vn_no = vn_no as u16;
            result.insert(
                (
                    vn_no * slots_per_partition,
                    (vn_no + 1) * slots_per_partition - 1,
                ),
                members,
            );
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

    fn propose(
        &self,
        old_version: u64,
        new_ring: Ring<T>,
        update: bool,
    ) -> Result<(), GenericError> {
        debug!("Proposing new ring against version {}", old_version);
        let new = Self::serialize(&new_ring).unwrap();
        // let r = self.etcd_client
        //     .compare_and_swap(&cluster_key, &new, None, None, Some(old_version))
        //     .map_err(|mut e| e.pop().unwrap())?;
        // if update {
        //     let mut inner = self.inner.lock().unwrap();
        //     inner.ring = new_ring;
        //     inner.ring_version = r.node.unwrap().modified_index.unwrap();
        //     debug!(
        //         "Updated ring to {:?} version {}",
        //         inner.ring,
        //         inner.ring_version
        //     );
        // }
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
                // Err(etcd::Error::Api(ref e)) if e.error_code == 101 => {
                //     warn!("Proposing new ring conflicted at version {}", ring_version);
                //     self.wait_new_version(ring_version);
                // }
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
    fn test_rebalance_leaving_nodes() {
        let _ = env_logger::init();
        let addr = "0.0.0.0:0".parse().unwrap();
        for i in 0..1_000 {
            let mut ring = Ring::new(0, addr, (), 64, 1 + thread_rng().gen::<u8>() % 4);
            for i in 0..thread_rng().gen::<u64>() % 64 {
                ring.join_node(i, addr, ()).unwrap();
            }
            ring.rebalance().unwrap();
            ring.finish_rebalance().unwrap();

            for i in 0..thread_rng().gen::<u64>() % ring.valid_nodes_count() as u64 {
                ring.leave_node(i).unwrap();
            }
            ring.rebalance().unwrap();
            ring.finish_rebalance().unwrap();
        }
    }

    #[test]
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

            for i in 0..thread_rng().gen::<u64>() % ring.valid_nodes_count() as u64 {
                ring.remove_node(i).unwrap();
            }
            ring.rebalance().unwrap();
            ring.finish_rebalance().unwrap();
        }
    }
}
