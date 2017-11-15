use std::{fmt, thread};
use std::net::SocketAddr;
use std::cmp::min;
use std::time::{Duration, Instant};
use std::sync::{Arc, RwLock};
use std::collections::BTreeMap;
use std::collections::hash_map::Entry as HMEntry;

use rand::{thread_rng, Rng};
use linear_map::{Entry as LMEntry, LinearMap};
use serde::Serialize;
use serde::de::DeserializeOwned;
use bincode;
use bytes::Bytes;

use config::Config;
use hash::{hash_slot, HASH_SLOTS};
use database::{NodeId, VNodeId};
use version_vector::VersionVector;
use fabric::{Fabric, FabricMsg, FabricMsgRef, FabricMsgType};
use types::PhysicalNodeId;
use utils::{GenericError, IdHashMap, IdHashSet, split_u64};

// can be called by the network thread or a worker doing a dht mutation
pub type DHTChangeFn = Box<Fn() + Send + Sync>;

// rwlock needs metadata to be sync, as it's read concurrently by multiple threads
pub trait Metadata
    : Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync + fmt::Debug + 'static
    {
}

impl<
    T: Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync + fmt::Debug + 'static,
> Metadata for T {
}

// *pseudo* interval used to calculate aae msg rate
const DHT_AAE_INTERVAL_MS: u64 = 1_000;
// interval for active anti entropy checks
const DHT_AAE_TRIGGER_INTERVAL_MS: u64 = 1_000;

/// The Cluster controller, it knows how to map keys to their vnodes and
/// whose nodes hold data for each vnodes.
/// Calls a callback on cluster changes so the database can execute logic to converge
/// it's state to match the DHT.
/// Only knows about NodeIds (and their fabric addresses), Vnodes and other high level info,
/// Extra (static) information is attached to NodeId through a Metadata type.
pub struct DHT<T: Metadata> {
    node: NodeId,
    inner: Arc<RwLock<Inner<T>>>,
}

struct Inner<T: Metadata> {
    node: NodeId,
    ring: Ring<T>,
    callback: Option<DHTChangeFn>,
    fabric: Arc<Fabric>,
    next_req_broadcast: Instant,
    sync_on_connect: bool,
    sync_aae: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
enum NodeStatus {
    // Valid node, as one would expect
    Valid,
    // Like valid but shouldn't be considered to own anything by rebalance
    Leaving,
    // Invalid node, does not own anything. Works like a tombstone.
    Invalid,
}

use self::NodeStatus::*;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
enum VNodeNodeStatus {
    // Takes read/write traffic
    Owner,
    // Takes write traffic while it bootstraps from an Owner
    Pending,
    // Like Owner, but will go away once there are no more pending
    // FIXME: once a retiring vnode goes away (vnode goes zombie for a grace period)
    // other nodes will not sync from it (as sync is a pull)
    Retiring,
}

use self::VNodeNodeStatus::*;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(bound = "T: DeserializeOwned")]
struct Node<T: Metadata> {
    addr: SocketAddr,
    status: NodeStatus,
    meta: T,
    version: VersionVector,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
struct VNode {
    // nodes with ownership
    owners: LinearMap<NodeId, VNodeNodeStatus>,
    // vv
    version: VersionVector,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(bound = "T: DeserializeOwned")]
struct Ring<T: Metadata> {
    vnodes: Vec<VNode>,
    nodes: IdHashMap<NodeId, Node<T>>,
    replication_factor: usize,
    // Note: For the version vector to be "valid" all proposedchanges by a node must be serialized
    version: VersionVector,
    cluster: String,
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
    fn serialize(ring: &Ring<T>) -> Result<Vec<u8>, GenericError> {
        bincode::serialize(ring, bincode::Infinite)
            .map_err(|e| format!("Can't serialize Ring: {:?}", e).into())
    }

    fn deserialize(bytes: &[u8]) -> Result<Ring<T>, GenericError> {
        bincode::deserialize(bytes).map_err(|e| format!("Can't deserialize Ring: {:?}", e).into())
    }

    fn new(cluster: &str, partitions: u16, replication_factor: u8) -> Self {
        Ring {
            version: Default::default(),
            replication_factor: replication_factor as usize,
            vnodes: vec![Default::default(); partitions as usize],
            nodes: Default::default(),
            cluster: cluster.into(),
        }
    }

    fn valid_nodes_count(&self) -> usize {
        self.nodes.values().filter(|n| n.status == Valid).count()
    }

    fn valid_physical_count(&self, physical: PhysicalNodeId) -> usize {
        self.nodes
            .iter()
            .filter(|&(&i, n)| split_u64(i).0 == physical && n.status == Valid)
            .count()
    }

    fn leave_node(&mut self, this: NodeId, leaving: NodeId) -> Result<(), GenericError> {
        if let Some(node) = self.nodes.get_mut(&leaving) {
            if node.status != Leaving {
                node.status = Leaving;
                node.version.event(this);
            }
        }
        for vn in &mut self.vnodes {
            if let Some(status) = vn.owners.get_mut(&leaving) {
                if *status != Retiring {
                    *status = Retiring;
                    vn.version.event(this);
                }
            }
        }
        Ok(())
    }

    fn join_node(
        &mut self,
        this: NodeId,
        node: NodeId,
        addr: SocketAddr,
        meta: T,
    ) -> Result<(), GenericError> {
        match self.nodes.entry(node) {
            HMEntry::Vacant(v) => {
                v.insert(Node {
                    addr,
                    meta,
                    status: Valid,
                    version: {
                        let mut version = VersionVector::new();
                        version.event(this);
                        version
                    },
                });
            }
            HMEntry::Occupied(mut o) => {
                let node = o.get_mut();
                if node.meta == meta && node.addr == addr {
                    return Ok(());
                }
                node.meta = meta;
                node.addr = addr;
                node.version.event(this);
            }
        }
        if self.valid_physical_count(split_u64(node).0) > 1 {
            return Err("Duplicated physical node id".into());
        }
        self.version.event(this);
        Ok(())
    }

    fn remove_node(&mut self, this: NodeId, removed: NodeId) -> Result<(), GenericError> {
        self.version.event(this);
        if let Some(node) = self.nodes.get_mut(&removed) {
            if node.status != Invalid {
                node.status = Invalid;
                node.version.event(this);
            }
        }
        for vn in &mut self.vnodes {
            if let Some(_) = vn.owners.remove(&removed) {
                vn.version.event(this);
            }
        }
        Ok(())
    }

    fn promote_pending_node(
        &mut self,
        this: NodeId,
        promoted: NodeId,
        vn_no: VNodeId,
    ) -> Result<(), GenericError> {
        self.version.event(this);
        let vn = &mut self.vnodes[vn_no as usize];
        if let Some(status) = vn.owners.get_mut(&promoted) {
            if *status != Pending && *status != Owner {
                return Err(format!("{} cant be promoted", promoted).into());
            }
            if *status != Owner {
                *status = Owner;
                vn.version.event(this);
            } else {
                debug!(
                    "Can't promote {}, it's already an owner of {}",
                    promoted,
                    vn_no
                );
                return Ok(());
            }
        } else {
            return Err(format!("{} is not part of vnodes[{}]", promoted, vn_no).into());
        }
        if !vn.owners.values().any(|&s| s == Pending) {
            debug!("Removing retiring nodes from vnode {}", vn_no);
            vn.owners.retain(|_, s| *s != Retiring);
        }

        Ok(())
    }

    fn replace_node(
        &mut self,
        this: NodeId,
        old: NodeId,
        new: NodeId,
        addr: SocketAddr,
        meta: T,
    ) -> Result<(), GenericError> {
        self.version.event(this);
        // insert new valid node
        let mut node = Node {
            addr,
            status: Valid,
            meta,
            version: Default::default(),
        };
        node.version.event(this);
        if self.nodes.insert(new, node).is_some() {
            return Err(format!("{} is already in the cluster", new).into());
        }
        // mark old invalid
        if let Some(node) = self.nodes.get_mut(&old) {
            if node.status != Invalid {
                node.status = Invalid;
                node.version.event(this);
            }
        } else {
            return Err(format!("{} is not in the cluster", new).into());
        };
        // replace ownership
        for vn in &mut self.vnodes {
            if let Some(status) = vn.owners.remove(&old) {
                vn.owners.insert(new, status);
                vn.version.event(this);
            }
        }
        if self.valid_physical_count(split_u64(new).0) > 1 {
            return Err("Duplicated physical node id".into());
        }
        Ok(())
    }

    fn merge(&mut self, mut other: Self) -> Result<bool, GenericError> {
        debug!("Merging rings {:?} {:?}", self.version, other.version);
        // sanity checks
        if self.cluster != other.cluster {
            return Err(
                format!(
                    "Cluster name differs {:?} != {:?}",
                    self.cluster,
                    other.cluster
                ).into(),
            );
        }
        if self.vnodes.len() != other.vnodes.len() && !self.vnodes.is_empty() {
            return Err(
                format!(
                    "Incompatible partition count {:?} != {:?}",
                    self.vnodes.len(),
                    other.vnodes.len()
                ).into(),
            );
        }
        if other.vnodes.is_empty() {
            return Err("Other ring isn't valid".into());
        }

        if self.version.descends(&other.version) {
            debug!("Accepting this ring {:?} {:?}", self.version, other.version);
            return Ok(false);
        }
        if other.version.descends(&self.version) {
            debug!(
                "Accepting other ring {:?} {:?}",
                self.version,
                other.version
            );
            *self = other;
            return Ok(true);
        }

        info!(
            "Merging diverging ring versions {:?} {:?}",
            self.version,
            other.version
        );
        self.version.merge(&other.version);

        // merge nodes
        for (n, other_node) in other.nodes.drain() {
            match self.nodes.entry(n) {
                HMEntry::Vacant(v) => {
                    v.insert(other_node);
                }
                HMEntry::Occupied(mut o) => {
                    let node = o.get_mut();
                    if node.version.descends(&other_node.version) {
                        continue;
                    }
                    if other_node.version.descends(&other_node.version) {
                        *node = other_node;
                        continue;
                    }

                    node.version.merge(&other_node.version);

                    match (other_node.status, other_node.status) {
                        (Valid, Valid) => (),
                        (Leaving, _) | (_, Leaving) => {
                            node.status = Leaving;
                        }
                        (Invalid, _) | (_, Invalid) => {
                            node.status = Invalid;
                        }
                    }
                }
            }
        }

        // merge vnodes
        if self.vnodes.is_empty() {
            self.vnodes = other.vnodes;
        } else {
            for (vn_no, (vnode, mut other_vnode)) in
                self.vnodes.iter_mut().zip(other.vnodes).enumerate()
            {
                if vnode.version.descends(&other_vnode.version) {
                    continue;
                }
                if other_vnode.version.descends(&vnode.version) {
                    *vnode = other_vnode;
                    continue;
                }

                vnode.version.merge(&other_vnode.version);

                for (n, other_status) in other_vnode.owners.drain() {
                    match vnode.owners.entry(n) {
                        LMEntry::Vacant(v) => {
                            v.insert(other_status);
                        }
                        LMEntry::Occupied(mut o) => {
                            let status = o.get_mut();
                            debug!(
                                "Conflicting vnode {} vnode {} status {:?} {:?}",
                                n,
                                vn_no,
                                status,
                                other_status
                            );
                            match (*status, other_status) {
                                (Owner, _) | (_, Owner) => {
                                    *status = Owner;
                                }
                                (Pending, _) | (_, Pending) => {
                                    *status = Pending;
                                }
                                (Retiring, Retiring) => (),
                            }
                        }
                    }
                }

                if !vnode.owners.values().any(|&s| s == Pending) {
                    debug!("Removing retiring nodes from vnone {}", vn_no);
                    vnode.owners.retain(|_, s| *s != Retiring);
                }
            }
        }

        Ok(true)
    }

    fn rebalance(&mut self, this: NodeId) -> Result<(), GenericError> {
        self.version.event(this);
        // special case when #nodes <= replication factor
        if self.valid_nodes_count() <= self.replication_factor {
            // special case 1 node case
            let status = if self.nodes.len() == 1 {
                Owner
            } else {
                Pending
            };
            for vn in &mut self.vnodes {
                for (&node_id, node) in &self.nodes {
                    if node.status != Valid {
                        continue;
                    }
                    match vn.owners.entry(node_id) {
                        LMEntry::Vacant(v) => {
                            v.insert(status);
                        }
                        LMEntry::Occupied(mut o) => {
                            if *o.get() != Retiring {
                                continue;
                            }
                            *o.get_mut() = status;
                        }
                    }
                    vn.version.event(this);
                }
            }
            return Ok(());
        }

        // simple 2-steps eager rebalancing
        // 1. assing replicas for under replicated vnodes
        // 2. take from who is doing too much work

        // map of active nodes to vnodes
        let mut node_map = IdHashMap::default();
        for (&node_id, node) in &self.nodes {
            if node.status == Valid {
                node_map.insert(node_id, IdHashSet::default());
            }
        }
        for (vn_no, vn) in self.vnodes.iter_mut().enumerate() {
            // update all vn versions
            vn.version.event(this);
            for (node, status) in &vn.owners {
                if *status != Retiring {
                    node_map.get_mut(node).unwrap().insert(vn_no);
                }
            }
        }

        // partitions per node
        let vnpn = ((self.vnodes.len() * self.replication_factor) as f64 / node_map.len() as f64)
            .ceil() as usize;

        // 1. complete replicas
        for (vn_no, vn) in self.vnodes.iter_mut().enumerate() {
            let replicas = vn.owners.values().filter(|&&s| s != Retiring).count();
            for _ in replicas..self.replication_factor {
                // try to find a candidate that is doing less work
                if let Some((&node, vns)) = node_map
                    .iter_mut()
                    .filter(|&(n, _)| !vn.owners.contains_key(n))
                    .min_by_key(|&(_, ref p)| p.len())
                {
                    assert!(vns.insert(vn_no));
                    assert!(vn.owners.insert(node, Pending).is_none());
                    continue;
                }
                // try to find candidate that was retiring from that vnode
                // only consider nodes in node_map (others could be leaving/invalid)
                if let Some((_, node)) = vn.owners
                    .iter()
                    .filter_map(|(&n, &s)| {
                        if s == Retiring {
                            node_map.get(&n).map(|p| (p.len(), n))
                        } else {
                            None
                        }
                    })
                    .min()
                {
                    assert!(node_map.get_mut(&node).unwrap().insert(vn_no));
                    assert!(vn.owners.insert(node, Owner).is_some());
                    continue;
                }
                unreachable!(
                    "Can't find replica for vnode {} {:?} rf:{}",
                    vn_no,
                    vn.owners,
                    self.replication_factor
                );
            }
        }

        // 2. robin-hood
        for (vn_no, vn) in self.vnodes.iter_mut().enumerate() {
            let doing_much: IdHashSet<_> = vn.owners
                .iter()
                .filter(|&(n, &s)| {
                    s != Retiring && node_map.get(n).unwrap().len() > vnpn
                })
                .map(|(n, _)| *n)
                .collect();
            let candidates: IdHashSet<_> = node_map
                .iter()
                .filter(|&(n, vns)| vns.len() < vnpn && !vn.owners.contains_key(n))
                .map(|(n, _)| *n)
                .collect();
            for (from, to) in doing_much.into_iter().zip(candidates) {
                assert!(vn.owners.insert(to, Pending).is_none());
                assert!(vn.owners.insert(from, Retiring).is_some());
                assert!(node_map.get_mut(&from).unwrap().remove(&vn_no));
                assert!(node_map.get_mut(&to).unwrap().insert(vn_no));
            }
        }

        self.is_valid()
    }

    #[cfg(test)]
    fn finish_rebalance(&mut self, this: NodeId) -> Result<(), GenericError> {
        self.is_valid().unwrap();
        self.version.event(this);
        for vn in self.vnodes.iter_mut() {
            vn.owners = vn.owners
                .iter()
                .filter_map(|(&n, &s)| {
                    if s == Retiring {
                        None
                    } else {
                        Some((n, Owner))
                    }
                })
                .collect();
        }
        self.is_valid()
    }

    fn is_valid(&self) -> Result<(), GenericError> {
        let valid_nodes_count = self.valid_nodes_count();
        let desired_replicas = min(valid_nodes_count, self.replication_factor);
        let vnpn = ((self.vnodes.len() * self.replication_factor) as f64 / valid_nodes_count as f64)
            .ceil() as usize;
        let vnpn_rest = self.vnodes.len() * self.replication_factor % valid_nodes_count;
        let mut node_map = IdHashMap::default();
        node_map.reserve(valid_nodes_count);

        for (vn_no, vn) in self.vnodes.iter().enumerate() {
            let owners: Vec<_> = vn.owners
                .iter()
                .filter(|&(_, &s)| s == Owner)
                .map(|(&n, _)| n)
                .collect();
            let pending: Vec<_> = vn.owners
                .iter()
                .filter(|&(_, &s)| s == Pending)
                .map(|(&n, _)| n)
                .collect();
            // let retiring: Vec<_> = vn.owners
            //     .iter()
            //     .filter(|&(_, &s)| s == Retiring)
            //     .map(|(&n, _)| n)
            //     .collect();
            if owners.len() + pending.len() != desired_replicas {
                return Err(
                    format!(
                        "vnode {} has only {:?}{:?} replicas",
                        vn_no,
                        owners,
                        pending
                    ).into(),
                );
            }
            for &node in owners.iter().chain(&pending) {
                *node_map.entry(node).or_insert(0usize) += 1;
            }
        }

        for (&n, &count) in &node_map {
            if count > vnpn + vnpn_rest {
                return Err(
                    format!(
                        "node {} is a replica for {} vnodes, expected {} max, {:?}",
                        n,
                        count,
                        vnpn + vnpn_rest,
                        node_map
                    ).into(),
                );
            }
        }

        Ok(())
    }
}

impl<T: Metadata> DHT<T> {
    pub fn init(
        fabric: Arc<Fabric>,
        config: &Config,
        meta: T,
        ring: RingDescription,
        old_node: Option<NodeId>,
    ) -> Result<DHT<T>, GenericError> {
        let addr = fabric.addr();
        let partitions = ring.partitions;
        let replication_factor = ring.replication_factor;
        assert!(
            partitions.is_power_of_two(),
            "Partition count must be a power of 2"
        );
        assert!(partitions >= 32, "Partition count must be >= 32");
        assert!(partitions <= 1024, "Partition count must be <= 1024");
        assert!(replication_factor >= 1, "Replication factor must be >= 1");
        assert!(replication_factor <= 6, "Replication factor must be <= 6");

        let dht = Self::new(fabric, config);
        let ring = Ring::new(&config.cluster_name, partitions, replication_factor);
        dht.inner.write().unwrap().ring = ring;

        if let Some(old_node) = old_node {
            dht.replace_node(old_node, dht.node, addr, meta).unwrap();
        } else {
            dht.join_node(dht.node, addr, meta).unwrap();
        }
        dht.rebalance().unwrap();

        Ok(dht)
    }

    pub fn restore(
        fabric: Arc<Fabric>,
        config: &Config,
        meta: T,
        serialized_ring: &[u8],
        old_node: Option<NodeId>,
    ) -> Result<DHT<T>, GenericError> {
        let addr = fabric.addr();
        let dht = Self::new(fabric, config);
        let ring = Ring::deserialize(serialized_ring)?;
        dht.inner.write().unwrap().ring = ring;

        if let Some(old_node) = old_node {
            dht.replace_node(old_node, dht.node, addr, meta).unwrap();
        } else {
            dht.join_node(dht.node, addr, meta).unwrap();
        }

        Ok(dht)
    }

    pub fn join_cluster(
        fabric: Arc<Fabric>,
        config: &Config,
        meta: T,
        seeds: &[SocketAddr],
        old_node: Option<NodeId>,
    ) -> Result<DHT<T>, GenericError> {
        let addr = fabric.addr();
        let dht = Self::new(fabric.clone(), config);

        info!("Registering seeds {:?}", seeds);
        for &seed in seeds {
            if seed != addr {
                fabric.register_seed(seed);
            }
        }

        info!("Connecting to seeds");
        let mut connections = Vec::new();
        for _ in 0..10 {
            thread::sleep(Duration::from_millis(500));
            connections = fabric.connections();
            if connections.len() >= seeds.len() / 2 + 1 {
                break;
            }
        }
        if connections.is_empty() {
            return Err("Cannot contact seed nodes".into());
        }
        for _ in 0..10 {
            thread::sleep(Duration::from_millis(500));
            if !dht.inner.write().unwrap().ring.vnodes.is_empty() {
                break;
            }
        }
        if dht.inner.write().unwrap().ring.vnodes.is_empty() {
            return Err("Didn't receive seed ring".into());
        }

        if let Some(old_node) = old_node {
            dht.replace_node(old_node, dht.node, addr, meta).unwrap();
        } else {
            dht.join_node(dht.node, addr, meta).unwrap();
        }
        Ok(dht)
    }

    fn new(fabric: Arc<Fabric>, config: &Config) -> DHT<T> {
        let inner = Arc::new(RwLock::new(Inner {
            node: fabric.node(),
            ring: Ring::new(&config.cluster_name, 0, 0),
            callback: Default::default(),
            fabric: fabric.clone(),
            next_req_broadcast: Instant::now(),
            sync_aae: config.dht_sync_aae,
            sync_on_connect: config.dht_sync_on_connect,
        }));

        // TODO: move this to Database
        let w_inner1 = Arc::downgrade(&inner);
        let w_inner2 = Arc::downgrade(&inner);
        let msg_cb = move |from, msg| if let Some(inner) = w_inner1.upgrade() {
            Self::on_message(&mut *inner.write().unwrap(), from, msg);
        };
        let con_cb = move |peer| if let Some(inner) = w_inner2.upgrade() {
            Self::on_connection(&*inner.read().unwrap(), peer);
        };
        fabric.register_msg_handler(FabricMsgType::DHT, Box::new(msg_cb));
        fabric.register_con_handler(Box::new(con_cb));

        DHT {
            node: fabric.node(),
            inner: inner,
        }
    }

    fn on_connection(inner: &Inner<T>, from: NodeId) {
        if inner.sync_on_connect && !inner.ring.vnodes.is_empty() {
            let serialized_ring: Bytes = Ring::serialize(&inner.ring).unwrap().into();
            let _ = inner
                .fabric
                .send_msg(from, &FabricMsg::DHTSync(serialized_ring));
        }
    }

    fn on_message(inner: &mut Inner<T>, from: NodeId, msg: FabricMsg) {
        match msg {
            FabricMsg::DHTAE(version) => if inner.ring.vnodes.is_empty() {
                warn!("Can't reply DHTAE while starting up");
            } else if !version.descends(&inner.ring.version) {
                // received version precends the local v
                let serialized_ring: Bytes = Ring::serialize(&inner.ring).unwrap().into();
                debug!("Replying DHTAE");
                let _ = inner
                    .fabric
                    .send_msg(from, &FabricMsg::DHTSync(serialized_ring));
            },
            FabricMsg::DHTSync(bytes) => {
                debug!("Incoming DHTSync");
                let mut ring = inner.ring.clone();
                match Ring::deserialize(&bytes).and_then(|new| ring.merge(new)) {
                    Ok(true) => {
                        inner.ring = ring;
                        Self::call_callback(inner);
                    }
                    Ok(false) => (),
                    Err(e) => error!("Failed to process DHTSync {:?}", e),
                };
            }
            msg => unreachable!("Can't handle {:?}", msg),
        }
    }

    fn call_callback(inner: &Inner<T>) {
        if let Some(callback) = inner.callback.as_ref() {
            callback();
        }
    }

    fn broadcast(inner: &Inner<T>) {
        let msg = FabricMsg::DHTSync(Ring::serialize(&inner.ring).unwrap().into());
        for (&node_id, node) in inner.ring.nodes.iter() {
            if node_id == inner.node || node.status != NodeStatus::Valid {
                continue;
            }
            let _ = inner.fabric.send_msg(node_id, &msg);
        }
    }

    fn broadcast_req(inner: &Inner<T>) {
        let peers = inner.ring.valid_nodes_count();
        if peers <= 1 {
            // if it's only this node, bail
            return;
        }
        // calculate a chance that gives a rate of DHT_AAE_INTERVAL_MS ^ -1
        let msgs_per_call = DHT_AAE_TRIGGER_INTERVAL_MS as f32 / DHT_AAE_INTERVAL_MS as f32;
        let chance = msgs_per_call / (peers - 1) as f32;
        trace!("AAE peers {} chance {}", peers, chance);

        let mut rng = thread_rng();
        for (&node_id, node) in inner.ring.nodes.iter() {
            if node_id == inner.node || node.status != NodeStatus::Valid {
                continue;
            }
            if rng.next_f32() < chance {
                let _ = inner
                    .fabric
                    .send_msg(node_id, FabricMsgRef::DHTAE(&inner.ring.version));
            }
        }
    }

    pub fn handler_fabric_msg(&self, from: NodeId, msg: FabricMsg) {
        Self::on_message(&mut *self.inner.write().unwrap(), from, msg);
    }

    pub fn handler_tick(&self, time: Instant) {
        let r_inner = self.inner.read().unwrap();
        if r_inner.sync_aae && time >= r_inner.next_req_broadcast {
            debug!("Triggered AAE");
            Self::broadcast_req(&*r_inner);
            drop(r_inner);
            self.inner.write().unwrap().next_req_broadcast +=
                Duration::from_millis(DHT_AAE_TRIGGER_INTERVAL_MS);
        }
    }

    pub fn set_callback(&self, callback: DHTChangeFn) {
        self.inner.write().unwrap().callback = Some(callback);
    }

    pub fn node(&self) -> NodeId {
        self.node
    }

    pub fn partitions(&self) -> usize {
        // FIXME: should not lock
        self.inner.read().unwrap().ring.vnodes.len()
    }

    pub fn replication_factor(&self) -> usize {
        self.inner.read().unwrap().ring.replication_factor
    }

    pub fn key_vnode(&self, key: &[u8]) -> VNodeId {
        // use / instead of % to get continuous hash slots for each vnode
        (hash_slot(key) / (HASH_SLOTS / self.partitions() as VNodeId)) as VNodeId
    }

    pub fn vnodes_for_node(&self, node: NodeId) -> (Vec<VNodeId>, Vec<VNodeId>) {
        let mut result = (Vec::new(), Vec::new());
        let inner = self.inner.read().unwrap();
        for (vn_no, vn) in inner.ring.vnodes.iter().enumerate() {
            if let Some(&status) = vn.owners.get(&node) {
                match status {
                    Owner => result.0.push(vn_no as VNodeId),
                    Pending => result.0.push(vn_no as VNodeId),
                    Retiring => (),
                }
            }
        }
        result
    }

    // TODO: split into read_ and write_
    pub fn nodes_for_vnode(
        &self,
        vn_no: VNodeId,
        include_pending: bool,
        include_retiring: bool,
    ) -> Vec<NodeId> {
        // FIXME: this shouldn't alloc
        let inner = self.inner.read().unwrap();
        let mut result = Vec::with_capacity(inner.ring.replication_factor + 1);
        for (&node, &status) in inner.ring.vnodes[vn_no as usize].owners.iter() {
            match status {
                Owner => (),
                Pending if include_pending => (),
                Retiring if include_retiring => (),
                _ => continue,
            }
            result.push(node);
        }
        result
    }

    pub fn nodes_for_vnode_ex(
        &self,
        vn_no: VNodeId,
        include_pending: bool,
        include_retiring: bool,
    ) -> Vec<(NodeId, (SocketAddr, T))> {
        // FIXME: this shouldn't alloc
        let inner = self.inner.read().unwrap();
        let mut result = Vec::with_capacity(inner.ring.replication_factor + 1);
        for (&node_id, &status) in inner.ring.vnodes[vn_no as usize].owners.iter() {
            match status {
                Owner => (),
                Pending if include_pending => (),
                Retiring if include_retiring => (),
                _ => continue,
            }
            let node = inner.ring.nodes.get(&node_id).unwrap();
            result.push((node_id, (node.addr, node.meta.clone())));
        }
        result
    }

    pub fn save_ring(&self) -> Vec<u8> {
        Ring::serialize(&self.inner.read().unwrap().ring).expect("Can't serialize ring")
    }

    pub fn members(&self) -> IdHashMap<NodeId, SocketAddr> {
        let inner = self.inner.read().unwrap();
        inner
            .ring
            .nodes
            .iter()
            .filter(|&(_, v)| v.status != Invalid)
            .map(|(k, v)| (k.clone(), v.addr))
            .collect()
    }

    pub fn slots(&self) -> BTreeMap<(u16, u16), Vec<(NodeId, (SocketAddr, T))>> {
        let slots_per_partition = HASH_SLOTS / self.partitions() as u16;
        let mut result = BTreeMap::new();
        let inner = self.inner.read().unwrap();
        for (vn_no, vn) in inner.ring.vnodes.iter().enumerate() {
            let mut members = Vec::with_capacity(vn.owners.len());
            for (&node_id, &status) in &vn.owners {
                if status == Retiring {
                    continue;
                }
                let node = inner.ring.nodes.get(&node_id).unwrap();
                members.push((node_id, (node.addr, node.meta.clone())));
            }
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

    pub fn rebalance(&self) -> Result<(), GenericError> {
        info!("Rebalancing ring");
        self.propose(|mut ring| {
            ring.rebalance(self.node)?;
            Ok(ring)
        })
    }

    #[cfg(test)]
    pub fn finish_rebalance(&self) -> Result<(), GenericError> {
        info!("Finish Rebalancing ring");
        self.propose(|mut ring| {
            ring.finish_rebalance(self.node)?;
            Ok(ring)
        })
    }

    pub fn remove_node(&self, node: NodeId) -> Result<(), GenericError> {
        info!("Removing node {}", node);
        self.propose(|mut ring| {
            ring.remove_node(self.node, node)?;
            Ok(ring)
        })
    }

    pub fn join_node(&self, node: NodeId, addr: SocketAddr, meta: T) -> Result<(), GenericError> {
        info!("Joining node {}", node);
        self.propose(|mut ring| {
            ring.join_node(self.node, node, addr, meta)?;
            Ok(ring)
        })
    }

    pub fn replace_node(
        &self,
        old_node: NodeId,
        node: NodeId,
        addr: SocketAddr,
        meta: T,
    ) -> Result<(), GenericError> {
        info!("Replacing node {} with {}", old_node, node);
        self.propose(|mut ring| {
            ring.replace_node(self.node, old_node, node, addr, meta)?;
            Ok(ring)
        })
    }

    pub fn leave_node(&self, node: NodeId) -> Result<(), GenericError> {
        info!("Leaving node {}", node);
        self.propose(|mut ring| {
            ring.leave_node(self.node, node)?;
            Ok(ring)
        })
    }

    pub fn promote_pending_node(&self, node: NodeId, vnode: VNodeId) -> Result<(), GenericError> {
        info!("Promoting pending node {} vnode {}", node, vnode);
        self.propose(|mut ring| {
            ring.promote_pending_node(self.node, node, vnode)?;
            Ok(ring)
        })
    }

    fn propose<C>(&self, proposal: C) -> Result<(), GenericError>
    where
        C: FnOnce(Ring<T>) -> Result<Ring<T>, GenericError>,
    {
        let mut inner = self.inner.write().unwrap();
        debug!("Executing proposal");
        inner.ring = proposal(inner.ring.clone())?;
        info!("Proposing new ring version {:?}", inner.ring.version);
        Self::broadcast(&mut *inner);
        Self::call_callback(&mut *inner);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use env_logger;
    use rand::{thread_rng, Rng};
    use config::Config;
    use fabric::Fabric;
    use utils::sleep_ms;
    use utils::join_u64;

    #[test]
    fn test_ring_dup_join() {
        let mut ring = Ring::new("", 64, 3);
        let addr = "127.0.0.1:1999".parse().unwrap();
        ring.join_node(0, join_u64(0, 1), addr, ()).unwrap();
        ring.join_node(0, join_u64(1, 1), addr, ()).unwrap();

        ring.clone()
            .join_node(0, join_u64(0, 2), addr, ())
            .unwrap_err();
        ring.clone()
            .join_node(0, join_u64(0, 3), addr, ())
            .unwrap_err();

        ring.clone()
            .join_node(0, join_u64(1, 2), addr, ())
            .unwrap_err();
        ring.clone()
            .join_node(0, join_u64(1, 3), addr, ())
            .unwrap_err();
    }

    #[test]
    fn test_ring_dup_replace() {
        let mut ring = Ring::new("", 64, 3);
        let addr = "127.0.0.1:1999".parse().unwrap();
        ring.join_node(0, join_u64(0, 1), addr, ()).unwrap();
        ring.join_node(0, join_u64(1, 1), addr, ()).unwrap();

        ring.clone()
            .replace_node(0, join_u64(0, 1), join_u64(1, 2), addr, ())
            .unwrap_err();
        ring.clone()
            .replace_node(0, join_u64(1, 1), join_u64(0, 2), addr, ())
            .unwrap_err();
    }

    #[test]
    fn test_dht_init() {
        let _ = env_logger::init();
        let config: Config = Default::default();

        let node = 0;
        let addr = config.fabric_addr;
        for rf in 1u8..4 {
            let fabric = Arc::new(Fabric::new(node, &config).unwrap());
            let dht = DHT::init(fabric, &config, (), RingDescription::new(rf, 32), None).unwrap();
            assert_eq!(dht.nodes_for_vnode(0, true, false), &[node]);
            assert_eq!(dht.nodes_for_vnode(0, false, false), &[node]);
            assert_eq!(dht.nodes_for_vnode(0, false, false), &[node]);
            assert_eq!(dht.nodes_for_vnode(0, true, true), &[node]);
            assert_eq!(dht.members(), [(node, addr)].iter().cloned().collect());
        }
    }

    #[test]
    fn test_dht_join() {
        let _ = env_logger::init();
        let config1: Config = Config {
            fabric_addr: "127.0.0.1:3331".parse().unwrap(),
            ..Default::default()
        };
        let config2: Config = Config {
            fabric_addr: "127.0.0.1:3332".parse().unwrap(),
            ..Default::default()
        };

        let fabric1 = Arc::new(Fabric::new(join_u64(0, 0), &config1).unwrap());
        let dht1 = DHT::init(fabric1, &config1, (), RingDescription::new(2, 32), None).unwrap();

        let fabric2 = Arc::new(Fabric::new(join_u64(1, 0), &config2).unwrap());
        let dht2 = DHT::join_cluster(fabric2, &config2, (), &[config1.fabric_addr], None).unwrap();

        sleep_ms(100);
        for dht in &[&dht1, &dht2] {
            assert_eq!(dht.nodes_for_vnode(0, false, false), &[join_u64(0, 0)]);
            assert_eq!(dht.nodes_for_vnode(0, true, false), &[join_u64(0, 0)]);
            assert_eq!(
                dht.members(),
                [(join_u64(0, 0), config1.fabric_addr), (join_u64(1, 0), config2.fabric_addr)]
                    .iter()
                    .cloned()
                    .collect()
            );
        }

        dht1.rebalance().unwrap();
        sleep_ms(100);
        for dht in &[&dht1, &dht2] {
            assert_eq!(dht.nodes_for_vnode(0, false, false), &[join_u64(0, 0)]);
            assert_eq!(
                dht.nodes_for_vnode(0, true, false),
                &[join_u64(0, 0), join_u64(1, 0)]
            );
            assert_eq!(
                dht.members(),
                [(join_u64(0, 0), config1.fabric_addr), (join_u64(1, 0), config2.fabric_addr)]
                    .iter()
                    .cloned()
                    .collect()
            );
        }

        dht1.finish_rebalance().unwrap();
        sleep_ms(100);
        for dht in &[&dht1, &dht2] {
            assert_eq!(
                dht.nodes_for_vnode(0, false, false),
                &[join_u64(0, 0), join_u64(1, 0)]
            );
            assert_eq!(
                dht.nodes_for_vnode(0, true, false),
                &[join_u64(0, 0), join_u64(1, 0)]
            );
            assert_eq!(
                dht.members(),
                [(join_u64(0, 0), config1.fabric_addr), (join_u64(1, 0), config2.fabric_addr)]
                    .iter()
                    .cloned()
                    .collect()
            );
        }
    }

    #[test]
    #[should_panic]
    fn test_dht_join_wrong_cluster() {
        let _ = env_logger::init();
        let config1: Config = Config {
            fabric_addr: "127.0.0.1:3331".parse().unwrap(),
            ..Default::default()
        };
        let config2: Config = Config {
            fabric_addr: "127.0.0.1:3332".parse().unwrap(),
            cluster_name: "anything but default".into(),
            ..Default::default()
        };

        let fabric1 = Arc::new(Fabric::new(1, &config1).unwrap());
        let _dht1 = DHT::init(fabric1, &config1, (), RingDescription::new(2, 32), None).unwrap();

        let fabric2 = Arc::new(Fabric::new(2, &config2).unwrap());
        let _dht2 = DHT::join_cluster(fabric2, &config2, (), &[config1.fabric_addr], None).unwrap();

        sleep_ms(100);
    }

    #[test]
    fn test_dht_aae() {
        let _ = env_logger::init();
        let config1: Config = Config {
            fabric_addr: "127.0.0.1:3331".parse().unwrap(),
            ..Default::default()
        };
        let config2: Config = Config {
            fabric_addr: "127.0.0.1:3332".parse().unwrap(),
            ..Default::default()
        };

        let fabric1 = Arc::new(Fabric::new(join_u64(0, 0), &config1).unwrap());
        let dht1 = DHT::init(fabric1, &config1, (), RingDescription::new(2, 32), None).unwrap();

        let fabric2 = Arc::new(Fabric::new(join_u64(1, 0), &config2).unwrap());
        let _dht2 =
            DHT::join_cluster(fabric2.clone(), &config2, (), &[config1.fabric_addr], None).unwrap();

        sleep_ms(100);

        use fabric::{FabricMsg, FabricMsgType};
        use std::sync::{Arc, Mutex};

        let msgs: Arc<Mutex<Vec<FabricMsg>>> = Default::default();
        let msgs_ = msgs.clone();
        fabric2.register_msg_handler(
            FabricMsgType::DHT,
            Box::new(move |_, m| {
                msgs_.lock().unwrap().push(m);
            }),
        );

        dht1.handler_tick(Instant::now() + Duration::from_millis(DHT_AAE_TRIGGER_INTERVAL_MS + 1));
        sleep_ms(100);

        let mut msgs = msgs.lock().unwrap();
        match msgs.pop() {
            Some(FabricMsg::DHTAE(..)) => (),
            _ => panic!("fabric2 didn't get a DHTAE"),
        }
    }

    #[test]
    fn test_rebalance_leaving_nodes() {
        let _ = env_logger::init();
        let addr = "0.0.0.0:0".parse().unwrap();
        for _ in 0..1_000 {
            let partitions = 32;
            let mut ring = Ring::new("", partitions as u16, 1 + thread_rng().gen::<u8>() % 4);
            for i in 0..1 + thread_rng().gen::<u64>() % partitions as u64 {
                ring.join_node(0, join_u64(i as _, 0), addr, ()).unwrap();
            }
            if thread_rng().gen::<f64>() < 0.5 {
                ring.rebalance(0).unwrap();
                ring.finish_rebalance(0).unwrap();
            }

            for i in 1..thread_rng().gen::<u64>() % ring.valid_nodes_count() as u64 {
                ring.leave_node(0, join_u64(i as _, 0)).unwrap();
            }
            ring.rebalance(0).unwrap();
            ring.finish_rebalance(0).unwrap();
        }
    }

    #[test]
    fn test_rebalance() {
        let _ = env_logger::init();
        let addr = "0.0.0.0:0".parse().unwrap();
        for _ in 0..1_000 {
            let partitions = 32;
            let mut ring = Ring::new("", partitions as u16, 1 + thread_rng().gen::<u8>() % 4);
            for i in 0..1 + thread_rng().gen::<u64>() % partitions as u64 {
                ring.join_node(0, join_u64(i as _, 0), addr, ()).unwrap();
            }
            if thread_rng().gen::<f64>() < 0.5 {
                ring.rebalance(0).unwrap();
                ring.finish_rebalance(0).unwrap();
            }

            for i in 1..thread_rng().gen::<u64>() % ring.valid_nodes_count() as u64 {
                ring.remove_node(0, join_u64(i as _, 0)).unwrap();
            }
            ring.rebalance(0).unwrap();
            ring.finish_rebalance(0).unwrap();
        }
    }
}
