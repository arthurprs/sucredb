use std::{fmt, thread};
use std::net::SocketAddr;
use std::cmp::min;
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex};
use std::collections::BTreeMap;
use std::collections::hash_map::Entry as HMEntry;

use linear_map::{Entry as LMEntry, LinearMap};
use serde::Serialize;
use serde::de::DeserializeOwned;
use bincode;
use bytes::Bytes;

use hash::{hash_slot, HASH_SLOTS};
use database::{NodeId, VNodeId};
use version_vector::VersionVector;
use fabric::{Fabric, FabricMsg, FabricMsgType};
use utils::{GenericError, IdHashMap, IdHashSet};

pub type DHTChangeFn = Box<Fn() + Send>;
pub trait Metadata
    : Clone + PartialEq + Serialize + DeserializeOwned + Send + fmt::Debug + 'static
    {
}

impl<T: Clone + PartialEq + Serialize + DeserializeOwned + Send + fmt::Debug + 'static> Metadata
    for T {
}

const BROADCAST_REQ_INTERVAL_MS: u64 = 30000;

/// The Cluster controller, it knows how to map keys to their vnodes and
/// whose nodes hold data for each vnodes.
/// Calls a callback on cluster changes so the database can execute logic to converge
/// it's state to match the DHT.
/// Only knows about NodeIds (and their fabric addresses), Vnodes and other high level info,
/// Extra (static) information is attached to NodeId through a Metadata type.
pub struct DHT<T: Metadata> {
    node: NodeId,
    slots_per_partition: u16,
    partitions: usize,
    replication_factor: usize,
    cluster: String,
    inner: Arc<Mutex<Inner<T>>>,
}

struct Inner<T: Metadata> {
    node: NodeId,
    ring: Ring<T>,
    callback: Option<DHTChangeFn>,
    fabric: Arc<Fabric>,
    next_req_broadcast: Instant,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
enum NodeStatus {
    Valid,
    Leaving,
    Invalid,
}

use self::NodeStatus::*;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
enum VNodeNodeStatus {
    Owner,
    Pending,
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
pub struct Ring<T: Metadata> {
    vnodes: Vec<VNode>,
    nodes: IdHashMap<NodeId, Node<T>>,
    replication_factor: usize,
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
        self.version.event(this);
        self.nodes.insert(
            node,
            Node {
                addr,
                meta,
                status: Valid,
                version: {
                    let mut version = VersionVector::new();
                    version.event(this);
                    version
                },
            },
        );
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
            }
        } else {
            return Err(format!("{} is not part of vnodes[{}]", promoted, vn_no).into());
        }
        Ok(())
    }

    fn replace(
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
        Ok(())
    }

    fn set_meta(&mut self, this: NodeId, node: NodeId, meta: T) -> Result<(), GenericError> {
        if let Some(node) = self.nodes.get_mut(&node) {
            if node.meta != meta {
                node.meta = meta;
                node.version.event(this);
            } else {
                return Ok(());
            }
        } else {
            return Err(format!("Node {} not found in ring nodes", node).into());
        }
        self.version.event(this);
        Ok(())
    }

    fn merge(&mut self, mut other: Self) -> Result<bool, GenericError> {
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
            return Err("other ring isn't valid".into());
        }

        if self.version.descends(&other.version) {
            return Ok(false);
        }
        if other.version.descends(&self.version) {
            *self = other;
            return Ok(true);
        }

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
            for (vnode, mut other_vnode) in self.vnodes.iter_mut().zip(other.vnodes) {
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
            }
        }

        Ok(true)
    }

    fn rebalance(&mut self, this: NodeId) -> Result<(), GenericError> {
        self.version.event(this);
        // special case when #nodes <= replication factor
        if self.valid_nodes_count() <= self.replication_factor {
            for vn in &mut self.vnodes {
                for (&node_id, node) in &self.nodes {
                    if node.status != Valid {
                        continue;
                    }
                    match vn.owners.entry(node_id) {
                        LMEntry::Vacant(v) => {
                            v.insert(Pending);
                        }
                        LMEntry::Occupied(mut o) => {
                            if *o.get() != Retiring {
                                continue;
                            }
                            *o.get_mut() = Pending;
                        }
                    }
                    vn.version.event(this);
                }
            }
            return Ok(());
        }

        // simple 2-steps eager rebalancing
        // 1. take from who is doing too much work
        // 2. assing replicas for under replicated vnodes

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

        // 1. robin-hood
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
                println!("rh ({}) {} -> {}", vn_no, from, to);
                assert!(vn.owners.insert(to, Pending).is_none());
                assert!(vn.owners.insert(from, Retiring).is_some());
                assert!(node_map.get_mut(&from).unwrap().remove(&vn_no));
                assert!(node_map.get_mut(&to).unwrap().insert(vn_no));
            }
        }

        // 2. complete replicas
        for (vn_no, vn) in self.vnodes.iter_mut().enumerate() {
            let replicas = vn.owners.values().filter(|&&s| s != Retiring).count();
            for _ in replicas..self.replication_factor {
                // try to find a candidate that is doing less work
                if let Some((&node, vns)) = node_map
                    .iter_mut()
                    .filter(|&(n, _)| !vn.owners.contains_key(n))
                    .min_by_key(|&(_, ref p)| p.len())
                {
                    println!("lw ({}) {}", vn_no, node);
                    assert!(vns.insert(vn_no));
                    assert!(vn.owners.insert(node, Pending).is_none());
                    continue;
                }
                // try to find candidate that was retiring from that vnode
                // only consider nodes in node_map (others could be leaving/invalid)
                if let Some((_, node)) = vn.owners
                    .iter()
                    .filter_map(|(&n, &s)| if s == Retiring {
                        node_map.get(&n).map(|p| (p.len(), n))
                    } else {
                        None
                    })
                    .min()
                {
                    println!("unretire ({}) {}", vn_no, node);
                    assert!(node_map.get_mut(&node).unwrap().insert(vn_no));
                    assert!(vn.owners.insert(node, Owner).is_some());
                    continue;
                }
                unreachable!(
                    "cant find replica for vnode {} {:?} rf:{}",
                    vn_no,
                    vn.owners,
                    self.replication_factor
                );
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
                .filter_map(|(&n, &s)| if s == Retiring {
                    None
                } else {
                    Some((n, Owner))
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
    pub fn init(
        fabric: Arc<Fabric>,
        cluster: &str,
        meta: T,
        ring: RingDescription,
    ) -> Result<DHT<T>, GenericError> {
        let mut dht = Self::new(fabric, cluster);
        dht.reset(cluster, meta, ring.partitions, ring.replication_factor)
            .unwrap();

        dht.partitions = ring.partitions as usize;
        dht.replication_factor = ring.replication_factor as usize;
        dht.slots_per_partition = HASH_SLOTS / dht.partitions as u16;
        Ok(dht)
    }

    pub fn restore(
        fabric: Arc<Fabric>,
        cluster: &str,
        meta: T,
        mut ring: Ring<T>,
        old_node: Option<NodeId>,
    ) -> Result<DHT<T>, GenericError> {
        if let Some(old_node) = old_node {
            ring.replace(fabric.node(), old_node, fabric.node(), fabric.addr(), meta)
                .unwrap();
        } else {
            ring.set_meta(fabric.node(), fabric.node(), meta).unwrap();
        }
        ring.is_valid().unwrap();

        let mut dht = Self::new(fabric, cluster);
        dht.partitions = ring.vnodes.len();
        dht.replication_factor = ring.replication_factor;
        dht.slots_per_partition = HASH_SLOTS / dht.partitions as u16;
        dht.inner.lock().unwrap().ring = ring;
        Ok(dht)
    }

    pub fn join_cluster(
        fabric: Arc<Fabric>,
        cluster: &str,
        meta: T,
        seeds: &[SocketAddr],
    ) -> Result<DHT<T>, GenericError> {
        let dht = Self::new(fabric.clone(), cluster);
        dht.inner
            .lock()
            .unwrap()
            .ring
            .join_node(fabric.node(), fabric.node(), fabric.addr(), meta)
            .unwrap();

        info!("registering seeds {:?}", seeds);
        for &seed in seeds {
            fabric.register_seed(seed)
        }

        info!("connecting to seeds");
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
        info!("requesting ring from seeds");
        for node in connections {
            let _ = fabric.send_msg(node, FabricMsg::DHTSyncReq(Default::default()));
        }
        for _ in 0..10 {
            thread::sleep(Duration::from_millis(500));
            if dht.partitions() != 0 {
                break;
            }
        }
        if dht.partitions() != 0 {
            Ok(dht)
        } else {
            Err("Didn't receive seed ring".into())
        }
    }

    fn new(fabric: Arc<Fabric>, cluster: &str) -> DHT<T> {
        let inner = Arc::new(Mutex::new(Inner {
            node: fabric.node(),
            ring: Ring::new(cluster, 0, 0),
            callback: None,
            fabric: fabric.clone(),
            next_req_broadcast: Instant::now(),
        }));

        // TODO: move this to Database
        // let w_inner1 = Arc::downgrade(&inner);
        // let w_inner2 = Arc::downgrade(&inner);
        // let msg_cb = move |from, msg| if let Some(inner) = w_inner1.upgrade() {
        //     Self::on_message(&mut *inner.lock().unwrap(), from, msg);
        // };
        // let con_cb = move |peer| if let Some(inner) = w_inner2.upgrade() {
        //     Self::on_connection(&mut *inner.lock().unwrap(), peer);
        // };
        // fabric.register_msg_handler(FabricMsgType::DHT, Box::new(msg_cb));
        // fabric.register_con_handler(Box::new(con_cb));

        DHT {
            node: fabric.node(),
            cluster: cluster.into(),
            inner: inner,
            slots_per_partition: 0,
            partitions: 0,
            replication_factor: 0,
        }
    }

    fn on_connection(inner: &mut Inner<T>, from: NodeId) {
        if !inner.ring.vnodes.is_empty() {
            let _ = inner.fabric.send_msg(
                from,
                FabricMsg::DHTSync(Self::serialize(&inner.ring).unwrap().into()),
            );
        }
    }

    fn on_message(inner: &mut Inner<T>, from: NodeId, msg: FabricMsg) {
        match msg {
            FabricMsg::DHTSyncReq(version) => if inner.ring.vnodes.is_empty() {
                error!("Can't reply DHTSyncReq while starting up");
            } else if inner.ring.version != version {
                let _ = inner.fabric.send_msg(
                    from,
                    FabricMsg::DHTSync(Self::serialize(&inner.ring).unwrap().into()),
                );
            },
            FabricMsg::DHTSync(bytes) => {
                let sync_res = Self::deserialize(&bytes).and_then(|new| inner.ring.merge(new));
                match sync_res {
                    Ok(true) => if let Some(callback) = inner.callback.as_ref() {
                        callback();
                    },
                    Ok(false) => (),
                    Err(e) => error!("Failed to process DHTSync {:?}", e),
                };
            }
            msg => unreachable!("Can't handle {:?}", msg),
        }
    }

    fn broadcast(inner: &mut Inner<T>) {
        let serialized_ring: Bytes = Self::serialize(&inner.ring).unwrap().into();
        for &node in inner.ring.nodes.keys() {
            if node == inner.node {
                continue;
            }
            let _ = inner
                .fabric
                .send_msg(node, FabricMsg::DHTSync(serialized_ring.clone()));
        }
        if let Some(callback) = inner.callback.as_ref() {
            callback();
        }
    }

    fn broadcast_req(inner: &mut Inner<T>) {
        for &node in inner.ring.nodes.keys() {
            if node == inner.node {
                continue;
            }
            let _ = inner
                .fabric
                .send_msg(node, FabricMsg::DHTSyncReq(inner.ring.version.clone()));
        }
    }

    fn reset(
        &self,
        cluster: &str,
        meta: T,
        partitions: u16,
        replication_factor: u8,
    ) -> Result<(), GenericError> {
        assert!(
            partitions.is_power_of_two(),
            "Partition count must be a power of 2"
        );
        assert!(partitions >= 32, "Partition count must be >= 32");
        assert!(partitions <= 1024, "Partition count must be <= 1024");
        assert!(replication_factor >= 1, "Replication factor must be >= 1");
        assert!(replication_factor <= 6, "Replication factor must be <= 6");

        let mut inner = self.inner.lock().unwrap();
        let addr = inner.fabric.addr();
        inner.ring = Ring::new(cluster, partitions, replication_factor);
        inner
            .ring
            .join_node(self.node, self.node, addr, meta)
            .unwrap();
        inner.ring.rebalance(self.node).unwrap();
        Ok(())
    }

    pub fn handler_tick(&self, time: Instant) {
        let mut inner = self.inner.lock().unwrap();
        if time >= inner.next_req_broadcast {
            inner.next_req_broadcast += Duration::from_millis(BROADCAST_REQ_INTERVAL_MS);
            Self::broadcast_req(&mut *inner);
        }
    }

    pub fn set_callback(&self, callback: DHTChangeFn) {
        self.inner.lock().unwrap().callback = Some(callback);
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

    pub fn vnodes_for_node(&self, node: NodeId) -> [Vec<VNodeId>; 3] {
        let mut result = [Vec::new(), Vec::new(), Vec::new()];
        let inner = self.inner.lock().unwrap();
        for (vn_no, vn) in inner.ring.vnodes.iter().enumerate() {
            if let Some(status) = vn.owners.get(&node) {
                result[*status as usize].push(vn_no as VNodeId);
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
        let mut result = Vec::with_capacity(self.replication_factor + 1);
        let inner = self.inner.lock().unwrap();
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
        let mut result = Vec::with_capacity(self.replication_factor + 1);
        let inner = self.inner.lock().unwrap();
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

    pub fn members(&self) -> IdHashMap<NodeId, SocketAddr> {
        let inner = self.inner.lock().unwrap();
        inner
            .ring
            .nodes
            .iter()
            .map(|(k, v)| (k.clone(), v.addr))
            .collect()
    }

    pub fn slots(&self) -> BTreeMap<(u16, u16), Vec<(NodeId, (SocketAddr, T))>> {
        let slots_per_partition = HASH_SLOTS / self.partitions() as u16;
        let mut result = BTreeMap::new();
        let inner = self.inner.lock().unwrap();
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

    fn ring_clone(&self) -> Ring<T> {
        self.inner.lock().unwrap().ring.clone()
    }

    pub fn rebalance(&self) -> Result<(), GenericError> {
        self.propose(|| {
            let mut ring = self.ring_clone();
            ring.rebalance(self.node)?;
            Ok(ring)
        })
    }

    pub fn remove_node(&self, node: NodeId) -> Result<(), GenericError> {
        self.propose(|| {
            let mut ring = self.ring_clone();
            ring.remove_node(self.node, node)?;
            Ok(ring)
        })
    }

    pub fn leave_node(&self, node: NodeId) -> Result<(), GenericError> {
        self.propose(|| {
            let mut ring = self.ring_clone();
            ring.leave_node(self.node, node)?;
            Ok(ring)
        })
    }

    pub fn promote_pending_node(&self, node: NodeId, vnode: VNodeId) -> Result<(), GenericError> {
        self.propose(|| {
            let mut ring = self.ring_clone();
            ring.promote_pending_node(self.node, node, vnode)?;
            Ok(ring)
        })
    }

    fn propose<C>(&self, proposal: C) -> Result<(), GenericError>
    where
        C: Fn() -> Result<Ring<T>, GenericError>,
    {
        let new_ring = proposal()?;
        info!("Proposing new ring version {:?}", new_ring.version);
        let mut inner = self.inner.lock().unwrap();
        if inner.ring.merge(new_ring)? {
            Self::broadcast(&mut *inner);
        }
        Ok(())
    }

    fn serialize(ring: &Ring<T>) -> Result<Vec<u8>, GenericError> {
        bincode::serialize(ring, bincode::Infinite).map_err(Into::into)
    }

    fn deserialize(bytes: &[u8]) -> Result<Ring<T>, GenericError> {
        bincode::deserialize(bytes).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net;
    use config;
    use rand::{self, thread_rng, Rng};
    use env_logger;
    //
    // #[test]
    // fn test_new() {
    //     let node = 0;
    //     let addr = "127.0.0.1:9000".parse().unwrap();
    //     for rf in 1..4 {
    //         let dht = DHT::new(
    //             node,
    //             addr,
    //             "test",
    //             (),
    //             Some(RingDescription::new(rf, 256)),
    //             None,
    //         );
    //         assert_eq!(dht.nodes_for_vnode(0, true, false), &[node]);
    //         assert_eq!(dht.nodes_for_vnode(0, false, false), &[node]);
    //         assert_eq!(dht.nodes_for_vnode(0, false, false), &[node]);
    //         assert_eq!(dht.nodes_for_vnode(0, true, true), &[node]);
    //         assert_eq!(dht.members(), [(node, addr)].iter().cloned().collect());
    //     }
    // }

    #[test]
    fn test_rebalance_leaving_nodes() {
        let _ = env_logger::init();
        let addr = "0.0.0.0:0".parse().unwrap();
        for i in 0..1_000 {
            let partitions = 32;
            let mut ring = Ring::new("", partitions as u16, 1 + thread_rng().gen::<u8>() % 4);
            for i in 0..1 + thread_rng().gen::<u64>() % partitions as u64 {
                ring.join_node(i, i, addr, ()).unwrap();
            }
            if thread_rng().gen::<f64>() < 0.5 {
                ring.rebalance(0).unwrap();
                ring.finish_rebalance(0).unwrap();
            }

            for i in 1..thread_rng().gen::<u64>() % ring.valid_nodes_count() as u64 {
                ring.leave_node(i, i).unwrap();
            }
            ring.rebalance(0).unwrap();
            ring.finish_rebalance(0).unwrap();
        }
    }

    #[test]
    fn test_rebalance() {
        let _ = env_logger::init();
        let addr = "0.0.0.0:0".parse().unwrap();
        for i in 0..1_000 {
            let partitions = 32;
            let mut ring = Ring::new("", partitions as u16, 1 + thread_rng().gen::<u8>() % 4);
            for i in 0..1 + thread_rng().gen::<u64>() % partitions as u64 {
                ring.join_node(i, i, addr, ()).unwrap();
            }
            println!("{:?}", ring);
            if thread_rng().gen::<f64>() < 0.5 {
                ring.rebalance(0).unwrap();
                ring.finish_rebalance(0).unwrap();
            }

            for i in 1..thread_rng().gen::<u64>() % ring.valid_nodes_count() as u64 {
                ring.remove_node(i, i).unwrap();
            }
            println!("{:?}", ring);
            let r = ring.rebalance(0);
            println!("{:?}", ring);
            r.unwrap();
            ring.finish_rebalance(0).unwrap();
        }
    }
}
