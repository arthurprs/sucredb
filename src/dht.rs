use std::net;
use hash::hash;
use std::collections::{HashMap, HashSet};
use rand::{self, Rng};
use serde::{Serialize, Deserialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct DHT<T: Clone + Serialize + Deserialize> {
    node: net::SocketAddr,
    ring: Vec<(net::SocketAddr, T)>,
    pending: HashMap<usize, (net::SocketAddr, T)>,
}

impl<T: Clone + Serialize + Deserialize> DHT<T> {
    pub fn new(node: net::SocketAddr, meta: T, partitions: usize) -> DHT<T> {
        DHT {
            node: node,
            ring: vec![(node, meta); partitions],
            pending: Default::default(),
        }
    }

    pub fn node(&self) -> net::SocketAddr {
        self.node
    }

    pub fn key_vnode(&self, key: &[u8]) -> usize {
        (hash(key) % self.ring.len() as u64) as usize
    }

    pub fn nodes_for_key(&self, key: &[u8], replication_factor: usize) -> Vec<net::SocketAddr> {
        let vnode = self.key_vnode(key);
        let ring_len = self.ring.len();
        let mut result = HashSet::new();
        for i in 0..replication_factor {
            if let Some(p) = self.ring.get((vnode + i) % ring_len) {
                result.insert(p.0);
            }
            if let Some(p) = self.pending.get(&vnode) {
                result.insert(p.0);
            }
        }
        result.iter().cloned().collect()
    }

    pub fn members(&self) -> Vec<net::SocketAddr> {
        let members_set: HashSet<_> = self.ring.iter().map(|&(n, _)| n).collect();
        members_set.iter().cloned().collect()
    }

    pub fn add_node(&mut self) {
        unimplemented!()
    }

    pub fn claim(&mut self, node: net::SocketAddr, meta: T) {
        let members = self.members().len() + 1;
        let partitions = self.ring.len();
        for _ in 0..members / partitions {
            let r = rand::thread_rng().gen::<usize>() % partitions;
            self.pending.insert(r, (node, meta.clone()));
        }
    }

    pub fn add_pending_node(&mut self, vnode: usize, node: net::SocketAddr, meta: T) {
        assert!(self.pending.insert(vnode, (node, meta)).is_none());
    }

    pub fn remove_pending_node(&mut self, vnode: usize, node: net::SocketAddr) {
        assert!(self.pending.remove(&vnode).unwrap().0 == node);
    }

    pub fn promote_pending_node(&mut self, vnode: usize, node: net::SocketAddr) {
        let p = self.pending.remove(&vnode).unwrap();
        debug_assert!(p.0 == node);
        self.ring[vnode] = p;
    }

    pub fn get_vnode(&self, vnode: usize) -> (net::SocketAddr, Option<net::SocketAddr>) {
        (self.ring.get(vnode).map(|a| a.0).unwrap(), self.pending.get(&vnode).map(|a| a.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net;

    #[test]
    fn test_new() {
        let node = net::lookup_host("127.0.0.1").unwrap().next().unwrap().unwrap();
        let dht = DHT::new(node, (), 256);
        assert_eq!(dht.nodes_for_key(b"abc", 1), &[node]);
        assert_eq!(dht.nodes_for_key(b"abc", 3), &[node]);
        assert_eq!(dht.members(), &[node]);
    }
}
