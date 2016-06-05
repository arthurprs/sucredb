use std::net;
use hash::hash;
use std::collections::{HashMap, HashSet};
use rand::{self, Rng};
use serde::{Serialize, Deserialize};
use std::sync::RwLock;

pub struct DHT<T: Clone + Serialize + Deserialize> {
    node: net::SocketAddr,
    inner: RwLock<Inner<T>>,
}

struct Inner<T: Clone + Serialize + Deserialize> {
    ring: Vec<(net::SocketAddr, T)>,
    pending: HashMap<u16, (net::SocketAddr, T)>,
}

impl<T: Clone + Serialize + Deserialize> DHT<T> {
    pub fn new(node: net::SocketAddr, meta: T, partitions: usize) -> DHT<T> {
        DHT {
            node: node,
            inner: RwLock::new(Inner {
                ring: vec![(node, meta); partitions],
                pending: Default::default(),
            }),
        }
    }

    pub fn node(&self) -> net::SocketAddr {
        self.node
    }

    pub fn key_vnode(&self, key: &[u8]) -> u16 {
        let inner = self.inner.read().unwrap();
        (hash(key) % inner.ring.len() as u64) as u16
    }

    pub fn nodes_for_key(&self, key: &[u8], replication_factor: usize) -> Vec<net::SocketAddr> {
        let inner = self.inner.read().unwrap();
        let vnode = self.key_vnode(key);
        let ring_len = inner.ring.len();
        let mut result = HashSet::new();
        for i in 0..replication_factor {
            let vnode_i = (vnode as usize + i) % ring_len;
            if let Some(p) = inner.ring.get(vnode_i) {
                result.insert(p.0);
            }
            if let Some(p) = inner.pending.get(&(vnode_i as u16)) {
                result.insert(p.0);
            }
        }
        result.iter().cloned().collect()
    }

    pub fn members(&self) -> Vec<net::SocketAddr> {
        let inner = self.inner.read().unwrap();
        let members_set: HashSet<_> = inner.ring.iter().map(|&(n, _)| n).collect();
        members_set.iter().cloned().collect()
    }

    pub fn add_node(&self) {
        unimplemented!()
    }

    pub fn claim(&self, node: net::SocketAddr, meta: T) -> Vec<u16> {
        let mut moves = Vec::new();
        let members = self.members().len() + 1;
        let mut inner = self.inner.write().unwrap();
        let partitions = inner.ring.len();
        for _ in 0..members / partitions {
            let r = (rand::thread_rng().gen::<usize>() % partitions) as u16;
            inner.pending.insert(r, (node, meta.clone()));
            moves.push(r);
        }
        moves
    }

    pub fn add_pending_node(&self, vnode: u16, node: net::SocketAddr, meta: T) {
        let mut inner = self.inner.write().unwrap();
        assert!(inner.pending.insert(vnode, (node, meta)).is_none());
    }

    pub fn remove_pending_node(&self, vnode: u16, node: net::SocketAddr) {
        let mut inner = self.inner.write().unwrap();
        assert!(inner.pending.remove(&vnode).unwrap().0 == node);
    }

    pub fn promote_pending_node(&self, vnode: u16, node: net::SocketAddr) {
        let mut inner = self.inner.write().unwrap();
        let p = inner.pending.remove(&vnode).unwrap();
        debug_assert!(p.0 == node);
        inner.ring[vnode as usize] = p;
    }

    pub fn get_vnode(&self, vnode: u16) -> (net::SocketAddr, Option<net::SocketAddr>) {
        let inner = self.inner.read().unwrap();
        (inner.ring.get(vnode as usize).map(|a| a.0).unwrap(),
         inner.pending.get(&vnode).map(|a| a.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net;

    #[test]
    fn test_new() {
        let node = "127.0.0.1".parse().unwrap();
        let dht = DHT::new(node, (), 256);
        assert_eq!(dht.nodes_for_key(b"abc", 1), &[node]);
        assert_eq!(dht.nodes_for_key(b"abc", 3), &[node]);
        assert_eq!(dht.members(), &[node]);
    }
}
