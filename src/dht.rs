use std::{thread, net};
use std::sync::{Arc, RwLock};
use std::collections::{HashMap, HashSet};
use rand::{self, Rng};
use serde::{Serialize, Deserialize};
use serde_json;
use hash::hash;
use etcd;

pub type DHTChangeFn = Box<Fn() + Send + Sync>;

pub struct DHT<T: Clone + Serialize + Deserialize + Sync + Send + 'static> {
    node: net::SocketAddr,
    inner: Arc<RwLock<Inner<T>>>,
    thread: thread::JoinHandle<()>,
}

struct Inner<T: Clone + Serialize + Deserialize + Sync + Send + 'static> {
    ring: Vec<(net::SocketAddr, T)>,
    pending: HashMap<u16, (net::SocketAddr, T)>,
    etcd: etcd::Client,
    callback: Option<DHTChangeFn>,
}

impl<T: Clone + Serialize + Deserialize + Sync + Send + 'static> DHT<T> {
    pub fn new(node: net::SocketAddr, meta: T, partitions: usize) -> DHT<T> {
        let etcd = Default::default();
        let inner = Arc::new(RwLock::new(Inner {
            ring: vec![(node, meta); partitions],
            pending: Default::default(),
            etcd: etcd,
            callback: None,
        }));
        let inner_cloned = inner.clone();
        let thread = thread::spawn(move || Self::run(inner_cloned));
        DHT {
            node: node,
            inner: inner,
            thread: thread,
        }
    }

    fn run(inner: Arc<RwLock<Inner<T>>>) {
        loop {
            // listen for changes
            let node = match inner.read().unwrap().etcd.watch("dht", None, false) {
                Ok(r) => {
                    debug!("etcd: {:?}", r);
                    if let Some(n) = r.node {
                        n
                    }
                    else {
                        continue;
                    }
                }
                Err(e) => {
                    warn!("etcd err: {:?}", e);
                    continue;
                }
            };
            // update state
            {
                let (r, p) = Self::deserialize(&node.value.unwrap()).unwrap();
                let mut inner = inner.write().unwrap();
                inner.ring = r;
                inner.pending = p;
            }
            // call callback
            inner.read().unwrap().callback.as_ref().map(|f| f());
        }
    }

    pub fn reset() {
        unimplemented!()
    }

    pub fn set_callback(&self, callback: DHTChangeFn) {
        self.inner.write().unwrap().callback = Some(callback);
    }

    pub fn node(&self) -> net::SocketAddr {
        self.node
    }

    pub fn key_vnode(&self, key: &[u8]) -> u16 {
        let inner = self.inner.read().unwrap();
        (hash(key) % inner.ring.len() as u64) as u16
    }

    pub fn nodes_for_key(&self, key: &[u8], n: usize, include_pending: bool)
                         -> (u16, Vec<net::SocketAddr>) {
        let inner = self.inner.read().unwrap();
        let vnode = self.key_vnode(key);
        let ring_len = inner.ring.len();
        let mut result = HashSet::new();
        for i in 0..n {
            let vnode_i = (vnode as usize + i) % ring_len;
            if let Some(p) = inner.ring.get(vnode_i) {
                result.insert(p.0);
            }
            if include_pending {
                if let Some(p) = inner.pending.get(&(vnode_i as u16)) {
                    result.insert(p.0);
                }
            }
        }
        (vnode, result.iter().cloned().collect())
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
        for _ in 0..(members / partitions) {
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

    fn propose(&self) {
        unimplemented!()
    }

    fn serialize(v: (&Vec<(net::SocketAddr, T)>, &HashMap<u16, (net::SocketAddr, T)>))
                 -> serde_json::Result<String> {
        serde_json::to_string(&v)
    }

    fn deserialize
        (v: &str)
         -> serde_json::Result<(Vec<(net::SocketAddr, T)>, HashMap<u16, (net::SocketAddr, T)>)> {
        serde_json::from_str(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net;

    #[test]
    fn test_new() {
        let node = "127.0.0.1:9000".parse().unwrap();
        let dht = DHT::new(node, (), 256);
        assert_eq!(dht.nodes_for_key(b"abc", 1).1, &[node]);
        assert_eq!(dht.nodes_for_key(b"abc", 3).1, &[node]);
        assert_eq!(dht.members(), &[node]);
    }
}
