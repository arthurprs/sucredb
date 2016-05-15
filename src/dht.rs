use std::net;
use hash::hash;
use linear_map::LinearMap;

struct DHT<T> {
    ready: bool,
    ring: Vec<(net::SocketAddr, T)>,
    pending: LinearMap<usize, (net::SocketAddr, T)>,
}

impl<T> DHT<T> {
    fn new() -> DHT<T> {
        DHT {
            ready: false,
            ring: Default::default(),
            pending: Default::default(),
        }
    }

    fn nodes_for_key(&self, key: &[u8], replication_factor: usize) -> Vec<net::SocketAddr> {
        let vnode = (hash(key) / self.ring.len() as u64) as usize;
        let ring_len = self.ring.len();
        let mut result: Vec<_> = (0..replication_factor)
                                     .map(|i| self.ring.get((vnode + i) % ring_len).unwrap().0)
                                     .collect();
        if let Some(p) = self.pending.get(&vnode) {
            result.push(p.0);
        }
        result
    }

    fn add_node(&mut self) {
        unimplemented!()
    }

    fn add_pending_node(&mut self, vnode: usize, node: net::SocketAddr, meta: T) {
        assert!(self.pending.insert(vnode, (node, meta)).is_none());
    }

    fn remove_pending_node(&mut self, vnode: usize, node: net::SocketAddr) {
        if let Some(a) = self.pending.remove(&vnode) {

        }
    }

    fn promote_pending_node(&mut self, vnode: usize, node: net::SocketAddr) {
        self.ring[vnode] = self.pending.remove(&vnode).unwrap();
    }

    fn get_vnode(&self, vnode: usize) -> (net::SocketAddr, Option<net::SocketAddr>) {
        (self.ring.get(vnode).map(|a| a.0).unwrap(), self.pending.get(&vnode).map(|a| a.0))
    }
}
