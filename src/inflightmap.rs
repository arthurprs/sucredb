use std::collections::{HashMap, BinaryHeap};
use std::hash::Hash;
use std::cmp::{Ord, Ordering, Eq};
use std::fmt;

#[derive(Debug)]
pub struct InFlightMap<K: Hash + Eq + Clone, V, T: Ord> {
    map: HashMap<K, V>,
    heap: BinaryHeap<Rev<T, K>>,
}

impl<K: Hash + Eq + Clone, V, T: Ord> InFlightMap<K, V, T> {
    pub fn new() -> InFlightMap<K, V, T> {
        InFlightMap {
            map: Default::default(),
            heap: Default::default(),
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.map.remove(key)
    }

    pub fn insert(&mut self, key: K, value: V, timeout: T) -> Option<V> {
        self.heap.push(Rev(timeout, key.clone()));
        self.map.insert(key, value)
    }

    pub fn pop_expired(&mut self, now: T) -> Option<(K, V)> {
        loop {
            match self.heap.peek() {
                Some(tv) if now >= tv.0 => (),
                _ => return None,
            }
            let key = self.heap.pop().unwrap().1;
            if let Some(v) = self.map.remove(&key) {
                return Some((key, v));
            }
        }
    }
}

struct Rev<T: Ord, V>(T, V);

impl<T: Ord, V> PartialEq<Rev<T, V>> for Rev<T, V> {
    fn eq(&self, other: &Rev<T, V>) -> bool {
        other.0.eq(&self.0)
    }
}

impl<T: Ord, V> Eq for Rev<T, V> {}

impl<T: Ord, V> PartialOrd<Rev<T, V>> for Rev<T, V> {
    fn partial_cmp(&self, other: &Rev<T, V>) -> Option<Ordering> {
        other.0.partial_cmp(&self.0)
    }
}

impl<T: Ord, V> Ord for Rev<T, V> {
    fn cmp(&self, other: &Rev<T, V>) -> Ordering {
        other.0.cmp(&self.0)
    }
}

impl<T: Ord + fmt::Debug, V: fmt::Debug> fmt::Debug for Rev<T, V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.debug_tuple("Rev").field(&self.0).field(&self.1).finish()
    }
}
