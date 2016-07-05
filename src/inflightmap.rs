use std::collections::{HashMap, BinaryHeap};
use std::hash::Hash;
use std::cmp::{Ord, Ordering, Eq};
use std::ops::{Deref, DerefMut};

#[derive(Debug)]
pub struct InFlightMap<K: Hash + Eq + Copy, V, T: Ord> {
    map: HashMap<K, V>,
    heap: BinaryHeap<Pair<T, K>>,
}

impl<K: Hash + Eq + Copy, V, T: Ord> InFlightMap<K, V, T> {
    pub fn new() -> InFlightMap<K, V, T> {
        InFlightMap {
            map: Default::default(),
            heap: Default::default(),
        }
    }

    pub fn insert(&mut self, key: K, value: V, timeout: T) -> Option<V> {
        self.heap.push(Pair(timeout, key.clone()));
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

impl<K: Hash + Eq + Copy, V, T: Ord> Deref for InFlightMap<K, V, T> {
    type Target = HashMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<K: Hash + Eq + Copy, V, T: Ord> DerefMut for InFlightMap<K, V, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

// Like a 2-tuple but comparison is only done for the first item
#[derive(Debug, Clone)]
struct Pair<T, V>(T, V);

impl<T: PartialEq, V> PartialEq<Pair<T, V>> for Pair<T, V> {
    fn eq(&self, other: &Pair<T, V>) -> bool {
        other.0.eq(&self.0)
    }
}

impl<T: Eq, V> Eq for Pair<T, V> {}

impl<T: PartialOrd, V> PartialOrd<Pair<T, V>> for Pair<T, V> {
    fn partial_cmp(&self, other: &Pair<T, V>) -> Option<Ordering> {
        other.0.partial_cmp(&self.0)
    }
}

impl<T: Ord, V> Ord for Pair<T, V> {
    fn cmp(&self, other: &Pair<T, V>) -> Ordering {
        other.0.cmp(&self.0)
    }
}
