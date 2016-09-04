use std::collections::{HashMap, BinaryHeap};
use std::hash::Hash;
use std::cmp::Ordering;
use std::ops::{Deref, DerefMut};

pub struct InFlightOption<V, T: Ord + Copy>(Option<(V, T)>);

impl<V, T: Ord + Copy> InFlightOption<V, T> {
    pub fn new(v: V, expire: T) -> Self {
        InFlightOption(Some((v, expire)))
    }

    pub fn pop_expired(&mut self) -> Option<V> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct InFlightMap<K: Hash + Eq + Copy, V, T: Ord + Copy> {
    map: HashMap<K, V>,
    heap: BinaryHeap<Pair<T, K>>,
}

impl<K: Hash + Eq + Copy, V, T: Ord + Copy> InFlightMap<K, V, T> {
    pub fn new() -> Self {
        InFlightMap {
            map: Default::default(),
            heap: Default::default(),
        }
    }

    pub fn insert(&mut self, key: K, value: V, expire: T) -> Option<V> {
        self.heap.push(Pair(expire, key));
        self.map.insert(key, value)
    }

    pub fn pop_expired(&mut self, now: T) -> Option<(K, V)> {
        loop {
            let key = match self.heap.peek() {
                Some(&Pair(e, k)) if now >= e => k,
                _ => return None,
            };
            self.heap.pop();
            if let Some(v) = self.map.remove(&key) {
                return Some((key, v));
            }
        }
    }

    pub fn touch_expired(&mut self, now: T, expire: T) -> Option<(K, &V)> {
        loop {
            let key = match self.heap.peek() {
                Some(&Pair(e, k)) if now >= e => k,
                _ => return None,
            };
            if let Some(v) = self.map.get(&key) {
                self.heap.replace(Pair(expire, key));
                return Some((key, &v));
            } else {
                self.heap.pop();
            }
        }
    }
}

impl<K: Hash + Eq + Copy, V, T: Ord + Copy> Deref for InFlightMap<K, V, T> {
    type Target = HashMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<K: Hash + Eq + Copy, V, T: Ord + Copy> DerefMut for InFlightMap<K, V, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

// Like a 2-tuple but comparison is only done for the first item
#[derive(Debug)]
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
