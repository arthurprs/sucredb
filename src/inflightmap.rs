use std::cmp::Ordering;
use std::collections::hash_map::{Entry, RandomState};
use std::collections::{BinaryHeap, HashMap};
use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::ops::Deref;

// TODO: need a more efficient implementation and possibly more flexibility

#[derive(Debug)]
pub struct InFlightMap<K: Hash + Eq + Copy, V, T: Ord + Copy, H: BuildHasher = RandomState> {
    map: HashMap<K, V, H>,
    heap: BinaryHeap<Pair<T, K>>,
}

impl<K: Hash + Eq + Copy + fmt::Debug, V, T: Ord + Copy, H: BuildHasher + Default>
    InFlightMap<K, V, T, H>
{
    pub fn new() -> Self {
        InFlightMap {
            map: Default::default(),
            heap: Default::default(),
        }
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.heap.clear();
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.map.remove(key)
    }

    pub fn entry_with_timeout(&mut self, key: K, expire: T) -> Entry<K, V> {
        self.heap.push(Pair(expire, key));
        self.map.entry(key)
    }

    pub fn entry(&mut self, key: K) -> Entry<K, V> {
        self.map.entry(key)
    }

    pub fn insert(&mut self, key: K, value: V, expire: T) -> &mut V {
        self.heap.push(Pair(expire, key));

        let mut inserted = false;
        let result = self.map.entry(key).or_insert_with(|| {
            inserted = true;
            value
        });

        if !inserted {
            panic!("{:?} is already present in the map", key);
        }

        result
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
                *self.heap.peek_mut().unwrap() = Pair(expire, key);
                return Some((key, &v));
            } else {
                self.heap.pop();
            }
        }
    }
}

impl<K: Hash + Eq + Copy, V, T: Ord + Copy, H: BuildHasher> Deref for InFlightMap<K, V, T, H> {
    type Target = HashMap<K, V, H>;

    fn deref(&self) -> &Self::Target {
        &self.map
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
