use bytes::Bytes;
use version_vector::*;
use linear_map::{LinearMap, Entry as LMEntry};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Cube {
    // the order is used to merge different types in a deterministic way
    Counter(Counter),
    Value(Value),
    // List(List),
    Map(Map),
    Set(Set),
    // SortedSet(SortedSet),
    // HyperLogLog(HyperLogLog),
    Default,
}

impl Default for Cube {
    fn default() -> Cube {
        Cube::Default
    }
}

impl Cube {
    pub fn merge(self, other: Self) -> Self {
        use self::Cube::*;
        match (self, other) {
            (Counter(a), Counter(b)) => Counter(a.merge(b)),
            (Value(a), Value(b)) => Value(a.merge(b)),
            // (List(a), List(b)) => List(a.merge(b)),
            (Map(a), Map(b)) => Map(a.merge(b)),
            (Set(a), Set(b)) => Set(a.merge(b)),
            // (SortedSet(a), SortedSet(b)) => SortedSet(a.merge(b)),
            // (HyperLogLog(a), HyperLogLog(b)) => HyperLogLog(a.merge(b)),
            (Default, a) | (a, Default) => a,
            (a, b) => {
                warn!("Merging Cubes with different types");
                match (a, b) {
                    (Counter(a), _) | (_, Counter(a)) => Counter(a),
                    (Value(a), _) | (_, Value(a)) => Value(a),
                    // (List(a), _) | (_, List(a)) => List(a),
                    (Map(a), _) | (_, Map(a)) => Map(a),
                    (Set(a), _) /*| (_, Set(a))*/ => Set(a),
                    // (SortedSet(a), _) | (_, SortedSet(a))=> SortedSet(a),
                    // (HyperLogLog(a), _) | (_, HyperLogLog(a)) => HyperLogLog(a),
                }
            }
        }
    }
}

// change fabric msgs to use Cube

// on incomming replication mutations do something like
// let b = mutation.object
// let a = storage.get()
// fn merge(a: Cube, b: Cube) -> Cube
// let c = merge(a, b);
// storage.set(c)
// storage.set_log(c dots, mutation key)
// send back set_ack(c)

// client cmds send a callback fn to do_get and do_set
//
// ON do_get(key, response_callback)
// let a = vec[storage get, replicas get]
// response_callback(&db, vec[storage get, replicas get])
//
// ON do_set(key, mutation or mutation_callback, response_callback)
// if mutation:
//      send mutation (arg) to nodes
// let a = storage.get()
// if not mutation:
//      mutation = mutation_callback(&a)
//      send mutation to nodes
// let b = merge(a, mutation);
// storage.set(key, b);
// storage.set_log(mutation dot, c key)
// response_callback(&db, b)

// PNCounter
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Counter {
    // I -> (P, N)
    pn: LinearMap<Id, (u64, u64)>,
}

impl Counter {
    fn merge(mut self, other: Self) -> Self {
        for (id, (p, n)) in other.pn {
            match self.pn.entry(id) {
                LMEntry::Occupied(mut o) => {
                    let p = p.max(o.get().0);
                    let n = n.max(o.get().1);
                    *o.get_mut() = (p, n);
                },
                LMEntry::Vacant(v) => {v.insert((p, n));},
            }
        }
        self
    }
}

// MultiRegister
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Value {
    values: DotMap<Bytes>,
    dvv: DeltaVersionVector,
}

impl Value {
    fn merge(mut self, mut other: Self) -> Self {
        self.values.merge(&mut other.values, &self.dvv, &other.dvv);
        self.dvv.merge(&other.dvv);
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Set {
    values: CausalMap<Bytes, DotSet>,
    dvv: DeltaVersionVector,
}

impl Set {
    fn merge(mut self, mut other: Self) -> Self {
        self.values.merge(&mut other.values, &self.dvv, &other.dvv);
        self.dvv.merge(&other.dvv);
        self
    }
}

// Observed removal
// LWW on value conflicts
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Map {
    map: CausalMap<Bytes, MapValue>,
    dvv: DeltaVersionVector,
}

impl Map {
    fn merge(mut self, mut other: Self) -> Self {
        self.map.merge(&mut other.map, &self.dvv, &other.dvv);
        self.dvv.merge(&other.dvv);
        self
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct MapValue {
    dot_set: DotSet,
    timestamp: u64, // micro secs since epoch
    value: Bytes,
}

impl CausalValue for MapValue {
    fn merge<VV: AbsVersionVector>(&mut self, other: &mut Self, s_vv: &VV, o_vv: &VV) {
        self.dot_set.merge(&mut other.dot_set, s_vv, o_vv);
        // resolve possible value collision
        // if timestamps are equal value becomes max(a, b)
        if self.timestamp > other.timestamp {
            // nothing to do
        } else if other.value > self.value {
            self.timestamp = other.timestamp;
            ::std::mem::swap(&mut self.value, &mut other.value);
        }
    }

    fn dots<CB: FnMut((Id, Version))>(&self, cb: &mut CB) {
        self.dot_set.dots(cb);
    }

    fn is_default(&self) -> bool {
        self.dot_set.is_default()
    }
}


// struct List();
//
// struct SortedSet();
//
// struct HyperLogLog();
