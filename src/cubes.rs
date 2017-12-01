use std::time;
use std::boxed::FnBox;
use bytes::Bytes;
use version_vector::*;
use linear_map::{Entry as LMEntry, LinearMap};
use command::CommandError;
use resp::RespValue;
use bincode;

pub type MutatorFn = Box<
    FnBox(Id, Version, Cube) -> Result<(Cube, Option<RespValue>), CommandError> + Send,
>;
pub type ResponseFn = Box<FnMut(Cube) -> RespValue + Send>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Cube {
    // the order is used to merge different types in a deterministic way
    Counter(Counter),
    Value(Value),
    Map(Map),
    Set(Set),
    Void(VersionVector),
}

macro_rules! impl_into{
    ($s:ident, $v:ident) => {
        pub fn $s(self) -> Option<$v>{
            match self {
                Cube::$v(a) => Some(a),
                Cube::Void(a) => Some($v::with(a)),
                _ => None,
            }
        }
    }
}

impl Default for Cube {
    fn default() -> Self {
        Cube::Void(Default::default())
    }
}

impl Cube {
    pub fn is_subsumed(&self, bvv: &BitmappedVersionVector) -> bool {
        use self::Cube::*;
        match *self {
            Counter(ref a) => a.values.is_empty() && a.vv.contained(bvv),
            Value(ref a) => a.values.is_empty() && a.vv.contained(bvv),
            Map(ref a) => a.values.is_empty() && a.vv.contained(bvv),
            Set(ref a) => a.values.is_empty() && a.vv.contained(bvv),
            Void(_) => unreachable!(),
        }
    }

    impl_into!(into_value, Value);
    impl_into!(into_counter, Counter);
    impl_into!(into_map, Map);
    impl_into!(into_set, Set);

    // minimum set of dots required to assemble this cube
    // see comment at the bottom
    pub fn for_each_dot<CB: FnMut(Id, Version)>(&self, mut cb: CB) {
        use self::Cube::*;
        match *self {
            Counter(ref a) => a.values.iter().for_each(|(&i, &(v, _))| cb(i, v)),
            Value(ref a) => a.values.iter().for_each(|(&(i, v), _)| cb(i, v)),
            Map(ref a) => a.dots.iter().for_each(|(i, v)| cb(i, v)),
            Set(ref a) => a.dots.iter().for_each(|(i, v)| cb(i, v)),
            Void(_) => unreachable!(),
        }
    }

    pub fn new(bvv: &BitmappedVersionVector) -> Cube {
        let mut vv = VersionVector::new();
        for (&n, bv) in bvv.iter() {
            vv.add(n, bv.base());
        }
        Cube::Void(vv)
    }

    pub fn del(&mut self, id: Id, version: Version, vv: &VersionVector) -> bool {
        use self::Cube::*;
        match *self {
            Counter(ref mut a) => a.clear(id, version),
            Value(ref mut a) => a.set(id, version, None, vv),
            Map(ref mut a) => a.clear(id, version),
            Set(ref mut a) => a.clear(id, version),
            Void(_) => return false,
        }
        true
    }

    pub fn merge(self, other: Self) -> Self {
        use self::Cube::*;
        match (self, other) {
            (Counter(a), Counter(b)) => Counter(a.merge(b)),
            (Value(a), Value(b)) => Value(a.merge(b)),
            (Map(a), Map(b)) => Map(a.merge(b)),
            (Set(a), Set(b)) => Set(a.merge(b)),
            (Void(vv), a) | (a, Void(vv)) => match a {
                Counter(a) => Counter(a.merge(self::Counter::with(vv))),
                Value(a) => Value(a.merge(self::Value::with(vv))),
                Map(a) => Map(a.merge(self::Map::with(vv))),
                Set(a) => Set(a.merge(self::Set::with(vv))),
                Void(mut o_vv) => {
                    o_vv.merge(&vv);
                    Void(o_vv)
                }
            },
            (a, b) => {
                warn!("Merging Cubes with different types");
                #[allow(unreachable_patterns)]
                match (a, b) {
                    (Counter(a), _) | (_, Counter(a)) => Counter(a),
                    (Value(a), _) | (_, Value(a)) => Value(a),
                    (Map(a), _) | (_, Map(a)) => Map(a),
                    (Set(a), _) | (_, Set(a)) => Set(a),
                    (Void(_), _) | (_, Void(_)) => unreachable!(),
                }
            }
        }
    }
}

// RWCounter
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Counter {
    values: LinearMap<Id, (Version, i64)>,
    vv: VersionVector,
}

impl Counter {
    fn with(vv: VersionVector) -> Self {
        Counter {
            values: Default::default(),
            vv,
        }
    }

    pub fn get(&self) -> i64 {
        self.values.values().map(|&(_, c)| c).sum()
    }

    pub fn inc(&mut self, node: Id, version: Version, by: i64) -> i64 {
        self.vv.add(node, version);
        let version_counter = self.values.entry(node).or_insert((0, 0));
        version_counter.0 = version;
        version_counter.1 += by;
        version_counter.1
    }

    pub fn clear(&mut self, node: Id, version: Version) {
        self.values.clear();
        self.vv.add(node, version);
    }

    fn merge(mut self, other: Self) -> Self {
        for (id, other) in other.values {
            match self.values.entry(id) {
                LMEntry::Occupied(mut oc) => if other.0 > oc.get().0 {
                    *oc.get_mut() = other;
                },
                LMEntry::Vacant(va) => {
                    va.insert(other);
                }
            }
        }
        self
    }
}

// MultiRegister
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Value {
    values: DotMap<Option<Bytes>>,
    vv: VersionVector,
}

impl Value {
    fn with(vv: VersionVector) -> Self {
        Value {
            values: Default::default(),
            vv,
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn set(&mut self, node: Id, version: Version, value: Option<Bytes>, vv: &VersionVector) {
        self.values.discard(vv);
        self.values.insert(node, version, value);
        self.vv.add(node, version);
    }

    fn merge(mut self, mut other: Self) -> Self {
        self.values.merge(&mut other.values, &self.vv, &other.vv);
        self.vv.merge(&other.vv);
        self
    }
}


/// Actor Observed removal
/// Add wins on conflict
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Set {
    values: CausalMap<Bytes, DotSet>,
    dots: VersionVector,
    vv: VersionVector,
}

impl Set {
    fn with(vv: VersionVector) -> Self {
        Set {
            values: Default::default(),
            dots: Default::default(),
            vv,
        }
    }

    pub fn insert(&mut self, node: Id, version: Version, item: Bytes) -> bool {
        let result = self.values
            .insert(item, DotSet::from_dot((node, version)))
            .is_none();
        self.vv.add(node, version);
        self.dots.add(node, version);
        result
    }

    pub fn remove(&mut self, node: Id, version: Version, item: &[u8]) -> bool {
        let result = self.values.remove(item).is_some();
        self.vv.add(node, version);
        self.dots.add(node, version);
        result
    }

    pub fn clear(&mut self, node: Id, version: Version) {
        self.values.clear();
        self.vv.add(node, version);
        self.dots.add(node, version);
    }

    fn merge(mut self, mut other: Self) -> Self {
        self.values.merge(&mut other.values, &self.vv, &other.vv);
        self.vv.merge(&other.vv);
        self.dots.merge(&other.dots);
        self
    }
}

// Actor Observed removal
// LWW on value conflict (max as tiebreaker)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Map {
    values: CausalMap<Bytes, MapValue>,
    dots: VersionVector,
    vv: VersionVector,
}

impl Map {
    fn with(vv: VersionVector) -> Self {
        Map {
            values: Default::default(),
            dots: Default::default(),
            vv,
        }
    }

    pub fn insert(&mut self, node: Id, version: Version, key: Bytes, value: Bytes) -> bool {
        let result = self.values
            .insert(key, MapValue::new((node, version), value))
            .is_none();
        self.vv.add(node, version);
        self.dots.add(node, version);
        result
    }

    pub fn remove(&mut self, node: Id, version: Version, key: &[u8]) -> bool {
        let result = self.values.remove(key).is_some();
        self.vv.add(node, version);
        self.dots.add(node, version);
        result
    }

    pub fn clear(&mut self, node: Id, version: Version) {
        self.values.clear();
        self.vv.add(node, version);
        self.dots.add(node, version);
    }

    fn merge(mut self, mut other: Self) -> Self {
        self.values.merge(&mut other.values, &self.vv, &other.vv);
        self.vv.merge(&other.vv);
        self.dots.merge(&other.dots);
        self
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct MapValue {
    dots: DotSet,
    value: Bytes,
    timestamp: u64, // millis since epoch
}

impl MapValue {
    fn new(dot: (Id, Version), value: Bytes) -> Self {
        let timestamp = time::UNIX_EPOCH.elapsed().unwrap();
        MapValue {
            dots: DotSet::from_dot(dot),
            value,
            timestamp: timestamp.as_secs() * 1_000 + (timestamp.subsec_nanos() / 1_000_000) as u64,
        }
    }
}

impl CausalValue for MapValue {
    fn merge<VV: AbsVersionVector>(&mut self, other: &mut Self, s_vv: &VV, o_vv: &VV) {
        self.dots.merge(&mut other.dots, s_vv, o_vv);
        // resolve possible value collision
        // if timestamps are equal value becomes max(a, b)
        if self.timestamp > other.timestamp {
            // nothing to do
        } else if other.timestamp > self.timestamp || other.value > self.value {
            self.timestamp = other.timestamp;
            ::std::mem::swap(&mut self.value, &mut other.value);
        }
    }

    fn is_empty(&self) -> bool {
        self.dots.is_empty()
    }
}

pub fn render_value_or_counter(cube: Cube) -> RespValue {
    match cube {
        Cube::Value(v) => {
            let serialized_vv = bincode::serialize(&v.vv, bincode::Infinite).unwrap();
            let mut values: Vec<_> = v.values
                .into_iter()
                .filter_map(|(_, ov)| ov.map(RespValue::Data))
                .collect();
            values.push(RespValue::Data(serialized_vv.into()));
            RespValue::Array(values)
        }
        Cube::Counter(c) => RespValue::Int(c.get()),
        Cube::Void(vv) => {
            let serialized_vv = bincode::serialize(&vv, bincode::Infinite).unwrap();
            RespValue::Array(vec![RespValue::Data(serialized_vv.into())])
        }
        _ => CommandError::TypeError.into(),
    }
}

pub fn render_type(cube: Cube) -> RespValue {
    use self::Cube::*;
    let ty = match cube {
        Counter(_) => "counter", // non-standard
        Value(_) => "string",
        Map(_) => "hash",
        Set(_) => "set",
        Void(_) => "none",
    };
    RespValue::Data(ty.into())
}

pub fn render_map(cube: Cube) -> RespValue {
    match cube {
        Cube::Map(m) => {
            let mut array = Vec::with_capacity(m.values.len() * 2);
            for (k, v) in m.values.into_iter() {
                array.push(RespValue::Data(k));
                array.push(RespValue::Data(v.value));
            }
            RespValue::Array(array)
        }
        Cube::Void(_) => RespValue::Array(vec![]),
        _ => CommandError::TypeError.into(),
    }
}


pub fn render_set(cube: Cube) -> RespValue {
    match cube {
        Cube::Set(s) => {
            let array = s.values
                .into_iter()
                .map(|(v, _)| RespValue::Data(v))
                .collect();
            RespValue::Array(array)
        }
        Cube::Void(_) => RespValue::Array(vec![]),
        _ => CommandError::TypeError.into(),
    }
}

/*
Using the vv from cubes to track key dots (the latest version from each node) doesn't work, example:

-> n3 is partitioned out
-> n1 "SET a v" gets dot n1-1
n1: a => [n1-1 v][n1 1] log: n1-1 => a
n2: a => [n1-1 v][n1 1] log: n1-1 => a
n3: --

-> n2 "SET a z [n1 1]" dot n2-1
n1: a => [n2-1 z][n1 1, n2 1] log: n1-1 => a, n2-1 => a
n2: a => [n2-1 z][n1 1, n2 1] log: n1-1 => a, n2-1 => a
n3: --

-> n2 "SET b y" gets dot n2-2
n1: b => [n2-2 y][n1 1, n2 2] a => [n2-1 z][n1 1, n2 1] log: n1-1 => a, n2-1 => a, n2-2 => b
n2: b => [n2-2 y][n1 1, n2 2] a => [n2-1 z][n1 1, n2 1] log: n1-1 => a, n2-1 => a, n2-2 => b
n3: --

-> n3 can receive messages
-> n2 "SET c y" gets dot n2-3 (merges with void cube w/ [n1 1, n2 2])
n1: c => [n2-3 y][n1 1, n2 3] b => [n2-2 y][n1 1, n2 2] a => [n2-1 z][n1 1, n2 1] log: n1-1 => a, n2-1 => a, n2-2 => b, n2-3 => c
n2: c => [n2-3 y][n1 1, n2 3] b => [n2-2 y][n1 1, n2 2] a => [n2-1 z][n1 1, n2 1] log: n1-1 => a, n2-1 => a, n2-2 => b, n2-3 => c
n3: c => [n2-3 y][n1 1, n2 3] log: n2-3 => c, n1-1 => c

n3 stores (n1-1 => c) in dot log but that's wrong

Problem:
VV from Voids pollutes the vv of new writes.

Solution:
For the MultiRegister the DotMap with optional values works as it's own dot tracker,
even if it can have more than 1 version per actor the number s should stay low.
Counters are similar to the MultiRegister case.
Sets/Maps carry an additional VV to track the dots.
Voids don't need any dot, their state is empty and they history is contained in the node clock (previous dots).

*/

/*
Optimized bootstrap dones't work?

Given n1 with lots of data churn and the only kv left is k => [n1-100 v][n1 100]
node clock says [n1 1000] and logs have all the expected entries

When n2 bootstrap n1 will send only non-deleted kvs as this is always <= number of dots (optimized bootstrap).
Thus only k => [n1-100 v][n1 100] is sent.
n2 will store k as above and log will contain only n1-100 => k
syncfin will update n2 node clock to [n1 1000]

If asked for any key other than k it'll return the same response as n1.
That is a void with a causal context [n1 1000]

Problem:
What if n3 asks to sync dots n1-101 n1-102 ... with n2? It'll get nothing but it'll get it's n1 clock bumped to 1000
If key y was deleted as dot n1-101 the delete will never get propagated to n3.

Fix: on SyncFin sync only remote (n2) part of clock, like: n3LocalClock[n2].merge(n2RemoteClock[n2])
It was in the updated paper all the time, I just didn't see it.

*/

/*
AAE based bootstrap any better?

Given n1 with lots of data churn and the only kv left is k => [n1-100 v][n1 100]
node clock says [n1 1000] and logs have all the expected entries

n2 comes up (or it was partitioned the entire time) and wants to sync with n1.
It needs all dots of n1, it'll get k => [n1-100 v][n1 100] and voids for all other keys.

What if n3 asks to sync dots n1-101 n1-102 ... with n2?

Same problem and fix as the above.

*/
