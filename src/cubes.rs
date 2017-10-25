use std::time;
use bytes::Bytes;
use rand::{thread_rng, Rng};
use version_vector::*;
use linear_map::{Entry as LMEntry, LinearMap};
use command::CommandError;
use resp::RespValue;
use bincode;

pub type MutatorFn<'a> = &'a mut FnMut(Id, Version, Cube, &VersionVector)
    -> Result<(Cube, Option<RespValue>), CommandError>;
pub type ResponseFn = fn(Cube) -> RespValue;

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

impl Cube {
    pub fn is_subsumed(&self, bvv: &BitmappedVersionVector) -> bool {
        use self::Cube::*;
        match *self {
            Counter(_) => false, // TODO
            Value(ref a) => a.values.is_empty() && a.vv.contained(bvv),
            Map(ref a) => a.map.is_empty() && a.vv.contained(bvv),
            Set(ref a) => a.values.is_empty() && a.vv.contained(bvv),
            Void(_) => unreachable!(),
        }
    }

    impl_into!(into_value, Value);
    impl_into!(into_counter, Counter);
    impl_into!(into_map, Map);
    impl_into!(into_set, Set);

    // minimum set of dots required to assemble this cube
    pub fn for_each_dot<CB: FnMut(Id, Version)>(&self, mut cb: CB) {
        use self::Cube::*;
        match *self {
            Counter(ref a) => a.for_each_dot(&mut cb),
            Value(ref a) => a.vv.iter().for_each(|(i, v)| cb(i, v)),
            Map(ref a) => a.vv.iter().for_each(|(i, v)| cb(i, v)),
            Set(ref a) => a.vv.iter().for_each(|(i, v)| cb(i, v)),
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
            Counter(_) => unimplemented!(),
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
                Counter(a) => Counter(a),
                Value(a) => Value(a.merge(self::Value::with(vv))),
                Map(a) => Map(a.merge(self::Map::with(vv))),
                Set(a) => Set(a.merge(self::Set::with(vv))),
                Void(mut o_vv) => {
                    o_vv.merge(&vv);
                    Void(o_vv)
                },
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

// LexCounter
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Counter {
    // I -> (V, C)
    pn: LinearMap<Id, (Version, i64)>,
}

impl Counter {
    fn with(_vv: VersionVector) -> Self {
        Counter {
            pn: Default::default(),
        }
    }

    fn get(&self) -> i64 {
        self.pn.values().map(|&(_, c)| c).sum()
    }

    fn for_each_dot<CB: FnMut(Id, Version)>(&self, cb: &mut CB) {
        self.pn.iter().for_each(|(&i, &(v, _))| cb(i, v));
    }

    fn merge(mut self, other: Self) -> Self {
        for (id, other) in other.pn {
            match self.pn.entry(id) {
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
    values: DotMap<Bytes>,
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
        if let Some(value) = value {
            self.values.insert(node, version, value);
        }
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
    vv: VersionVector,
}

impl Set {
    fn with(vv: VersionVector) -> Self {
        Set {
            values: Default::default(),
            vv,
        }
    }

    pub fn insert(&mut self, node: Id, version: Version, item: Bytes) -> bool {
        self.vv.add(node, version);
        self.values
            .insert(item, DotSet::from_dot((node, version)))
            .is_none()
    }

    pub fn remove(&mut self, node: Id, version: Version, item: &[u8]) -> bool {
        self.vv.add(node, version);
        self.values.remove(item).is_some()
    }

    pub fn pop(&mut self, node: Id, version: Version) -> Option<Bytes> {
        self.vv.add(node, version);
        if self.values.is_empty() {
            None
        } else {
            let i = thread_rng().gen::<usize>() % self.values.len();
            let k = self.values.keys().skip(i).next().unwrap().clone();
            self.values.remove(&k).unwrap();
            Some(k)
        }
    }

    pub fn clear(&mut self, node: Id, version: Version) {
        self.vv.add(node, version);
        self.values.clear();
    }

    fn merge(mut self, mut other: Self) -> Self {
        self.values.merge(&mut other.values, &self.vv, &other.vv);
        self.vv.merge(&other.vv);
        self
    }
}

// Actor Observed removal
// LWW on value conflict (max as tiebreaker)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Map {
    map: CausalMap<Bytes, MapValue>,
    vv: VersionVector,
}

impl Map {
    fn with(vv: VersionVector) -> Self {
        Map {
            map: Default::default(),
            vv,
        }
    }

    pub fn insert(&mut self, node: Id, version: Version, key: Bytes, value: Bytes) -> bool {
        self.vv.add(node, version);
        self.map.insert(key, MapValue::new((node, version), value)).is_none()
    }

    pub fn remove(&mut self, node: Id, version: Version, key: &[u8]) -> bool {
        self.vv.add(node, version);
        self.map.remove(key).is_some()
    }

    pub fn clear(&mut self, node: Id, version: Version) {
        self.vv.add(node, version);
        self.map.clear();
    }

    fn merge(mut self, mut other: Self) -> Self {
        self.map.merge(&mut other.map, &self.vv, &other.vv);
        self.vv.merge(&other.vv);
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

pub fn render_dummy(_: Cube) -> RespValue {
    unreachable!()
}

pub fn render_value_or_counter(cube: Cube) -> RespValue {
    match cube {
        Cube::Value(v) => {
            let serialized_vv = bincode::serialize(&v.vv, bincode::Infinite).unwrap();
            let mut values: Vec<_> = v.values
                .into_iter()
                .map(|(_, v)| RespValue::Data(v))
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
            let mut array = Vec::with_capacity(m.map.len() * 2);
            for (k, v) in m.map.into_iter() {
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
            let mut array = Vec::with_capacity(s.values.len());
            for (k, _) in s.values.into_iter() {
                array.push(RespValue::Data(k));
            }
            RespValue::Array(array)
        }
        Cube::Void(_) => RespValue::Array(vec![]),
        _ => CommandError::TypeError.into(),
    }
}
