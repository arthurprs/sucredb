use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use linear_map::set::LinearSet;
use linear_map::{self, Entry as LMEntry, LinearMap};
use roaring::{RoaringBitmap, RoaringTreemap};
use serde;
use std::hash::Hash;
use std::{cmp, str};
use types::NodeId;

pub type Id = NodeId;
pub type Version = u64;
/// A Dot is a Node Version Pair
// pub type Dot = (NodeId, Version);

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct BitmappedVersion {
    base: Version,
    #[serde(serialize_with = "serialize_bitmap", deserialize_with = "deserialize_bitmap")]
    bitmap: RoaringTreemap,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct BitmappedVersionVector(LinearMap<Id, BitmappedVersion>);

// same as DotMap<()> but this serializes better (unit is not emited)
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct DotSet(LinearSet<(Id, Version)>);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DotMap<T>(LinearMap<(Id, Version), T>);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CausalMap<K: Eq + Hash, V: CausalValue>(LinearMap<K, V>);

pub trait AbsVersionVector: Default {
    fn contains(&self, id: Id, version: Version) -> bool;
}

pub trait CausalValue: Default {
    fn merge<VV: AbsVersionVector>(&mut self, other: &mut Self, s_vv: &VV, o_vv: &VV);
    fn is_empty(&self) -> bool;
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionVector(LinearMap<Id, Version>);

// A VersionVector with exceptions (non continuous dots)
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DeltaVersionVector(Vec<(Id, Version)>);

impl BitmappedVersion {
    fn int_to_bitmap(base: Version, bitmap: u32) -> RoaringTreemap {
        let mut result = RoaringTreemap::new();
        if bitmap != 0 {
            for i in 0..32 {
                if bitmap & (1 << i) != 0 {
                    result.insert(base + 1 + i as Version);
                }
            }
        }
        result
    }

    pub fn new(base: Version, bitmap: u32) -> BitmappedVersion {
        BitmappedVersion {
            base: base,
            bitmap: Self::int_to_bitmap(base, bitmap),
        }
    }

    pub fn merge(&mut self, other: &Self) {
        self.bitmap |= &other.bitmap;
        if self.base < other.base {
            self.base = other.base;
        }
        self.norm();
    }

    pub fn add(&mut self, version: Version) -> bool {
        if version == self.base + 1 {
            self.base += 1;
        } else if version > self.base {
            if !self.bitmap.insert(version) {
                return false;
            }
        } else {
            return false;
        };
        self.norm();
        true
    }

    pub fn add_all(&mut self, version: Version) -> bool {
        if version > self.base {
            self.base = version;
            self.norm();
            true
        } else {
            false
        }
    }

    fn norm(&mut self) {
        // TODO: amortize the cost of this
        while self.bitmap.contains(self.base + 1) {
            self.base += 1;
        }
        self.bitmap.remove_range(0..self.base + 1);
    }

    pub fn fill_holes(&mut self) {
        self.base = self.bitmap.max().unwrap_or(0);
        self.bitmap.clear();
    }

    pub fn base(&self) -> Version {
        self.base
    }

    pub fn contains(&self, version: Version) -> bool {
        self.base >= version || self.bitmap.contains(version)
    }

    pub fn contains_all(&self, version: Version) -> bool {
        self.base >= version
    }

    /// self - other
    pub fn delta(&self, other: &Self) -> BitmappedVersionDelta {
        if self.base < other.base {
            return Default::default();
        }
        let last_version = cmp::max(self.bitmap.max().unwrap_or(0), self.base);
        BitmappedVersionDelta {
            from: other.clone(),
            to: self.clone(),
            pos: 0,
            len: last_version - other.base,
        }
    }
}

#[derive(Default)]
pub struct BitmappedVersionDelta {
    from: BitmappedVersion,
    to: BitmappedVersion,
    pos: Version,
    len: Version,
}

impl Iterator for BitmappedVersionDelta {
    type Item = Version;

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.len {
            self.pos += 1;
            let version = self.from.base + self.pos;
            if !self.from.bitmap.contains(version) && self.to.contains(version) {
                return Some(version);
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let hint = (self.len - self.pos) as usize;
        (hint, Some(hint))
    }
}

pub struct BitmappedVersionVectorDelta {
    iter: Box<Iterator<Item = (Id, Version)> + Send>,
    min_versions: Vec<(Id, Version)>,
}

impl Iterator for BitmappedVersionVectorDelta {
    type Item = (Id, Version);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl BitmappedVersionVectorDelta {
    pub fn min_versions(&self) -> &[(Id, Version)] {
        &self.min_versions
    }
}

pub fn serialize_bitmap<S>(value: &RoaringTreemap, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::Error;

    let mut bitmap_count = 0;
    let mut buffer_len = 4;
    for (_, b) in value.bitmaps() {
        bitmap_count += 1;
        buffer_len += 4 + b.serialized_size();
    }
    let mut buffer = Vec::with_capacity(buffer_len);
    buffer
        .write_u32::<LittleEndian>(bitmap_count)
        .map_err(Error::custom)?;
    for (p, b) in value.bitmaps() {
        buffer.write_u32::<LittleEndian>(p).map_err(Error::custom)?;
        b.serialize_into(&mut buffer).map_err(Error::custom)?;
    }
    serializer.serialize_bytes(&buffer)
}

pub fn deserialize_bitmap<'de, D>(deserializer: D) -> Result<RoaringTreemap, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{Error, Visitor};
    use std::fmt;

    struct ByteBufVisitor;

    impl<'de> Visitor<'de> for ByteBufVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("byte array")
        }

        #[inline]
        fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Self::Value::from(v))
        }

        #[inline]
        fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Self::Value::from(v))
        }
    }

    deserializer
        .deserialize_byte_buf(ByteBufVisitor)
        .and_then(|buffer| {
            let mut buffer = &buffer[..];
            let bitmap_count = buffer.read_u32::<LittleEndian>().map_err(Error::custom)?;
            let mut bitmaps = Vec::with_capacity(bitmap_count as usize);
            for _ in 0..bitmap_count {
                let p = buffer.read_u32::<LittleEndian>().map_err(Error::custom)?;
                let b = RoaringBitmap::deserialize_from(&mut buffer).map_err(Error::custom)?;
                bitmaps.push((p, b));
            }
            Ok(RoaringTreemap::from_bitmaps(bitmaps))
        })
}

impl BitmappedVersionVector {
    pub fn new() -> Self {
        BitmappedVersionVector(Default::default())
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn from_version(id: Id, bv: BitmappedVersion) -> Self {
        let mut bvv = Self::new();
        bvv.0.insert(id, bv);
        bvv
    }

    pub fn add(&mut self, id: Id, version: Version) -> bool {
        self.0
            .entry(id)
            .or_insert_with(Default::default)
            .add(version)
    }

    pub fn add_all(&mut self, id: Id, version: Version) -> bool {
        self.0
            .entry(id)
            .or_insert_with(Default::default)
            .add_all(version)
    }

    pub fn add_bv(&mut self, id: Id, bv: &BitmappedVersion) {
        self.0.entry(id).or_insert_with(Default::default).merge(bv);
    }

    pub fn get_mut(&mut self, id: Id) -> Option<&mut BitmappedVersion> {
        self.0.get_mut(&id)
    }

    pub fn get(&self, id: Id) -> Option<&BitmappedVersion> {
        self.0.get(&id)
    }

    pub fn entry_or_default(&mut self, id: Id) -> &mut BitmappedVersion {
        self.0.entry(id).or_insert_with(Default::default)
    }

    pub fn merge(&mut self, other: &Self) {
        for (&id, other_bitmap_version) in &other.0 {
            match self.0.entry(id) {
                LMEntry::Vacant(vac) => {
                    vac.insert(other_bitmap_version.clone());
                }
                LMEntry::Occupied(mut ocu) => {
                    ocu.get_mut().merge(other_bitmap_version);
                }
            }
        }
    }

    pub fn event(&mut self, id: Id) -> Version {
        match self.0.entry(id) {
            LMEntry::Vacant(vac) => {
                vac.insert(BitmappedVersion::new(1, 0));
                1
            }
            LMEntry::Occupied(mut ocu) => {
                let bv = ocu.get_mut();
                debug_assert_eq!(bv.bitmap.len(), 0);
                bv.base += 1;
                bv.base
            }
        }
    }

    pub fn contains(&self, id: Id, v: Version) -> bool {
        self.0.get(&id).map_or(false, |bv| bv.contains(v))
    }

    pub fn contains_all(&self, id: Id, v: Version) -> bool {
        self.0.get(&id).map_or(false, |bv| bv.contains_all(v))
    }

    pub fn iter_mut(&mut self) -> linear_map::IterMut<Id, BitmappedVersion> {
        self.0.iter_mut()
    }

    pub fn iter(&self) -> linear_map::Iter<Id, BitmappedVersion> {
        self.0.iter()
    }

    pub fn clone_if<F: FnMut(Id) -> bool>(&self, mut cond: F) -> Self {
        let mut result = Self::default();
        for (&id, bv) in self.0.iter() {
            if cond(id) {
                result.add_bv(id, bv);
            }
        }
        result
    }

    pub fn delta(&self, other: &Self) -> BitmappedVersionVectorDelta {
        let min_versions: Vec<_> = self.0
            .iter()
            .filter_map(|(&id, bv)| {
                if let Some(other_bv) = other.get(id) {
                    bv.delta(other_bv).map(move |v| (id, v)).next()
                } else {
                    Some((id, 1)) // start from 1 if the other bvv don't have this node
                }
            })
            .collect();
        let empty_bv = BitmappedVersion::new(0, 0);
        let other = other.clone();
        let iter = self.0.clone().into_iter().flat_map(move |(id, bv)| {
            let other_bv = other.get(id).unwrap_or(&empty_bv);
            bv.delta(other_bv).map(move |v| (id, v))
        });
        BitmappedVersionVectorDelta {
            iter: Box::new(iter),
            min_versions: min_versions,
        }
    }
}

impl PartialEq for DeltaVersionVector {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }
        // FIXME: inefficient
        let mut v: Vec<(Id, Version)> = Vec::with_capacity(self.0.len() * 2);
        v.extend(&self.0);
        v.extend(&other.0);
        v.sort_unstable();
        v.dedup();
        v.len() == self.0.len()
    }
}

impl DeltaVersionVector {
    fn new() -> Self {
        DeltaVersionVector(Default::default())
    }

    pub fn from_vv(vv: VersionVector) -> Self {
        DeltaVersionVector(unsafe { ::std::mem::transmute::<LinearMap<_, _>, Vec<_>>(vv.0) })
    }

    fn contains(&self, id: Id, version: Version) -> bool {
        assert!(version != 0, "Can't check for version 0");
        let mut p = if let Some(p) = self.0.iter().position(|x| x.0 == id) {
            p
        } else {
            return false;
        };
        if self.0[p].1 <= version {
            return true;
        }
        p += 1;
        while p < self.0.len() {
            if self.0[p].0 != id {
                return false;
            }
            if self.0[p].1 == version {
                return true;
            }
            p += 1;
        }

        false
    }

    fn add_all(&mut self, id: Id, version: Version) {
        let base_p = if let Some(p) = self.0.iter().position(|x| x.0 == id) {
            p
        } else {
            self.0.push((id, version));
            return;
        };
        if self.0[base_p].1 >= version {
            return;
        }
        // V > base
        // remove any version <= V
        // remove X continuous versions starting from V + 1
        // new base = V + X
        let mut new_base = version;
        let mut p = base_p + 1;
        while p < self.0.len() {
            if self.0[p].0 != id {
                break;
            }
            if self.0[p].1 > new_base {
                break;
            }
            p += 1;
        }
        while p < self.0.len() {
            if self.0[p].0 != id {
                break;
            }
            if self.0[p].1 != new_base + 1 {
                break;
            }
            new_base += 1;
            p += 1;
        }

        self.0[base_p].1 = new_base;
        self.0.drain(base_p + 1..p);
    }

    pub fn add(&mut self, id: Id, version: Version) {
        assert!(version != 0, "Version 0 is not valid");
        let base_p = if let Some(p) = self.0.iter().position(|x| x.0 == id) {
            p
        } else {
            if version != 1 {
                self.0.push((id, 0)); // 0 as a base may be problematic
            }
            self.0.push((id, version));
            return;
        };
        if self.0[base_p].1 >= version {
            return;
        }

        if self.0[base_p].1 + 1 == version {
            // adding a V == base + 1
            // remove X continuous versions starting from V + 1
            // new base = version + X
            let mut new_base = version;
            let mut p = base_p + 1;
            while p < self.0.len() {
                if self.0[p].0 != id {
                    break;
                }
                if self.0[p].1 != new_base + 1 {
                    break;
                }
                new_base += 1;
                p += 1;
            }
            self.0[base_p].1 = new_base;
            self.0.drain(base_p + 1..p);
        } else {
            // adding a version > base + 1
            // no summarization can happen
            let mut p = base_p + 1;
            while p < self.0.len() {
                if self.0[p].0 != id {
                    break;
                }
                if self.0[p].1 >= version {
                    if self.0[p].1 == version {
                        // no change
                        return;
                    }
                    break;
                }
                p += 1;
            }
            self.0.insert(p, (id, version));
        }
    }

    pub fn merge(&mut self, other: &Self) {
        let mut p = 0;
        while p < other.0.len() {
            let (id, v) = other.0[p];
            self.add_all(id, v);
            p += 1;
            while p < other.0.len() && other.0[p].0 == id {
                self.add(id, other.0[p].1);
                p += 1;
            }
        }
    }

    pub fn iter<'a>(&'a self) -> impl 'a + Iterator<Item = (Id, Version)> {
        self.0.iter().cloned()
    }

    pub fn contained(&self, bvv: &BitmappedVersionVector) -> bool {
        let mut p = 0;
        while p < self.0.len() {
            let (id, v) = self.0[p];
            let bv = if let Some(bv) = bvv.get(id) {
                bv
            } else {
                return false;
            };
            if !bv.contains_all(v) {
                return false;
            }
            p += 1;
            while p < self.0.len() && self.0[p].0 == id {
                if !bv.contains(self.0[p].1) {
                    return false;
                }
                p += 1;
            }
        }
        true
    }
}

impl AbsVersionVector for DeltaVersionVector {
    fn contains(&self, id: Id, version: Version) -> bool {
        Self::contains(self, id, version)
    }
}

impl VersionVector {
    pub fn new() -> Self {
        VersionVector(Default::default())
    }

    pub fn contains(&self, id: Id, version: Version) -> bool {
        self.0.get(&id).map(|&x| x >= version).unwrap_or(false)
    }

    pub fn merge(&mut self, other: &Self) {
        for (&id, &version) in &other.0 {
            self.add(id, version);
        }
    }

    pub fn descends(&self, other: &Self) -> bool {
        other
            .0
            .iter()
            .all(|(k, ov)| self.0.get(k).map(|v| v >= ov).unwrap_or(false))
    }

    pub fn strict_descends(&self, other: &Self) -> bool {
        self.descends(other) && self.0 != other.0
    }

    pub fn add(&mut self, id: Id, version: Version) {
        match self.0.entry(id) {
            LMEntry::Vacant(vac) => {
                vac.insert(version);
            }
            LMEntry::Occupied(mut ocu) => if *ocu.get() < version {
                ocu.insert(version);
            },
        }
    }

    pub fn event(&mut self, id: Id) {
        match self.0.entry(id) {
            LMEntry::Vacant(vac) => {
                vac.insert(1);
            }
            LMEntry::Occupied(mut ocu) => {
                *ocu.get_mut() += 1;
            }
        }
    }

    pub fn iter<'a>(&'a self) -> impl 'a + Iterator<Item = (Id, Version)> {
        self.0.iter().map(|(&i, &v)| (i, v))
    }

    pub fn contained(&self, bvv: &BitmappedVersionVector) -> bool {
        self.0.iter().all(|(&i, &v)| bvv.contains_all(i, v))
    }
}

impl AbsVersionVector for VersionVector {
    fn contains(&self, id: Id, version: Version) -> bool {
        Self::contains(self, id, version)
    }
}

impl DotSet {
    pub fn new() -> Self {
        DotSet(Default::default())
    }

    pub fn from_dot(dot: (Id, Version)) -> Self {
        let mut result = Self::new();
        result.0.insert(dot);
        result
    }
}

impl CausalValue for DotSet {
    fn merge<VV: AbsVersionVector>(&mut self, other: &mut Self, s_vv: &VV, o_vv: &VV) {
        // retain in self what's also exists in other or is not outdated
        self.0
            .retain(|&(id, version)| other.0.remove(&(id, version)) || !o_vv.contains(id, version));

        // drain other into self filtering outdated versions
        for &(id, version) in &other.0 {
            if !s_vv.contains(id, version) {
                self.0.insert((id, version));
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<T> DotMap<T> {
    pub fn new() -> DotMap<T> {
        DotMap(Default::default())
    }

    pub fn discard(&mut self, vv: &VersionVector) {
        self.0.retain(|&(id, version), _| !vv.contains(id, version));
    }

    pub fn insert(&mut self, id: Id, version: Version, value: T) {
        self.0.insert((id, version), value);
    }

    pub fn into_iter(self) -> impl Iterator<Item = ((Id, Version), T)> {
        self.0.into_iter()
    }
}

impl<T> ::std::ops::Deref for DotMap<T> {
    type Target = LinearMap<(NodeId, Version), T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> Default for DotMap<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> CausalValue for DotMap<T> {
    fn merge<VV: AbsVersionVector>(&mut self, other: &mut Self, s_vv: &VV, o_vv: &VV) {
        // retain in self what's also exists in other or is not outdated
        self.0.retain(|&(id, version), _| {
            other.0.remove(&(id, version)).is_some() || !o_vv.contains(id, version)
        });

        // drain other into self filtering outdated versions
        for ((id, version), value) in other.0.drain() {
            if !s_vv.contains(id, version) {
                self.0.insert((id, version), value);
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

// FIXME: the implementation assumes always full-state crdts and
// that the operation causal context is the same as the object.
// Just enough for now.
impl<K: Eq + Hash, V: CausalValue> CausalMap<K, V> {
    pub fn new() -> Self {
        CausalMap(Default::default())
    }

    pub fn clear(&mut self) {
        self.0.clear()
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.0.insert(key, value)
    }

    pub fn remove<Q: Eq + ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: ::std::borrow::Borrow<Q>,
    {
        self.0.remove(key)
    }

    pub fn into_iter(self) -> impl Iterator<Item = (K, V)> {
        self.0.into_iter()
    }
}

impl<K: Eq + Hash, V: CausalValue> ::std::ops::Deref for CausalMap<K, V> {
    type Target = LinearMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K: Eq + Hash, V: CausalValue> Default for CausalMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash, V: CausalValue> CausalValue for CausalMap<K, V> {
    fn merge<VV: AbsVersionVector>(&mut self, other: &mut Self, s_vv: &VV, o_vv: &VV) {
        // retain in self what's not outdated or also exists in other
        self.0.retain(|v, dot_set| {
            if let Some(mut other_dot_set) = other.0.remove(v) {
                dot_set.merge(&mut other_dot_set, s_vv, o_vv);
                !dot_set.is_empty()
            } else {
                true
            }
        });

        // drain other into self filtering outdated versions
        for (v, mut other_dot_set) in other.0.drain() {
            other_dot_set.merge(&mut Default::default(), o_vv, s_vv);
            if !other_dot_set.is_empty() {
                self.0.insert(v, other_dot_set);
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg(test)]
mod test_bv {
    use super::*;
    use bincode;

    #[test]
    fn test_bv_serde() {
        for &base in &[0u64, 123] {
            for &bits in &[0u32, 0b10000000000000, 0b10000000000001, 0b10000000000010] {
                let bv1 = BitmappedVersion::new(base, bits);
                let buffer = bincode::serialize(&bv1).unwrap();
                let bv2: BitmappedVersion = bincode::deserialize(&buffer).unwrap();
                assert_eq!(bv1.base, bv2.base);
                assert_eq!(bv1.bitmap, bv2.bitmap);
            }
        }
    }

    #[test]
    fn delta() {
        let a = BitmappedVersion::new(5, 0b10);
        let b = BitmappedVersion::new(2, 0b100);
        assert_eq!(vec![3, 4, 7], a.delta(&b).collect::<Vec<Version>>());
        let a = BitmappedVersion::new(7, 0);
        let b = BitmappedVersion::new(2, 0b100);
        assert_eq!(vec![3, 4, 6, 7], a.delta(&b).collect::<Vec<Version>>());
        let a = BitmappedVersion::new(7, 0);
        let b = BitmappedVersion::new(7, 0);
        assert!(a.delta(&b).collect::<Vec<Version>>().is_empty());
        let a = BitmappedVersion::new(7, 0);
        let b = BitmappedVersion::new(8, 0);
        assert!(a.delta(&b).collect::<Vec<Version>>().is_empty());
    }

    #[test]
    fn norm() {
        let mut a = BitmappedVersion::new(1, 0b10);
        a.norm();
        assert_eq!(a, BitmappedVersion::new(1, 0b10));
        a.add(2);
        a.norm();
        assert_eq!(a, BitmappedVersion::new(3, 0));
    }
}

#[cfg(test)]
mod test_bvv {
    use super::*;

    #[test]
    fn add_get() {
        let mut a = BitmappedVersionVector::new();
        assert!(a.get(1).is_none());
        a.add(1, 1);
        a.add(1, 3);
        assert_eq!(a.get(1).unwrap(), &BitmappedVersion::new(1, 0b10)); // second bit set
        a.add(1, 2);
        // expect normalization to occur
        assert_eq!(a.get(1).unwrap(), &BitmappedVersion::new(3, 0));
    }

    #[test]
    fn delta() {
        let mut bvv1 = BitmappedVersionVector::new();
        let mut bvv2 = BitmappedVersionVector::new();
        bvv1.0.insert(1, BitmappedVersion::new(5, 0b10));
        bvv2.0.insert(1, BitmappedVersion::new(2, 0b100));
        bvv1.0.insert(2, BitmappedVersion::new(7, 0));
        bvv2.0.insert(2, BitmappedVersion::new(2, 0b100));
        bvv1.0.insert(3, BitmappedVersion::new(7, 0));
        bvv2.0.insert(3, BitmappedVersion::new(7, 0));
        bvv1.0.insert(4, BitmappedVersion::new(7, 0));
        bvv2.0.insert(4, BitmappedVersion::new(6, 0b1));
        bvv1.0.insert(5, BitmappedVersion::new(2, 0));
        let delta_dots: Vec<_> = bvv1.delta(&bvv2).collect();
        assert_eq!(
            vec![
                (1, 3),
                (1, 4),
                (1, 7),
                (2, 3),
                (2, 4),
                (2, 6),
                (2, 7),
                (5, 1),
                (5, 2),
            ],
            delta_dots
        );
        let min_versions: Vec<_> = bvv1.delta(&bvv2).min_versions().to_owned();
        assert_eq!(vec![(1, 3), (2, 3), (5, 1)], min_versions);
    }

    #[test]
    fn merge() {
        let mut a = BitmappedVersionVector::new();
        a.0.insert(1, BitmappedVersion::new(5, 3));
        let mut b = BitmappedVersionVector::new();
        b.0.insert(1, BitmappedVersion::new(2, 4));
        a.merge(&b);
        assert_eq!(a.get(1).unwrap(), &BitmappedVersion::new(7, 0));

        let mut a = BitmappedVersionVector::new();
        a.0.insert(1, BitmappedVersion::new(2, 4));
        let mut b = BitmappedVersionVector::new();
        b.0.insert(1, BitmappedVersion::new(5, 3));
        a.merge(&b);
        assert_eq!(a.get(1).unwrap(), &BitmappedVersion::new(7, 0));

        let mut a = BitmappedVersionVector::new();
        a.0.insert(1, BitmappedVersion::new(5, 3));
        let mut b = BitmappedVersionVector::new();
        b.0.insert(2, BitmappedVersion::new(2, 4));
        a.merge(&b);
        assert_eq!(a.get(1).unwrap(), &BitmappedVersion::new(5, 3));
        assert_eq!(a.get(2).unwrap(), &BitmappedVersion::new(2, 4));
    }

    #[test]
    fn event() {
        let mut a = BitmappedVersionVector::new();
        assert_eq!(a.event(1), 1);
        assert_eq!(a.get(1).unwrap(), &BitmappedVersion::new(1, 0));

        assert_eq!(a.event(1), 2);
        assert_eq!(a.get(1).unwrap(), &BitmappedVersion::new(2, 0));
    }

}

#[cfg(test)]
mod test_dvv {
    use super::*;

    #[test]
    fn test_add_all() {
        let mut dvv = DeltaVersionVector::new();
        dvv.add_all(1, 0);
        assert_eq!(dvv, DeltaVersionVector(vec![(1, 0)]));
        dvv.add_all(1, 5);
        assert_eq!(dvv, DeltaVersionVector(vec![(1, 5)]));
        dvv.add_all(1, 5);
        assert_eq!(dvv, DeltaVersionVector(vec![(1, 5)]));
        dvv.add_all(1, 1);
        assert_eq!(dvv, DeltaVersionVector(vec![(1, 5)]));
        dvv.add_all(2, 2);
        assert_eq!(dvv, DeltaVersionVector(vec![(1, 5), (2, 2)]));
        dvv.add_all(2, 3);
        assert_eq!(dvv, DeltaVersionVector(vec![(1, 5), (2, 3)]));
        dvv.add_all(2, 1);
        assert_eq!(dvv, DeltaVersionVector(vec![(1, 5), (2, 3)]));

        let mut dvv = DeltaVersionVector(vec![(1, 0), (1, 3)]);
        dvv.add_all(1, 2);
        assert_eq!(dvv, DeltaVersionVector(vec![(1, 3)]));
    }

    #[test]
    fn test_add() {
        let mut dvv = DeltaVersionVector::new();
        dvv.add(1, 1);
        assert_eq!(dvv, DeltaVersionVector(vec![(1, 1)]));
        dvv.add(1, 3);
        assert_eq!(dvv, DeltaVersionVector(vec![(1, 1), (1, 3)]));
        dvv.add(1, 3);
        assert_eq!(dvv, DeltaVersionVector(vec![(1, 1), (1, 3)]));
        dvv.add(1, 4);
        assert_eq!(dvv, DeltaVersionVector(vec![(1, 1), (1, 3), (1, 4)]));
        dvv.add(2, 2);
        assert_eq!(
            dvv,
            DeltaVersionVector(vec![(1, 1), (1, 3), (1, 4), (2, 0), (2, 2)])
        );
        dvv.add(1, 2);
        assert_eq!(dvv, DeltaVersionVector(vec![(1, 4), (2, 0), (2, 2)]));
        dvv.add(2, 1);
        assert_eq!(dvv, DeltaVersionVector(vec![(1, 4), (2, 2)]));
    }

    #[test]
    fn test_merge() {
        let mut dvv = DeltaVersionVector(vec![(1, 1), (1, 3), (1, 4), (2, 0), (2, 2)]);
        let dvv_ = dvv.clone();
        dvv.merge(&dvv_);
        assert_eq!(dvv, dvv);

        dvv.merge(&DeltaVersionVector(vec![
            (1, 0),
            (1, 2),
            (2, 1),
            (3, 0),
            (3, 3),
        ]));
        assert_eq!(
            dvv,
            DeltaVersionVector(vec![(1, 4), (2, 2), (3, 0), (3, 3)])
        );

        dvv.merge(&DeltaVersionVector(vec![(3, 5), (3, 7)]));
        assert_eq!(
            dvv,
            DeltaVersionVector(vec![(1, 4), (2, 2), (3, 5), (3, 7)])
        );

        dvv.merge(&DeltaVersionVector(vec![(3, 6)]));
        assert_eq!(dvv, DeltaVersionVector(vec![(1, 4), (2, 2), (3, 7)]));
    }
}

#[cfg(test)]
mod test_vv {
    use super::*;

    fn contains() {
        let mut a1 = VersionVector::new();
        a1.add(1, 2);
        a1.add(2, 4);
        assert!(a1.contains(1, 1));
        assert!(a1.contains(1, 2));
        assert!(!a1.contains(1, 3));
        assert!(a1.contains(2, 4));
        assert!(!a1.contains(2, 5));
        assert!(!a1.contains(3, 1));
    }
}
//
// #[cfg(test)]
// mod test_dcc {
//     use super::*;
//
//     fn data() -> [DottedCausalContainer<&'static str>; 5] {
//         let mut d1 = DottedCausalContainer::new();
//         d1.dots.0.insert((1, 8), "red");
//         d1.dots.0.insert((2, 2), "green");
//         let mut d2 = DottedCausalContainer::new();
//         d2.vv.0.insert(1, 4);
//         d2.vv.0.insert(2, 20);
//         let mut d3 = DottedCausalContainer::new();
//         d3.dots.0.insert((1, 1), "black");
//         d3.dots.0.insert((1, 3), "red");
//         d3.dots.0.insert((2, 1), "green");
//         d3.dots.0.insert((2, 2), "green");
//         d3.vv.0.insert(1, 4);
//         d3.vv.0.insert(2, 7);
//         let mut d4 = DottedCausalContainer::new();
//         d4.dots.0.insert((1, 2), "gray");
//         d4.dots.0.insert((1, 3), "red");
//         d4.dots.0.insert((1, 5), "red");
//         d4.dots.0.insert((2, 2), "green");
//         d4.vv.0.insert(1, 5);
//         d4.vv.0.insert(2, 5);
//         let mut d5 = DottedCausalContainer::new();
//         d5.dots.0.insert((1, 5), "gray");
//         d5.vv.0.insert(1, 5);
//         d5.vv.0.insert(2, 5);
//         d5.vv.0.insert(3, 4);
//         [d1, d2, d3, d4, d5]
//     }
//
//     #[test]
//     fn sync() {
//         let d = data();
//         let mut d34: DottedCausalContainer<&'static str> = DottedCausalContainer::new();
//         d34.dots.0.insert((1, 3), "red");
//         d34.dots.0.insert((1, 5), "red");
//         d34.dots.0.insert((2, 2), "green");
//         d34.vv.0.insert(1, 5);
//         d34.vv.0.insert(2, 7);
//
//         for d in &d {
//             let mut ds = d.clone();
//             ds.sync(d.clone());
//             assert_eq!(&ds, d);
//         }
//         let mut ds = d[2].clone();
//         ds.sync(d[3].clone());
//         assert_eq!(&ds, &d34);
//         ds = d[3].clone();
//         ds.sync(d[2].clone());
//         assert_eq!(&ds, &d34);
//     }
//
//     #[test]
//     fn add_dots() {
//         let d1 = data()[0].clone();
//         let mut bvv0 = BitmappedVersionVector::new();
//         bvv0.0.insert(1, BitmappedVersion::new(5, 3));
//         bvv0.add_dots(&d1);
//         let mut bvv1 = BitmappedVersionVector::new();
//         bvv1.0.insert(1, BitmappedVersion::new(8, 0));
//         bvv1.0.insert(2, BitmappedVersion::new(0, 2));
//         assert_eq!(bvv0, bvv1);
//     }
//
//     #[test]
//     fn add() {
//         let mut d1 = data()[0].clone();
//         d1.add(1, 11, "purple");
//         let mut d1e = DottedCausalContainer::new();
//         d1e.dots.0.insert((1, 8), "red");
//         d1e.dots.0.insert((2, 2), "green");
//         d1e.dots.0.insert((1, 11), "purple");
//         d1e.vv.0.insert(1, 11);
//         assert_eq!(d1, d1e);
//
//         let mut d2 = data()[1].clone();
//         d2.add(2, 11, "purple");
//         let mut d2e = DottedCausalContainer::new();
//         d2e.dots.0.insert((2, 11), "purple");
//         d2e.vv.0.insert(1, 4);
//         d2e.vv.0.insert(2, 20);
//         assert_eq!(d2, d2e);
//     }
//
//     #[test]
//     fn discard() {
//         let mut d3 = data()[2].clone();
//         d3.discard(&VersionVector::new());
//         assert_eq!(&d3, &data()[2]);
//
//         let mut vv = VersionVector::new();
//         vv.add(1, 2);
//         vv.add(2, 15);
//         vv.add(3, 15);
//         let mut d3 = data()[2].clone();
//         d3.discard(&vv);
//         let mut d3e = DottedCausalContainer::new();
//         d3e.dots.0.insert((1, 3), "red");
//         d3e.vv.0.insert(1, 4);
//         d3e.vv.0.insert(2, 15);
//         d3e.vv.0.insert(3, 15);
//         assert_eq!(d3, d3e);
//
//         let mut vv = VersionVector::new();
//         vv.add(1, 3);
//         vv.add(2, 15);
//         vv.add(3, 15);
//         let mut d3 = data()[2].clone();
//         d3.discard(&vv);
//         let mut d3e = DottedCausalContainer::new();
//         d3e.vv.0.insert(1, 4);
//         d3e.vv.0.insert(2, 15);
//         d3e.vv.0.insert(3, 15);
//         assert_eq!(d3, d3e);
//     }
//
//     #[test]
//     #[ignore]
//     fn fill() {
//         unimplemented!()
//     }
//
//     #[test]
//     #[ignore]
//     fn strip() {
//         unimplemented!()
//     }
// }
