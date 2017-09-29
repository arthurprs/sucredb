use std::{cmp, str};
use linear_map::{self, Entry, LinearMap};
use roaring::{RoaringBitmap, RoaringTreemap};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde;

pub type Version = u64;
pub type Id = u64;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionVector(LinearMap<Id, Version>);

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct BitmappedVersion {
    base: Version,
    #[serde(serialize_with = "serialize_bitmap", deserialize_with = "deserialize_bitmap")]
    bitmap:
        RoaringTreemap,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BitmappedVersionVector(LinearMap<Id, BitmappedVersion>);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Dots<T>(LinearMap<(Id, Version), T>);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DottedCausalContainer<T> {
    dots: Dots<T>,
    vv: VersionVector,
}

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

    pub fn join(&mut self, other: &Self) {
        self.bitmap |= &other.bitmap;
        if self.base < other.base {
            self.base = other.base;
        }
        self.norm();
    }

    pub fn add(&mut self, version: Version) {
        if version == self.base + 1 {
            self.base += 1;
        } else if version > self.base {
            self.bitmap.insert(version);
        }
        self.norm();
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

    pub fn add_dots<T>(&mut self, dcc: &DottedCausalContainer<T>) {
        for &(id, version) in dcc.dots.0.keys() {
            self.add(id, version);
        }
    }

    pub fn add(&mut self, id: Id, version: Version) {
        self.0
            .entry(id)
            .or_insert_with(Default::default)
            .add(version);
    }

    pub fn add_bv(&mut self, id: Id, bv: &BitmappedVersion) {
        self.0.entry(id).or_insert_with(Default::default).join(bv);
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

    pub fn join(&mut self, other: &Self) {
        for (&id, other_bitmap_version) in &other.0 {
            if let Some(bitmap_version) = self.0.get_mut(&id) {
                bitmap_version.join(other_bitmap_version);
            }
        }
    }

    pub fn merge(&mut self, other: &Self) {
        for (&id, other_bitmap_version) in &other.0 {
            match self.0.entry(id) {
                Entry::Vacant(vac) => {
                    vac.insert(other_bitmap_version.clone());
                }
                Entry::Occupied(mut ocu) => {
                    ocu.get_mut().join(other_bitmap_version);
                }
            }
        }
    }

    pub fn event(&mut self, id: Id) -> Version {
        match self.0.entry(id) {
            Entry::Vacant(vac) => {
                vac.insert(BitmappedVersion::new(1, 0));
                1
            }
            Entry::Occupied(mut ocu) => {
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

    pub fn iter_mut(&mut self) -> linear_map::IterMut<Id, BitmappedVersion> {
        self.0.iter_mut()
    }

    pub fn iter(&self) -> linear_map::Iter<Id, BitmappedVersion> {
        self.0.iter()
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

    // pub fn reset(&mut self) {
    //     for (_, bitmap_version) in &mut self.0 {
    //         bitmap_version.bitmap = ramp::Int::zero();
    //     }
    // }

    // pub fn clone_base(&self) -> Self {
    //     let mut new = Self::new();
    //     new.0.reserve(self.0.len());
    //     for (&id, bitmap_version) in &self.0 {
    //         new.0.insert(id, BitmappedVersion::new(bitmap_version.base, 0));
    //     }
    //     new
    // }
}

impl VersionVector {
    pub fn new() -> Self {
        VersionVector(Default::default())
    }

    pub fn get(&self, id: Id) -> Option<Version> {
        self.0.get(&id).cloned()
    }

    pub fn remove(&mut self, id: Id) -> Option<Version> {
        self.0.remove(&id)
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

    pub fn add(&mut self, id: Id, version: Version) {
        match self.0.entry(id) {
            Entry::Vacant(vac) => {
                vac.insert(version);
            }
            Entry::Occupied(mut ocu) => if *ocu.get() < version {
                ocu.insert(version);
            },
        }
    }

    pub fn event(&mut self, id: Id) {
        match self.0.entry(id) {
            Entry::Vacant(vac) => {
                vac.insert(1);
            }
            Entry::Occupied(mut ocu) => {
                *ocu.get_mut() += 1;
            }
        }
    }

    pub fn iter(&self) -> linear_map::Iter<Id, Version> {
        self.0.iter()
    }
}

impl<T> Dots<T> {
    fn new() -> Dots<T> {
        Dots(Default::default())
    }

    fn merge(&mut self, other: &mut Self, vv1: &VersionVector, vv2: &VersionVector) {
        // retain in self what's not outdated or also exists in other
        self.0.retain(|&(id, version), _| {
            version > vv1.get(id).unwrap_or(0) || version > vv2.get(id).unwrap_or(0)
                || other.0.remove(&(id, version)).is_some()
        });

        // drain other into self filtering outdated versions
        for ((id, version), value) in other.0.drain() {
            if version > vv1.get(id).unwrap_or(0) || version > vv2.get(id).unwrap_or(0) {
                self.0.insert((id, version), value);
            }
        }
    }

    fn add(&mut self, dot: (Id, Version), value: T) {
        self.0.insert(dot, value);
    }
}

impl<T> DottedCausalContainer<T> {
    pub fn new() -> DottedCausalContainer<T> {
        DottedCausalContainer {
            dots: Dots::new(),
            vv: VersionVector::new(),
        }
    }

    /// Returns true if dcc has no values AND causal context is empty
    pub fn is_dcc_empty(&self) -> bool {
        self.dots.0.is_empty() && self.vv.0.is_empty()
    }

    pub fn sync(&mut self, mut other: Self) {
        self.dots.merge(&mut other.dots, &self.vv, &other.vv);
        self.vv.merge(&other.vv);
    }

    pub fn add(&mut self, id: Id, version: Version, value: T) {
        self.dots.add((id, version), value);
        self.vv.add(id, version);
    }

    pub fn contained(&self, bvv: &BitmappedVersionVector) -> bool {
        self.dots.0.keys().all(|&(id, v)| bvv.contains(id, v))
    }

    pub fn discard(&mut self, vv: &VersionVector) {
        self.dots
            .0
            .retain(|&(id, version), _| version > vv.get(id).unwrap_or(0));
        self.vv.merge(vv);
    }

    pub fn strip(&mut self, bvv: &BitmappedVersionVector) {
        self.vv.0.retain(|&id, &mut version| {
            version > bvv.get(id).map(|b| b.base).unwrap_or(0)
        });
    }

    pub fn fill(&mut self, bvv: &BitmappedVersionVector) {
        for (&id, bitmap_version) in &bvv.0 {
            self.vv.add(id, bitmap_version.base);
        }
    }

    pub fn iter(&self) -> linear_map::Iter<(Id, Version), T> {
        self.dots.0.iter()
    }

    pub fn into_iter(self) -> linear_map::IntoIter<(Id, Version), T> {
        self.dots.0.into_iter()
    }

    pub fn values(&self) -> linear_map::Values<(Id, Version), T> {
        self.dots.0.values()
    }

    pub fn version_vector(&self) -> &VersionVector {
        &self.vv
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
                let buffer = bincode::serialize(&bv1, bincode::Infinite).unwrap();
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

    // #[test]
    // fn values() {
    //     let mut a = BitmappedVersion::new(0, 0);
    //     assert!(a.values().is_empty());
    //     a.base = 2;
    //     assert_eq!(a.values(), [1 as Version, 2]);
    //     a.bitmap = 3.into();
    //     assert_eq!(a.values(), [1 as Version, 2, 3, 4]);
    //     a.bitmap = 5.into();
    //     assert_eq!(a.values(), [1 as Version, 2, 3, 5]);
    // }

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
    fn merge() {
        let mut a = BitmappedVersionVector::new();
        a.0.insert(1, BitmappedVersion::new(5, 3));
        let mut b = BitmappedVersionVector::new();
        b.0.insert(1, BitmappedVersion::new(2, 4));
        a.merge(&b);
        assert_eq!(a.get(1).unwrap(), &BitmappedVersion::new(7, 0));

        a = BitmappedVersionVector::new();
        a.0.insert(1, BitmappedVersion::new(5, 3));
        b = BitmappedVersionVector::new();
        b.0.insert(2, BitmappedVersion::new(2, 4));
        a.merge(&b);
        assert_eq!(a.get(1).unwrap(), &BitmappedVersion::new(5, 3));
        assert_eq!(a.get(2).unwrap(), &BitmappedVersion::new(2, 4));
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
            vec![(1, 3), (1, 4), (1, 7), (2, 3), (2, 4), (2, 6), (2, 7), (5, 1), (5, 2)],
            delta_dots
        );
        let min_versions: Vec<_> = bvv1.delta(&bvv2).min_versions().to_owned();
        assert_eq!(vec![(1, 3), (2, 3), (5, 1)], min_versions);
    }

    #[test]
    fn join() {
        let mut a = BitmappedVersionVector::new();
        a.0.insert(1, BitmappedVersion::new(5, 3));
        let mut b = BitmappedVersionVector::new();
        b.0.insert(1, BitmappedVersion::new(2, 4));
        a.join(&b);
        assert_eq!(a.get(1).unwrap(), &BitmappedVersion::new(7, 0));

        let mut a = BitmappedVersionVector::new();
        a.0.insert(1, BitmappedVersion::new(2, 4));
        let mut b = BitmappedVersionVector::new();
        b.0.insert(1, BitmappedVersion::new(5, 3));
        a.join(&b);
        assert_eq!(a.get(1).unwrap(), &BitmappedVersion::new(7, 0));

        let mut a = BitmappedVersionVector::new();
        a.0.insert(1, BitmappedVersion::new(5, 3));
        let mut b = BitmappedVersionVector::new();
        b.0.insert(2, BitmappedVersion::new(2, 4));
        a.join(&b);
        assert_eq!(a.get(1).unwrap(), &BitmappedVersion::new(5, 3));
        assert!(a.get(2).is_none());
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
mod test_vv {
    use super::*;

    // #[test]
    // fn reset() {
    //     let mut a1 = VersionVector::new();
    //     a1.add(1, 2);
    //     a1.add(2, 4);
    //     a1.add(3, 4);
    //     a1.reset();
    //     assert_eq!(a1.get(1), Some(0));
    //     assert_eq!(a1.get(2), Some(0));
    //     assert_eq!(a1.get(3), Some(0));
    // }

    #[test]
    fn remove() {
        let mut a1 = VersionVector::new();
        a1.add(1, 2);
        a1.add(2, 4);
        a1.add(3, 4);
        assert_eq!(a1.remove(1), Some(2));
        assert_eq!(a1.remove(2), Some(4));
        assert_eq!(a1.remove(3), Some(4));
        assert_eq!(a1.remove(1), None);
        assert_eq!(a1.remove(2), None);
        assert_eq!(a1.remove(3), None);
    }
}

#[cfg(test)]
mod test_dcc {
    use super::*;

    fn data() -> [DottedCausalContainer<&'static str>; 5] {
        let mut d1 = DottedCausalContainer::new();
        d1.dots.0.insert((1, 8), "red");
        d1.dots.0.insert((2, 2), "green");
        let mut d2 = DottedCausalContainer::new();
        d2.vv.0.insert(1, 4);
        d2.vv.0.insert(2, 20);
        let mut d3 = DottedCausalContainer::new();
        d3.dots.0.insert((1, 1), "black");
        d3.dots.0.insert((1, 3), "red");
        d3.dots.0.insert((2, 1), "green");
        d3.dots.0.insert((2, 2), "green");
        d3.vv.0.insert(1, 4);
        d3.vv.0.insert(2, 7);
        let mut d4 = DottedCausalContainer::new();
        d4.dots.0.insert((1, 2), "gray");
        d4.dots.0.insert((1, 3), "red");
        d4.dots.0.insert((1, 5), "red");
        d4.dots.0.insert((2, 2), "green");
        d4.vv.0.insert(1, 5);
        d4.vv.0.insert(2, 5);
        let mut d5 = DottedCausalContainer::new();
        d5.dots.0.insert((1, 5), "gray");
        d5.vv.0.insert(1, 5);
        d5.vv.0.insert(2, 5);
        d5.vv.0.insert(3, 4);
        [d1, d2, d3, d4, d5]
    }

    #[test]
    fn sync() {
        let d = data();
        let mut d34: DottedCausalContainer<&'static str> = DottedCausalContainer::new();
        d34.dots.0.insert((1, 3), "red");
        d34.dots.0.insert((1, 5), "red");
        d34.dots.0.insert((2, 2), "green");
        d34.vv.0.insert(1, 5);
        d34.vv.0.insert(2, 7);

        for d in &d {
            let mut ds = d.clone();
            ds.sync(d.clone());
            assert_eq!(&ds, d);
        }
        let mut ds = d[2].clone();
        ds.sync(d[3].clone());
        assert_eq!(&ds, &d34);
        ds = d[3].clone();
        ds.sync(d[2].clone());
        assert_eq!(&ds, &d34);
    }

    #[test]
    fn add_dots() {
        let d1 = data()[0].clone();
        let mut bvv0 = BitmappedVersionVector::new();
        bvv0.0.insert(1, BitmappedVersion::new(5, 3));
        bvv0.add_dots(&d1);
        let mut bvv1 = BitmappedVersionVector::new();
        bvv1.0.insert(1, BitmappedVersion::new(8, 0));
        bvv1.0.insert(2, BitmappedVersion::new(0, 2));
        assert_eq!(bvv0, bvv1);
    }

    #[test]
    fn add() {
        let mut d1 = data()[0].clone();
        d1.add(1, 11, "purple");
        let mut d1e = DottedCausalContainer::new();
        d1e.dots.0.insert((1, 8), "red");
        d1e.dots.0.insert((2, 2), "green");
        d1e.dots.0.insert((1, 11), "purple");
        d1e.vv.0.insert(1, 11);
        assert_eq!(d1, d1e);

        let mut d2 = data()[1].clone();
        d2.add(2, 11, "purple");
        let mut d2e = DottedCausalContainer::new();
        d2e.dots.0.insert((2, 11), "purple");
        d2e.vv.0.insert(1, 4);
        d2e.vv.0.insert(2, 20);
        assert_eq!(d2, d2e);
    }

    #[test]
    fn discard() {
        let mut d3 = data()[2].clone();
        d3.discard(&VersionVector::new());
        assert_eq!(&d3, &data()[2]);

        let mut vv = VersionVector::new();
        vv.add(1, 2);
        vv.add(2, 15);
        vv.add(3, 15);
        let mut d3 = data()[2].clone();
        d3.discard(&vv);
        let mut d3e = DottedCausalContainer::new();
        d3e.dots.0.insert((1, 3), "red");
        d3e.vv.0.insert(1, 4);
        d3e.vv.0.insert(2, 15);
        d3e.vv.0.insert(3, 15);
        assert_eq!(d3, d3e);

        let mut vv = VersionVector::new();
        vv.add(1, 3);
        vv.add(2, 15);
        vv.add(3, 15);
        let mut d3 = data()[2].clone();
        d3.discard(&vv);
        let mut d3e = DottedCausalContainer::new();
        d3e.vv.0.insert(1, 4);
        d3e.vv.0.insert(2, 15);
        d3e.vv.0.insert(3, 15);
        assert_eq!(d3, d3e);
    }

    #[test]
    #[ignore]
    fn fill() {
        unimplemented!()
    }

    #[test]
    #[ignore]
    fn strip() {
        unimplemented!()
    }
}
