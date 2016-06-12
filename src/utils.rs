use std::{hash, cmp};
use std::marker::PhantomData;
use std::str::FromStr;
use std::string::ToString;
use std::collections::HashMap;
use std::error::Error;
use serde;

pub type GenericError = Box<Error + Send + Sync + 'static>;

pub fn json_serialize_map<K, T, S>(value: &HashMap<K, T>, serializer: &mut S) -> Result<(), S::Error>
    where K: cmp::Eq + hash::Hash + ToString,
          T: serde::Serialize,
          S: serde::Serializer
{
    let iter = value.iter().map(|(key, value)| (key.to_string(), value));
    let visitor = serde::ser::impls::MapIteratorVisitor::new(iter, Some(value.len()));
    serializer.serialize_map(visitor)
}

pub fn json_deserialize_map<K, T, D>(deserializer: &mut D) -> Result<HashMap<K, T>, D::Error>
    where K: cmp::Eq + hash::Hash + FromStr + serde::Deserialize,
          T: serde::Deserialize,
          D: serde::Deserializer
{
    use serde::de::Error;

    struct MapVisitor<K, T>(PhantomData<(K, T)>);

    impl<K, T> serde::de::Visitor for MapVisitor<K, T>
        where K: cmp::Eq + hash::Hash + FromStr + serde::Deserialize,
              T: serde::Deserialize
    {
        type Value = HashMap<K, T>;

        #[inline]
        fn visit_map<V>(&mut self, mut visitor: V) -> Result<Self::Value, V::Error>
            where V: serde::de::MapVisitor
        {
            let mut map = HashMap::new();

            while let Some((key, value)) = try!(visitor.visit::<String, _>()) {
                let key = match K::from_str(&key) {
                    Ok(key) => key,
                    Err(_) => {
                        return Err(V::Error::invalid_value("key must be parseable"));
                    }
                };

                map.insert(key, value);
            }

            try!(visitor.end());
            Ok(map)
        }
    }

    deserializer.deserialize_map(MapVisitor(PhantomData))
}

macro_rules! assert_eq_repr {
    ($left:expr , $right:expr) => ({
        match (format!("{:?}", &$left), format!("{:?}", &$right)) {
            (left_val, right_val) => {
                if !(left_val == right_val) {
                    panic!("repr assertion failed: `(debug(left) == debug(right))` \
                           (left: `{:?}`, right: `{:?}`)", left_val, right_val)
                }
            }
        }
    })
}
