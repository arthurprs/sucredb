use std::hash::Hasher;
use std::{path, fs};
use std::hash::BuildHasherDefault;
use std::collections::HashMap;
use std::error::Error;
use serde;
use serde_yaml;

pub type GenericError = Box<Error + Send + Sync + 'static>;

pub type IdHasherBuilder = BuildHasherDefault<IdHasher>;
pub type IdHashMap<K, V> = HashMap<K, V, IdHasherBuilder>;
pub struct IdHasher(u64);

impl Default for IdHasher {
    #[inline]
    fn default() -> IdHasher {
        IdHasher(0)
    }
}

impl Hasher for IdHasher {
    #[inline]
    fn finish(&self) -> u64 {
        // Primes used in XXH64's finalizer.
        const PRIME_2: u64 = 14029467366897019727;
        const PRIME_3: u64 = 1609587929392839161;
        let mut hash = self.0;
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(PRIME_2);
        hash ^= hash >> 29;
        hash = hash.wrapping_mul(PRIME_3);
        hash ^= hash >> 32;
        hash
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        debug_assert!(bytes.len() <= 8);
        unsafe {
            let mut temp = 0u64;
            ::std::ptr::copy_nonoverlapping(bytes.as_ptr(),
                                            &mut temp as *mut _ as *mut u8,
                                            bytes.len());
            self.0 ^= temp;
        }
    }
}

pub fn get_or_gen_node_id() -> u64 {
    use std::io;
    use rand::{thread_rng, Rng};
    let the_path = "_nodeid.yaml";
    let res = read_yaml_from_file(the_path);
    let err = match res {
        Ok(node_id) => return node_id,
        Err(e) => e,
    };
    match err.downcast_ref::<io::Error>() {
        Some(e) if e.kind() == io::ErrorKind::NotFound => {
            let id = thread_rng().gen::<i64>().abs() as u64;
            write_yaml_to_file(&id, the_path).unwrap();
            return id;
        }
        _ => panic!("{}", err),
    }
}

pub fn write_yaml_to_file<T: serde::Serialize, P: AsRef<path::Path>>
    (data: &T, path: P)
     -> Result<(), GenericError> {
    let mut tmp_ext = path.as_ref().extension().unwrap().to_owned();
    tmp_ext.push(".tmp");
    let tmp_path = path.as_ref().with_extension(tmp_ext);
    let mut tmp_file = try!(fs::File::create(&tmp_path));
    try!(serde_yaml::to_writer(&mut tmp_file, data));
    drop(tmp_file);
    try!(fs::rename(&tmp_path, path));
    Ok(())
}

pub fn read_yaml_from_file<T: serde::Deserialize, P: AsRef<path::Path>>
    (path: P)
     -> Result<T, GenericError> {
    let file = try!(fs::File::open(path));
    let result = try!(serde_yaml::from_reader(&file));
    Ok(result)
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
