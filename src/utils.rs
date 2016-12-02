use std::hash::{Hasher, BuildHasherDefault};
use std::collections::HashMap;
use std::{path, fs};
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
        let mut hash = self.0;
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
        hash ^= hash >> 33;
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
            self.0 = self.0.rotate_left(32) ^ temp;
        }
    }
}

pub fn assume_str(bytes: &[u8]) -> &str {
    unsafe { ::std::str::from_utf8_unchecked(bytes) }
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
