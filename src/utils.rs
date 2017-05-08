use std::hash::{Hasher, BuildHasherDefault};
use std::collections::{HashMap, HashSet};
use std::{io, path, fs};
use std::error::Error;
use serde;
use serde_json;

pub type GenericError = Box<Error + Send + Sync + 'static>;

pub type IdHasherBuilder = BuildHasherDefault<IdHasher>;
pub type IdHashMap<K, V> = HashMap<K, V, IdHasherBuilder>;
pub type IdHashSet<K> = HashSet<K, IdHasherBuilder>;
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
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {

        #[inline]
        fn mix(mut x: u64) -> u64 {
            // Seahash diffuse method
            x = x.wrapping_mul(0x6eed0e9da4d94a4f);
            let a = x >> 32;
            let b = x >> 60;
            x ^= a >> b;
            x.wrapping_mul(0x6eed0e9da4d94a4f)
        }

        debug_assert!(bytes.len() <= 8);
        unsafe {
            let mut temp = 0u64;
            ::std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                &mut temp as *mut _ as *mut u8,
                bytes.len(),
            );
            self.0 ^= mix(temp);
        }
    }
}

pub fn into_io_error<E: Error + Send + Sync + 'static>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

pub fn split_u64(uint: u64) -> (u32, u32) {
    ((uint >> 32) as u32, uint as u32)
}

pub fn join_u64(hi: u32, lo: u32) -> u64 {
    ((hi as u64) << 32) | (lo as u64)
}

pub fn assume_str(bytes: &[u8]) -> &str {
    unsafe { ::std::str::from_utf8_unchecked(bytes) }
}

pub fn write_json_to_file<T: serde::Serialize, P: AsRef<path::Path>>
    (data: &T, path: P)
     -> Result<(), GenericError> {
    let mut tmp_ext = path.as_ref().extension().unwrap().to_owned();
    tmp_ext.push(".tmp");
    let tmp_path = path.as_ref().with_extension(tmp_ext);
    let mut tmp_file = fs::File::create(&tmp_path)?;
    serde_json::to_writer_pretty(&mut tmp_file, data)?;
    drop(tmp_file);
    fs::rename(&tmp_path, path)?;
    Ok(())
}

pub fn read_json_from_file<T: serde::de::DeserializeOwned, P: AsRef<path::Path>>
    (path: P)
     -> Result<T, GenericError> {
    let file = fs::File::open(path)?;
    let result = serde_json::from_reader(&file)?;
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
