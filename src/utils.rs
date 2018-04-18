use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::hash::{BuildHasherDefault, Hasher};
use std::{fmt, fs, io, path};

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

pub fn replace_default<T: Default>(subject: &mut T) -> T {
    ::std::mem::replace(subject, Default::default())
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

pub fn is_dir_empty_or_absent<P: AsRef<path::Path>>(path: P) -> io::Result<bool> {
    match fs::read_dir(path.as_ref()) {
        Ok(dir) => Ok(dir.count() == 0),
        Err(ref err) if err.kind() == io::ErrorKind::NotFound => Ok(true),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
pub fn sleep_ms(ms: u64) {
    ::std::thread::sleep(::std::time::Duration::from_millis(ms));
}

#[cfg(test)]
macro_rules! assert_eq_repr {
    ($left:expr, $right:expr) => {{
        match (format!("{:?}", &$left), format!("{:?}", &$right)) {
            (left_val, right_val) => {
                if !(left_val == right_val) {
                    panic!(
                        "repr assertion failed: `(debug(left) == debug(right))` \
                         (left: `{:?}`, right: `{:?}`)",
                        left_val, right_val
                    )
                }
            }
        }
    }};
}

pub trait LoggerExt {
    fn log_error(&self, msg: &str);
    fn log_warn(&self, msg: &str);
}

impl<T, U: fmt::Debug> LoggerExt for Result<T, U> {
    fn log_error(&self, msg: &str) {
        if let &Err(ref e) = self {
            error!("{}: {:?}", msg, e);
        }
    }
    fn log_warn(&self, msg: &str) {
        if let &Err(ref e) = self {
            warn!("{}: {:?}", msg, e);
        }
    }
}
