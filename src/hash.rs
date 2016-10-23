#[allow(deprecated)]
use std::hash::{Hasher, SipHasher};

#[allow(deprecated)]
pub fn hash(key: &[u8]) -> u64 {
    let mut hasher = SipHasher::new();
    hasher.write(key);
    hasher.finish()
}
