use std::hash::{Hasher, SipHasher};

pub fn hash(key: &[u8]) -> u64 {
    let mut hasher = SipHasher::new();
    hasher.write(key);
    hasher.finish()
}
