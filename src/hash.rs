#[allow(deprecated)]
use std::hash::{Hasher, Hash, SipHasher};

#[allow(deprecated)]
pub fn hash<T: Hash>(key: T) -> u64 {
    let mut hasher = SipHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}
