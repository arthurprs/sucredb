use crc16;

pub const HASH_SLOTS: u16 = 16384;

/// RedisCluster style partitioning
pub fn hash_slot(mut key: &[u8]) -> u16 {
    if let Some(open) = key.iter().position(|&x| x == b'{') {
        // note that close will be relative to open due to the skip()
        if let Some(close) = key.iter().skip(open + 1).position(|&x| x == b'}') {
            if close > 0 {
                // found  { and } with something in between
                key = &key[open + 1..open + 1 + close];
            }
        }
    }
    crc16::State::<crc16::XMODEM>::calculate(key) % HASH_SLOTS
}


#[cfg(test)]
mod tests {
    use super::*;

    fn raw_hash(key: &[u8]) -> u16 {
        crc16::State::<crc16::XMODEM>::calculate(key) % HASH_SLOTS
    }

    #[test]
    fn test_hash_slot() {
        assert_eq!(hash_slot(b"{}"), raw_hash(b"{}"));
        assert_eq!(hash_slot(b"_{abc}"), raw_hash(b"abc"));
        assert_eq!(hash_slot(b"{abc}_"), raw_hash(b"abc"));
        assert_eq!(hash_slot(b"_{abc}_"), raw_hash(b"abc"));
        assert_eq!(hash_slot(b"{abc}{def}"), raw_hash(b"abc"));
        assert_eq!(hash_slot(b"{}{abc}"), raw_hash(b"{}{abc}"));
        assert_eq!(hash_slot(b"{abc}{}"), raw_hash(b"abc"));
        assert_eq!(hash_slot(b"{{abc}}"), raw_hash(b"{abc"));
    }
}
