use crc16;

pub const HASH_SLOTS: u16 = 16384;

/// RedisCluster style partitioning
pub fn hash_slot(key: &[u8]) -> u16 {
    crc16::State::<crc16::XMODEM>::calculate(key) % HASH_SLOTS
}
