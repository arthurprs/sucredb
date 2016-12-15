use crc16;

pub const HASH_SLOTS: u16 = 16384;

pub fn hash_slot(key: &[u8]) -> u16 {
    crc16::State::<crc16::XMODEM>::calculate(key) % HASH_SLOTS
}
