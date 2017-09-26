use std::convert::TryFrom;
use std::str::FromStr;
use std::fmt;

/// Identifier for a Database instance
/// node id should be a positive i64 to work nicelly with the RESP protocol
pub type NodeId = u64;
/// Identifier for physical node (high u32 of NodeId)
pub type PhysicalNodeId = u32;
/// Identifier for connection with client
pub type Token = u64;
/// Identifier for a vnode
pub type VNodeId = u16;

/// Identifier for communication between nodes
#[derive(PartialEq, Eq, Hash, Serialize, Deserialize, Default, Copy, Clone)]
pub struct Cookie(u64, u64);

impl Cookie {
    pub fn new(a: u64, b: u64) -> Self {
        Cookie(a, b)
    }
}

impl fmt::Debug for Cookie {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:016X}{:016X}", self.0, self.1)
    }
}

/// Consistency Level as in Dynamo/Riak/Cassandra style
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ConsistencyLevel {
    One,
    Quorum,
    All,
}

#[derive(Copy, Clone, Debug)]
pub struct ConsistencyLevelParseError;

impl<'a> TryFrom<&'a [u8]> for ConsistencyLevel {
    type Error = ConsistencyLevelParseError;
    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() > 0 {
            match bytes[0] {
                b'1' | b'o' | b'O' => return Ok(ConsistencyLevel::One),
                b'q' | b'Q' => return Ok(ConsistencyLevel::Quorum),
                b'a' | b'A' => return Ok(ConsistencyLevel::All),
                _ => (),
            }
        }
        Err(ConsistencyLevelParseError)
    }
}

impl FromStr for ConsistencyLevel {
    type Err = ConsistencyLevelParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s.as_bytes())
    }
}

impl ConsistencyLevel {
    pub fn required(&self, replicas: u8) -> u8 {
        match *self {
            ConsistencyLevel::One => 1,
            ConsistencyLevel::Quorum => replicas / 2 + 1,
            ConsistencyLevel::All => replicas,
        }
    }
}
