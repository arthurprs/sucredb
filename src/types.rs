pub type NodeId = u64;
pub type Token = u64;
pub type VNodeId = u16;
#[derive(PartialEq, Eq, Hash, Serialize, Deserialize, Default, Copy, Clone)]
pub struct Cookie(u64, u64);

impl Cookie {
    pub fn new(a: u64, b: u64) -> Self {
        Cookie(a, b)
    }
}

impl ::std::fmt::Debug for Cookie {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "{:016X}{:016X}", self.0, self.1)
    }
}

#[derive(Copy, Clone, Debug)]
pub enum ConsistencyLevel {
    One,
    Quorum,
    All,
}

#[derive(Copy, Clone, Debug)]
pub struct ConsistencyLevelParseError;

impl ::std::str::FromStr for ConsistencyLevel {
    type Err = ConsistencyLevelParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = s.as_bytes();
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

impl ConsistencyLevel {
    pub fn required(&self, replicas: u8) -> u8 {
        match *self {
            ConsistencyLevel::One => 1,
            ConsistencyLevel::Quorum => replicas / 2 + 1,
            ConsistencyLevel::All => replicas,
        }
    }
}
