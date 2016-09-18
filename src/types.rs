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
        write!(f, "{:x}{:x}", self.0, self.1)
    }
}
