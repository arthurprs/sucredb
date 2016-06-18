use version_vector::{VersionVector, DottedCausalContainer};

// TODO: have only a few toplevel types
// like Gossip, KV, Boostrap, etc..
// TODO: error support
#[derive(Debug, Copy, Clone)]
pub enum FabricMsgType {
    Crud,
    Bootstrap,
    Synch,
    Unknown,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FabricMsgError {
    VNodeNotFound,
    CookieNotFound,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FabricMsg {
    GetRemote(FabricMsgGetRemote),
    GetRemoteAck(FabricMsgGetRemoteAck),
    Set(FabricMsgSet),
    SetAck(FabricMsgSetAck),
    SetRemote(FabricMsgSetRemote),
    SetRemoteAck(FabricMsgSetRemoteAck),
    BootstrapStart(FabricBootstrapStart),
    BootstrapSend(FabricBootstrapSend),
    BootstrapAck(FabricBootstrapAck),
    BootstrapFin(FabricBootstrapFin),
    SyncStart(FabricBootstrapStart),
    SyncSend(FabricBootstrapSend),
    SyncAck(FabricBootstrapAck),
    SyncFin(FabricBootstrapSend),
    Unknown,
}

macro_rules! fmsg {
    ($e: expr, $v: path) => (
        match $e {
            $v(r) => r,
            _ => unreachable!(),
        }
    );
}

impl FabricMsg {
    pub fn get_type(&self) -> FabricMsgType {
        match *self {
            FabricMsg::GetRemote(..) => FabricMsgType::Crud,
            FabricMsg::GetRemoteAck(..) => FabricMsgType::Crud,
            FabricMsg::Set(..) => FabricMsgType::Crud,
            FabricMsg::SetAck(..) => FabricMsgType::Crud,
            FabricMsg::SetRemote(..) => FabricMsgType::Crud,
            FabricMsg::SetRemoteAck(..) => FabricMsgType::Crud,
            FabricMsg::BootstrapStart(..) => FabricMsgType::Bootstrap,
            FabricMsg::BootstrapSend(..) => FabricMsgType::Bootstrap,
            FabricMsg::BootstrapAck(..) => FabricMsgType::Bootstrap,
            FabricMsg::BootstrapFin(..) => FabricMsgType::Bootstrap,
            FabricMsg::SyncStart(..) => FabricMsgType::Synch,
            FabricMsg::SyncSend(..) => FabricMsgType::Synch,
            FabricMsg::SyncAck(..) => FabricMsgType::Synch,
            FabricMsg::SyncFin(..) => FabricMsgType::Synch,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FabricMsgGetRemote {
    pub vnode: u16,
    pub cookie: u64,
    pub key: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FabricMsgGetRemoteAck {
    pub vnode: u16,
    pub cookie: u64,
    pub result: Result<DottedCausalContainer<Vec<u8>>, FabricMsgError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FabricMsgSetRemote {
    pub vnode: u16,
    pub cookie: u64,
    pub key: Vec<u8>,
    pub container: DottedCausalContainer<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FabricMsgSetRemoteAck {
    pub vnode: u16,
    pub cookie: u64,
    pub result: Result<(), FabricMsgError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FabricMsgSet {
    pub vnode: u16,
    pub cookie: u64,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub version_vector: VersionVector,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FabricMsgSetAck {
    pub vnode: u16,
    pub cookie: u64,
    pub result: Result<(), FabricMsgError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FabricBootstrapStart {
    pub vnode: u16,
    pub cookie: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FabricBootstrapFin {
    pub vnode: u16,
    pub cookie: u64,
    pub result: Result<(), FabricMsgError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FabricBootstrapSend {
    pub vnode: u16,
    pub cookie: u64,
    pub key: Vec<u8>,
    pub container: DottedCausalContainer<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FabricBootstrapAck {
    pub vnode: u16,
    pub cookie: u64,
}

macro_rules! impl_into {
    ($w: ident, $msg: ident) => (
        impl Into<FabricMsg> for $msg {
            fn into(self) -> FabricMsg {
                FabricMsg::$w(self)
            }
        }
    );
}


impl_into!(GetRemote, FabricMsgGetRemote);
impl_into!(GetRemoteAck, FabricMsgGetRemoteAck);
impl_into!(Set, FabricMsgSet);
impl_into!(SetAck, FabricMsgSetAck);
impl_into!(SetRemote, FabricMsgSetRemote);
impl_into!(SetRemoteAck, FabricMsgSetRemoteAck);

impl_into!(BootstrapAck, FabricBootstrapAck);
impl_into!(BootstrapSend, FabricBootstrapSend);
impl_into!(BootstrapFin, FabricBootstrapFin);
impl_into!(BootstrapStart, FabricBootstrapStart);
