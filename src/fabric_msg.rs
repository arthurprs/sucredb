use version_vector::{VersionVector, DottedCausalContainer};

// TODO: have only a few toplevel types
// like Gossip, KV, Boostrap, etc..
// TODO: error support
#[derive(Debug, Copy, Clone)]
#[repr(u8)]
pub enum FabricMsgType {
    Crud,
    Bootstrap,
    Other,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FabricMsgError {
    VNodeNotFound,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FabricMsg {
    GetRemote(FabricMsgGetRemote),
    GetRemoteAck(FabricMsgGetRemoteAck),
    Set(FabricMsgSet),
    SetAck(FabricMsgSetAck),
    SetRemote(FabricMsgSetRemote),
    SetRemoteAck(FabricMsgSetRemoteAck),
    Bootstrap,
    BootstrapStream,
    BootstrapFin,
    SyncStart,
    SyncStream,
    SyncAck,
    SyncFin,
    Other,
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
            FabricMsg::Bootstrap => FabricMsgType::Bootstrap,
            FabricMsg::BootstrapStream => FabricMsgType::Bootstrap,
            FabricMsg::BootstrapFin => FabricMsgType::Bootstrap,
            // FabricMsg::SyncStart => FabricMsgType::SyncStart,
            // FabricMsg::SyncStream => FabricMsgType::SyncStream,
            // FabricMsg::SyncAck => FabricMsgType::SyncAck,
            // FabricMsg::SyncFin => FabricMsgType::SyncFin,
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
