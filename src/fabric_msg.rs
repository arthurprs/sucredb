use version_vector::{VersionVector, DottedCausalContainer};

// TODO: have only a few toplevel types
// like Gossip, KV, Boostrap, etc..
// TODO: error support
#[derive(Debug, Copy, Clone)]
pub enum FabricMsgType {
    Get,
    GetAck,
    Set,
    SetAck,
    SetRemote,
    SetRemoteAck,
    Bootstrap,
    BootstrapStream,
    BootstrapFin,
    SyncStart,
    SyncStream,
    SyncAck,
    SyncFin,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FabricMsg {
    Get(FabricMsgGet),
    GetAck(FabricMsgGetAck),
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
            FabricMsg::Get(..) => FabricMsgType::Get,
            FabricMsg::GetAck(..) => FabricMsgType::GetAck,
            FabricMsg::Set(..) => FabricMsgType::Set,
            FabricMsg::SetAck(..) => FabricMsgType::SetAck,
            FabricMsg::SetRemote(..) => FabricMsgType::SetRemote,
            FabricMsg::SetRemoteAck(..) => FabricMsgType::SetRemoteAck,
            FabricMsg::Bootstrap => FabricMsgType::Bootstrap,
            FabricMsg::BootstrapStream => FabricMsgType::BootstrapStream,
            FabricMsg::BootstrapFin => FabricMsgType::BootstrapFin,
            FabricMsg::SyncStart => FabricMsgType::SyncStart,
            FabricMsg::SyncStream => FabricMsgType::SyncStream,
            FabricMsg::SyncAck => FabricMsgType::SyncAck,
            FabricMsg::SyncFin => FabricMsgType::SyncFin,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FabricMsgGet {
    pub vnode: u16,
    pub cookie: u64,
    pub key: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FabricMsgGetAck {
    pub vnode: u16,
    pub cookie: u64,
    pub container: DottedCausalContainer<Vec<u8>>,
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
}
