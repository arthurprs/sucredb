use version_vector::*;
use database::*;

#[derive(Debug, Copy, Clone)]
pub enum FabricMsgType {
    Crud,
    Synch,
    Unknown,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FabricMsgError {
    CookieNotFound,
    BadVNodeStatus,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FabricMsg {
    RemoteGet(MsgRemoteGet),
    RemoteGetAck(MsgRemoteGetAck),
    RemoteSet(MsgRemoteSet),
    RemoteSetAck(MsgRemoteSetAck),
    SyncStart(MsgSyncStart),
    SyncSend(MsgSyncSend),
    SyncAck(MsgSyncAck),
    SyncFin(MsgSyncFin),
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
            FabricMsg::RemoteGet(..) => FabricMsgType::Crud,
            FabricMsg::RemoteGetAck(..) => FabricMsgType::Crud,
            FabricMsg::RemoteSet(..) => FabricMsgType::Crud,
            FabricMsg::RemoteSetAck(..) => FabricMsgType::Crud,
            FabricMsg::SyncStart(..) => FabricMsgType::Synch,
            FabricMsg::SyncSend(..) => FabricMsgType::Synch,
            FabricMsg::SyncAck(..) => FabricMsgType::Synch,
            FabricMsg::SyncFin(..) => FabricMsgType::Synch,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgRemoteGet {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub key: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgRemoteGetAck {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub result: Result<DottedCausalContainer<Vec<u8>>, FabricMsgError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgRemoteSet {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub key: Vec<u8>,
    pub container: DottedCausalContainer<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgRemoteSetAck {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub result: Result<(), FabricMsgError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgSet {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub version_vector: VersionVector,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgSetAck {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub result: Result<(), FabricMsgError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgSyncStart {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub clocks_in_peer: BitmappedVersionVector,
    pub target: Option<NodeId>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgSyncFin {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub result: Result<BitmappedVersionVector, FabricMsgError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgSyncSend {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub seq: u64,
    pub key: Vec<u8>,
    pub container: DottedCausalContainer<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgSyncAck {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub seq: u64,
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

impl_into!(RemoteGet, MsgRemoteGet);
impl_into!(RemoteGetAck, MsgRemoteGetAck);
impl_into!(RemoteSet, MsgRemoteSet);
impl_into!(RemoteSetAck, MsgRemoteSetAck);
impl_into!(SyncAck, MsgSyncAck);
impl_into!(SyncSend, MsgSyncSend);
impl_into!(SyncFin, MsgSyncFin);
impl_into!(SyncStart, MsgSyncStart);
