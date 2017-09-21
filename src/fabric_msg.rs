use version_vector::*;
use database::*;
use bytes::Bytes;

#[derive(Debug, Copy, Clone)]
pub enum FabricMsgType {
    Crud,
    Synch,
    DHT,
    Unknown,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum FabricMsgError {
    CookieNotFound,
    BadVNodeStatus,
    NotReady,
    SyncInterrupted,
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
    DHTSyncReq(VersionVector),
    DHTSync(Bytes),
    Unknown,
}

impl FabricMsg {
    pub fn get_type(&self) -> FabricMsgType {
        match *self {
            FabricMsg::RemoteGet(..) |
            FabricMsg::RemoteGetAck(..) |
            FabricMsg::RemoteSet(..) |
            FabricMsg::RemoteSetAck(..) => FabricMsgType::Crud,
            FabricMsg::SyncStart(..) |
            FabricMsg::SyncSend(..) |
            FabricMsg::SyncAck(..) |
            FabricMsg::SyncFin(..) => FabricMsgType::Synch,
            FabricMsg::DHTSync(..) | FabricMsg::DHTSyncReq(..) => FabricMsgType::DHT,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgRemoteGet {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub key: Bytes,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgRemoteGetAck {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub result: Result<DottedCausalContainer<Bytes>, FabricMsgError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgRemoteSet {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub key: Bytes,
    pub container: DottedCausalContainer<Bytes>,
    pub reply: bool,
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
    pub key: Bytes,
    pub value: Option<Bytes>,
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
    pub key: Bytes,
    pub container: DottedCausalContainer<Bytes>,
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
