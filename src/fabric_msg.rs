use version_vector::*;
use database::*;

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
    GetRemote(MsgGetRemote),
    GetRemoteAck(MsgGetRemoteAck),
    Set(MsgSet),
    SetAck(MsgSetAck),
    SetRemote(MsgSetRemote),
    SetRemoteAck(MsgSetRemoteAck),
    BootstrapStart(MsgBootstrapStart),
    BootstrapSend(MsgBootstrapSend),
    BootstrapAck(MsgBootstrapAck),
    BootstrapFin(MsgBootstrapFin),
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
pub struct MsgGetRemote {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub key: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgGetRemoteAck {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub result: Result<DottedCausalContainer<Vec<u8>>, FabricMsgError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgSetRemote {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub key: Vec<u8>,
    pub container: DottedCausalContainer<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgSetRemoteAck {
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
pub struct MsgBootstrapStart {
    pub vnode: VNodeId,
    pub cookie: Cookie,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgBootstrapFin {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub result: Result<BitmappedVersionVector, FabricMsgError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgBootstrapSend {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub key: Vec<u8>,
    pub container: DottedCausalContainer<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgBootstrapAck {
    pub vnode: VNodeId,
    pub cookie: Cookie,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgSyncStart {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub target: NodeId,
    pub clock_in_peer: BitmappedVersion,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgSyncFin {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub result: Result<BitmappedVersion, FabricMsgError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgSyncSend {
    pub vnode: VNodeId,
    pub cookie: Cookie,
    pub key: Vec<u8>,
    pub container: DottedCausalContainer<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgSyncAck {
    pub vnode: VNodeId,
    pub cookie: Cookie,
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

impl_into!(GetRemote, MsgGetRemote);
impl_into!(GetRemoteAck, MsgGetRemoteAck);
impl_into!(Set, MsgSet);
impl_into!(SetAck, MsgSetAck);
impl_into!(SetRemote, MsgSetRemote);
impl_into!(SetRemoteAck, MsgSetRemoteAck);

impl_into!(BootstrapAck, MsgBootstrapAck);
impl_into!(BootstrapSend, MsgBootstrapSend);
impl_into!(BootstrapFin, MsgBootstrapFin);
impl_into!(BootstrapStart, MsgBootstrapStart);

impl_into!(SyncAck, MsgSyncAck);
impl_into!(SyncSend, MsgSyncSend);
impl_into!(SyncFin, MsgSyncFin);
impl_into!(SyncStart, MsgSyncStart);
