use std::time;
use std::collections::{BTreeMap, HashMap};
use linear_map::LinearMap;
use version_vector::*;
use storage::*;
use database::*;
use bincode::{self, serde as bincode_serde};
use fabric::*;
use rand::{Rng, thread_rng};

// FIXME: use a more efficient ring buffer
// 1MB of storage considering key avg length of 32B and 16B overhead
const PEER_LOG_SIZE: usize = 1024 * 1024 / (32 + 16);

#[repr(u8)]
pub enum VNodeStatus {
    InSync,
    Syncing,
    Zombie,
}

pub struct VNode {
    state: VNodeState,
    migrations: LinearMap<Cookie, Migration>,
    syncs: LinearMap<Cookie, Synchronization>,
    inflight: HashMap<Cookie, ReqState>,
}

struct VNodeState {
    num: u16,
    peers: LinearMap<NodeId, VNodePeer>,
    clocks: BitmappedVersionVector,
    log: BTreeMap<u64, Vec<u8>>,
    storage: Storage,
    unflushed_coord_writes: usize,
}

struct SavedVNodeState {
    peers: LinearMap<NodeId, VNodePeer>,
    clocks: BitmappedVersionVector,
    log: BTreeMap<u64, Vec<u8>>,
    clean_shutdown: bool,
}

#[derive(Default)]
struct VNodePeer {
    knowledge: u64,
    log: BTreeMap<u64, Vec<u8>>,
}

pub struct ReqState {
    replies: u8,
    succesfull: u8,
    required: u8,
    total: u8,
    // only used for get
    container: DottedCausalContainer<Vec<u8>>,
    token: Token,
}

enum Migration {
    Outgoing {
        cookie: Cookie,
        peer: NodeId,
        clocks_snapshot: BitmappedVersionVector,
        iterator: StorageIterator,
        count: u64,
    },
    Incomming {
        cookie: Cookie,
        peer: NodeId,
        count: u64,
    },
}

enum Synchronization {
    Outgoing {
        clock_in_peer: BitmappedVersion,
        clock_snapshot: BitmappedVersion,
        missing_dots: BitmappedVersionDelta,
        cookie: Cookie,
        peer: NodeId,
        count: u64,
    },
    Incomming {
        cookie: Cookie,
        peer: NodeId,
        count: u64,
    },
}

impl VNodePeer {
    fn new() -> VNodePeer {
        VNodePeer {
            knowledge: 0,
            log: Default::default(),
        }
    }

    fn advance_knowledge(&mut self, until: u64) {
        debug_assert!(until >= self.knowledge);
        self.knowledge = until;
    }

    fn log(&mut self, dot: u64, key: Vec<u8>) {
        let prev = self.log.insert(dot, key);
        // FIXME: if present keys should match, use entry api for this
        debug_assert!(prev.is_none());
        while self.log.len() > PEER_LOG_SIZE {
            let min = self.log.keys().next().cloned().unwrap();
            self.log.remove(&min).unwrap();
        }
    }

    fn get(&self, dot: u64) -> Option<Vec<u8>> {
        self.log.get(&dot).cloned()
    }
}

macro_rules! forward {
    ($this: expr, $db: expr, $from: expr, $msg: expr, $emsg: ident, $col: ident, $f: ident) => {
        // FIXME: use entry api
        let cookie = $msg.cookie;
        if {
            match $this.$col.get_mut(&cookie) {
                Some(x) => x.$f($db, &mut $this.state, $msg),
                None => {
                    debug!("NotFound {}[{:?}]", stringify!($col), cookie);
                    $db.fabric.send_message($from,  $emsg {
                        cookie: $msg.cookie,
                        vnode: $msg.vnode,
                        result: Err(FabricMsgError::CookieNotFound),
                    }).unwrap();
                    return;
                }
            }
        } {
            info!("Removing {}[{:?}]", stringify!($col), ($from, cookie));
            $this.$col.remove(&cookie).unwrap();
        }
    }
}

impl ReqState {
    fn new(token: Token, nodes: usize) -> Self {
        ReqState {
            required: (nodes as u8) / 2 + 1,
            total: nodes as u8,
            replies: 0,
            succesfull: 0,
            container: DottedCausalContainer::new(),
            token: token,
        }
    }
}

impl VNode {
    pub fn new(db: &Database, num: u16, create: bool) -> VNode {
        let storage = db.storage_manager.open(num as i32, true).unwrap();
        if create {
            db.meta_storage.del(num.to_string().as_bytes());
            storage.clear();
        } else {
            // TODO: load state :)
        }
        VNode {
            state: VNodeState {
                num: num,
                clocks: BitmappedVersionVector::new(),
                peers: Default::default(),
                log: Default::default(),
                storage: storage,
                unflushed_coord_writes: 0,
            },
            inflight: Default::default(),
            migrations: Default::default(),
            syncs: Default::default(),
        }

    }

    pub fn clear(db: &Database, num: u16) {
        db.meta_storage.del(num.to_string().as_bytes());
        if let Ok(storage) = db.storage_manager.open(num as i32, false) {
            storage.clear();
        }
    }

    pub fn syncs_inflight(&self) -> usize {
        self.syncs.len()
    }

    pub fn migrations_inflight(&self) -> usize {
        self.migrations.len()
    }

    fn gen_cookie(&mut self) -> u64 {
        // FIXME: make cookie really unique
        thread_rng().gen()
    }

    // TICK
    pub fn handler_tick(&mut self, time: time::SystemTime) {
        //
    }

    // CLIENT CRUD
    pub fn do_get(&mut self, db: &Database, token: Token, key: &[u8]) {
        let nodes = db.dht.nodes_for_vnode(self.state.num, false);
        let cookie = self.gen_cookie();
        assert!(self.inflight.insert(cookie, ReqState::new(token, nodes.len())).is_none());

        for node in nodes {
            if node == db.dht.node() {
                let container = self.state.storage_get(key);
                self.process_get(db, cookie, Some(container));
            } else {
                db.fabric
                    .send_message(node,
                                  MsgGetRemote {
                                      cookie: cookie,
                                      vnode: self.state.num,
                                      key: key.into(),
                                  })
                    .unwrap();
            }
        }
    }

    pub fn do_set(&mut self, db: &Database, token: Token, key: &[u8], value_opt: Option<&[u8]>,
                  vv: VersionVector) {
        let nodes = db.dht.nodes_for_vnode(self.state.num, true);
        let cookie = self.gen_cookie();
        assert!(self.inflight.insert(cookie, ReqState::new(token, nodes.len())).is_none());

        let dcc = self.state
            .storage_set_local(db.dht.node(), key, value_opt, &vv);
        self.process_set(db, cookie, true);
        for node in nodes {
            if node != db.dht.node() {
                db.fabric
                    .send_message(node,
                                  MsgSetRemote {
                                      cookie: cookie,
                                      vnode: self.state.num,
                                      key: key.into(),
                                      container: dcc.clone(),
                                  })
                    .unwrap();
            }
        }
    }

    // OTHER
    fn process_get(&mut self, db: &Database, cookie: Cookie,
                   container_opt: Option<DottedCausalContainer<Vec<u8>>>) {
        // FIXME: entry API to avoid double fetch
        if {
            let state = self.inflight.get_mut(&cookie).unwrap();
            state.replies += 1;
            if let Some(container) = container_opt {
                state.container.sync(container);
                state.succesfull += 1;
            }
            trace!("process_get c:{} tk:{} tl:{} - r:{} s:{} r:{}",
                   cookie,
                   state.token,
                   state.total,
                   state.succesfull,
                   state.replies,
                   state.required);
            if state.succesfull == state.required {
                // return to client & remove state
                true
            } else {
                false
            }
        } {
            let state = self.inflight.remove(&cookie).unwrap();
            db.respond_get(state.token, state.container);
        }
    }

    fn process_set(&mut self, db: &Database, cookie: Cookie, succesfull: bool) {
        // FIXME: entry API to avoid double fetch
        if {
            let state = self.inflight.get_mut(&cookie).unwrap();
            state.replies += 1;
            if succesfull {
                state.succesfull += 1;
            }
            trace!("process_set c:{} tk:{} tl:{} - r:{} s:{} r:{}",
                   cookie,
                   state.token,
                   state.total,
                   state.succesfull,
                   state.replies,
                   state.required);
            if state.succesfull == state.required {
                // return to client & remove state
                true
            } else {
                false
            }
        } {
            let state = self.inflight.remove(&cookie).unwrap();
            db.respond_set(state.token, state.container);
        }
    }

    // CRUD HANDLERS

    pub fn handler_get_remote_ack(&mut self, db: &Database, _from: NodeId, msg: MsgGetRemoteAck) {
        self.process_get(db, msg.cookie, msg.result.ok());
    }

    pub fn handler_get_remote(&mut self, db: &Database, from: NodeId, msg: MsgGetRemote) {
        let dcc = self.state.storage_get(&msg.key);
        db.fabric
            .send_message(from,
                          FabricMsg::GetRemoteAck(MsgGetRemoteAck {
                              cookie: msg.cookie,
                              vnode: msg.vnode,
                              result: Ok(dcc),
                          }))
            .unwrap();
    }

    pub fn handler_set(&mut self, db: &Database, from: NodeId, msg: MsgSet) {
        unimplemented!()
    }

    pub fn handler_set_remote(&mut self, db: &Database, from: NodeId, msg: MsgSetRemote) {
        let MsgSetRemote { key, container, vnode, cookie } = msg;
        let result = self.state.storage_set_remote(&key, container, Some(from));
        db.fabric
            .send_message(from,
                          FabricMsg::SetRemoteAck(MsgSetRemoteAck {
                              vnode: vnode,
                              cookie: cookie,
                              result: Ok(result),
                          }))
            .unwrap();
    }

    pub fn handler_set_remote_ack(&mut self, db: &Database, from: NodeId, msg: MsgSetRemoteAck) {
        self.process_set(db, msg.cookie, msg.result.is_ok());
    }

    // BOOTSTRAP
    pub fn handler_bootstrap_start(&mut self, db: &Database, from: NodeId, msg: MsgBootstrapStart) {
        let cookie = msg.cookie;
        let mut migration = Migration::Outgoing {
            cookie: cookie,
            clocks_snapshot: self.state.clocks.clone(),
            iterator: self.state.storage.iter(),
            peer: from,
            count: 0,
        };
        migration.on_start(db, &mut self.state, msg);

        let p = self.migrations.insert(cookie, migration);
        assert!(p.is_none());
    }

    pub fn handler_bootstrap_send(&mut self, db: &Database, from: NodeId, msg: MsgBootstrapSend) {
        forward!(self, db, from, msg, MsgBootstrapFin, migrations, on_send);
    }

    pub fn handler_bootstrap_ack(&mut self, db: &Database, from: NodeId, msg: MsgBootstrapAck) {
        forward!(self, db, from, msg, MsgBootstrapFin, migrations, on_ack);
    }

    pub fn handler_bootstrap_fin(&mut self, db: &Database, from: NodeId, msg: MsgBootstrapFin) {
        forward!(self, db, from, msg, MsgBootstrapFin, migrations, on_fin);
    }

    pub fn handler_sync_start(&mut self, db: &Database, from: NodeId, msg: MsgSyncStart) {
        let cookie = msg.cookie;
        let clock_snapshot = self.state.clocks.get(db.dht.node()).cloned().unwrap_or_default();
        let clock_in_peer = msg.clock_in_peer.clone();
        let mut sync = Synchronization::Outgoing {
            cookie: cookie,
            missing_dots: clock_snapshot.delta(&clock_in_peer),
            clock_in_peer: clock_in_peer,
            clock_snapshot: clock_snapshot,
            peer: from,
            count: 0,
        };
        sync.on_start(db, &mut self.state, msg);

        let p = self.syncs.insert(cookie, sync);
        assert!(p.is_none());
    }

    pub fn handler_sync_send(&mut self, db: &Database, from: NodeId, msg: MsgSyncSend) {
        forward!(self, db, from, msg, MsgSyncFin, syncs, on_send);
    }

    pub fn handler_sync_ack(&mut self, db: &Database, from: NodeId, msg: MsgSyncAck) {
        forward!(self, db, from, msg, MsgSyncFin, syncs, on_ack);
    }

    pub fn handler_sync_fin(&mut self, db: &Database, from: NodeId, msg: MsgSyncFin) {
        forward!(self, db, from, msg, MsgSyncFin, syncs, on_fin);
    }

    /// //////
    pub fn start_migration(&mut self, db: &Database) {
        let cookie = self.gen_cookie();
        let mut nodes = db.dht.nodes_for_vnode(self.state.num, false);
        thread_rng().shuffle(&mut nodes);
        for node in nodes {
            if node == db.dht.node() {
                continue;
            }
            let migration = Migration::Incomming {
                cookie: cookie,
                peer: node,
                count: 0,
            };

            db.fabric
                .send_message(node,
                              MsgBootstrapStart {
                                  cookie: cookie,
                                  vnode: self.state.num,
                              })
                .unwrap();

            let p = self.migrations.insert(cookie, migration);
            assert!(p.is_none());
            return;
        }
        unreachable!();
    }

    pub fn start_sync(&mut self, db: &Database) {
        let incomming_count = self.syncs
            .values()
            .filter(|&s| match *s {
                Synchronization::Incomming { .. } => true,
                _ => false,
            })
            .count();

        // TODO: this limit should be configurable
        if incomming_count >= 1 {
            return;
        }

        let peer = {
            let mut peers = db.dht.nodes_for_vnode(self.state.num, false);
            peers.retain(|&i| i != db.dht.node());
            if let Some(&peer) = thread_rng().choose(&peers) {
                peer
            } else {
                return;
            }
        };
        let cookie = self.gen_cookie();
        let clock_in_peer = self.state.clocks.get(peer).cloned().unwrap_or_default();
        let sync = Synchronization::Incomming {
            peer: peer,
            cookie: cookie,
            count: 0,
        };

        db.fabric
            .send_message(peer,
                          MsgSyncStart {
                              cookie: cookie,
                              vnode: self.state.num,
                              clock_in_peer: clock_in_peer,
                          })
            .unwrap();
        let p = self.syncs.insert(cookie, sync);
        assert!(p.is_none());
    }
}

impl Drop for VNode {
    fn drop(&mut self) {
        // clean up any references to the storage
        self.inflight.clear();
        self.migrations.clear();
        self.syncs.clear();
    }
}


impl VNodeState {
    // STORAGE
    pub fn storage_get(&self, key: &[u8]) -> DottedCausalContainer<Vec<u8>> {
        let mut dcc = if let Some(bytes) = self.storage.get_vec(key) {
            bincode_serde::deserialize(&bytes).unwrap()
        } else {
            DottedCausalContainer::new()
        };
        dcc.fill(&self.clocks);
        dcc
    }

    pub fn storage_set_local(&mut self, id: u64, key: &[u8], value_opt: Option<&[u8]>,
                             vv: &VersionVector)
                             -> DottedCausalContainer<Vec<u8>> {
        let mut dcc = self.storage_get(key);
        dcc.discard(vv);
        let dot = self.clocks.event(id);
        if let Some(value) = value_opt {
            dcc.add(id, dot, value.into());
        }
        dcc.strip(&self.clocks);

        if dcc.is_empty() {
            self.storage.del(key);
        } else {
            let mut bytes = Vec::new();
            bincode_serde::serialize_into(&mut bytes, &dcc, bincode::SizeLimit::Infinite).unwrap();
            self.storage.set(key, &bytes);
        }

        self.log.insert(dot, key.into());

        self.unflushed_coord_writes += 1;
        if self.unflushed_coord_writes >= PEER_LOG_SIZE {
            self.unflushed_coord_writes = 0;
            self.storage.sync();
        }

        dcc
    }

    pub fn storage_set_remote(&mut self, key: &[u8], mut new_dcc: DottedCausalContainer<Vec<u8>>,
                              log: Option<u64>) {
        let old_dcc = self.storage_get(key);
        new_dcc.add_to_bvv(&mut self.clocks);
        new_dcc.sync(old_dcc);
        new_dcc.strip(&self.clocks);

        if new_dcc.is_empty() {
            self.storage.del(key);
        } else {
            let mut bytes = Vec::new();
            bincode_serde::serialize_into(&mut bytes, &new_dcc, bincode::SizeLimit::Infinite)
                .unwrap();
            self.storage.set(key, &bytes);
        }

        if let Some(peer_id) = log {
            self.peers
                .entry(peer_id)
                .or_insert_with(|| Default::default())
                .log(self.clocks.get(peer_id).unwrap().base(), key.into());
        }
    }
}

#[allow(unused_variables)]
impl Synchronization {
    fn outgoing_send(&mut self, db: &Database, state: &mut VNodeState) -> bool {
        match *self {
            Synchronization::Outgoing { peer,
                                        cookie,
                                        ref mut missing_dots,
                                        ref mut count,
                                        ref clock_snapshot,
                                        .. } => {
                let log = &state.log;
                let kv_opt = missing_dots.next()
                    .and_then(|dot| log.get(&dot))
                    .and_then(|k| state.storage.get_vec(&k).map(|v| (k, v)));
                if let Some((k, v)) = kv_opt {
                    db.fabric
                        .send_message(peer,
                                      MsgSyncSend {
                                          cookie: cookie,
                                          vnode: state.num,
                                          key: k.clone(),
                                          container: bincode_serde::deserialize(&v).unwrap(),
                                      })
                        .unwrap();
                    *count += 1;
                } else {
                    debug!("[{}] synchronization of {} is done", db.dht.node(), state.num);
                    db.fabric
                        .send_message(peer,
                                      MsgSyncFin {
                                          cookie: cookie,
                                          vnode: state.num,
                                          result: Ok(clock_snapshot.clone()),
                                      })
                        .unwrap();
                }
                false
            }
            _ => unreachable!(),
        }
    }

    fn on_start(&mut self, db: &Database, state: &mut VNodeState, msg: MsgSyncStart) -> bool {
        match *self {
            Synchronization::Outgoing { .. } => self.outgoing_send(db, state),
            _ => unreachable!(),
        }
    }

    fn on_send(&mut self, db: &Database, state: &mut VNodeState, msg: MsgSyncSend) -> bool {
        match *self {
            Synchronization::Incomming { peer, ref mut count, .. } => {
                let mut bytes = Vec::new();
                bincode_serde::serialize_into(&mut bytes,
                                              &msg.container,
                                              bincode::SizeLimit::Infinite)
                    .unwrap();
                state.storage.set(&msg.key, &bytes);

                db.fabric
                    .send_message(peer,
                                  MsgSyncAck {
                                      cookie: msg.cookie,
                                      vnode: state.num,
                                  })
                    .unwrap();

                *count += 1;
                false
            }
            _ => unreachable!(),
        }
    }

    fn on_fin(&mut self, db: &Database, state: &mut VNodeState, msg: MsgSyncFin) -> bool {
        match *self {
            Synchronization::Incomming { peer, .. } => {
                if msg.result.is_ok() {
                    let bvv = BitmappedVersionVector::new();
                    state.clocks.entry_or_default(db.dht.node()).join(msg.result.as_ref().unwrap());
                    state.storage.sync();
                    // send it back as a form of ack-ack
                    db.fabric.send_message(peer, msg).unwrap();
                }
            }
            Synchronization::Outgoing { ref clock_in_peer, peer, .. } => {
                // TODO: advance to the snapshot base instead? Is it safe to do so?
                state.peers
                    .entry(peer)
                    .or_insert_with(|| Default::default())
                    .advance_knowledge(clock_in_peer.base());
            }
        }
        true
    }

    fn on_ack(&mut self, db: &Database, state: &mut VNodeState, msg: MsgSyncAck) -> bool {
        match *self {
            Synchronization::Outgoing { .. } => self.outgoing_send(db, state),
            _ => unreachable!(),
        }
    }
}

#[allow(unused_variables)]
impl Migration {
    fn outgoing_send(&mut self, db: &Database, state: &mut VNodeState) -> bool {
        match *self {
            Migration::Outgoing { peer,
                                  cookie,
                                  ref mut iterator,
                                  ref mut count,
                                  ref clocks_snapshot } => {
                let done = !iterator.iter(|k, v| {
                    db.fabric
                        .send_message(peer,
                                      MsgBootstrapSend {
                                          cookie: cookie,
                                          vnode: state.num,
                                          key: k.into(),
                                          container: bincode_serde::deserialize(v).unwrap(),
                                      })
                        .unwrap();
                    *count += 1;
                    false
                });
                if done {
                    debug!("[{}] sync of {} is done", db.dht.node(), state.num);
                    db.fabric
                        .send_message(peer,
                                      MsgBootstrapFin {
                                          cookie: cookie,
                                          vnode: state.num,
                                          result: Ok(clocks_snapshot.clone()),
                                      })
                        .unwrap();
                }
                false
            }
            _ => unreachable!(),
        }
    }

    fn on_start(&mut self, db: &Database, state: &mut VNodeState, msg: MsgBootstrapStart) -> bool {
        match *self {
            Migration::Outgoing { .. } => self.outgoing_send(db, state),
            _ => unreachable!(),
        }
    }

    fn on_send(&mut self, db: &Database, state: &mut VNodeState, msg: MsgBootstrapSend) -> bool {
        match *self {
            Migration::Incomming { peer, ref mut count, .. } => {
                state.storage_set_remote(&msg.key, msg.container, None);
                db.fabric
                    .send_message(peer,
                                  MsgBootstrapAck {
                                      cookie: msg.cookie,
                                      vnode: state.num,
                                  })
                    .unwrap();

                *count += 1;
                false
            }
            _ => unreachable!(),
        }
    }

    fn on_fin(&mut self, db: &Database, state: &mut VNodeState, msg: MsgBootstrapFin) -> bool {
        match *self {
            Migration::Incomming { peer, .. } => {
                if msg.result.is_ok() {
                    state.clocks.merge(msg.result.as_ref().unwrap());
                    state.storage.sync();
                    db.dht.promote_pending_node(state.num, db.dht.node()).unwrap();
                    // send it back as a form of ack-ack
                    db.fabric.send_message(peer, msg).unwrap();
                }
            }
            Migration::Outgoing { .. } => (),
        }
        true
    }

    fn on_ack(&mut self, db: &Database, state: &mut VNodeState, msg: MsgBootstrapAck) -> bool {
        match *self {
            Migration::Outgoing { .. } => self.outgoing_send(db, state),
            _ => unreachable!(),
        }
    }
}
