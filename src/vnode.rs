use std::time;
use std::collections::{BTreeMap, HashMap};
use std::collections::hash_map::Entry as HMEntry;
use linear_map::{LinearMap, Entry as LMEntry};
use version_vector::*;
use storage::*;
use database::*;
use bincode::{self, serde as bincode_serde};
use fabric::*;
use rand::{Rng, thread_rng};

// FIXME: use a more efficient ring buffer
// 1MB of storage considering key avg length of 32B and 16B overhead
const PEER_LOG_SIZE: usize = 1024 * 1024 / (32 + 16);

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum VNodeStatus {
    // steady state
    Ready,
    // was ready but wasn't shutdown cleanly, so it has to finish reverse sync to all replicas before coordinating writes
    Recover,
    // was absent, now it's streamming data from another node
    Bootstrap,
    // after another node takes over this vnode it stays as zombie until syncs are completed, etc.
    Zombie,
    // no actual date is present (metadata is retained though)
    Absent,
}

pub struct VNode {
    state: VNodeState,
    migrations: LinearMap<Cookie, Migration>,
    syncs: LinearMap<Cookie, Synchronization>,
    inflight: HashMap<Cookie, ReqState>,
}

struct VNodeState {
    num: u16,
    status: VNodeStatus,
    peers: LinearMap<NodeId, VNodePeer>,
    clocks: BitmappedVersionVector,
    log: VNodePeer,
    storage: Storage,
    unflushed_coord_writes: usize,
}

#[derive(Serialize, Deserialize)]
struct SavedVNodeState {
    peers: LinearMap<NodeId, VNodePeer>,
    clocks: BitmappedVersionVector,
    log: VNodePeer,
    clean_shutdown: bool,
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct VNodePeer {
    knowledge: Version,
    log: BTreeMap<Version, Vec<u8>>,
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

// TODO: take mut buffers instead of returning them
type SyncIteratorFn =
    Box<FnMut(&Storage) -> Option<(Vec<u8>, DottedCausalContainer<Vec<u8>>)> + Send>;

enum Synchronization {
    Outgoing {
        clock_in_peer: BitmappedVersion,
        clock_snapshot: BitmappedVersion,
        iterator: SyncIteratorFn,
        cookie: Cookie,
        peer: NodeId,
        count: u64,
        target: NodeId,
    },
    Incomming {
        cookie: Cookie,
        peer: NodeId,
        count: u64,
        target: NodeId,
    },
}

impl VNodePeer {
    fn new() -> VNodePeer {
        VNodePeer {
            knowledge: 0,
            log: Default::default(),
        }
    }

    fn advance_knowledge(&mut self, until: Version) {
        debug_assert!(until >= self.knowledge);
        self.knowledge = until;
    }

    fn log(&mut self, version: Version, key: Vec<u8>) {
        let min = self.min_version();
        if version > min {
            if cfg!(debug) {
                if let Some(removed) = self.log.insert(version, key.clone()) {
                    debug_assert!(removed == key);
                }
            } else {
                self.log.insert(version, key);
            }
            if self.log.len() > PEER_LOG_SIZE {
                self.log.remove(&min).unwrap();
            }
        }
    }

    fn get(&self, version: Version) -> Option<Vec<u8>> {
        self.log.get(&version).cloned()
    }

    fn min_version(&self) -> Version {
        self.log.keys().next().cloned().unwrap_or(0)
    }
}

macro_rules! check_status {
    ($this: expr, $($status:pat)|*, $db: expr, $from: expr, $msg: expr, $emsg: ident, $col: ident) => {
        match $this.status() {
            $($status)|* => (),
            state => {
                let cookie = $msg.cookie;
                debug!("Incorrect state for {}[{:?}] expected {} was {:?}",
                    stringify!($col), cookie, stringify!($($status)|*), state);
                $db.fabric.send_message($from,  $emsg {
                    cookie: $msg.cookie,
                    vnode: $msg.vnode,
                    result: Err(FabricMsgError::BadVNodeStatus),
                }).unwrap();
                return;
            },
        }
    }
}

macro_rules! forward {
    ($this: expr, $db: expr, $from: expr, $msg: expr, $emsg: ident, $col: ident, $f: ident) => {{
        let cookie = $msg.cookie;
        match $this.$col.entry(cookie) {
            LMEntry::Occupied(mut o) => {
                if o.get_mut().$f($db, &mut $this.state, $msg) {
                    o.remove();
                    debug!("Removing {}[{:?}]", stringify!($col), ($from, cookie));
                }
            },
            _ => {
                debug!("NotFound {}[{:?}]", stringify!($col), cookie);
                $db.fabric.send_message($from,  $emsg {
                    cookie: $msg.cookie,
                    vnode: $msg.vnode,
                    result: Err(FabricMsgError::CookieNotFound),
                }).unwrap();
                return;
            }
        }
    }};
    ($this: expr, $($status:pat)|*, $db: expr, $from: expr, $msg: expr, $emsg: ident, $col: ident, $f: ident) => {
        check_status!($this, $($status)|*, $db, $from, $msg, $emsg, $col);
        forward!($this, $db, $from, $msg, $emsg, $col, $f)
    };
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
    pub fn new(db: &Database, num: u16, status: VNodeStatus, is_create: bool) -> VNode {
        let state = VNodeState::load(num, db, status, is_create);
        state.save(db, false);

        if is_create {
            assert_eq!(status, VNodeStatus::Ready, "status must be ready when creating");
        }

        let mut vnode = VNode {
            state: state,
            inflight: Default::default(),
            migrations: Default::default(),
            syncs: Default::default(),
        };

        // FIXME: need to handle failure of these
        match vnode.status() {
            VNodeStatus::Bootstrap => {
                vnode.start_migration(db);
            }
            VNodeStatus::Recover => {
                vnode.start_sync(db, true);
            }
            _ => (),
        }

        vnode
    }

    // pub fn clear(db: &Database, num: u16) {
    //     db.meta_storage.del(num.to_string().as_bytes());
    //     if let Ok(storage) = db.storage_manager.open(num as i32, false) {
    //         storage.clear();
    //     }
    // }

    pub fn save(&mut self, db: &Database, shutdown: bool) {
        self.state.save(db, shutdown);
    }

    pub fn status(&self) -> VNodeStatus {
        self.state.status
    }

    #[cfg(test)]
    pub fn _log_len(&self) -> usize {
        self.state.log.log.len()
    }

    pub fn syncs_inflight(&self) -> usize {
        self.syncs.len()
    }

    pub fn migrations_inflight(&self) -> usize {
        self.migrations.len()
    }

    fn gen_cookie(&mut self) -> Cookie {
        // FIXME: make cookie really unique
        let mut rng = thread_rng();
        (rng.gen(), rng.gen())
    }

    // DHT Changes
    pub fn handler_dht_change(&mut self, db: &Database, x_status: VNodeStatus) {
        use self::VNodeStatus::*;
        match (self.status(), x_status) {
            (Ready, Absent) |
            (Bootstrap, Absent) |
            (Recover, Absent) => {
                self.state.status = Zombie;
                // cancel incomming bootstrap
                let cancels = self.migrations
                    .iter()
                    .filter_map(|(&cookie, m)| {
                        match *m {
                            Migration::Incomming { peer, .. } => Some((cookie, peer)),
                            _ => None,
                        }
                    })
                    .collect::<Vec<_>>();
                for (cookie, peer) in cancels {
                    if let Some(_) = self.migrations.remove(&cookie) {
                        db.fabric
                            .send_message(peer,
                                          MsgBootstrapFin {
                                              vnode: self.state.num,
                                              cookie: cookie,
                                              result: Err(FabricMsgError::BadVNodeStatus),
                                          })
                            .unwrap();
                    }
                }

                // cancel incomming sync
                let cancels = self.syncs
                    .iter()
                    .filter_map(|(&cookie, s)| {
                        match *s {
                            Synchronization::Incomming { peer, .. } => Some((cookie, peer)),
                            _ => None,
                        }
                    })
                    .collect::<Vec<_>>();
                for (cookie, peer) in cancels {
                    if let Some(_) = self.syncs.remove(&cookie) {
                        db.fabric
                            .send_message(peer,
                                          MsgSyncFin {
                                              vnode: self.state.num,
                                              cookie: cookie,
                                              result: Err(FabricMsgError::BadVNodeStatus),
                                          })
                            .unwrap();
                    }
                }

            }
            (Zombie, Absent) => (),
            (Zombie, Ready) => {
                // fast-recomission!
                self.state.status = Ready;
            }
            (Absent, Ready) => {
                // recomission by bootstrap
                self.start_migration(db);
            }
            (Bootstrap, Ready) |
            (Recover, Ready) => (),
            (a, b) if a == b => (),
            (a, b) => panic!("Invalid change {:?} -> {:?}", a, b),
        }
    }

    // TICK
    pub fn handler_tick(&mut self, _time: time::SystemTime) {
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
                                  MsgRemoteGet {
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
            .storage_set_local(db, key, value_opt, &vv);
        self.process_set(db, cookie, true);
        for node in nodes {
            if node != db.dht.node() {
                db.fabric
                    .send_message(node,
                                  MsgRemoteSet {
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
        if let HMEntry::Occupied(mut o) = self.inflight.entry(cookie) {
            if {
                let state = o.get_mut();
                state.replies += 1;
                if let Some(container) = container_opt {
                    state.container.sync(container);
                    state.succesfull += 1;
                }
                if state.succesfull == state.required || state.replies == state.total {
                    // return to client & remove state
                    true
                } else {
                    false
                }
            } {
                let state = o.remove();
                db.respond_get(state.token, state.container);
            }
        } else {
            debug!("process_get cookie not found {:?}", cookie);
        }

    }

    fn process_set(&mut self, db: &Database, cookie: Cookie, succesfull: bool) {
        if let HMEntry::Occupied(mut o) = self.inflight.entry(cookie) {
            if {
                let state = o.get_mut();
                state.replies += 1;
                if succesfull {
                    state.succesfull += 1;
                }
                if state.succesfull == state.required || state.replies == state.total {
                    // return to client & remove state
                    true
                } else {
                    false
                }
            } {
                let state = o.remove();
                db.respond_set(state.token, state.container);
            }
        } else {
            debug!("process_set cookie not found {:?}", cookie);
        }
    }

    // CRUD HANDLERS

    pub fn handler_get_remote_ack(&mut self, db: &Database, _from: NodeId, msg: MsgRemoteGetAck) {
        self.process_get(db, msg.cookie, msg.result.ok());
    }

    pub fn handler_get_remote(&mut self, db: &Database, from: NodeId, msg: MsgRemoteGet) {
        let dcc = self.state.storage_get(&msg.key);
        db.fabric
            .send_message(from,
                          MsgRemoteGetAck {
                              cookie: msg.cookie,
                              vnode: msg.vnode,
                              result: Ok(dcc),
                          })
            .unwrap();
    }

    pub fn handler_set(&mut self, _db: &Database, _from: NodeId, _msg: MsgSet) {
        unimplemented!()
    }

    pub fn handler_set_remote(&mut self, db: &Database, from: NodeId, msg: MsgRemoteSet) {
        let MsgRemoteSet { key, container, vnode, cookie } = msg;
        let result = self.state.storage_set_remote(db, &key, container);
        db.fabric
            .send_message(from,
                          MsgRemoteSetAck {
                              vnode: vnode,
                              cookie: cookie,
                              result: Ok(result),
                          })
            .unwrap();
    }

    pub fn handler_set_remote_ack(&mut self, db: &Database, _from: NodeId, msg: MsgRemoteSetAck) {
        self.process_set(db, msg.cookie, msg.result.is_ok());
    }

    // BOOTSTRAP
    pub fn handler_bootstrap_start(&mut self, db: &Database, from: NodeId, msg: MsgBootstrapStart) {
        check_status!(self,
                      VNodeStatus::Ready | VNodeStatus::Zombie,
                      db,
                      from,
                      msg,
                      MsgBootstrapFin,
                      migrations);

        let cookie = msg.cookie;
        let mut migration = Migration::Outgoing {
            cookie: cookie,
            clocks_snapshot: self.state.clocks.clone(),
            iterator: self.state.storage.iterator(),
            peer: from,
            count: 0,
        };
        migration.on_start(db, &mut self.state, msg);

        assert!(self.migrations.insert(cookie, migration).is_none());
    }

    pub fn handler_bootstrap_send(&mut self, db: &Database, from: NodeId, msg: MsgBootstrapSend) {
        forward!(self,
                 VNodeStatus::Bootstrap,
                 db,
                 from,
                 msg,
                 MsgBootstrapFin,
                 migrations,
                 on_send);
    }

    pub fn handler_bootstrap_ack(&mut self, db: &Database, from: NodeId, msg: MsgBootstrapAck) {
        forward!(self,
                 VNodeStatus::Ready | VNodeStatus::Zombie,
                 db,
                 from,
                 msg,
                 MsgBootstrapFin,
                 migrations,
                 on_ack);
    }

    pub fn handler_bootstrap_fin(&mut self, db: &Database, from: NodeId, msg: MsgBootstrapFin) {
        forward!(self,
                 VNodeStatus::Ready | VNodeStatus::Bootstrap | VNodeStatus::Zombie,
                 db,
                 from,
                 msg,
                 MsgBootstrapFin,
                 migrations,
                 on_fin);
    }

    pub fn handler_sync_start(&mut self, db: &Database, from: NodeId, msg: MsgSyncStart) {
        check_status!(self,
                      VNodeStatus::Ready | VNodeStatus::Zombie,
                      db,
                      from,
                      msg,
                      MsgSyncFin,
                      syncs);

        let cookie = msg.cookie;
        let target = msg.target;
        let clock_in_peer = msg.clock_in_peer.clone();
        let clock_snapshot = self.state.clocks.get(db.dht.node()).cloned().unwrap_or_default();

        let log_snapshot = if msg.target == db.dht.node() {
            self.state.log.clone()
        } else {
            self.state.peers.get(&msg.target).cloned().unwrap_or_default()
        };
        let iterator: SyncIteratorFn = if log_snapshot.min_version() <= clock_in_peer.base() {
            let mut iter = clock_snapshot.delta(&clock_in_peer)
                .filter_map(move |v| log_snapshot.get(v));
            Box::new(move |storage| {
                iter.by_ref()
                    .filter_map(|k| {
                        storage.get_vec(&k)
                            .map(|v| (k, bincode_serde::deserialize(&v).unwrap()))
                    })
                    .next()
            })
        } else {
            let mut storage_iterator = self.state.storage.iterator();
            let clock_in_peer_base = clock_in_peer.base();
            let mut iter = (0..)
                .map(move |_| {
                    storage_iterator.iter().next().map(|(k, v)| {
                        (k.into(),
                         bincode_serde::deserialize::<DottedCausalContainer<Vec<u8>>>(&v).unwrap())
                    })
                })
                .take_while(|o| o.is_some())
                .map(|o| o.unwrap())
                .filter(move |&(_, ref dcc)| {
                    dcc.versions()
                        .any(|&(node, version)| node == target && version > clock_in_peer_base)
                });
            Box::new(move |_| iter.next())
        };

        let mut sync = Synchronization::Outgoing {
            cookie: cookie,
            iterator: iterator,
            clock_in_peer: clock_in_peer,
            clock_snapshot: clock_snapshot,
            peer: from,
            count: 0,
            target: msg.target,
        };
        sync.on_start(db, &mut self.state, msg);

        assert!(self.syncs.insert(cookie, sync).is_none());
    }

    pub fn handler_sync_send(&mut self, db: &Database, from: NodeId, msg: MsgSyncSend) {
        forward!(self,
                 VNodeStatus::Ready | VNodeStatus::Recover,
                 db,
                 from,
                 msg,
                 MsgSyncFin,
                 syncs,
                 on_send);
    }

    pub fn handler_sync_ack(&mut self, db: &Database, from: NodeId, msg: MsgSyncAck) {
        forward!(self,
                 VNodeStatus::Ready | VNodeStatus::Zombie,
                 db,
                 from,
                 msg,
                 MsgSyncFin,
                 syncs,
                 on_ack);
    }

    pub fn handler_sync_fin(&mut self, db: &Database, from: NodeId, msg: MsgSyncFin) {
        // TODO: need a way to control reverse syncs logic
        // TODO: need a way to promote recover -> ready
        forward!(self,
                 VNodeStatus::Ready | VNodeStatus::Recover | VNodeStatus::Zombie,
                 db,
                 from,
                 msg,
                 MsgSyncFin,
                 syncs,
                 on_fin);
    }

    /// //////
    pub fn start_migration(&mut self, db: &Database) -> bool {
        // TODO: clear storage, update state
        let cookie = self.gen_cookie();
        let mut nodes = db.dht.nodes_for_vnode(self.state.num, false);
        thread_rng().shuffle(&mut nodes);
        for node in nodes {
            if node == db.dht.node() {
                continue;
            }
            self.state.status = VNodeStatus::Bootstrap;
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

            assert!(self.migrations.insert(cookie, migration).is_none());
            return true;
        }
        false
    }

    pub fn start_sync(&mut self, db: &Database, reverse: bool) -> usize {
        let mut result = 0;
        let nodes = db.dht.nodes_for_vnode(self.state.num, false);
        for node in nodes {
            if node == db.dht.node() {
                continue;
            }
            let target = if reverse {
                self.state.status = VNodeStatus::Recover;
                db.dht.node()
            } else {
                node
            };
            let cookie = self.gen_cookie();
            let clock_in_peer = self.state.clocks.get(node).cloned().unwrap_or_default();
            let sync = Synchronization::Incomming {
                peer: node,
                cookie: cookie,
                count: 0,
                target: target,
            };

            db.fabric
                .send_message(node,
                              MsgSyncStart {
                                  cookie: cookie,
                                  vnode: self.state.num,
                                  target: target,
                                  clock_in_peer: clock_in_peer,
                              })
                .unwrap();
            assert!(self.syncs.insert(cookie, sync).is_none());
            result += 1;
        }
        result
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
    fn new_empty(num: u16, db: &Database, status: VNodeStatus) -> Self {
        let storage = db.storage_manager.open(num as i32, true).unwrap();
        db.meta_storage.del(num.to_string().as_bytes());
        storage.clear();

        VNodeState {
            status: status,
            num: num,
            clocks: BitmappedVersionVector::new(),
            peers: Default::default(),
            log: Default::default(),
            storage: storage,
            unflushed_coord_writes: 0,
        }
    }

    fn load(num: u16, db: &Database, mut status: VNodeStatus, is_create: bool) -> Self {
        info!("Loading vnode {} state", num);
        let saved_state_opt = db.meta_storage
            .get(num.to_string().as_bytes(), |bytes| bincode_serde::deserialize(bytes).unwrap());
        if saved_state_opt.is_none() {
            info!("No saved state, falling back to default");
            return Self::new_empty(num,
                                   db,
                                   if status == VNodeStatus::Ready && !is_create {
                                       VNodeStatus::Recover
                                   } else {
                                       status
                                   });
        };
        let SavedVNodeState { mut clocks, mut log, mut peers, clean_shutdown } =
            saved_state_opt.unwrap();

        let storage = match status {
            VNodeStatus::Ready => db.storage_manager.open(num as i32, true).unwrap(),
            VNodeStatus::Absent | VNodeStatus::Bootstrap => {
                // TODO: if possible don't load storage as an optimization
                db.storage_manager.open(num as i32, true).unwrap()
            }
            _ => panic!("Invalid status for load {:?}", status),
        };

        if status == VNodeStatus::Ready && !is_create && !clean_shutdown {
            info!("Unclean shutdown, recovering from the storage");
            status = VNodeStatus::Recover;
            let this_node = db.dht.node();
            let peer_nodes = db.dht.nodes_for_vnode(num, true);
            let mut iter = storage.iterator();
            let mut count = 0;
            for (k, v) in iter.iter() {
                // FIXME: optimize?
                let dcc: DottedCausalContainer<Vec<u8>> = bincode_serde::deserialize(v).unwrap();
                warn!("{:?}", dcc);
                dcc.add_to_bvv(&mut clocks);
                for &(node, version) in dcc.versions() {
                    if node == this_node {
                        log.log(version, k.into());
                    } else if peer_nodes.contains(&node) {
                        peers.entry(node)
                            .or_insert_with(|| Default::default())
                            .log(version, k.into());
                    }
                }
                count += 1;
                if count % 1000 == 0 {
                    debug!("Processed {} keys", count)
                }
            }
            if count % 1000 != 0 {
                debug!("Processed {} keys", count)
            }
        }

        VNodeState {
            num: num,
            status: status,
            clocks: clocks,
            storage: storage,
            unflushed_coord_writes: 0,
            log: log,
            peers: peers,
        }
    }

    fn save(&self, db: &Database, shutdown: bool) {
        let (peers, log) = if shutdown {
            (self.peers.clone(), self.log.clone())
        } else {
            (Default::default(), Default::default())
        };

        let saved_state = SavedVNodeState {
            peers: peers,
            clocks: self.clocks.clone(),
            log: log,
            clean_shutdown: shutdown,
        };
        let serialized_saved_state =
            bincode_serde::serialize(&saved_state, bincode::SizeLimit::Infinite).unwrap();
        db.meta_storage.set(self.num.to_string().as_bytes(), &serialized_saved_state);
    }

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

    pub fn storage_set_local(&mut self, db: &Database, key: &[u8], value_opt: Option<&[u8]>,
                             vv: &VersionVector)
                             -> DottedCausalContainer<Vec<u8>> {
        let mut dcc = self.storage_get(key);
        dcc.discard(vv);
        let node = db.dht.node();
        let dot = self.clocks.event(node);
        if let Some(value) = value_opt {
            dcc.add(node, dot, value.into());
        }
        dcc.strip(&self.clocks);

        if dcc.is_empty() {
            self.storage.del(key);
        } else {
            let bytes = bincode_serde::serialize(&dcc, bincode::SizeLimit::Infinite).unwrap();
            self.storage.set(key, &bytes);
        }

        self.log.log(dot, key.into());

        self.unflushed_coord_writes += 1;
        if self.unflushed_coord_writes >= PEER_LOG_SIZE / 2 {
            self.save(db, false);
            self.unflushed_coord_writes = 0;
        }

        // we striped above so we have to fill again :(
        dcc.fill(&self.clocks);
        dcc
    }

    pub fn storage_set_remote(&mut self, db: &Database, key: &[u8],
                              mut new_dcc: DottedCausalContainer<Vec<u8>>) {
        let old_dcc = self.storage_get(key);
        new_dcc.add_to_bvv(&mut self.clocks);
        new_dcc.sync(old_dcc);
        new_dcc.strip(&self.clocks);

        if new_dcc.is_empty() {
            self.storage.del(key);
        } else {
            let bytes = bincode_serde::serialize(&new_dcc, bincode::SizeLimit::Infinite).unwrap();
            self.storage.set(key, &bytes);
        }

        for &(node, version) in new_dcc.versions() {
            if node == db.dht.node() {
                self.log.log(version, key.into());
            } else {
                self.peers
                    .entry(node)
                    .or_insert_with(|| Default::default())
                    .log(version, key.into());
            }
        }
    }
}

impl Synchronization {
    fn outgoing_send(&mut self, db: &Database, state: &mut VNodeState) -> bool {
        match *self {
            Synchronization::Outgoing { peer,
                                        cookie,
                                        ref mut iterator,
                                        ref mut count,
                                        ref clock_snapshot,
                                        .. } => {
                if let Some((k, dcc)) = iterator(&state.storage) {
                    db.fabric
                        .send_message(peer,
                                      MsgSyncSend {
                                          cookie: cookie,
                                          vnode: state.num,
                                          key: k,
                                          container: dcc,
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

    fn on_start(&mut self, db: &Database, state: &mut VNodeState, _msg: MsgSyncStart) -> bool {
        match *self {
            Synchronization::Outgoing { .. } => self.outgoing_send(db, state),
            _ => unreachable!(),
        }
    }

    fn on_send(&mut self, db: &Database, state: &mut VNodeState, msg: MsgSyncSend) -> bool {
        match *self {
            Synchronization::Incomming { peer, ref mut count, .. } => {
                state.storage_set_remote(db, &msg.key, msg.container);

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
            Synchronization::Incomming { peer, target, .. } => {
                if msg.result.is_ok() {
                    state.clocks.entry_or_default(db.dht.node()).join(msg.result.as_ref().unwrap());
                    state.save(db, false);
                    state.storage.sync();
                    // if reverse update status: recover -> ready
                    if target == db.dht.node() {
                        assert!(state.status == VNodeStatus::Recover);
                        state.status = VNodeStatus::Ready;
                    }
                    // send it back as a form of ack-ack
                    db.fabric.send_message(peer, msg).unwrap();
                } else if target == db.dht.node() {
                    // FIXME: if reverse we need to retry?
                    unimplemented!();
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

    fn on_ack(&mut self, db: &Database, state: &mut VNodeState, _msg: MsgSyncAck) -> bool {
        match *self {
            Synchronization::Outgoing { .. } => self.outgoing_send(db, state),
            _ => unreachable!(),
        }
    }
}

impl Migration {
    fn outgoing_send(&mut self, db: &Database, state: &mut VNodeState) -> bool {
        match *self {
            Migration::Outgoing { peer,
                                  cookie,
                                  ref mut iterator,
                                  ref mut count,
                                  ref clocks_snapshot } => {
                if let Some((k, v)) = iterator.iter().next() {
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
                } else {
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

    fn on_start(&mut self, db: &Database, state: &mut VNodeState, _msg: MsgBootstrapStart) -> bool {
        match *self {
            Migration::Outgoing { .. } => self.outgoing_send(db, state),
            _ => unreachable!(),
        }
    }

    fn on_send(&mut self, db: &Database, state: &mut VNodeState, msg: MsgBootstrapSend) -> bool {
        match *self {
            Migration::Incomming { peer, ref mut count, .. } => {
                state.storage_set_remote(db, &msg.key, msg.container);

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
                    state.save(db, false);
                    state.storage.sync();
                    // now we're ready!
                    state.status = VNodeStatus::Ready;
                    db.dht.promote_pending_node(state.num, db.dht.node()).unwrap();
                    // send it back as a form of ack-ack
                    db.fabric.send_message(peer, msg).unwrap();
                }
            }
            Migration::Outgoing { .. } => (),
        }
        true
    }

    fn on_ack(&mut self, db: &Database, state: &mut VNodeState, _msg: MsgBootstrapAck) -> bool {
        match *self {
            Migration::Outgoing { .. } => self.outgoing_send(db, state),
            _ => unreachable!(),
        }
    }
}
