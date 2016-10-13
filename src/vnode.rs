use std::time::{Instant, Duration};
use std::collections::{BTreeMap, HashSet};
use std::collections::hash_map::Entry as HMEntry;
use version_vector::*;
use storage::*;
use database::*;
use command::CommandError;
use bincode::{self, serde as bincode_serde};
use inflightmap::InFlightMap;
use fabric::*;
use vnode_sync::*;
use rand::{Rng, thread_rng};
use utils::{IdHashMap, IdHasherBuilder};

// FIXME: use a more efficient ring buffer
// 1MB of storage considering key avg length of 32B and 16B overhead
const PEER_LOG_SIZE: usize = 1024 * 1024 / (32 + 16);
const ZOMBIE_TIMEOUT_MS: u64 = 5000;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum VNodeStatus {
    // steady state
    Ready,
    // had data but wasn't shutdown cleanly, so it has to finish reverse sync to all replicas before coordinating writes
    Recover,
    // had no data, now it's streamming data from another node
    Bootstrap,
    // after another node takes over this vnode it stays as zombie until syncs are completed, etc.
    Zombie,
    // no actual data is present (metadata is retained though)
    Absent,
}

pub struct VNode {
    state: VNodeState,
    syncs: IdHashMap<Cookie, Synchronization>,
    inflight: InFlightMap<Cookie, ReqState, Instant, IdHasherBuilder>,
}

pub struct VNodeState {
    num: u16,
    status: VNodeStatus,
    last_status_change: Instant,
    pub clocks: BitmappedVersionVector,
    pub log: VNodePeer,
    pub peers: IdHashMap<NodeId, VNodePeer>,
    pub storage: Storage,
    pub unflushed_coord_writes: usize,
    // sync
    pub sync_nodes: HashSet<NodeId>,
    // rev syncs
    pub pending_recoveries: usize,
}

#[derive(Serialize, Deserialize)]
struct SavedVNodeState {
    peers: IdHashMap<NodeId, VNodePeer>,
    clocks: BitmappedVersionVector,
    log: VNodePeer,
    clean_shutdown: bool,
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct VNodePeer {
    knowledge: Version,
    log: BTreeMap<Version, Vec<u8>>,
}

struct ReqState {
    replies: u8,
    succesfull: u8,
    required: u8,
    total: u8,
    proxied: bool,
    // only used for get
    container: DottedCausalContainer<Vec<u8>>,
    token: Token,
}

impl VNodePeer {
    fn new() -> VNodePeer {
        VNodePeer {
            knowledge: 0,
            log: Default::default(),
        }
    }

    // fn advance_knowledge(&mut self, until: Version) {
    //     debug_assert!(until >= self.knowledge);
    //     self.knowledge = until;
    // }

    pub fn log(&mut self, version: Version, key: Vec<u8>) {
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

    pub fn get(&self, version: Version) -> Option<Vec<u8>> {
        self.log.get(&version).cloned()
    }

    pub fn min_version(&self) -> Version {
        self.log.keys().next().cloned().unwrap_or(0)
    }

    pub fn clear(&mut self) {
        *self = Self::new();
    }
}

macro_rules! assert_any {
    ($value: expr, $($status:pat)|*) => {
        match $value {
            $($status)|* => (),
            _ => panic!("{:?} is not any of {}", $value, stringify!($($status)|*))
        }
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
                fabric_send_error!($db, $from, $msg, $emsg, FabricMsgError::BadVNodeStatus);
                return;
            },
        }
    }
}

macro_rules! forward {
    ($this: expr, $db: expr, $from: expr, $msg: expr, $emsg: ident, $col: ident, $f: ident) => {
        match $this.$col.entry($msg.cookie) {
            HMEntry::Occupied(mut o) => {
                o.get_mut().$f($db, &mut $this.state, $msg);
            },
            _ => {
                fabric_send_error!($db, $from, $msg, $emsg, FabricMsgError::CookieNotFound);
            }
        }
    };
    ($this: expr, $($status:pat)|*, $db: expr, $from: expr, $msg: expr, $emsg: ident, $col: ident, $f: ident) => {
        check_status!($this, $($status)|*, $db, $from, $msg, $emsg, $col);
        forward!($this, $db, $from, $msg, $emsg, $col, $f)
    };
}

impl ReqState {
    fn new(token: Token, nodes: usize) -> Self {
        ReqState {
            required: 1,
            total: nodes as u8,
            replies: 0,
            succesfull: 0,
            proxied: false,
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
            assert_eq!(status, VNodeStatus::Ready, "Status must be ready when creating");
        }

        let mut vnode = VNode {
            state: state,
            inflight: InFlightMap::new(),
            syncs: Default::default(),
        };

        // FIXME: need to handle failure
        match vnode.status() {
            VNodeStatus::Ready | VNodeStatus::Absent => (),
            VNodeStatus::Bootstrap => {
                vnode.start_bootstrap(db);
            }
            VNodeStatus::Recover => {
                vnode.start_rev_sync(db);
            }
            status => panic!("{:?} isn't a valid state after load", status),
        }

        vnode
    }

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

    pub fn syncs_inflight(&self) -> (usize, usize) {
        self.syncs.values().fold((0, 0), |(inc, out), s| {
            match *s {
                Synchronization::BootstrapReceiver { .. } => (inc + 1, out),
                Synchronization::SyncReceiver { .. } => (inc + 1, out),
                Synchronization::BootstrapSender { .. } => (inc, out + 1),
                Synchronization::SyncSender { .. } => (inc, out + 1),
            }
        })
    }

    fn gen_cookie(&mut self) -> Cookie {
        let mut rng = thread_rng();
        Cookie::new(rng.gen(), rng.gen())
    }

    // DHT Changes
    pub fn handler_dht_change(&mut self, db: &Database, x_status: VNodeStatus) {
        match (self.status(), x_status) {
            (VNodeStatus::Ready, VNodeStatus::Absent) |
            (VNodeStatus::Bootstrap, VNodeStatus::Absent) |
            (VNodeStatus::Recover, VNodeStatus::Absent) => {
                {
                    // cancel incomming syncs
                    let state = &mut self.state;
                    let canceled = self.syncs
                        .iter_mut()
                        .filter_map(|(&cookie, m)| {
                            match *m {
                                Synchronization::BootstrapReceiver { .. } |
                                Synchronization::SyncReceiver { .. } => {
                                    m.on_cancel(db, state);
                                    Some(cookie)
                                }
                                _ => None,
                            }
                        })
                        .collect::<Vec<_>>();
                    for cookie in canceled {
                        self.syncs.remove(&cookie).unwrap().on_remove(db, state);
                    }
                }

                if self.status() == VNodeStatus::Recover {
                    self.state.set_status(VNodeStatus::Absent);
                } else {
                    self.state.set_status(VNodeStatus::Zombie);
                }
            }
            (VNodeStatus::Zombie, VNodeStatus::Absent) => (),
            (VNodeStatus::Zombie, VNodeStatus::Ready) => {
                // fast-recomission!
                self.state.set_status(VNodeStatus::Ready);
            }
            (VNodeStatus::Absent, VNodeStatus::Ready) => {
                // recomission by bootstrap
                self.state.set_status(VNodeStatus::Bootstrap);
                self.start_bootstrap(db);
            }
            (VNodeStatus::Bootstrap, VNodeStatus::Ready) |
            (VNodeStatus::Recover, VNodeStatus::Ready) => (),
            (a, b) if a == b => (),
            (a, b) => panic!("Invalid change {:?} -> {:?}", a, b),
        }
    }

    // TICK
    pub fn handler_tick(&mut self, db: &Database, _time: Instant) {
        {
            let terminated = {
                let state = &mut self.state;
                self.syncs
                    .iter_mut()
                    .filter_map(|(&cookie, s)| {
                        match s.on_tick(db, state) {
                            SyncResult::Continue => None,
                            result => Some((cookie, result)),
                        }
                    })
                    .collect::<Vec<_>>()
            };
            for (cookie, result) in terminated {
                match result {
                    SyncResult::RetryBoostrap => {
                        info!("Retrying bootstrap {:?}", cookie);
                        self.start_bootstrap(db);
                    }
                    _ => (),
                }
                self.syncs.remove(&cookie).unwrap().on_remove(db, &mut self.state);
            }
        }

        let now = Instant::now();
        while let Some((cookie, req)) = self.inflight.pop_expired(now) {
            debug!("Request cookie:{:?} token:{} timed out", cookie, req.token);
            db.respond_error(req.token, CommandError::Timeout);
        }

        if self.state.status == VNodeStatus::Zombie && self.syncs.is_empty() &&
           self.inflight.is_empty() &&
           self.state.last_status_change.elapsed() > Duration::from_millis(ZOMBIE_TIMEOUT_MS) {
            self.state.set_status(VNodeStatus::Absent);
        }
    }

    // CLIENT CRUD
    pub fn do_get(&mut self, db: &Database, token: Token, key: &[u8]) {
        let nodes = db.dht.nodes_for_vnode(self.state.num, false);
        let cookie = self.gen_cookie();
        let expire = Instant::now() + Duration::from_millis(1000);
        assert!(self.inflight.insert(cookie, ReqState::new(token, nodes.len()), expire).is_none());

        for node in nodes {
            if node == db.dht.node() {
                let container = self.state.storage_get(key);
                self.process_get(db, cookie, Some(container));
            } else {
                db.fabric
                    .send_msg(node,
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
        let expire = Instant::now() + Duration::from_millis(1000);
        assert!(self.inflight.insert(cookie, ReqState::new(token, nodes.len()), expire).is_none());

        let dcc = self.state
            .storage_set_local(db, key, value_opt, &vv);
        self.process_set(db, cookie, true);
        for node in nodes {
            if node != db.dht.node() {
                db.fabric
                    .send_msg(node,
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
        check_status!(self,
                      VNodeStatus::Ready | VNodeStatus::Zombie,
                      db,
                      from,
                      msg,
                      MsgRemoteGetAck,
                      inflight_get);
        let dcc = self.state.storage_get(&msg.key);
        db.fabric
            .send_msg(from,
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

    pub fn handler_set_ack(&mut self, _db: &Database, _from: NodeId, _msg: MsgSetAck) {
        unimplemented!()
    }

    pub fn handler_set_remote(&mut self, db: &Database, from: NodeId, msg: MsgRemoteSet) {
        check_status!(self, VNodeStatus::Ready, db, from, msg, MsgRemoteSetAck, inflight_set);
        let MsgRemoteSet { key, container, vnode, cookie } = msg;
        let result = self.state.storage_set_remote(db, &key, container);
        db.fabric
            .send_msg(from,
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

    // SYNC
    pub fn handler_sync_start(&mut self, db: &Database, from: NodeId, msg: MsgSyncStart) {
        if !(self.state.status == VNodeStatus::Ready ||
             (self.state.status == VNodeStatus::Zombie &&
              self.state.last_status_change.elapsed() > Duration::from_millis(ZOMBIE_TIMEOUT_MS))) {
            fabric_send_error!(db, from, msg, MsgSyncFin, FabricMsgError::BadVNodeStatus);
        } else if !self.syncs.contains_key(&msg.cookie) {
            let cookie = msg.cookie;
            let sync = match (msg.target.clone(), msg.clock_in_peer.as_ref()) {
                (None, None) => {
                    info!("starting bootstrap sender {:?} peer:{}", cookie, from);
                    Synchronization::new_bootstrap_sender(db, &mut self.state, from, msg)
                }
                (Some(target), Some(_)) => {
                    info!("starting sync sender {:?} target:{:?} peer:{}", cookie, target, from);
                    Synchronization::new_sync_sender(db, &mut self.state, from, msg)
                }
                _ => unreachable!(),
            };
            self.syncs.insert(cookie, sync);
            self.syncs.get_mut(&cookie).unwrap().on_start(db, &mut self.state);
        }
    }

    pub fn handler_sync_send(&mut self, db: &Database, from: NodeId, msg: MsgSyncSend) {
        forward!(self,
                 VNodeStatus::Ready | VNodeStatus::Recover | VNodeStatus::Bootstrap,
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
        check_status!(self,
                      VNodeStatus::Ready | VNodeStatus::Recover | VNodeStatus::Zombie |
                      VNodeStatus::Bootstrap,
                      db,
                      from,
                      msg,
                      MsgSyncFin,
                      syncs);
        let result = if let HMEntry::Occupied(mut o) = self.syncs.entry(msg.cookie) {
            let cookie = msg.cookie;
            let result = o.get_mut().on_fin(db, &mut self.state, msg);
            match result {
                SyncResult::Done |
                SyncResult::RetryBoostrap => {
                    info!("Removing sync/bootstrap {:?}", cookie);
                    o.remove().on_remove(db, &mut self.state);
                }
                SyncResult::Continue => (),
            }
            result
        } else {
            // if msg is success send an error reply
            if msg.result.is_ok() {
                fabric_send_error!(db, from, msg, MsgSyncFin, FabricMsgError::CookieNotFound);
            }
            return;
        };

        match result {
            SyncResult::RetryBoostrap => {
                self.start_bootstrap(db);
            }
            SyncResult::Done | SyncResult::Continue => (),
        }
    }

    /// //////
    pub fn start_bootstrap(&mut self, db: &Database) {
        // TODO: clear storage
        assert_eq!((self.state.status, self.syncs.len()),
                   (VNodeStatus::Bootstrap, 0),
                   "status must be Bootstrap before starting bootstrap");
        let cookie = self.gen_cookie();
        let mut nodes = db.dht.nodes_for_vnode(self.state.num, false);
        thread_rng().shuffle(&mut nodes);
        for node in nodes {
            if node == db.dht.node() {
                continue;
            }
            info!("starting bootstrap receiver {:?} peer:{}", cookie, node);
            let bootstrap =
                Synchronization::new_bootstrap_receiver(db, &mut self.state, node, cookie);
            assert!(self.syncs.insert(cookie, bootstrap).is_none());
            return;
        }
        self.state.set_status(VNodeStatus::Ready);
    }

    pub fn maybe_start_sync(&mut self, db: &Database) -> usize {
        match self.state.status {
            VNodeStatus::Ready | VNodeStatus::Zombie => self.do_start_sync(db, false),
            _ => 0,
        }
    }

    pub fn start_sync(&mut self, db: &Database) -> usize {
        assert_any!(self.state.status, VNodeStatus::Ready | VNodeStatus::Zombie);
        self.do_start_sync(db, false)
    }

    pub fn start_rev_sync(&mut self, db: &Database) {
        assert_eq!((self.state.status, self.state.pending_recoveries),
                   (VNodeStatus::Recover, 0),
                   "Status must be Reverse before starting a reverse sync");
        if self.do_start_sync(db, true) == 0 {
            self.state.set_status(VNodeStatus::Ready);
        }
    }

    fn do_start_sync(&mut self, db: &Database, reverse: bool) -> usize {
        let mut started = 0;
        let nodes = db.dht.nodes_for_vnode(self.state.num, false);
        for node in nodes {
            if node == db.dht.node() || self.state.sync_nodes.contains(&node) {
                continue;
            }

            let cookie = self.gen_cookie();
            let target = if reverse {
                self.state.pending_recoveries += 1;
                db.dht.node()
            } else {
                self.state.sync_nodes.insert(node);
                node
            };
            info!("starting sync receiver {:?} target:{:?} peer:{}", cookie, target, node);
            let sync =
                Synchronization::new_sync_receiver(db, &mut self.state, node, target, cookie);
            assert!(self.syncs.insert(cookie, sync).is_none());
            started += 1;
        }
        started
    }
}

impl Drop for VNode {
    fn drop(&mut self) {
        // clean up any references to the storage
        self.inflight.clear();
        self.syncs.clear();
    }
}

impl VNodeState {
    pub fn num(&self) -> u16 {
        self.num
    }

    pub fn status(&self) -> VNodeStatus {
        self.status
    }

    pub fn set_status(&mut self, new: VNodeStatus) {
        debug!("VNode {} status change {:?} -> {:?}", self.num, self.status, new);
        if new != self.status {
            match new {
                VNodeStatus::Bootstrap => {
                    assert_eq!(self.pending_recoveries, 0);
                    assert_eq!(self.sync_nodes.len(), 0);
                    self.storage.clear();
                }
                VNodeStatus::Ready => {
                    if self.status == VNodeStatus::Recover {
                        assert_eq!(self.pending_recoveries, 0);
                    }
                }
                VNodeStatus::Absent => {
                    assert_eq!(self.pending_recoveries, 0);
                    assert_eq!(self.sync_nodes.len(), 0);
                    self.log.clear();
                    self.peers.clear();
                    self.storage.clear();
                }
                _ => (),
            }

            self.last_status_change = Instant::now();
            self.status = new;
        }
    }

    fn new_empty(num: u16, db: &Database, status: VNodeStatus) -> Self {
        db.meta_storage.del(num.to_string().as_bytes());
        let storage = db.storage_manager.open(num as i32, true).unwrap();
        storage.clear();

        VNodeState {
            num: num,
            status: status,
            last_status_change: Instant::now(),
            clocks: BitmappedVersionVector::new(),
            peers: Default::default(),
            log: Default::default(),
            storage: storage,
            unflushed_coord_writes: 0,
            sync_nodes: Default::default(),
            pending_recoveries: 0,
        }
    }

    fn load(num: u16, db: &Database, mut status: VNodeStatus, is_create: bool) -> Self {
        info!("Loading vnode {} state", num);
        let saved_state_opt = db.meta_storage
            .get(num.to_string().as_bytes(), |bytes| bincode_serde::deserialize(bytes).unwrap());
        if saved_state_opt.is_none() {
            info!("No saved state");
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
                let storage = db.storage_manager.open(num as i32, true).unwrap();
                storage.clear();
                storage
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
                let dcc: DottedCausalContainer<Vec<u8>> = bincode_serde::deserialize(v).unwrap();
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
                    debug!("Recovered {} keys", count)
                }
            }
            debug!("Recovered {} keys in total", count);
        }

        VNodeState {
            num: num,
            status: status,
            last_status_change: Instant::now(),
            clocks: clocks,
            storage: storage,
            log: log,
            peers: peers,
            unflushed_coord_writes: 0,
            sync_nodes: Default::default(),
            pending_recoveries: 0,
        }
    }

    pub fn save(&self, db: &Database, shutdown: bool) {
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

        // FIXME: we striped above so we have to fill again :(
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
