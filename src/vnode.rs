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

// FIXME: use a more efficient log data structure
// 1MB of storage considering key avg length of 32B and 16B overhead
const PEER_LOG_SIZE: usize = 1024 * 1024 / (32 + 16);
const RETIRING_TIMEOUT_MS: u64 = 5000;
const RECOVER_FAST_FORWARD: Version = 100_000;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum VNodeStatus {
    // steady state
    Ready,
    // had data but wasn't shutdown cleanly, so it has to finish reverse sync to all replicas before coordinating writes
    Recover,
    // was absent, now it's streamming data from another node
    Bootstrap,
    // another will take over this vnode it stays in retiring status
    // until syncs are completed, etc.
    Retiring,
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
    unflushed_coord_writes: usize,
    pub clocks: BitmappedVersionVector,
    pub log: VNodePeer,
    pub peers: IdHashMap<NodeId, VNodePeer>,
    pub storage: Storage,
    // state for sync
    pub sync_nodes: HashSet<NodeId, IdHasherBuilder>,
    // state for rev syncs
    pub pending_recoveries: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct SavedVNodeState {
    peers: IdHashMap<NodeId, VNodePeer>,
    clocks: BitmappedVersionVector,
    log: VNodePeer,
    clean_shutdown: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct VNodePeer {
    knowledge: Version,
    log: BTreeMap<Version, Vec<u8>>,
}

struct ReqState {
    replies: u8,
    succesfull: u8,
    required: u8,
    total: u8,
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

    // TODO: gc log
    // fn advance_knowledge(&mut self, until: Version) {
    //     debug_assert!(until >= self.knowledge);
    //     self.knowledge = until;
    // }

    pub fn log(&mut self, version: Version, key: Vec<u8>) {
        let min = self.min_version().unwrap_or(0);
        if version > min {
            if let Some(removed) = self.log.insert(version, key.clone()) {
                debug_assert!(removed == key);
            }
            if self.log.len() > PEER_LOG_SIZE {
                self.log.remove(&min).unwrap();
            }
        }
    }

    pub fn get(&self, version: Version) -> Option<&Vec<u8>> {
        self.log.get(&version)
    }

    pub fn min_version(&self) -> Option<Version> {
        self.log.keys().next().cloned()
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
                let _ = fabric_send_error!($db, $from, $msg, $emsg, FabricMsgError::BadVNodeStatus);
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
                let _ = fabric_send_error!($db, $from, $msg, $emsg, FabricMsgError::CookieNotFound);
            }
        }
    };
    ($this: expr, $($status:pat)|*, $db: expr, $from: expr, $msg: expr, $emsg: ident, $col: ident, $f: ident) => {
        check_status!($this, $($status)|*, $db, $from, $msg, $emsg, $col);
        forward!($this, $db, $from, $msg, $emsg, $col, $f)
    };
}

impl ReqState {
    fn new(token: Token, nodes: usize, consistency: ConsistencyLevel) -> Self {
        ReqState {
            required: consistency.required(nodes as u8),
            total: nodes as u8,
            replies: 0,
            succesfull: 0,
            container: DottedCausalContainer::new(),
            token: token,
        }
    }
}

impl VNode {
    pub fn new(db: &Database, num: u16, status: VNodeStatus) -> VNode {
        let state = VNodeState::load(num, db, status);
        state.save(db, false);

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
        match x_status {
            VNodeStatus::Absent | VNodeStatus::Ready => (),
            status => panic!("Invalid final status {:?}", status),
        }
        match (self.status(), x_status) {
            (a, b) if a == b => (),
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
                    self.state.set_status(db, VNodeStatus::Absent);
                } else {
                    self.state.set_status(db, VNodeStatus::Retiring);
                }
            }
            (VNodeStatus::Retiring, VNodeStatus::Absent) => {
                // do nothing, retiring will timeout and switch to absent eventually
            }
            (VNodeStatus::Retiring, VNodeStatus::Ready) => {
                // fast-recomission!
                self.state.set_status(db, VNodeStatus::Ready);
            }
            (VNodeStatus::Absent, VNodeStatus::Ready) => {
                // recomission by bootstrap
                self.state.set_status(db, VNodeStatus::Bootstrap);
                self.start_bootstrap(db);
            }
            (VNodeStatus::Bootstrap, VNodeStatus::Ready) => {
                self.state.set_status(db, VNodeStatus::Ready);
            }
            (a, b) => panic!("Invalid status change from dht {:?} -> {:?}", a, b),
        }
    }

    // TICK
    pub fn handler_tick(&mut self, db: &Database, _time: Instant) {
        if self.state.status == VNodeStatus::Retiring && self.syncs.is_empty() &&
           self.inflight.is_empty() &&
           self.state.last_status_change.elapsed() > Duration::from_millis(RETIRING_TIMEOUT_MS) {
            match db.dht.retire_retiring_node(db.dht.node(), self.state.num) {
                Ok(_) => self.state.set_status(db, VNodeStatus::Absent),
                Err(e) => {
                    warn!("Can't retire node {} vnode {}: {}", db.dht.node(), self.state.num, e)
                }
            }
        }

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
            self.syncs.remove(&cookie).unwrap().on_remove(db, &mut self.state);
            match result {
                SyncResult::RetryBoostrap => {
                    info!("Retrying bootstrap {:?}", cookie);
                    self.start_bootstrap(db);
                }
                _ => (),
            }
        }

        let now = Instant::now();
        while let Some((cookie, req)) = self.inflight.pop_expired(now) {
            debug!("Request cookie:{:?} token:{} timed out", cookie, req.token);
            db.respond_error(req.token, CommandError::Timeout);
        }
    }

    // CLIENT CRUD
    pub fn do_get(&mut self, db: &Database, token: Token, key: &[u8],
                  consistency: ConsistencyLevel) {
        // TODO: lots of optimizations to be done here
        let nodes = db.dht.nodes_for_vnode(self.state.num, false, true);
        let cookie = self.gen_cookie();
        let expire = Instant::now() + Duration::from_millis(1000);
        assert!(self.inflight
            .insert(cookie, ReqState::new(token, nodes.len(), consistency), expire)
            .is_none());

        for node in nodes {
            if node == db.dht.node() {
                let container = self.state.storage_get(key);
                self.process_get(db, cookie, Some(container));
            } else {
                let _ = db.fabric
                    .send_msg(node,
                              MsgRemoteGet {
                                  cookie: cookie,
                                  vnode: self.state.num,
                                  key: key.into(),
                              });
            }
        }
    }

    fn respond_cant_coordinate(&mut self, db: &Database, token: Token, status: VNodeStatus) {
        let mut nodes = db.dht.write_members_for_vnode(self.state.num());
        thread_rng().shuffle(&mut nodes);
        for (node, (_, addr)) in nodes {
            if node != db.dht.node() {
                match status {
                    VNodeStatus::Absent => {
                        db.respond_move(token, self.state.num, addr);
                    }
                    VNodeStatus::Bootstrap | VNodeStatus::Recover => {
                        db.respond_ask(token, self.state.num, addr);
                    }
                    VNodeStatus::Ready | VNodeStatus::Retiring => unreachable!(),
                }
                return;
            }
        }

        db.respond_error(token, CommandError::Unavailable);
    }

    pub fn do_set(&mut self, db: &Database, token: Token, key: &[u8], value_opt: Option<&[u8]>,
                  vv: VersionVector, consistency: ConsistencyLevel) {
        match self.status() {
            VNodeStatus::Ready | VNodeStatus::Retiring => (),
            status => return self.respond_cant_coordinate(db, token, status),
        }

        let nodes = db.dht.nodes_for_vnode(self.state.num, true, true);
        let cookie = self.gen_cookie();
        let expire = Instant::now() + Duration::from_millis(1000);
        assert!(self.inflight
            .insert(cookie, ReqState::new(token, nodes.len(), consistency), expire)
            .is_none());

        let dcc = self.state.storage_set_local(db, key, value_opt, &vv);
        debug!("set_local {} {:?}", String::from_utf8_lossy(key), dcc);
        self.process_set(db, cookie, true);
        for node in nodes {
            if node != db.dht.node() {
                let _ = db.fabric
                    .send_msg(node,
                              MsgRemoteSet {
                                  cookie: cookie,
                                  vnode: self.state.num,
                                  key: key.into(),
                                  container: dcc.clone(),
                              });
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
                      VNodeStatus::Ready | VNodeStatus::Retiring,
                      db,
                      from,
                      msg,
                      MsgRemoteGetAck,
                      inflight_get);
        let dcc = self.state.storage_get(&msg.key);
        let _ = db.fabric
            .send_msg(from,
                      MsgRemoteGetAck {
                          cookie: msg.cookie,
                          vnode: msg.vnode,
                          result: Ok(dcc),
                      });
    }

    pub fn handler_set(&mut self, _db: &Database, _from: NodeId, _msg: MsgSet) {
        unimplemented!()
    }

    pub fn handler_set_ack(&mut self, _db: &Database, _from: NodeId, _msg: MsgSetAck) {
        unimplemented!()
    }

    pub fn handler_set_remote(&mut self, db: &Database, from: NodeId, msg: MsgRemoteSet) {
        check_status!(self,
                      VNodeStatus::Ready | VNodeStatus::Bootstrap,
                      db,
                      from,
                      msg,
                      MsgRemoteSetAck,
                      inflight_set);
        let MsgRemoteSet { key, container, vnode, cookie } = msg;
        let result = self.state.storage_set_remote(db, &key, container);
        let _ = db.fabric
            .send_msg(from,
                      MsgRemoteSetAck {
                          vnode: vnode,
                          cookie: cookie,
                          result: Ok(result),
                      });
    }

    pub fn handler_set_remote_ack(&mut self, db: &Database, _from: NodeId, msg: MsgRemoteSetAck) {
        self.process_set(db, msg.cookie, msg.result.is_ok());
    }

    // SYNC
    pub fn handler_sync_start(&mut self, db: &Database, from: NodeId, msg: MsgSyncStart) {
        if !(self.state.status == VNodeStatus::Ready ||
             (self.state.status == VNodeStatus::Recover &&
              msg.target.map_or(false, |t| t == from)) ||
             (self.state.status == VNodeStatus::Retiring &&
              self.state.last_status_change.elapsed() <
              Duration::from_millis(RETIRING_TIMEOUT_MS))) {
            let _ = fabric_send_error!(db, from, msg, MsgSyncFin, FabricMsgError::BadVNodeStatus);
        } else if !self.syncs.contains_key(&msg.cookie) {
            let cookie = msg.cookie;
            let sync = match msg.target {
                None => {
                    info!("starting bootstrap sender {:?} peer:{}", cookie, from);
                    Synchronization::new_bootstrap_sender(db, &mut self.state, from, msg)
                }
                Some(target) => {
                    info!("starting sync sender {:?} target:{:?} peer:{}", cookie, target, from);
                    Synchronization::new_sync_sender(db, &mut self.state, from, msg)
                }
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
                 VNodeStatus::Ready | VNodeStatus::Retiring,
                 db,
                 from,
                 msg,
                 MsgSyncFin,
                 syncs,
                 on_ack);
    }

    pub fn handler_sync_fin(&mut self, db: &Database, from: NodeId, msg: MsgSyncFin) {
        check_status!(self,
                      VNodeStatus::Ready | VNodeStatus::Recover | VNodeStatus::Retiring |
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
            // only send error if Ok, otherwise the message will be sent back and forth forever
            if msg.result.is_ok() {
                let _ =
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
        assert_eq!((self.state.status, self.syncs.len()),
                   (VNodeStatus::Bootstrap, 0),
                   "status must be Bootstrap before starting bootstrap");
        self.state.storage.clear();
        let cookie = self.gen_cookie();
        let mut nodes = db.dht.nodes_for_vnode(self.state.num, false, true);
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
        // nothing to boostrap
        self.state.set_status(db, VNodeStatus::Ready);
    }

    pub fn maybe_start_sync(&mut self, db: &Database) -> usize {
        match self.state.status {
            VNodeStatus::Ready | VNodeStatus::Retiring => self.do_start_sync(db, false),
            _ => 0,
        }
    }

    pub fn start_sync(&mut self, db: &Database) -> usize {
        assert_any!(self.state.status, VNodeStatus::Ready | VNodeStatus::Retiring);
        self.do_start_sync(db, false)
    }

    pub fn start_rev_sync(&mut self, db: &Database) {
        assert_eq!((self.state.status, self.state.pending_recoveries),
                   (VNodeStatus::Recover, 0),
                   "Status must be Reverse before starting a reverse sync");
        if self.do_start_sync(db, true) == 0 {
            // nothing to sync from
            self.state.set_status(db, VNodeStatus::Ready);
        }
    }

    fn do_start_sync(&mut self, db: &Database, reverse: bool) -> usize {
        let mut started = 0;
        let nodes = db.dht.nodes_for_vnode(self.state.num, false, true);
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

    pub fn set_status(&mut self, db: &Database, new: VNodeStatus) {
        info!("VNode {} status change {:?} -> {:?}", self.num, self.status, new);
        if new != self.status {
            match new {
                VNodeStatus::Bootstrap => {
                    assert_eq!(self.pending_recoveries, 0);
                    assert_eq!(self.sync_nodes.len(), 0);
                    self.storage.clear();
                }
                VNodeStatus::Ready => {
                    assert_eq!(self.pending_recoveries, 0);
                    if self.status == VNodeStatus::Recover {
                        assert_eq!(self.sync_nodes.len(), 0);
                        self.clocks.fast_foward(db.dht.node(), RECOVER_FAST_FORWARD);
                    }
                }
                VNodeStatus::Absent => {
                    assert_eq!(self.pending_recoveries, 0);
                    assert_eq!(self.sync_nodes.len(), 0);
                    self.log.clear();
                    self.peers.clear();
                    self.storage.clear();
                }
                VNodeStatus::Retiring => {
                    assert_eq!(self.pending_recoveries, 0);
                }
                VNodeStatus::Recover => {
                    panic!("Can only set status to Recover on load");
                }
            }

            self.last_status_change = Instant::now();
            self.status = new;
            // not important in all cases but nice to do
            self.save(db, false);
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

    fn load(num: u16, db: &Database, mut status: VNodeStatus) -> Self {
        info!("Loading vnode {} state", num);
        let saved_state_opt = db.meta_storage
            .get(num.to_string().as_bytes(), |bytes| bincode_serde::deserialize(bytes).unwrap());
        if saved_state_opt.is_none() {
            info!("No saved state");
            return Self::new_empty(num,
                                   db,
                                   if status == VNodeStatus::Ready {
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

        if status == VNodeStatus::Ready && !clean_shutdown {
            info!("Unclean shutdown, recovering from the storage");
            status = VNodeStatus::Recover;
            let this_node = db.dht.node();
            let peer_nodes = db.dht.nodes_for_vnode(num, true, true);
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
                            .or_insert_with(Default::default)
                            .log(version, k.into());
                    }
                }
                count += 1;
                if count % 1000 == 0 {
                    debug!("Recovered {} keys", count);
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
        debug!("Saving state for vnode {:?} {:?}", self.num, saved_state);
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
        let dot = self.clocks.event(db.dht.node());
        if let Some(value) = value_opt {
            dcc.add(db.dht.node(), dot, value.into());
        }
        dcc.strip(&self.clocks);

        if dcc.is_dcc_empty() {
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

        if new_dcc.is_dcc_empty() {
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
                    .or_insert_with(Default::default)
                    .log(version, key.into());
            }
        }
    }
}
