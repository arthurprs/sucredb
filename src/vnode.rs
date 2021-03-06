use bincode;
use bytes::Bytes;
use command::CommandError;
use cubes::*;
use database::*;
use fabric::*;
use hash::hash_slot;
use inflightmap::InFlightMap;
use rand::{thread_rng, Rng};
use std::collections::hash_map::Entry as HMEntry;
use std::time::{Duration, Instant};
use storage::*;
use utils::{join_u64, split_u64};
use utils::{replace_default, IdHashMap, IdHashSet, IdHasherBuilder};
use version_vector::*;
use vnode_sync::*;

const ZOMBIE_TIMEOUT_MS: u64 = 60 * 1_000;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum VNodeStatus {
    /* TODO: consider adding an status for a node that just came back up and
        is still part of the cluster, so it potentially has highly stale data */
    // steady state
    Ready,
    // streaming data from another node, can only accept replicated writes in this state
    Bootstrap,
    // another node took over this vnode, this will stay in zombie until it times out
    // and syncs are completed, etc.
    Zombie,
    // no actual data is present
    Absent,
}

pub struct VNode {
    state: VNodeState,
    syncs: IdHashMap<Cookie, Synchronization>,
    requests: InFlightMap<Cookie, ReqState, Instant, IdHasherBuilder>,
}

// This VNodeState id is based on the upper half of the actual node id
// but the lower half is randomized on every bootstrap though.
// If this is not done the vnode may reuse dots (Id, Version) pairs
// after a round of boostraps:
// 1. SET (id1, v1)
// 2. Absent...
// 3. Boostrap...
// 4. SET (id1, v1) <---- reused (id1, v1)
//
// log_clocks is always a subset of the clocks.
// clocks and log_clocks differ in the sense that only dots in
// log_clocks are trusted to be in the dot-key log.
pub struct VNodeState {
    id: NodeId,
    num: u16,
    status: VNodeStatus,
    last_status_change: Instant,
    pub clocks: BitmappedVersionVector,
    pub log_clocks: BitmappedVersionVector,
    pub storage: Storage,
    // state for syncs
    pub pending_bootstrap: bool,
    pub sync_nodes: IdHashSet<NodeId>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SavedVNodeState {
    id: NodeId,
    clocks: BitmappedVersionVector,
    log_clocks: BitmappedVersionVector,
    clean_shutdown: bool,
}

struct ReqState {
    replies: u8,
    succesfull: u8,
    required: u8,
    total: u8,
    context: Context,
}

#[cfg(test)]
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
                let _ = fabric_send_error!($db, $from, $msg, $emsg, FabricError::BadVNodeStatus);
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
                let _ = fabric_send_error!($db, $from, $msg, $emsg, FabricError::CookieNotFound);
            }
        }
    };
    ($this: expr, $($status:pat)|*, $db: expr, $from: expr, $msg: expr, $emsg: ident, $col: ident, $f: ident) => {
        check_status!($this, $($status)|*, $db, $from, $msg, $emsg, $col);
        forward!($this, $db, $from, $msg, $emsg, $col, $f)
    };
}

impl ReqState {
    fn new(context: Context, nodes: usize, consistency: ConsistencyLevel) -> Self {
        ReqState {
            required: consistency.required(nodes as u8),
            total: nodes as u8,
            replies: 0,
            succesfull: 0,
            context,
        }
    }

    fn done(&self) -> bool {
        self.satisfied() || self.replies == self.total
    }

    fn satisfied(&self) -> bool {
        self.succesfull >= self.required
    }
}

impl VNode {
    pub fn new(db: &Database, num: u16, status: VNodeStatus) -> VNode {
        let state = VNodeState::load(num, db, status);
        state.save(db, false);

        let mut vnode = VNode {
            state: state,
            requests: InFlightMap::new(),
            syncs: Default::default(),
        };

        match vnode.status() {
            VNodeStatus::Ready | VNodeStatus::Absent => (),
            VNodeStatus::Bootstrap => {
                // mark pending if it doesn't start
                vnode.start_bootstrap(db);
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
    pub fn _dump_log(&self) -> Vec<((u64, u64), Vec<u8>)> {
        self.state
            .storage
            .log_iterator_all()
            .iter()
            .map(|(dot, k)| (dot, Vec::from(k)))
            .collect()
    }

    pub fn syncs_inflight(&self) -> (usize, usize) {
        let pend = if self.state.pending_bootstrap { 1 } else { 0 };
        self.syncs
            .values()
            .fold((pend, 0), |(inc, out), s| match *s {
                Synchronization::BootstrapReceiver { .. }
                | Synchronization::SyncReceiver { .. } => (inc + 1, out),
                Synchronization::BootstrapSender { .. } | Synchronization::SyncSender { .. } => {
                    (inc, out + 1)
                }
            })
    }

    fn gen_cookie(&self) -> Cookie {
        let mut rng = thread_rng();
        Cookie::new(rng.gen(), rng.gen())
    }

    // DHT Changes
    pub fn handler_dht_change(&mut self, db: &Database, x_status: VNodeStatus) {
        match x_status {
            VNodeStatus::Absent | VNodeStatus::Ready => (),
            status => panic!("Invalid final status {:?}", status),
        }
        let status = self.status();
        match (status, x_status) {
            (VNodeStatus::Ready, VNodeStatus::Absent)
            | (VNodeStatus::Bootstrap, VNodeStatus::Absent) => {
                {
                    // cancel incomming syncs
                    let state = &mut self.state;
                    let canceled = self
                        .syncs
                        .iter_mut()
                        .filter_map(|(&cookie, m)| {
                            if let SyncDirection::Incomming = m.direction() {
                                m.on_cancel(db, state);
                                Some(cookie)
                            } else {
                                None
                            }
                        }).collect::<Vec<_>>();
                    for cookie in canceled {
                        self.syncs.remove(&cookie).unwrap().on_remove(db, state);
                    }
                }

                // vnode goes into zombie unless it was bootstraping
                let new_status = if status == VNodeStatus::Bootstrap {
                    VNodeStatus::Absent
                } else {
                    VNodeStatus::Zombie
                };

                self.state.set_status(db, new_status);
            }
            (VNodeStatus::Zombie, VNodeStatus::Absent) => {
                // do nothing, zombie will timeout and switch to absent eventually
            }
            (VNodeStatus::Zombie, VNodeStatus::Ready) => {
                // fast-recomission!
                self.state.set_status(db, VNodeStatus::Ready);
            }
            (VNodeStatus::Absent, VNodeStatus::Ready) => {
                // recomission by bootstrap
                self.state.set_status(db, VNodeStatus::Bootstrap);
                self.start_bootstrap(db);
            }
            (VNodeStatus::Bootstrap, VNodeStatus::Ready) => {
                // check if there's a pending bootstrap we need to start
                if self.state.pending_bootstrap {
                    self.start_bootstrap(db);
                }
            }
            (a, b) if a == b => (), // nothing to do
            (a, b) => panic!("Invalid status change from dht {:?} -> {:?}", a, b),
        }
    }

    // TICK
    pub fn handler_tick(&mut self, db: &Database, _time: Instant) {
        let terminated_syncs = {
            let state = &mut self.state;
            self.syncs
                .iter_mut()
                .filter_map(|(&cookie, s)| match s.on_tick(db, state) {
                    SyncResult::Continue => None,
                    result => Some((cookie, result)),
                }).collect::<Vec<_>>()
        };
        for (cookie, result) in terminated_syncs {
            self.syncs
                .remove(&cookie)
                .unwrap()
                .on_remove(db, &mut self.state);
            if self.status() == VNodeStatus::Bootstrap {
                self.handle_bootstrap_result(db, result);
            }
        }

        let now = Instant::now();
        while let Some((cookie, mut req)) = self.requests.pop_expired(now) {
            debug!(
                "Request cookie:{:?} token:{} timed out",
                cookie, req.context.token
            );
            req.context.clear();
            db.respond_error(&mut req.context, CommandError::Timeout);
        }

        if self.state.pending_bootstrap {
            // check if there's a pending bootstrap we need to start
            self.start_bootstrap(db);
        } else if self.status() == VNodeStatus::Zombie
            && self.requests.is_empty()
            && self.syncs.is_empty()
            && self.state.last_status_change.elapsed() > Duration::from_millis(ZOMBIE_TIMEOUT_MS)
        {
            // go absent when zombie timeout
            self.state.set_status(db, VNodeStatus::Absent);
        }
    }

    // CLIENT CRUD
    pub fn do_get(
        &mut self,
        db: &Database,
        context: &mut Context,
        keys: &[&Bytes],
        consistency: ConsistencyLevel,
        response_fn: ResponseFn,
    ) -> Result<(), CommandError> {
        debug!(
            "vnode:{:?} do_get ({:?}) {:?}",
            self.state.num(),
            context.token,
            consistency
        );
        let nodes = db.dht.nodes_for_vnode(self.state.num, false, true);
        if nodes.is_empty() {
            debug!("vnode:{:?} no nodes", self.state.num());
            return Err(CommandError::Unavailable);
        }
        let participate = nodes.contains(&db.dht.node());
        let cookie = self.gen_cookie();
        let expire = Instant::now() + Duration::from_millis(db.config.request_timeout as _);

        let mut response_fn = Some(response_fn);
        for key in keys {
            let value = if participate {
                self.state.storage_get(key).unwrap()
            } else {
                Default::default()
            };
            context.reads.push(ContextRead {
                cube: value,
                response: response_fn.take(),
            });
        }

        let req = ReqState::new(replace_default(context), nodes.len(), consistency);
        self.requests.insert(cookie, req, expire);

        if participate {
            // register the results added above
            if self.process_get::<Option<_>>(db, cookie, Ok(None)) {
                return Ok(());
            }
        }

        let msg = MsgRemoteGet {
            cookie: cookie,
            vnode: self.state.num,
            keys: keys.iter().map(|&x| x.clone()).collect(),
        };
        for node in nodes {
            if node != db.dht.node() {
                if let Err(err) = db.fabric.send_msg(node, &msg) {
                    if self.process_get::<Option<_>>(db, cookie, Err(err)) {
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }

    fn respond_cant_coordinate(
        &mut self,
        db: &Database,
        context: &mut Context,
        status: VNodeStatus,
    ) {
        let hash_slot = hash_slot(&context.writes[0].key);
        let mut nodes = db.dht.nodes_for_vnode_ex(self.state.num(), true, false);
        thread_rng().shuffle(&mut nodes);
        for (node, (_, addr)) in nodes {
            if node != db.dht.node() {
                match status {
                    VNodeStatus::Absent | VNodeStatus::Zombie => {
                        return db.respond_moved(context, hash_slot, addr);
                    }
                    VNodeStatus::Bootstrap => {
                        return db.respond_ask(context, hash_slot, addr);
                    }
                    VNodeStatus::Ready => unreachable!(),
                }
            }
        }

        db.respond_error(context, CommandError::Unavailable);
    }

    pub fn do_flush(
        &mut self,
        db: &Database,
        context: &mut Context,
        consistency: ConsistencyLevel,
    ) -> Result<(), CommandError> {
        match self.status() {
            VNodeStatus::Ready => (),
            status => return Ok(self.respond_cant_coordinate(db, context, status)),
        }

        let mut error = None;
        for write in &mut context.writes {
            let old_cube = match self
                .state
                .storage_get(&write.key)
                .map_err(|_| CommandError::StorageError)
            {
                Ok(old_cube) => old_cube,
                Err(e) => {
                    error = Some(e);
                    break;
                }
            };

            write.version = self.state.clocks.event(self.state.id);
            self.state.log_clocks.add(self.state.id, write.version);
            let mutator = write.mutator_fn.take().expect("No MutatorFn");
            match mutator(self.state.id, write.version, old_cube) {
                Ok((cube, opt_resp)) => {
                    write.cube = cube;
                    write.response = opt_resp;
                }
                Err(e) => {
                    error = Some(e);
                    break;
                }
            };
        }

        if let Some(e) = error {
            return Err(e);
        }

        let cookie = self.gen_cookie();
        let expire = Instant::now() + Duration::from_millis(db.config.request_timeout as _);
        let nodes = db.dht.nodes_for_vnode(self.state.num, true, true);

        match self.state.storage_set_local(
            db,
            context
                .writes
                .iter()
                .map(|w| (w.version, &w.key[..], &w.cube)),
        ) {
            Ok(()) => (),
            Err(e) => return Err(e),
        };

        // The code bellow is carefully ordered to move Cubes around without cloning

        // 1. move the cubes to the msg
        let msg = MsgRemoteSet {
            cookie: cookie,
            vnode: self.state.num,
            writes: context
                .writes
                .iter_mut()
                .map(|w| (w.key.clone(), replace_default(&mut w.cube), w.reply_result))
                .collect(),
            reply: consistency != ConsistencyLevel::One,
        };

        // 2. create reqstate, note that writes have have nil cubes at this point
        let req = ReqState::new(replace_default(context), nodes.len(), consistency);
        self.requests.insert(cookie, req, expire);

        // 3. send the msgs
        for &node in &nodes {
            if node != db.dht.node() {
                if let Err(err) = db.fabric.send_msg(node, &msg) {
                    if self.process_set::<Option<_>>(db, cookie, Err(err)) {
                        return Ok(());
                    }
                }
            }
        }

        // 4. get back the cubes from msg and process_set
        self.process_set(db, cookie, Ok(msg.writes.into_iter().map(|w| Some(w.1))));

        Ok(())
    }

    // OTHER
    fn process_get<I: IntoIterator<Item = Cube>>(
        &mut self,
        db: &Database,
        cookie: Cookie,
        response: Result<I, FabricError>,
    ) -> bool {
        if let HMEntry::Occupied(mut o) = self.requests.entry(cookie) {
            debug!("process_get {:?}", cookie);
            let done = {
                let state = o.get_mut();
                state.replies += 1;
                if let Ok(response) = response {
                    state.succesfull += 1;
                    for (response, read) in response.into_iter().zip(&mut state.context.reads) {
                        let cube = replace_default(&mut read.cube);
                        read.cube = cube.merge(response);
                    }
                }
                state.done()
            };
            if done {
                let mut state = o.remove();
                if !state.satisfied() {
                    debug!("get {:?} done but not satisfied", cookie);
                    state.context.clear();
                    db.respond_error(&mut state.context, CommandError::Unavailable);
                } else {
                    let ReqState { mut context, .. } = state;
                    let mut render_fn = None;
                    context.response.extend(context.reads.drain(..).map(|r| {
                        if render_fn.is_none() {
                            render_fn = r.response;
                        }
                        render_fn.as_mut().expect("No ResponseFn")(r.cube)
                    }));
                    db.respond(&mut context);
                }
            }
            done
        } else {
            debug!("process_get cookie not found {:?}", cookie);
            true
        }
    }

    fn process_set<I: IntoIterator<Item = Option<Cube>>>(
        &mut self,
        db: &Database,
        cookie: Cookie,
        response: Result<I, FabricError>,
    ) -> bool {
        if let HMEntry::Occupied(mut o) = self.requests.entry(cookie) {
            debug!("process_set {:?}", cookie);
            let done = {
                let state = o.get_mut();
                state.replies += 1;
                if let Ok(response) = response {
                    state.succesfull += 1;
                    for (response, write) in response.into_iter().zip(&mut state.context.writes) {
                        if let Some(response) = response {
                            let cube = replace_default(&mut write.cube);
                            write.cube = cube.merge(response);
                        }
                    }
                }
                state.done()
            };
            if done {
                let mut state = o.remove();
                if !state.satisfied() {
                    debug!("set {:?} done but not satisfied", cookie);
                    state.context.clear();
                    db.respond_error(&mut state.context, CommandError::Unavailable);
                } else {
                    let ReqState { mut context, .. } = state;
                    context.response.extend(context.writes.drain(..).map(|w| {
                        let ContextWrite {
                            response,
                            response_fn,
                            cube,
                            ..
                        } = w;
                        response.unwrap_or_else(|| response_fn.expect("No ResponseFn")(cube))
                    }));
                    db.respond(&mut context);
                }
            }
            done
        } else {
            debug!("process_set cookie not found {:?}", cookie);
            true
        }
    }

    // CRUD HANDLERS
    pub fn handler_get_remote_ack(&mut self, db: &Database, _from: NodeId, msg: MsgRemoteGetAck) {
        self.process_get(db, msg.cookie, msg.result);
    }

    pub fn handler_get_remote(&mut self, db: &Database, from: NodeId, msg: MsgRemoteGet) {
        // accept zombie to reduce chance of timeouts due to races on cluster change
        check_status!(
            self,
            VNodeStatus::Ready | VNodeStatus::Zombie,
            db,
            from,
            msg,
            MsgRemoteGetAck,
            inflight_get
        );
        let mut result = Vec::with_capacity(msg.keys.len());
        for key in &msg.keys {
            let value = self
                .state
                .storage_get(&key)
                .map_err(|_| FabricError::StorageError)
                .unwrap();
            result.push(value);
        }
        let _ = db.fabric.send_msg(
            from,
            &MsgRemoteGetAck {
                cookie: msg.cookie,
                vnode: msg.vnode,
                result: Ok(result),
            },
        );
    }

    pub fn handler_set_remote(&mut self, db: &Database, from: NodeId, msg: MsgRemoteSet) {
        check_status!(
            self,
            VNodeStatus::Ready | VNodeStatus::Bootstrap,
            db,
            from,
            msg,
            MsgRemoteSetAck,
            inflight_set
        );
        let MsgRemoteSet {
            writes,
            vnode,
            cookie,
            reply,
        } = msg;
        // Is this really ok?
        // This optimization prevents a class of errors (storage errrors..)
        // from propagating to the coordinator
        // if reply && !reply_result {
        //     let _ = db.fabric.send_msg(
        //         from,
        //         &MsgRemoteSetAck {
        //             vnode: vnode,
        //             cookie: cookie,
        //             result: Ok(None),
        //         },
        //     );
        // }
        let result = self
            .state
            .storage_set_remote(db, writes)
            .map_err(|_| FabricError::StorageError);
        if
        /*reply_result && */
        reply {
            let _ = db.fabric.send_msg(
                from,
                &MsgRemoteSetAck {
                    vnode: vnode,
                    cookie: cookie,
                    result: result,
                },
            );
        }
    }

    pub fn handler_set_remote_ack(&mut self, db: &Database, _from: NodeId, msg: MsgRemoteSetAck) {
        self.process_set(db, msg.cookie, msg.result);
    }

    // SYNC
    pub fn handler_sync_start(&mut self, db: &Database, from: NodeId, msg: MsgSyncStart) {
        if !(self.state.status == VNodeStatus::Ready
            || (self.state.status == VNodeStatus::Zombie
                && self.state.last_status_change.elapsed()
                    < Duration::from_millis(ZOMBIE_TIMEOUT_MS)))
        {
            debug!("Can't start sync when {:?}", self.state.status);
            let _ = fabric_send_error!(db, from, msg, MsgSyncFin, FabricError::BadVNodeStatus);
        } else if !self.syncs.contains_key(&msg.cookie) {
            if !db.signal_sync_start(SyncDirection::Outgoing) {
                debug!("Refusing remote sync request, limit exceeded");
                let _ = fabric_send_error!(db, from, msg, MsgSyncFin, FabricError::NotReady);
                return;
            }

            let cookie = msg.cookie;
            let sync = match msg.target {
                None => {
                    info!("Starting bootstrap sender {:?} peer:{}", cookie, from);
                    Synchronization::new_bootstrap_sender(db, &mut self.state, from, msg)
                }
                Some(target) => {
                    assert_eq!(target, db.dht.node());
                    info!("Starting sync sender {:?} peer:{}", cookie, from);
                    Synchronization::new_sync_sender(db, &mut self.state, from, msg)
                }
            };
            match self.syncs.entry(cookie) {
                HMEntry::Vacant(v) => {
                    v.insert(sync).on_start(db, &mut self.state);
                }
                HMEntry::Occupied(_) => unreachable!(),
            }
        }
    }

    pub fn handler_sync_send(&mut self, db: &Database, from: NodeId, msg: MsgSyncSend) {
        forward!(
            self,
            VNodeStatus::Ready | VNodeStatus::Bootstrap,
            db,
            from,
            msg,
            MsgSyncFin,
            syncs,
            on_msg_send
        );
    }

    pub fn handler_sync_ack(&mut self, db: &Database, from: NodeId, msg: MsgSyncAck) {
        forward!(
            self,
            VNodeStatus::Ready | VNodeStatus::Zombie,
            db,
            from,
            msg,
            MsgSyncFin,
            syncs,
            on_msg_ack
        );
    }

    pub fn handler_sync_fin(&mut self, db: &Database, from: NodeId, msg: MsgSyncFin) {
        check_status!(
            self,
            VNodeStatus::Ready | VNodeStatus::Zombie | VNodeStatus::Bootstrap,
            db,
            from,
            msg,
            MsgSyncFin,
            syncs
        );
        let cookie = msg.cookie;
        let result = if let HMEntry::Occupied(mut o) = self.syncs.entry(cookie) {
            let result = o.get_mut().on_msg_fin(db, &mut self.state, msg);
            match result {
                SyncResult::Done | SyncResult::Error => {
                    info!("Removing sync/bootstrap {:?}", cookie);
                    o.remove().on_remove(db, &mut self.state);
                }
                SyncResult::Continue => (),
            }
            trace!("handler_sync_fin {:?}: {:?}", cookie, result);
            result
        } else {
            trace!("Can't find cookie {:?} for msg sync fin", cookie);
            // only send error if Ok, otherwise the message will be sent back and forth forever
            if msg.result.is_ok() {
                let _ = fabric_send_error!(db, from, msg, MsgSyncFin, FabricError::CookieNotFound);
            }
            return;
        };

        if self.status() == VNodeStatus::Bootstrap {
            self.handle_bootstrap_result(db, result);
        }
    }

    fn handle_bootstrap_result(&mut self, db: &Database, result: SyncResult) {
        match result {
            SyncResult::Error => {
                info!("Retrying bootstrap");
                self.start_bootstrap(db);
            }
            SyncResult::Done => {
                match db.dht.promote_pending_node(db.dht.node(), self.state.num()) {
                    Ok(_) => {
                        // now we're ready!
                        self.state.set_status(db, VNodeStatus::Ready);
                    }
                    Err(e) => {
                        // it's not clear what happened
                        // go absent and wait for a dht callback to fix it
                        self.state.set_status(db, VNodeStatus::Absent);
                        warn!(
                            "Can't promote node {} vnode {}: {}",
                            db.dht.node(),
                            self.state.num(),
                            e
                        );
                    }
                }
            }
            SyncResult::Continue => (),
        }
    }

    fn start_bootstrap(&mut self, db: &Database) {
        debug!(
            "start_bootstrap vn:{} p:{:?}",
            self.state.num, self.state.pending_bootstrap
        );
        assert_eq!(self.state.status, VNodeStatus::Bootstrap);
        assert_eq!(self.syncs.len(), 0);
        self.state.pending_bootstrap = false;
        let cookie = self.gen_cookie();
        let mut nodes = db.dht.nodes_for_vnode(self.state.num, false, true);
        let connected_nodes = db.fabric.connections();
        nodes.retain(|x| connected_nodes.contains(x));
        if nodes.is_empty() || nodes == &[db.dht.node()] {
            // nothing to boostrap from
            self.handle_bootstrap_result(db, SyncResult::Done);
            return;
        }

        thread_rng().shuffle(&mut nodes);
        for node in nodes {
            if node == db.dht.node() {
                continue;
            }
            if !db.signal_sync_start(SyncDirection::Incomming) {
                debug!("Bootstrap not allowed to start, go pending");
                self.state.pending_bootstrap = true;
                return;
            }
            info!("starting bootstrap receiver {:?} peer:{}", cookie, node);
            let sync = Synchronization::new_bootstrap_receiver(db, &mut self.state, node, cookie);
            match self.syncs.entry(cookie) {
                HMEntry::Vacant(v) => {
                    v.insert(sync).on_start(db, &mut self.state);
                }
                HMEntry::Occupied(_) => unreachable!(),
            }
            return;
        }
        unreachable!();
    }

    pub fn start_sync_if_ready(&mut self, db: &Database) -> bool {
        match self.state.status {
            VNodeStatus::Ready => self.do_start_sync(db),
            _ => false,
        }
    }

    #[cfg(test)]
    pub fn _start_sync(&mut self, db: &Database) -> bool {
        assert_any!(self.state.status, VNodeStatus::Ready);
        self.do_start_sync(db)
    }

    fn do_start_sync(&mut self, db: &Database) -> bool {
        trace!("do_start_sync vn:{}", self.state.num);
        let mut nodes = db.dht.nodes_for_vnode(self.state.num, false, true);
        let connected_nodes = db.fabric.connections();
        nodes.retain(|x| connected_nodes.contains(x));
        thread_rng().shuffle(&mut nodes);
        for node in nodes {
            if node == db.dht.node() || self.state.sync_nodes.contains(&node) {
                continue;
            }
            if !db.signal_sync_start(SyncDirection::Incomming) {
                debug!("Refusing start sync, limit exceeded");
                continue;
            }

            let cookie = self.gen_cookie();
            info!("Starting sync receiver {:?} peer:{}", cookie, node);
            let sync = Synchronization::new_sync_receiver(db, &mut self.state, node, cookie);
            match self.syncs.entry(cookie) {
                HMEntry::Vacant(v) => {
                    v.insert(sync).on_start(db, &mut self.state);
                }
                HMEntry::Occupied(_) => unreachable!(),
            }
            return true;
        }
        false
    }
}

impl Drop for VNode {
    fn drop(&mut self) {
        info!("Droping vnode {:?}", self.state.num);
        // clean up any references to the storage
        self.requests.clear();
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

    pub fn clear(&mut self) {
        self.clocks.clear();
        self.storage.clear();
    }

    fn generate_id(base: NodeId) -> NodeId {
        join_u64(split_u64(base).0, thread_rng().gen())
    }

    pub fn set_status(&mut self, db: &Database, new: VNodeStatus) {
        if new == self.status {
            return;
        }
        info!(
            "VNode {} status change {:?} -> {:?}",
            self.num, self.status, new
        );
        match new {
            VNodeStatus::Bootstrap => {
                assert!(!self.pending_bootstrap);
                assert_eq!(self.sync_nodes.len(), 0);
                self.clear();
                self.id = Self::generate_id(self.id);
            }
            VNodeStatus::Absent => {
                assert_eq!(self.sync_nodes.len(), 0);
                self.clear();
            }
            VNodeStatus::Ready | VNodeStatus::Zombie => {}
        }

        self.last_status_change = Instant::now();
        self.pending_bootstrap = false;
        self.status = new;
        // not important in all cases but nice to do
        self.save(db, false);
    }

    fn new_empty(num: u16, db: &Database, status: VNodeStatus) -> Self {
        db.meta_storage
            .del(num.to_string().as_bytes())
            .expect("Can't del vnode state");
        let storage = db.storage_manager.open(num).expect("Can't open storage");
        storage.clear();

        VNodeState {
            id: Self::generate_id(db.dht.node()),
            num: num,
            status: status,
            last_status_change: Instant::now(),
            clocks: Default::default(),
            log_clocks: Default::default(),
            storage: storage,
            pending_bootstrap: false,
            sync_nodes: Default::default(),
        }
    }

    fn load(num: u16, db: &Database, status: VNodeStatus) -> Self {
        info!("Loading vnode {} state", num);
        let saved_state_opt = db
            .meta_storage
            .get(num.to_string().as_bytes(), |bytes| {
                bincode::deserialize(bytes).expect("Can't deserialize vnode state")
            }).expect("Can't read saved vnode state");

        if status == VNodeStatus::Absent || saved_state_opt.is_none() {
            info!("No saved state");
            return Self::new_empty(num, db, status);
        };

        assert_eq!(status, VNodeStatus::Ready);
        let SavedVNodeState {
            mut id,
            clocks,
            log_clocks,
            clean_shutdown,
        } = saved_state_opt.unwrap();

        let storage = db.storage_manager.open(num).expect("Can't open storage");

        if !clean_shutdown {
            id = Self::generate_id(id);
        }

        let mut state = VNodeState {
            id: id,
            num: num,
            status: status,
            last_status_change: Instant::now(),
            clocks: clocks,
            log_clocks: log_clocks,
            storage: storage,
            sync_nodes: Default::default(),
            pending_bootstrap: false,
        };

        if !clean_shutdown {
            info!("Unclean shutdown, recovering from the storage");
            state.recover_dots();
        }
        state
    }

    fn recover_dots(&mut self) {
        for (&node, bv) in self.clocks.iter_mut() {
            let mut iterator = self.storage.log_iterator(node, bv.base() + 1);
            for ((_, dot), _) in iterator.iter() {
                bv.add(dot);
            }
        }
    }

    pub fn save(&self, db: &Database, shutdown: bool) {
        let saved_state = SavedVNodeState {
            id: self.id,
            clocks: self.clocks.clone(),
            log_clocks: self.log_clocks.clone(),
            clean_shutdown: shutdown,
        };
        debug!("Saving state for vnode {:?} {:?}", self.num, saved_state);
        let serialized_saved_state =
            bincode::serialize(&saved_state).expect("Can't serialize vnode state");
        db.meta_storage
            .set(self.num.to_string().as_bytes(), &serialized_saved_state)
            .expect("Can't save vnode state");
    }

    // STORAGE
    pub fn storage_get(&self, key: &[u8]) -> Result<Cube, ()> {
        let result = self.storage.get(key, |v| bincode::deserialize::<Cube>(v));
        match result {
            Ok(Some(Ok(cube))) => Ok(cube),
            Ok(Some(Err(_de))) => Err(()),
            Ok(None) => Ok(Cube::new(&self.clocks)),
            Err(_se) => Err(()),
        }
    }

    pub fn storage_set_local<'a, I: Iterator<Item = (Version, &'a [u8], &'a Cube)>>(
        &mut self,
        _db: &Database,
        writes: I,
    ) -> Result<(), CommandError> {
        let mut batch = self.storage.batch_new(0);
        for (version, key, cube) in writes {
            // TODO: integrate is_subsumed logic into the result of merge and MutatorFn
            if cube.is_subsumed(&self.clocks) {
                batch.del(key);
            } else {
                let bytes = bincode::serialize(cube).expect("Can't serialize Cube");
                batch.set(key, &bytes);
            }

            batch.log_set((self.id, version), key);
        }
        self.storage
            .batch_write(batch)
            .map_err(|_| CommandError::StorageError)?;

        Ok(())
    }

    pub fn storage_set_remote(
        &mut self,
        _db: &Database,
        writes: Vec<(Bytes, Cube, bool)>,
    ) -> Result<Vec<Option<Cube>>, ()> {
        let mut batch = self.storage.batch_new(0);
        let mut results = Vec::with_capacity(writes.len());
        for (key, proposed, reply_result) in writes {
            // need to fetch old before adding any dot
            // otherwise the dots might be added to Void cubes
            let old = self.storage_get(&key).map_err(|_| ())?;

            let mut empty = true;
            {
                let clocks = &mut self.clocks;
                let log_clocks = &mut self.log_clocks;
                proposed.for_each_dot(|i, v| {
                    // note that clocks is a superset of log_clocks
                    if log_clocks.add(i, v) {
                        clocks.add(i, v);
                        empty = false;
                        batch.log_set((i, v), &key);
                    }
                });
            }

            if empty && !reply_result {
                results.push(None);
                continue;
            }

            let new = old.merge(proposed);

            if !empty {
                if new.is_subsumed(&self.clocks) {
                    batch.del(&key);
                } else {
                    let serialized = bincode::serialize(&new).expect("Can't serialize Cube");
                    batch.set(&key, &serialized);
                }
            }

            results.push(if reply_result { Some(new) } else { None });
        }
        self.storage.batch_write(batch).map_err(|_| ())?;
        Ok(results)
    }
}
