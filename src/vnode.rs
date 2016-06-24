use std::path::Path;
use std::collections::BTreeMap;
use linear_map::LinearMap;
use version_vector::*;
use storage::{Storage, StorageIterator};
use database::Database;
use bincode::{self, serde as bincode_serde};
use fabric::*;
use utils::GenericError;
use rand::{Rng, thread_rng};
use hash::hash;

const PEER_LOG_SIZE: usize = 1000;

#[repr(u8)]
enum VNodeStatus {
    InSync,
    Syncing,
    Zombie,
}

pub struct VNode {
    pub state: VNodeState,
    migrations: LinearMap<(NodeId, u64), Migration>,
    syncs: LinearMap<(NodeId, u64), Synchronization>,
}

pub struct VNodeState {
    num: u16,
    peers: LinearMap<u64, VNodePeer>,
    clock: BitmappedVersionVector,
    log: BTreeMap<u64, Vec<u8>>,
    storage: Storage,
    unflushed_coord_writes: usize,
}

struct VNodePeer {
    knowledge: u64,
    log: BTreeMap<u64, Vec<u8>>,
}

enum Migration {
    Outgoing {
        vnode: u16,
        cookie: u64,
        peer: NodeId,
        iterator: StorageIterator,
        count: u64,
    },
    Incomming {
        vnode: u16,
        cookie: u64,
        peer: NodeId,
        count: u64,
    },
}

enum Synchronization {
    Outgoing {
        clock_in_peer: BitmappedVersion,
        clock_snapshot: BitmappedVersion,
        missing_dots: BitmappedVersionDelta,
        vnode: u16,
        cookie: u64,
        peer: NodeId,
        count: u64,
    },
    Incomming {
        vnode: u16,
        cookie: u64,
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
        debug_assert!(until > self.knowledge);
        self.knowledge = until;
    }

    fn log(&mut self, dot: u64, key: Vec<u8>) {
        let prev = self.log.insert(dot, key);
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
    ($db: expr, $from: expr, $msg: expr, $emsg: ident, $r: expr) => (
        match $r {
            Ok(ok) => ok,
            Err(e) => {
                $db.fabric.send_message($from,  $emsg {
                    cookie: $msg.cookie,
                    vnode: $msg.vnode,
                    result: Err(e),
                }).unwrap();
                return;
            }
        }
    );
    ($this: expr, $db: expr, $from: expr, $msg: expr, $emsg: ident, $col: ident, $f: ident) => {
        let cookie = $msg.cookie;
        if {
            let sync = forward!($db, $from, $msg, $emsg,
                $this.$col
                    .get_mut(&($from, cookie))
                    .ok_or(FabricMsgError::CookieNotFound));
            sync.$f($db, &mut $this.state, $msg)
        } {
            $this.$col.remove(&($from, cookie)).unwrap();
        }
    }
}

impl VNode {
    pub fn new(storage_dir: &Path, num: u16) -> VNode {
        VNode {
            state: VNodeState {
                num: num,
                clock: BitmappedVersionVector::new(),
                peers: Default::default(),
                log: Default::default(),
                storage: Storage::open(storage_dir, num as i32, true).unwrap(),
                unflushed_coord_writes: 0,
            },
            migrations: Default::default(),
            syncs: Default::default(),
        }
    }

    fn cookie(&mut self) -> u64 {
        thread_rng().gen()
    }

    // HANDLERS
    pub fn handler_bootstrap_start(&mut self, db: &Database, from: NodeId,
                                   msg: FabricBootstrapStart) {
        let cookie = msg.cookie;
        let mut migration = Migration::Outgoing {
            vnode: self.state.num,
            cookie: cookie,
            iterator: self.state.storage.iter(),
            peer: from,
            count: 0,
        };
        migration.on_start(db, &mut self.state, msg);

        let p = self.migrations.insert((from, cookie), migration);
        assert!(p.is_none());
    }

    pub fn handler_bootstrap_send(&mut self, db: &Database, from: NodeId, msg: FabricBootstrapSend) {
        forward!(self, db, from, msg, FabricBootstrapFin, migrations, on_send);
    }

    pub fn handler_bootstrap_ack(&mut self, db: &Database, from: NodeId, msg: FabricBootstrapAck) {
        forward!(self, db, from, msg, FabricBootstrapFin, migrations, on_ack);
    }

    pub fn handler_bootstrap_fin(&mut self, db: &Database, from: NodeId, msg: FabricBootstrapFin) {
        forward!(self, db, from, msg, FabricBootstrapFin, migrations, on_fin);
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
        let cookie = self.cookie();
        let mut nodes = db.dht.nodes_for_vnode(self.state.num, db.replication_factor, false);
        thread_rng().shuffle(&mut nodes);
        for node in nodes {
            if node == db.dht.node() {
                continue;
            }
            let migration = Migration::Incomming {
                vnode: self.state.num,
                cookie: cookie,
                peer: node,
                count: 0,
            };

            db.fabric
                .send_message(node,
                              FabricBootstrapStart {
                                  cookie: cookie,
                                  vnode: self.state.num,
                              })
                .unwrap();

            let p = self.migrations.insert((node, cookie), migration);
            assert!(p.is_none());
            return;
        }
        unreachable!();
    }

    // pub fn start_sync(&mut self, db: &Database) {
    //     self.state.peers.iter().max_by_key(|&(&n, vp)|
    //         self.state.clock.get(n).map_or(0, |bv| bv.base()) - vp.knowledge
    //     )
    //     .map(|(&n, vp)| {
    //         let cookie = self.cookie();
    //         let sync = Synchronization::Incomming {
    //             peer: n,
    //             cookie: cookie,
    //             vnode: self.state.num,
    //             count: 0,
    //         };
    //
    //         db.fabric
    //             .send_message(n,
    //                           MsgSyncStart {
    //                               cookie: cookie,
    //                               vnode: self.state.num,
    //                           })
    //             .unwrap();
    //         let p = self.syncs.insert((n, cookie), sync);
    //         assert!(p.is_none());
    //         return;
    //     });
    // }
}


impl VNodeState {
    // STORAGE
    pub fn storage_get(&self, key: &[u8]) -> DottedCausalContainer<Vec<u8>> {
        let mut dcc = if let Some(bytes) = self.storage.get_vec(key) {
            bincode_serde::deserialize(&bytes).unwrap()
        } else {
            DottedCausalContainer::new()
        };
        dcc.fill(&self.clock);
        dcc
    }

    pub fn storage_set_local(&mut self, id: u64, key: &[u8], value_opt: Option<&[u8]>,
                             vv: &VersionVector)
                             -> DottedCausalContainer<Vec<u8>> {
        let mut dcc = self.storage_get(key);
        dcc.discard(vv);
        let dot = self.clock.event(id);
        if let Some(value) = value_opt {
            dcc.add(id, dot, value.into());
        }
        dcc.strip(&self.clock);

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
        new_dcc.add_to_bvv(&mut self.clock);
        new_dcc.sync(old_dcc);
        new_dcc.strip(&self.clock);

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
                .or_insert_with(|| VNodePeer::new())
                .log(self.clock.get(peer_id).unwrap().base(), key.into());
        }
    }
}

#[allow(unused_variables)]
impl Synchronization {
    fn outgoing_send(&mut self, db: &Database, state: &mut VNodeState) -> bool {
        match *self {
            Synchronization::Outgoing { peer,
                                        cookie,
                                        vnode,
                                        ref mut missing_dots,
                                        ref mut count,
                                        .. } => {
                let vnpeer = state.peers.get(&hash(peer)).unwrap();
                let kv_opt = missing_dots.next()
                    .and_then(|dot| vnpeer.get(dot))
                    .and_then(|k| state.storage.get_vec(&k).map(|v| (k, v)));
                if let Some((k, v)) = kv_opt {
                    db.fabric
                        .send_message(peer,
                                      MsgSyncSend {
                                          cookie: cookie,
                                          vnode: vnode,
                                          key: k.into(),
                                          container: bincode_serde::deserialize(&v).unwrap(),
                                      })
                        .unwrap();
                    *count += 1;
                    false
                } else {
                    debug!("[{}] synchronization of {} is done", db.dht.node(), vnode);
                    db.fabric
                        .send_message(peer,
                                      MsgSyncFin {
                                          cookie: cookie,
                                          vnode: vnode,
                                          result: Ok(()),
                                      })
                        .unwrap();
                    true
                }
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
            Synchronization::Incomming { peer, vnode, ref mut count, .. } => {
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
                                      vnode: vnode,
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
                // merge remote clock snapshot into our clocks
                state.storage.sync();
                // send it back as a form of ack-ack
                db.fabric.send_message(peer, msg).unwrap();
            }
            Synchronization::Outgoing { ref clock_in_peer, peer, .. } => {
                let vnpeer = state.peers.get_mut(&hash(peer)).unwrap();
                vnpeer.advance_knowledge(clock_in_peer.base());
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
    fn outgoing_send(&mut self, db: &Database) -> bool {
        match *self {
            Migration::Outgoing { peer, cookie, vnode, ref mut iterator, ref mut count } => {
                let done = !iterator.iter(|k, v| {
                    db.fabric
                        .send_message(peer,
                                      FabricBootstrapSend {
                                          cookie: cookie,
                                          vnode: vnode,
                                          key: k.into(),
                                          container: bincode_serde::deserialize(v).unwrap(),
                                      })
                        .unwrap();
                    *count += 1;
                    false
                });
                if done {
                    debug!("[{}] sync of {} is done", db.dht.node(), vnode);
                    db.fabric
                        .send_message(peer,
                                      FabricBootstrapFin {
                                          cookie: cookie,
                                          vnode: vnode,
                                          result: Ok(()),
                                      })
                        .unwrap();
                }
                done
            }
            _ => unreachable!(),
        }
    }

    fn on_start(&mut self, db: &Database, state: &mut VNodeState, msg: FabricBootstrapStart) -> bool {
        match *self {
            Migration::Outgoing { .. } => self.outgoing_send(db),
            _ => unreachable!(),
        }
    }

    fn on_send(&mut self, db: &Database, state: &mut VNodeState, msg: FabricBootstrapSend) -> bool {
        match *self {
            Migration::Incomming { vnode, peer, ref mut count, .. } => {
                state.storage_set_remote(&msg.key, msg.container, None);
                db.fabric
                    .send_message(peer,
                                  FabricBootstrapAck {
                                      cookie: msg.cookie,
                                      vnode: vnode,
                                  })
                    .unwrap();

                *count += 1;
                false
            }
            _ => unreachable!(),
        }
    }

    fn on_fin(&mut self, db: &Database, state: &mut VNodeState, msg: FabricBootstrapFin) -> bool {
        match *self {
            Migration::Incomming { peer, .. } => {
                state.storage.sync();
                // db.dht.promote_pending_node(vnode: u16, node: NodeId)
                // send it back as a form of ack-ack
                db.fabric.send_message(peer, msg).unwrap();
            }
            Migration::Outgoing { .. } => (),
        }
        true
    }

    fn on_ack(&mut self, db: &Database, state: &mut VNodeState, msg: FabricBootstrapAck) -> bool {
        match *self {
            Migration::Outgoing { .. } => self.outgoing_send(db),
            _ => unreachable!(),
        }
    }
}
