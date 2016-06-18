use std::net;
use std::path::Path;
use std::collections::{HashMap, BTreeMap};
use linear_map::LinearMap;
use version_vector::*;
use storage::{Storage, StorageIterator};
use database::Database;
use bincode::{self, serde as bincode_serde};
use fabric::*;
use utils::GenericError;
use rand::{Rng, thread_rng};

const PEER_LOG_SIZE: usize = 1000;

#[repr(u8)]
enum VNodeState {
    Sync,
    Syncing,
    Zombie,
    Wait,
}

pub struct VNode {
    num: u16,
    cookie_gen: u64,
    // state: VNodeState,
    clock: BitmappedVersionVector,
    log: BTreeMap<u64, Vec<u8>>,
    peers: LinearMap<u64, VNodePeer>,
    migrations: LinearMap<u64, Migration>,
    syncs: LinearMap<u64, Synchronization>,
    storage: Storage,
}

struct VNodePeer {
    knowledge: u64,
    log: BTreeMap<u64, Vec<u8>>,
}

enum Migration {
    Outgoing {
        peer: net::SocketAddr,
        iterator: StorageIterator,
        count: u64,
    },
    Incomming {
        peer: net::SocketAddr,
        count: u64,
    },
}

enum Synchronization {
    Outgoing {},
    Incomming {},
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
}

macro_rules! handle {
    ($db: expr, $from: expr, $msg: expr, $eet: ident, $r: expr) => (
        match $r {
            Ok(ok) => ok,
            Err(e) => {
                $db.fabric.send_message(&$from,  $eet {
                    cookie: $msg.cookie,
                    vnode: $msg.vnode,
                    result: Err(e),
                }).unwrap();
                return;
            }
        }
    );
}

impl VNode {
    pub fn new(storage_dir: &Path, num: u16) -> VNode {
        VNode {
            num: num,
            cookie_gen: 0,
            clock: BitmappedVersionVector::new(),
            peers: Default::default(),
            migrations: Default::default(),
            syncs: Default::default(),
            storage: Storage::open(storage_dir, num as i32, true).unwrap(),
            log: Default::default(),
        }
    }

    fn cookie(&mut self) -> u64 {
        let cookie = self.cookie_gen;
        self.cookie_gen += 1;
        cookie
    }

    // HANDLERS
    pub fn handler_bootstrap_start(&mut self, db: &Database, from: &net::SocketAddr,
                                   msg: FabricBootstrapStart) {
        let mut migration = Migration::Outgoing {
            iterator: self.storage.iter(),
            peer: *from,
            count: 0,
        };

        migration.send(db, self.num, msg.cookie);

        let p = self.migrations.insert(msg.cookie, migration);
        assert!(p.is_none());
    }

    pub fn handler_bootstrap_send(&mut self, db: &Database, from: &net::SocketAddr,
                                  msg: FabricBootstrapSend) {
        let migration =
            handle!(db,
                    from,
                    msg,
                    FabricBootstrapFin,
                    self.migrations.get_mut(&msg.cookie).ok_or(FabricMsgError::CookieNotFound));

        match *migration {
            Migration::Incomming { ref mut count, .. } => *count += 1,
            _ => unreachable!(),
        }

        let mut bytes = Vec::new();
        bincode_serde::serialize_into(&mut bytes, &msg.container, bincode::SizeLimit::Infinite)
            .unwrap();
        self.storage.set(&msg.key, &bytes);

        db.fabric
            .send_message(&from,
                          FabricBootstrapAck {
                              cookie: msg.cookie,
                              vnode: self.num,
                          })
            .unwrap();
    }

    pub fn handler_bootstrap_ack(&mut self, db: &Database, from: &net::SocketAddr,
                                 msg: FabricBootstrapAck) {
        let migration =
            handle!(db,
                    from,
                    msg,
                    FabricBootstrapFin,
                    self.migrations.get_mut(&msg.cookie).ok_or(FabricMsgError::CookieNotFound));

        migration.send(db, self.num, msg.cookie);
    }

    pub fn handler_bootstrap_fin(&mut self, db: &Database, from: &net::SocketAddr,
                                 msg: FabricBootstrapFin) {
        match msg.result {
            Ok(_) => {
                info!("{:?} from {}", msg, from);
                self.migrations.remove(&msg.cookie).unwrap();
            }
            Err(_) => {
                warn!("{:?} from {}", msg, from);
                self.migrations.remove(&msg.cookie).unwrap();
            }
        }
    }

    pub fn start_migration(&mut self, db: &Database) {
        let cookie = self.cookie();
        let mut nodes = db.dht.nodes_for_vnode(self.num, db.replication_factor, false);
        thread_rng().shuffle(&mut nodes);
        for node in nodes {
            if node == db.dht.node() {
                continue;
            }
            let migration = Migration::Incomming {
                peer: node,
                count: 0,
            };

            db.fabric
                .send_message(&node,
                              FabricBootstrapStart {
                                  cookie: cookie,
                                  vnode: self.num,
                              })
                .unwrap();

            let p = self.migrations.insert(cookie, migration);
            assert!(p.is_none());
            return;
        }
        unreachable!();
    }

    // STORAGE
    pub fn storage_get(&self, _db: &Database, key: &[u8]) -> DottedCausalContainer<Vec<u8>> {
        let mut dcc = if let Some(bytes) = self.storage.get_vec(key) {
            bincode_serde::deserialize(&bytes).unwrap()
        } else {
            DottedCausalContainer::new()
        };
        dcc.fill(&self.clock);
        dcc
    }

    pub fn storage_set_local(&mut self, db: &Database, id: u64, key: &[u8],
                             value_opt: Option<&[u8]>, vv: &VersionVector)
                             -> DottedCausalContainer<Vec<u8>> {
        let mut dcc = self.storage_get(db, key);
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
        dcc
    }

    pub fn storage_set_remote(&mut self, db: &Database, peer_id: u64, key: &[u8],
                              mut new_dcc: DottedCausalContainer<Vec<u8>>) {
        let old_dcc = self.storage_get(db, key);
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

        self.peers
            .entry(peer_id)
            .or_insert_with(|| VNodePeer::new())
            .log(self.clock.get(peer_id).unwrap().base(), key.into());
    }
}

impl Migration {
    fn send(&mut self, db: &Database, vnode: u16, cookie: u64) -> bool {
        match *self {
            Migration::Outgoing { peer, ref mut iterator, ref mut count } => {
                let done = !iterator.iter(|k, v| {
                    db.fabric
                        .send_message(&peer,
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
                    debug!("[{}] migration of {} is done", db.dht.node(), vnode);
                    db.fabric
                        .send_message(&peer,
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
}
