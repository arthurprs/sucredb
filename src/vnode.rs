use std::path::Path;
use std::collections::{BTreeMap};
use linear_map::LinearMap;
use version_vector::*;
use storage::Storage;
use database::Database;
use bincode::{self, serde as bincode_serde};

const PEER_LOG_SIZE: usize = 1000;

#[repr(u8)]
enum VNodeState {
    Sync,
    Syncing,
    Zombie,
    Wait,
}

struct VNodePeer {
    knowledge: u64,
    log: BTreeMap<u64, Vec<u8>>,
}

pub struct VNode {
    num: u16,
    // state: VNodeState,
    clock: BitmappedVersionVector,
    log: BTreeMap<u64, Vec<u8>>,
    peers: LinearMap<u64, VNodePeer>,
    storage: Storage,
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

impl VNode {
    pub fn new(num: u16) -> VNode {
        VNode {
            num: num,
            clock: BitmappedVersionVector::new(),
            peers: Default::default(),
            storage: Storage::open(Path::new("./vnode_t"), num as i32, true).unwrap(),
            log: Default::default(),
        }
    }

    pub fn get(&self, _db: &Database, key: &[u8]) -> DottedCausalContainer<Vec<u8>> {
        let mut dcc = if let Some(bytes) = self.storage.get_vec(key) {
            bincode_serde::deserialize(&bytes).unwrap()
        } else {
            DottedCausalContainer::new()
        };
        dcc.fill(&self.clock);
        dcc
    }

    pub fn set_local(&mut self, db: &Database, id: u64, key: &[u8], value_opt: Option<&[u8]>,
                 vv: &VersionVector)
                 -> DottedCausalContainer<Vec<u8>> {
        let mut dcc = self.get(db, key);
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

    pub fn set_remote(&mut self, db: &Database, peer_id: u64, key: &[u8],
                  mut new_dcc: DottedCausalContainer<Vec<u8>>) {
        let old_dcc = self.get(db, key);
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
