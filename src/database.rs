use std::{net, time};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use dht::DHT;
use version_vector::*;
use fabric::*;
use vnode::*;
use workers::*;
use resp::RespValue;
use storage::{StorageManager, Storage};

pub type NodeId = u64;
pub type Token = u64;
pub type Cookie = (u64, u64);
pub type VNodeId = u16;

pub type DatabaseResponseFn = Box<Fn(Token, RespValue) + Send + Sync>;

pub struct Database {
    pub dht: DHT<()>,
    pub fabric: Fabric,
    pub meta_storage: Storage,
    pub storage_manager: StorageManager,
    workers: Mutex<WorkerManager>,
    vnodes: RwLock<HashMap<VNodeId, Mutex<VNode>>>,
    inflight: Mutex<HashMap<Cookie, ProxyReqState>>,
    pub response_fn: DatabaseResponseFn,
}

struct ProxyReqState {
    from: NodeId,
    cookie: Cookie,
}

macro_rules! vnode {
    ($s: expr, $k: expr, $ok: expr) => ({
        let vnodes = $s.vnodes.read().unwrap();
        vnodes.get(&$k).map(|vn| vn.lock().unwrap()).map($ok);
    });
    ($s: expr, $k: expr, $ok: expr, $el: expr) => ({
        let vnodes = $s.vnodes.read().unwrap();
        vnodes.get(&$k).ok_or($k).map(|vn| vn.lock().unwrap()).map_or_else($el, $ok);
    });
}

impl Database {
    pub fn new(node: NodeId, bind_addr: net::SocketAddr, storage_dir: &str, create: bool,
               response_fn: DatabaseResponseFn)
               -> Arc<Database> {
        let storage_manager = StorageManager::new(storage_dir).unwrap();
        let meta_storage = storage_manager.open(-1, true).unwrap();
        let workers = WorkerManager::new(8, time::Duration::from_millis(1000));
        let db = Arc::new(Database {
            fabric: Fabric::new(node, bind_addr).unwrap(),
            dht: DHT::new(3,
                          node,
                          bind_addr,
                          if create {
                              Some(((), 64))
                          } else {
                              None
                          }),
            storage_manager: storage_manager,
            meta_storage: meta_storage,
            inflight: Default::default(),
            response_fn: response_fn,
            vnodes: Default::default(),
            workers: Mutex::new(workers),
        });

        // register callback and then current nodes into fabric
        let cdb = Arc::downgrade(&db);
        db.dht.set_callback(Box::new(move || {
            cdb.upgrade().map(|db| {
                for (node, meta) in db.dht.members() {
                    db.fabric.register_node(node, meta);
                }
            });
        }));
        // current nodes
        db.dht.members().into_iter().map(|(n, a)| db.fabric.register_node(n, a)).count();

        let (sync_vnodes, pending_vnodes) = db.dht.vnodes_for_node(db.dht.node());
        // create storages, possibly clearing them
        for i in 0..db.dht.partitions() as VNodeId {
            if sync_vnodes.contains(&i) {
                db.init_vnode(i, false);
            } else if pending_vnodes.contains(&i) {
                db.init_vnode(i, false);
                // db.start_migration(i);
            } else {
                db.remove_vnode(i);
            }
        }

        db.workers.lock().unwrap().start(|| {
            let cdb = Arc::downgrade(&db);
            Box::new(move |chan| {
                for wm in chan {
                    trace!("worker got msg {:?}", wm);
                    let db = if let Some(db) = cdb.upgrade() {
                        db
                    } else {
                        break;
                    };
                    match wm {
                        WorkerMsg::Fabric(from, m) => db.handler_fabric_msg(from, m),
                        WorkerMsg::Tick(time) => db.handler_tick(time),
                        WorkerMsg::Command(token, cmd) => db.handler_cmd(token, cmd),
                        WorkerMsg::Exit => break,
                    }
                }
                info!("Exiting worker")
            })
        });

        // FIXME: fabric should have a start method
        // set fabric callbacks
        for &msg_type in &[FabricMsgType::Crud, FabricMsgType::Synch, FabricMsgType::Bootstrap] {
            let mut sender = db.sender();
            db.fabric.register_msg_handler(msg_type,
                                           Box::new(move |f, m| {
                                               sender.send(WorkerMsg::Fabric(f, m));
                                           }));
        }

        db
    }

    pub fn save(&self, shutdown: bool) {
        for vn in self.vnodes.write().unwrap().values() {
            vn.lock().unwrap().save(self, shutdown);
        }
    }

    fn init_vnode(&self, vnode_n: VNodeId, create: bool) {
        self.vnodes
            .write()
            .unwrap()
            .entry(vnode_n)
            .or_insert_with(|| Mutex::new(VNode::new(self, vnode_n, create)));
    }

    fn remove_vnode(&self, vnode_n: VNodeId) {
        let mut vnodes = self.vnodes.write().unwrap();
        vnodes.remove(&vnode_n);
        VNode::clear(self, vnode_n);
    }

    // FIXME: leaky abstraction
    pub fn sender(&self) -> WorkerSender {
        self.workers.lock().unwrap().sender()
    }

    fn handler_tick(&self, time: time::SystemTime) {
        for vn in self.vnodes.read().unwrap().values() {
            vn.lock().unwrap().handler_tick(time);
        }
    }

    fn handler_fabric_msg(&self, from: NodeId, msg: FabricMsg) {
        match msg {
            FabricMsg::GetRemote(m) => self.handler_get_remote(from, m),
            FabricMsg::GetRemoteAck(m) => self.handler_get_remote_ack(from, m),
            FabricMsg::Set(m) => self.handler_set(from, m),
            FabricMsg::SetAck(m) => self.handler_set_ack(from, m),
            FabricMsg::SetRemote(m) => self.handler_set_remote(from, m),
            FabricMsg::SetRemoteAck(m) => self.handler_set_remote_ack(from, m),
            FabricMsg::BootstrapStart(m) => {
                vnode!(self, m.vnode, |mut vn| vn.handler_bootstrap_start(self, from, m))
            }
            FabricMsg::BootstrapSend(m) => {
                vnode!(self, m.vnode, |mut vn| vn.handler_bootstrap_send(self, from, m))
            }
            FabricMsg::BootstrapAck(m) => {
                vnode!(self, m.vnode, |mut vn| vn.handler_bootstrap_ack(self, from, m))
            }
            FabricMsg::BootstrapFin(m) => {
                vnode!(self, m.vnode, |mut vn| vn.handler_bootstrap_fin(self, from, m))
            }
            FabricMsg::SyncStart(m) => {
                vnode!(self, m.vnode, |mut vn| vn.handler_sync_start(self, from, m))
            }
            FabricMsg::SyncSend(m) => {
                vnode!(self, m.vnode, |mut vn| vn.handler_sync_send(self, from, m))
            }
            FabricMsg::SyncAck(m) => {
                vnode!(self, m.vnode, |mut vn| vn.handler_sync_ack(self, from, m))
            }
            FabricMsg::SyncFin(m) => {
                vnode!(self, m.vnode, |mut vn| vn.handler_sync_fin(self, from, m))
            }
            msg @ _ => unreachable!("Can't handle {:?}", msg),
        };
    }

    fn migrations_inflight(&self) -> usize {
        self.vnodes
            .read()
            .unwrap()
            .values()
            .map(|vn| vn.lock().unwrap().migrations_inflight())
            .sum()
    }

    fn syncs_inflight(&self) -> usize {
        self.vnodes.read().unwrap().values().map(|vn| vn.lock().unwrap().syncs_inflight()).sum()
    }

    fn start_migration(&self, vnode: VNodeId) {
        let vnodes = self.vnodes.read().unwrap();
        vnodes.get(&vnode).unwrap().lock().unwrap().start_migration(self);
    }

    fn start_sync(&self, vnode: VNodeId, reverse: bool) {
        let vnodes = self.vnodes.read().unwrap();
        vnodes.get(&vnode).unwrap().lock().unwrap().start_sync(self, reverse);
    }

    fn send_set(&self, addr: NodeId, vnode: VNodeId, cookie: Cookie, key: &[u8],
                value_opt: Option<&[u8]>, vv: VersionVector) {
        self.fabric
            .send_message(addr,
                          FabricMsg::Set(MsgSet {
                              cookie: cookie,
                              vnode: vnode,
                              key: key.into(),
                              value: value_opt.map(|x| x.into()),
                              version_vector: vv,
                          }))
            .unwrap();
    }

    // CLIENT CRUD
    pub fn set(&self, token: Token, key: &[u8], value: Option<&[u8]>, vv: VersionVector) {
        let vnode = self.dht.key_vnode(key);
        vnode!(self, vnode, |mut vn| {
            vn.do_set(self, token, key, value, vv);
        });
    }

    pub fn get(&self, token: Token, key: &[u8]) {
        let vnode = self.dht.key_vnode(key);
        vnode!(self, vnode, |mut vn| {
            vn.do_get(self, token, key);
        });
    }

    // CRUD HANDLERS
    fn handler_set(&self, from: NodeId, msg: MsgSet) {
        vnode!(self, msg.vnode, |mut vn| {
            vn.handler_set(self, from, msg);
        });
    }

    fn handler_set_ack(&self, _from: NodeId, _msg: MsgSetAck) {}

    fn handler_set_remote(&self, from: NodeId, msg: MsgSetRemote) {
        vnode!(self, msg.vnode, |mut vn| {
            vn.handler_set_remote(self, from, msg);
        });
    }

    fn handler_set_remote_ack(&self, from: NodeId, msg: MsgSetRemoteAck) {
        vnode!(self, msg.vnode, |mut vn| {
            vn.handler_set_remote_ack(self, from, msg);
        });
    }

    fn handler_get_remote(&self, from: NodeId, msg: MsgGetRemote) {
        vnode!(self, msg.vnode, |mut vn| {
            vn.handler_get_remote(self, from, msg);
        });
    }

    fn handler_get_remote_ack(&self, from: NodeId, msg: MsgGetRemoteAck) {
        vnode!(self, msg.vnode, |mut vn| {
            vn.handler_get_remote_ack(self, from, msg);
        });
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let _ = self.vnodes.write().map(|mut vns| vns.clear());
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, net, fs, ops};
    use std::sync::{Mutex, Arc};
    use std::collections::HashMap;
    use super::*;
    use version_vector::{DottedCausalContainer, VersionVector};
    use env_logger;
    use bincode::{serde as bincode_serde, SizeLimit};
    use resp::RespValue;

    struct TestDatabase {
        db: Arc<Database>,
        responses: Arc<Mutex<HashMap<Token, RespValue>>>,
    }

    impl TestDatabase {
        fn new(node: NodeId, bind_addr: net::SocketAddr, storage_dir: &str, create: bool) -> Self {
            let responses1 = Arc::new(Mutex::new(HashMap::new()));
            let responses2 = responses1.clone();
            let db = Database::new(node,
                                   bind_addr,
                                   storage_dir,
                                   create,
                                   Box::new(move |t, v| {
                                       let r = responses1.lock().unwrap().insert(t, v);
                                       assert!(r.is_none(), "replaced a result");
                                   }));
            TestDatabase {
                db: db,
                responses: responses2,
            }
        }

        fn response(&self, token: Token) -> Option<DottedCausalContainer<Vec<u8>>> {
            (0..200)
                .filter_map(|_| {
                    thread::sleep_ms(10);
                    self.responses.lock().unwrap().remove(&token).and_then(|v| resp_to_dcc(v))
                })
                .next()
        }
    }

    impl ops::Deref for TestDatabase {
        type Target = Database;
        fn deref(&self) -> &Self::Target {
            &self.db
        }
    }

    fn resp_to_dcc(value: RespValue) -> Option<DottedCausalContainer<Vec<u8>>> {
        match value {
            RespValue::Data(bytes) => bincode_serde::deserialize(&bytes).ok(),
            _ => None,
        }
    }

    fn test_reload_stub(shutdown: bool) {
        let _ = fs::remove_dir_all("./t");
        let _ = env_logger::init();
        let mut db = TestDatabase::new(1, "127.0.0.1:9000".parse().unwrap(), "t/db", true);

        db.get(1, b"test");
        assert!(db.response(1).unwrap().is_empty());

        db.set(1, b"test", Some(b"value1"), VersionVector::new());
        assert!(db.response(1).unwrap().is_empty());

        db.get(1, b"test");
        assert!(db.response(1).unwrap().values().eq(vec![b"value1"]));

        db.save(shutdown);
        drop(db);
        db = TestDatabase::new(1, "127.0.0.1:9000".parse().unwrap(), "t/db", false);

        db.get(1, b"test");
        assert!(db.response(1).unwrap().values().eq(vec![b"value1"]));

        assert_eq!(1,
                   db.vnodes
                       .read()
                       .unwrap()
                       .values()
                       .map(|vn| vn.lock().unwrap()._log_len())
                       .sum::<usize>());
    }

    #[test]
    fn test_reload_shutdown() {
        test_reload_stub(true);
    }

    #[test]
    fn test_reload_dirty() {
        test_reload_stub(false);
    }

    #[test]
    fn test_one() {
        let _ = fs::remove_dir_all("./t");
        let _ = env_logger::init();
        let db = TestDatabase::new(1, "127.0.0.1:9000".parse().unwrap(), "t/db", true);
        for i in 0u16..64 {
            db.init_vnode(i, true);
        }
        db.get(1, b"test");
        assert!(db.response(1).unwrap().is_empty());

        db.set(1, b"test", Some(b"value1"), VersionVector::new());
        assert!(db.response(1).unwrap().is_empty());

        db.get(1, b"test");
        assert!(db.response(1).unwrap().values().eq(vec![b"value1"]));

        db.set(1, b"test", Some(b"value2"), VersionVector::new());
        assert!(db.response(1).unwrap().is_empty());

        db.get(1, b"test");
        let state = db.response(1).unwrap();
        assert!(state.values().eq(vec![b"value1", b"value2"]));

        db.set(1, b"test", Some(b"value12"), state.version_vector().clone());
        assert!(db.response(1).unwrap().is_empty());

        db.get(1, b"test");
        let state = db.response(1).unwrap();
        assert!(state.values().eq(vec![b"value12"]));

        db.set(1, b"test", None, state.version_vector().clone());
        assert!(db.response(1).unwrap().is_empty());

        db.get(1, b"test");
        assert!(db.response(1).unwrap().is_empty());
    }

    #[test]
    fn test_two() {
        let _ = fs::remove_dir_all("./t");
        let _ = env_logger::init();
        let db1 = TestDatabase::new(1, "127.0.0.1:9000".parse().unwrap(), "t/db1", true);
        let db2 = TestDatabase::new(2, "127.0.0.1:9001".parse().unwrap(), "t/db2", false);
        for i in 0u16..64 {
            db1.init_vnode(i, true);
            db2.init_vnode(i, true);
        }
        for i in 0u16..32 {
            db1.dht.add_pending_node(i * 2 + 1, db2.dht.node(), ());
            db1.dht.promote_pending_node(i * 2 + 1, db2.dht.node());
        }

        db1.get(1, b"test");
        thread::sleep_ms(100);
        assert!(db1.response(1).unwrap().is_empty());

        db1.set(1, b"test", Some(b"value1"), VersionVector::new());
        thread::sleep_ms(100);
        assert!(db1.response(1).unwrap().is_empty());

        for &db in &[&db1, &db2] {
            db.get(1, b"test");
            assert!(db.response(1).unwrap().values().eq(vec![b"value1"]));
        }
    }

    const TEST_JOIN_SIZE: u64 = 10;

    #[test]
    fn test_join_migrate() {
        let _ = fs::remove_dir_all("./t");
        let _ = env_logger::init();
        let db1 = TestDatabase::new(1, "127.0.0.1:9000".parse().unwrap(), "t/db1", true);
        for i in 0u16..64 {
            db1.init_vnode(i, true);
        }
        for i in 0..TEST_JOIN_SIZE {
            db1.set(i,
                    i.to_string().as_bytes(),
                    Some(i.to_string().as_bytes()),
                    VersionVector::new());
            db1.response(i).unwrap();
        }
        for i in 0..TEST_JOIN_SIZE {
            db1.get(i, i.to_string().as_bytes());
            assert!(db1.response(i).unwrap().values().eq(&[i.to_string().as_bytes()]));
        }

        let db2 = TestDatabase::new(2, "127.0.0.1:9001".parse().unwrap(), "t/db2", false);

        for i in 0u16..64 {
            db2.init_vnode(i, true);
        }
        warn!("will check data in db2 before balancing");
        for i in 0..TEST_JOIN_SIZE {
            db2.get(i, i.to_string().as_bytes());
            assert!(db2.response(i).unwrap().values().eq(&[i.to_string().as_bytes()]));
        }

        db2.dht.claim(db2.dht.node(), ());
        for i in 0u16..64 {
            if db2.dht.nodes_for_vnode(i, true).contains(&db2.dht.node()) {
                db2.start_migration(i);
            }
        }
        warn!("will check data in db2 during balancing");
        for i in 0..TEST_JOIN_SIZE {
            db2.get(i, i.to_string().as_bytes());
            let result = db2.response(i);
            assert!(result.unwrap().values().eq(&[i.to_string().as_bytes()]));
        }

        while db1.migrations_inflight() + db2.migrations_inflight() > 0 {
            warn!("waiting for migrations to finish");
            thread::sleep_ms(1000);
        }

        warn!("will check data in db2 after balancing");
        for i in 0..TEST_JOIN_SIZE {
            db2.get(i, i.to_string().as_bytes());
            assert!(db2.response(i).unwrap().values().eq(&[i.to_string().as_bytes()]));
        }
    }

    #[test]
    fn test_join_sync_reverse() {
        let _ = fs::remove_dir_all("./t");
        let _ = env_logger::init();
        let mut db1 = TestDatabase::new(1, "127.0.0.1:9000".parse().unwrap(), "t/db1", true);
        let mut db2 = TestDatabase::new(2, "127.0.0.1:9001".parse().unwrap(), "t/db2", false);
        for i in 0u16..64 {
            db1.init_vnode(i, true);
            db2.init_vnode(i, true);
        }
        db2.dht.claim(db2.dht.node(), ());
        for i in 0u16..64 {
            db2.dht.promote_pending_node(i, db2.dht.node());
        }
        for i in 0..TEST_JOIN_SIZE {
            db1.set(i,
                    i.to_string().as_bytes(),
                    Some(i.to_string().as_bytes()),
                    VersionVector::new());
            db1.response(i).unwrap();
        }
        for i in 0..TEST_JOIN_SIZE {
            db1.get(i, i.to_string().as_bytes());
            let result1 = db1.response(i);
            db2.get(i, i.to_string().as_bytes());
            let result2 = db2.response(i);
            assert_eq!(result1, result2);
        }

        drop(db1);
        let _ = fs::remove_dir_all("t/db1");
        db1 = TestDatabase::new(1, "127.0.0.1:9000".parse().unwrap(), "t/db1", false);

        for i in 0u16..64 {
            if db1.dht.nodes_for_vnode(i, true).contains(&db1.dht.node()) {
                db1.start_sync(i, true);
            }
        }

        while db1.syncs_inflight() + db2.syncs_inflight() > 0 {
            warn!("waiting for syncs to finish");
            thread::sleep_ms(1000);
        }

        warn!("will check data in db1 after sync");
        for i in 0..TEST_JOIN_SIZE {
            db1.get(i, i.to_string().as_bytes());
            assert!(db1.response(i).unwrap().values().eq(&[i.to_string().as_bytes()]));
        }
    }

    #[test]
    fn test_join_sync() {
        let _ = fs::remove_dir_all("./t");
        let _ = env_logger::init();
        let db1 = TestDatabase::new(1, "127.0.0.1:9000".parse().unwrap(), "t/db1", true);
        for i in 0u16..64 {
            db1.init_vnode(i, true);
        }
        for i in 0..TEST_JOIN_SIZE {
            db1.set(i,
                    i.to_string().as_bytes(),
                    Some(i.to_string().as_bytes()),
                    VersionVector::new());
            db1.response(i).unwrap();
        }
        for i in 0..TEST_JOIN_SIZE {
            db1.get(i, i.to_string().as_bytes());
            assert!(db1.response(i).unwrap().values().eq(&[i.to_string().as_bytes()]));
        }

        let db2 = TestDatabase::new(2, "127.0.0.1:9001".parse().unwrap(), "t/db2", false);

        for i in 0u16..64 {
            db2.init_vnode(i, true);
        }
        warn!("will check data in db2 before balancing");
        for i in 0..TEST_JOIN_SIZE {
            db2.get(i, i.to_string().as_bytes());
            assert!(db2.response(i).unwrap().values().eq(&[i.to_string().as_bytes()]));
        }

        db2.dht.claim(db2.dht.node(), ());
        for i in 0u16..64 {
            if db2.dht.nodes_for_vnode(i, true).contains(&db2.dht.node()) {
                db2.start_sync(i, false);
            }
        }
        warn!("will check data in db2 during balancing");
        for i in 0..TEST_JOIN_SIZE {
            db2.get(i, i.to_string().as_bytes());
            let result = db2.response(i);
            assert!(result.unwrap().values().eq(&[i.to_string().as_bytes()]));
        }

        while db1.syncs_inflight() + db2.syncs_inflight() > 0 {
            warn!("waiting for migrations to finish");
            thread::sleep_ms(1000);
        }

        for i in db2.vnodes.read().unwrap().keys().cloned() {
            db2.dht.promote_pending_node(i, db2.dht.node());
        }

        warn!("will check data in db2 after balancing");
        for i in 0..TEST_JOIN_SIZE {
            db2.get(i, i.to_string().as_bytes());
            assert!(db2.response(i).unwrap().values().eq(&[i.to_string().as_bytes()]));
        }
    }
}
