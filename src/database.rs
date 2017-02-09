use std::{str, time, net};
use std::sync::{Arc, Mutex, RwLock};
use dht::{self, DHT};
use version_vector::*;
use fabric::*;
use vnode::*;
use workers::*;
use resp::RespValue;
use storage::{StorageManager, Storage};
use rand::{thread_rng, Rng};
use utils::{IdHashMap, split_u64, join_u64};
pub use types::*;
use config::Config;

pub type DatabaseResponseFn = Box<Fn(Token, RespValue) + Send + Sync>;

pub struct Database {
    pub dht: DHT<net::SocketAddr>,
    pub fabric: Fabric,
    pub meta_storage: Storage,
    pub storage_manager: StorageManager,
    pub response_fn: DatabaseResponseFn,
    pub config: Config,
    vnodes: RwLock<IdHashMap<VNodeId, Mutex<VNode>>>,
    workers: Mutex<WorkerManager>,
}

macro_rules! fabric_send_error{
    ($db: expr, $to: expr, $msg: expr, $emsg: ident, $err: expr) => {
        $db.fabric.send_msg($to, $emsg {
            vnode: $msg.vnode,
            cookie: $msg.cookie,
            result: Err($err),
        })
    };
    ($db: expr, $to: expr, $vnode: expr, $cookie: expr, $emsg: ident, $err: expr) => {
        $db.fabric.send_msg($to, $emsg {
            vnode: $vnode,
            cookie: $cookie,
            result: Err($err),
        })
    };
}

macro_rules! vnode {
    ($s: expr, $k: expr, $ok: expr) => ({
        let vnodes = $s.vnodes.read().unwrap();
        vnodes.get(&$k).map(|vn| vn.lock().unwrap()).map($ok);
    });
}

impl Database {
    pub fn new(config: &Config, response_fn: DatabaseResponseFn) -> Arc<Database> {
        let storage_manager = StorageManager::new(&config.data_dir).unwrap();
        let meta_storage = storage_manager.open(-1, true).unwrap();

        let (old_node, node) = if let Some(s_node) = meta_storage.get_vec(b"node") {
            let prev_node = String::from_utf8(s_node).unwrap().parse::<NodeId>().unwrap();
            if meta_storage.get_vec(b"clean_shutdown").is_some() {
                (None, prev_node)
            } else {
                let node = join_u64(split_u64(prev_node).0, thread_rng().gen());
                (Some(prev_node), node)
            }
        } else {
            (None, thread_rng().gen::<i64>().abs() as NodeId)
        };
        if let Some(s_cluster) = meta_storage.get_vec(b"cluster") {
            assert_eq!(s_cluster, config.cluster_name.as_bytes(), "Stored cluster name differs!");
        }
        meta_storage.set(b"cluster", config.cluster_name.as_bytes());
        meta_storage.set(b"node", node.to_string().as_bytes());
        meta_storage.del(b"clean_shutdown");
        meta_storage.sync();

        info!("old_node:{:?} node:{:?}", old_node, node);

        let fabric = Fabric::new(node, config.fabric_addr).unwrap();
        let dht = DHT::new(node,
                           config.fabric_addr,
                           &config.cluster_name,
                           &config.etcd_addr,
                           config.listen_addr,
                           if let Some(init) = config.cmd_init.as_ref() {
                               Some(dht::RingDescription::new(init.replication_factor,
                                                              init.partitions))
                           } else {
                               None
                           },
                           old_node);
        let workers = WorkerManager::new(node,
                                         config.workers as _,
                                         time::Duration::from_millis(config.worker_timer as _));
        let db = Arc::new(Database {
            fabric: fabric,
            dht: dht,
            storage_manager: storage_manager,
            meta_storage: meta_storage,
            response_fn: response_fn,
            vnodes: Default::default(),
            workers: Mutex::new(workers),
            config: config.clone(),
        });

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
                        WorkerMsg::DHTChange => db.handler_dht_change(),
                        WorkerMsg::Exit => break,
                    }
                }
                info!("Exiting worker")
            })
        });

        let mut dht_change_sender = db.sender();
        db.dht.set_callback(Box::new(move || {
            dht_change_sender.send(WorkerMsg::DHTChange);
        }));

        // register nodes into fabric
        db.dht.members().into_iter().map(|(n, a)| db.fabric.register_node(n, a)).count();
        // FIXME: fabric should have a start method that receives the callbacks
        // set fabric callbacks
        for &msg_type in &[FabricMsgType::Crud, FabricMsgType::Synch] {
            let mut sender = db.sender();
            db.fabric.register_msg_handler(msg_type,
                                           Box::new(move |f, m| {
                                               sender.send(WorkerMsg::Fabric(f, m));
                                           }));
        }

        {
            // acquire exclusive lock to vnodes to intialize them
            let mut vnodes = db.vnodes.write().unwrap();
            let (ready_vnodes, pending_vnodes) = db.dht.vnodes_for_node(db.dht.node());
            // create vnodes
            *vnodes = (0..db.dht.partitions() as VNodeId)
                .map(|i| {
                    let vn = if ready_vnodes.contains(&i) {
                        VNode::new(&db, i, VNodeStatus::Ready)
                    } else if pending_vnodes.contains(&i) {
                        VNode::new(&db, i, VNodeStatus::Bootstrap)
                    } else {
                        VNode::new(&db, i, VNodeStatus::Absent)
                    };
                    (i, Mutex::new(vn))
                })
                .collect();
        }

        db
    }

    pub fn save(&self, shutdown: bool) {
        for vn in self.vnodes.read().unwrap().values() {
            vn.lock().unwrap().save(self, shutdown);
        }
        self.meta_storage.set(b"clean_shutdown", b"1");
    }

    // FIXME: leaky abstraction
    pub fn sender(&self) -> WorkerSender {
        self.workers.lock().unwrap().sender()
    }

    fn handler_dht_change(&self) {
        for (node, addr) in self.dht.members() {
            self.fabric.register_node(node, addr);
        }

        for (&i, vn) in self.vnodes.read().unwrap().iter() {
            let final_status =
                if self.dht.nodes_for_vnode(i, true, true).contains(&self.dht.node()) {
                    VNodeStatus::Ready
                } else {
                    VNodeStatus::Absent
                };
            vn.lock().unwrap().handler_dht_change(self, final_status);
        }
    }

    fn handler_tick(&self, time: time::Instant) {
        let mut incomming_syncs = 0usize;
        let vnodes = self.vnodes.read().unwrap();
        for vn in vnodes.values() {
            let mut vn = vn.lock().unwrap();
            vn.handler_tick(self, time);
            incomming_syncs += vn.syncs_inflight().0;
        }
        if incomming_syncs < self.config.max_incomming_syncs as usize {
            let mut rng = thread_rng();
            for i in (0..vnodes.len()).map(|_| rng.gen::<u16>() % vnodes.len() as u16) {
                incomming_syncs += vnodes.get(&i).unwrap().lock().unwrap().maybe_start_sync(self);
                if incomming_syncs >= self.config.max_incomming_syncs as usize {
                    break;
                }
            }
        }
    }

    fn handler_fabric_msg(&self, from: NodeId, msg: FabricMsg) {
        match msg {
            FabricMsg::RemoteGet(m) => {
                vnode!(self, m.vnode, |mut vn| vn.handler_get_remote(self, from, m));
            }
            FabricMsg::RemoteGetAck(m) => {
                vnode!(self, m.vnode, |mut vn| vn.handler_get_remote_ack(self, from, m));
            }
            FabricMsg::RemoteSet(m) => {
                vnode!(self, m.vnode, |mut vn| vn.handler_set_remote(self, from, m));
            }
            FabricMsg::RemoteSetAck(m) => {
                vnode!(self, m.vnode, |mut vn| vn.handler_set_remote_ack(self, from, m));
            }
            FabricMsg::SyncStart(m) => {
                vnode!(self, m.vnode, |mut vn| vn.handler_sync_start(self, from, m));
            }
            FabricMsg::SyncSend(m) => {
                vnode!(self, m.vnode, |mut vn| vn.handler_sync_send(self, from, m));
            }
            FabricMsg::SyncAck(m) => {
                vnode!(self, m.vnode, |mut vn| vn.handler_sync_ack(self, from, m));
            }
            FabricMsg::SyncFin(m) => {
                vnode!(self, m.vnode, |mut vn| vn.handler_sync_fin(self, from, m));
            }
            msg @ _ => unreachable!("Can't handle {:?}", msg),
        }
    }

    fn syncs_inflight(&self) -> usize {
        self.vnodes
            .read()
            .unwrap()
            .values()
            .map(|vn| {
                let inf = vn.lock().unwrap().syncs_inflight();
                inf.0 + inf.1
            })
            .sum()
    }

    fn start_sync(&self, vnode: VNodeId) {
        let vnodes = self.vnodes.read().unwrap();
        let mut vnode = vnodes.get(&vnode).unwrap().lock().unwrap();
        vnode.start_sync(self);
    }

    // CLIENT CRUD
    pub fn set(&self, token: Token, key: &[u8], value: Option<&[u8]>, vv: VersionVector,
               consistency: ConsistencyLevel, reply_result: bool) {
        let vnode = self.dht.key_vnode(key);
        vnode!(self, vnode, |mut vn| {
            vn.do_set(self, token, key, value, vv, consistency, reply_result);
        });
    }

    pub fn get(&self, token: Token, key: &[u8], consistency: ConsistencyLevel) {
        let vnode = self.dht.key_vnode(key);
        vnode!(self, vnode, |mut vn| {
            vn.do_get(self, token, key, consistency);
        });
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // force dropping vnodes before other components
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
    use config;
    use types::ConsistencyLevel::*;

    fn sleep_ms(ms: u64) {
        use std::time::Duration;
        thread::sleep(Duration::from_millis(ms));
    }

    struct TestDatabase {
        db: Arc<Database>,
        responses: Arc<Mutex<HashMap<Token, RespValue>>>,
    }

    impl TestDatabase {
        fn new(fabric_addr: net::SocketAddr, data_dir: &str, create: bool) -> Self {
            let responses1 = Arc::new(Mutex::new(HashMap::new()));
            let responses2 = responses1.clone();
            let config = config::Config {
                data_dir: data_dir.into(),
                fabric_addr: fabric_addr,
                cluster_name: "test".into(),
                max_incomming_syncs: 0,
                cmd_init: if create {
                    Some(config::InitCommand {
                        replication_factor: 3,
                        partitions: 64,
                    })
                } else {
                    None
                },
                ..Default::default()
            };
            let db = Database::new(&config,
                                   Box::new(move |t, v| {
                                       info!("response for {}", t);
                                       let r = responses1.lock().unwrap().insert(t, v);
                                       assert!(r.is_none(), "replaced a result");
                                   }));
            TestDatabase {
                db: db,
                responses: responses2,
            }
        }

        fn resp_response(&self, token: Token) -> RespValue {
            (0..1000)
                .filter_map(|_| {
                    sleep_ms(10);
                    self.responses.lock().unwrap().remove(&token)
                })
                .next()
                .unwrap()
        }

        fn response(&self, token: Token) -> (VersionVector, Vec<Vec<u8>>) {
            decode_response(self.resp_response(token))
        }

        fn values_response(&self, token: Token) -> Vec<Vec<u8>> {
            self.response(token).1
        }

    }

    impl ops::Deref for TestDatabase {
        type Target = Database;
        fn deref(&self) -> &Self::Target {
            &self.db
        }
    }

    fn decode_response(value: RespValue) -> (VersionVector, Vec<Vec<u8>>) {
        if let RespValue::Array(ref arr) = value {
            let values: Vec<_> =  arr[0..arr.len() - 1]
                .iter()
                .map(|d| {
                    if let RespValue::Data(ref d) = *d {
                        d[..].to_owned()
                    } else {
                        panic!();
                    }
                })
                .collect();
            let vv = if let RespValue::Data(ref d) = arr[arr.len() -1] {
                bincode_serde::deserialize(&d[..]).unwrap()
            } else {
                panic!();
            };
            return (vv, values);
        }
        panic!("Invalid bincode data")
    }

    fn test_reload_stub(shutdown: bool) {
        let _ = fs::remove_dir_all("./t");
        let _ = env_logger::init();
        let mut db = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db", true);

        db.get(1, b"test", One);
        assert!(db.values_response(1).len() == 0);

        db.set(1, b"test", Some(b"value1"), VersionVector::new(), One, true);
        assert!(db.values_response(1).len() == 0);

        db.get(1, b"test", One);
        assert!(db.values_response(1).eq(&[b"value1"]));

        db.save(shutdown);
        drop(db);
        db = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db", false);

        db.get(1, b"test", One);
        assert!(db.values_response(1).eq(&[b"value1"]));

        assert_eq!(1,
                   db.vnodes
                       .read()
                       .unwrap()
                       .values()
                       .map(|vn| vn.lock().unwrap()._log_len(db.dht.node()))
                       .sum::<usize>());
    }

    #[test]
    fn test_reload_clearn_shutdown() {
        test_reload_stub(true);
    }

    #[test]
    fn test_reload_dirty_shutdown() {
        test_reload_stub(false);
    }

    #[test]
    fn test_one() {
        let _ = fs::remove_dir_all("./t");
        let _ = env_logger::init();
        let db = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db", true);
        db.get(1, b"test", One);
        assert!(db.values_response(1).len() == 0);

        db.set(1, b"test", Some(b"value1"), VersionVector::new(), One, true);
        assert!(db.values_response(1).len() == 0);

        db.get(1, b"test", One);
        assert!(db.values_response(1).eq(&[b"value1"]));

        db.set(1, b"test", Some(b"value2"), VersionVector::new(), One, true);
        assert!(db.values_response(1).len() == 0);

        db.get(1, b"test", One);
        let (vv, values) = db.response(1);
        assert!(values.eq(&[b"value1", b"value2"]));

        db.set(1, b"test", Some(b"value12"), vv, One, true);
        assert!(db.values_response(1).len() == 0);

        db.get(1, b"test", One);
        let (vv, values) = db.response(1);
        assert!(values.eq(&[b"value12"]));

        db.set(1, b"test", None, vv, One, true);
        assert!(db.values_response(1).len() == 0);

        db.get(1, b"test", One);
        assert!(db.values_response(1).len() == 0);
    }

    #[test]
    fn test_two() {
        let _ = fs::remove_dir_all("./t");
        let _ = env_logger::init();
        let db1 = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db1", true);
        let db2 = TestDatabase::new("127.0.0.1:9001".parse().unwrap(), "t/db2", false);
        db2.dht.rebalance();

        sleep_ms(1000);
        while db1.syncs_inflight() + db2.syncs_inflight() > 0 {
            warn!("waiting for syncs to finish");
            sleep_ms(1000);
        }

        db1.get(1, b"test", One);
        assert!(db1.values_response(1).len() == 0);

        db1.set(1, b"test", Some(b"value1"), VersionVector::new(), One, true);
        assert!(db1.values_response(1).len() == 0);

        for &db in &[&db1, &db2] {
            db.get(1, b"test", One);
            assert!(db.values_response(1).eq(&[b"value1"]));
        }
    }

    const TEST_JOIN_SIZE: u64 = 10;

    #[test]
    fn test_join_migrate() {
        let _ = fs::remove_dir_all("./t");
        let _ = env_logger::init();
        let db1 = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db1", true);
        for i in 0..TEST_JOIN_SIZE {
            db1.set(i,
                    i.to_string().as_bytes(),
                    Some(i.to_string().as_bytes()),
                    VersionVector::new(),
                    One,
                    true);
            db1.values_response(i);
        }

        let db2 = TestDatabase::new("127.0.0.1:9001".parse().unwrap(), "t/db2", false);
        warn!("will check data in db2 before balancing");
        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db1, &db2] {
                db.get(i, i.to_string().as_bytes(), One);
                assert!(db.values_response(i).eq(&[i.to_string().as_bytes()]));
            }
        }

        db2.dht.rebalance();

        warn!("will check data in both dbs during balancing");
        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db1, &db2] {
                db.get(i, i.to_string().as_bytes(), One);
                assert!(db.values_response(i).eq(&[i.to_string().as_bytes()]));
            }
        }

        sleep_ms(1000);
        while db1.syncs_inflight() + db2.syncs_inflight() > 0 {
            warn!("waiting for syncs to finish");
            sleep_ms(1000);
        }

        warn!("will check after balancing");
        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db1, &db2] {
                db.get(i, i.to_string().as_bytes(), One);
                assert!(db.values_response(i).eq(&[i.to_string().as_bytes()]));
            }
        }
    }

    #[test]
    #[ignore]
    fn test_join_sync_reverse() {
        let _ = fs::remove_dir_all("./t");
        let _ = env_logger::init();
        let mut db1 = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db1", true);
        let db2 = TestDatabase::new("127.0.0.1:9001".parse().unwrap(), "t/db2", false);
        db2.dht.rebalance();

        sleep_ms(1000);
        while db1.syncs_inflight() + db2.syncs_inflight() > 0 {
            warn!("waiting for syncs to finish");
            sleep_ms(1000);
        }

        for i in 0..TEST_JOIN_SIZE {
            db1.set(i,
                    i.to_string().as_bytes(),
                    Some(i.to_string().as_bytes()),
                    VersionVector::new(),
                    One,
                    true);
            db1.values_response(i);
        }
        // wait for all writes to be replicated
        sleep_ms(5);
        for i in 0..TEST_JOIN_SIZE {
            db1.get(i, i.to_string().as_bytes(), One);
            let result1 = db1.values_response(i);
            db2.get(i, i.to_string().as_bytes(), One);
            let result2 = db2.values_response(i);
            assert_eq!(result1, result2);
        }

        // sim unclean shutdown
        assert_ne!(db1.storage_manager.drop_buffer(), 0);
        drop(db1);
        db1 = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db1", false);

        {
            // during recover ASK is expected
            db1.set(0, b"k", None, VersionVector::new(), One, true);
            let response = format!("{:?}", db1.resp_response(0));
            assert!(response.starts_with("Error(\"ASK"), "{} is not an Error(\"ASK", response);
        }

        sleep_ms(1000);
        while db1.syncs_inflight() + db2.syncs_inflight() > 0 {
            warn!("waiting for syncs to finish");
            sleep_ms(1000);
        }

        warn!("will check after balancing");
        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db1, &db2] {
                db.get(i, i.to_string().as_bytes(), One);
                assert!(db.values_response(i).eq(&[i.to_string().as_bytes()]));
            }
        }
    }

    #[test]
    fn test_join_sync_normal() {
        let _ = fs::remove_dir_all("./t");
        let _ = env_logger::init();
        let db1 = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db1", true);
        let mut db2 = TestDatabase::new("127.0.0.1:9001".parse().unwrap(), "t/db2", false);
        db2.dht.rebalance();

        sleep_ms(1000);
        while db1.syncs_inflight() + db2.syncs_inflight() > 0 {
            warn!("waiting for syncs to finish");
            sleep_ms(1000);
        }

        // sim partition
        warn!("droping db2");
        drop(db2);

        for i in 0..TEST_JOIN_SIZE {
            db1.set(i,
                    i.to_string().as_bytes(),
                    Some(i.to_string().as_bytes()),
                    VersionVector::new(),
                    One,
                    true);
            db1.values_response(i);
        }
        for i in 0..TEST_JOIN_SIZE {
            db1.get(i, i.to_string().as_bytes(), One);
            assert_eq!(db1.values_response(i), vec![i.to_string().as_bytes()]);
        }

        // sim partition heal
        warn!("bringing back db2");
        db2 = TestDatabase::new("127.0.0.1:9001".parse().unwrap(), "t/db2", false);

        warn!("will check before sync");
        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db1, &db2] {
                db.get(i, i.to_string().as_bytes(), Quorum);
                assert!(db.values_response(i).eq(&[i.to_string().as_bytes()]));
            }
        }

        // force some syncs
        warn!("starting syncs");
        for i in 0..64u16 {
            db2.start_sync(i);
        }

        sleep_ms(1000);
        while db1.syncs_inflight() + db2.syncs_inflight() > 0 {
            warn!("waiting for syncs to finish");
            sleep_ms(1000);
        }

        warn!("will check after balancing");
        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db1, &db2] {
                db.get(i, i.to_string().as_bytes(), One);
                assert!(db.values_response(i).eq(&[i.to_string().as_bytes()]));
            }
        }
    }
}
