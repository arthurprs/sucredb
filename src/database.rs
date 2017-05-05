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
use metrics::{self, Gauge};
use vnode_sync::SyncDirection;

pub type DatabaseResponseFn = Box<Fn(Token, RespValue) + Send + Sync>;

#[derive(Default)]
struct Stats {
    incomming_syncs: u16,
    outgoing_syncs: u16,
}

pub struct Database {
    pub dht: DHT<net::SocketAddr>,
    pub fabric: Fabric,
    pub meta_storage: Storage,
    pub storage_manager: StorageManager,
    pub response_fn: DatabaseResponseFn,
    pub config: Config,
    stats: Mutex<Stats>,
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
        let meta_storage = storage_manager.open(u16::max_value(), true).unwrap();

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

        info!("Metadata loaded! node:{:?} old_node:{:?}", node, old_node);

        let fabric = Fabric::new(node, config.fabric_addr).unwrap();
        let dht = DHT::new(
            node,
            config.fabric_addr,
            &config.cluster_name,
            &config.etcd_addr,
            config.listen_addr,
            if let Some(init) = config.cmd_init.as_ref() {
                Some(dht::RingDescription::new(init.replication_factor, init.partitions))
            } else {
                None
            },
            old_node,
        );
        let workers = WorkerManager::new(
            node,
            config.worker_count as _,
            time::Duration::from_millis(config.worker_timer as _),
        );
        let db = Arc::new(
            Database {
                fabric: fabric,
                dht: dht,
                storage_manager: storage_manager,
                meta_storage: meta_storage,
                response_fn: response_fn,
                vnodes: Default::default(),
                workers: Mutex::new(workers),
                config: config.clone(),
                stats: Default::default(),
            },
        );

        db.workers
            .lock()
            .unwrap()
            .start(
                || {
                    let cdb = Arc::downgrade(&db);
                    Box::new(
                        move |chan| {
                            for wm in chan {
                                let db = if let Some(db) = cdb.upgrade() {
                                    db
                                } else {
                                    break;
                                };
                                trace!(
                                    "worker {} got msg {:?}",
                                    ::std::thread::current().name().unwrap(),
                                    wm
                                );
                                match wm {
                                    WorkerMsg::Fabric(from, m) => db.handler_fabric_msg(from, m),
                                    WorkerMsg::Tick(time) => db.handler_tick(time),
                                    WorkerMsg::Command(token, cmd) => db.handler_cmd(token, cmd),
                                    WorkerMsg::DHTChange => db.handler_dht_change(),
                                    WorkerMsg::Exit => break,
                                }
                            }
                            info!("Exiting worker")
                        },
                    )
                },
            );

        let mut dht_change_sender = db.sender();
        db.dht
            .set_callback(Box::new(move || { dht_change_sender.send(WorkerMsg::DHTChange); }));

        // register nodes into fabric
        db.fabric.set_nodes(db.dht.members().into_iter());
        // FIXME: fabric should have a start method that receives the callbacks
        // set fabric callbacks
        for &msg_type in &[FabricMsgType::Crud, FabricMsgType::Synch] {
            let mut sender = db.sender();
            let callback = Box::new(
                move |f, m| {
                    // trace!("fabric callback {:?} {:?}", msg_type, m);
                    sender.send(WorkerMsg::Fabric(f, m));
                },
            );
            db.fabric.register_msg_handler(msg_type, callback);
        }

        {
            // acquire exclusive lock to vnodes to initialize them
            let mut vnodes = db.vnodes.write().unwrap();
            let (ready_vnodes, pending_vnodes) = db.dht.vnodes_for_node(db.dht.node());
            // create vnodes
            // TODO: this can be done in parallel
            *vnodes = (0..db.dht.partitions() as VNodeId)
                .map(
                    |i| {
                        let vn = if ready_vnodes.contains(&i) {
                            VNode::new(&db, i, VNodeStatus::Ready)
                        } else if pending_vnodes.contains(&i) {
                            VNode::new(&db, i, VNodeStatus::Bootstrap)
                        } else {
                            VNode::new(&db, i, VNodeStatus::Absent)
                        };
                        (i, Mutex::new(vn))
                    },
                )
                .collect();
        }

        db
    }

    pub fn save(&self, shutdown: bool) {
        for vn in self.vnodes.read().unwrap().values() {
            vn.lock().unwrap().save(self, shutdown);
        }
        if shutdown {
            self.meta_storage.set(b"clean_shutdown", b"");
        }
        self.meta_storage.sync();
    }

    // Gets a Sender handle that allows sending work to the database worker pool
    // FIXME: leaky abstraction
    pub fn sender(&self) -> WorkerSender {
        self.workers.lock().unwrap().sender()
    }

    fn handler_dht_change(&self) {
        // register nodes
        self.fabric.set_nodes(self.dht.members().into_iter());

        for (&i, vn) in self.vnodes.read().unwrap().iter() {
            let final_status = if
                self.dht.nodes_for_vnode(i, true, true).contains(&self.dht.node()) {
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
        // auto start sync in random vnodes
        if self.config.sync_auto && incomming_syncs < self.config.sync_incomming_max as usize {
            let vnodes_len = vnodes.len() as u16;
            let rnd = thread_rng().gen::<u16>() % vnodes_len;
            for vnode in (0..vnodes_len).map(|i| vnodes.get(&((i + rnd) % vnodes_len))) {
                incomming_syncs += vnode.unwrap().lock().unwrap().start_sync_if_ready(self) as _;
                if incomming_syncs >= self.config.sync_incomming_max as usize {
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
            .map(
                |vn| {
                    let inf = vn.lock().unwrap().syncs_inflight();
                    inf.0 + inf.1
                },
            )
            .sum()
    }

    #[cfg(test)]
    fn _start_sync(&self, vnode: VNodeId) -> bool {
        let vnodes = self.vnodes.read().unwrap();
        let mut vnode = vnodes.get(&vnode).unwrap().lock().unwrap();
        vnode._start_sync(self)
    }

    pub fn signal_sync_start(&self, direction: SyncDirection) -> bool {
        let mut stats = self.stats.lock().unwrap();
        match direction {
            SyncDirection::Incomming => {
                if stats.incomming_syncs < self.config.sync_incomming_max {
                    stats.incomming_syncs += 1;
                    metrics::SYNC_INCOMING.inc();
                    true
                } else {
                    false
                }
            }
            SyncDirection::Outgoing => {
                if stats.outgoing_syncs < self.config.sync_outgoing_max {
                    stats.outgoing_syncs += 1;
                    metrics::SYNC_OUTGOING.inc();
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn signal_sync_end(&self, direction: SyncDirection) {
        let mut stats = self.stats.lock().unwrap();
        match direction {
            SyncDirection::Incomming => {
                stats.incomming_syncs = stats.incomming_syncs.checked_sub(1).unwrap();
                metrics::SYNC_INCOMING.dec();
            }
            SyncDirection::Outgoing => {
                stats.outgoing_syncs = stats.outgoing_syncs.checked_sub(1).unwrap();
                metrics::SYNC_OUTGOING.dec();
            }
        }
    }

    // CLIENT CRUD
    pub fn set(
        &self, token: Token, key: &[u8], value: Option<&[u8]>, vv: VersionVector,
        consistency: ConsistencyLevel, reply_result: bool
    ) {
        let vnode = self.dht.key_vnode(key);
        vnode!(
            self,
            vnode,
            |mut vn| { vn.do_set(self, token, key, value, vv, consistency, reply_result); }
        );
    }

    pub fn get(&self, token: Token, key: &[u8], consistency: ConsistencyLevel) {
        let vnode = self.dht.key_vnode(key);
        vnode!(self, vnode, |mut vn| { vn.do_get(self, token, key, consistency); });
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
    use version_vector::VersionVector;
    use env_logger;
    use bincode;
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
                sync_incomming_max: 10,
                sync_outgoing_max: 10,
                sync_auto: false,
                cmd_init: if create {
                    Some(
                        config::InitCommand {
                            replication_factor: 3,
                            partitions: 64,
                        },
                    )
                } else {
                    None
                },
                ..Default::default()
            };
            let db = Database::new(
                &config,
                Box::new(
                    move |t, v| {
                        info!("response for {}", t);
                        let r = responses1.lock().unwrap().insert(t, v);
                        assert!(r.is_none(), "replaced a result");
                    },
                ),
            );
            TestDatabase {
                db: db,
                responses: responses2,
            }
        }

        fn force_syncs(&self) {
            for i in 0..64u16 {
                while !self._start_sync(i) {
                    sleep_ms(200);
                }
            }
            self.wait_syncs();
        }

        fn wait_syncs(&self) {
            sleep_ms(200);
            while self.syncs_inflight() != 0 {
                //warn!("waiting for syncs to finish");
                sleep_ms(200);
            }
        }

        fn resp_response(&self, token: Token) -> RespValue {
            (0..1000)
                .filter_map(
                    |_| {
                        sleep_ms(10);
                        self.responses.lock().unwrap().remove(&token)
                    },
                )
                .next()
                .unwrap()
        }

        fn response(&self, token: Token) -> (Vec<Vec<u8>>, VersionVector) {
            decode_response(self.resp_response(token))
        }

        fn values_response(&self, token: Token) -> Vec<Vec<u8>> {
            self.response(token).0
        }
    }

    impl ops::Deref for TestDatabase {
        type Target = Database;
        fn deref(&self) -> &Self::Target {
            &self.db
        }
    }

    fn decode_response(value: RespValue) -> (Vec<Vec<u8>>, VersionVector) {
        if let RespValue::Array(ref arr) = value {
            let values: Vec<_> = arr[0..arr.len() - 1]
                .iter()
                .map(
                    |d| if let RespValue::Data(ref d) = *d {
                        d[..].to_owned()
                    } else {
                        panic!();
                    },
                )
                .collect();
            let vv = if let RespValue::Data(ref d) = arr[arr.len() - 1] {
                bincode::deserialize(&d[..]).unwrap()
            } else {
                panic!();
            };
            return (values, vv);
        }
        panic!("Can't decode response {:?}", value);
    }

    fn test_reload_stub(shutdown: bool) {
        let _ = fs::remove_dir_all("./t");
        let _ = env_logger::init();
        let mut db = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db", true);
        let prev_node = db.dht.node();
        db.get(1, b"test", One);
        assert!(db.values_response(1).len() == 0);

        db.set(1, b"test", Some(b"value1"), VersionVector::new(), One, true);
        assert!(db.values_response(1).len() == 1);

        db.get(1, b"test", One);
        assert!(db.values_response(1).eq(&[b"value1"]));

        db.save(shutdown);
        drop(db);
        db = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db", false);

        db.get(1, b"test", One);
        assert!(db.values_response(1).eq(&[b"value1"]));

        if shutdown {
            assert_eq!(db.dht.node(), prev_node);
        } else {
            assert_ne!(db.dht.node(), prev_node);
        }

        assert_eq!(
            1,
            db.vnodes
                .read()
                .unwrap()
                .values()
                .map(|vn| vn.lock().unwrap()._log_len(prev_node))
                .sum::<usize>()
        );
    }

    #[test]
    fn test_reload_clean_shutdown() {
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
        assert!(db.values_response(1).len() == 1);

        db.get(1, b"test", One);
        assert!(db.values_response(1).eq(&[b"value1"]));

        db.set(1, b"test", Some(b"value2"), VersionVector::new(), One, true);
        assert!(db.values_response(1).len() == 2);

        db.get(1, b"test", One);
        let (values, vv) = db.response(1);
        assert!(values.eq(&[b"value1", b"value2"]));

        db.set(1, b"test", Some(b"value12"), vv, One, true);
        assert!(db.values_response(1).len() == 1);

        db.get(1, b"test", One);
        let (values, vv) = db.response(1);
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
        assert!(db1.values_response(1).len() == 1);

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
            db1.set(
                i,
                i.to_string().as_bytes(),
                Some(i.to_string().as_bytes()),
                VersionVector::new(),
                One,
                true,
            );
            db1.values_response(i);
        }

        let db2 = TestDatabase::new("127.0.0.1:9001".parse().unwrap(), "t/db2", false);
        warn!("will check data before balancing");
        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db1, &db2] {
                db.get(i, i.to_string().as_bytes(), One);
                assert!(db.values_response(i).eq(&[i.to_string().as_bytes()]));
            }
        }

        db2.dht.rebalance();

        warn!("will check data during balancing");
        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db1, &db2] {
                db.get(i, i.to_string().as_bytes(), One);
                assert!(db.values_response(i).eq(&[i.to_string().as_bytes()]));
            }
        }

        db1.wait_syncs();
        db2.wait_syncs();

        warn!("will check data after balancing");
        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db1, &db2] {
                db.get(i, i.to_string().as_bytes(), One);
                assert!(db.values_response(i).eq(&[i.to_string().as_bytes()]));
            }
        }
    }

    #[test]
    fn test_join_sync_recover() {
        let _ = fs::remove_dir_all("./t");
        let _ = env_logger::init();
        let mut db1 = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db1", true);
        let db2 = TestDatabase::new("127.0.0.1:9001".parse().unwrap(), "t/db2", false);
        db2.dht.rebalance();

        warn!("wait for rebalance");
        db1.wait_syncs();
        db2.wait_syncs();

        warn!("inserting test data");
        for i in 0..TEST_JOIN_SIZE {
            db1.set(
                i,
                i.to_string().as_bytes(),
                Some(i.to_string().as_bytes()),
                VersionVector::new(),
                One,
                true,
            );
            db1.values_response(i);
        }
        // wait for all writes to be replicated
        sleep_ms(5);
        warn!("check before sync");
        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db1, &db2] {
                db.get(i, i.to_string().as_bytes(), One);
                assert!(db.values_response(i).eq(&[i.to_string().as_bytes()]));
            }
        }

        // sim unclean shutdown
        warn!("killing db1");
        drop(db1);
        db1 = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db1", false);

        // {
        //     // during recover ASK is expected
        //     db1.set(0, b"k", None, VersionVector::new(), One, true);
        //     let response = format!("{:?}", db1.resp_response(0));
        //     assert!(response.starts_with("Error(\"ASK"), "{} is not an Error(\"ASK", response);
        // }

        // force some syncs
        warn!("starting syncs");
        db1.force_syncs();

        warn!("will check after sync");
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
            db1.set(
                i,
                i.to_string().as_bytes(),
                Some(i.to_string().as_bytes()),
                VersionVector::new(),
                One,
                true,
            );
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
        db2.force_syncs();

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
