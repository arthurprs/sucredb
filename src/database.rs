use bytes::Bytes;
use command::CommandError;
use config::Config;
use cubes::*;
use dht::{RingDescription, DHT};
use fabric::*;
use metrics::{self, Gauge};
use rand::{thread_rng, Rng};
use resp::RespValue;
use std::sync::{Arc, Mutex, RwLock};
use std::{net, time};
use storage::{Storage, StorageManager};
pub use types::*;
use utils::LoggerExt;
use utils::{assume_str, is_dir_empty_or_absent, join_u64, replace_default, split_u64, IdHashMap};
use version_vector::Version;
use vnode::*;
use vnode_sync::SyncDirection;
use workers::*;

// require sync as it can be called from any worker thread
pub type DatabaseResponseFn = Box<Fn(Context) + Send + Sync>;

#[derive(Default)]
struct Stats {
    incomming_syncs: u16,
    outgoing_syncs: u16,
}

pub struct ContextRead {
    pub cube: Cube,
    // first key contains the render_fn for all keys
    pub response: Option<ResponseFn>,
}

pub struct ContextWrite {
    pub version: Version,
    pub mutator_fn: Option<MutatorFn>,
    pub key: Bytes,
    pub cube: Cube,
    pub reply_result: bool,
    pub response: Option<RespValue>,
    pub response_fn: Option<ResponseFn>,
}

#[derive(Default)]
pub struct Context {
    pub token: Token,
    pub is_multi: bool,
    pub is_exec: bool,
    // response queue
    // if not multi the first element is threated as a standalone response
    // if multi it contains an array response
    pub response: Vec<RespValue>,
    // commands queue
    // if multi it may contain pending commands
    // the latest command is always at the back
    pub commands: Vec<RespValue>,
    pub reads: Vec<ContextRead>,
    pub writes: Vec<ContextWrite>,
}

impl Context {
    pub fn new(token: Token) -> Self {
        Context {
            token,
            is_multi: false,
            is_exec: false,
            response: Default::default(),
            commands: Default::default(),
            writes: Default::default(),
            reads: Default::default(),
        }
    }

    pub fn take_response(&mut self) -> RespValue {
        if self.is_exec {
            self.is_multi = false;
            self.is_exec = false;
            RespValue::Array(replace_default(&mut self.response))
        } else {
            self.response.pop().unwrap()
        }
    }

    pub fn respond(&mut self, response: RespValue) {
        self.response.push(response);
    }

    pub fn clear(&mut self) {
        self.is_multi = false;
        self.is_exec = false;
        self.response.clear();
        self.commands.clear();
        self.reads.clear();
        self.writes.clear();
    }
}

// TODO: some things to investigate
// track deleted keys pending physical deletion (is this a good idea? maybe a compaction filter?)
// pruning old nodes from node clocks (is it possible?)
// inner vnode parallelism
// zombie vnodes need to eagerly sync with other nodes
// track dead peers with the fabric or gossip and use that info

pub struct Database {
    pub dht: DHT<net::SocketAddr>,
    pub fabric: Arc<Fabric>,
    pub meta_storage: Storage,
    pub storage_manager: StorageManager,
    pub response_fn: DatabaseResponseFn,
    pub config: Config,
    stats: Mutex<Stats>,
    vnodes: RwLock<IdHashMap<VNodeNo, Mutex<VNode>>>,
    workers: Mutex<WorkerManager>,
}

macro_rules! fabric_send_error {
    ($db:expr, $to:expr, $msg:expr, $emsg:ident, $err:expr) => {
        $db.fabric.send_msg(
            $to,
            &$emsg {
                vnode: $msg.vnode,
                cookie: $msg.cookie,
                result: Err($err),
            },
        )
    };
    ($db:expr, $to:expr, $vnode:expr, $cookie:expr, $emsg:ident, $err:expr) => {
        $db.fabric.send_msg(
            $to,
            *$emsg {
                vnode: $vnode,
                cookie: $cookie,
                result: Err($err),
            },
        )
    };
}

macro_rules! vnode {
    ($s:expr, $k:expr, $ok:expr) => {{
        let vnodes = $s.vnodes.read().unwrap();
        let mut locked_vnode = vnodes[&$k].lock().unwrap();
        Some(&mut *locked_vnode).map($ok).unwrap()
    }};
}

impl Database {
    pub fn new(config: &Config, response_fn: DatabaseResponseFn) -> Arc<Database> {
        info!("Initializing database");
        if config.cmd_init.is_some()
            && !is_dir_empty_or_absent(&config.data_dir).expect("Failed to open data dir")
        {
            panic!("Can't init cluster when data directory isn't clean");
        }

        let storage_manager =
            StorageManager::new(&config.data_dir).expect("Failed to create storage manager");
        let meta_storage = storage_manager
            .open(u16::max_value())
            .expect("Can't open storage");
        let meta_node = meta_storage
            .get_vec(b"node")
            .expect("Can't read node id from storage");
        let meta_cluster = meta_storage
            .get_vec(b"cluster")
            .expect("Can't read cluster name from storage");
        let meta_clean_shutdown = meta_storage
            .get_vec(b"clean_shutdown")
            .expect("Can't read shutdown flag from storage");
        let meta_ring = meta_storage
            .get_vec(b"ring")
            .expect("Can't read previous ring from storage");

        let (old_node, node) = if let Some(s_node) = meta_node {
            let prev_node: NodeId = String::from_utf8(s_node).unwrap().parse().unwrap();
            if meta_clean_shutdown.is_some() {
                (None, prev_node)
            } else {
                let node = join_u64(split_u64(prev_node).0, thread_rng().gen());
                (Some(prev_node), node)
            }
        } else {
            (None, thread_rng().gen::<i64>().abs() as NodeId)
        };
        if let Some(cluster_in_storage) = meta_cluster {
            if cluster_in_storage != config.cluster_name.as_bytes() {
                panic!(
                    "Cluster name differs! Expected `{}` got `{}`",
                    config.cluster_name,
                    assume_str(&cluster_in_storage)
                );
            };
        }
        // save init (1 of 2)
        meta_storage
            .del(b"clean_shutdown")
            .expect("Can't delete shutdown flag");
        meta_storage
            .set(b"cluster", config.cluster_name.as_bytes())
            .expect("Can't save cluster name");
        meta_storage
            .set(b"node", node.to_string().as_bytes())
            .expect("Can't save node id");
        meta_storage.sync().expect("Can't sync storage");

        info!("Metadata loaded! node_id:{} previous:{:?}", node, old_node);

        let fabric = Arc::new(Fabric::new(node, config).unwrap());

        let dht = if let Some(init) = config.cmd_init.as_ref() {
            DHT::init(
                fabric.clone(),
                config,
                config.listen_addr,
                RingDescription::new(init.replication_factor, init.partitions),
                old_node,
            ).expect("Can't init cluster")
        } else if let Some(saved_ring) = meta_ring {
            DHT::restore(
                fabric.clone(),
                config,
                config.listen_addr,
                &saved_ring,
                old_node,
            ).expect("Can't restore cluster")
        } else {
            DHT::join_cluster(
                fabric.clone(),
                config,
                config.listen_addr,
                &config.seed_nodes,
                old_node,
            ).expect("Can't join cluster")
        };

        // save init (2 of 2)
        meta_storage
            .set(b"ring", &dht.save_ring())
            .expect("Can't save ring");
        meta_storage.sync().expect("Can't sync storage");

        let workers = WorkerManager::new(
            node,
            config.worker_count as _,
            time::Duration::from_millis(config.worker_timer as _),
        );

        let db = Arc::new(Database {
            fabric: fabric,
            dht: dht,
            storage_manager: storage_manager,
            meta_storage: meta_storage,
            response_fn: response_fn,
            vnodes: Default::default(),
            workers: Mutex::new(workers),
            config: config.clone(),
            stats: Default::default(),
        });

        db.workers.lock().unwrap().start(|| {
            let cdb = Arc::downgrade(&db);
            Box::new(move |chan| {
                for wm in chan {
                    let db = if let Some(db) = cdb.upgrade() {
                        db
                    } else {
                        break;
                    };
                    match wm {
                        WorkerMsg::Fabric(from, m) => db.handler_fabric_msg(from, m),
                        WorkerMsg::Command(context) => db.handler_cmd(context),
                        WorkerMsg::Tick(time) => db.handler_tick(time),
                        WorkerMsg::DHTFabric(from, m) => db.dht.handler_fabric_msg(from, m),
                        WorkerMsg::DHTChange => db.handler_dht_change(),
                        WorkerMsg::Exit => break,
                    }
                }
            })
        });

        // register dht nodes into fabric
        db.fabric.set_nodes(db.dht.members().into_iter());
        // fabric dht messages
        let mut sender = db.sender();
        let callback = move |f, m| {
            sender.send(WorkerMsg::DHTFabric(f, m));
        };
        db.fabric
            .register_msg_handler(FabricMsgType::DHT, Box::new(callback));

        // setup dht change callback
        let sender = Mutex::new(db.sender());
        let callback = move || {
            sender.lock().unwrap().send(WorkerMsg::DHTChange);
        };
        db.dht.set_callback(Box::new(callback));

        // other types of fabric msgs
        for &msg_type in &[FabricMsgType::Crud, FabricMsgType::Synch] {
            let mut sender = db.sender();
            let callback = move |f, m| {
                sender.send(WorkerMsg::Fabric(f, m));
            };
            db.fabric.register_msg_handler(msg_type, Box::new(callback));
        }

        // create vnodes
        {
            // acquire exclusive lock to vnodes to initialize them
            let mut vnodes = db.vnodes.write().unwrap();
            let (ready_vnodes, pending_vnodes) = db.dht.vnodes_for_node(db.dht.node());
            // TODO: this can be done in parallel
            *vnodes = (0..db.dht.partitions() as VNodeNo)
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
        if shutdown {
            self.meta_storage
                .set(b"clean_shutdown", b"")
                .expect("Can't save shutdown flag");
        }
        self.meta_storage.sync().expect("Can't sync storage");
    }

    // Gets a Sender handle that allows sending work to the database worker pool
    pub fn sender(&self) -> WorkerSender {
        self.workers.lock().unwrap().sender()
    }

    fn handler_dht_change(&self) {
        // save dht
        self.meta_storage
            .set(b"ring", &self.dht.save_ring())
            .log_error("Can't save ring on dht change");

        // register nodes
        self.fabric.set_nodes(self.dht.members().into_iter());

        for (&i, vn) in self.vnodes.read().unwrap().iter() {
            let final_status = if self.dht
                .nodes_for_vnode(i, true, true)
                .contains(&self.dht.node())
            {
                VNodeStatus::Ready
            } else {
                VNodeStatus::Absent
            };
            vn.lock().unwrap().handler_dht_change(self, final_status);
        }
    }

    fn handler_tick(&self, time: time::Instant) {
        self.dht.handler_tick(time);

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
                incomming_syncs +=
                    vnode.unwrap().lock().unwrap().start_sync_if_ready(self) as usize;
                if incomming_syncs >= self.config.sync_incomming_max as usize {
                    break;
                }
            }
        }
    }

    fn handler_fabric_msg(&self, from: NodeId, msg: FabricMsg) {
        match msg {
            FabricMsg::RemoteGet(m) => {
                vnode!(self, m.vnode, |vn| vn.handler_get_remote(self, from, m));
            }
            FabricMsg::RemoteGetAck(m) => {
                vnode!(self, m.vnode, |vn| vn.handler_get_remote_ack(self, from, m));
            }
            FabricMsg::RemoteSet(m) => {
                vnode!(self, m.vnode, |vn| vn.handler_set_remote(self, from, m));
            }
            FabricMsg::RemoteSetAck(m) => {
                vnode!(self, m.vnode, |vn| vn.handler_set_remote_ack(self, from, m));
            }
            FabricMsg::SyncStart(m) => {
                vnode!(self, m.vnode, |vn| vn.handler_sync_start(self, from, m));
            }
            FabricMsg::SyncSend(m) => {
                vnode!(self, m.vnode, |vn| vn.handler_sync_send(self, from, m));
            }
            FabricMsg::SyncAck(m) => {
                vnode!(self, m.vnode, |vn| vn.handler_sync_ack(self, from, m));
            }
            FabricMsg::SyncFin(m) => {
                vnode!(self, m.vnode, |vn| vn.handler_sync_fin(self, from, m));
            }
            msg => unreachable!("Can't handle {:?}", msg),
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

    #[cfg(test)]
    fn _start_sync(&self, vnode: VNodeNo) -> bool {
        let vnodes = self.vnodes.read().unwrap();
        let mut vnode = vnodes.get(&vnode).unwrap().lock().unwrap();
        vnode._start_sync(self)
    }

    pub fn signal_sync_start(&self, direction: SyncDirection) -> bool {
        let mut stats = self.stats.lock().unwrap();
        match direction {
            SyncDirection::Incomming => if stats.incomming_syncs < self.config.sync_incomming_max {
                stats.incomming_syncs += 1;
                metrics::SYNC_INCOMING.inc();
                true
            } else {
                false
            },
            SyncDirection::Outgoing => if stats.outgoing_syncs < self.config.sync_outgoing_max {
                stats.outgoing_syncs += 1;
                metrics::SYNC_OUTGOING.inc();
                true
            } else {
                false
            },
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
    pub fn set_flush(
        &self,
        context: &mut Context,
        consistency: ConsistencyLevel,
    ) -> Result<(), CommandError> {
        debug_assert!(context.is_multi && context.is_exec);
        let mut multi_vnode = None;
        for (wi, w) in context.writes.iter().enumerate() {
            // vnode can't changes mid way a batch we need to error
            let write_vnode = self.dht.key_vnode(&w.key);
            if multi_vnode.is_none() {
                multi_vnode = Some(write_vnode);
            } else if multi_vnode != Some(write_vnode) {
                return Err(CommandError::MultiplePartitions);
            }

            // make sure key doesn't appear in the rest of the commands
            for w2 in &context.writes[wi + 1..] {
                if w.key == w2.key {
                    return Err(CommandError::MultipleKeyMutations);
                }
            }
        }

        if let Some(vnode) = multi_vnode {
            vnode!(self, vnode, |vn| vn.do_flush(self, context, consistency))
        } else {
            Ok(self.respond_resp(context, RespValue::Array(Default::default())))
        }
    }

    pub fn set(
        &self,
        context: &mut Context,
        key: &Bytes,
        mutator_fn: MutatorFn,
        consistency: ConsistencyLevel,
        reply_result: bool,
        response_fn: Option<ResponseFn>,
    ) -> Result<(), CommandError> {
        debug_assert!(!context.is_exec);
        context.writes.push(ContextWrite {
            version: 0,
            mutator_fn: Some(mutator_fn),
            key: key.clone(),
            cube: Default::default(),
            reply_result: reply_result,
            response: None,
            response_fn: response_fn,
        });

        if context.is_multi {
            Ok(())
        } else {
            debug_assert_eq!(context.writes.len(), 1);
            let vnode = self.dht.key_vnode(key);
            vnode!(self, vnode, |vn| vn.do_flush(self, context, consistency))
        }
    }

    pub fn get(
        &self,
        context: &mut Context,
        key: &Bytes,
        consistency: ConsistencyLevel,
        response_fn: ResponseFn,
    ) -> Result<(), CommandError> {
        debug_assert!(!context.is_multi && !context.is_exec);
        let vnode = self.dht.key_vnode(key);
        vnode!(self, vnode, |vn| vn.do_get(
            self,
            context,
            &[key],
            consistency,
            response_fn
        ))
    }

    pub fn mget(
        &self,
        context: &mut Context,
        keys: &[&Bytes],
        consistency: ConsistencyLevel,
        response_fn: ResponseFn,
    ) -> Result<(), CommandError> {
        debug_assert!(context.is_multi && context.is_exec);
        let mut multi_vnode = None;
        for key in keys {
            let write_vnode = self.dht.key_vnode(key);
            if multi_vnode.is_none() {
                multi_vnode = Some(write_vnode);
            } else if multi_vnode != Some(write_vnode) {
                return Err(CommandError::MultiplePartitions);
            }
        }
        if let Some(vnode) = multi_vnode {
            vnode!(self, vnode, |vn| vn.do_get(
                self,
                context,
                keys,
                consistency,
                response_fn
            ))
        } else {
            Ok(self.respond_resp(context, RespValue::Array(Default::default())))
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        debug!("Droping database");
        // force dropping vnodes before other components
        let _ = self.vnodes.write().map(|mut vns| vns.clear());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode;
    use config;
    use env_logger;
    use resp::RespValue;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::{fs, net, ops};
    use utils::sleep_ms;
    use version_vector::VersionVector;

    #[allow(non_upper_case_globals)]
    const One: &[u8] = b"One";
    #[allow(non_upper_case_globals)]
    const Quorum: &[u8] = b"Quorum";
    #[allow(non_upper_case_globals)]
    const All: &[u8] = b"All";

    struct TestDatabase {
        db: Arc<Database>,
        responses: Arc<Mutex<HashMap<Token, RespValue>>>,
    }

    const PARTITIONS: usize = 64;

    impl TestDatabase {
        fn new(fabric_addr: net::SocketAddr, data_dir: &str, create: bool) -> Self {
            let responses1 = Arc::new(Mutex::new(HashMap::new()));
            let responses2 = responses1.clone();
            let config = config::Config {
                data_dir: data_dir.into(),
                fabric_addr: fabric_addr,
                cluster_name: "test".into(),
                sync_incomming_max: 100,
                sync_outgoing_max: 100,
                sync_auto: false,
                cmd_init: if create {
                    Some(config::InitCommand {
                        replication_factor: 3,
                        partitions: PARTITIONS as _,
                    })
                } else {
                    None
                },
                seed_nodes: vec!["127.0.0.1:9000".parse().unwrap()],
                ..Default::default()
            };
            let db = Database::new(
                &config,
                Box::new(move |mut ctx| {
                    info!("response for {}", ctx.token);
                    let r = responses1
                        .lock()
                        .unwrap()
                        .insert(ctx.token, ctx.take_response());
                    assert!(r.is_none(), "replaced a result");
                }),
            );
            TestDatabase {
                db: db,
                responses: responses2,
            }
        }

        fn force_syncs(&self) {
            for i in 0..PARTITIONS as u16 {
                while !self._start_sync(i) {
                    sleep_ms(1);
                }
            }
            self.wait_syncs();
        }

        fn wait_syncs(&self) {
            // wait a bit so syncs can actually start after requested
            sleep_ms(200);
            while self.syncs_inflight() != 0 {
                //warn!("waiting for syncs to finish");
                sleep_ms(1);
            }
        }

        fn response_resp(&self, token: Token) -> RespValue {
            (0..1000)
                .filter_map(|_| {
                    sleep_ms(1);
                    self.responses.lock().unwrap().remove(&token)
                })
                .next()
                .unwrap()
        }

        fn response_values(&self, token: Token) -> (Vec<Vec<u8>>, VersionVector) {
            decode_values(self.response_resp(token))
        }

        fn do_cmd(&self, token: Token, args: &[&[u8]]) {
            let mut context = Context::new(token);
            context.commands.push(RespValue::Array(
                args.iter().map(|&x| RespValue::Data(x.into())).collect(),
            ));
            self.handler_cmd(context)
        }
    }

    impl ops::Deref for TestDatabase {
        type Target = Database;
        fn deref(&self) -> &Self::Target {
            &self.db
        }
    }

    fn decode_values(value: RespValue) -> (Vec<Vec<u8>>, VersionVector) {
        if let RespValue::Array(ref arr) = value {
            let mut values: Vec<_> = arr[0..arr.len() - 1]
                .iter()
                .map(|d| {
                    if let RespValue::Data(ref d) = *d {
                        d[..].to_owned()
                    } else {
                        panic!("cant decode values from {:?}", value);
                    }
                })
                .collect();
            // sort so we have a deterministic sibling order for tests
            values.sort();
            let vv = if let RespValue::Data(ref d) = arr[arr.len() - 1] {
                bincode::deserialize(&d[..]).unwrap()
            } else {
                panic!("cant decode values from {:?}", value);
            };
            return (values, vv);
        }
        panic!("Can't decode response {:?}", value);
    }

    fn encode_vv(vv: &VersionVector) -> Vec<u8> {
        bincode::serialize(vv).unwrap()
    }

    fn test_reload_stub(shutdown: bool) {
        let _ = fs::remove_dir_all("t/");
        let _ = env_logger::try_init();
        let mut db = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db", true);
        let prev_node = db.dht.node();
        db.do_cmd(1, &[b"GET", b"test", One]);
        assert!(db.response_values(1).0.len() == 0);

        db.do_cmd(1, &[b"GETSET", b"test", b"value1", b"", One]);
        assert!(db.response_values(1).0.len() == 1);

        db.do_cmd(1, &[b"GET", b"test", One]);
        assert_eq!(db.response_values(1).0, [b"value1"]);

        db.save(shutdown);
        drop(db);
        db = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db", false);

        db.do_cmd(1, &[b"GET", b"test", One]);
        assert_eq!(db.response_values(1).0, [b"value1"]);

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
        let _ = fs::remove_dir_all("t/");
        let _ = env_logger::try_init();
        let db = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db", true);

        db.do_cmd(1, &[b"GET", b"test", One]);
        assert_eq!(db.response_values(1).0.len(), 0);

        db.do_cmd(1, &[b"GETSET", b"test", b"value1", b"", One]);
        assert_eq!(db.response_values(1).0.len(), 1);

        db.do_cmd(1, &[b"GET", b"test", One]);
        assert_eq!(db.response_values(1).0, [b"value1"]);

        db.do_cmd(1, &[b"GETSET", b"test", b"value2", b"", One]);
        assert_eq!(db.response_values(1).0.len(), 2);

        db.do_cmd(1, &[b"GET", b"test", One]);
        let (values, vv) = db.response_values(1);
        assert_eq!(values, [b"value1", b"value2"]);

        db.do_cmd(1, &[b"GETSET", b"test", b"value12", &encode_vv(&vv), One]);
        assert_eq!(db.response_values(1).0.len(), 1);

        db.do_cmd(1, &[b"GET", b"test", One]);
        let (values, vv) = db.response_values(1);
        assert_eq!(values, [b"value12"]);

        db.do_cmd(1, &[b"DEL", b"test", &encode_vv(&vv), One]);
        assert_eq!(db.response_resp(1), RespValue::Int(1));

        db.do_cmd(1, &[b"GET", b"test", One]);
        assert_eq!(db.response_values(1).0.len(), 0);
    }

    #[test]
    fn test_two() {
        let _ = fs::remove_dir_all("t/");
        let _ = env_logger::try_init();
        let db1 = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db1", true);
        let db2 = TestDatabase::new("127.0.0.1:9001".parse().unwrap(), "t/db2", false);
        db2.dht.rebalance().unwrap();

        db1.wait_syncs();
        db2.wait_syncs();

        db1.do_cmd(1, &[b"GET", b"test", One]);
        assert_eq!(db1.response_values(1).0.len(), 0);

        db1.do_cmd(1, &[b"GETSET", b"test", b"value1", b"", All]);
        assert_eq!(db1.response_values(1).0, &[b"value1"]);

        for &db in &[&db1, &db2] {
            db.do_cmd(1, &[b"GET", b"test", One]);
            assert_eq!(db.response_values(1).0, [b"value1"]);
        }
    }

    const TEST_JOIN_SIZE: u64 = 100;

    #[test]
    fn test_bootstrap() {
        let _ = fs::remove_dir_all("t/");
        let _ = env_logger::try_init();
        let db1 = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db1", true);
        for i in 0..TEST_JOIN_SIZE {
            db1.do_cmd(
                i,
                &[
                    b"GETSET",
                    i.to_string().as_bytes(),
                    i.to_string().as_bytes(),
                    b"",
                    One,
                ],
            );
            db1.response_values(i);
        }

        // boostrap node 2 from 1
        let db2 = TestDatabase::new("127.0.0.1:9001".parse().unwrap(), "t/db2", false);
        warn!("will check data before balancing");
        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db1, &db2] {
                db.do_cmd(i, &[b"GET", i.to_string().as_bytes(), One]);
                assert_eq!(db.response_values(i).0, [i.to_string().as_bytes()]);
            }
        }

        db2.dht.rebalance().unwrap();

        warn!("will check data during balancing");
        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db1, &db2] {
                db.do_cmd(i, &[b"GET", i.to_string().as_bytes(), One]);
                assert_eq!(db.response_values(i).0, [i.to_string().as_bytes()]);
            }
        }

        db2.wait_syncs();

        warn!("will check data after balancing");
        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db1, &db2] {
                db.do_cmd(i, &[b"GET", i.to_string().as_bytes(), One]);
                assert_eq!(db.response_values(i).0, [i.to_string().as_bytes()]);
            }
        }

        // remove first node
        db2.dht.remove_node(db1.dht.node()).unwrap();
        sleep_ms(100);

        let db3 = TestDatabase::new("127.0.0.1:9002".parse().unwrap(), "t/db3", false);

        db3.dht.rebalance().unwrap();
        db3.wait_syncs();

        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db2, &db3] {
                db.do_cmd(i, &[b"GET", i.to_string().as_bytes(), One]);
                assert_eq!(db.response_values(i).0, [i.to_string().as_bytes()]);
            }
        }
    }

    #[test]
    fn test_bootstrap_2() {
        // similar to the previous, but values in n1 are rewritten + sibling
        let _ = fs::remove_dir_all("t/");
        let _ = env_logger::try_init();
        let db1 = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db1", true);
        for i in 0..TEST_JOIN_SIZE {
            db1.do_cmd(i, &[b"GETSET", i.to_string().as_bytes(), b"", b"", One]);
            let (_, vv) = db1.response_values(i);

            // rewrite key
            db1.do_cmd(
                i,
                &[
                    b"GETSET",
                    i.to_string().as_bytes(),
                    i.to_string().as_bytes(),
                    &encode_vv(&vv),
                    One,
                ],
            );
            db1.response_values(i);

            // add sibling
            db1.do_cmd(
                i,
                &[
                    b"GETSET",
                    i.to_string().as_bytes(),
                    i.to_string().as_bytes(),
                    &encode_vv(&vv),
                    One,
                ],
            );
            db1.response_values(i);
        }

        // boostrap node 2 from 1
        let db2 = TestDatabase::new("127.0.0.1:9001".parse().unwrap(), "t/db2", false);

        db2.dht.rebalance().unwrap();
        db2.wait_syncs();

        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db1, &db2] {
                db.do_cmd(i, &[b"GET", i.to_string().as_bytes(), One]);
                assert_eq!(
                    db.response_values(i).0,
                    [i.to_string().as_bytes(), i.to_string().as_bytes()]
                );
            }
        }

        // remove first node so node 3 boostrap from 1
        db2.dht.remove_node(db1.dht.node()).unwrap();
        sleep_ms(100);

        let db3 = TestDatabase::new("127.0.0.1:9002".parse().unwrap(), "t/db3", false);

        db3.dht.rebalance().unwrap();
        db3.wait_syncs();

        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db2, &db3] {
                db.do_cmd(i, &[b"GET", i.to_string().as_bytes(), One]);
                assert_eq!(
                    db.response_values(i).0,
                    [i.to_string().as_bytes(), i.to_string().as_bytes()]
                );
            }
        }
    }

    #[test]
    fn test_sync() {
        let _ = fs::remove_dir_all("t/");
        let _ = env_logger::try_init();
        let db1 = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db1", true);
        let mut db2 = TestDatabase::new("127.0.0.1:9001".parse().unwrap(), "t/db2", false);
        let mut db3 = TestDatabase::new("127.0.0.1:9002".parse().unwrap(), "t/db3", false);

        db1.dht.rebalance().unwrap();
        db1.wait_syncs();
        db2.wait_syncs();
        db3.wait_syncs();

        // sim partition
        warn!("droping db2");
        drop(db2);
        warn!("droping db3");
        drop(db3);

        for i in 0..TEST_JOIN_SIZE {
            db1.do_cmd(i, &[b"GETSET", i.to_string().as_bytes(), b"", b"", One]);
            let (_, vv) = db1.response_values(i);

            // rewrite key
            db1.do_cmd(
                i,
                &[
                    b"GETSET",
                    i.to_string().as_bytes(),
                    i.to_string().as_bytes(),
                    &encode_vv(&vv),
                    One,
                ],
            );
            db1.response_values(i);

            // add sibling
            db1.do_cmd(
                i,
                &[
                    b"GETSET",
                    i.to_string().as_bytes(),
                    i.to_string().as_bytes(),
                    &encode_vv(&vv),
                    One,
                ],
            );
            db1.response_values(i);
        }

        // sim partition heal
        warn!("bringing back db2");
        db2 = TestDatabase::new("127.0.0.1:9001".parse().unwrap(), "t/db2", false);
        sleep_ms(200); // wait for fabric to reconnect

        warn!("will check before sync");
        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db1, &db2] {
                db.do_cmd(i, &[b"GET", i.to_string().as_bytes(), Quorum]);
                assert_eq!(
                    db.response_values(i).0,
                    [i.to_string().as_bytes(), i.to_string().as_bytes()]
                );
            }
        }

        // force some syncs
        warn!("starting syncs");
        db2.force_syncs();
        db2.wait_syncs();

        warn!("will check after sync");
        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db2] {
                db.do_cmd(i, &[b"GET", i.to_string().as_bytes(), One]);
                assert_eq!(
                    db.response_values(i).0,
                    [i.to_string().as_bytes(), i.to_string().as_bytes()]
                );
            }
        }

        // nuke db1 so db3 is forced to sync with db2
        warn!("droping db1");
        drop(db1);
        // sim partition heal
        warn!("bringing back db3");
        db3 = TestDatabase::new("127.0.0.1:9002".parse().unwrap(), "t/db3", false);
        sleep_ms(200); // wait for fabric to reconnect

        // force some syncs
        warn!("starting syncs");
        db3.force_syncs();
        db3.wait_syncs();

        warn!("will check after sync");
        for i in 0..TEST_JOIN_SIZE {
            for &db in &[&db3] {
                db.do_cmd(i, &[b"GET", i.to_string().as_bytes(), One]);
                assert_eq!(
                    db.response_values(i).0,
                    [i.to_string().as_bytes(), i.to_string().as_bytes()]
                );
            }
        }
    }

    #[test]
    fn test_consistency_level() {
        let _ = fs::remove_dir_all("t/");
        let _ = env_logger::try_init();
        let db1 = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db1", true);
        let db2 = TestDatabase::new("127.0.0.1:9001".parse().unwrap(), "t/db2", false);
        let db3 = TestDatabase::new("127.0.0.1:9002".parse().unwrap(), "t/db3", false);
        db1.dht.rebalance().unwrap();

        db1.wait_syncs();
        db2.wait_syncs();
        db3.wait_syncs();

        // test data
        db1.do_cmd(0, &[b"GETSET", b"key", b"value", b"", All]);
        assert_eq!(db1.response_values(0).0, [b"value"]);

        for &cl in &[One, Quorum, All] {
            db1.do_cmd(0, &[b"GET", b"key", cl]);
            assert_eq!(db1.response_values(0).0, [b"value"]);
            db1.do_cmd(0, &[b"GETSET", b"other", b"", b"", cl]);
            db1.response_values(0);
        }

        drop(db3);
        for &cl in &[One, Quorum] {
            db1.do_cmd(0, &[b"GET", b"key", cl]);
            assert_eq!(db1.response_values(0).0, [b"value"]);
            db1.do_cmd(0, &[b"GETSET", b"other", b"", b"", cl]);
            db1.response_values(0);
        }
        for &cl in &[All] {
            db1.do_cmd(0, &[b"GET", b"key", cl]);
            assert_eq!(db1.response_resp(0), RespValue::Error("Unavailable".into()));
            db1.do_cmd(0, &[b"GETSET", b"other", b"", b"", cl]);
            assert_eq!(db1.response_resp(0), RespValue::Error("Unavailable".into()));
        }

        drop(db2);
        for &cl in &[One] {
            db1.do_cmd(0, &[b"GET", b"key", cl]);
            assert_eq!(db1.response_values(0).0, [b"value"]);
            db1.do_cmd(0, &[b"GETSET", b"other", b"", b"", cl]);
            db1.response_values(0);
        }
        for &cl in &[Quorum, All] {
            db1.do_cmd(0, &[b"GET", b"key", cl]);
            assert_eq!(db1.response_resp(0), RespValue::Error("Unavailable".into()));
            db1.do_cmd(0, &[b"GETSET", b"other", b"", b"", cl]);
            assert_eq!(db1.response_resp(0), RespValue::Error("Unavailable".into()));
        }
    }

    fn stub_aae_converge(drop: usize) {
        use std::env;
        use std::ffi::OsString;
        let _ = fs::remove_dir_all("t/");
        let _ = env_logger::try_init();
        let db1 = TestDatabase::new("127.0.0.1:9000".parse().unwrap(), "t/db1", true);
        let db2 = TestDatabase::new("127.0.0.1:9001".parse().unwrap(), "t/db2", false);
        let db3 = TestDatabase::new("127.0.0.1:9002".parse().unwrap(), "t/db3", false);
        db1.dht.rebalance().unwrap();

        db1.wait_syncs();
        db2.wait_syncs();
        db3.wait_syncs();

        env::set_var("FABRIC_DROP", &OsString::from(drop.to_string()));

        for (j, &db) in [&db1, &db2, &db2].iter().enumerate() {
            db.do_cmd(
                0,
                &[
                    b"SET",
                    (j + 10000).to_string().as_bytes(),
                    (j + 10000).to_string().as_bytes(),
                    b"",
                    One,
                ],
            );
            db.response_resp(0);
            for i in 0..TEST_JOIN_SIZE {
                db.do_cmd(
                    0,
                    &[
                        b"GETSET",
                        i.to_string().as_bytes(),
                        i.to_string().as_bytes(),
                        b"",
                        One,
                    ],
                );
                let vv = db.response_values(0).1;
                if i > TEST_JOIN_SIZE / 2 {
                    db.do_cmd(0, &[b"DEL", i.to_string().as_bytes(), &encode_vv(&vv), One]);
                    db.response_resp(0);
                }
            }
        }

        env::set_var("FABRIC_DROP", "0");

        db1.force_syncs();
        db2.force_syncs();
        db3.force_syncs();
        db1.wait_syncs();
        db2.wait_syncs();
        db3.wait_syncs();

        for &db in &[&db1, &db2, &db2] {
            for j in 0..3 {
                db.do_cmd(0, &[b"GET", (j + 10000).to_string().as_bytes(), One]);
                assert_eq!(
                    db.response_values(0).0,
                    [(j + 10000).to_string().as_bytes()]
                );
            }
            for i in 0..TEST_JOIN_SIZE {
                db.do_cmd(0, &[b"GET", i.to_string().as_bytes(), One]);
                let values = db.response_values(0).0;
                if i > TEST_JOIN_SIZE / 2 {
                    assert_eq!(values.len(), 0);
                } else {
                    assert_eq!(
                        values,
                        [
                            i.to_string().as_bytes(),
                            i.to_string().as_bytes(),
                            i.to_string().as_bytes(),
                        ]
                    );
                }
            }
        }
    }

    #[test]
    fn test_aae_converge_100() {
        stub_aae_converge(100);
    }

    #[test]
    fn test_aae_converge_50() {
        stub_aae_converge(50);
    }
}
