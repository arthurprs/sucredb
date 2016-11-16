use std::net::SocketAddr;
use std::path::PathBuf;
use types::ConsistencyLevel;

pub const DEFAULT_LISTEN_ADDR: &'static str = "127.0.0.1:6379";
pub const DEFAULT_FABRIC_ADDR: &'static str = "127.0.0.1:16379";
pub const DEFAULT_ETCD_ADDR: &'static str = "http://127.0.0.1:2379";
pub const DEFAULT_CLUSTER: &'static str = "default";
pub const DEFAULT_REPLICATION_FACTOR: &'static str = "3";
pub const DEFAULT_PARTITIONS: &'static str = "64";

#[derive(Debug, Clone)]
pub struct Config {
    pub data_dir: PathBuf,
    pub cluster_name: String,
    pub listen_addr: SocketAddr,
    pub fabric_addr: SocketAddr,
    pub etcd_addr: String,
    pub cmd_init: Option<InitCommand>,
    pub worker_timer: u32,
    pub workers: u16,
    pub max_incomming_syncs: u16,
    pub max_outgoing_syncs: u16,
    pub fabric_reconnect_interval: u32,
    pub fabric_keepalive: u32,
    pub fabric_timeout: u32,
    pub max_connections: u32,
    pub read_consistency: ConsistencyLevel,
    pub write_consistency: ConsistencyLevel,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            data_dir: "/data".into(),
            cluster_name: "default".into(),
            listen_addr: DEFAULT_LISTEN_ADDR.parse().unwrap(),
            fabric_addr: DEFAULT_FABRIC_ADDR.parse().unwrap(),
            etcd_addr: DEFAULT_ETCD_ADDR.into(),
            cmd_init: None,
            worker_timer: 2000,
            workers: if cfg!(test) { 1 } else { 2 },
            max_incomming_syncs: if cfg!(test) { 0 } else { 1 },
            max_outgoing_syncs: if cfg!(test) { 0 } else { 1 },
            fabric_reconnect_interval: 1000,
            fabric_keepalive: 1000,
            fabric_timeout: 1000,
            max_connections: 100,
            read_consistency: ConsistencyLevel::Quorum,
            write_consistency: ConsistencyLevel::Quorum,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InitCommand {
    pub replication_factor: u8,
    pub partitions: u16,
}
