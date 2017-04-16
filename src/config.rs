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
    pub worker_count: u16,
    pub sync_incomming_max: u16,
    pub sync_outgoing_max: u16,
    pub sync_auto: bool,
    pub sync_timeout: u32,
    pub sync_msg_timeout: u32,
    pub sync_msg_inflight: u32,
    pub fabric_reconnect_interval: u32,
    pub fabric_keepalive: u32,
    pub fabric_timeout: u32,
    pub request_timeout: u32,
    pub connections_max: u32,
    pub value_version_max: u16,
    pub consistency_read: ConsistencyLevel,
    pub consistency_write: ConsistencyLevel,
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
            worker_timer: 500,
            worker_count: 4,
            sync_incomming_max: 1,
            sync_outgoing_max: 1,
            sync_timeout: 10_000,
            sync_msg_timeout: 1000,
            sync_msg_inflight: 5,
            sync_auto: true,
            fabric_reconnect_interval: 1000,
            fabric_keepalive: 1000,
            fabric_timeout: 1000,
            request_timeout: 1000,
            connections_max: 100,
            value_version_max: 100,
            consistency_read: ConsistencyLevel::One,
            consistency_write: ConsistencyLevel::One,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InitCommand {
    pub replication_factor: u8,
    pub partitions: u16,
}
