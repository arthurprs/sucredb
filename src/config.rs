use std::io::Read;
use std::fs::File;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::convert::TryInto;
use types::ConsistencyLevel;
use utils::GenericError;
use toml;

pub const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:6379";
pub const DEFAULT_FABRIC_ADDR: &str = "127.0.0.1:16379";
pub const DEFAULT_ETCD_ADDR: &str = "http://127.0.0.1:2379";
pub const DEFAULT_CLUSTER: &str = "default";
pub const DEFAULT_REPLICATION_FACTOR: &str = "3";
pub const DEFAULT_PARTITIONS: &str = "64";

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
    // TODO: add config for socket buffers
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
            sync_msg_inflight: 10,
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

fn split_number_suffix(s: &str) -> Result<(u64, &str), GenericError> {
    let digits_end = s.trim().chars().position(|c| !c.is_digit(10)).unwrap_or(s.len());
    let (digits, suffix) = (&s[0..digits_end], &s[digits_end..]);
    Ok((digits.parse::<u64>()?, suffix.trim()))
}

pub fn parse_duration(duration_text: &str) -> Result<u64, GenericError> {
    let (number, suffix) = split_number_suffix(duration_text)?;
    let scale = match suffix.to_lowercase().as_ref() {
        "ms" => 1,
        "s" => 1000,
        "m" => 1000 * 60,
        "h" => 1000 * 60 * 60,
        "d" => 1000 * 60 * 60 * 24,
        _ => return Err(format!("Unknown suffix `{}`", suffix).into()),
    };
    number.checked_mul(scale).ok_or("Overflow error".into())
}

pub fn parse_size(size_text: &str) -> Result<u64, GenericError> {
    let (number, suffix) = split_number_suffix(size_text)?;
    let scale = match suffix.to_lowercase().as_ref() {
        "b" => 1,
        "k" | "kb" => 1024,
        "m" | "mb" => 1024 * 1024,
        "g" | "gb" => 1024 * 1024 * 1024,
        _ => return Err(format!("Unknown suffix `{}`", suffix).into()),
    };
    number.checked_mul(scale).ok_or("Overflow error".into())
}

macro_rules! cfg {
    ($toml: ident, $target: ident, $string: ident, $method: ident) => (
        if let Some(v) = $toml.get(stringify!($string)).and_then(|v| v.$method()) {
            $target.$string = v.into();
        }
    );
    ($toml: ident, $target: ident, $string: ident, $method: ident, try_into) => (
        if let Some(v) = $toml.get(stringify!($string)).and_then(|v| v.$method()) {
            $target.$string = v.try_into().expect("Can't convert");
        }
    );
    ($toml: ident, $target: ident, $string: ident, $method: ident, $convert: ident) => (
        if let Some(v) = $toml.get(stringify!($string)).and_then(|v| v.$method()) {
            $target.$string =
                $convert(v).expect(concat!("Can't parse with ", stringify!($convert)))
        }
    );
    ($toml: ident, $target: ident, $string: ident, $method: ident, $convert: ident, try_into) => (
        if let Some(v) = $toml.get(stringify!($string)).and_then(|v| v.$method()) {
            $target.$string =
                $convert(v).expect(concat!("Can't parse with ", stringify!($convert)))
                .try_into().expect("Can't convert");
        }
    );
}

fn read_config(path: &Path, config: &mut Config) {
    debug!("reading config file");
    let toml = {
        let mut s = String::new();
        File::open(path)
            .expect("Error opening config file")
            .read_to_string(&mut s)
            .expect("Error reading config file");
        s.parse::<toml::Value>().expect("Error parsing toml config file")
    };
    debug!("done reading config file: {:?}", config);

    cfg!(toml, config, data_dir, as_str);
    cfg!(toml, config, cluster_name, as_str);
    cfg!(toml, config, listen_addr, as_str, try_into);
    cfg!(toml, config, fabric_addr, as_str, try_into);
    cfg!(toml, config, etcd_addr, as_str, try_into);
    // pub cmd_init: Option<InitCommand>,
    cfg!(toml, config, worker_timer, as_str, parse_duration, try_into);
    cfg!(toml, config, worker_count, as_integer, try_into);
    cfg!(toml, config, sync_incomming_max, as_integer, try_into);
    cfg!(toml, config, sync_outgoing_max, as_integer, try_into);
    cfg!(toml, config, sync_auto, as_bool);
    cfg!(toml, config, sync_timeout, as_str, parse_duration, try_into);
    cfg!(toml, config, sync_msg_timeout, as_str, parse_duration, try_into);
    cfg!(toml, config, sync_msg_inflight, as_integer, try_into);
    cfg!(toml, config, fabric_reconnect_interval, as_str, parse_duration, try_into);
    cfg!(toml, config, fabric_keepalive, as_str, parse_duration, try_into);
    cfg!(toml, config, fabric_timeout, as_str, parse_duration, try_into);
    cfg!(toml, config, request_timeout, as_str, parse_duration, try_into);
    cfg!(toml, config, connections_max, as_integer, try_into);
    cfg!(toml, config, value_version_max, as_integer, try_into);
    cfg!(toml, config, consistency_read, as_str, try_into);
    cfg!(toml, config, consistency_write, as_str, try_into);
}
