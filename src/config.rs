use std::io::Read;
use std::fs::File;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::convert::TryInto;
use std::cmp::max;

use log;
use log4rs;
use num_cpus;
use serde_yaml as yaml;

use utils::GenericError;
use types::ConsistencyLevel;

// Remember to update defaults in sucredb.yaml!
pub const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:6379";
pub const DEFAULT_FABRIC_ADDR: &str = "127.0.0.1:16379";
pub const DEFAULT_ETCD_ADDR: &str = "http://127.0.0.1:2379";
pub const DEFAULT_CLUSTER_NAME: &str = "default";
pub const DEFAULT_DATA_DIR: &str = "./data";
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
    pub fabric_timeout: u32,
    pub request_timeout: u32,
    pub client_connection_max: u32,
    pub value_version_max: u16,
    // TODO: these should be in the cluster config instead
    pub consistency_read: ConsistencyLevel,
    pub consistency_write: ConsistencyLevel,
}

impl Default for Config {
    fn default() -> Self {
        // Remember to update defaults in sucre.yaml!
        Config {
            data_dir: DEFAULT_DATA_DIR.into(),
            cluster_name: DEFAULT_CLUSTER_NAME.into(),
            listen_addr: DEFAULT_LISTEN_ADDR.parse().unwrap(),
            fabric_addr: DEFAULT_FABRIC_ADDR.parse().unwrap(),
            etcd_addr: DEFAULT_ETCD_ADDR.into(),
            cmd_init: None,
            worker_timer: 500,
            worker_count: max(4, 1 + num_cpus::get() as u16 * 2),
            sync_incomming_max: 10,
            sync_outgoing_max: 10,
            sync_timeout: 10_000,
            sync_msg_timeout: 1000,
            sync_msg_inflight: 10,
            sync_auto: true,
            fabric_timeout: 1000,
            request_timeout: 1000,
            client_connection_max: 100,
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

fn split_number_suffix(s: &str) -> Result<(i64, &str), GenericError> {
    let digits_end = s.trim().chars().position(|c| !c.is_digit(10)).unwrap_or(s.len());
    let (digits, suffix) = s.split_at(digits_end);
    Ok((digits.parse::<i64>()?, suffix.trim_left()))
}

pub fn parse_duration(duration_text: &str) -> Result<i64, GenericError> {
    let (number, suffix) = split_number_suffix(duration_text)?;
    let scale = match suffix.to_lowercase().as_ref() {
        "ms" => 1,
        "s" => 1000,
        "m" => 1000 * 60,
        "h" => 1000 * 60 * 60,
        _ => return Err(format!("Unknown duration suffix `{}`", suffix).into()),
    };
    number.checked_mul(scale).ok_or("Overflow error".into())
}

pub fn parse_size(size_text: &str) -> Result<i64, GenericError> {
    let (number, suffix) = split_number_suffix(size_text)?;
    let scale = match suffix.to_lowercase().as_ref() {
        "b" => 1,
        "k" | "kb" => 1024,
        "m" | "mb" => 1024 * 1024,
        "g" | "gb" => 1024 * 1024 * 1024,
        _ => return Err(format!("Unknown size suffix `{}`", suffix).into()),
    };
    number.checked_mul(scale).ok_or("Overflow error".into())
}

macro_rules! cfg {
    ($yaml: ident, $target: ident, $string: ident, $method: ident) => (
        if let Some(v) = $yaml.get(stringify!($string)).and_then(|v| v.$method()) {
            $target.$string = v.into();
        }
    );
    ($yaml: ident, $target: ident, $string: ident, $method: ident, try_into) => (
        if let Some(v) = $yaml.get(stringify!($string)).and_then(|v| v.$method()) {
            $target.$string = v.try_into().expect("Can't convert");
        }
    );
    ($yaml: ident, $target: ident, $string: ident, $method: ident, $convert: ident) => (
        if let Some(v) = $yaml.get(stringify!($string)).and_then(|v| v.$method()) {
            $target.$string =
                $convert(v).expect(concat!("Can't parse with ", stringify!($convert)))
        }
    );
    ($yaml: ident, $target: ident, $string: ident, $method: ident, $convert: ident, try_into) => (
        if let Some(v) = $yaml.get(stringify!($string)).and_then(|v| v.$method()) {
            $target.$string =
                $convert(v).expect(concat!("Can't parse with ", stringify!($convert)))
                .try_into().expect("Can't convert");
        }
    );
}

pub fn read_config_file(path: &Path, config: &mut Config) {
    debug!("reading config file");
    let yaml = {
        let mut s = String::new();
        File::open(path)
            .and_then(|mut f| f.read_to_string(&mut s))
            .expect("Error reading config file");
        yaml::from_str::<yaml::Value>(&s).expect("Error parsing config file")
    };
    debug!("done reading config file: {:?}", config);

    cfg!(yaml, config, data_dir, as_str);
    cfg!(yaml, config, cluster_name, as_str);
    cfg!(yaml, config, listen_addr, as_str, try_into);
    cfg!(yaml, config, fabric_addr, as_str, try_into);
    cfg!(yaml, config, etcd_addr, as_str, try_into);
    // pub cmd_init: Option<InitCommand>,
    cfg!(yaml, config, worker_timer, as_str, parse_duration, try_into);
    cfg!(yaml, config, worker_count, as_u64, try_into);
    cfg!(yaml, config, sync_incomming_max, as_u64, try_into);
    cfg!(yaml, config, sync_outgoing_max, as_u64, try_into);
    // cfg!(yaml, config, sync_auto, as_bool);
    cfg!(yaml, config, sync_timeout, as_str, parse_duration, try_into);
    cfg!(yaml, config, sync_msg_timeout, as_str, parse_duration, try_into);
    cfg!(yaml, config, sync_msg_inflight, as_u64, try_into);
    cfg!(yaml, config, fabric_timeout, as_str, parse_duration, try_into);
    cfg!(yaml, config, request_timeout, as_str, parse_duration, try_into);
    cfg!(yaml, config, client_connection_max, as_u64, try_into);
    cfg!(yaml, config, value_version_max, as_u64, try_into);
    cfg!(yaml, config, consistency_read, as_str, try_into);
    cfg!(yaml, config, consistency_write, as_str, try_into);

    if let Some(config_value) = yaml.get("logging") {
        setup_logging(config_value);
    }
}

pub fn setup_logging(config_value: &yaml::Value) {
    let raw_config: log4rs::file::RawConfig =
        yaml::from_value(config_value.clone()).expect("failed to parse logging config");

    let (appenders, errors) = raw_config.appenders_lossy(&mut Default::default());
    if !errors.is_empty() {
        panic!("failed to configure logging: {:?}", errors);
    }

    let (config, errors) = log4rs::config::Config::builder()
        .appenders(appenders)
        .loggers(raw_config.loggers())
        .build_lossy(raw_config.root());

    if !errors.is_empty() {
        panic!("failed to configure logging: {:?}", errors);
    }

    log4rs::init_config(config).expect("failed to init logging");
}

pub fn setup_default_logging() {
    let config = log4rs::config::Config::builder()
        .appender(
            log4rs::config::Appender::builder()
                .build(
                    "console",
                    Box::new(
                        log4rs::append::console::ConsoleAppender::builder()
                            .target(log4rs::append::console::Target::Stderr)
                            .build(),
                    ),
                ),
        )
        .logger(
            log4rs::config::Logger::builder()
                .appender("console")
                .build("sucredb", log::LogLevelFilter::Info),
        )
        .build(log4rs::config::Root::builder().build(log::LogLevelFilter::Off))
        .expect("failed to setup default logging");

    log4rs::init_config(config).expect("failed to init logging");
}
