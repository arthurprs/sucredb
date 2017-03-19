#![feature(
    proc_macro,
    slice_patterns,
    fnbox,
    conservative_impl_trait,
    try_from)]
#![allow(dead_code)]
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate rand;
extern crate ramp;
extern crate linear_map;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate tendril;
extern crate lmdb_rs;
extern crate byteorder;
extern crate etcd;
extern crate nodrop;
extern crate clap;
extern crate crc16;

extern crate futures;
#[macro_use]
extern crate tokio_core;

#[macro_use]
mod utils;
mod types;
mod extra_futures;
mod version_vector;
// mod gossip;
mod inflightmap;
mod dht;
mod fabric_msg;
mod fabric;
mod storage;
mod hash;
#[macro_use]
mod database;
mod vnode;
mod vnode_sync;
mod resp;
mod server;
mod workers;
mod command;
mod config;


fn configure() -> config::Config {
    use config::*;
    use clap::{Arg, App, SubCommand};

    let matches = App::new("SucreDB")
        .version("0.0")
        .author("Noone")
        .about("Does a few things with keys and values")
        .arg(Arg::with_name("data_dir")
                 .short("d")
                 .long("data")
                 .takes_value(true)
                 .help("Data directory")
                 .required(true))
        .arg(Arg::with_name("etcd_addr")
                 .short("e")
                 .long("etcd")
                 .help("etcd addres")
                 .default_value(DEFAULT_ETCD_ADDR)
                 .takes_value(true))
        .arg(Arg::with_name("cluster_name")
                 .short("c")
                 .long("cluster")
                 .help("The cluster name")
                 .default_value(DEFAULT_CLUSTER)
                 .takes_value(true))
        .arg(Arg::with_name("listen_addr")
                 .short("l")
                 .long("listen")
                 .help("Listen addr")
                 .default_value(DEFAULT_LISTEN_ADDR)
                 .takes_value(true))
        .arg(Arg::with_name("fabric_addr")
                 .short("f")
                 .long("fabric")
                 .help("Fabric listen addr")
                 .default_value(DEFAULT_FABRIC_ADDR)
                 .takes_value(true))
        .subcommand(SubCommand::with_name("init")
                        .about("Init and configure the cluster")
                        .arg(Arg::with_name("replication_factor")
                                 .short("r")
                                 .help("Number of replicas")
                                 .default_value(DEFAULT_REPLICATION_FACTOR))
                        .arg(Arg::with_name("partitions")
                                 .short("p")
                                 .help("Number of partitions")
                                 .default_value(DEFAULT_PARTITIONS)))
        .get_matches();

    Config {
        data_dir: matches.value_of("data_dir").unwrap().into(),
        cluster_name: matches.value_of("cluster_name").unwrap().into(),
        listen_addr: matches.value_of("listen_addr").unwrap().parse().unwrap(),
        fabric_addr: matches.value_of("fabric_addr").unwrap().parse().unwrap(),
        etcd_addr: matches.value_of("etcd_addr").unwrap().into(),
        cmd_init: matches.subcommand_matches("init").map(|matches| {
            InitCommand {
                partitions: matches.value_of("partitions").unwrap().parse().unwrap(),
                replication_factor: matches.value_of("replication_factor")
                    .unwrap()
                    .parse()
                    .unwrap(),
            }
        }),
        ..Default::default()
    }
}

#[cfg(not(test))]
fn main() {
    env_logger::init().unwrap();
    let server = server::Server::new(configure());
    server.run();
}
