#![feature(slice_patterns, fnbox, try_from)]
#![allow(dead_code)]
#![cfg_attr(feature = "cargo-clippy", allow(cast_lossless))]

// #![feature(alloc_system, global_allocator, allocator_api)]
//
// extern crate alloc_system;
//
// use alloc_system::System;
//
// #[global_allocator]
// static A: System = System;

extern crate bincode;
extern crate byteorder;
extern crate bytes;
extern crate clap;
extern crate crc16;
extern crate futures;
#[macro_use]
extern crate lazy_static;
extern crate linear_map;
extern crate log4rs;
#[macro_use]
extern crate log;
extern crate metrics as rust_metrics;
extern crate num_cpus;
extern crate rand;
extern crate roaring;
extern crate rocksdb;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_yaml;
extern crate tokio_core;
extern crate tokio_io;

#[cfg(test)]
extern crate env_logger;

#[macro_use]
mod utils;
mod types;
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
mod metrics;

fn configure() -> config::Config {
    use config::*;
    use clap::{App, Arg, SubCommand};
    use std::path::Path;

    let matches = App::new("SucreDB")
        .version("0.1")
        .about("A database made of sugar cubes")
        .arg(
            Arg::with_name("config_file")
                .short("c")
                .long("config")
                .takes_value(true)
                .help(".yaml config file")
                .long_help(
                    "Path to the .yaml config file. Note that configuration \
                     set through the command line will take precedence \
                     over the config file.",
                )
                .display_order(0),
        )
        .arg(
            Arg::with_name("data_dir")
                .short("d")
                .long("data")
                .takes_value(true)
                .help("Data directory"),
        )
        .arg(
            Arg::with_name("cluster_name")
                .short("n")
                .long("cluster")
                .help("The cluster name")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("listen_addr")
                .short("l")
                .long("listen")
                .help("Listen addr")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("fabric_addr")
                .short("f")
                .long("fabric")
                .help("Fabric listen addr")
                .takes_value(true),
        )
        .subcommand(
            SubCommand::with_name("init")
                .about("Init and configure the cluster")
                .arg(
                    Arg::with_name("replication_factor")
                        .short("r")
                        .help("Number of replicas")
                        .default_value(DEFAULT_REPLICATION_FACTOR),
                )
                .arg(
                    Arg::with_name("partitions")
                        .short("p")
                        .help("Number of partitions")
                        .long_help(
                            "Number of partitions, the recommended value is \
                             `expected node count * 10` rounded up to the next power of 2.",
                        )
                        .default_value(DEFAULT_PARTITIONS),
                )
                .display_order(0),
        )
        .get_matches();

    let mut config = Default::default();

    if let Some(v) = matches.value_of("config_file") {
        read_config_file(Path::new(v), &mut config);
    } else {
        setup_default_logging();
    }

    if let Some(v) = matches.value_of("data_dir") {
        config.data_dir = v.into();
    }

    if let Some(v) = matches.value_of("cluster_name") {
        config.cluster_name = v.into();
    }

    if let Some(v) = matches.value_of("listen_addr") {
        config.listen_addr = v.parse().unwrap();
    }

    if let Some(v) = matches.value_of("fabric_addr") {
        config.fabric_addr = v.parse().unwrap();
    }

    if let Some(sub) = matches.subcommand_matches("init") {
        config.cmd_init = Some(InitCommand {
            partitions: sub.value_of("partitions").unwrap().parse().unwrap(),
            replication_factor: sub.value_of("replication_factor").unwrap().parse().unwrap(),
        });
    }

    config
}

#[cfg(not(test))]
fn main() {
    let server = server::Server::new(configure());
    server.run();
}
