#![feature(custom_derive, plugin, question_mark, arc_counts,
binary_heap_extras, fnbox, conservative_impl_trait)]
#![allow(dead_code)]
#![plugin(serde_macros)]
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate rand;
extern crate ramp;
extern crate linear_map;
extern crate serde;
extern crate serde_yaml;
extern crate bincode;
extern crate tendril;
extern crate lmdb_rs;
extern crate byteorder;
extern crate etcd;
extern crate nodrop;

extern crate futures;
#[macro_use]
extern crate tokio_core;

#[macro_use]
mod utils;
mod types;
mod my_futures;
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

#[cfg(not(test))]
fn main() {
    env_logger::init().unwrap();
    let server = server::Server::new();
    server.run();
}
