#![feature(custom_derive, plugin)]
#![feature(question_mark)]
#![feature(fnbox, binary_heap_extras)]
#![allow(dead_code)]
#![feature(arc_counts)]
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
#[macro_use]
extern crate rotor;
extern crate rotor_stream;
extern crate rotor_tools;
extern crate byteorder;
extern crate etcd;
extern crate nodrop;

#[macro_use]
mod utils;
mod types;
mod version_vector;
// mod gossip;
mod inflightmap;
mod dht;
#[macro_use]
mod fabric_msg;
mod fabric;
mod storage;
mod hash;
#[macro_use]
mod database;
mod vnode;
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
