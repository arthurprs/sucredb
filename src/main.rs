#![feature(custom_derive, plugin)]
#![feature(question_mark)]
#![allow(dead_code)]
#![plugin(serde_macros)]
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate rand;
extern crate ramp;
extern crate linear_map;
extern crate serde;
extern crate serde_json;
extern crate lmdb_rs;
extern crate tendril;
extern crate bincode;
#[macro_use]
extern crate rotor;
extern crate rotor_stream;
extern crate rotor_tools;
extern crate byteorder;
extern crate etcd;

#[macro_use]
mod utils;
mod version_vector;
#[cfg(not(test))]
mod gossip;
mod inflightmap;
mod dht;
#[macro_use]
mod fabric_msg;
mod fabric;
mod storage;
mod hash;
mod database;
#[cfg(not(test))]
mod resp;
#[cfg(not(test))]
mod server;

#[cfg(not(test))]
fn main() {
    env_logger::init().unwrap();
    let server = server::Server::new();
    server.run();
}
