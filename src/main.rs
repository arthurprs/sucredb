#![feature(custom_derive, plugin)]
#![feature(question_mark)]
#![allow(dead_code)]
#![plugin(serde_macros)]
#![feature(lookup_host)]
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
mod utils;
mod version_vector;
mod gossip;
mod inflightmap;
mod dht;
mod storage;
mod hash;
mod protocol;
mod database;
mod server;

fn main() {
    env_logger::init().unwrap();
    let mut server = server::Server::new();
    server.run();
}
