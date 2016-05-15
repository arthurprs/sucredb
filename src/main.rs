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

mod version_vector;
mod gossip;
mod inflightmap;
mod dht;
mod storage;
mod utils;
mod hash;
mod protocol;
mod database;

fn main() {
    env_logger::init().unwrap();
    println!("Hello, world!");
}
