use resp::RespValue;
use database::Database;
use types::*;
use utils::assume_str;
use version_vector::*;
use std::num::ParseIntError;
use std::{str, net};
use std::convert::TryInto;
use bincode::{serde as bincode_serde, SizeLimit};

#[derive(Debug)]
pub enum CommandError {
    Timeout,
    ProtocolError,
    UnknownCommand,
    InvalidArgCount,
    InvalidConsistencyValue,
    InvalidIntValue,
    Unavailable,
}

impl From<ConsistencyLevelParseError> for CommandError {
    fn from(_: ConsistencyLevelParseError) -> Self {
        CommandError::InvalidConsistencyValue
    }
}

impl From<ParseIntError> for CommandError {
    fn from(_: ParseIntError) -> Self {
        CommandError::InvalidIntValue
    }
}

fn check_arg_count(count: usize, min: usize, max: usize) -> Result<(), CommandError> {
    if count < min || count > max {
        Err(CommandError::InvalidArgCount)
    } else {
        Ok(())
    }
}

fn quick_int(bytes: &[u8]) -> Result<i64, CommandError> {
    if bytes.len() == 1 {
        match bytes[0] {
            b'0'...b'9' => Ok((bytes[0] - b'0') as i64),
            _ => Err(CommandError::ProtocolError),
        }
    } else {
        assume_str(bytes)
            .parse::<i64>()
            .map_err(|_| CommandError::ProtocolError)
    }
}

impl Database {
    pub fn handler_cmd(&self, token: u64, cmd: RespValue) {
        debug!("Processing ({:?}) {:?}", token, cmd);

        let mut arg_: [&[u8]; 8] = [b""; 8];
        let mut argc = 0;
        match cmd {
            RespValue::Array(ref a) if a.len() > 0 && a.len() <= arg_.len() => {
                for v in a {
                    if let &RespValue::Data(ref d) = v {
                        arg_[argc] = d.as_ref();
                        argc += 1;
                    } else {
                        argc = 0;
                        break;
                    };
                }
            }
            _ => (),
        }

        if argc == 0 {
            self.respond_error(token, CommandError::ProtocolError);
            return;
        }

        let arg0 = arg_[0];
        let args = &arg_[1..argc];

        let ret = match arg0 {
            b"GET" | b"MGET" | b"get" | b"mget" => self.cmd_get(token, args),
            b"SET" | b"MSET" | b"HSET" | b"set" | b"mset" | b"hset" => {
                self.cmd_set(token, args, false)
            }
            b"GETSET" | b"getset" => self.cmd_set(token, args, true),
            b"DEL" | b"MDEL" | b"HDEL" | b"del" | b"mdel" | b"hdel" => self.cmd_del(token, args),
            b"CLUSTER" | b"cluster" => self.cmd_cluster(token, args),
            b"ECHO" | b"echo" => {
                self.respond(token, cmd.clone());
                Ok(())
            }
            _ if args.len() > 0 => {
                let arg1 = args[0];
                let args = &args[1..];
                match (arg0, arg1) {
                    (b"CONFIG", b"GET") |
                    (b"config", b"get") => self.cmd_config_get(token, args),
                    _ => Err(CommandError::UnknownCommand),
                }
            }
            _ => Err(CommandError::UnknownCommand),
        };

        if let Err(err) = ret {
            self.respond_error(token, err);
        }
    }

    fn cmd_config_get(&self, token: u64, _args: &[&[u8]]) -> Result<(), CommandError> {
        Ok(self.respond(token, RespValue::Array(Default::default())))
    }

    fn cmd_get(&self, token: u64, args: &[&[u8]]) -> Result<(), CommandError> {
        try!(check_arg_count(args.len(), 1, 2));
        let consistency: ConsistencyLevel = if args.len() >= 2 {
            try!(args[1].try_into())
        } else {
            self.config.read_consistency
        };
        Ok(self.get(token, args[0], consistency))
    }

    fn cmd_set(&self, token: u64, args: &[&[u8]], reply_result: bool) -> Result<(), CommandError> {
        try!(check_arg_count(args.len(), 2, 4));
        let vv: VersionVector = if args.len() >= 3 && !args[2].is_empty() {
            bincode_serde::deserialize(args[2]).unwrap()
        } else {
            VersionVector::new()
        };
        let consistency: ConsistencyLevel = if args.len() >= 4 {
            try!(args[3].try_into())
        } else {
            self.config.write_consistency
        };
        Ok(self.set(token, args[0], Some(args[1]), vv, consistency, reply_result))
    }

    fn cmd_del(&self, token: u64, args: &[&[u8]]) -> Result<(), CommandError> {
        try!(check_arg_count(args.len(), 1, 3));
        let vv: VersionVector = if args.len() >= 2 && !args[1].is_empty() {
            bincode_serde::deserialize(args[1]).unwrap()
        } else {
            VersionVector::new()
        };
        let consistency: ConsistencyLevel = if args.len() >= 3 {
            try!(args[2].try_into())
        } else {
            self.config.write_consistency
        };
        Ok(self.set(token, args[0], None, vv, consistency, false))
    }

    fn cmd_cluster(&self, token: u64, args: &[&[u8]]) -> Result<(), CommandError> {
        match args {
            &[b"REBALANCE"] | &[b"rebalance"] => {
                self.dht.rebalance().unwrap();
                Ok(self.respond_ok(token))
            }
            &[b"SLOTS"] | &[b"slots"] => {
                let mut slots = Vec::new();
                for (&(start, end), members) in self.dht.slots().iter() {
                    let mut slot = vec![RespValue::Int(start as _), RespValue::Int(end as _)];
                    slot.extend(members.iter()
                        .map(|&(node, (_, ext_addr))| {
                            RespValue::Array(vec![RespValue::Data(ext_addr.ip()
                                                      .to_string()
                                                      .as_bytes()
                                                      .into()),
                                                  RespValue::Int(ext_addr.port() as _),
                                                  RespValue::Data(node.to_string()
                                                      .as_bytes()
                                                      .into())])
                        }));
                    slots.push(RespValue::Array(slot));
                }
                Ok(self.respond(token, RespValue::Array(slots)))
            }
            _ => Err(CommandError::UnknownCommand),
        }
    }

    pub fn respond(&self, token: Token, resp: RespValue) {
        debug!("Respond request ({}) {:?}", token, resp);
        (&self.response_fn)(token, resp);
    }

    pub fn respond_ok(&self, token: Token) {
        self.respond(token, RespValue::Status("OK".into()));
    }

    pub fn respond_error(&self, token: Token, error: CommandError) {
        self.respond(token, RespValue::Error(format!("{:?}", error).into()));
    }

    pub fn respond_dcc(&self, token: Token, dcc: DottedCausalContainer<Vec<u8>>) {
        self.respond(token, dcc_to_resp(dcc));
    }

    pub fn respond_moved(&self, token: Token, vnode: VNodeId, addr: net::SocketAddr) {
        self.respond(token, RespValue::Error(format!("MOVED {} {}", vnode, addr).into()));
    }

    pub fn respond_ask(&self, token: Token, vnode: VNodeId, addr: net::SocketAddr) {
        self.respond(token, RespValue::Error(format!("ASK {} {}", vnode, addr).into()));
    }
}

fn dcc_to_resp(dcc: DottedCausalContainer<Vec<u8>>) -> RespValue {
    let mut values: Vec<_> = dcc.values().map(|v| RespValue::Data(v.as_slice().into())).collect();
    let buffer = bincode_serde::serialize(dcc.version_vector(), SizeLimit::Infinite).unwrap();
    values.push(RespValue::Data(buffer.as_slice().into()));
    RespValue::Array(values)
}
