use std::num::ParseIntError;
use std::net;
use std::convert::TryInto;
use bincode;
use resp::RespValue;
use database::Database;
use types::*;
use config;
use version_vector::*;
use metrics::{self, Meter};
use bytes::Bytes;

#[derive(Debug)]
pub enum CommandError {
    Timeout,
    ProtocolError,
    UnknownCommand,
    TooManyVersions,
    InvalidArgCount,
    InvalidKey,
    InvalidValue,
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

fn check_key_len(key_len: usize) -> Result<(), CommandError> {
    if key_len > config::MAX_KEY_LEN {
        Err(CommandError::InvalidKey)
    } else {
        Ok(())
    }
}

fn check_value_len(value_len: usize) -> Result<(), CommandError> {
    if value_len > config::MAX_VALUE_LEN {
        Err(CommandError::InvalidKey)
    } else {
        Ok(())
    }
}

impl Database {
    pub fn handler_cmd(&self, token: u64, cmd: RespValue) {
        debug!("Processing ({:?}) {:?}", token, cmd);

        let mut arg_: [&[u8]; 8] = [b""; 8];
        let mut argc = 0;
        match cmd {
            RespValue::Array(ref a) if a.len() > 0 && a.len() <= arg_.len() => for v in a {
                if let &RespValue::Data(ref d) = v {
                    arg_[argc] = d.as_ref();
                    argc += 1;
                } else {
                    argc = 0;
                    break;
                };
            },
            _ => (),
        }

        if argc == 0 {
            self.respond_error(token, CommandError::ProtocolError);
            return;
        }

        let arg0 = arg_[0];
        let args = &arg_[1..argc];

        let ret = match arg0 {
            b"GET" | b"MGET" | b"HGET" | b"get" | b"mget" | b"hget" => self.cmd_get(token, args),
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
            b"ASKING" | b"asking" | b"READONLY" | b"readonly" | b"READWRITE" | b"readwrite" => {
                self.respond_ok(token);
                Ok(())
            }
            b"CONFIG" | b"config" => self.cmd_config(token, args),
            _ => {
                debug!("Unknown command {:?}", cmd);
                Err(CommandError::UnknownCommand)
            }
        };

        if let Err(err) = ret {
            self.respond_error(token, err);
        }
    }

    fn cmd_config(&self, token: u64, _args: &[&[u8]]) -> Result<(), CommandError> {
        Ok(self.respond(token, RespValue::Array(Default::default())))
    }

    fn cmd_get(&self, token: u64, args: &[&[u8]]) -> Result<(), CommandError> {
        check_arg_count(args.len(), 1, 2)?;
        check_key_len(args[0].len())?;
        let consistency: ConsistencyLevel = if args.len() >= 2 {
            args[1].try_into()?
        } else {
            self.config.consistency_read
        };
        metrics::REQUEST_GET.mark(1);
        Ok(self.get(token, args[0], consistency))
    }

    fn cmd_set(&self, token: u64, args: &[&[u8]], reply_result: bool) -> Result<(), CommandError> {
        check_arg_count(args.len(), 2, 4)?;
        check_key_len(args[0].len())?;
        check_value_len(args[1].len())?;
        let vv: VersionVector = if args.len() >= 3 && !args[2].is_empty() {
            bincode::deserialize(args[2]).unwrap()
        } else {
            VersionVector::new()
        };
        let consistency: ConsistencyLevel = if args.len() >= 4 {
            args[3].try_into()?
        } else {
            self.config.consistency_write
        };
        metrics::REQUEST_SET.mark(1);
        Ok(self.set(
            token,
            args[0],
            Some(args[1]),
            vv,
            consistency,
            reply_result,
        ))
    }

    fn cmd_del(&self, token: u64, args: &[&[u8]]) -> Result<(), CommandError> {
        check_arg_count(args.len(), 1, 3)?;
        check_key_len(args[0].len())?;
        let vv: VersionVector = if args.len() >= 2 && !args[1].is_empty() {
            bincode::deserialize(args[1]).unwrap()
        } else {
            VersionVector::new()
        };
        let consistency: ConsistencyLevel = if args.len() >= 3 {
            args[2].try_into()?
        } else {
            self.config.consistency_write
        };
        metrics::REQUEST_DEL.mark(1);
        Ok(self.set(token, args[0], None, vv, consistency, false))
    }

    fn cmd_cluster(&self, token: u64, args: &[&[u8]]) -> Result<(), CommandError> {
        check_arg_count(args.len(), 1, 1)?;
        match args[0] {
            b"REBALANCE" | b"rebalance" => {
                self.dht.rebalance().unwrap();
                Ok(self.respond_ok(token))
            }
            b"SLOTS" | b"slots" => {
                let mut slots = Vec::new();
                for (&(start, end), members) in &self.dht.slots() {
                    let mut slot = vec![RespValue::Int(start as _), RespValue::Int(end as _)];
                    slot.extend(members.iter().map(|&(node, (_, ext_addr))| {
                        RespValue::Array(vec![
                            RespValue::Data(ext_addr.ip().to_string().as_bytes().into()),
                            RespValue::Int(ext_addr.port() as _),
                            RespValue::Data(node.to_string().as_bytes().into()),
                        ])
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

    pub fn respond_int(&self, token: Token, int: i64) {
        self.respond(token, RespValue::Int(int));
    }

    pub fn respond_ok(&self, token: Token) {
        self.respond(token, RespValue::Status("OK".into()));
    }

    pub fn respond_error(&self, token: Token, error: CommandError) {
        self.respond(token, RespValue::Error(format!("{:?}", error).into()));
    }

    pub fn respond_dcc(&self, token: Token, dcc: DottedCausalContainer<Bytes>) {
        self.respond(token, dcc_to_resp(dcc));
    }

    pub fn respond_moved(&self, token: Token, vnode: VNodeId, addr: net::SocketAddr) {
        self.respond(
            token,
            RespValue::Error(format!("MOVED {} {}", vnode, addr).into()),
        );
    }

    pub fn respond_ask(&self, token: Token, vnode: VNodeId, addr: net::SocketAddr) {
        self.respond(
            token,
            RespValue::Error(format!("ASK {} {}", vnode, addr).into()),
        );
    }
}

fn dcc_to_resp(dcc: DottedCausalContainer<Bytes>) -> RespValue {
    let serialized_vv = bincode::serialize(dcc.version_vector(), bincode::Infinite).unwrap();
    let mut values: Vec<_> = dcc.into_iter().map(|(_, v)| RespValue::Data(v)).collect();
    values.push(RespValue::Data(serialized_vv.into()));
    RespValue::Array(values)
}
