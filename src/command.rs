use std::num::ParseIntError;
use std::net;
use std::convert::TryInto;
use bytes::Bytes;
use bincode;
use resp::RespValue;
use database::Database;
use types::*;
use config;
use version_vector::*;
use cubes::{self, Cube};
use metrics::{self, Meter};

#[derive(Debug)]
pub enum CommandError {
    Timeout,
    ProtocolError,
    StorageError,
    UnknownCommand,
    TooManyVersions,
    TypeError,
    InvalidContext,
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

impl Into<RespValue> for CommandError {
    fn into(self) -> RespValue {
        RespValue::Error(format!("{:?}", self).into())
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

        let dummy = Bytes::new();
        let mut argc = 0;
        let mut args = [&dummy; 8];
        match cmd {
            RespValue::Array(ref a) if a.len() < args.len() => for v in a.iter() {
                if let &RespValue::Data(ref b) = v {
                    args[argc] = b;
                    argc += 1;
                } else {
                    argc = 0;
                    break;
                }
            },
            _ => (),
        }

        if argc == 0 {
            self.respond_error(token, CommandError::ProtocolError);
            return;
        }

        let arg0 = args[0];
        let args = &args[1..argc];

        let ret = match arg0.as_ref() {
            b"GET" | b"get" => self.cmd_get(token, args),
            b"SET" | b"set" => self.cmd_set(token, args, false),
            b"HGETALL" | b"hgetall" => self.cmd_hgetall(token, args),
            b"HSET" | b"hset" => self.cmd_hset(token, args),
            b"HDEL" | b"hdel" => self.cmd_hdel(token, args),
            b"SMEMBERS" | b"smembers" => self.cmd_smembers(token, args),
            b"SADD" | b"sadd" => self.cmd_sadd(token, args),
            b"SREM" | b"srem" => self.cmd_srem(token, args),
            b"SPOP" | b"spop" => self.cmd_spop(token, args),
            b"GETSET" | b"getset" => self.cmd_set(token, args, true),
            b"DEL" | b"del" => self.cmd_del(token, args),
            b"CLUSTER" | b"cluster" => self.cmd_cluster(token, args),
            b"TYPE" | b"type" => self.cmd_type(token, args),
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

    fn parse_vv(
        &self,
        try: bool,
        args: &[&Bytes],
        i: usize,
    ) -> Result<VersionVector, CommandError> {
        if try {
            bincode::deserialize(args[i]).map_err(|_| CommandError::InvalidContext)
        } else {
            Ok(Default::default())
        }
    }

    fn parse_consistency(
        &self,
        try: bool,
        args: &[&Bytes],
        i: usize,
    ) -> Result<ConsistencyLevel, CommandError> {
        Ok(if try {
            args[i].as_ref().try_into()?
        } else {
            self.config.consistency_read
        })
    }

    fn cmd_config(&self, token: u64, _args: &[&Bytes]) -> Result<(), CommandError> {
        Ok(self.respond(token, RespValue::Array(Default::default())))
    }

    fn cmd_hgetall(&self, token: u64, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_GET.mark(1);
        check_arg_count(args.len(), 1, 2)?;
        check_key_len(args[0].len())?;
        let consistency = self.parse_consistency(args.len() >= 2, args, 1)?;
        Ok(self.get(
            token,
            args[0].as_ref(),
            consistency,
            cubes::render_map,
        ))
    }

    fn cmd_hset(&self, token: u64, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_SET.mark(1);
        check_arg_count(args.len(), 3, 4)?;
        check_key_len(args[0].len())?;
        check_key_len(args[1].len())?;
        check_value_len(args[2].len())?;
        let consistency = self.parse_consistency(args.len() >= 4, args, 3)?;
        Ok(self.set(
            token,
            args[0].as_ref(),
            &mut |i, v, c, _vv| {
                let mut map = c.into_map().ok_or(CommandError::TypeError)?;
                let result = map.insert(i, v, args[1].clone(), args[2].clone()) as i64;
                Ok((Cube::Map(map), Some(RespValue::Int(result))))
            },
            Default::default(),
            consistency,
            false,
            cubes::render_dummy,
        ))
    }

    fn cmd_hdel(&self, token: u64, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_DEL.mark(1);
        check_arg_count(args.len(), 2, 3)?;
        check_key_len(args[0].len())?;
        check_key_len(args[1].len())?;
        let consistency = self.parse_consistency(args.len() >= 3, args, 2)?;
        Ok(self.set(
            token,
            args[0],
            &mut |i, v, c, _vv| {
                let mut map = c.into_map().ok_or(CommandError::TypeError)?;
                let result = map.remove(i, v, &args[1]) as i64;
                Ok((Cube::Map(map), Some(RespValue::Int(result))))
            },
            Default::default(),
            consistency,
            false,
            cubes::render_dummy,
        ))
    }


    fn cmd_smembers(&self, token: u64, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_GET.mark(1);
        check_arg_count(args.len(), 1, 2)?;
        check_key_len(args[0].len())?;
        let consistency = self.parse_consistency(args.len() >= 2, args, 1)?;
        Ok(self.get(
            token,
            args[0].as_ref(),
            consistency,
            cubes::render_set,
        ))
    }

    fn cmd_sadd(&self, token: u64, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_SET.mark(1);
        check_arg_count(args.len(), 2, 3)?;
        check_key_len(args[0].len())?;
        check_value_len(args[1].len())?;
        let consistency = self.parse_consistency(args.len() >= 3, args, 2)?;
        Ok(self.set(
            token,
            args[0].as_ref(),
            &mut |i, v, c, _vv| {
                let mut set = c.into_set().ok_or(CommandError::TypeError)?;
                let result = set.insert(i, v, args[1].clone()) as i64;
                Ok((Cube::Set(set), Some(RespValue::Int(result))))
            },
            Default::default(),
            consistency,
            false,
            cubes::render_dummy,
        ))
    }

    fn cmd_srem(&self, token: u64, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_DEL.mark(1);
        check_arg_count(args.len(), 2, 3)?;
        check_key_len(args[0].len())?;
        check_key_len(args[1].len())?;
        let consistency = self.parse_consistency(args.len() >= 3, args, 2)?;
        Ok(self.set(
            token,
            args[0],
            &mut |i, v, c, _vv| {
                let mut set = c.into_set().ok_or(CommandError::TypeError)?;
                let result = set.remove(i, v, &args[1]) as i64;
                Ok((Cube::Set(set), Some(RespValue::Int(result))))
            },
            Default::default(),
            consistency,
            false,
            cubes::render_dummy,
        ))
    }

    fn cmd_spop(&self, token: u64, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_DEL.mark(1);
        check_arg_count(args.len(), 1, 2)?;
        check_key_len(args[0].len())?;
        let consistency = self.parse_consistency(args.len() >= 2, args, 1)?;
        Ok(self.set(
            token,
            args[0],
            &mut |i, v, c, _vv| {
                let mut set = c.into_set().ok_or(CommandError::TypeError)?;
                let result = set.pop(i, v);
                let resp = result
                    .map(|x| RespValue::Data(x))
                    .unwrap_or_else(|| RespValue::Nil);
                Ok((Cube::Set(set), Some(resp)))
            },
            Default::default(),
            consistency,
            false,
            cubes::render_dummy,
        ))
    }

    fn cmd_get(&self, token: u64, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_GET.mark(1);
        check_arg_count(args.len(), 1, 2)?;
        check_key_len(args[0].len())?;
        let consistency = self.parse_consistency(args.len() >= 2, args, 1)?;
        Ok(self.get(
            token,
            args[0].as_ref(),
            consistency,
            cubes::render_value_or_counter,
        ))
    }

    fn cmd_set(&self, token: u64, args: &[&Bytes], reply_result: bool) -> Result<(), CommandError> {
        metrics::REQUEST_SET.mark(1);
        check_arg_count(args.len(), 2, 4)?;
        check_key_len(args[0].len())?;
        check_value_len(args[1].len())?;
        let vv = self.parse_vv(args.len() >= 3 && !args[2].is_empty(), args, 2)?;
        let consistency = self.parse_consistency(args.len() >= 4, args, 3)?;
        Ok(self.set(
            token,
            args[0],
            &mut |i, v, c, vv| {
                let mut value = c.into_value().ok_or(CommandError::TypeError)?;
                value.set(i, v, Some(args[1].clone()), vv);
                let resp = if reply_result {
                    None
                } else {
                    Some(RespValue::Status("OK".into()))
                };
                Ok((Cube::Value(value), resp))
            },
            vv,
            consistency,
            reply_result,
            if reply_result {
                cubes::render_value_or_counter
            } else {
                cubes::render_dummy
            },
        ))
    }

    fn cmd_del(&self, token: u64, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_DEL.mark(1);
        check_arg_count(args.len(), 1, 3)?;
        check_key_len(args[0].len())?;
        let vv = self.parse_vv(args.len() >= 2 && !args[1].is_empty(), args, 1)?;
        let consistency = self.parse_consistency(args.len() >= 3, args, 2)?;
        Ok(self.set(
            token,
            args[0],
            &mut |i, v, mut c, vv| {
                let result = c.del(i, v, vv) as i64;
                Ok((c, Some(RespValue::Int(result))))
            },
            vv,
            consistency,
            false,
            cubes::render_dummy,
        ))
    }

    fn cmd_type(&self, token: u64, args: &[&Bytes]) -> Result<(), CommandError> {
        check_arg_count(args.len(), 1, 2)?;
        let consistency = self.parse_consistency(args.len() >= 2, args, 1)?;
        Ok(self.get(
            token,
            args[0].as_ref(),
            consistency,
            cubes::render_type,
        ))
    }

    fn cmd_cluster(&self, token: u64, args: &[&Bytes]) -> Result<(), CommandError> {
        check_arg_count(args.len(), 1, 1)?;
        match args[0].as_ref() {
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
        self.respond(token, error.into());
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
