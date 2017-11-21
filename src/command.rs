use std::num::ParseIntError;
use std::net;
use std::convert::TryInto;
use bytes::Bytes;
use bincode;
use resp::RespValue;
use database::{Context, Database};
use types::*;
use utils::replace_default;
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
    InvalidCommand,
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
    pub fn handler_cmd(&self, mut context: Context, cmd: RespValue) {
        let context = &mut context;
        debug!("Processing ({:?}) {:?}", context.token, cmd);
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
            self.respond_error(context, CommandError::ProtocolError);
            return;
        }

        let arg0 = args[0];
        let args = &args[1..argc];

        let ret = if context.is_multi {
            match arg0.as_ref() {
                b"EXEC"| b"exec" => self.cmd_exec(context, args),
                _ => {
                    context.cmds.push(cmd.clone());
                    self.respond_resp(context, RespValue::Status("QUEUED".into()));
                    Ok(())
                }
            }
        } else {
            match arg0.as_ref() {
                b"GET" | b"get" => self.cmd_get(context, args),
                b"SET" | b"set" => self.cmd_set(context, args, false),
                b"HGETALL" | b"hgetall" => self.cmd_hgetall(context, args),
                b"HSET" | b"hset" => self.cmd_hset(context, args),
                b"HDEL" | b"hdel" => self.cmd_hdel(context, args),
                b"SMEMBERS" | b"smembers" => self.cmd_smembers(context, args),
                b"SADD" | b"sadd" => self.cmd_sadd(context, args),
                b"SREM" | b"srem" => self.cmd_srem(context, args),
                b"SPOP" | b"spop" => self.cmd_spop(context, args),
                b"GETSET" | b"getset" => self.cmd_set(context, args, true),
                b"DEL" | b"del" => self.cmd_del(context, args),
                b"CLUSTER" | b"cluster" => self.cmd_cluster(context, args),
                b"TYPE" | b"type" => self.cmd_type(context, args),
                b"MULTI" | b"multi" => self.cmd_multi(context, args),
                b"EXEC" | b"exec" => self.cmd_exec(context, args),
                b"ECHO" | b"echo" => {
                    self.respond_resp(context, cmd.clone());
                    Ok(())
                }
                b"ASKING" | b"asking" | b"READONLY" | b"readonly" | b"READWRITE" | b"readwrite" => {
                    check_arg_count(args.len(), 0, 0).and_then(|_| {
                        self.respond_ok(context);
                        Ok(())
                    })
                }
                b"CONFIG" | b"config" => self.cmd_config(context, args),
                _ => {
                    debug!("Unknown command {:?}", cmd);
                    Err(CommandError::UnknownCommand)
                }
            }
        };

        if let Err(err) = ret {
            self.respond_error(context, err);
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

    fn cmd_multi(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        check_arg_count(args.len(), 0, 0)?;
        if context.is_multi {
            Err(CommandError::InvalidCommand)
        } else {
            context.is_multi = true;
            Ok(())
        }
    }

    fn cmd_exec(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        check_arg_count(args.len(), 0, 1)?;
        let consistency = self.parse_consistency(args.len() > 0, args, 0)?;
        if context.is_multi {
            Ok(self.flush(context, consistency))
        } else {
            Err(CommandError::InvalidCommand)
        }
    }

    fn cmd_config(&self, context: &mut Context, _args: &[&Bytes]) -> Result<(), CommandError> {
        Ok(self.respond_resp(context, RespValue::Array(Default::default())))
    }

    fn cmd_hgetall(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_GET.mark(1);
        check_arg_count(args.len(), 1, 2)?;
        check_key_len(args[0].len())?;
        let consistency = self.parse_consistency(args.len() > 1, args, 1)?;
        Ok(self.get(
            context,
            &args[0],
            consistency,
            cubes::render_map,
        ))
    }

    fn cmd_hset(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_SET.mark(1);
        check_arg_count(args.len(), 3, 4)?;
        check_key_len(args[0].len())?;
        check_key_len(args[1].len())?;
        check_value_len(args[2].len())?;
        let consistency = self.parse_consistency(args.len() > 3, args, 3)?;
        self.set(
            context,
            args[0],
            &mut |i, v, c| {
                let mut map = c.into_map().ok_or(CommandError::TypeError)?;
                let result = map.insert(i, v, args[1].clone(), args[2].clone()) as i64;
                Ok((Cube::Map(map), Some(RespValue::Int(result))))
            },
            consistency,
            false,
            cubes::render_dummy,
        )
    }

    fn cmd_hdel(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_DEL.mark(1);
        check_arg_count(args.len(), 2, 3)?;
        check_key_len(args[0].len())?;
        check_key_len(args[1].len())?;
        let consistency = self.parse_consistency(args.len() > 2, args, 2)?;
        self.set(
            context,
            args[0],
            &mut |i, v, c| {
                let mut map = c.into_map().ok_or(CommandError::TypeError)?;
                let result = map.remove(i, v, &args[1]) as i64;
                Ok((Cube::Map(map), Some(RespValue::Int(result))))
            },
            consistency,
            false,
            cubes::render_dummy,
        )
    }


    fn cmd_smembers(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_GET.mark(1);
        check_arg_count(args.len(), 1, 2)?;
        check_key_len(args[0].len())?;
        let consistency = self.parse_consistency(args.len() > 1, args, 1)?;
        Ok(self.get(
            context,
            &args[0],
            consistency,
            cubes::render_set,
        ))
    }

    fn cmd_sadd(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_SET.mark(1);
        check_arg_count(args.len(), 2, 3)?;
        check_key_len(args[0].len())?;
        check_value_len(args[1].len())?;
        let consistency = self.parse_consistency(args.len() > 2, args, 2)?;
        self.set(
            context,
            &args[0],
            &mut |i, v, c| {
                let mut set = c.into_set().ok_or(CommandError::TypeError)?;
                let result = set.insert(i, v, args[1].clone()) as i64;
                Ok((Cube::Set(set), Some(RespValue::Int(result))))
            },
            consistency,
            false,
            cubes::render_dummy,
        )
    }

    fn cmd_srem(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_DEL.mark(1);
        check_arg_count(args.len(), 2, 3)?;
        check_key_len(args[0].len())?;
        check_key_len(args[1].len())?;
        let consistency = self.parse_consistency(args.len() > 2, args, 2)?;
        self.set(
            context,
            &args[0],
            &mut |i, v, c| {
                let mut set = c.into_set().ok_or(CommandError::TypeError)?;
                let result = set.remove(i, v, &args[1]) as i64;
                Ok((Cube::Set(set), Some(RespValue::Int(result))))
            },
            consistency,
            false,
            cubes::render_dummy,
        )
    }

    fn cmd_spop(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_DEL.mark(1);
        check_arg_count(args.len(), 1, 2)?;
        check_key_len(args[0].len())?;
        let consistency = self.parse_consistency(args.len() > 1, args, 1)?;
        self.set(
            context,
            args[0],
            &mut |i, v, c| {
                let mut set = c.into_set().ok_or(CommandError::TypeError)?;
                let result = set.pop(i, v);
                let resp = result
                    .map(|x| RespValue::Data(x))
                    .unwrap_or_else(|| RespValue::Nil);
                Ok((Cube::Set(set), Some(resp)))
            },
            consistency,
            false,
            cubes::render_dummy,
        )
    }

    fn cmd_get(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_GET.mark(1);
        check_arg_count(args.len(), 1, 2)?;
        check_key_len(args[0].len())?;
        let consistency = self.parse_consistency(args.len() > 1, args, 1)?;
        Ok(self.get(
            context,
            &args[0],
            consistency,
            cubes::render_value_or_counter,
        ))
    }

    fn cmd_set(
        &self,
        context: &mut Context,
        args: &[&Bytes],
        reply_result: bool,
    ) -> Result<(), CommandError> {
        metrics::REQUEST_SET.mark(1);
        check_arg_count(args.len(), 2, 4)?;
        check_key_len(args[0].len())?;
        check_value_len(args[1].len())?;
        let vv = self.parse_vv(args.len() > 2 && !args[2].is_empty(), args, 2)?;
        let consistency = self.parse_consistency(args.len() > 3, args, 3)?;
        self.set(
            context,
            args[0],
            &mut |i, v, c| {
                let mut value = c.into_value().ok_or(CommandError::TypeError)?;
                value.set(i, v, Some(args[1].clone()), &vv);
                let resp = if reply_result {
                    None
                } else {
                    Some(RespValue::Status("OK".into()))
                };
                Ok((Cube::Value(value), resp))
            },
            consistency,
            reply_result,
            if reply_result {
                cubes::render_value_or_counter
            } else {
                cubes::render_dummy
            },
        )
    }

    fn cmd_del(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_DEL.mark(1);
        check_arg_count(args.len(), 1, 3)?;
        check_key_len(args[0].len())?;
        let vv = self.parse_vv(args.len() > 1 && !args[1].is_empty(), args, 1)?;
        let consistency = self.parse_consistency(args.len() > 2, args, 2)?;
        self.set(
            context,
            args[0],
            &mut |i, v, mut c| {
                let result = c.del(i, v, &vv) as i64;
                Ok((c, Some(RespValue::Int(result))))
            },
            consistency,
            false,
            cubes::render_dummy,
        )
    }

    fn cmd_type(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        check_arg_count(args.len(), 1, 2)?;
        let consistency = self.parse_consistency(args.len() > 1, args, 1)?;
        Ok(self.get(
            context,
            &args[0],
            consistency,
            cubes::render_type,
        ))
    }

    fn cmd_cluster(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        check_arg_count(args.len(), 1, 1)?;
        match args[0].as_ref() {
            b"REBALANCE" | b"rebalance" => {
                self.dht.rebalance().unwrap();
                Ok(self.respond_ok(context))
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
                Ok(self.respond_resp(context, RespValue::Array(slots)))
            }
            _ => Err(CommandError::UnknownCommand),
        }
    }

    pub fn respond(&self, context: &mut Context) {
        debug!("Respond request ({}) {:?}", context.token, context.response);
        (&self.response_fn)(replace_default(context));
    }

    pub fn respond_int(&self, context: &mut Context, int: i64) {
        self.respond_resp(context, RespValue::Int(int));
    }

    pub fn respond_resp(&self, context: &mut Context, resp: RespValue) {
        context.response.push(resp);
        self.respond(context);
    }

    pub fn respond_ok(&self, context: &mut Context) {
        self.respond_resp(context, RespValue::Status("OK".into()));
    }

    pub fn respond_error(&self, context: &mut Context, error: CommandError) {
        self.respond_resp(context, error.into());
    }
}

impl Context {
    pub fn respond_int(&mut self, int: i64) {
        self.respond(RespValue::Int(int));
    }

    pub fn respond_ok(&mut self) {
        self.respond(RespValue::Status("OK".into()));
    }

    pub fn respond_error(&mut self, error: CommandError) {
        self.respond(error.into());
    }

    pub fn respond_moved(&mut self, vnode: VNodeId, addr: net::SocketAddr) {
        self.respond(
            RespValue::Error(format!("MOVED {} {}", vnode, addr).into()),
        );
    }

    pub fn respond_ask(&mut self, vnode: VNodeId, addr: net::SocketAddr) {
        self.respond(
            RespValue::Error(format!("ASK {} {}", vnode, addr).into()),
        );
    }
}
