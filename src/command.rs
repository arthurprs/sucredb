use std::num::ParseIntError;
use std::net;
use std::convert::TryInto;
use bytes::Bytes;
use bincode;
use resp::RespValue;
use database::{Context, Database};
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
    pub fn handler_cmd(&self, context: Context, cmd: RespValue) {
        debug!("Processing ({:?}) {:?}", context.token, cmd);
        let token = context.token;
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

        // if context.is_multi {
        //     let ret = match arg0.as_ref() {
        //         b"EXEC"| b"exec" => self.cmd_exec(context, args),
        //         _ => context.push_cmd(cmd.clone()),
        //     };
        //     if let Err(err) = ret {
        //         self.respond_error(context, err);
        //     } else {
        //         self.respond(context, RespValue::Status("QUEUED".into()));
        //     }
        //     return;
        // }

        let ret = match arg0.as_ref() {
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
                self.respond(context, cmd.clone());
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
        };

        if let Err(err) = ret {
            // NOTE that the context here is recreated
            self.respond_error(Context::new(token), err);
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

    fn cmd_multi(&self, context: Context, args: &[&Bytes]) -> Result<(), CommandError> {
        check_arg_count(args.len(), 0, 0)?;
        if context.is_multi {
            Err(CommandError::InvalidCommand)
        } else {
            // context.is_multi = true;
            Ok(())
        }
    }

    fn cmd_exec(&self, context: Context, args: &[&Bytes]) -> Result<(), CommandError> {
        check_arg_count(args.len(), 0, 1)?;
        let _consistency = self.parse_consistency(args.len() > 0, args, 0)?;
        if context.is_multi {
            unimplemented!()
        } else {
            Err(CommandError::InvalidCommand)
        }
    }

    fn cmd_config(&self, context: Context, _args: &[&Bytes]) -> Result<(), CommandError> {
        Ok(self.respond(context, RespValue::Array(Default::default())))
    }

    fn cmd_hgetall(&self, context: Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_GET.mark(1);
        check_arg_count(args.len(), 1, 2)?;
        check_key_len(args[0].len())?;
        let consistency = self.parse_consistency(args.len() > 1, args, 1)?;
        Ok(self.get(
            context,
            args[0].as_ref(),
            consistency,
            cubes::render_map,
        ))
    }

    fn cmd_hset(&self, context: Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_SET.mark(1);
        check_arg_count(args.len(), 3, 4)?;
        check_key_len(args[0].len())?;
        check_key_len(args[1].len())?;
        check_value_len(args[2].len())?;
        let consistency = self.parse_consistency(args.len() > 3, args, 3)?;
        Ok(self.set(
            context,
            args[0].as_ref(),
            &mut |i, v, c| {
                let mut map = c.into_map().ok_or(CommandError::TypeError)?;
                let result = map.insert(i, v, args[1].clone(), args[2].clone()) as i64;
                Ok((Cube::Map(map), Some(RespValue::Int(result))))
            },
            consistency,
            false,
            cubes::render_dummy,
        ))
    }

    fn cmd_hdel(&self, context: Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_DEL.mark(1);
        check_arg_count(args.len(), 2, 3)?;
        check_key_len(args[0].len())?;
        check_key_len(args[1].len())?;
        let consistency = self.parse_consistency(args.len() > 2, args, 2)?;
        Ok(self.set(
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
        ))
    }


    fn cmd_smembers(&self, context: Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_GET.mark(1);
        check_arg_count(args.len(), 1, 2)?;
        check_key_len(args[0].len())?;
        let consistency = self.parse_consistency(args.len() > 1, args, 1)?;
        Ok(self.get(
            context,
            args[0].as_ref(),
            consistency,
            cubes::render_set,
        ))
    }

    fn cmd_sadd(&self, context: Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_SET.mark(1);
        check_arg_count(args.len(), 2, 3)?;
        check_key_len(args[0].len())?;
        check_value_len(args[1].len())?;
        let consistency = self.parse_consistency(args.len() > 2, args, 2)?;
        Ok(self.set(
            context,
            args[0].as_ref(),
            &mut |i, v, c| {
                let mut set = c.into_set().ok_or(CommandError::TypeError)?;
                let result = set.insert(i, v, args[1].clone()) as i64;
                Ok((Cube::Set(set), Some(RespValue::Int(result))))
            },
            consistency,
            false,
            cubes::render_dummy,
        ))
    }

    fn cmd_srem(&self, context: Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_DEL.mark(1);
        check_arg_count(args.len(), 2, 3)?;
        check_key_len(args[0].len())?;
        check_key_len(args[1].len())?;
        let consistency = self.parse_consistency(args.len() > 2, args, 2)?;
        Ok(self.set(
            context,
            args[0],
            &mut |i, v, c| {
                let mut set = c.into_set().ok_or(CommandError::TypeError)?;
                let result = set.remove(i, v, &args[1]) as i64;
                Ok((Cube::Set(set), Some(RespValue::Int(result))))
            },
            consistency,
            false,
            cubes::render_dummy,
        ))
    }

    fn cmd_spop(&self, context: Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_DEL.mark(1);
        check_arg_count(args.len(), 1, 2)?;
        check_key_len(args[0].len())?;
        let consistency = self.parse_consistency(args.len() > 1, args, 1)?;
        Ok(self.set(
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
        ))
    }

    fn cmd_get(&self, context: Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_GET.mark(1);
        check_arg_count(args.len(), 1, 2)?;
        check_key_len(args[0].len())?;
        let consistency = self.parse_consistency(args.len() > 1, args, 1)?;
        Ok(self.get(
            context,
            args[0].as_ref(),
            consistency,
            cubes::render_value_or_counter,
        ))
    }

    fn cmd_set(
        &self,
        context: Context,
        args: &[&Bytes],
        reply_result: bool,
    ) -> Result<(), CommandError> {
        metrics::REQUEST_SET.mark(1);
        check_arg_count(args.len(), 2, 4)?;
        check_key_len(args[0].len())?;
        check_value_len(args[1].len())?;
        let vv = self.parse_vv(args.len() > 2 && !args[2].is_empty(), args, 2)?;
        let consistency = self.parse_consistency(args.len() > 3, args, 3)?;
        Ok(self.set(
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
        ))
    }

    fn cmd_del(&self, context: Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_DEL.mark(1);
        check_arg_count(args.len(), 1, 3)?;
        check_key_len(args[0].len())?;
        let vv = self.parse_vv(args.len() > 1 && !args[1].is_empty(), args, 1)?;
        let consistency = self.parse_consistency(args.len() > 2, args, 2)?;
        Ok(self.set(
            context,
            args[0],
            &mut |i, v, mut c| {
                let result = c.del(i, v, &vv) as i64;
                Ok((c, Some(RespValue::Int(result))))
            },
            consistency,
            false,
            cubes::render_dummy,
        ))
    }

    fn cmd_type(&self, context: Context, args: &[&Bytes]) -> Result<(), CommandError> {
        check_arg_count(args.len(), 1, 2)?;
        let consistency = self.parse_consistency(args.len() > 1, args, 1)?;
        Ok(self.get(
            context,
            args[0].as_ref(),
            consistency,
            cubes::render_type,
        ))
    }

    fn cmd_cluster(&self, context: Context, args: &[&Bytes]) -> Result<(), CommandError> {
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
                Ok(self.respond(context, RespValue::Array(slots)))
            }
            _ => Err(CommandError::UnknownCommand),
        }
    }

    pub fn respond(&self, mut context: Context, resp: RespValue) {
        debug!("Respond request ({}) {:?}", context.token, resp);
        context.push_response(resp);
        (&self.response_fn)(context);
    }

    pub fn respond_int(&self, context: Context, int: i64) {
        self.respond(context, RespValue::Int(int));
    }

    pub fn respond_ok(&self, context: Context) {
        self.respond(context, RespValue::Status("OK".into()));
    }

    pub fn respond_error(&self, context: Context, error: CommandError) {
        self.respond(context, error.into());
    }

    pub fn respond_moved(&self, context: Context, vnode: VNodeId, addr: net::SocketAddr) {
        self.respond(
            context,
            RespValue::Error(format!("MOVED {} {}", vnode, addr).into()),
        );
    }

    pub fn respond_ask(&self, context: Context, vnode: VNodeId, addr: net::SocketAddr) {
        self.respond(
            context,
            RespValue::Error(format!("ASK {} {}", vnode, addr).into()),
        );
    }
}
