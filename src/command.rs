use bincode;
use bytes::Bytes;
use config;
use cubes::{self, Cube};
use database::{Context, Database};
use metrics::{self, Meter};
use resp::RespValue;
use std::convert::TryInto;
use std::net;
use types::*;
use utils::{assume_str, replace_default};
use version_vector::*;

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
    InvalidExec,
    InvalidCommand,
    InvalidMultiCommand,
    MultiplePartitions,
    MultipleKeyMutations,
    Unavailable,
}

impl Into<RespValue> for CommandError {
    fn into(self) -> RespValue {
        RespValue::Error(format!("{:?}", self).into())
    }
}

fn parse_int<T: ::std::str::FromStr + Default>(
    try: bool,
    args: &[&Bytes],
    i: usize,
) -> Result<T, CommandError> {
    if try {
        assume_str(&args[i])
            .parse()
            .map_err(|_| CommandError::InvalidIntValue)
    } else {
        Ok(Default::default())
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
    pub fn handler_cmd(&self, mut context: Context) {
        let cmd = context.commands.pop().unwrap();
        if let Err(e) = self.handle_cmd(&mut context, cmd) {
            context.clear();
            self.respond_error(&mut context, e);
        }
    }

    fn handle_cmd(&self, context: &mut Context, cmd: RespValue) -> Result<(), CommandError> {
        debug!("Processing ({:?}) {:?}", context.token, cmd);
        let mut args = Vec::new();
        match cmd {
            RespValue::Array(ref a) => {
                args.reserve_exact(a.len());
                for v in a.iter() {
                    if let &RespValue::Data(ref b) = v {
                        args.push(b);
                    } else {
                        args.clear();
                        break;
                    }
                }
            }
            _ => (),
        }

        if args.is_empty() {
            return Err(CommandError::ProtocolError);
        }

        let arg0 = args[0];
        let args = &args[1..];

        if context.is_exec {
            match arg0.as_ref() {
                b"CSET" | b"cset" => self.cmd_cset(context, args),
                b"INCRBY" | b"incrby" => self.cmd_incrby(context, args),
                b"SET" | b"set" => self.cmd_set(context, args, false),
                b"HSET" | b"hset" => self.cmd_hset(context, args),
                b"HDEL" | b"hdel" => self.cmd_hdel(context, args),
                b"SADD" | b"sadd" => self.cmd_sadd(context, args),
                b"SREM" | b"srem" => self.cmd_srem(context, args),
                b"GETSET" | b"getset" => self.cmd_set(context, args, true),
                b"DEL" | b"del" => self.cmd_del(context, args),
                _ => {
                    debug!("Unknown command for multi {:?}", cmd);
                    Err(CommandError::InvalidMultiCommand)
                }
            }
        } else if context.is_multi {
            match arg0.as_ref() {
                b"EXEC" | b"exec" => self.cmd_exec(context, args),
                _ => {
                    context.commands.push(cmd.clone());
                    Ok(self.respond_resp(context, RespValue::Status("QUEUED".into())))
                }
            }
        } else {
            match arg0.as_ref() {
                b"GET" | b"get" => self.cmd_get(context, args),
                b"MGET" | b"mget" => self.cmd_mget(context, args),
                b"SET" | b"set" => self.cmd_set(context, args, false),
                b"CGET" | b"cget" => self.cmd_cget(context, args),
                b"CSET" | b"cset" => self.cmd_cset(context, args),
                b"INCRBY" | b"incrby" => self.cmd_incrby(context, args),
                b"HGETALL" | b"hgetall" => self.cmd_hgetall(context, args),
                b"HSET" | b"hset" => self.cmd_hset(context, args),
                b"HDEL" | b"hdel" => self.cmd_hdel(context, args),
                b"SMEMBERS" | b"smembers" => self.cmd_smembers(context, args),
                b"SADD" | b"sadd" => self.cmd_sadd(context, args),
                b"SREM" | b"srem" => self.cmd_srem(context, args),
                b"GETSET" | b"getset" => self.cmd_set(context, args, true),
                b"DEL" | b"del" => self.cmd_del(context, args),
                b"CLUSTER" | b"cluster" => self.cmd_cluster(context, args),
                b"TYPE" | b"type" => self.cmd_type(context, args),
                b"MULTI" | b"multi" => self.cmd_multi(context, args),
                b"EXEC" | b"exec" => self.cmd_exec(context, args),
                b"ECHO" | b"echo" => Ok(self.respond_resp(context, cmd.clone())),
                b"PING" | b"ping" => Ok(self.respond_resp(context, RespValue::Data("PONG".into()))),
                b"ASKING" | b"asking" | b"READONLY" | b"readonly" | b"READWRITE" | b"readwrite" => {
                    check_arg_count(args.len(), 0, 0).and_then(|_| Ok(self.respond_ok(context)))
                }
                b"CONFIG" | b"config" => self.cmd_config(context, args),
                _ => {
                    debug!("Unknown command {:?}", cmd);
                    Err(CommandError::UnknownCommand)
                }
            }
        }
    }

    fn parse_vv(
        &self,
        try: bool,
        args: &[&Bytes],
        i: usize,
    ) -> Result<VersionVector, CommandError> {
        if try && !args[i].is_empty() {
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
            args[i]
                .as_ref()
                .try_into()
                .map_err(|_| CommandError::InvalidConsistencyValue)?
        } else {
            self.config.consistency_read
        })
    }

    fn cmd_multi(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        assert!(!context.is_multi);
        context.is_multi = true;
        check_arg_count(args.len(), 0, 0)?;
        Ok(self.respond_ok(context))
    }

    fn cmd_exec(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        if !context.is_multi {
            return Err(CommandError::InvalidExec);
        }
        check_arg_count(args.len(), 0, 1)?;
        let consistency = self.parse_consistency(args.len() > 0, args, 0)?;
        assert!(!context.is_exec);
        context.is_exec = true;
        let mut cmds = replace_default(&mut context.commands);
        for cmd in cmds.drain(..) {
            debug!("token:{} exec: {:?}", context.token, cmd);
            self.handle_cmd(context, cmd)?;
        }
        context.commands = cmds;
        self.set_flush(context, consistency)
    }

    fn cmd_config(&self, context: &mut Context, _args: &[&Bytes]) -> Result<(), CommandError> {
        Ok(self.respond_resp(context, RespValue::Array(Default::default())))
    }

    fn cmd_hgetall(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_GET.mark(1);
        check_arg_count(args.len(), 1, 2)?;
        check_key_len(args[0].len())?;
        let consistency = self.parse_consistency(args.len() > 1, args, 1)?;
        self.get(context, args[0], consistency, Box::new(cubes::render_map))
    }

    fn cmd_hset(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_SET.mark(1);
        check_arg_count(args.len(), 3, 4)?;
        check_key_len(args[0].len())?;
        check_key_len(args[1].len())?;
        check_value_len(args[2].len())?;
        let hash_key = args[1].clone();
        let hash_value = args[2].clone();
        let consistency = self.parse_consistency(args.len() > 3, args, 3)?;
        self.set(
            context,
            args[0],
            Box::new(move |i, v, c: Cube| {
                let mut map = c.into_map().ok_or(CommandError::TypeError)?;
                let result = map.insert(i, v, hash_key, hash_value) as i64;
                Ok((Cube::Map(map), Some(RespValue::Int(result))))
            }),
            consistency,
            false,
            None,
        )
    }

    fn cmd_hdel(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_DEL.mark(1);
        check_arg_count(args.len(), 2, 3)?;
        check_key_len(args[0].len())?;
        check_key_len(args[1].len())?;
        let hash_key = args[1].clone();
        let consistency = self.parse_consistency(args.len() > 2, args, 2)?;
        self.set(
            context,
            args[0],
            Box::new(move |i, v, c: Cube| {
                let mut map = c.into_map().ok_or(CommandError::TypeError)?;
                let result = map.remove(i, v, &hash_key) as i64;
                Ok((Cube::Map(map), Some(RespValue::Int(result))))
            }),
            consistency,
            false,
            None,
        )
    }

    fn cmd_smembers(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_GET.mark(1);
        check_arg_count(args.len(), 1, 2)?;
        check_key_len(args[0].len())?;
        let consistency = self.parse_consistency(args.len() > 1, args, 1)?;
        self.get(context, args[0], consistency, Box::new(cubes::render_set))
    }

    fn cmd_sadd(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_SET.mark(1);
        check_arg_count(args.len(), 2, 3)?;
        check_key_len(args[0].len())?;
        check_value_len(args[1].len())?;
        let set_value = args[1].clone();
        let consistency = self.parse_consistency(args.len() > 2, args, 2)?;
        self.set(
            context,
            args[0],
            Box::new(move |i, v, c: Cube| {
                let mut set = c.into_set().ok_or(CommandError::TypeError)?;
                let result = set.insert(i, v, set_value) as i64;
                Ok((Cube::Set(set), Some(RespValue::Int(result))))
            }),
            consistency,
            false,
            None,
        )
    }

    fn cmd_srem(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_DEL.mark(1);
        check_arg_count(args.len(), 2, 3)?;
        check_key_len(args[0].len())?;
        check_value_len(args[1].len())?;
        let set_value = args[1].clone();
        let consistency = self.parse_consistency(args.len() > 2, args, 2)?;
        self.set(
            context,
            args[0],
            Box::new(move |i, v, c: Cube| {
                let mut set = c.into_set().ok_or(CommandError::TypeError)?;
                let result = set.remove(i, v, &set_value) as i64;
                Ok((Cube::Set(set), Some(RespValue::Int(result))))
            }),
            consistency,
            false,
            None,
        )
    }

    fn cmd_get(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_GET.mark(1);
        check_arg_count(args.len(), 1, 2)?;
        check_key_len(args[0].len())?;
        let consistency = self.parse_consistency(args.len() > 1, args, 1)?;
        self.get(context, args[0], consistency, Box::new(cubes::render_value))
    }

    fn cmd_mget(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        assert!(!context.is_multi && !context.is_exec);
        context.is_multi = true;
        context.is_exec = true;
        metrics::REQUEST_GET.mark(1);
        check_arg_count(args.len(), 1, 100)?;
        let key_count: usize = parse_int(args.len() > 0, args, 0)?;
        if key_count >= args.len() {
            return Err(CommandError::InvalidCommand);
        }
        let keys = &args[1..1 + key_count];
        let consistency = self.parse_consistency(args.len() > 1 + key_count, args, 1 + key_count)?;
        for key in keys {
            check_key_len(key.len())?;
        }
        self.mget(context, keys, consistency, Box::new(cubes::render_value))
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
        let value = args[1].clone();
        let vv = self.parse_vv(args.len() > 2, args, 2)?;
        let consistency = self.parse_consistency(args.len() > 3, args, 3)?;
        self.set(
            context,
            args[0],
            Box::new(move |i, v, c: Cube| {
                let mut cube_value = c.into_value().ok_or(CommandError::TypeError)?;
                cube_value.set(i, v, Some(value), &vv);
                let resp = if reply_result {
                    None
                } else {
                    Some(RespValue::Status("OK".into()))
                };
                Ok((Cube::Value(cube_value), resp))
            }),
            consistency,
            reply_result,
            if reply_result {
                Some(Box::new(cubes::render_value))
            } else {
                None
            },
        )
    }

    fn cmd_del(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_DEL.mark(1);
        check_arg_count(args.len(), 1, 3)?;
        check_key_len(args[0].len())?;
        let vv = self.parse_vv(args.len() > 1, args, 1)?;
        let consistency = self.parse_consistency(args.len() > 2, args, 2)?;
        self.set(
            context,
            args[0],
            Box::new(move |i, v, mut c: Cube| {
                let result = c.del(i, v, &vv) as i64;
                Ok((c, Some(RespValue::Int(result))))
            }),
            consistency,
            false,
            None,
        )
    }

    fn cmd_cset(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_SET.mark(1);
        check_arg_count(args.len(), 2, 3)?;
        check_key_len(args[0].len())?;
        let value: i64 = parse_int(args.len() > 1, args, 1)?;
        let consistency = self.parse_consistency(args.len() > 2, args, 2)?;
        self.set(
            context,
            args[0],
            Box::new(move |i, v, c: Cube| {
                let mut counter = c.into_counter().ok_or(CommandError::TypeError)?;
                counter.clear(i, v);
                counter.inc(i, v, value);
                Ok((Cube::Counter(counter), Some(RespValue::Status("OK".into()))))
            }),
            consistency,
            false,
            None,
        )
    }

    fn cmd_cget(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_GET.mark(1);
        check_arg_count(args.len(), 1, 2)?;
        check_key_len(args[0].len())?;
        let consistency = self.parse_consistency(args.len() > 1, args, 1)?;
        self.get(
            context,
            args[0],
            consistency,
            Box::new(cubes::render_counter),
        )
    }

    fn cmd_incrby(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        metrics::REQUEST_SET.mark(1);
        check_arg_count(args.len(), 2, 3)?;
        check_key_len(args[0].len())?;
        let inc: i64 = parse_int(args.len() > 1, args, 1)?;
        let consistency = self.parse_consistency(args.len() > 2, args, 2)?;
        self.set(
            context,
            args[0],
            Box::new(move |i, v, c: Cube| {
                let mut counter = c.into_counter().ok_or(CommandError::TypeError)?;
                counter.inc(i, v, inc);
                Ok((Cube::Counter(counter), Some(RespValue::Status("OK".into()))))
            }),
            consistency,
            false,
            None,
        )
    }

    fn cmd_type(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        check_arg_count(args.len(), 1, 2)?;
        let consistency = self.parse_consistency(args.len() > 1, args, 1)?;
        self.get(context, args[0], consistency, Box::new(cubes::render_type))
    }

    fn cmd_cluster(&self, context: &mut Context, args: &[&Bytes]) -> Result<(), CommandError> {
        check_arg_count(args.len(), 1, 1)?;
        match args[0].as_ref() {
            b"CONNECTIONS" | b"connections" => {
                let conns = self.fabric.connections();
                let resp_conns = conns.into_iter().map(|x| RespValue::Int(x as _)).collect();
                Ok(self.respond_resp(context, RespValue::Array(resp_conns)))
            },
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

    pub fn respond_resp(&self, context: &mut Context, resp: RespValue) {
        context.response.push(resp);
        self.respond(context);
    }

    pub fn respond_int(&self, context: &mut Context, int: i64) {
        self.respond_resp(context, RespValue::Int(int));
    }

    pub fn respond_ok(&self, context: &mut Context) {
        self.respond_resp(context, RespValue::Status("OK".into()));
    }

    pub fn respond_error(&self, context: &mut Context, error: CommandError) {
        self.respond_resp(context, error.into());
    }

    pub fn respond_moved(&self, context: &mut Context, vnode: VNodeNo, addr: net::SocketAddr) {
        self.respond_resp(
            context,
            RespValue::Error(format!("MOVED {} {}", vnode, addr).into()),
        );
    }

    pub fn respond_ask(&self, context: &mut Context, vnode: VNodeNo, addr: net::SocketAddr) {
        self.respond_resp(
            context,
            RespValue::Error(format!("ASK {} {}", vnode, addr).into()),
        );
    }
}
