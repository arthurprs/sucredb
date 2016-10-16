use resp::{ByteTendril, RespValue};
use database::{Token, Database};
use version_vector::*;
use std::str;
use bincode::{serde as bincode_serde, SizeLimit};

#[derive(Debug)]
pub enum CommandError {
    Timeout,
    ProtocolError,
    UnknownCommand,
}

fn quick_int(bytes: &[u8]) -> Result<i64, CommandError> {
    if bytes.len() == 1 {
        match bytes[0] {
            b'0'...b'9' => Ok((bytes[0] - b'0') as i64),
            _ => Err(CommandError::ProtocolError),
        }
    } else {
        unsafe { str::from_utf8_unchecked(bytes) }
            .parse::<i64>()
            .map_err(|_| CommandError::ProtocolError)
    }
}

impl Database {
    pub fn handler_cmd(&self, token: u64, cmd: RespValue) {
        let mut arg_: [&[u8]; 32] = [b""; 32];
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

        match arg0 {
            b"GET" | b"MGET" => self.cmd_get(token, args),
            b"SET" | b"MSET" => self.cmd_set(token, args),
            b"DEL" | b"MDEL" => self.cmd_del(token, args),
            b"ECHO" if args.len() > 0 => {
                (&self.response_fn)(token, RespValue::Data(args[0].into()))
            }
            b"CLUSTER" => self.cmd_cluster(token, args),
            _ => self.respond_error(token, CommandError::UnknownCommand),
        };
    }

    fn cmd_get(&self, token: u64, args: &[&[u8]]) {
        for key in args {
            self.get(token, key);
        }
    }

    fn cmd_set(&self, token: u64, args: &[&[u8]]) {
        for w in args.chunks(3) {
            self.set(token, w[0], Some(w[1]), VersionVector::new());
        }
    }

    fn cmd_del(&self, token: u64, args: &[&[u8]]) {
        for w in args.chunks(2) {
            self.set(token, w[0], None, VersionVector::new());
        }
    }

    fn cmd_cluster(&self, token: u64, args: &[&[u8]]) {
        match args {
            &[b"REBALANCE"] => {
                self.dht.claim(self.dht.node(), ());
                self.respond_ok(token);
            }
            _ => self.respond_error(token, CommandError::UnknownCommand),
        }
    }

    pub fn respond_ok(&self, token: Token) {
        (&self.response_fn)(token, RespValue::Status("OK".into()));
    }

    pub fn respond_error(&self, token: Token, error: CommandError) {
        (&self.response_fn)(token, RespValue::Error(format!("{:?}", error).into()));
    }

    pub fn respond_get(&self, token: Token, dcc: DottedCausalContainer<Vec<u8>>) {
        (&self.response_fn)(token, dcc_to_resp(dcc));
    }

    pub fn respond_set(&self, token: Token, dcc: DottedCausalContainer<Vec<u8>>) {
        (&self.response_fn)(token, dcc_to_resp(dcc));
    }
}

fn dcc_to_resp(dcc: DottedCausalContainer<Vec<u8>>) -> RespValue {
    let mut buffer =
        ByteTendril::with_capacity(1024 + dcc.values().map(|v| v.len()).sum::<usize>() as u32);
    bincode_serde::serialize_into(&mut buffer, &dcc, SizeLimit::Infinite).unwrap();
    RespValue::Data(buffer)
}
