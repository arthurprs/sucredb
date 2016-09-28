use resp::{ByteTendril, RespValue};
use database::{Token, Database};
use version_vector::*;
use bincode::{serde as bincode_serde, SizeLimit};

#[derive(Debug)]
pub enum CommandError {
    Timeout,
    ProtocolError,
}

impl Database {
    pub fn handler_cmd(&self, token: u64, cmd: RespValue) {
        let mut args: [&[u8]; 32] = [b""; 32];
        let mut argc = 0;
        match cmd {
            RespValue::Array(ref a) if a.len() > 0 && a.len() <= args.len() => {
                for v in a {
                    if let &RespValue::Data(ref d) = v {
                        args[argc] = d.as_ref();
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

        let arg0 = args[0];
        let args = &args[1..argc];

        match arg0 {
            b"GET" | b"MGET" => self.cmd_get(token, args),
            b"SET" | b"MSET" => self.cmd_set(token, args),
            b"DEL" | b"MDEL" => self.cmd_del(token, args),
            b"CONFIG" => unimplemented!(),
            _ => unimplemented!(),
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
