use resp::RespValue;
use database::Database;
use version_vector::*;

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
                        unimplemented!();
                    };
                }
            }
            _ => unimplemented!()
        }

        let args = &args[..argc];

        match args[0] {
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
}
