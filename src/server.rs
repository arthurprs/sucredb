use std::io::{Write, stderr};
use std::net;
use std::str;
use std::error::Error;
use std::time::Duration;
use database::Database;
use rotor::{self, Scope};
use rotor::mio::tcp::{TcpListener, TcpStream};
use rotor_stream::{Accept, Stream, Protocol, Intent, Transport, Exception};
use protocol;

enum Resp {
    Sending,
    Receiving,
}

struct Context {
    // database: Database,
}

pub struct Server {
}

impl Protocol for Resp {
    type Context = Context;
    type Socket = TcpStream;
    type Seed = ();

    fn create(_seed: (), _sock: &mut TcpStream, scope: &mut Scope<Context>) -> Intent<Self> {
        debug!("incomming connection from {:?}", _sock.local_addr().unwrap());
        Intent::of(Resp::Receiving)
            .expect_delimiter(b"\r\n", 1024)
            .deadline(scope.now() + Duration::new(10, 0))
    }

    fn bytes_read(self,
                  transport: &mut Transport<TcpStream>,
                  _end: usize,
                  scope: &mut Scope<Context>)
                  -> Intent<Self> {
        let len = transport.input().len();
        let mut buf = protocol::ByteTendril::new();
        debug!("buffer is {:?} ({})", str::from_utf8(&transport.input()[..]), len);
        let _ = buf.write(&transport.input()[..]);
        let mut parser = protocol::Parser::new(buf);
        match parser.parse() {
            Ok(req) => {
                // parse it and send to correct worker thread?
                info!("received request {:?}", req);
                transport.input().consume(len - parser.bytes_left());
                transport.output().write(b"+OK\r\n").unwrap();
                Intent::of(Resp::Receiving)
                    .expect_delimiter(b"\r\n", 1024)
                    .deadline(scope.now() + Duration::new(10, 0))
            }
            Err(protocol::ProtocolError::Incomplete) => {
                Intent::of(Resp::Receiving)
                    .expect_delimiter_after(len - 1, b"\r\n", 64 * 1024)
                    .deadline(scope.now() + Duration::new(10, 0))
            },
            Err(protocol::ProtocolError::Invalid(err_str)) => {
                warn!("protocol error {}", err_str);
                Intent::done()
            }
        }
    }

    fn bytes_flushed(self,
                     _transport: &mut Transport<TcpStream>,
                     _scope: &mut Scope<Context>)
                     -> Intent<Self> {
        // TODO(tailhook) or maybe start over?
        debug!("bytes_flushed");
        Intent::done()
    }
    fn timeout(self,
               _transport: &mut Transport<TcpStream>,
               _scope: &mut Scope<Context>)
               -> Intent<Self> {
        writeln!(&mut stderr(), "Timeout happened").ok();
        Intent::done()
    }

    fn wakeup(self,
              _transport: &mut Transport<TcpStream>,
              _scope: &mut Scope<Context>)
              -> Intent<Self> {
        unreachable!();
    }
    fn exception(self,
                 _transport: &mut Transport<Self::Socket>,
                 reason: Exception,
                 _scope: &mut Scope<Self::Context>)
                 -> Intent<Self> {
        writeln!(&mut stderr(), "Error: {}", reason).ok();
        Intent::done()
    }
    fn fatal(self, reason: Exception, _scope: &mut Scope<Self::Context>) -> Option<Box<Error>> {
        writeln!(&mut stderr(), "Error: {}", reason).ok();
        None
    }
}

impl Server {
    pub fn new() -> Server {
        Server {}
    }
    pub fn run(&self) {
        let mut event_loop = rotor::Loop::new(&rotor::Config::new()).unwrap();
        let lst = TcpListener::bind(&"127.0.0.1:6379".parse().unwrap()).unwrap();
        event_loop
            .add_machine_with(|scope| Accept::<Stream<Resp>, _>::new(lst, (), scope))
            .unwrap();
        event_loop.run(Context{}).unwrap();
    }
}
