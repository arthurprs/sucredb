use std::io::{Write, stderr};
use std::str;
use std::error::Error;
use database::Database;
use rotor::{self, Scope};
use rotor::mio::tcp::{TcpListener, TcpStream};
use rotor_stream::{Accept, Stream, Protocol, Intent, Transport, Exception};
use resp;

enum RespConnection {
    Sending,
    Receiving,
}

struct Context {
    // database: Database,
}

pub struct Server {

}

impl Protocol for RespConnection {
    type Context = Context;
    type Socket = TcpStream;
    type Seed = ();

    fn create(_seed: (), _sock: &mut TcpStream, _scope: &mut Scope<Context>) -> Intent<Self> {
        debug!("incomming connection from {:?}", _sock.local_addr().unwrap());
        Intent::of(RespConnection::Receiving).expect_delimiter(b"\r\n", 1024)
    }

    fn bytes_read(self, transport: &mut Transport<TcpStream>, _end: usize,
                  _scope: &mut Scope<Context>)
                  -> Intent<Self> {
        let buf_len = transport.input().len();
        debug!("buffer is {:?} ({})", str::from_utf8(&transport.input()[..]), buf_len);
        let mut parser = resp::Parser::new(&transport.input()[..]);
        match parser.parse() {
            Ok(req) => {
                // parse it and send to correct worker thread?
                info!("received request {:?}", req);
                transport.input().consume(parser.bytes_consumed());
                transport.output().write(b"+OK\r\n").unwrap();
                Intent::of(RespConnection::Receiving).expect_delimiter(b"\r\n", 1024)
            }
            Err(resp::RespError::Incomplete) => {
                Intent::of(RespConnection::Receiving)
                    .expect_delimiter_after(buf_len - 1, b"\r\n", 64 * 1024)
            }
            Err(resp::RespError::Invalid(err_str)) => {
                warn!("protocol error {}", err_str);
                Intent::done()
            }
        }
    }

    fn bytes_flushed(self, _transport: &mut Transport<TcpStream>, _scope: &mut Scope<Context>)
                     -> Intent<Self> {
        // TODO(tailhook) or maybe start over?
        debug!("bytes_flushed");
        Intent::done()
    }
    fn timeout(self, _transport: &mut Transport<TcpStream>, _scope: &mut Scope<Context>)
               -> Intent<Self> {
        writeln!(&mut stderr(), "Timeout happened").ok();
        Intent::done()
    }

    fn wakeup(self, _transport: &mut Transport<TcpStream>, _scope: &mut Scope<Context>)
              -> Intent<Self> {
        unreachable!();
    }
    fn exception(self, _transport: &mut Transport<Self::Socket>, reason: Exception,
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
        event_loop.add_machine_with(|scope| Accept::<Stream<RespConnection>, _>::new(lst, (), scope))
            .unwrap();
        event_loop.run(Context {}).unwrap();
    }
}
