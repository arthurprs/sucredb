use std::mem;
use futures::{Future, Poll};

/// Tries to read some bytes directly into the given `buf` at offset `at`
//// in asynchronous manner, returning a future type.
///
/// The returned future will resolve to both the I/O stream as well as the
/// buffer, `at` and amount of bytes read, once the read operation is completed.
pub fn read_at<R, T>(rd: R, buf: T, at: usize) -> ReadAt<R, T>
where
    R: ::std::io::Read,
    T: AsMut<[u8]>,
{
    ReadAt {
        state: ReadAtState::Pending {
            rd: rd,
            buf: buf,
            at: at,
        },
    }
}

enum ReadAtState<R, T> {
    Pending { rd: R, buf: T, at: usize },
    Empty,
}

/// A future which can be used to easily read available number of bytes to fill
/// a buffer.
pub struct ReadAt<R, T> {
    state: ReadAtState<R, T>,
}

impl<R, T> Future for ReadAt<R, T>
where
    R: ::std::io::Read,
    T: AsMut<[u8]>,
{
    type Item = (R, T, usize, usize);
    type Error = ::std::io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let nread = match self.state {
            ReadAtState::Pending {
                ref mut rd,
                ref mut buf,
                at,
            } => try_nb!(rd.read(&mut buf.as_mut()[at..])),
            ReadAtState::Empty => panic!("poll a Read after it's done"),
        };

        if nread == 0 {
            return Err(::std::io::Error::new(::std::io::ErrorKind::UnexpectedEof, "eof"));
        }

        match mem::replace(&mut self.state, ReadAtState::Empty) {
            ReadAtState::Pending { rd, buf, at } => Ok((rd, buf, at, nread).into()),
            ReadAtState::Empty => panic!("invalid internal state"),
        }
    }
}
