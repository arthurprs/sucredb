use std::{mem, str, fmt};
use std::io::Write;
use std::error::Error;
use tendril;

pub type ByteTendril = tendril::Tendril<tendril::fmt::Bytes, tendril::Atomic>;
pub type StrTendril = tendril::Tendril<tendril::fmt::UTF8, tendril::Atomic>;

#[derive(Eq, PartialEq, Debug)]
pub enum RespError {
    Incomplete,
    Invalid(&'static str),
}

impl From<&'static str> for RespError {
    fn from(from: &'static str) -> Self {
        RespError::Invalid(from)
    }
}

pub type RespResult<T> = Result<T, RespError>;

#[derive(Clone)]
pub enum RespValue {
    Nil,
    Int(i64),
    Data(ByteTendril),
    Array(Vec<RespValue>),
    Status(StrTendril),
    Error(StrTendril),
}

impl RespValue {
    pub fn serialize_to<W: Write>(self, f: &mut W) {
        match self {
                RespValue::Nil => write!(f, "$-1\r\n"),
                RespValue::Int(v) => write!(f, ":{}\r\n", v),
                RespValue::Data(v) => {
                    write!(f, "${}\r\n", v.len32()).unwrap();
                    f.write_all(v.as_ref()).unwrap();
                    write!(f, "\r\n")
                }
                RespValue::Array(a) => {
                    write!(f, "*{}\r\n", a.len()).unwrap();
                    for v in a {
                        v.serialize_to(f);
                    }
                    Ok(())
                }
                RespValue::Status(v) => write!(f, "+{}\r\n", v.as_ref()),
                RespValue::Error(v) => write!(f, "-{}\r\n", v.as_ref()),
            }
            .unwrap()
    }
}

impl<T: Error> From<T> for RespValue {
    fn from(from: T) -> Self {
        RespValue::Error(StrTendril::format(format_args!("{}", from)))
    }
}

impl fmt::Debug for RespValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            RespValue::Nil => write!(f, "Nil"),
            RespValue::Int(v) => write!(f, "Int({:?})", v),
            RespValue::Data(ref v) => write!(f, "Data({:?})", String::from_utf8_lossy(v)),
            RespValue::Array(ref b) => {
                try!(write!(f, "Array("));
                try!(f.debug_list().entries(b).finish());
                write!(f, ")")
            }
            RespValue::Status(ref v) => write!(f, "Status({:?})", v.as_ref()),
            RespValue::Error(ref v) => write!(f, "Error({:?})", v.as_ref()),
        }
    }
}

/// The internal redis response parser.
pub struct Parser {
    original_len: usize,
    body: ByteTendril,
}

impl Parser {
    pub fn new<T: Into<ByteTendril>>(body: T) -> Parser {
        let body = body.into();
        Parser {
            original_len: body.len(),
            body: body,
        }
    }

    pub fn bytes_consumed(&self) -> usize {
        self.original_len - self.body.len()
    }

    /// parses a single value out of the stream.  If there are multiple
    /// values you can call this multiple times.
    pub fn parse(&mut self) -> RespResult<RespValue> {
        match try!(self.read_byte()) {
            b'+' => self.parse_status(),
            b':' => self.parse_int(),
            b'$' => self.parse_data(),
            b'*' => self.parse_array(),
            b'-' => self.parse_error(),
            _ => Err("Invalid response when parsing value".into()),
        }
    }

    #[inline]
    fn read_byte(&mut self) -> RespResult<u8> {
        if self.body.len() >= 1 {
            let byte = self.body[0];
            self.body.pop_front(1);
            Ok(byte)
        } else {
            Err(RespError::Incomplete)
        }
    }

    #[inline]
    fn read(&mut self, len: usize) -> RespResult<ByteTendril> {
        if self.body.len() >= len {
            let (first_part, second_part) = Self::split_at(self.body.clone(), len);
            self.body = second_part;
            Ok(first_part)
        } else {
            Err(RespError::Incomplete)
        }
    }

    fn split_at(mut tendril: ByteTendril, position: usize) -> (ByteTendril, ByteTendril) {
        let offset = position as u32;
        let len = tendril.len32() - offset;
        let second_part = tendril.subtendril(offset, len);
        tendril.pop_back(len);
        (tendril, second_part)
    }

    fn read_line(&mut self) -> RespResult<ByteTendril> {
        let nl_pos = match self.body.iter().position(|&b| b == b'\r') {
            Some(nl_pos) => nl_pos,
            None => return Err(RespError::Incomplete),
        };
        let (first_part, second_part) = Self::split_at(try!(self.read(nl_pos + 2)), nl_pos);
        if second_part.as_ref() != b"\r\n" {
            return Err("Invalid line separator".into());
        }

        Ok(first_part)
    }

    fn read_string_line(&mut self) -> RespResult<StrTendril> {
        let line = try!(self.read_line());
        match str::from_utf8(line.as_ref()) {
            Ok(_) => Ok(unsafe { mem::transmute(line) }),
            Err(_) => Err("Expected valid string, got garbage".into()),
        }
    }

    fn read_int_line(&mut self) -> RespResult<i64> {
        let line = try!(self.read_line());
        let line_str = unsafe { str::from_utf8_unchecked(line.as_ref()) };
        match line_str.parse::<i64>() {
            Err(_) => Err("Expected integer, got garbage".into()),
            Ok(value) => Ok(value),
        }
    }

    fn parse_status(&mut self) -> RespResult<RespValue> {
        let line = try!(self.read_string_line());
        Ok(RespValue::Status(line))
    }

    fn parse_int(&mut self) -> RespResult<RespValue> {
        Ok(RespValue::Int(try!(self.read_int_line())))
    }

    fn parse_data(&mut self) -> RespResult<RespValue> {
        let length = try!(self.read_int_line());
        if length < 0 {
            Ok(RespValue::Nil)
        } else {
            let length = length as usize;
            let (data, line_sep) = Self::split_at(try!(self.read(length + 2)), length);
            if line_sep.as_ref() != b"\r\n" {
                return Err("Invalid line separator".into());
            }
            Ok(RespValue::Data(data))
        }
    }

    fn parse_array(&mut self) -> RespResult<RespValue> {
        let length = try!(self.read_int_line());
        if length < 0 {
            Ok(RespValue::Nil)
        } else {
            let mut rv = Vec::with_capacity(length as usize);
            for _ in 0..length {
                let value = try!(self.parse());
                rv.push(value);
            }
            Ok(RespValue::Array(rv))
        }
    }

    fn parse_error(&mut self) -> RespResult<RespValue> {
        let line = try!(self.read_string_line());
        Ok(RespValue::Error(line))
    }
}

#[cfg(test)]
mod tests {
    use super::{RespResult, RespError, Parser, RespValue};
    use std::io::Write;

    fn parse(slice: &[u8]) -> RespResult<RespValue> {
        Parser::new(slice).parse()
    }

    #[test]
    fn parse_incomplete() {
        let r = parse(b"*2\r\n$3\r\nfoo");
        assert_eq_repr!(r.unwrap_err(), RespError::Incomplete);
    }

    #[test]
    fn parse_error() {
        let r = parse(b"-foo\r\n");
        assert_eq_repr!(r.unwrap(), RespValue::Error("foo".into()));

        let r = parse(b"-invalid line sep\r\r");
        assert!(if let RespError::Invalid(_) = r.unwrap_err() {
            true
        } else {
            false
        });
    }

    #[test]
    fn parse_valid_array() {
        let r = parse(b"*2\r\n$3\r\nfoo\r\n$4\r\nbarz\r\n");
        assert!(r.is_ok(), "{:?} not ok", r.unwrap_err());
        assert_eq_repr!(r.unwrap(),
                        RespValue::Array(vec![RespValue::Data(b"foo".as_ref().into()),
                                              RespValue::Data(b"barz".as_ref().into())]));
    }

    #[test]
    fn parser_multiple2() {
        let mut parser =
            Parser::new(b"*2\r\n$3\r\nfoo\r\n$4\r\nbarz\r\n*2\r\n$3\r\nfoo\r\n$4\r\nbarz\r\n"
                .as_ref());
        for _ in 0..2 {
            let r = parser.parse();
            assert!(r.is_ok(), "{:?} not ok", r.unwrap_err());
            assert_eq_repr!(r.unwrap(),
                            RespValue::Array(vec![RespValue::Data(b"foo".as_ref().into()),
                                                  RespValue::Data(b"barz".as_ref().into())]));
        }
        let r = parser.parse();
        assert_eq_repr!(r.unwrap_err(), RespError::Incomplete);
    }

    #[test]
    fn message_response() {
        let mut parser = Parser::new(
            b"*2\r\n*2\r\n:7270781675605147315\r\n$25\r\nmessage 1 from producer 0\r\n*2\r\n:4590316895040267280\r\n$25\r\nmessage 2 from producer 0\r\n".as_ref(),
        );
        let r = parser.parse();
        assert!(r.is_ok(), "{:?} not ok", r.unwrap_err());
        assert_eq!(parser.body.len(), 0);
    }
}
