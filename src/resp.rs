use std::{str, fmt};
use std::io::{self, Write};
use std::error::Error;
use bytes::Bytes;
use utils::assume_str;

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

#[derive(Clone, PartialEq)]
pub enum RespValue {
    Nil,
    Int(i64),
    Data(Bytes),
    Array(Vec<RespValue>),
    Status(Bytes),
    Error(Bytes),
}

impl RespValue {
    pub fn serialized_size(&self) -> usize {
        match *self {
            RespValue::Nil => "$-1\r\n".len(),
            RespValue::Int(_) => ":-9223372036854775808\r\n".len(),
            RespValue::Data(ref v) => "$4294967296\r\n".len() + v.len() + "\r\n".len(),
            RespValue::Array(ref a) => {
                "*4294967296\r\n".len() + a.iter().map(Self::serialized_size).sum::<usize>()
            }
            RespValue::Status(ref v) |
            RespValue::Error(ref v) => "+".len() + v.len() + "\r\n".len(),
        }
    }

    pub fn serialize_into<W: Write>(self, f: &mut W) -> io::Result<()> {
        match self {
            RespValue::Nil => write!(f, "$-1\r\n"),
            RespValue::Int(v) => write!(f, ":{}\r\n", v),
            RespValue::Data(v) => {
                write!(f, "${}\r\n", v.len())?;
                f.write_all(v.as_ref())?;
                write!(f, "\r\n")
            }
            RespValue::Array(a) => {
                write!(f, "*{}\r\n", a.len())?;
                for v in a {
                    v.serialize_into(f)?;
                }
                Ok(())
            }
            RespValue::Status(v) => {
                write!(f, "+")?;
                f.write_all(v.as_ref())?;
                write!(f, "\r\n")
            }
            RespValue::Error(v) => {
                write!(f, "-")?;
                f.write_all(v.as_ref())?;
                write!(f, "\r\n")
            }
        }
    }
}

impl<T: Error> From<T> for RespValue {
    fn from(from: T) -> Self {
        RespValue::Error(format!("{}", from).into())
    }
}

impl fmt::Debug for RespValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            RespValue::Nil => write!(f, "Nil"),
            RespValue::Int(v) => write!(f, "Int({:?})", v),
            RespValue::Data(ref v) => write!(f, "Data({:?})", v),
            RespValue::Array(ref b) => {
                write!(f, "Array(")?;
                f.debug_list().entries(b).finish()?;
                write!(f, ")")
            }
            RespValue::Status(ref v) => write!(f, "Status({:?})", v),
            RespValue::Error(ref v) => write!(f, "Error({:?})", v),
        }
    }
}

/// The internal redis response parser.
pub struct Parser {
    consumed: usize,
    body: Bytes,
}

impl Parser {
    pub fn new<T: AsRef<[u8]>>(body: T) -> RespResult<Parser> {
        let valid_to = Self::speculate_buffer(body.as_ref())?;
        Ok(Parser {
            consumed: 0,
            body: body.as_ref()[..valid_to].into(),
        })
    }

    // Quickly speculate a buffer, checking whatever it has a complete resp objects or not.
    // If succesfull returns the resp objects length in bytes.
    fn speculate_buffer(buf: &[u8]) -> RespResult<usize> {
        if buf.len() < 3 {
            return Err(RespError::Incomplete);
        }
        if &buf[buf.len() - 2..] == b"\r\n" {
            return Ok(buf.len());
        }
        let mut valid = 0;
        let mut i = 0;
        let mut values_pending = 0;
        while i < buf.len() {
            match buf[i] {
                b'$' | b'*' => {
                    let is_multi = buf[i] == b'*';
                    let mut len = 0i64;
                    i += 1;
                    while i < buf.len() {
                        match buf[i] {
                            b'0'...b'9' => len = len * 10 + (buf[i] - b'0') as i64,
                            b'-' => {
                                // only valid negative len is -1
                                len = -1;
                                i += 2;
                                break;
                            }
                            b'\r' => break,
                            _ => return Err(RespError::Invalid("Invalid digit")),
                        }
                        i += 1;
                    }
                    if len >= 0 {
                        if is_multi {
                            values_pending = len + 1;
                        } else {
                            i += 2 + len as usize;
                        }
                    }
                }
                b':' | b'+' | b'-' => {
                    i += 1;
                    while i < buf.len() && buf[i] != b'\r' {
                        i += 1;
                    }
                }
                b'\r' => {
                    i += 2;
                    continue;
                }
                _ => return Err(RespError::Invalid("Invalid prefix")),
            }
            // skip delimiter
            i += 2;
            if values_pending > 0 {
                values_pending -= 1;
            }
            if values_pending == 0 && i <= buf.len() {
                valid = i;
            }
        }
        if valid != 0 {
            Ok(valid)
        } else {
            Err(RespError::Incomplete)
        }
    }

    pub fn consumed(&self) -> usize {
        self.consumed
    }

    /// parses a single value out of the stream.  If there are multiple
    /// values you can call this multiple times.
    pub fn parse(&mut self) -> RespResult<RespValue> {
        let saved_len = self.body.len();
        let value = self.parse_value();
        if value.is_ok() {
            self.consumed += saved_len - self.body.len();
        }
        value
    }

    fn parse_value(&mut self) -> RespResult<RespValue> {
        match self.read_byte()? {
            b'$' => self.parse_data(),
            b'*' => self.parse_array(),
            b':' => self.parse_int(),
            b'+' => self.parse_status(),
            b'-' => self.parse_error(),
            c => {
                if c == b'\r' && self.read_byte()? == b'\n' {
                    return self.parse_value();
                }
                debug!(
                    "Invalid prefix {:?}{:?} when parsing value",
                    c as char,
                    String::from_utf8_lossy(self.body.as_ref())
                );
                Err("Invalid prefix when parsing value".into())
            }
        }
    }

    #[inline]
    fn read_byte(&mut self) -> RespResult<u8> {
        if self.body.len() >= 1 {
            let byte = self.body[0];
            self.body = self.body.slice_from(1);
            Ok(byte)
        } else {
            Err(RespError::Incomplete)
        }
    }

    #[inline]
    fn read(&mut self, len: usize) -> RespResult<Bytes> {
        if self.body.len() >= len {
            Ok(self.body.split_to(len))
        } else {
            Err(RespError::Incomplete)
        }
    }

    fn read_with_separator(&mut self, len: usize) -> RespResult<Bytes> {
        let result = self.read(len + 2)?;
        if &result[len..] != b"\r\n" {
            Err("Invalid line separator".into())
        } else {
            Ok(result.slice_to(len))
        }
    }

    fn read_line(&mut self) -> RespResult<Bytes> {
        let nl_pos = match self.body.iter().position(|&b| b == b'\r') {
            Some(nl_pos) => nl_pos,
            None => return Err(RespError::Incomplete),
        };
        Ok(self.read_with_separator(nl_pos)?)
    }

    fn read_int_line(&mut self) -> RespResult<i64> {
        let line = self.read_line()?;
        match assume_str(line.as_ref()).parse::<i64>() {
            Err(_) => Err("Expected integer, got garbage".into()),
            Ok(value) => Ok(value),
        }
    }

    fn parse_status(&mut self) -> RespResult<RespValue> {
        Ok(RespValue::Status(self.read_line()?))
    }

    fn parse_int(&mut self) -> RespResult<RespValue> {
        Ok(RespValue::Int(self.read_int_line()?))
    }

    fn parse_data(&mut self) -> RespResult<RespValue> {
        let length = self.read_int_line()?;
        if length < 0 {
            Ok(RespValue::Nil)
        } else {
            let data = self.read_with_separator(length as usize)?;
            Ok(RespValue::Data(data))
        }
    }

    fn parse_array(&mut self) -> RespResult<RespValue> {
        let length = self.read_int_line()?;
        if length < 0 {
            Ok(RespValue::Nil)
        } else {
            let mut rv = Vec::with_capacity(length as usize);
            for _ in 0..length {
                rv.push(self.parse_value()?);
            }
            Ok(RespValue::Array(rv))
        }
    }

    fn parse_error(&mut self) -> RespResult<RespValue> {
        Ok(RespValue::Error(self.read_line()?))
    }
}

#[cfg(test)]
mod tests {
    use super::{RespResult, RespError, Parser, RespValue};

    fn parse(slice: &[u8]) -> RespResult<RespValue> {
        Parser::new(slice)?.parse()
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
        assert_eq_repr!(
            r.unwrap(),
            RespValue::Array(vec![
                RespValue::Data(b"foo".as_ref().into()),
                RespValue::Data(b"barz".as_ref().into()),
            ])
        );
    }

    #[test]
    fn parser_multiple2() {
        let mut parser = Parser::new(
            b"*2\r\n$3\r\nfoo\r\n$4\r\nbarz\r\n*2\r\n$3\r\nfoo\r\n$4\r\nbarz\r\n"
                .as_ref(),
        ).unwrap();
        for _ in 0..2 {
            let r = parser.parse();
            assert!(r.is_ok(), "{:?} not ok", r.unwrap_err());
            assert_eq_repr!(
                r.unwrap(),
                RespValue::Array(vec![
                    RespValue::Data(b"foo".as_ref().into()),
                    RespValue::Data(b"barz".as_ref().into()),
                ])
            );
        }
        let r = parser.parse();
        assert_eq_repr!(r.unwrap_err(), RespError::Incomplete);
    }

    #[test]
    fn message_response() {
        let mut parser = Parser::new(
            b"*2\r\n*2\r\n:7270781675605147315\r\n$25\r\nmessage 1 from producer 0\r\n*2\r\n:4590316895040267280\r\n$25\r\nmessage 2 from producer 0\r\n".as_ref(),
        ).unwrap();
        let r = parser.parse();
        assert!(r.is_ok(), "{:?} not ok", r.unwrap_err());
        assert_eq!(parser.body.len(), 0);
    }
}
