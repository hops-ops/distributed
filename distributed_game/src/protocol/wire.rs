//! Primitive wire encodings (see `PROTOCOL.md` §2).
//!
//! All multi-byte fixed-width numbers are little-endian. `varuint` is
//! unsigned LEB128 (7 bits per byte, low group first, high bit = continue);
//! encoders always emit the minimal form. `varint` is a zigzag-mapped
//! `varuint`. Strings are UTF-8 prefixed by a `varuint` byte length; short
//! strings use a single `u8` length.

use std::fmt;

/// Largest `string`/`bytes` length accepted by decoders (1 MiB).
pub const MAX_BLOB_LEN: usize = 1 << 20;

/// Decoding failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// Input ended mid-value.
    UnexpectedEof,
    /// A varuint exceeded 32 bits.
    VarintOverflow,
    /// A string was not valid UTF-8.
    InvalidUtf8,
    /// Unknown message type byte.
    UnknownMessage(u8),
    /// Unknown field type tag in a schema.
    UnknownFieldType(u8),
    /// Delta referenced an entity kind / id the decoder does not know.
    UnknownEntity(u32),
    /// Structurally invalid value.
    Invalid(&'static str),
    /// Bytes left over after a complete message.
    TrailingBytes(usize),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for DecodeError {}

/// Append-only encoder.
#[derive(Debug, Default, Clone)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    /// Empty writer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Bytes written so far.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// True if nothing was written.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Finish and take the bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    /// Borrow the bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Discard contents, keeping capacity.
    pub fn clear(&mut self) {
        self.buf.clear();
    }

    /// Raw bytes, no prefix.
    pub fn raw(&mut self, bytes: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(bytes);
        self
    }

    /// One byte.
    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.buf.push(v);
        self
    }

    /// `0x00`/`0x01`.
    pub fn bool(&mut self, v: bool) -> &mut Self {
        self.u8(u8::from(v))
    }

    /// Unsigned LEB128 (max 5 bytes).
    pub fn varuint(&mut self, mut v: u32) -> &mut Self {
        loop {
            let byte = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                self.buf.push(byte);
                return self;
            }
            self.buf.push(byte | 0x80);
        }
    }

    /// Zigzag-mapped signed varint: `(v << 1) ^ (v >> 31)`.
    pub fn varint(&mut self, v: i32) -> &mut Self {
        self.varuint(((v << 1) ^ (v >> 31)) as u32)
    }

    /// IEEE-754 binary32, little-endian.
    pub fn f32(&mut self, v: f32) -> &mut Self {
        self.raw(&v.to_le_bytes())
    }

    /// IEEE-754 binary64, little-endian.
    pub fn f64(&mut self, v: f64) -> &mut Self {
        self.raw(&v.to_le_bytes())
    }

    /// `varuint` length + UTF-8.
    pub fn string(&mut self, v: &str) -> &mut Self {
        self.bytes(v.as_bytes())
    }

    /// `u8` length + UTF-8; truncated at a char boundary to <= 255 bytes.
    pub fn short_string(&mut self, v: &str) -> &mut Self {
        let mut end = v.len().min(255);
        while !v.is_char_boundary(end) {
            end -= 1;
        }
        self.u8(end as u8).raw(&v.as_bytes()[..end])
    }

    /// `varuint` length + raw bytes.
    pub fn bytes(&mut self, v: &[u8]) -> &mut Self {
        self.varuint(v.len() as u32).raw(v)
    }
}

/// Cursor over a byte slice.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    /// Read from the start of `buf`.
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// Bytes not yet consumed.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Error unless fully consumed.
    pub fn finish(&self) -> Result<(), DecodeError> {
        match self.remaining() {
            0 => Ok(()),
            n => Err(DecodeError::TrailingBytes(n)),
        }
    }

    /// Next `n` raw bytes.
    pub fn raw(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.remaining() < n {
            return Err(DecodeError::UnexpectedEof);
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    /// One byte.
    pub fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.raw(1)?[0])
    }

    /// Strict bool (`0` or `1`).
    pub fn bool(&mut self) -> Result<bool, DecodeError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(DecodeError::Invalid("bool must be 0 or 1")),
        }
    }

    /// Unsigned LEB128 up to 32 bits.
    pub fn varuint(&mut self) -> Result<u32, DecodeError> {
        let mut value: u32 = 0;
        for index in 0..5 {
            let byte = self.u8()?;
            if index == 4 && byte > 0x0f {
                return Err(DecodeError::VarintOverflow);
            }
            value |= u32::from(byte & 0x7f) << (7 * index);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(DecodeError::VarintOverflow)
    }

    /// Zigzag signed varint.
    pub fn varint(&mut self) -> Result<i32, DecodeError> {
        let raw = self.varuint()?;
        Ok(((raw >> 1) as i32) ^ -((raw & 1) as i32))
    }

    /// binary32 LE.
    pub fn f32(&mut self) -> Result<f32, DecodeError> {
        let bytes = self.raw(4)?;
        Ok(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// binary64 LE.
    pub fn f64(&mut self) -> Result<f64, DecodeError> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.raw(8)?);
        Ok(f64::from_le_bytes(bytes))
    }

    /// `varuint` length + raw bytes.
    pub fn bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let len = self.varuint()? as usize;
        if len > MAX_BLOB_LEN {
            return Err(DecodeError::Invalid("length exceeds 1 MiB"));
        }
        self.raw(len)
    }

    /// `varuint` length + UTF-8.
    pub fn string(&mut self) -> Result<String, DecodeError> {
        let bytes = self.bytes()?;
        String::from_utf8(bytes.to_vec()).map_err(|_| DecodeError::InvalidUtf8)
    }

    /// `u8` length + UTF-8.
    pub fn short_string(&mut self) -> Result<String, DecodeError> {
        let len = self.u8()? as usize;
        String::from_utf8(self.raw(len)?.to_vec()).map_err(|_| DecodeError::InvalidUtf8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varuint_round_trips_and_is_minimal() {
        for (value, hex) in [
            (0u32, "00"),
            (1, "01"),
            (127, "7f"),
            (128, "8001"),
            (300, "ac02"),
            (u32::MAX, "ffffffff0f"),
        ] {
            let mut w = Writer::new();
            w.varuint(value);
            let got: String = w.as_bytes().iter().map(|b| format!("{b:02x}")).collect();
            assert_eq!(got, hex);
            assert_eq!(Reader::new(w.as_bytes()).varuint().unwrap(), value);
        }
        assert_eq!(
            Reader::new(&[0xff, 0xff, 0xff, 0xff, 0x10]).varuint(),
            Err(DecodeError::VarintOverflow)
        );
    }

    #[test]
    fn zigzag_round_trips() {
        for (value, raw) in [(0, 0u32), (-1, 1), (1, 2), (-2, 3), (i32::MIN, u32::MAX)] {
            let mut w = Writer::new();
            w.varint(value);
            assert_eq!(Reader::new(w.as_bytes()).varuint().unwrap(), raw);
            assert_eq!(Reader::new(w.as_bytes()).varint().unwrap(), value);
        }
    }
}
