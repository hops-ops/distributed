//! Binary wire protocol (normative spec: `PROTOCOL.md`).
//!
//! One message per WebSocket binary frame; the first byte is the message
//! type. [`wire`] holds the primitives, [`messages`] the message codecs and
//! the reference [`ClientMirror`](messages::ClientMirror).

pub mod messages;
pub mod wire;

pub use messages::*;
pub use wire::{DecodeError, Reader, Writer};

/// Length-prefix a frame (`varuint` length) for stream transports that lack
/// message boundaries (`PROTOCOL.md` §1.2).
pub fn frame_stream(message: &[u8]) -> Vec<u8> {
    let mut w = Writer::new();
    w.bytes(message);
    w.into_bytes()
}

/// Split one length-prefixed frame off the front of `buf`; returns the frame
/// and the bytes consumed, or `None` when more data is needed.
pub fn unframe_stream(buf: &[u8]) -> Result<Option<(&[u8], usize)>, DecodeError> {
    let mut r = Reader::new(buf);
    match r.bytes() {
        Ok(frame) => Ok(Some((frame, buf.len() - r.remaining()))),
        Err(DecodeError::UnexpectedEof) => Ok(None),
        Err(e) => Err(e),
    }
}
