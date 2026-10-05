//! Frame codec: `u32 LE body_len | u32 LE crc32c(body) | rkyv body`.

use bytes::{Buf, BytesMut};
use rkyv::rancor;
use rkyv::util::AlignedVec;

use crate::messages::{ClientFrame, ServerFrame};

/// Bytes in a frame header (`body_len` + `crc32c`).
pub const FRAME_HEADER_LEN: usize = 8;

/// Largest accepted frame body. Larger frames are a protocol violation.
pub const MAX_FRAME_LEN: usize = 64 * 1024 * 1024;

/// Framing failures. Every one is fatal for the connection.
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    /// The body length exceeds [`MAX_FRAME_LEN`]. On decode, the buffer is
    /// left untouched (the stream cannot be resynchronized).
    #[error("frame body of {len} bytes exceeds the {MAX_FRAME_LEN}-byte limit")]
    TooLarge {
        /// Declared or encoded body length.
        len: usize,
    },
    /// The body does not match its CRC32C. The frame was consumed.
    #[error("frame checksum mismatch: header {expected:#010x}, body {actual:#010x}")]
    BadChecksum {
        /// CRC32C from the header.
        expected: u32,
        /// CRC32C computed over the received body.
        actual: u32,
    },
    /// The body is not a valid archive of the expected type. The frame was
    /// consumed.
    #[error("invalid frame body: {0}")]
    InvalidArchive(String),
    /// Serializing a message failed.
    #[error("cannot serialize frame: {0}")]
    Encode(String),
}

/// Encode a complete client frame (header plus body).
pub fn encode_client_frame(frame: &ClientFrame) -> Result<Vec<u8>, FrameError> {
    let body = rkyv::to_bytes::<rancor::Error>(frame)
        .map_err(|error| FrameError::Encode(error.to_string()))?;
    frame_body(&body)
}

/// Encode a complete server frame (header plus body).
pub fn encode_server_frame(frame: &ServerFrame) -> Result<Vec<u8>, FrameError> {
    let body = rkyv::to_bytes::<rancor::Error>(frame)
        .map_err(|error| FrameError::Encode(error.to_string()))?;
    frame_body(&body)
}

/// Decode one client frame from the front of `buf`.
///
/// Returns `Ok(None)` (consuming nothing) when `buf` does not yet hold a whole
/// frame; on success the frame's bytes are consumed.
pub fn decode_client_frame(buf: &mut BytesMut) -> Result<Option<ClientFrame>, FrameError> {
    let Some(body) = take_frame_body(buf)? else {
        return Ok(None);
    };
    rkyv::from_bytes::<ClientFrame, rancor::Error>(&body)
        .map(Some)
        .map_err(|error| FrameError::InvalidArchive(error.to_string()))
}

/// Decode one server frame from the front of `buf`; same contract as
/// [`decode_client_frame`].
pub fn decode_server_frame(buf: &mut BytesMut) -> Result<Option<ServerFrame>, FrameError> {
    let Some(body) = take_frame_body(buf)? else {
        return Ok(None);
    };
    rkyv::from_bytes::<ServerFrame, rancor::Error>(&body)
        .map(Some)
        .map_err(|error| FrameError::InvalidArchive(error.to_string()))
}

fn frame_body(body: &[u8]) -> Result<Vec<u8>, FrameError> {
    if body.len() > MAX_FRAME_LEN {
        return Err(FrameError::TooLarge { len: body.len() });
    }
    let mut out = Vec::with_capacity(FRAME_HEADER_LEN + body.len());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(body).to_le_bytes());
    out.extend_from_slice(body);
    Ok(out)
}

/// Split one verified frame body off the front of `buf`, copied into an
/// aligned buffer so the archive can be validated in place.
///
/// Decoding currently deserializes into owned types. A zero-copy path for the
/// `Append` payload would validate the aligned body with `rkyv::access` and
/// hand the archived `data` slice to the disk layer instead.
fn take_frame_body(buf: &mut BytesMut) -> Result<Option<AlignedVec>, FrameError> {
    if buf.len() < FRAME_HEADER_LEN {
        return Ok(None);
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().expect("4 bytes")) as usize;
    if len > MAX_FRAME_LEN {
        return Err(FrameError::TooLarge { len });
    }
    if buf.len() < FRAME_HEADER_LEN + len {
        buf.reserve(FRAME_HEADER_LEN + len - buf.len());
        return Ok(None);
    }
    let expected = u32::from_le_bytes(buf[4..8].try_into().expect("4 bytes"));
    buf.advance(FRAME_HEADER_LEN);
    let raw = buf.split_to(len);
    let actual = crc32c::crc32c(&raw);
    if actual != expected {
        return Err(FrameError::BadChecksum { expected, actual });
    }
    let mut body = AlignedVec::with_capacity(len);
    body.extend_from_slice(&raw);
    Ok(Some(body))
}
