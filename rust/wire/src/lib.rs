//! Wire protocol between Chorus clients and a Chorus storage node.
//!
//! Runtime-agnostic: rkyv message types, a write-handle encoding, and a frame
//! codec over [`bytes::BytesMut`]. `PROTOCOL.md` in this crate is the
//! normative description of the protocol.
#![warn(missing_docs)]

mod codec;
mod handle;
// The rkyv derives generate public `Archived*`/`*Resolver` items whose
// fields cannot carry docs; every hand-written item there is documented.
#[allow(missing_docs)]
mod messages;

pub use codec::{
    decode_client_frame, decode_server_frame, encode_client_frame, encode_server_frame, FrameError,
    FRAME_HEADER_LEN, MAX_FRAME_LEN,
};
pub use handle::WriteHandle;
pub use messages::*;

#[cfg(test)]
mod tests;
