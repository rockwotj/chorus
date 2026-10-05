//! Chorus storage node: GCS-Rapid-style appendable objects served over the
//! `chorus-wire` TCP protocol.
//!
//! Layers, top to bottom:
//! - [`net`]: listener, per-connection framing, handshake and dispatch.
//! - [`store`]: the in-memory coordination layer (object table, generations,
//!   writer epochs, append sessions, durable tails).
//! - [`backend`]: the persistence boundary. [`backend::MemoryBackend`] keeps
//!   everything in memory.
//!
//! Everything runs on one single-threaded compio runtime.
#![warn(missing_docs)]

pub mod backend;
pub mod net;
pub mod store;

pub use backend::{Backend, MemoryBackend, ObjectKey, ObjectMeta};
pub use net::{Server, ServerControl};
pub use store::Store;
