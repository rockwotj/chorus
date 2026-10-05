//! Chorus storage node: GCS-Rapid-style appendable objects served over the
//! `chorus-wire` TCP protocol.
//!
//! Layers, top to bottom:
//! - [`net`]: listener, per-connection framing, handshake and dispatch.
//! - [`store`]: the in-memory coordination layer (object table, generations,
//!   writer epochs, append sessions, durable tails).
//! - [`backend`]: the persistence boundary. [`backend::MemoryBackend`] keeps
//!   everything in memory; [`disk::DiskBackend`] keeps it in files under a
//!   data directory (io_uring through compio).
//!
//! Everything runs on one single-threaded compio runtime.
#![warn(missing_docs)]

pub mod backend;
pub mod disk;
pub mod net;
pub mod store;

pub use backend::{Backend, MemoryBackend, ObjectKey, ObjectMeta};
pub use disk::DiskBackend;
pub use net::{Server, ServerControl};
pub use store::Store;
