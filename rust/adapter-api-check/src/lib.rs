//! Compile the real adapter outside chorus-client's privacy boundary.
//!
//! This unpublished workspace member deliberately provides only public Chorus
//! exports. Private engine methods or types used by the adapter fail to compile.
//! Unit tests stay in chorus-client; this crate checks production source as a
//! library, with no copied adapter implementation to drift out of sync.

pub use chorus_client::{
    AppendReceipt, Error, ReadOnlyConfig, ReadOnlyFollower, Recovery, SegmentedVolume,
    WalEngineConfig, WalGcHandle, WalHandle, WalSeqNo,
};

#[path = "../../client/src/slatedb.rs"]
#[cfg(not(test))]
pub mod adapter;
