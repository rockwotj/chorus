#![warn(missing_docs)]
//! Database-ready quorum WAL over 1, 3, or 5 GCS Rapid zonal buckets
//! (typically three).
//!
//! Start with [`SegmentedVolume::recover`] using the database's durable
//! checkpoint boundary. Consume the returned [`Recovery`] stream through its
//! fixed end, then call [`Recovery::start`]. Submit caller-numbered opaque records
//! through [`WalHandle::enqueue_append`]; admission returns an
//! [`AppendCompletion`] without waiting for durability. A completion success is
//! durable on a strict-majority quorum of the configured zones. Use
//! [`Error::may_have_committed`] on a completion failure: ambiguous outcomes
//! may replay after takeover, so delivery is at-least-once.
//! [`Error::ActiveSegmentFull`] is different: it is definitive admission
//! backpressure, consumes no sequence number, and leaves the writer healthy.
//! Truncate retained sealed history so a deferred rotation can proceed, then
//! retry the same record.
//!
//! One sequence number identifies one application-encoded transaction record;
//! the WAL never adds another application-level batching layer. Startup replay
//! uses record boundaries through [`WalSeqNo`] and is available only on
//! [`Recovery`]. Once the database has
//! durably checkpointed its own state, call [`WalHandle::truncate_before`] to
//! delete whole sealed segments below that checkpoint. Startup and periodic
//! maintenance retry already-authorized deletion tombstones before repairing
//! missing immutable sealed copies; degraded rotations also schedule a targeted
//! repair. Active segments are never repaired in place, and maintenance never
//! advances the database's checkpoint floor autonomously. See the `database_wal`
//! example for the complete lifecycle.
//! Independent read-replica processes can call
//! [`SegmentedVolume::open_readonly`] to follow sealed history and the
//! quorum-visible active tail without claiming a writer epoch. The follower
//! automatically polls with bidirectional range reads and returns
//! [`Error::ReadOnlyLagged`] if writer truncation overtakes its durable
//! checkpoint.
//! Every fallible public operation returns [`Error`].
//!
//! # Operational constraints
//!
//! A typical production volume uses three Rapid buckets in distinct zones of
//! one region plus a regional bucket for the default GCS manifest store. Bucket
//! arguments are full v2 resource names, and zonal list order is durable replica
//! identity. Current manifests bind that ordered set in `chorus.buckets`;
//! its length is the replica count, and missing, duplicate, or later mismatched
//! bindings are rejected. Physical zone placement is not discoverable through
//! this API and remains an operator check.
//!
//! Rapid zonal buckets have neither Object Versioning nor soft delete, so
//! replacement and truncation deletes are permanent. Archive immutable sealed
//! segments before truncation when the database needs point-in-time recovery.
//! The default GCS manifest directory retains roughly 115 current checksummed
//! segments before rotation defers; custom [`ManifestStore`] implementations
//! may report a larger budget. [`WalEngineConfig::max_active_segment_bytes`]
//! bounds that deferral with non-poisoning [`Error::ActiveSegmentFull`]
//! backpressure.
//!
//! The default GCS manifest is one repeatedly updated object. Treat
//! [`WalEngineConfig::max_segment_bytes`] as the single-pending refill floor:
//! for encoded throughput `T` bytes/s and worst-case provision-plus-fold
//! latency `L`, size it above `T * L` with operational headroom. Exceeding that
//! bound is fail-closed: append dispatch pauses before an unregistered segment
//! can receive records.

#![doc = ""]
#![doc = "# Complete example"]
#![doc = ""]
#![doc = "This is the same source built as the `database_wal` Cargo example."]
#![doc = ""]
#![doc = "```no_run"]
#![doc = include_str!("../examples/database_wal.rs")]
#![doc = "```"]

mod auth;
mod engine;
mod error;
mod grpc;
mod maintenance;
mod manifest;
mod manifest_store;
mod metrics;
mod protocol;
mod record;
mod segment;
mod transport;

#[cfg(test)]
mod grpc_internal_tests;

pub use auth::{AccessTokenSource, BearerAuth, RefreshingAuthConfig};
pub use engine::{AppendCompletion, AppendReceipt, WalEngineConfig, WalHandle};
pub use error::Error;
pub use grpc::GrpcReplicaFactory;
pub use manifest_store::{ManifestStore, ManifestStoreError, ManifestVersion, VersionedManifest};
pub use metrics::{
    CounterFn, GaugeFn, HistogramFn, MetricsRecorder, NoopMetricsRecorder, UpDownCounterFn,
};
pub use protocol::ClientConfig;
pub use segment::{
    ReadOnlyConfig, ReadOnlyFollower, Recovery, RecoveryTimings, RepairReport, SegmentedVolume,
    TruncationReport, WalRecord, WalSeqNo,
};
pub use transport::TransportCode;

/// Transport seam exposed only to the deterministic-simulation harness, which
/// supplies an in-memory `ReplicaFactory` in place of the gRPC transport.
#[cfg(feature = "dst-support")]
pub use transport::{
    AppendToken, LaneDurableChange, ListedObject, Replica, ReplicaFactory, ReplicaRangeRead,
    ReplicaSnapshot, TransportError,
};

/// Helpers for repository probes that intentionally share transport details.
///
/// This module is excluded from the default API and is available only with the
/// `probe-support` Cargo feature.
#[cfg(feature = "probe-support")]
pub mod probe_support {
    pub use crate::grpc::{
        probe_generation_zero_takeover, redirect_routing_token, GenerationZeroOpenObservation,
        GenerationZeroTakeoverProbeResult,
    };
    pub use crate::transport::TransportError;
}

/// Capacity introspection for the deterministic simulation harness.
///
/// Excluded from the default API and available only with the `dst-support`
/// Cargo feature. The harness uses this to mirror the engine's rotation gate:
/// a swap may only begin when the directory can hold both the old tail and the
/// in-flight pending that a swap-window crash would force recovery to seal.
#[cfg(feature = "dst-support")]
pub mod dst_support {
    use crate::manifest::directory_has_room_for;
    use crate::manifest_store::GCS_MAX_DIRECTORY_BYTES;

    /// Whether the GCS-backed segment directory whose entries are currently
    /// `encoded_segments` (the `chorus.segments` register value) can take
    /// `additional` more sealed entries. Matches the byte budget the engine
    /// reserves before a fold CAS, so a harness check and the engine's
    /// `rotation_due` gate agree exactly.
    pub fn gcs_segment_directory_has_room(encoded_segments: &str, additional: usize) -> bool {
        directory_has_room_for(encoded_segments.len(), additional, GCS_MAX_DIRECTORY_BYTES)
    }
}
