use std::time::Duration;

use crate::manifest_store::ManifestStoreError;
use crate::protocol::ProtocolError;
use crate::segment::WalSeqNo;
use crate::transport::{TransportCode, TransportError};

/// Failure returned by every fallible `chorus-client` public operation.
///
/// Admission errors are definitive and may be corrected by the caller. In
/// particular, [`ActiveSegmentFull`](Self::ActiveSegmentFull) does not consume
/// the supplied sequence number: truncate retained history so rotation can
/// proceed, then retry the same append. Use [`may_have_committed`](Self::may_have_committed)
/// on an append completion error before deciding whether recovery must resolve
/// an ambiguous outcome.
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The background WAL task was aborted, failed, or shut down.
    #[error("WAL engine is closed")]
    Closed,
    /// Graceful shutdown exceeded its configured deadline and aborted the
    /// remaining owned tasks.
    #[error("WAL shutdown exceeded its {timeout:?} deadline")]
    #[non_exhaustive]
    ShutdownTimeout {
        /// Configured graceful-shutdown deadline.
        timeout: Duration,
    },
    /// A gRPC channel could not be constructed or connected.
    #[error("storage connection failed: {0}")]
    Connection(String),
    /// The configured identity provider could not issue a token.
    #[error("access-token source failed: {0}")]
    TokenSource(String),
    /// A bearer token cannot be represented in an ASCII gRPC header.
    #[error("access token cannot be encoded as gRPC metadata")]
    InvalidToken,
    /// Refreshing would busy-loop because an interval was zero.
    #[error("auth refresh and retry intervals must be non-zero")]
    InvalidRefreshInterval,
    /// A WAL engine capacity or size limit was invalid or inconsistent.
    #[error("invalid WAL configuration: {0}")]
    InvalidConfig(&'static str),
    /// Fewer than a strict-majority quorum of replicas could be read.
    #[error("segment discovery could not read a quorum")]
    NoReadQuorum,
    /// An operation could not reach a strict-majority replica quorum.
    #[error("operation did not reach a replica quorum")]
    NoQuorum,
    /// Listed objects or decoded finalized bytes did not form one chain.
    #[error("invalid segment catalog: {0}")]
    InvalidCatalog(String),
    /// The application supplied a checkpoint below its prior checkpoint.
    #[error("checkpoint regressed from {current:?} to {requested:?}")]
    #[non_exhaustive]
    CheckpointRegression {
        /// Checkpoint previously supplied to this writer.
        current: WalSeqNo,
        /// Regressing checkpoint supplied by the caller.
        requested: WalSeqNo,
    },
    /// Finalized bytes or inferred segment bounds disagree with record framing.
    #[error("invalid sealed segment data: {0}")]
    InvalidSegmentData(String),
    /// Recovery witnesses contain different bytes at one record boundary.
    #[error("recovery witnesses contain different bytes at record {record_index}")]
    #[non_exhaustive]
    ConflictingPrefix {
        /// First record index where the recovered witnesses disagree.
        record_index: usize,
    },
    /// Recovery did not find the committed number of records.
    #[error("recovery prefix has {actual} records, expected at least {expected}")]
    #[non_exhaustive]
    RecoveryPrefixTooShort {
        /// Minimum record count required by the committed boundary.
        expected: usize,
        /// Record count recovered from the available witnesses.
        actual: usize,
    },
    /// Recovered bytes disagree with the manifest's committed SHA-256 seal.
    #[error("recovered seal digest {actual} does not match committed digest {expected}")]
    #[non_exhaustive]
    SealDigestMismatch {
        /// Digest committed by the manifest.
        expected: String,
        /// Digest computed from the recovered bytes.
        actual: String,
    },
    /// Recovered bytes disagree with the manifest's committed CRC32C.
    #[error("recovered seal CRC32C {actual:08x} does not match committed CRC32C {expected:08x}")]
    #[non_exhaustive]
    SealCrc32cMismatch {
        /// CRC32C committed by the manifest.
        expected: u32,
        /// CRC32C computed from the recovered bytes.
        actual: u32,
    },
    /// The manifest register is malformed or inconsistent with this volume.
    #[error("manifest register is invalid: {0}")]
    InvalidManifest(String),
    /// The manifest cannot retain another sealed-segment directory entry.
    #[error(
        "the manifest segment directory is full: truncate the WAL to free retained sealed \
         segments before sealing again"
    )]
    SegmentDirectoryFull,
    /// The manifest register remained unavailable through its retry budget.
    #[error("manifest register is unavailable")]
    ManifestUnavailable,
    /// A caller-supplied manifest register operation failed.
    #[error(transparent)]
    ManifestStore(#[from] ManifestStoreError),
    /// An internal commit notification path closed before reporting a result.
    #[error("commit pipeline closed before reporting a result")]
    PipelineClosed,
    /// The low-level segment writer is already finalized.
    #[error("segment writer is sealed")]
    Finalized,
    /// An internal invariant failed without a more stable public taxonomy.
    #[error("internal WAL invariant failed: {0}")]
    Internal(String),
    /// An indeterminate append broke the ordered commit prefix.
    #[error("WAL writer is poisoned by an indeterminate record; restart recovery is required")]
    Poisoned,
    /// This writer lost ownership to a newer manifest epoch.
    #[error("WAL writer was fenced: {0}")]
    Fenced(String),
    /// The caller supplied a sequence number other than the next admission.
    #[error("WAL append is out of order: expected {expected:?}, received {actual:?}")]
    #[non_exhaustive]
    OutOfOrder {
        /// Exact sequence number required for the next admission.
        expected: WalSeqNo,
        /// Sequence number supplied by the caller.
        actual: WalSeqNo,
    },
    /// A record exceeded the configured application-payload limit.
    #[error("WAL record contains {actual} payload bytes, exceeding configured maximum {max}")]
    #[non_exhaustive]
    RecordTooLarge {
        /// Configured per-record payload-byte limit.
        max: usize,
        /// Payload bytes supplied by the caller.
        actual: usize,
    },
    /// The active object cannot admit another encoded record without crossing
    /// its configured hard ceiling.
    ///
    /// The writer remains healthy and the sequence number was not consumed.
    /// This is backpressure, not poison: truncating old sealed segments may
    /// free manifest-directory room so the engine can rotate and resume
    /// admission. A permanently failed seal still requires restart recovery
    /// before rotation can resume.
    #[error(
        "active WAL segment holds {current} encoded bytes; admitting {requested} more would \
         exceed the configured ceiling {max}"
    )]
    #[non_exhaustive]
    ActiveSegmentFull {
        /// Configured hard active-object ceiling.
        max: usize,
        /// Encoded bytes already charged to the active segment.
        current: usize,
        /// Encoded bytes required by the rejected record.
        requested: usize,
    },
    /// No further contiguous sequence number can be represented.
    #[error("WAL sequence-number space is exhausted")]
    SequenceExhausted,
    /// Startup replay did not reach its fixed end successfully.
    #[error("recovery replay must complete before starting the WAL")]
    RecoveryIncomplete,
    /// Provider operation failed with a stable retry/fencing classification.
    #[error("zone {zone}: {code:?}: {message}")]
    #[non_exhaustive]
    Transport {
        /// Replica zone where the operation failed.
        zone: usize,
        /// Stable protocol-facing classification.
        code: TransportCode,
        /// Provider diagnostic text; correctness never matches this string.
        message: String,
    },
}

impl Error {
    /// Whether this error can be observed after an append was admitted but
    /// before recovery determines its durable outcome.
    ///
    /// `true` is deliberately conservative. [`Closed`](Self::Closed),
    /// [`Internal`](Self::Internal), and transport failures can also arise
    /// outside append completion, but callers handling an admitted append must
    /// assume it may replay after takeover. [`Poisoned`](Self::Poisoned),
    /// [`Fenced`](Self::Fenced), and [`PipelineClosed`](Self::PipelineClosed)
    /// always require that same treatment.
    ///
    /// Admission and configuration errors, sequence errors,
    /// [`ActiveSegmentFull`](Self::ActiveSegmentFull), [`NoQuorum`](Self::NoQuorum),
    /// manifest/catalog validation errors, and low-level finalized-writer
    /// errors are definitive for the attempted append.
    pub fn may_have_committed(&self) -> bool {
        matches!(
            self,
            Self::Closed
                | Self::Internal(_)
                | Self::Poisoned
                | Self::Fenced(_)
                | Self::PipelineClosed
                | Self::Transport { .. }
        )
    }
}

impl From<TransportError> for Error {
    fn from(error: TransportError) -> Self {
        Self::Transport {
            zone: error.zone,
            code: error.code,
            message: error.message,
        }
    }
}

impl From<ProtocolError> for Error {
    fn from(error: ProtocolError) -> Self {
        match error {
            ProtocolError::ReplicaCount => Self::InvalidConfig("replica count must be 1, 3, or 5"),
            ProtocolError::NoQuorum => Self::NoQuorum,
            ProtocolError::Poisoned => Self::Poisoned,
            ProtocolError::Fenced(message) => Self::Fenced(message),
            ProtocolError::ConflictingPrefix { record_index } => {
                Self::ConflictingPrefix { record_index }
            }
            ProtocolError::RecoveryPrefixTooShort { expected, actual } => {
                Self::RecoveryPrefixTooShort { expected, actual }
            }
            ProtocolError::SealDigestMismatch { expected, actual } => {
                Self::SealDigestMismatch { expected, actual }
            }
            ProtocolError::SealCrc32cMismatch { expected, actual } => {
                Self::SealCrc32cMismatch { expected, actual }
            }
            ProtocolError::InvalidManifest(message) => Self::InvalidManifest(message),
            ProtocolError::SegmentDirectoryFull => Self::SegmentDirectoryFull,
            ProtocolError::ManifestStore(error) => Self::ManifestStore(error),
            ProtocolError::ManifestUnavailable => Self::ManifestUnavailable,
            ProtocolError::PipelineClosed => Self::PipelineClosed,
            ProtocolError::Finalized => Self::Finalized,
            ProtocolError::Record(error) => Self::InvalidSegmentData(error.to_string()),
            ProtocolError::Transport(error) => error.into(),
        }
    }
}

impl From<crate::record::RecordError> for Error {
    fn from(error: crate::record::RecordError) -> Self {
        Self::InvalidSegmentData(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{Error, WalSeqNo};
    use crate::manifest_store::ManifestStoreError;
    use crate::protocol::ProtocolError;
    use crate::transport::{TransportCode, TransportError};

    #[test]
    fn append_outcome_predicate_is_conservative_only_for_ambiguous_classes() {
        for error in [
            Error::Closed,
            Error::Internal("opaque invariant".into()),
            Error::Poisoned,
            Error::Fenced("new owner".into()),
            Error::PipelineClosed,
            Error::Transport {
                zone: 0,
                code: TransportCode::Unavailable,
                message: "response lost".into(),
            },
        ] {
            assert!(error.may_have_committed(), "{error}");
        }

        for error in [
            Error::InvalidConfig("bad limit"),
            Error::NoQuorum,
            Error::SegmentDirectoryFull,
            Error::ManifestUnavailable,
            Error::Finalized,
            Error::OutOfOrder {
                expected: WalSeqNo::ZERO,
                actual: WalSeqNo::record(1),
            },
            Error::ActiveSegmentFull {
                max: 8,
                current: 8,
                requested: 1,
            },
        ] {
            assert!(!error.may_have_committed(), "{error}");
        }
    }

    #[test]
    fn protocol_errors_keep_structured_public_classifications() {
        assert!(matches!(
            Error::from(ProtocolError::NoQuorum),
            Error::NoQuorum
        ));
        assert!(matches!(
            Error::from(ProtocolError::Fenced("new owner".into())),
            Error::Fenced(message) if message == "new owner"
        ));
        assert!(matches!(
            Error::from(ProtocolError::SegmentDirectoryFull),
            Error::SegmentDirectoryFull
        ));
        assert!(matches!(
            Error::from(ProtocolError::ManifestUnavailable),
            Error::ManifestUnavailable
        ));
        assert!(matches!(
            Error::from(ProtocolError::InvalidManifest("bad register".into())),
            Error::InvalidManifest(message) if message == "bad register"
        ));
        assert!(matches!(
            Error::from(ProtocolError::Finalized),
            Error::Finalized
        ));
        assert!(matches!(
            Error::from(ProtocolError::ConflictingPrefix { record_index: 7 }),
            Error::ConflictingPrefix { record_index: 7 }
        ));
        assert!(matches!(
            Error::from(ProtocolError::RecoveryPrefixTooShort {
                expected: 4,
                actual: 3,
            }),
            Error::RecoveryPrefixTooShort {
                expected: 4,
                actual: 3,
            }
        ));
        assert!(matches!(
            Error::from(ProtocolError::SealDigestMismatch {
                expected: "expected".into(),
                actual: "actual".into(),
            }),
            Error::SealDigestMismatch { expected, actual }
                if expected == "expected" && actual == "actual"
        ));
        assert!(matches!(
            Error::from(ProtocolError::SealCrc32cMismatch {
                expected: 1,
                actual: 2,
            }),
            Error::SealCrc32cMismatch {
                expected: 1,
                actual: 2,
            }
        ));
        assert!(matches!(
            Error::from(ProtocolError::ManifestStore(
                ManifestStoreError::Backend("failed".into())
            )),
            Error::ManifestStore(ManifestStoreError::Backend(message)) if message == "failed"
        ));
        assert!(matches!(
            Error::from(ProtocolError::PipelineClosed),
            Error::PipelineClosed
        ));
        assert!(matches!(
            Error::from(ProtocolError::Transport(TransportError {
                zone: 2,
                code: TransportCode::Unavailable,
                message: "down".into(),
            })),
            Error::Transport {
                zone: 2,
                code: TransportCode::Unavailable,
                message,
            } if message == "down"
        ));
    }
}
