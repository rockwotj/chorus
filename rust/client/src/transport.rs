use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Metadata key selecting the durable segment and record encoding.
pub const META_FORMAT: &str = "chorus.format";
/// Current durable object format written by this crate.
pub const FORMAT_VERSION: &str = "1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Provider-neutral object metadata returned during segment discovery.
pub struct ListedObject {
    /// Replica zone assigned by the factory.
    pub zone: usize,
    /// Object name relative to the bucket resource.
    pub name: String,
    /// Immutable provider generation used for guarded deletion.
    pub generation: i64,
    /// Whether the provider reports the object as finalized.
    pub finalized: bool,
    /// Custom metadata containing the object format marker.
    pub metadata: HashMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Full object read used by recovery and replay validation.
pub struct ReplicaSnapshot {
    /// Replica zone.
    pub zone: usize,
    /// Provider object generation.
    pub generation: i64,
    /// Provider metadata generation.
    pub metageneration: i64,
    /// Authoritative durable tail for the open stream, derived from write
    /// responses or bytes read, never from `GetObject.size` while open.
    pub persisted_size: i64,
    /// Whether the provider finalized the object.
    pub finalized: bool,
    /// Provider-computed CRC32C of the complete object, when supplied.
    pub crc32c: Option<u32>,
    /// Segment-format metadata.
    pub metadata: HashMap<String, String>,
    /// Bytes visible through the object read path.
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Byte range observed through the provider's bidirectional object-read path.
pub struct ReplicaRangeRead {
    /// Replica zone.
    pub zone: usize,
    /// Object generation returned with the read.
    pub generation: i64,
    /// Requested bytes currently visible at the open object's durable tail.
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// State for one live zonal append stream, generation-bound once resumed.
///
/// This is not a distributed ownership record. Logical writer identity comes
/// from the manifest's epoch and owner; conditional segment creation provides
/// data-plane exclusivity for the manifest-selected object. This value carries
/// only the physical preconditions and persisted offset needed by one lane.
pub struct AppendToken {
    /// Replica zone.
    pub zone: usize,
    /// Object generation bound to this append stream. A retained conditional-
    /// create stream learns this lazily only if it must be resumed.
    pub generation: Option<i64>,
    /// Metadata generation that guarded the fresh append open. This is also
    /// initially unknown for a retained create stream.
    pub metageneration: Option<i64>,
    /// Next append offset learned from `persisted_size` responses.
    pub persisted_size: i64,
    /// Session handle returned by the fresh append open. Presenting it on
    /// later opens resumes the same server-side session instead of issuing
    /// another takeover: handle-free opens are object *mutations* and are
    /// rate limited per object by the live service.
    pub write_handle: Option<Bytes>,
}

#[derive(Clone, Debug)]
pub(crate) struct PackedAppendMessage {
    pub(crate) relative_offset: i64,
    pub(crate) content: Bytes,
    pub(crate) crc32c: u32,
}

/// An append group whose immutable wire messages were packed once before
/// replica dispatch, shared across lanes. Public because it appears in the
/// [`Replica::lane_send_packed`] signature exposed to the simulation harness.
#[derive(Clone, Debug)]
pub struct PackedAppend {
    chunks: Box<[Bytes]>,
    messages: Box<[PackedAppendMessage]>,
    len: usize,
}

impl PackedAppend {
    pub(crate) fn new(chunks: Vec<Bytes>, messages: Vec<PackedAppendMessage>, len: usize) -> Self {
        Self {
            chunks: chunks.into_boxed_slice(),
            messages: messages.into_boxed_slice(),
            len,
        }
    }

    pub(crate) fn chunks(&self) -> &[Bytes] {
        &self.chunks
    }

    pub(crate) fn messages(&self) -> &[PackedAppendMessage] {
        &self.messages
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Stable transport classification used by retry and fencing logic.
pub enum TransportCode {
    /// Object was not found.
    NotFound,
    /// Conditional create found an existing object.
    AlreadyExists,
    /// The request itself was invalid for the provider API surface.
    InvalidArgument,
    /// A precondition failed; on append/open this is a terminal writer fence,
    /// including both takeover revocation and finalized-object rejection.
    FailedPrecondition,
    /// GCS redirected or aborted an operation. Rich zonal bidirectional
    /// redirects are consumed by the gRPC transport before reaching protocol
    /// code.
    Aborted,
    /// Requested offset lies outside the provider's accepted range.
    OutOfRange,
    /// GCS throttled the request with HTTP 429 / `RESOURCE_EXHAUSTED` because
    /// a per-object mutation-rate or per-project quota was exhausted. The
    /// request remains valid and retry-with-backoff can succeed once quota
    /// state resets; this is not a permanent rejection.
    ResourceExhausted,
    /// The provider does not implement the required operation.
    Unimplemented,
    /// Stored bytes or checksums are corrupt.
    DataLoss,
    /// The provider response cannot distinguish absence from a selector or
    /// routing failure. Recovery must fail closed rather than retrying away or
    /// interpreting this observation as a missing object.
    Ambiguous,
    /// No valid credential was supplied.
    Unauthenticated,
    /// The credential lacks bucket permission.
    PermissionDenied,
    /// Temporary service or zone outage.
    Unavailable,
    /// Operation exceeded its deadline.
    DeadlineExceeded,
    /// Unclassified internal transport failure.
    Internal,
}

impl TransportCode {
    /// Whether retrying the same operation can be safe.
    pub fn transient(self) -> bool {
        matches!(
            self,
            Self::ResourceExhausted | Self::Unavailable | Self::DeadlineExceeded | Self::Internal
        )
    }

    /// Whether an append path must stop the current writer incarnation.
    pub fn fences_writer(self) -> bool {
        matches!(self, Self::FailedPrecondition | Self::Aborted)
    }
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("zone {zone}: {code:?}: {message}")]
/// Transport failure annotated with replica zone and retry classification.
pub struct TransportError {
    /// Replica zone where the operation failed.
    pub zone: usize,
    /// Stable protocol-facing classification.
    pub code: TransportCode,
    /// Provider diagnostic text. Correctness logic must not match this string.
    pub message: String,
}

/// Outcome of awaiting durable progress on a lane: the new durable byte offset,
/// and any error observed in the same step (a response and a stream error can be
/// observed together, so both are reported).
#[derive(Clone, Debug)]
pub struct LaneDurableChange {
    /// Durable byte offset now reported by the session.
    pub persisted_size: i64,
    /// Error observed alongside the durable progress, if any.
    pub error: Option<TransportError>,
}

#[async_trait]
/// Creates object-specific replicas and lists one zonal bucket.
///
/// Implement this trait to plug in a storage backend with GCS-equivalent
/// generation, finalization, listing, and append-takeover semantics.
pub trait ReplicaFactory: Send + Sync {
    /// Globally unique bucket name, without project or location qualifiers.
    fn bucket_name(&self) -> &str;

    /// Bind the factory's bucket and channel to one object name.
    fn replica(&self, object: &str) -> Arc<dyn Replica>;

    /// Strongly consistently list objects under `prefix`.
    async fn list(&self, prefix: &str) -> Result<Vec<ListedObject>, TransportError>;
}

#[async_trait]
/// Storage operations required by the quorum protocol for one zonal object.
pub trait Replica: Send + Sync {
    /// Read object bytes and metadata. For open objects, implementations must
    /// not derive `persisted_size` from provider metadata that hides appends.
    async fn snapshot(&self) -> Result<ReplicaSnapshot, TransportError>;

    /// Read the currently visible suffix of an appendable object starting at
    /// `offset` through the provider's bidirectional range-read API.
    ///
    /// The default keeps non-production test doubles source-compatible. A
    /// readonly follower requires an implementation and treats this error as a
    /// missing replica observation.
    async fn read_range(&self, offset: i64) -> Result<ReplicaRangeRead, TransportError> {
        Err(TransportError {
            zone: 0,
            code: TransportCode::Unimplemented,
            message: format!("bidirectional range reads are unavailable at offset {offset}"),
        })
    }

    /// Read object metadata only, without the content read. Metadata reads are
    /// content-blind: they succeed even when the stored bytes are rotted, so
    /// repair can learn the generation of a copy whose `snapshot` fails with
    /// `DATA_LOSS`. The returned snapshot carries empty bytes and must never
    /// be used to infer a durable tail.
    async fn stat(&self) -> Result<ReplicaSnapshot, TransportError>;

    /// Conditionally create a new appendable object.
    async fn create_appendable(
        &self,
        metadata: HashMap<String, String>,
    ) -> Result<ReplicaSnapshot, TransportError>;

    /// Conditionally create a new appendable object and retain that create
    /// RPC as its live append session.
    async fn create_append_session(
        &self,
        metadata: HashMap<String, String>,
    ) -> Result<AppendToken, TransportError>;

    /// Conditionally create a finalized, non-appendable control object with
    /// an empty body and the supplied metadata. The manifest register lives
    /// in a regional bucket, where appendable objects do not exist; it is
    /// created once with this call and afterwards mutated only through
    /// [`Replica::update_register`].
    async fn create_register(
        &self,
        metadata: HashMap<String, String>,
    ) -> Result<ReplicaSnapshot, TransportError>;

    /// Conditionally replace the register's custom metadata, guarded by its
    /// metageneration alone. The register is created exactly once and never
    /// deleted or recreated, so its generation is constant and the
    /// metageneration by itself names one register state.
    async fn update_register(
        &self,
        metageneration: i64,
        metadata: HashMap<String, String>,
    ) -> Result<ReplicaSnapshot, TransportError>;

    /// Re-learn the durable tail after a lane disturbance by resuming the
    /// append session (with its handle when available, else a guarded fresh
    /// open). This remains the authoritative writer-lane tail even though
    /// readonly followers can independently observe flushed bytes through
    /// bidirectional reads.
    async fn resume_tail(&self, token: &mut AppendToken) -> Result<i64, TransportError>;

    /// Open a fresh, handle-free append stream guarded by the observed object.
    /// The open
    /// is the server-enforced fence: it revokes any previously open writer and
    /// returns the authoritative durable tail from `persisted_size`.
    async fn takeover(&self, observed: &ReplicaSnapshot) -> Result<AppendToken, TransportError>;

    /// Fence the current live generation and return its authoritative durable
    /// tail in the same RPC.
    ///
    /// The latest-generation lookup confirms identity or unambiguous absence;
    /// its tail-blind size is never used for recovery. A `NotFound` from the
    /// subsequent exact-generation takeover is ambiguous and must not be
    /// reclassified as object absence.
    async fn takeover_current(&self) -> Result<AppendToken, TransportError> {
        let observed = self.stat().await?;
        match self.takeover(&observed).await {
            Err(error) if error.code == TransportCode::NotFound => Err(TransportError {
                zone: error.zone,
                code: TransportCode::Ambiguous,
                message: format!(
                    "exact-generation takeover returned NOT_FOUND after current object lookup: {}",
                    error.message
                ),
            }),
            result => result,
        }
    }

    /// Replace the current appendable generation with an exact byte prefix.
    /// Recovery uses this conditional overwrite after takeover to install the
    /// canonical prefix; the returned token names the replacement generation.
    async fn replace_appendable(
        &self,
        observed: &ReplicaSnapshot,
        data: Bytes,
        metadata: HashMap<String, String>,
    ) -> Result<AppendToken, TransportError>;

    /// Append one checksummed chunk at an authoritative persisted offset.
    async fn append(
        &self,
        token: &AppendToken,
        write_offset: i64,
        data: Vec<u8>,
    ) -> Result<i64, TransportError>;

    /// Queue a non-empty ordered group of checksummed chunks on the live
    /// append session without waiting for acknowledgments. The final wire
    /// message always flushes everything queued through the end of the group.
    /// Returns an error (without blocking) when no session is live — the lane
    /// then resumes via [`Replica::resume_tail`] and resends its unacknowledged
    /// suffix as another flushed group.
    async fn lane_send(&self, write_offset: i64, chunks: &[Bytes]) -> Result<(), TransportError>;

    /// Queue a group whose immutable wire messages were packed before replica
    /// dispatch. Backends that do not consume the shared representation can
    /// retain the chunk-oriented implementation.
    async fn lane_send_packed(
        &self,
        write_offset: i64,
        packed: &PackedAppend,
    ) -> Result<(), TransportError> {
        self.lane_send(write_offset, packed.chunks()).await
    }

    /// Wait until the session's durable tail exceeds `seen` or the session
    /// fails. A response and stream error may be observed together; in that
    /// case the durable offset and error are returned in one observation so the
    /// protocol can publish the physical progress before classifying the error.
    async fn lane_durable_change(&self, seen: i64) -> Result<LaneDurableChange, TransportError>;

    /// Delete exactly the supplied object generation; missing is handled by the
    /// caller as idempotent success.
    async fn delete(&self, generation: i64) -> Result<(), TransportError>;

    /// Finalize the appendable object at exactly `write_offset`.
    ///
    /// Implementations must make retry idempotent when the same generation is
    /// already finalized at the requested length.
    async fn finalize(
        &self,
        token: &mut AppendToken,
        write_offset: i64,
    ) -> Result<ReplicaSnapshot, TransportError>;

    /// Stop and join backend-owned background work for this object.
    ///
    /// Backends without object-local tasks may keep the default no-op.
    async fn shutdown(&self) {}
}

#[cfg(test)]
mod tests {
    use super::TransportCode;

    #[test]
    fn failed_precondition_is_a_terminal_writer_fence() {
        assert!(!TransportCode::FailedPrecondition.transient());
        assert!(TransportCode::FailedPrecondition.fences_writer());
    }

    #[test]
    fn terminal_request_rejections_are_not_retried() {
        assert!(!TransportCode::InvalidArgument.transient());
        assert!(!TransportCode::Unimplemented.transient());
    }

    #[test]
    fn resource_exhaustion_is_retried() {
        assert!(TransportCode::ResourceExhausted.transient());
    }
}
