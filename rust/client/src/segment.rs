use std::collections::{BTreeSet, HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::future::join_all;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::manifest::{
    Manifest, ManifestAccess, ManifestRecord, ManifestUpdate, PendingFold, MANIFEST_OBJECT,
};
use crate::manifest_store::{GcsManifestStore, ManifestStore};
use crate::metrics::{Metrics, MetricsRecorder, NoopMetricsRecorder};
#[cfg(test)]
use crate::protocol::PendingCommit;
use crate::protocol::{
    canonical_prefix, majority, valid_format, AttemptedBytes, ClientConfig, CommitRange,
    ProtocolError, QuorumVolume, RecoveredTail, RecoveryCandidate, Writer,
};
use crate::record::RecordFrame;
use crate::transport::{Replica, ReplicaFactory, ReplicaSnapshot, TransportCode, TransportError};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Quorum-derived state of one opaque segment-object id across replicas.
pub(crate) struct SegmentDescriptor {
    /// Object id (the name under `<prefix>/segments/`): claimed writer epoch
    /// plus a per-incarnation counter, so name order matches creation order.
    /// Identity is decoupled from position: every base lives in the
    /// manifest — the segment directory for sealed segments, `tail_base`
    /// for the active one — so a successor is created before its base is
    /// known.
    pub id: String,
    /// First global record index, from the manifest's segment directory.
    pub base_record_index: u64,
    /// Inclusive record end, derived from the following segment's base (or
    /// the committed tail base for the most recent seal).
    pub end_record_index: u64,
    /// Full-object CRC32C committed in the manifest directory.
    pub crc32c: u32,
    /// Number of zones where listing observed the object.
    pub copies: usize,
    /// Number of observed copies finalized by GCS.
    pub finalized_copies: usize,
    /// The fold CAS committed this seal, but the maintenance task has not
    /// finalized the object yet. Repair skips it — there is no finalized
    /// source to copy from until the seal lands, by design rather than by
    /// loss — and the maintenance task clears the flag once it seals.
    #[serde(default)]
    pub seal_pending: bool,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CatalogSegment {
    pub id: String,
    pub base_record_index: u64,
    pub end_record_index: Option<u64>,
    pub crc32c: Option<u32>,
    pub copies: usize,
    pub finalized_copies: usize,
    pub seal_pending: bool,
}

#[cfg(test)]
impl From<&SegmentDescriptor> for CatalogSegment {
    fn from(segment: &SegmentDescriptor) -> Self {
        Self {
            id: segment.id.clone(),
            base_record_index: segment.base_record_index,
            end_record_index: Some(segment.end_record_index),
            crc32c: Some(segment.crc32c),
            copies: segment.copies,
            finalized_copies: segment.finalized_copies,
            seal_pending: segment.seal_pending,
        }
    }
}

#[derive(Clone)]
/// WAL namespace: the zonal replica factories for segment data (one per
/// zone) plus one regional factory hosting the manifest control register.
pub struct SegmentedVolume {
    factories: Vec<Arc<dyn ReplicaFactory>>,
    manifest_store: Arc<dyn ManifestStore>,
    bucket_names: Vec<String>,
    prefix: String,
    client_config: ClientConfig,
    metrics: Arc<Metrics>,
}

/// Wall-clock cost of the recovery phases that finish before the replay stream
/// is handed back. `epoch_claim` is the manifest CAS that fences prior writers;
/// `prepare` covers directory adoption and repair, committed-seal enforcement,
/// and the appendable-candidate takeover walk. Replay and [`Recovery::start`]
/// are timed by the caller. This is diagnostic only and is never read on the
/// correctness path.
#[derive(Clone, Copy, Debug, Default)]
pub struct RecoveryTimings {
    /// Duration of the manifest claim CAS that fences prior writers.
    pub epoch_claim: Duration,
    /// Duration of directory adoption, committed-seal enforcement, and the
    /// appendable-candidate takeover walk.
    pub prepare: Duration,
}

/// Startup recovery stream and capability to resume from a fenced frontier.
///
/// Recovery claims an epoch, adopts the directory, and walks the manifest's
/// ordered `[tail, pending?]` candidates with takeover-before-size fencing.
/// This stream then emits the fixed range `[from, end)`. Consume it through
/// `None` before calling [`start`](Self::start), which resumes the takeover
/// handle for the first empty frontier or conditionally creates the bootstrap
/// object for a new log.
pub struct Recovery {
    /// Inclusive database checkpoint supplied to recovery.
    pub from: WalSeqNo,
    /// Exclusive recovered boundary and base of the resumed empty frontier.
    pub end: WalSeqNo,
    /// Phase timings collected up to the point the replay stream is returned.
    pub timings: RecoveryTimings,
    volume: SegmentedVolume,
    manifest: Manifest,
    writer_state: RecoveredWriterState,
    inner: StartupReplayStream,
    completed: bool,
    failed: bool,
}

/// A swapped-out segment handed to the maintenance task, which finalizes
/// it off the hot path.
pub(crate) struct SwappedSegment {
    pub id: String,
    pub base_record_index: u64,
    pub end_record_index: u64,
    /// Digest committed beside this seal in the manifest. The live writer is
    /// destructive to seal, so maintenance retains the decision separately
    /// for idempotent reconstruction after any fast-path failure.
    pub digest: String,
    /// Full-object CRC32C committed in the same fold CAS.
    pub crc32c: u32,
    pub writer: Writer,
}

/// A rotation whose in-memory flip has landed but whose background fold has
/// not. Admissions already route to the preregistered successor while the
/// swapped-out segment drains toward its admitted end. Once the engine's
/// in-order completion stream passes `end_record_index`, the fold can publish
/// the old tail's seal, advance the manifest tail to the consumed pending id,
/// and register the already provisioned refill in one CAS.
pub(crate) struct PendingSwap {
    pub id: String,
    pub base_record_index: u64,
    pub end_record_index: u64,
    /// Digest over the frozen admitted byte range. Admission to this segment
    /// stopped at the swap, so this equals the committed digest at CAS time.
    pub digest: String,
    /// Full-object CRC32C over the same frozen admitted byte range.
    pub crc32c: u32,
    /// The consumed pending segment the fold names as the new tail.
    pub successor_id: String,
    /// Live rotation retains the old writer for maintenance finalization.
    /// Recovery has already enforced a finalized quorum, so no writer remains.
    pub writer: Option<Writer>,
}

impl PendingSwap {
    /// Hand the drained, sealed-in-the-manifest segment to maintenance.
    pub(crate) fn into_segment(self) -> Option<SwappedSegment> {
        Some(SwappedSegment {
            id: self.id,
            base_record_index: self.base_record_index,
            end_record_index: self.end_record_index,
            digest: self.digest,
            crc32c: self.crc32c,
            writer: self.writer?,
        })
    }
}

/// Recovery's conservative basis for deleting dead-incarnation segment objects.
///
/// `keep` is the full manifest directory, including truncation tombstones, plus
/// the tail from the snapshot read immediately after claiming `claimed_epoch`.
/// The snapshot may be stale when maintenance eventually uses it. That is safe:
/// deletion is additionally restricted to ids minted strictly before the claim.
/// This incarnation and every later one can add only ids at or above that epoch,
/// and no later manifest decision can resurrect an unreferenced below-epoch id
/// into replay.
#[derive(Clone)]
pub(crate) struct DeadSegmentSweep {
    keep: HashSet<String>,
    pub(crate) claimed_epoch: u64,
}

pub(crate) struct DeadSegmentSweepReport {
    pub(crate) orphan_segments: usize,
    pub(crate) deleted_objects: usize,
    pub(crate) deferred_operations: usize,
    pub(crate) failure: Option<Error>,
}

/// Everything `prepare_recovery` learns and decides: the enforced sealed
/// chain, the replay boundary, and the identity the writer starts from.
struct RecoveredWriterState {
    sealed_segments: Vec<SegmentDescriptor>,
    base_record_index: u64,
    checkpoint_floor: u64,
    active_id: String,
    active_writer: Option<Writer>,
    spare: Option<(String, Writer)>,
    pending_fold: Option<PendingSwap>,
    next_segment_seq: u64,
    dead_segment_sweep: DeadSegmentSweep,
}

struct RecoveredPredecessor {
    id: String,
    base_record_index: u64,
    end_record_index: u64,
    digest: String,
    crc32c: u32,
    had_proven_gap: bool,
    deferred_seal: Option<Box<RecoveredSeal>>,
}

struct RecoveredSeal {
    volume: QuorumVolume,
    tail: RecoveredTail,
}

impl RecoveredPredecessor {
    async fn enforce_seal(&mut self, metrics: &Metrics) -> Result<(), Error> {
        let Some(seal) = self.deferred_seal.take() else {
            return Ok(());
        };
        seal.volume.enforce_seal(seal.tail.canonical()).await?;
        metrics.segments_sealed.increment();
        Ok(())
    }

    fn into_pending_fold(self, successor_id: String) -> PendingSwap {
        debug_assert!(
            self.deferred_seal.is_none(),
            "recovered predecessor must be sealed before writer admission"
        );
        PendingSwap {
            id: self.id,
            base_record_index: self.base_record_index,
            end_record_index: self.end_record_index,
            digest: self.digest,
            crc32c: self.crc32c,
            successor_id,
            writer: None,
        }
    }
}

enum RecoveredManifestCandidate {
    Absent,
    Empty {
        reusable_writer: Option<Box<Writer>>,
    },
    NonEmpty(RecoveredPredecessor),
}

impl Stream for Recovery {
    type Item = Result<WalRecord, crate::Error>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(context) {
            Poll::Ready(Some(Ok(record))) => Poll::Ready(Some(Ok(record))),
            Poll::Ready(Some(Err(error))) => {
                self.failed = true;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.completed = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Recovery {
    /// Number of sealed segments in the recovered chain. Diagnostic only.
    pub fn sealed_segment_count(&self) -> usize {
        self.writer_state.sealed_segments.len()
    }

    /// Conditionally create the segment at [`end`](Self::end) and start the WAL.
    ///
    /// This consumes the recovery so startup replay cannot overlap append
    /// admission. Calling it before the stream reaches `None`, or after a replay
    /// item failed, returns [`crate::Error::RecoveryIncomplete`].
    pub async fn start(
        self,
        config: crate::WalEngineConfig,
    ) -> Result<crate::WalHandle, crate::Error> {
        if !self.completed || self.failed {
            return Err(crate::Error::RecoveryIncomplete);
        }
        let mut manifest = self.manifest;
        manifest.validate_owner().await?;
        let writer = self.volume.open_writer(self.writer_state, manifest).await?;
        crate::engine::WalEngine::start(writer, config)
    }
}

/// Low-level recovered chain used by the engine and internal tests.
pub(crate) struct SegmentedWriter {
    manifest: Manifest,
    factories: Vec<Arc<dyn ReplicaFactory>>,
    prefix: String,
    client_config: ClientConfig,
    sealed_segments: Vec<SegmentDescriptor>,
    segment_writer: Writer,
    active_id: String,
    base_record_index: u64,
    checkpoint_floor: u64,
    dead_segment_sweep: DeadSegmentSweep,
    /// Per-incarnation counter behind [`segment_id`]: with the claimed
    /// epoch it yields unique, creation-ordered ids without randomness.
    next_segment_seq: u64,
    /// Pre-provisioned successor: object created and every zone's append
    /// session opened ahead of need, so rotation swaps lanes instead of
    /// creating anything on the hot path.
    spare: Option<PreparedSpare>,
    pending_fold: Option<PendingSwap>,
    max_replica_lag_bytes: usize,
    lane_stall_timeout: Duration,
    metrics: Arc<Metrics>,
}

pub(crate) struct ProvisionParts {
    pub(crate) factories: Vec<Arc<dyn ReplicaFactory>>,
    pub(crate) prefix: String,
    pub(crate) client_config: ClientConfig,
    pub(crate) max_replica_lag_bytes: usize,
    pub(crate) lane_stall_timeout: Duration,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) manifest: ManifestAccess,
}

struct PreparedSpare {
    id: String,
    writer: Writer,
    registered: bool,
}

pub(crate) struct RegisteredSpare {
    pub(crate) id: String,
    pub(crate) writer: Writer,
    pub(crate) manifest: ManifestUpdate,
}

/// Quorum future for a record submitted directly to [`SegmentedWriter`].
#[cfg(test)]
pub(crate) struct RecordPendingCommit {
    /// Stable global record index used by replay and checkpoints.
    pub global_record_index: u64,
    inner: PendingCommit,
}

#[cfg(test)]
impl RecordPendingCommit {
    /// Wait for majority-quorum durability and return the global record
    /// index. Use [`Error::may_have_committed`] to classify an error.
    pub(crate) async fn wait(self) -> Result<u64, Error> {
        self.inner.wait().await?;
        Ok(self.global_record_index)
    }
}

pub(crate) struct RecordCommitRange {
    base_record_index: u64,
    first_global_record_index: u64,
    end_global_record_index: u64,
    inner: CommitRange,
}

impl RecordCommitRange {
    pub(crate) fn first_global_record_index(&self) -> u64 {
        self.first_global_record_index
    }

    pub(crate) fn end_global_record_index(&self) -> u64 {
        self.end_global_record_index
    }

    pub(crate) fn progress(&mut self) -> (u64, Option<Error>) {
        let (committed, failure) = self.inner.progress();
        (
            self.base_record_index + committed as u64,
            failure.map(Error::from),
        )
    }

    pub(crate) async fn changed(&mut self) -> Result<(), Error> {
        self.inner.changed().await.map_err(Error::from)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Result of one application-triggered truncation pass.
pub struct TruncationReport {
    /// Individual zonal objects deleted during this pass.
    pub deleted_objects: usize,
    /// Segments now absent from a quorum and removed from the chain.
    pub deleted_segments: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
/// Result of one best-effort immutable sealed-segment repair pass.
pub struct RepairReport {
    /// Sealed segments inspected.
    pub segments_examined: usize,
    /// Missing or divergent zonal objects copied and sealed successfully.
    pub objects_repaired: usize,
    /// Zonal copies already matching the immutable sealed source.
    pub objects_already_healthy: usize,
    /// Transiently unavailable targets left for a later pass.
    pub transient_failures: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// One opaque application record emitted by startup replay.
pub struct WalRecord {
    /// Sequence number assigned by the application when the record was admitted.
    pub seqno: WalSeqNo,
    /// Opaque database WAL payload.
    pub payload: Bytes,
}

#[derive(
    Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
/// Application-assigned record number and durable checkpoint boundary.
///
/// An append uses the value as that record's identity. A recovery or truncation
/// checkpoint uses the same value as the first record still needed by the
/// database. Consequently, after durably applying record `n`, persist
/// [`WalSeqNo::record(n + 1)`](Self::record) as the next checkpoint.
pub struct WalSeqNo {
    /// Zero-based record index represented by this boundary.
    pub record_index: u64,
}

impl WalSeqNo {
    /// Beginning of an untruncated log.
    pub const ZERO: Self = Self { record_index: 0 };

    /// Construct a record-boundary sequence number.
    pub const fn record(record_index: u64) -> Self {
        Self { record_index }
    }
}

impl WalRecord {
    /// Exclusive replay/checkpoint boundary immediately after this record.
    pub fn next_seqno(&self) -> WalSeqNo {
        WalSeqNo::record(self.seqno.record_index + 1)
    }
}

type StartupReplayStream = BoxStream<'static, Result<WalRecord, Error>>;

/// Startup recovery, manifest-backed catalog reconstruction, and replay.
mod recovery {
    use super::*;

    impl SegmentedVolume {
        /// Bind 1, 3, or 5 GCS Rapid zonal buckets to one WAL object prefix.
        /// Any other factory count returns [`crate::Error`]. Durability and
        /// availability follow the strict-majority quorum: a single replica
        /// tolerates no zone loss, three tolerate one, five tolerate two.
        ///
        /// Each element must target a different zone. Keep both the list order
        /// and the factory's diagnostic zone index stable across restarts; recovery
        /// treats list position as the replica identity. The prefix is dedicated to
        /// this WAL. The zonal data prefix contains opaque, creation-ordered
        /// segment ids; the same prefix in the regional bucket contains the
        /// manifest control object.
        ///
        /// The client validates replica count and bucket-name consistency but
        /// cannot verify physical placement. Operators must provision Rapid
        /// buckets in distinct zones of one region. Pass full v2 bucket resource
        /// names and use a gRPC-compatible endpoint such as
        /// `https://storage.googleapis.com`; Cloud Storage regional JSON/XML
        /// endpoints do not support gRPC.
        ///
        /// Registers persist the ordered zonal bucket names in
        /// `chorus.buckets`; its length is the replica count. A later wrong,
        /// reordered, or duplicate set is rejected, and registers without the
        /// binding fail closed.
        ///
        /// Independent processes may recover the same prefix concurrently. GCS
        /// append takeover and conditional segment creation elect one writer; the
        /// loser receives a terminal fencing or conditional-create error. Rotation
        /// policy is supplied later through [`crate::WalEngineConfig`], not through
        /// this storage description.
        pub fn new(
            factories: Vec<crate::GrpcReplicaFactory>,
            manifest_factory: crate::GrpcReplicaFactory,
            prefix: impl Into<String>,
            client_config: ClientConfig,
        ) -> Result<Self, Error> {
            Self::new_with_metrics_recorder(
                factories,
                manifest_factory,
                prefix,
                client_config,
                Arc::new(NoopMetricsRecorder),
            )
        }

        /// Bind a WAL namespace and emit metrics through `metrics_recorder`.
        ///
        /// Chorus registers all handles during construction and updates them
        /// directly on event paths. The recorder owns aggregation and export; the
        /// WAL does not retain a readable metrics registry.
        ///
        /// Most handles have no labels. When one backend recorder serves several
        /// volumes, wrap it per volume and inject a stable volume label into every
        /// registration; otherwise those volumes share indistinguishable series.
        pub fn new_with_metrics_recorder(
            factories: Vec<crate::GrpcReplicaFactory>,
            manifest_factory: crate::GrpcReplicaFactory,
            prefix: impl Into<String>,
            client_config: ClientConfig,
            metrics_recorder: Arc<dyn MetricsRecorder>,
        ) -> Result<Self, Error> {
            Self::new_with_factories_and_metrics_recorder(
                factories
                    .into_iter()
                    .map(|factory| Arc::new(factory) as Arc<dyn ReplicaFactory>)
                    .collect(),
                Arc::new(manifest_factory) as Arc<dyn ReplicaFactory>,
                prefix,
                client_config,
                metrics_recorder,
            )
        }

        #[cfg(test)]
        pub(crate) fn new_with_factories(
            factories: Vec<Arc<dyn ReplicaFactory>>,
            manifest_factory: Arc<dyn ReplicaFactory>,
            prefix: impl Into<String>,
            client_config: ClientConfig,
        ) -> Result<Self, Error> {
            Self::new_with_factories_and_metrics_recorder(
                factories,
                manifest_factory,
                prefix,
                client_config,
                Arc::new(NoopMetricsRecorder),
            )
        }

        /// Construct a volume over trait-object factories, for the deterministic
        /// simulation transport. Available only with the `dst-support` feature;
        /// production code uses the concrete-factory constructors above.
        #[cfg(feature = "dst-support")]
        pub fn new_with_dyn_factories_and_metrics_recorder(
            factories: Vec<Arc<dyn ReplicaFactory>>,
            manifest_factory: Arc<dyn ReplicaFactory>,
            prefix: impl Into<String>,
            client_config: ClientConfig,
            metrics_recorder: Arc<dyn MetricsRecorder>,
        ) -> Result<Self, Error> {
            Self::new_with_factories_and_metrics_recorder(
                factories,
                manifest_factory,
                prefix,
                client_config,
                metrics_recorder,
            )
        }

        pub(crate) fn new_with_factories_and_metrics_recorder(
            factories: Vec<Arc<dyn ReplicaFactory>>,
            manifest_factory: Arc<dyn ReplicaFactory>,
            prefix: impl Into<String>,
            client_config: ClientConfig,
            metrics_recorder: Arc<dyn MetricsRecorder>,
        ) -> Result<Self, Error> {
            let prefix = prefix.into().trim_end_matches('/').to_string();
            let object = format!("{prefix}/{MANIFEST_OBJECT}");
            let manifest_store = Arc::new(GcsManifestStore::new(manifest_factory.replica(&object)));
            Self::build(
                factories,
                manifest_store,
                prefix,
                client_config,
                metrics_recorder,
            )
        }

        /// Bind a WAL namespace whose manifest register lives in a caller-supplied
        /// [`ManifestStore`] instead of the default regional GCS object.
        ///
        /// The store must be scoped to exactly this WAL (one register per WAL
        /// prefix) and provide the linearizable compare-and-swap semantics the
        /// trait documents; Firestore, Spanner, or a SQL row with optimistic
        /// locking all qualify. Segment data still lives in the zonal factories.
        /// The required bucket binding covers the ordered zonal data factories;
        /// the caller remains responsible for scoping the custom store to this WAL.
        pub fn new_with_manifest_store(
            factories: Vec<crate::GrpcReplicaFactory>,
            manifest_store: Arc<dyn ManifestStore>,
            prefix: impl Into<String>,
            client_config: ClientConfig,
        ) -> Result<Self, Error> {
            Self::new_with_manifest_store_and_metrics_recorder(
                factories,
                manifest_store,
                prefix,
                client_config,
                Arc::new(NoopMetricsRecorder),
            )
        }

        /// [`Self::new_with_manifest_store`] with a metrics recorder.
        pub fn new_with_manifest_store_and_metrics_recorder(
            factories: Vec<crate::GrpcReplicaFactory>,
            manifest_store: Arc<dyn ManifestStore>,
            prefix: impl Into<String>,
            client_config: ClientConfig,
            metrics_recorder: Arc<dyn MetricsRecorder>,
        ) -> Result<Self, Error> {
            Self::build(
                factories
                    .into_iter()
                    .map(|factory| Arc::new(factory) as Arc<dyn ReplicaFactory>)
                    .collect(),
                manifest_store,
                prefix.into().trim_end_matches('/').to_string(),
                client_config,
                metrics_recorder,
            )
        }

        #[cfg(test)]
        pub(crate) fn new_with_factories_and_manifest_store(
            factories: Vec<Arc<dyn ReplicaFactory>>,
            manifest_store: Arc<dyn ManifestStore>,
            prefix: impl Into<String>,
            client_config: ClientConfig,
        ) -> Result<Self, Error> {
            Self::build(
                factories,
                manifest_store,
                prefix.into().trim_end_matches('/').to_string(),
                client_config,
                Arc::new(NoopMetricsRecorder),
            )
        }

        fn build(
            factories: Vec<Arc<dyn ReplicaFactory>>,
            manifest_store: Arc<dyn ManifestStore>,
            prefix: String,
            client_config: ClientConfig,
            metrics_recorder: Arc<dyn MetricsRecorder>,
        ) -> Result<Self, Error> {
            if !crate::protocol::SUPPORTED_REPLICA_COUNTS.contains(&factories.len()) {
                return Err(ProtocolError::ReplicaCount.into());
            }
            let bucket_names: Vec<String> = factories
                .iter()
                .map(|factory| factory.bucket_name().to_string())
                .collect();
            let replica_count = factories.len();
            Ok(Self {
                factories,
                manifest_store,
                bucket_names,
                prefix,
                client_config,
                metrics: Arc::new(Metrics::new(metrics_recorder.as_ref(), replica_count)),
            })
        }

        fn quorum(&self) -> usize {
            majority(self.factories.len())
        }

        /// Open the manifest register for this WAL prefix.
        async fn open_manifest(&self) -> Result<Manifest, Error> {
            Manifest::open(
                Arc::clone(&self.manifest_store),
                self.client_config.clone(),
                Arc::clone(&self.metrics),
                self.factories.len(),
                self.bucket_names.clone(),
            )
            .await
            .map_err(Into::into)
        }

        /// Fence and seal the prior writer, then prepare database startup replay.
        ///
        /// `checkpoint` is the first record not represented by the database's durable
        /// state. The returned [`Recovery`] emits the fixed range
        /// `[checkpoint, Recovery::end)` and does not create an appendable segment
        /// yet. Apply every emitted record, consume the stream through `None`, then
        /// call [`Recovery::start`] to conditionally create the manifest-selected
        /// active segment and begin admission.
        pub async fn recover(&self, checkpoint: WalSeqNo) -> Result<Recovery, Error> {
            self.metrics.recoveries_run.increment();
            let mut manifest = self.open_manifest().await?;
            let claim_started = tokio::time::Instant::now();
            manifest.claim().await.map_err(Error::from)?;
            let epoch_claim = claim_started.elapsed();
            tracing::info!(
                epoch = manifest.record().epoch,
                "WAL recovery epoch claimed"
            );
            self.recover_with_manifest(checkpoint, manifest, epoch_claim)
                .await
        }

        /// Recover from the truncation floor committed in the manifest.
        ///
        /// This is intended for diagnostic and maintenance tools that do not own a
        /// database checkpoint. Database integrations should use [`Self::recover`]
        /// with their durable replay boundary.
        pub async fn recover_from_committed_floor(&self) -> Result<Recovery, Error> {
            self.metrics.recoveries_run.increment();
            let mut manifest = self.open_manifest().await?;
            let claim_started = tokio::time::Instant::now();
            manifest.claim().await.map_err(Error::from)?;
            let epoch_claim = claim_started.elapsed();
            tracing::info!(
                epoch = manifest.record().epoch,
                "WAL recovery epoch claimed"
            );
            let checkpoint = WalSeqNo::record(manifest.record().trunc);
            self.recover_with_manifest(checkpoint, manifest, epoch_claim)
                .await
        }

        /// Recover the committed chain and repair immutable sealed segment copies.
        ///
        /// This volume-level primitive is intended for diagnostic and maintenance
        /// tools that need a detailed [`RepairReport`]. Applications should run the
        /// WAL engine, which performs the same repair automatically in the
        /// background. The operation claims the WAL writer epoch and creates the
        /// next active segment, just like normal recovery startup.
        pub async fn repair_sealed_segments(&self) -> Result<RepairReport, Error> {
            let mut recovery = self.recover_from_committed_floor().await?;
            while let Some(record) = recovery.next().await {
                record?;
            }
            let Recovery {
                volume,
                manifest,
                writer_state,
                ..
            } = recovery;
            let mut manifest = manifest;
            manifest.validate_owner().await?;
            let mut writer = volume.open_writer(writer_state, manifest).await?;
            writer.repair_sealed_segments().await
        }

        async fn recover_with_manifest(
            &self,
            checkpoint: WalSeqNo,
            mut manifest: Manifest,
            epoch_claim: Duration,
        ) -> Result<Recovery, Error> {
            let prepare_started = tokio::time::Instant::now();
            let writer_state = self.prepare_recovery(checkpoint, &mut manifest).await?;
            let prepare = prepare_started.elapsed();
            let end = WalSeqNo::record(writer_state.base_record_index);
            tracing::info!(
                replay_from = checkpoint.record_index,
                replay_end = end.record_index,
                "WAL recovery replay range prepared"
            );
            let inner = self.scan_sealed_range(&writer_state.sealed_segments, checkpoint, end);
            Ok(Recovery {
                from: checkpoint,
                end,
                timings: RecoveryTimings {
                    epoch_claim,
                    prepare,
                },
                volume: self.clone(),
                manifest,
                writer_state,
                inner,
                completed: false,
                failed: false,
            })
        }

        /// Recover from any reachable zone quorum and return an exclusive writer.
        ///
        /// This compatibility helper consumes the startup replay internally before
        /// creating the next segment. Database integrations should use
        /// [`SegmentedVolume::recover`] so they can apply that replay themselves.
        #[cfg(test)]
        pub(crate) async fn recover_writer(&self) -> Result<SegmentedWriter, Error> {
            self.recover_writer_from(WalSeqNo::ZERO).await
        }

        /// Recover using the durable resume boundary stored in the database's own
        /// checkpoint.
        ///
        /// The manifest truncation floor prevents deleted history from returning.
        /// This boundary selects the database replay start and must not lie below
        /// that committed floor. `recover_writer()` is only appropriate while the
        /// database checkpoint remains zero.
        #[cfg(test)]
        pub(crate) async fn recover_writer_from(
            &self,
            checkpoint: WalSeqNo,
        ) -> Result<SegmentedWriter, Error> {
            self.metrics.recoveries_run.increment();
            let mut manifest = self.open_manifest().await?;
            manifest.claim().await.map_err(Error::from)?;
            tracing::info!(
                epoch = manifest.record().epoch,
                "WAL recovery epoch claimed"
            );
            let writer_state = self.prepare_recovery(checkpoint, &mut manifest).await?;
            let mut replay = self.scan_sealed_range(
                &writer_state.sealed_segments,
                checkpoint,
                WalSeqNo::record(writer_state.base_record_index),
            );
            while let Some(record) = replay.next().await {
                record?;
            }
            self.open_writer(writer_state, manifest).await
        }

        async fn prepare_recovery(
            &self,
            checkpoint: WalSeqNo,
            manifest: &mut Manifest,
        ) -> Result<RecoveredWriterState, Error> {
            let adopted: ManifestRecord = manifest.record().clone();
            let claimed_epoch = adopted.epoch;
            let bootstrap_id = segment_id(claimed_epoch, 0);
            let mut next_segment_seq =
                u64::from(adopted.tail_id.as_deref() == Some(bootstrap_id.as_str()));
            let mut fresh_id = || {
                let id = segment_id(claimed_epoch, next_segment_seq);
                next_segment_seq += 1;
                id
            };
            if checkpoint.record_index < adopted.trunc {
                return Err(Error::InvalidCatalog(format!(
                    "checkpoint {} lies below the committed truncation floor {}: \
                 those records were deleted after the database checkpointed them",
                    checkpoint.record_index, adopted.trunc
                )));
            }
            // The caller's checkpoint positions replay; it carries NO authority
            // over which chain members exist. Pruning and catalog inclusion use
            // only the committed manifest floor — otherwise a checkpoint past
            // the most recent sealed segment (`seal_id`) would exclude it from
            // enforcement before its seal has landed on every zone.
            let floor = adopted.trunc;
            // the committed segment directory is the chain authority
            let directory = adopted.segments.clone();
            // Carry janitor authority to maintenance instead of listing buckets
            // on the recovery path. The complete directory (including
            // truncation tombstones) and tail protect every manifest-accounted
            // id. The strict epoch guard in `sweep_dead_segments` makes this
            // snapshot safe even after it becomes stale: later decisions can
            // only introduce ids at this claimed epoch or above.
            let dead_segment_sweep = DeadSegmentSweep {
                keep: directory
                    .iter()
                    .map(|entry| entry.id.clone())
                    .chain(adopted.tail_id.clone())
                    .chain(adopted.pending_id.clone())
                    .collect(),
                claimed_epoch,
            };
            // Derive each entry's inclusive end from contiguity: the next
            // entry's base, or the committed tail base for the last entry. The
            // encoding cannot disagree with the chain because it never stores
            // an end.
            let mut chain = Vec::with_capacity(directory.len());
            for (index, entry) in directory.iter().enumerate() {
                let next_base = directory
                    .get(index + 1)
                    .map_or(adopted.tail_base, |next| next.base);
                let end = next_base.checked_sub(1).ok_or_else(|| {
                    Error::InvalidCatalog("segment bases must be positive and increasing".into())
                })?;
                if end < entry.base {
                    return Err(Error::InvalidCatalog(format!(
                        "directory entry {} at base {} extends past the following base {next_base}",
                        entry.id, entry.base
                    )));
                }
                // entries wholly below the committed floor are truncation
                // tombstones: deleted history kept in the register only until
                // every zonal copy is confirmed gone. The next truncation pass
                // retries their deletes; recovery just leaves them out.
                if end < floor {
                    continue;
                }
                chain.push((entry.id.clone(), entry.base, end, entry.crc32c));
            }
            if let Some((_, first_base, _, _)) = chain.first() {
                if *first_base > checkpoint.record_index && *first_base > adopted.trunc {
                    return Err(Error::InvalidCatalog(format!(
                        "oldest segment starts at {first_base}, after checkpoint boundary {}",
                        checkpoint.record_index
                    )));
                }
            }
            // future truncation calls must not regress below what the caller
            // has already durably applied
            let checkpoint_floor = adopted.trunc.max(checkpoint.record_index);
            let mut sealed_segments = Vec::with_capacity(chain.len() + 1);
            for (id, base, end, crc32c) in &chain {
                // The most recent seal may still have enforcement in flight, so
                // recover and enforce it against the committed digest. Older
                // entries once reached a finalized quorum, but may since have
                // lost replicas. Before startup continues, use the directory
                // CRC32C to find any surviving canonical copy, repair reachable
                // missing or divergent copies, and require a restored quorum.
                // This closes the window where startup could admit new commits
                // while old committed history survived on only one zone.
                if adopted.seal_id.as_deref() == Some(id.as_str()) {
                    let expected_digest = (adopted.seal_base == Some(*base))
                        .then(|| adopted.seal_digest.clone())
                        .flatten();
                    let sealed = self
                        .recover_and_seal_segment(
                            id,
                            *base,
                            *end,
                            Some((*end - *base + 1) as usize),
                            expected_digest.as_deref(),
                            *crc32c,
                        )
                        .await?;
                    sealed_segments.push(sealed);
                } else {
                    let mut sealed = SegmentDescriptor {
                        id: id.clone(),
                        base_record_index: *base,
                        end_record_index: *end,
                        crc32c: *crc32c,
                        copies: 0,
                        finalized_copies: 0,
                        seal_pending: false,
                    };
                    let healthy =
                        restore_sealed_quorum(&self.factories, &self.prefix, &sealed).await?;
                    sealed.copies = healthy;
                    sealed.finalized_copies = healthy;
                    sealed_segments.push(sealed);
                }
            }

            // Walk the only two admissible appendable candidates in manifest
            // order. Every size observation follows a takeover fence. The first
            // empty candidate is the write frontier; non-empty predecessors are
            // validated, replayed, and finalized before recovery advances.
            let tail_id = adopted
                .tail_id
                .clone()
                .ok_or_else(|| Error::InvalidCatalog("claimed manifest has no tail id".into()))?;
            let mut predecessors = Vec::with_capacity(2);
            let mut next_record_index = adopted.tail_base;
            let mut active = None;
            let mut spare = None;
            let mut pending_fold = None;

            if tail_id == bootstrap_id && adopted.pending_id.is_none() {
                // A claim that initialized an empty register minted this id.
                // Defer both object creates until replay has completed.
                active = Some((tail_id.clone(), None));
            } else {
                let tail = self
                    .recover_manifest_candidate(tail_id.clone(), next_record_index)
                    .await?;
                match tail {
                    RecoveredManifestCandidate::Empty {
                        reusable_writer: Some(writer),
                    } => {
                        active = Some((tail_id.clone(), Some(*writer)));
                    }
                    RecoveredManifestCandidate::Empty {
                        reusable_writer: None,
                        ..
                    } => {
                        let active_id = fresh_id();
                        let pending_id = fresh_id();
                        let (_, active_writer) = self.provision_segment(active_id.clone()).await?;
                        let (_, pending_writer) =
                            self.provision_segment(pending_id.clone()).await?;
                        manifest
                            .replace_empty_frontier(
                                Some(&tail_id),
                                adopted.pending_id.as_deref(),
                                adopted.tail_base,
                                active_id.clone(),
                                pending_id.clone(),
                            )
                            .await
                            .map_err(Error::from)?;
                        tracing::info!(
                            old_tail = %tail_id,
                            new_tail = %active_id,
                            "recovery retired a quorum-only empty tail frontier"
                        );
                        active = Some((active_id, Some(active_writer)));
                        spare = Some((pending_id, pending_writer));
                    }
                    RecoveredManifestCandidate::NonEmpty(predecessor) => {
                        next_record_index = predecessor.end_record_index + 1;
                        sealed_segments.push(SegmentDescriptor {
                            id: predecessor.id.clone(),
                            base_record_index: predecessor.base_record_index,
                            end_record_index: predecessor.end_record_index,
                            crc32c: predecessor.crc32c,
                            copies: self.quorum(),
                            finalized_copies: self.quorum(),
                            seal_pending: false,
                        });
                        predecessors.push(predecessor);
                    }
                    RecoveredManifestCandidate::Absent => {
                        let active_id = fresh_id();
                        let pending_id = fresh_id();
                        let (_, active_writer) = self.provision_segment(active_id.clone()).await?;
                        let (_, pending_writer) =
                            self.provision_segment(pending_id.clone()).await?;
                        manifest
                            .replace_empty_frontier(
                                Some(&tail_id),
                                adopted.pending_id.as_deref(),
                                adopted.tail_base,
                                active_id.clone(),
                                pending_id.clone(),
                            )
                            .await
                            .map_err(Error::from)?;
                        tracing::info!(
                            old_tail = %tail_id,
                            new_tail = %active_id,
                            "recovery retired an absent tail frontier"
                        );
                        active = Some((active_id, Some(active_writer)));
                        spare = Some((pending_id, pending_writer));
                    }
                }
            }

            if active.is_some() {
                let deferred_bootstrap =
                    active.as_ref().is_some_and(|(_, writer)| writer.is_none());
                if spare.is_none() && !deferred_bootstrap {
                    if let Some(pending_id) = adopted.pending_id.clone() {
                        match self
                            .recover_manifest_candidate(pending_id.clone(), next_record_index)
                            .await?
                        {
                            RecoveredManifestCandidate::Empty {
                                reusable_writer: Some(writer),
                            } => {
                                spare = Some((pending_id, *writer));
                            }
                            RecoveredManifestCandidate::Empty {
                                reusable_writer: None,
                                ..
                            }
                            | RecoveredManifestCandidate::Absent => {
                                let active_id = fresh_id();
                                let replacement_pending_id = fresh_id();
                                let (_, active_writer) =
                                    self.provision_segment(active_id.clone()).await?;
                                let (_, pending_writer) = self
                                    .provision_segment(replacement_pending_id.clone())
                                    .await?;
                                manifest
                                    .replace_empty_frontier(
                                        Some(&tail_id),
                                        Some(&pending_id),
                                        adopted.tail_base,
                                        active_id.clone(),
                                        replacement_pending_id.clone(),
                                    )
                                    .await
                                    .map_err(Error::from)?;
                                tracing::info!(
                                    old_tail = %tail_id,
                                    old_pending = %pending_id,
                                    new_tail = %active_id,
                                    "recovery retired a quorum-only empty pending frontier"
                                );
                                active = Some((active_id, Some(active_writer)));
                                spare = Some((replacement_pending_id, pending_writer));
                            }
                            RecoveredManifestCandidate::NonEmpty(_) => {
                                return Err(Error::InvalidCatalog(
                                    "pending segment is non-empty after an empty tail frontier"
                                        .into(),
                                ));
                            }
                        }
                    } else {
                        let pending_id = fresh_id();
                        let (_, pending_writer) =
                            self.provision_segment(pending_id.clone()).await?;
                        manifest
                            .register_pending(pending_id.clone())
                            .await
                            .map_err(Error::from)?;
                        spare = Some((pending_id, pending_writer));
                    }
                }
            } else {
                match adopted.pending_id.clone() {
                    Some(pending_id) => match self
                        .recover_manifest_candidate(pending_id.clone(), next_record_index)
                        .await?
                    {
                        RecoveredManifestCandidate::Empty {
                            reusable_writer: Some(writer),
                        } => {
                            let mut predecessor = predecessors
                                .pop()
                                .expect("non-empty tail produced one predecessor");
                            predecessor.enforce_seal(&self.metrics).await?;
                            pending_fold = Some(predecessor.into_pending_fold(pending_id.clone()));
                            active = Some((pending_id, Some(*writer)));
                        }
                        RecoveredManifestCandidate::Empty {
                            reusable_writer: None,
                            ..
                        } => {
                            let mut predecessor = predecessors
                                .pop()
                                .expect("non-empty tail produced one predecessor");
                            predecessor.enforce_seal(&self.metrics).await?;
                            let active_id = fresh_id();
                            let replacement_pending_id = fresh_id();
                            let (_, active_writer) =
                                self.provision_segment(active_id.clone()).await?;
                            let (_, pending_writer) = self
                                .provision_segment(replacement_pending_id.clone())
                                .await?;
                            manifest
                                .fold_pending(&PendingFold {
                                    old_tail_id: predecessor.id,
                                    old_tail_base: predecessor.base_record_index,
                                    old_tail_end: predecessor.end_record_index,
                                    old_tail_digest: predecessor.digest,
                                    old_tail_crc32c: predecessor.crc32c,
                                    consumed_pending_id: pending_id,
                                    successor_tail_id: active_id.clone(),
                                    refill_pending_id: replacement_pending_id.clone(),
                                })
                                .await
                                .map_err(Error::from)?;
                            active = Some((active_id, Some(active_writer)));
                            spare = Some((replacement_pending_id, pending_writer));
                        }
                        RecoveredManifestCandidate::NonEmpty(mut predecessor) => {
                            if predecessors.first().is_some_and(|tail| tail.had_proven_gap) {
                                // The reachable quorum positively observed
                                // physical tail bytes beyond the longest
                                // compatible complete-record prefix. Global
                                // completions are in order, so no pending
                                // record above that hole could have been
                                // acknowledged. Retire the speculative pending
                                // object and resume from the recovered tail
                                // boundary.
                                let mut old_tail = predecessors
                                    .pop()
                                    .expect("non-empty tail produced one predecessor");
                                let active_id = fresh_id();
                                let replacement_pending_id = fresh_id();
                                let (_, active_writer) =
                                    self.provision_segment(active_id.clone()).await?;
                                let (_, pending_writer) = self
                                    .provision_segment(replacement_pending_id.clone())
                                    .await?;
                                manifest
                                    .fold_pending(&PendingFold {
                                        old_tail_id: old_tail.id.clone(),
                                        old_tail_base: old_tail.base_record_index,
                                        old_tail_end: old_tail.end_record_index,
                                        old_tail_digest: old_tail.digest.clone(),
                                        old_tail_crc32c: old_tail.crc32c,
                                        consumed_pending_id: pending_id,
                                        successor_tail_id: active_id.clone(),
                                        refill_pending_id: replacement_pending_id.clone(),
                                    })
                                    .await
                                    .map_err(Error::from)?;
                                old_tail.enforce_seal(&self.metrics).await?;
                                tracing::warn!(
                                    discarded_pending = %predecessor.id,
                                    new_tail = %active_id,
                                    tail_base = next_record_index,
                                    "recovery discarded speculative pending records above an uncommitted tail gap"
                                );
                                active = Some((active_id, Some(active_writer)));
                                spare = Some((replacement_pending_id, pending_writer));
                            } else {
                                next_record_index = predecessor.end_record_index + 1;
                                sealed_segments.push(SegmentDescriptor {
                                    id: predecessor.id.clone(),
                                    base_record_index: predecessor.base_record_index,
                                    end_record_index: predecessor.end_record_index,
                                    crc32c: predecessor.crc32c,
                                    copies: self.quorum(),
                                    finalized_copies: self.quorum(),
                                    seal_pending: false,
                                });
                                predecessor.enforce_seal(&self.metrics).await?;
                                predecessors.push(predecessor);
                                let refill_id = fresh_id();
                                let (_, refill_writer) =
                                    self.provision_segment(refill_id.clone()).await?;
                                let mut first = predecessors.remove(0);
                                first.enforce_seal(&self.metrics).await?;
                                manifest
                                    .fold_pending(&PendingFold {
                                        old_tail_id: first.id.clone(),
                                        old_tail_base: first.base_record_index,
                                        old_tail_end: first.end_record_index,
                                        old_tail_digest: first.digest.clone(),
                                        old_tail_crc32c: first.crc32c,
                                        consumed_pending_id: pending_id.clone(),
                                        successor_tail_id: pending_id.clone(),
                                        refill_pending_id: refill_id.clone(),
                                    })
                                    .await
                                    .map_err(Error::from)?;
                                let mut last = predecessors
                                    .pop()
                                    .expect("non-empty pending produced a predecessor");
                                last.enforce_seal(&self.metrics).await?;
                                let replacement_pending_id = fresh_id();
                                let (_, pending_writer) = self
                                    .provision_segment(replacement_pending_id.clone())
                                    .await?;
                                manifest
                                    .fold_pending(&PendingFold {
                                        old_tail_id: last.id,
                                        old_tail_base: last.base_record_index,
                                        old_tail_end: last.end_record_index,
                                        old_tail_digest: last.digest,
                                        old_tail_crc32c: last.crc32c,
                                        consumed_pending_id: refill_id.clone(),
                                        successor_tail_id: refill_id.clone(),
                                        refill_pending_id: replacement_pending_id.clone(),
                                    })
                                    .await
                                    .map_err(Error::from)?;
                                active = Some((refill_id, Some(refill_writer)));
                                spare = Some((replacement_pending_id, pending_writer));
                            }
                        }
                        RecoveredManifestCandidate::Absent => {
                            return Err(Error::InvalidCatalog(
                                "manifest pending segment is absent on a quorum".into(),
                            ));
                        }
                    },
                    None => {
                        let pending_id = fresh_id();
                        let (_, pending_writer) =
                            self.provision_segment(pending_id.clone()).await?;
                        manifest
                            .register_pending(pending_id.clone())
                            .await
                            .map_err(Error::from)?;
                        let mut predecessor = predecessors
                            .pop()
                            .expect("non-empty tail produced one predecessor");
                        predecessor.enforce_seal(&self.metrics).await?;
                        pending_fold = Some(predecessor.into_pending_fold(pending_id.clone()));
                        active = Some((pending_id, Some(pending_writer)));
                    }
                }
            }
            let (active_id, active_writer) =
                active.expect("recovery always establishes an empty frontier");
            if checkpoint.record_index > next_record_index {
                return Err(Error::InvalidCatalog(format!(
                    "checkpoint {} lies past the committed end {}",
                    checkpoint.record_index, next_record_index
                )));
            }
            self.metrics
                .recovery_segments_adopted
                .add(sealed_segments.len() as u64);
            Ok(RecoveredWriterState {
                sealed_segments,
                base_record_index: next_record_index,
                checkpoint_floor,
                active_id,
                active_writer,
                spare,
                pending_fold,
                next_segment_seq,
                dead_segment_sweep,
            })
        }

        pub(super) async fn open_writer(
            &self,
            state: RecoveredWriterState,
            mut manifest: Manifest,
        ) -> Result<SegmentedWriter, Error> {
            let RecoveredWriterState {
                sealed_segments,
                base_record_index,
                checkpoint_floor,
                active_id,
                active_writer,
                mut spare,
                pending_fold,
                mut next_segment_seq,
                dead_segment_sweep,
            } = state;
            // a segment object's metadata is fixed at creation and never
            // touched again: its base lives in the manifest, so open append
            // sessions never see a metageneration change
            let writer = match active_writer {
                Some(writer) => writer,
                None => {
                    let object = self.segment_object(&active_id);
                    self.volume_for(&object)?.create_writer().await?
                }
            };
            if spare.is_none() {
                if pending_fold.is_none() && manifest.record().pending_id.is_some() {
                    return Err(Error::InvalidCatalog(
                        "recovery did not acquire the manifest pending segment".into(),
                    ));
                }
                let pending_id = segment_id(manifest.record().epoch, next_segment_seq);
                next_segment_seq = next_segment_seq.checked_add(1).ok_or_else(|| {
                    Error::InvalidCatalog("segment id sequence overflowed u64".into())
                })?;
                let (_, pending_writer) = self.provision_segment(pending_id.clone()).await?;
                if pending_fold.is_none() {
                    manifest
                        .register_pending(pending_id.clone())
                        .await
                        .map_err(Error::from)?;
                }
                spare = Some((pending_id, pending_writer));
            }
            // the create is not epoch-guarded (zonal operations never are), so a
            // recoverer deposed after validate_owner can still win it. Re-check
            // ownership now that the object exists: a deposed creator abdicates
            // here instead of running as a zombie writer whose first manifest
            // CAS would fence it anyway. This shrinks the window; the next
            // owner's takeover and the fold CAS's quorum intersection carry the
            // safety argument for what remains.
            manifest.validate_owner().await.map_err(Error::from)?;
            self.metrics.open_segments.add(1);
            self.metrics
                .committed_records_watermark
                .set_u64(base_record_index);
            let spare_registered = pending_fold.is_none();
            Ok(SegmentedWriter {
                factories: self.factories.clone(),
                prefix: self.prefix.clone(),
                client_config: self.client_config.clone(),
                manifest,
                sealed_segments,
                segment_writer: writer,
                active_id,
                base_record_index,
                checkpoint_floor,
                dead_segment_sweep,
                next_segment_seq,
                spare: spare.map(|(id, writer)| PreparedSpare {
                    id,
                    writer,
                    registered: spare_registered,
                }),
                pending_fold,
                max_replica_lag_bytes: usize::MAX,
                lane_stall_timeout: crate::protocol::DEFAULT_LANE_STALL_TIMEOUT,
                metrics: Arc::clone(&self.metrics),
            })
        }

        fn scan_sealed_range(
            &self,
            segments: &[SegmentDescriptor],
            from: WalSeqNo,
            end: WalSeqNo,
        ) -> StartupReplayStream {
            debug_assert!(from.record_index <= end.record_index);
            let segments = segments.to_vec();
            let factories = self.factories.clone();
            let prefix = self.prefix.clone();
            async_stream::try_stream! {
            for segment in segments {
                let segment_end = segment.end_record_index;
                if segment_end < from.record_index || segment.base_record_index >= end.record_index {
                    continue;
                }
                let records = read_sealed_segment(
                    &factories,
                    &format!("{prefix}/segments/{}", segment.id),
                    &segment,
                )
                .await?;
                for record in replay_records(&segment, &records, from, end)? {
                    yield record;
                }
            }
        }
        .boxed()
        }

        async fn finalized_segment_descriptor(
            &self,
            id: &str,
            base_record_index: u64,
            object: &str,
            expected_records: Option<u64>,
            expected_digest: Option<&str>,
            expected_crc32c: u32,
        ) -> Result<Option<SegmentDescriptor>, Error> {
            let replicas = replicas_for(&self.factories, object);
            // Stat-only fast path: a committed seal records the canonical range
            // (its record count is known from the manifest, `expected_records`)
            // and its full-object CRC32C. If a quorum of replicas are already
            // finalized carrying that exact CRC32C, the seal is enforced on a
            // quorum and recovery can adopt it from metadata alone, without
            // downloading the sealed bytes -- the dominant cost on the recovery
            // path. This is the same finalized-object CRC32C the stat-only
            // repair health pre-check trusts, under the non-Byzantine CRC32C
            // threat model. Any shortfall (missing, unfinalized, or
            // CRC-divergent copies) falls through to the authoritative byte read
            // and rewrite below, which re-enforces the seal.
            if let Some(expected) = expected_records {
                let stats = join_all(replicas.iter().map(|replica| replica.stat())).await;
                let matching = stats
                    .into_iter()
                    .flatten()
                    .filter(|snapshot| {
                        snapshot.finalized
                            && valid_format(&snapshot.metadata)
                            && snapshot.crc32c == Some(expected_crc32c)
                    })
                    .count();
                if matching >= self.quorum() {
                    let last_offset = expected.checked_sub(1).ok_or_else(|| {
                        Error::InvalidCatalog("committed seal has zero records".into())
                    })?;
                    let end = base_record_index.checked_add(last_offset).ok_or_else(|| {
                        Error::InvalidCatalog("segment end overflowed u64".into())
                    })?;
                    return Ok(Some(SegmentDescriptor {
                        id: id.to_string(),
                        base_record_index,
                        end_record_index: end,
                        crc32c: expected_crc32c,
                        copies: matching,
                        finalized_copies: matching,
                        seal_pending: false,
                    }));
                }
            }
            let reads = join_all(replicas.iter().map(|replica| replica.snapshot())).await;
            let finalized: Vec<_> = reads
                .into_iter()
                .flatten()
                .filter(|snapshot| snapshot.finalized && valid_format(&snapshot.metadata))
                .collect();
            if finalized.len() < self.quorum() {
                return Ok(None);
            }
            let mut groups: HashMap<Vec<u8>, Vec<ReplicaSnapshot>> = HashMap::new();
            for snapshot in finalized {
                if RecordFrame::decode_all(&snapshot.bytes).is_ok() {
                    groups
                        .entry(snapshot.bytes.clone())
                        .or_default()
                        .push(snapshot);
                }
            }
            let Some((canonical_bytes, finalized)) = groups
                .into_iter()
                .filter(|(_, copies)| copies.len() >= self.quorum())
                .filter(|(bytes, _)| {
                    expected_records.is_none_or(|expected| {
                        RecordFrame::decode_all(bytes)
                            .is_ok_and(|records| records.len() as u64 == expected)
                    })
                })
                .max_by(|(left, _), (right, _)| left.len().cmp(&right.len()).then(left.cmp(right)))
            else {
                return Ok(None);
            };
            let record_count = RecordFrame::decode_all(&canonical_bytes)?.len() as u64;
            let Some(last_record_offset) = record_count.checked_sub(1) else {
                return Err(Error::InvalidCatalog(format!(
                    "finalized segment {base_record_index} is empty"
                )));
            };
            if let Some(expected) = expected_digest {
                let digest = crate::protocol::digest_bytes(&canonical_bytes);
                if digest != expected {
                    return Err(Error::InvalidCatalog(format!(
                    "finalized segment {base_record_index} does not match the committed seal digest"
                )));
                }
            }
            let actual = crc32c::crc32c(&canonical_bytes);
            if actual != expected_crc32c {
                return Err(Error::InvalidCatalog(format!(
                    "finalized segment {base_record_index} has CRC32C {actual:08x}, expected {expected_crc32c:08x}"
                )));
            }
            let end = base_record_index
                .checked_add(last_record_offset)
                .ok_or_else(|| Error::InvalidCatalog("segment end overflowed u64".into()))?;
            Ok(Some(SegmentDescriptor {
                id: id.to_string(),
                base_record_index,
                end_record_index: end,
                crc32c: expected_crc32c,
                copies: finalized.len(),
                finalized_copies: finalized.len(),
                seal_pending: false,
            }))
        }

        /// Finish a seal whose decision is already committed: either by the
        /// manifest (`expected_digest`) or by the following segment's base
        /// (`end_record_index`). No new decision is taken here.
        async fn recover_and_seal_segment(
            &self,
            id: &str,
            base_record_index: u64,
            end_record_index: u64,
            canonical_records: Option<usize>,
            expected_digest: Option<&str>,
            expected_crc32c: u32,
        ) -> Result<SegmentDescriptor, Error> {
            let object = self.segment_object(id);
            if let Some(completed) = self
                .finalized_segment_descriptor(
                    id,
                    base_record_index,
                    &object,
                    canonical_records.map(|count| count as u64),
                    expected_digest,
                    expected_crc32c,
                )
                .await?
            {
                if completed.end_record_index != end_record_index {
                    return Err(Error::InvalidCatalog(format!(
                    "segment {base_record_index} seals at {:?}, following segment requires {end_record_index}",
                    completed.end_record_index
                )));
                }
                return Ok(completed);
            }

            let sealed = enforce_committed_seal(
                &self.factories,
                &self.prefix,
                &self.client_config,
                Arc::clone(&self.metrics),
                id,
                base_record_index,
                end_record_index,
                expected_digest,
                expected_crc32c,
            )
            .await?;
            self.metrics.segments_sealed.increment();
            Ok(sealed)
        }

        /// A segment object's metadata is fixed at creation: the format marker
        /// and nothing else. Chain position lives in the manifest's segment
        /// directory, so rewrites carry the same constant metadata.
        fn volume_for(&self, object: &str) -> Result<QuorumVolume, Error> {
            QuorumVolume::with_metadata(
                replicas_for(&self.factories, object),
                self.client_config.clone(),
                crate::protocol::protocol_metadata(),
                Arc::clone(&self.metrics),
            )
            .map_err(Into::into)
        }

        fn segments_prefix(&self) -> String {
            format!("{}/segments/", self.prefix)
        }

        fn segment_object(&self, id: &str) -> String {
            format!("{}{id}", self.segments_prefix())
        }

        async fn provision_segment(&self, id: String) -> Result<(String, Writer), Error> {
            provision_spare(
                self.factories.clone(),
                self.prefix.clone(),
                self.client_config.clone(),
                usize::MAX,
                crate::protocol::DEFAULT_LANE_STALL_TIMEOUT,
                id,
                Arc::clone(&self.metrics),
            )
            .await
        }

        async fn recover_manifest_candidate(
            &self,
            id: String,
            base_record_index: u64,
        ) -> Result<RecoveredManifestCandidate, Error> {
            let object = self.segment_object(&id);
            let volume = self.volume_for(&object)?;
            match volume.recover_candidate(None).await? {
                RecoveryCandidate::Absent => Ok(RecoveredManifestCandidate::Absent),
                RecoveryCandidate::Empty { reusable_writer } => {
                    Ok(RecoveredManifestCandidate::Empty { reusable_writer })
                }
                RecoveryCandidate::NonEmpty(tail) => {
                    let record_count = u64::try_from(tail.len()).map_err(|_| {
                        Error::InvalidCatalog("recovered record count does not fit in u64".into())
                    })?;
                    let last_record_offset = record_count.checked_sub(1).ok_or_else(|| {
                        Error::InvalidCatalog(
                            "non-empty recovery candidate contains no complete records".into(),
                        )
                    })?;
                    let end_record_index = base_record_index
                        .checked_add(last_record_offset)
                        .ok_or_else(|| {
                            Error::InvalidCatalog("segment end overflowed u64".into())
                        })?;
                    let digest = tail.digest();
                    let crc32c = tail.crc32c();
                    let had_proven_gap = tail.had_discarded_suffix();
                    Ok(RecoveredManifestCandidate::NonEmpty(RecoveredPredecessor {
                        id,
                        base_record_index,
                        end_record_index,
                        digest,
                        crc32c,
                        had_proven_gap,
                        deferred_seal: Some(Box::new(RecoveredSeal { volume, tail })),
                    }))
                }
            }
        }
    }
}

/// Active writer operation, spare adoption, and rotation transitions.
mod writer {
    use super::*;

    impl Drop for SegmentedWriter {
        fn drop(&mut self) {
            self.metrics.open_segments.add(-1);
        }
    }

    impl SegmentedWriter {
        pub(crate) fn metrics(&self) -> Arc<Metrics> {
            Arc::clone(&self.metrics)
        }

        pub(crate) fn client_config(&self) -> ClientConfig {
            self.client_config.clone()
        }

        pub(crate) fn set_max_replica_lag_bytes(&mut self, limit: usize) {
            self.max_replica_lag_bytes = limit;
            self.segment_writer.set_max_replica_lag_bytes(limit);
            if let Some(spare) = &mut self.spare {
                spare.writer.set_max_replica_lag_bytes(limit);
            }
        }

        pub(crate) fn set_lane_stall_timeout(&mut self, timeout: Duration) {
            self.lane_stall_timeout = timeout;
            self.segment_writer.set_lane_stall_timeout(timeout);
            if let Some(spare) = &mut self.spare {
                spare.writer.set_lane_stall_timeout(timeout);
            }
        }

        pub(crate) async fn shutdown_background_tasks(&mut self) {
            self.segment_writer.shutdown_background_tasks().await;
            if let Some(spare) = &mut self.spare {
                spare.writer.shutdown_background_tasks().await;
            }
        }

        fn quorum(&self) -> usize {
            majority(self.factories.len())
        }

        /// Return the current in-memory segment chain, sealed segments first
        /// and the active segment last.
        #[cfg(test)]
        pub(crate) fn catalog(&self) -> Vec<CatalogSegment> {
            let mut segments = self
                .sealed_segments
                .iter()
                .map(CatalogSegment::from)
                .collect::<Vec<_>>();
            segments.push(CatalogSegment {
                id: self.active_id.clone(),
                base_record_index: self.base_record_index,
                end_record_index: None,
                crc32c: None,
                copies: self.quorum(),
                finalized_copies: 0,
                seal_pending: false,
            });
            segments
        }

        /// Base record index of the active appendable segment.
        #[cfg(test)]
        pub(crate) fn active_segment_base(&self) -> u64 {
            self.base_record_index
        }

        /// Exclusive global record end of the active canonical prefix.
        pub(crate) fn committed_record_end(&self) -> u64 {
            self.base_record_index + self.segment_writer.committed_len() as u64
        }

        /// Encoded bytes already committed in the active object when the engine
        /// adopts this writer. Engine-lifetime admissions are accounted separately
        /// so a segment swap can rebase queued bytes onto the successor without
        /// retaining record payloads in control-plane state.
        pub(crate) fn active_segment_bytes(&self) -> usize {
            self.segment_writer.physical_size()
        }

        /// Whether the manifest can catalog the active object if it gains a
        /// committed record and the process then crashes. Recovery may create
        /// an empty tail with no remaining slot, but admission must wait for
        /// truncation before making that tail part of committed history.
        pub(crate) fn active_segment_has_seal_room(&self) -> bool {
            self.manifest.directory_has_room(1)
        }

        /// Application checkpoint supplied during recovery or truncation.
        #[cfg(test)]
        pub(crate) fn checkpoint_floor(&self) -> u64 {
            self.checkpoint_floor
        }

        /// Return whether the configured encoded-size threshold was crossed.
        ///
        /// A full segment directory defers rotation: the fold CAS would have no
        /// room for the new entry, so the active segment grows past the advisory
        /// target until truncation frees retained history. The engine's separate
        /// hard active-segment ceiling bounds that deferral and returns admission
        /// backpressure rather than allowing provider-limit growth.
        ///
        /// Reserve room for two entries, not one. A normal swap folds only the
        /// old tail (the pending becomes the new appendable tail). But a crash
        /// in the swap window leaves the consumed pending carrying acknowledged
        /// records that recovery cannot re-adopt as an appendable frontier, so
        /// recovery seals both the old tail and that pending — two directory
        /// entries. Admitting a swap with room for only one wedges that
        /// recovery on [`ProtocolError::SegmentDirectoryFull`]; gating on two
        /// keeps the swap window always recoverable.
        pub(crate) fn rotation_due(&self, max_segment_bytes: usize) -> bool {
            self.segment_writer.committed_len() != 0
                && !self.segment_writer.is_poisoned()
                && self.segment_writer.physical_size() >= max_segment_bytes
                && self.manifest.directory_has_room(2)
        }

        /// Re-read the epoch-free directory fields changed by maintenance, then
        /// evaluate rotation against the fresh capacity. The writer epoch and
        /// owner are untouched: truncation removes only entries below the
        /// committed floor, but without this refresh an engine held at the hard
        /// active-segment ceiling would keep trusting its pre-truncation full
        /// directory until another unrelated manifest operation.
        pub(crate) async fn refresh_rotation_due(
            &mut self,
            max_segment_bytes: usize,
        ) -> Result<bool, Error> {
            self.manifest
                .refreshed_record()
                .await
                .map_err(Error::from)?;
            Ok(self.rotation_due(max_segment_bytes))
        }

        /// Submit already-formed atomic records to the segment pipeline.
        ///
        /// Database integrations should use [`crate::WalHandle::enqueue_append`]
        /// so caller-assigned sequence numbers are checked during admission and the
        /// engine manages its continuously replenished window.
        #[cfg(test)]
        pub(crate) async fn enqueue_records(
            &mut self,
            records: Vec<RecordFrame>,
            on_attempted: AttemptedBytes,
        ) -> Result<Vec<RecordPendingCommit>, Error> {
            let pending = self
                .segment_writer
                .enqueue_data_window(records, on_attempted)
                .await?
                .into_pending();
            Ok(pending
                .into_iter()
                .map(|inner| RecordPendingCommit {
                    global_record_index: self.base_record_index + inner.logical_offset,
                    inner,
                })
                .collect())
        }

        pub(crate) async fn enqueue_records_for_engine(
            &mut self,
            records: Vec<RecordFrame>,
            on_attempted: AttemptedBytes,
        ) -> Result<RecordCommitRange, Error> {
            let range = self
                .segment_writer
                .enqueue_data_window(records, on_attempted)
                .await?;
            let first_offset = range.first_offset() as u64;
            let end_offset = range.end_offset() as u64;
            Ok(RecordCommitRange {
                base_record_index: self.base_record_index,
                first_global_record_index: self.base_record_index + first_offset,
                end_global_record_index: self.base_record_index + end_offset,
                inner: range,
            })
        }

        /// Whether a spare successor is provisioned and ready to swap in.
        pub(crate) fn spare_ready(&self) -> bool {
            self.spare.as_ref().is_some_and(|spare| spare.registered)
        }

        /// Rotation can assign the successor base only after the old segment's
        /// admitted prefix is fully committed. Otherwise a later segment could
        /// become durable above an unresolved global sequence gap.
        pub(crate) fn swap_boundary_ready(&self) -> bool {
            let admitted = self.segment_writer.admitted_len();
            admitted != 0 && admitted == self.segment_writer.committed_len()
        }

        /// Whether the engine should start provisioning a spare. One spare is
        /// kept warm at all times: it is an empty object plus one idle session
        /// per zone, and a session the service expires self-heals through
        /// handle resume on the first post-swap append.
        pub(crate) fn spare_wanted(&self) -> bool {
            self.spare.is_none()
        }

        pub(crate) fn unregistered_spare_ready(&self) -> bool {
            self.spare.as_ref().is_some_and(|spare| !spare.registered)
        }

        pub(crate) fn adopt_registered_spare(&mut self, spare: RegisteredSpare) {
            self.manifest.install_update(spare.manifest);
            self.adopt_spare(spare.id, spare.writer, true);
        }

        pub(crate) fn adopt_unregistered_spare(&mut self, id: String, writer: Writer) {
            self.adopt_spare(id, writer, false);
        }

        fn adopt_spare(&mut self, id: String, mut writer: Writer, registered: bool) {
            writer.set_max_replica_lag_bytes(self.max_replica_lag_bytes);
            writer.set_lane_stall_timeout(self.lane_stall_timeout);
            self.spare = Some(PreparedSpare {
                id,
                writer,
                registered,
            });
        }

        /// Allocate the next creation-ordered segment id under this writer's
        /// claimed epoch. An id whose provisioning fails is simply abandoned —
        /// gaps are fine, and a later recovery makes the partial orphan eligible
        /// for asynchronous maintenance cleanup.
        pub(crate) fn next_segment_id(&mut self) -> String {
            let id = segment_id(self.manifest.record().epoch, self.next_segment_seq);
            self.next_segment_seq += 1;
            id
        }

        pub(crate) fn provision_parts(&self) -> Result<ProvisionParts, Error> {
            Ok(ProvisionParts {
                factories: self.factories.clone(),
                prefix: self.prefix.clone(),
                client_config: self.client_config.clone(),
                max_replica_lag_bytes: self.max_replica_lag_bytes,
                lane_stall_timeout: self.lane_stall_timeout,
                metrics: Arc::clone(&self.metrics),
                manifest: self.manifest.off_path_access().map_err(Error::from)?,
            })
        }

        pub(crate) fn take_recovered_fold(&mut self) -> Option<PendingSwap> {
            self.pending_fold.take()
        }

        /// Freeze the old segment at its fully committed boundary, wait for its
        /// already-dispatched digest work, and switch lanes to the pending
        /// segment's pre-opened sessions. The pending id is already durable in
        /// the manifest, so this hot-path operation publishes no control-plane
        /// decision.
        /// Nothing touches the spare's object metadata, so its open append
        /// sessions never see a metageneration change.
        pub(crate) async fn begin_swap(&mut self) -> Result<Option<PendingSwap>, Error> {
            if self.segment_writer.is_poisoned() {
                return Err(Error::Poisoned);
            }
            let admitted = self.segment_writer.admitted_len();
            if admitted == 0 {
                return Ok(None);
            }
            let committed = self.segment_writer.committed_len();
            if admitted != committed {
                return Err(Error::Internal(format!(
                    "rotation boundary has {committed} of {admitted} admitted records committed"
                )));
            }
            let Some(spare) = self.spare.take() else {
                return Err(Error::Internal("rotation swap without a spare".into()));
            };
            if !spare.registered {
                self.spare = Some(spare);
                return Err(Error::Internal(
                    "rotation swap attempted with an unregistered spare".into(),
                ));
            }
            let PreparedSpare {
                id: spare_id,
                writer: spare_writer,
                ..
            } = spare;
            let end = self.base_record_index + committed as u64 - 1;
            // admission to this segment stops here, so the admitted digest is
            // frozen and equals the committed digest once the drain completes
            let digest = self.segment_writer.seal_digest().await;
            let crc32c = self.segment_writer.seal_crc32c();
            let old_base = self.base_record_index;
            self.base_record_index = end + 1;
            let old_id = std::mem::replace(&mut self.active_id, spare_id.clone());
            let old_writer = std::mem::replace(&mut self.segment_writer, spare_writer);
            tracing::info!(
                segment_base = old_base,
                segment_end = end,
                "WAL rotation flipped to registered pending segment"
            );
            Ok(Some(PendingSwap {
                id: old_id,
                base_record_index: old_base,
                end_record_index: end,
                digest,
                crc32c,
                successor_id: spare_id,
                writer: Some(old_writer),
            }))
        }

        pub(crate) fn pending_fold_request(
            &self,
            swap: &PendingSwap,
        ) -> Result<PendingFold, Error> {
            let spare = self
                .spare
                .as_ref()
                .filter(|spare| !spare.registered)
                .ok_or_else(|| Error::Internal("pending fold has no refill spare".into()))?;
            Ok(PendingFold {
                old_tail_id: swap.id.clone(),
                old_tail_base: swap.base_record_index,
                old_tail_end: swap.end_record_index,
                old_tail_digest: swap.digest.clone(),
                old_tail_crc32c: swap.crc32c,
                consumed_pending_id: swap.successor_id.clone(),
                successor_tail_id: swap.successor_id.clone(),
                refill_pending_id: spare.id.clone(),
            })
        }

        pub(crate) fn confirm_fold(
            &mut self,
            swap: &PendingSwap,
            update: ManifestUpdate,
        ) -> Result<(), Error> {
            let quorum = self.quorum();
            self.manifest.install_update(update);
            let spare = self
                .spare
                .as_mut()
                .ok_or_else(|| Error::Internal("fold completed without a refill spare".into()))?;
            spare.registered = true;
            tracing::info!(
                segment_base = swap.base_record_index,
                segment_end = swap.end_record_index,
                "WAL pending segment fold committed"
            );
            if let Some(segment) = self
                .sealed_segments
                .iter_mut()
                .find(|segment| segment.id == swap.id)
            {
                segment.seal_pending = swap.writer.is_some();
            } else {
                self.sealed_segments.push(SegmentDescriptor {
                    id: swap.id.clone(),
                    base_record_index: swap.base_record_index,
                    end_record_index: swap.end_record_index,
                    crc32c: swap.crc32c,
                    copies: quorum,
                    finalized_copies: usize::from(swap.writer.is_none()) * quorum,
                    seal_pending: swap.writer.is_some(),
                });
            }
            self.metrics.rotations_completed.increment();
            Ok(())
        }

        #[cfg(test)]
        pub(crate) async fn rotate(&mut self) -> Result<(), Error> {
            if self.segment_writer.is_poisoned() {
                return Err(Error::Poisoned);
            }
            let attempted = self.segment_writer.admitted_len();
            let committed = self.segment_writer.committed_len();
            if attempted != committed {
                return Err(Error::Internal(format!(
                    "automatic rotation found {committed} of {attempted} records committed"
                )));
            }
            if committed == 0 {
                return Ok(());
            }
            let swap = self.begin_swap().await?.ok_or_else(|| {
                Error::Internal("non-empty test rotation produced no swap".into())
            })?;
            let parts = self.provision_parts()?;
            let manifest = parts.manifest.clone();
            let refill_id = self.next_segment_id();
            let (refill_id, refill) = provision_spare(
                parts.factories,
                parts.prefix,
                parts.client_config,
                parts.max_replica_lag_bytes,
                parts.lane_stall_timeout,
                refill_id,
                parts.metrics,
            )
            .await?;
            self.adopt_unregistered_spare(refill_id, refill);
            let fold = self.pending_fold_request(&swap)?;
            let update = fold_registered_pending(manifest, fold).await?;
            self.confirm_fold(&swap, update)?;
            let sealed_id = swap.id.clone();
            let mut segment = swap
                .into_segment()
                .ok_or_else(|| Error::Internal("live rotation lost its old writer".into()))?;
            segment.writer.seal().await?;
            self.metrics.segments_sealed.increment();
            tracing::info!(
                segment_base = segment.base_record_index,
                segment_end = segment.end_record_index,
                "WAL segment rotation completed"
            );
            let quorum = self.quorum();
            if let Some(descriptor) = self
                .sealed_segments
                .iter_mut()
                .find(|descriptor| descriptor.id == sealed_id)
            {
                descriptor.finalized_copies = quorum;
                descriptor.seal_pending = false;
            }
            Ok(())
        }

        /// Re-replicate immutable sealed segments to missing or divergent zones.
        ///
        /// This operation never repairs the active appendable segment. The WAL
        /// engine runs equivalent full passes at startup and on its configured
        /// periodic maintenance interval; degraded rotations schedule a targeted
        /// pass for only the new seal. Diagnostic tools may invoke this directly
        /// while they exclusively own the writer.
        pub(crate) async fn repair_sealed_segments(&mut self) -> Result<RepairReport, Error> {
            let floor = self
                .manifest
                .refreshed_record()
                .await
                .map_err(Error::from)?
                .trunc;
            repair_sealed_pass(&self.factories, &self.prefix, &self.sealed_segments, floor).await
        }

        /// Delete whole sealed segments whose inclusive end is below `floor`.
        /// The engine routes truncation through the maintenance task; this
        /// writer-level form serves diagnostics and tests that own the writer.
        #[cfg(test)]
        pub(crate) async fn truncate_before(
            &mut self,
            floor: WalSeqNo,
        ) -> Result<TruncationReport, Error> {
            truncate_pass(
                &self.factories,
                &self.prefix,
                &mut self.sealed_segments,
                &mut self.checkpoint_floor,
                &mut self.manifest,
                floor,
            )
            .await
        }

        /// Everything the background maintenance task needs, cloned out of the
        /// writer so the task shares no mutable state with the engine.
        pub(crate) fn maintenance_config(
            &self,
            repair_interval: Option<std::time::Duration>,
        ) -> crate::maintenance::MaintenanceConfig {
            crate::maintenance::MaintenanceConfig {
                factories: self.factories.clone(),
                manifest_store: self.manifest.store(),
                bucket_names: self.manifest.bucket_names().to_vec(),
                prefix: self.prefix.clone(),
                client_config: self.client_config.clone(),
                checkpoint_floor: self.checkpoint_floor,
                dead_segment_sweep: self.dead_segment_sweep.clone(),
                repair_interval,
            }
        }

        /// Snapshot of the sealed catalog for the maintenance watch channel.
        pub(crate) fn sealed_segments_snapshot(&self) -> Vec<SegmentDescriptor> {
            self.sealed_segments.clone()
        }
    }
}

/// A fresh segment id: the claimed writer epoch plus a per-incarnation
/// counter, fixed-width hex so lexicographic name order equals creation
/// order. Identity still never encodes position — the chain authority is
/// the manifest's segment directory — but ordered names make bucket
/// listings legible and ids collision-free without randomness (epochs are
/// CAS-granted to one incarnation; the counter is local).
pub(crate) fn segment_id(epoch: u64, seq: u64) -> String {
    crate::manifest::segment_id(epoch, seq)
}

/// The claimed-epoch prefix baked into every id by [`segment_id`]. `None`
/// for names that do not follow the scheme — callers must leave those alone.
fn id_epoch(id: &str) -> Option<u64> {
    let (epoch, _) = id.split_once('-')?;
    if epoch.len() != 16 {
        return None;
    }
    u64::from_str_radix(epoch, 16).ok()
}

/// Full object name for a segment id.
pub(crate) fn segment_object(prefix: &str, id: &str) -> String {
    format!("{prefix}/segments/{id}")
}

async fn read_sealed_segment(
    factories: &[Arc<dyn ReplicaFactory>],
    object: &str,
    segment: &SegmentDescriptor,
) -> Result<Vec<RecordFrame>, Error> {
    let replicas = replicas_for(factories, object);
    let expected_end = segment.end_record_index;
    let expected_records = expected_end
        .checked_sub(segment.base_record_index)
        .and_then(|span| span.checked_add(1))
        .ok_or_else(|| {
            Error::InvalidCatalog(format!(
                "sealed segment {} has invalid end {expected_end}",
                segment.base_record_index
            ))
        })?;
    let expected_crc32c = segment.crc32c;
    if replicas.is_empty() {
        return Err(Error::InvalidCatalog(
            "sealed segment has no configured replicas".into(),
        ));
    }
    let mut snapshots = Vec::new();
    for replica in &replicas {
        if let Ok(snapshot) = replica.snapshot().await {
            if snapshot.crc32c == Some(expected_crc32c)
                && crc32c::crc32c(&snapshot.bytes) == expected_crc32c
            {
                if let Some(records) = decoded_sealed_snapshot(&snapshot, expected_records) {
                    return Ok(records);
                }
            }
            snapshots.push(snapshot);
        }
    }
    snapshots.retain(|snapshot| decoded_sealed_snapshot(snapshot, expected_records).is_some());
    let quorum = majority(replicas.len());
    if snapshots.len() < quorum {
        return Err(Error::NoReadQuorum);
    }
    let records = canonical_prefix(&snapshots, quorum)?;
    if records.len() as u64 != expected_records {
        return Err(Error::InvalidSegmentData(format!(
            "segment {} should contain {expected_records} records but contains {}",
            segment.base_record_index,
            records.len()
        )));
    }
    let actual_crc32c = encoded_records_crc32c(&records)?;
    if actual_crc32c != expected_crc32c {
        return Err(Error::InvalidSegmentData(format!(
            "segment {} quorum bytes have CRC32C {actual_crc32c:08x}, expected {expected_crc32c:08x}",
            segment.base_record_index
        )));
    }
    Ok(records)
}

fn decoded_sealed_snapshot(
    snapshot: &ReplicaSnapshot,
    expected_records: u64,
) -> Option<Vec<RecordFrame>> {
    if !snapshot.finalized || !valid_format(&snapshot.metadata) {
        return None;
    }
    let records = RecordFrame::decode_all(&snapshot.bytes).ok()?;
    (records.len() as u64 == expected_records).then_some(records)
}

fn encoded_records_crc32c(records: &[RecordFrame]) -> Result<u32, Error> {
    let mut crc32c = 0;
    for record in records {
        crc32c = crc32c::crc32c_append(crc32c, &record.encode()?);
    }
    Ok(crc32c)
}

/// Provision a spare successor under an engine-allocated ordered id:
/// conditional create on every zone, append sessions opened —
/// everything rotation needs, done entirely off the hot path. Its metadata
/// is fixed at creation and never touched again. A crash before the swap
/// leaves an orphan outside the manifest directory that a later recovery
/// delegates to asynchronous maintenance cleanup.
mod rotation {
    use super::*;

    pub(crate) fn provision_replicas(
        factories: &[Arc<dyn ReplicaFactory>],
        prefix: &str,
        id: &str,
    ) -> Vec<Arc<dyn Replica>> {
        replicas_for(factories, &segment_object(prefix, id))
    }

    pub(crate) async fn provision_spare_with_replicas(
        replicas: Vec<Arc<dyn Replica>>,
        config: ClientConfig,
        max_replica_lag_bytes: usize,
        lane_stall_timeout: Duration,
        id: String,
        metrics: Arc<Metrics>,
    ) -> Result<(String, Writer), Error> {
        let mut writer = QuorumVolume::with_metadata(
            replicas,
            config,
            crate::protocol::protocol_metadata(),
            metrics,
        )?
        .create_writer()
        .await?;
        writer.set_max_replica_lag_bytes(max_replica_lag_bytes);
        writer.set_lane_stall_timeout(lane_stall_timeout);
        Ok((id, writer))
    }

    pub(crate) async fn provision_spare(
        factories: Vec<Arc<dyn ReplicaFactory>>,
        prefix: String,
        config: ClientConfig,
        max_replica_lag_bytes: usize,
        lane_stall_timeout: Duration,
        id: String,
        metrics: Arc<Metrics>,
    ) -> Result<(String, Writer), Error> {
        let replicas = provision_replicas(&factories, &prefix, &id);
        provision_spare_with_replicas(
            replicas,
            config,
            max_replica_lag_bytes,
            lane_stall_timeout,
            id,
            metrics,
        )
        .await
    }

    pub(crate) async fn provision_registered_spare_with_replicas(
        parts: ProvisionParts,
        id: String,
        replicas: Vec<Arc<dyn Replica>>,
    ) -> Result<RegisteredSpare, Error> {
        let ProvisionParts {
            client_config,
            max_replica_lag_bytes,
            lane_stall_timeout,
            metrics,
            manifest,
            ..
        } = parts;
        let (id, writer) = provision_spare_with_replicas(
            replicas,
            client_config,
            max_replica_lag_bytes,
            lane_stall_timeout,
            id,
            metrics,
        )
        .await?;
        let manifest = manifest
            .register_pending(id.clone())
            .await
            .map_err(Error::from)?;
        Ok(RegisteredSpare {
            id,
            writer,
            manifest,
        })
    }

    pub(crate) async fn fold_registered_pending(
        manifest: ManifestAccess,
        fold: PendingFold,
    ) -> Result<ManifestUpdate, Error> {
        manifest.fold_pending(fold).await.map_err(Error::from)
    }
}

pub(crate) use rotation::{
    fold_registered_pending, provision_registered_spare_with_replicas, provision_replicas,
    provision_spare, provision_spare_with_replicas,
};

/// Reconstruct and enforce a seal whose record range is already decided.
///
/// This path owns no live append lanes and takes no new protocol decision. A
/// read quorum is fenced and reduced to the exact committed record count, the
/// resulting bytes are checked against the manifest digest when one names the
/// decision, and [`QuorumVolume::enforce_seal`] installs that immutable prefix
/// on a finalized quorum. Consequently the whole operation is idempotent:
/// maintenance may retry it after [`Writer::seal`] consumed its lanes, and
/// recovery may repeat it after a crash at any intermediate rewrite.
mod maintenance {
    use super::*;

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn enforce_committed_seal(
        factories: &[Arc<dyn ReplicaFactory>],
        prefix: &str,
        client_config: &ClientConfig,
        metrics: Arc<Metrics>,
        id: &str,
        base_record_index: u64,
        end_record_index: u64,
        expected_digest: Option<&str>,
        expected_crc32c: u32,
    ) -> Result<SegmentDescriptor, Error> {
        let expected_records_u64 = end_record_index
            .checked_sub(base_record_index)
            .and_then(|span| span.checked_add(1))
            .ok_or_else(|| {
                Error::InvalidCatalog(format!(
                    "segment {base_record_index} has invalid committed end {end_record_index}"
                ))
            })?;
        let expected_records = usize::try_from(expected_records_u64).map_err(|_| {
            Error::InvalidCatalog(format!(
                "segment {base_record_index} record count does not fit in memory"
            ))
        })?;
        let object = segment_object(prefix, id);
        let volume = QuorumVolume::with_metadata(
            replicas_for(factories, &object),
            client_config.clone(),
            crate::protocol::protocol_metadata(),
            metrics,
        )?;
        let recovered = volume.recover_for_seal(Some(expected_records)).await?;
        if recovered.len() != expected_records {
            return Err(Error::InvalidCatalog(format!(
                "segment {base_record_index} recovered {} records, expected {expected_records}",
                recovered.len()
            )));
        }
        if let Some(expected) = expected_digest {
            let actual = recovered.digest();
            if actual != expected {
                return Err(Error::InvalidCatalog(format!(
                "segment {base_record_index} recovered seal digest {actual}, expected {expected}"
            )));
            }
        }
        let actual = recovered.crc32c();
        if actual != expected_crc32c {
            return Err(Error::InvalidCatalog(format!(
                "segment {base_record_index} recovered CRC32C {actual:08x}, expected {expected_crc32c:08x}"
            )));
        }
        volume.enforce_seal(recovered.canonical()).await?;
        let quorum = majority(factories.len());
        Ok(SegmentDescriptor {
            id: id.to_string(),
            base_record_index,
            end_record_index,
            crc32c: expected_crc32c,
            copies: quorum,
            finalized_copies: quorum,
            seal_pending: false,
        })
    }

    /// One repair pass over `segments` at the committed `floor`: stat-precheck
    /// every copy, re-replicate from a healthy finalized source only when a copy
    /// is missing, rotted, or divergent. Free of writer state so the background
    /// maintenance task can run it concurrently with appends.
    pub(crate) async fn repair_sealed_pass(
        factories: &[Arc<dyn ReplicaFactory>],
        prefix: &str,
        segments: &[SegmentDescriptor],
        floor: u64,
    ) -> Result<RepairReport, Error> {
        let mut report = RepairReport::default();
        for segment in segments {
            if segment.end_record_index < floor {
                continue;
            }
            if segment.seal_pending {
                // The fold CAS committed this seal but maintenance has not
                // finalized the object yet: there is no finalized source to copy
                // from by design, not by loss. The seal's own follow-up pass
                // returns here with the flag cleared.
                continue;
            }
            report.segments_examined += 1;
            let object = segment_object(prefix, &segment.id);
            let expected_crc32c = segment.crc32c;

            // Health pre-check by stat only. The committed CRC32C independently
            // identifies every healthy copy without a content read.
            let replicas = replicas_for(factories, &object);
            let stats = join_all(replicas.iter().map(|replica| replica.stat())).await;
            let mut targets = Vec::new();
            for (replica, stat) in replicas.iter().zip(stats) {
                let healthy = stat.is_ok_and(|stat| {
                    stat.finalized
                        && valid_format(&stat.metadata)
                        && stat.crc32c == Some(expected_crc32c)
                });
                if healthy {
                    report.objects_already_healthy += 1;
                } else {
                    targets.push(Arc::clone(replica));
                }
            }
            if targets.is_empty() {
                continue;
            }

            // `seal_pending` already filtered the only segment that legitimately
            // has no finalized copy, so a missing read quorum here is real loss
            // and aborts the pass loudly.
            let bytes = read_one_sealed_source(factories, &object, segment).await?;
            // a rewritten copy carries the constant creation metadata: the
            // format marker only, like every segment object
            let metadata = crate::protocol::protocol_metadata();
            for replica in targets {
                match repair_sealed_copy(
                    &replica,
                    Bytes::from(bytes.clone()),
                    metadata.clone(),
                    expected_crc32c,
                )
                .await
                {
                    Ok(true) => report.objects_repaired += 1,
                    Ok(false) => report.objects_already_healthy += 1,
                    Err(error) if error.code.transient() => report.transient_failures += 1,
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Ok(report)
    }

    /// Restore one manifest-owned historical segment to a finalized quorum.
    ///
    /// Startup needs this stronger gate before it can admit new work, but it
    /// need not wait for an unreachable minority once a quorum is healthy.
    /// The manifest CRC32C identifies a surviving canonical source. Recovery
    /// reads every reachable copy because a metadata stat can still report the
    /// original provider checksum when the content read detects storage rot;
    /// only finalized, well-formed bytes whose computed CRC matches count.
    /// Each successful repair is finalized and checksum-verified before
    /// counting.
    pub(crate) async fn restore_sealed_quorum(
        factories: &[Arc<dyn ReplicaFactory>],
        prefix: &str,
        segment: &SegmentDescriptor,
    ) -> Result<usize, Error> {
        let quorum = majority(factories.len());
        let expected_crc32c = segment.crc32c;
        let object = segment_object(prefix, &segment.id);
        let replicas = replicas_for(factories, &object);
        let expected_records = segment
            .end_record_index
            .checked_sub(segment.base_record_index)
            .and_then(|span| span.checked_add(1))
            .ok_or_else(|| {
                Error::InvalidCatalog(format!(
                    "sealed segment {} has no valid committed end",
                    segment.base_record_index
                ))
            })?;
        let snapshots = join_all(replicas.iter().map(|replica| replica.snapshot())).await;
        let mut healthy = 0;
        let mut repair_targets = Vec::new();
        let mut canonical = None;
        for (replica, snapshot) in replicas.iter().zip(snapshots) {
            match snapshot {
                Ok(snapshot)
                    if snapshot.crc32c == Some(expected_crc32c)
                        && crc32c::crc32c(&snapshot.bytes) == expected_crc32c
                        && decoded_sealed_snapshot(&snapshot, expected_records).is_some() =>
                {
                    healthy += 1;
                    canonical.get_or_insert(snapshot.bytes);
                }
                _ => repair_targets.push(Arc::clone(replica)),
            }
        }
        if healthy >= quorum {
            return Ok(healthy);
        }

        let bytes = canonical.ok_or(Error::NoReadQuorum)?;
        let metadata = crate::protocol::protocol_metadata();
        for replica in repair_targets {
            match repair_sealed_copy(
                &replica,
                Bytes::from(bytes.clone()),
                metadata.clone(),
                expected_crc32c,
            )
            .await
            {
                Ok(_) => {
                    healthy += 1;
                    if healthy >= quorum {
                        return Ok(healthy);
                    }
                }
                Err(error) if error.code.transient() => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(Error::NoReadQuorum)
    }

    /// One floor-committed truncation pass, free of writer state so the
    /// background maintenance task can run it (serialized with repair on the
    /// same task) concurrently with appends. The truncator discipline is
    /// epoch-free: the manifest CAS raises `chorus.trunc` monotonically while
    /// preserving every other field.
    ///
    /// The work list is the register's segment directory, not this process's
    /// chain snapshot: an entry leaves the directory only once its copy is
    /// confirmed deleted on every zone, so an entry whose zone slept through a
    /// pass survives as a tombstone the next pass retries — no bucket relisting
    /// and no separate tombstone objects. The cached `(directory, tail_base)`
    /// pair is internally consistent even when stale (the tail base only moves
    /// in the same CAS that appends an entry), so derived ends are always right
    /// for the entries the cache knows about.
    pub(crate) async fn truncate_pass(
        factories: &[Arc<dyn ReplicaFactory>],
        prefix: &str,
        sealed_segments: &mut Vec<SegmentDescriptor>,
        checkpoint_floor: &mut u64,
        manifest: &mut Manifest,
        floor: WalSeqNo,
    ) -> Result<TruncationReport, Error> {
        let floor = floor.record_index;
        if floor < *checkpoint_floor {
            return Err(Error::CheckpointRegression {
                current: WalSeqNo::record(*checkpoint_floor),
                requested: WalSeqNo::record(floor),
            });
        }
        // commit the floor through the regional register before deleting
        // anything: recovery and repair ignore objects below it, so a stale
        // zone cannot resurrect deleted history even if the database loses
        // its own checkpoint
        manifest.raise_trunc(floor).await.map_err(Error::from)?;
        let report =
            delete_segments_below_committed_floor(factories, prefix, sealed_segments, manifest)
                .await?;
        *checkpoint_floor = floor;
        Ok(report)
    }

    /// Delete segment objects left behind by dead writer incarnations.
    ///
    /// The carried keep set is the recovery claim's complete directory and tail
    /// snapshot. It is deliberately allowed to age: an id is eligible only when
    /// it is absent from that set and its embedded epoch is strictly below the
    /// claimed epoch. The claiming incarnation and every successor mint ids at
    /// or above that boundary, so a later manifest can never make an eligible
    /// below-epoch orphan part of replay. Unknown formats, kept ids, and ids at
    /// or above the boundary are never deleted.
    ///
    /// Each delete is generation-matched and therefore idempotent. Transient
    /// per-zone failures are counted as deferred work so maintenance retains the
    /// sweep for a later pass. Terminal failures are also retained in the report
    /// while the pass continues best-effort in other zones; they remain
    /// maintenance-only and retry on a future tick or restart.
    pub(crate) async fn sweep_dead_segments(
        factories: &[Arc<dyn ReplicaFactory>],
        prefix: &str,
        sweep: &DeadSegmentSweep,
    ) -> DeadSegmentSweepReport {
        let objects_prefix = format!("{prefix}/segments/");
        let listed = join_all(
            factories
                .iter()
                .map(|factory| factory.list(&objects_prefix)),
        )
        .await;
        let mut ids = BTreeSet::new();
        let mut deferred_operations = 0usize;
        let mut failure = None;
        for result in listed {
            match result {
                Ok(objects) => {
                    for object in objects {
                        // The sweep is a janitor, not a format gate.
                        if !valid_format(&object.metadata) {
                            continue;
                        }
                        if let Some(id) = object.name.strip_prefix(&objects_prefix) {
                            ids.insert(id.to_string());
                        }
                    }
                }
                Err(error) if error.code.transient() => deferred_operations += 1,
                Err(error) => {
                    deferred_operations += 1;
                    if failure.is_none() {
                        failure = Some(error.into());
                    }
                }
            }
        }

        let mut orphan_segments = 0usize;
        let mut deleted_objects = 0usize;
        for id in ids {
            if sweep.keep.contains(&id) {
                continue;
            }
            let Some(epoch) = id_epoch(&id) else {
                continue;
            };
            if epoch >= sweep.claimed_epoch {
                continue;
            }

            orphan_segments += 1;
            let object = segment_object(prefix, &id);
            for replica in replicas_for(factories, &object) {
                match replica.stat().await {
                    Ok(snapshot) => match replica.delete(snapshot.generation).await {
                        Ok(()) => deleted_objects += 1,
                        Err(error) if error.code == TransportCode::NotFound => {}
                        Err(error) if error.code.transient() => deferred_operations += 1,
                        Err(error) => {
                            deferred_operations += 1;
                            if failure.is_none() {
                                failure = Some(error.into());
                            }
                        }
                    },
                    Err(error) if error.code == TransportCode::NotFound => {}
                    Err(error) if error.code.transient() => deferred_operations += 1,
                    Err(error) => {
                        deferred_operations += 1;
                        if failure.is_none() {
                            failure = Some(error.into());
                        }
                    }
                }
            }
        }

        DeadSegmentSweepReport {
            orphan_segments,
            deleted_objects,
            deferred_operations,
            failure,
        }
    }

    /// Retry deletion tombstones already authorized by the committed manifest
    /// floor, without advancing that floor.
    ///
    /// This is the autonomous part of truncation maintenance: startup and periodic
    /// ticks may remove storage and directory entries for history the application
    /// previously made unreachable, but they never choose a new checkpoint. The
    /// manifest is refreshed first so a task that slept through the original
    /// `truncate_before` observes both the latest monotone floor and every
    /// surviving tombstone.
    pub(crate) async fn cleanup_tombstones_pass(
        factories: &[Arc<dyn ReplicaFactory>],
        prefix: &str,
        sealed_segments: &mut Vec<SegmentDescriptor>,
        manifest: &mut Manifest,
    ) -> Result<TruncationReport, Error> {
        manifest.refreshed_record().await.map_err(Error::from)?;
        delete_segments_below_committed_floor(factories, prefix, sealed_segments, manifest).await
    }

    /// Delete directory entries wholly below the manifest's cached floor and drop
    /// only fully absent entries from the register.
    ///
    /// Generation-matched deletes make every zonal action idempotent. The pending
    /// seal exclusion is intentionally shared by application truncation and
    /// autonomous cleanup: a committed floor authorizes deletion, but the engine's
    /// finalized-quorum gate still needs its source object until enforcement
    /// completes.
    async fn delete_segments_below_committed_floor(
        factories: &[Arc<dyn ReplicaFactory>],
        prefix: &str,
        sealed_segments: &mut Vec<SegmentDescriptor>,
        manifest: &mut Manifest,
    ) -> Result<TruncationReport, Error> {
        let record = manifest.record().clone();
        let floor = record.trunc;
        let directory = record.segments.clone();
        // A swap whose maintenance seal has not settled yet is published with
        // `seal_pending` in the chain snapshot (the engine sends the snapshot
        // before it enqueues the seal). Deleting copies underneath that
        // in-flight seal would fail it and gate rotation until restart, so
        // those entries wait for a later pass.
        let seal_in_flight: HashSet<&str> = sealed_segments
            .iter()
            .filter(|segment| segment.seal_pending)
            .map(|segment| segment.id.as_str())
            .collect();
        let mut deleted_objects = 0usize;
        let mut deleted_bases = HashSet::new();
        let mut fully_deleted = HashSet::new();
        for (index, entry) in directory.iter().enumerate() {
            let next_base = directory
                .get(index + 1)
                .map_or(record.tail_base, |next| next.base);
            let Some(end) = next_base.checked_sub(1) else {
                continue;
            };
            if end >= floor {
                continue;
            }
            if seal_in_flight.contains(entry.id.as_str()) {
                continue;
            }
            let object = segment_object(prefix, &entry.id);
            let replicas = replicas_for(factories, &object);
            let snapshots = join_all(replicas.iter().map(|replica| replica.stat())).await;
            let mut absent = 0usize;
            for (replica, snapshot) in replicas.iter().zip(snapshots) {
                match snapshot {
                    // Any copy of a directory entry below the committed floor is
                    // deletable, finalized or not: the entry is a committed
                    // seal, so admission to the object ended long ago and a
                    // straggling unfinalized lane copy is just deleted history.
                    Ok(snapshot) => match replica.delete(snapshot.generation).await {
                        Ok(()) => {
                            deleted_objects += 1;
                            absent += 1;
                        }
                        Err(error) if error.code == TransportCode::NotFound => absent += 1,
                        Err(error) if error.code.transient() => {}
                        Err(error) => return Err(error.into()),
                    },
                    Err(error) if error.code == TransportCode::NotFound => absent += 1,
                    Err(error) if error.code.transient() => {}
                    Err(error) => return Err(error.into()),
                }
            }
            if absent >= majority(replicas.len()) {
                deleted_bases.insert(entry.base);
            }
            if absent == replicas.len() {
                fully_deleted.insert(entry.id.clone());
            }
        }
        let chain_before = sealed_segments.len();
        sealed_segments.retain(|segment| !deleted_bases.contains(&segment.base_record_index));
        let deleted_segments = chain_before - sealed_segments.len();
        manifest
            .remove_segments(&fully_deleted, floor)
            .await
            .map_err(Error::from)?;
        Ok(TruncationReport {
            deleted_objects,
            deleted_segments,
        })
    }

    async fn read_one_sealed_source(
        factories: &[Arc<dyn ReplicaFactory>],
        object: &str,
        segment: &SegmentDescriptor,
    ) -> Result<Vec<u8>, Error> {
        let records = read_sealed_segment(factories, object, segment).await?;
        let mut bytes = Vec::new();
        for record in records {
            bytes.extend_from_slice(&record.encode()?);
        }
        Ok(bytes)
    }

    async fn repair_sealed_copy(
        replica: &Arc<dyn Replica>,
        bytes: Bytes,
        metadata: HashMap<String, String>,
        expected_crc32c: u32,
    ) -> Result<bool, TransportError> {
        let current = match replica.snapshot().await {
            Ok(snapshot) => Some(snapshot),
            Err(error) if error.code == TransportCode::NotFound => None,
            Err(error) if error.code == TransportCode::DataLoss => {
                // Rotted copy: the content read fails but the content-blind stat
                // still names the generation. Delete exactly that generation and
                // fall through to recreate from the healthy canonical bytes. A
                // racing repairer makes the guarded delete fail, and the next
                // pass converges.
                let stat = replica.stat().await?;
                match replica.delete(stat.generation).await {
                    Ok(()) => {}
                    Err(error) if error.code == TransportCode::NotFound => {}
                    Err(error) => return Err(error),
                }
                None
            }
            Err(error) => return Err(error),
        };
        if current.as_ref().is_some_and(|snapshot| {
            snapshot.finalized
                && snapshot.bytes == bytes
                && valid_format(&snapshot.metadata)
                && snapshot.crc32c == Some(expected_crc32c)
                && metadata
                    .iter()
                    .all(|(key, value)| snapshot.metadata.get(key) == Some(value))
        }) {
            return Ok(false);
        }

        let mut token = match current {
            Some(snapshot) => {
                replica
                    .replace_appendable(&snapshot, bytes.clone(), metadata)
                    .await?
            }
            None => {
                let created = replica.create_appendable(metadata).await?;
                let mut token = replica.takeover(&created).await?;
                if !bytes.is_empty() {
                    token.persisted_size = replica.append(&token, 0, bytes.to_vec()).await?;
                }
                token
            }
        };
        token.persisted_size = bytes.len() as i64;
        let finalized = replica.finalize(&mut token, bytes.len() as i64).await?;
        if finalized.crc32c != Some(expected_crc32c) {
            let actual = finalized
                .crc32c
                .map(|crc| format!("{crc:08x}"))
                .unwrap_or_else(|| "missing".into());
            return Err(TransportError {
                zone: finalized.zone,
                code: TransportCode::DataLoss,
                message: format!(
                    "repaired sealed object reported CRC32C {actual}, expected {expected_crc32c:08x}"
                ),
            });
        }
        Ok(true)
    }
}

pub(crate) use maintenance::{
    cleanup_tombstones_pass, enforce_committed_seal, repair_sealed_pass, restore_sealed_quorum,
    sweep_dead_segments, truncate_pass,
};

fn replay_records(
    segment: &SegmentDescriptor,
    frames: &[RecordFrame],
    from: WalSeqNo,
    end: WalSeqNo,
) -> Result<Vec<WalRecord>, Error> {
    let mut records = Vec::new();
    for (offset, frame) in frames.iter().enumerate() {
        let record_index = segment.base_record_index + offset as u64;
        if record_index < from.record_index || record_index >= end.record_index {
            continue;
        }
        records.push(WalRecord {
            seqno: WalSeqNo::record(record_index),
            payload: frame.payload.clone(),
        });
    }
    Ok(records)
}

fn replicas_for(factories: &[Arc<dyn ReplicaFactory>], object: &str) -> Vec<Arc<dyn Replica>> {
    factories
        .iter()
        .map(|factory| factory.replica(object))
        .collect()
}
