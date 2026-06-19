use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use sha2::{Digest, Sha256};
use tokio::sync::watch;

use crate::grpc::pack_append;
use crate::metrics::Metrics;
use crate::record::{RecordError, RecordFrame};
use crate::transport::{
    AppendToken, LaneDurableChange, PackedAppend, Replica, ReplicaSnapshot, TransportCode,
    TransportError, FORMAT_VERSION, META_FORMAT,
};

/// Replication widths the protocol supports. Each uses a strict-majority
/// quorum: 1-of-1, 2-of-3, 3-of-5.
pub(crate) const SUPPORTED_REPLICA_COUNTS: [usize; 3] = [1, 3, 5];

/// A lane must make some durable-tail progress within this interval whenever
/// it retains unacknowledged writes. Five seconds is deliberately above the
/// default exponential-backoff budget while bounding a genuinely stuck lane
/// well below the retained-byte limit at normal WAL throughputs.
pub(crate) const DEFAULT_LANE_STALL_TIMEOUT: Duration = Duration::from_secs(5);

/// Strict-majority quorum size for a replica set of `replica_count` zones.
pub(crate) fn majority(replica_count: usize) -> usize {
    replica_count / 2 + 1
}

fn select_recovery_size(sizes: &mut [i64], replica_count: usize) -> Option<i64> {
    let quorum = majority(replica_count);
    if sizes.len() < quorum || sizes.len() > replica_count {
        return None;
    }
    sizes.sort_unstable();
    let unavailable = replica_count - sizes.len();
    let required_available_support = quorum.checked_sub(unavailable)?;
    (required_available_support > 0).then(|| sizes[sizes.len() - required_available_support])
}

pub(crate) type AttemptedBytes = Arc<dyn Fn(u64) + Send + Sync>;

#[derive(Clone, Debug)]
/// Retry policy for transport operations used by recovery and writes.
///
/// Only transient transport codes are retried. `FAILED_PRECONDITION` and
/// append-open `ABORTED` are terminal fencing signals and stop the writer.
pub struct ClientConfig {
    /// Number of retries after the initial attempt.
    pub max_retries: usize,
    /// Base exponential-backoff delay. DST uses zero; production should retain
    /// a nonzero value to avoid synchronized retry pressure.
    pub retry_base: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            retry_base: Duration::from_millis(20),
        }
    }
}

pub(crate) struct QuorumVolume {
    replicas: Vec<Arc<dyn Replica>>,
    config: ClientConfig,
    metadata: HashMap<String, String>,
    metrics: Arc<Metrics>,
}

/// A live lane: the ordered work channel into its writer task plus the task
/// handle that yields the final token at shutdown.
struct LaneHandle {
    work: tokio::sync::mpsc::UnboundedSender<LaneBatch>,
    done: tokio::task::JoinHandle<Option<AppendToken>>,
    budget: Arc<LaneBudget>,
    stall_timeout: Arc<LaneStallTimeout>,
}

#[derive(Debug)]
struct LaneBudget {
    outstanding: AtomicUsize,
    limit: AtomicUsize,
}

impl LaneBudget {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            outstanding: AtomicUsize::new(0),
            limit: AtomicUsize::new(usize::MAX),
        })
    }

    fn set_limit(&self, limit: usize) {
        self.limit.store(limit, Ordering::Relaxed);
    }

    fn try_reserve(self: &Arc<Self>, bytes: usize) -> Option<Arc<LaneReservation>> {
        let mut current = self.outstanding.load(Ordering::Relaxed);
        loop {
            let next = current.checked_add(bytes)?;
            if next > self.limit.load(Ordering::Relaxed) {
                return None;
            }
            match self.outstanding.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(Arc::new(LaneReservation {
                        budget: Arc::clone(self),
                        bytes,
                    }));
                }
                Err(observed) => current = observed,
            }
        }
    }
}

#[derive(Debug)]
struct LaneStallTimeout {
    nanos: AtomicU64,
}

impl LaneStallTimeout {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            nanos: AtomicU64::new(Self::encode(DEFAULT_LANE_STALL_TIMEOUT)),
        })
    }

    fn set(&self, timeout: Duration) {
        self.nanos.store(Self::encode(timeout), Ordering::Relaxed);
    }

    fn get(&self) -> Duration {
        Duration::from_nanos(self.nanos.load(Ordering::Relaxed))
    }

    fn encode(timeout: Duration) -> u64 {
        u64::try_from(timeout.as_nanos()).unwrap_or(u64::MAX).max(1)
    }
}

#[derive(Debug)]
struct LaneReservation {
    budget: Arc<LaneBudget>,
    bytes: usize,
}

impl Drop for LaneReservation {
    fn drop(&mut self) {
        self.budget
            .outstanding
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

pub(crate) struct Writer {
    replicas: Vec<Arc<dyn Replica>>,
    config: ClientConfig,
    lanes: Vec<Option<LaneHandle>>,
    admitted: AdmittedPrefix,
    commits: Arc<CommitTracker>,
    sealed: bool,
    metadata: HashMap<String, String>,
    metrics: Arc<Metrics>,
}

/// Lightweight description of a live segment's admitted prefix.
///
/// Replica lanes retain unacknowledged encoded chunks and their boundaries for
/// retry. The writer retains only admitted byte/record counts plus the ordered
/// seal digest and CRC32C; the commit tracker separately holds unresolved
/// boundaries. Canonical bytes are reconstructed from storage only if degraded
/// finalization requires enforcement. SHA-256 updates run after lane dispatch
/// and are awaited only when rotation freezes the prefix.
struct AdmittedPrefix {
    records: usize,
    bytes: usize,
    digest: DigestState,
    crc32c: u32,
}

enum DigestState {
    Ready(Sha256),
    Pending {
        sender: tokio::sync::mpsc::UnboundedSender<Arc<[Bytes]>>,
        task: tokio::task::JoinHandle<Sha256>,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct SealReport {
    finalized: Vec<Option<ReplicaSnapshot>>,
}

impl SealReport {
    pub fn all_replicas_finalized(&self) -> bool {
        // the default report (slow-path enforcement) is empty and must keep
        // requesting targeted repair
        !self.finalized.is_empty()
            && self.finalized.iter().flatten().count() == self.finalized.len()
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct CanonicalPrefix {
    bytes: Vec<u8>,
    records: Vec<RecordFrame>,
    record_ends: Vec<usize>,
}

/// The canonical content of a fenced tail segment, computed by
/// [`QuorumVolume::recover_for_seal`]. The seal *decision* (advancing the
/// manifest `tail_base` with the canonical digest) belongs to the caller;
/// [`QuorumVolume::enforce_seal`] then installs and finalizes the bytes.
pub(crate) struct RecoveredTail {
    canonical: CanonicalPrefix,
    had_discarded_suffix: bool,
}

/// Result of fencing one manifest candidate in recovery order.
pub(crate) enum RecoveryCandidate {
    /// A quorum confirmed the object name is absent.
    Absent,
    /// The committed complete-record prefix is empty. A writer is reusable only
    /// when every replica supplied a live zero-length session; finalized-empty
    /// or partial-record witnesses prove the empty frontier but require the
    /// caller to retire this object name.
    Empty {
        reusable_writer: Option<Box<Writer>>,
    },
    /// A non-empty committed prefix, frozen for seal enforcement.
    NonEmpty(RecoveredTail),
}

impl RecoveredTail {
    pub fn len(&self) -> usize {
        self.canonical.len()
    }

    /// SHA-256 hex digest of the exact canonical bytes (the manifest
    /// `chorus.seal_digest` value).
    pub fn digest(&self) -> String {
        digest_bytes(&self.canonical.bytes)
    }

    /// Full-object CRC32C of the exact canonical bytes.
    pub fn crc32c(&self) -> u32 {
        crc32c::crc32c(&self.canonical.bytes)
    }

    pub fn canonical(&self) -> &CanonicalPrefix {
        &self.canonical
    }

    /// Whether any fenced witness extended past the recovered complete-record
    /// prefix. A pending successor above such a tail gap is speculative: the
    /// engine could not have acknowledged it before the missing tail record.
    pub fn had_discarded_suffix(&self) -> bool {
        self.had_discarded_suffix
    }
}

#[derive(Clone, Debug)]
enum CommitFailure {
    Poisoned,
    Fenced(String),
    Transport(TransportError),
}

impl CommitFailure {
    fn protocol_error(&self) -> ProtocolError {
        match self {
            Self::Poisoned => ProtocolError::Poisoned,
            Self::Fenced(error) => ProtocolError::Fenced(error.clone()),
            Self::Transport(error) => ProtocolError::Transport(error.clone()),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct CommitSnapshot {
    committed: usize,
    failure: Option<CommitFailure>,
}

#[derive(Debug)]
struct LaneCommitState {
    durable: i64,
    represented_end: i64,
    finished: bool,
    error: Option<TransportError>,
}

impl Default for LaneCommitState {
    fn default() -> Self {
        Self {
            durable: 0,
            represented_end: 0,
            finished: true,
            error: None,
        }
    }
}

#[derive(Debug)]
struct CommitState {
    boundaries: VecDeque<i64>,
    admitted: usize,
    committed: usize,
    committed_bytes: usize,
    failure: Option<CommitFailure>,
    lanes: Vec<LaneCommitState>,
}

/// Segment-scoped aggregation of lane durability. Lanes publish monotonic byte
/// offsets; this tracker resolves the quorum offset against only the unresolved
/// record boundaries and broadcasts one contiguous record watermark.
struct CommitTracker {
    quorum: usize,
    state: Mutex<CommitState>,
    updates: watch::Sender<CommitSnapshot>,
    metrics: Arc<Metrics>,
}

pub(crate) struct CommitRange {
    first_offset: usize,
    end_offset: usize,
    updates: watch::Receiver<CommitSnapshot>,
}

#[cfg(test)]
pub(crate) struct PendingCommit {
    pub logical_offset: u64,
    updates: watch::Receiver<CommitSnapshot>,
}

#[cfg(test)]
impl PendingCommit {
    pub async fn wait(mut self) -> Result<u64, ProtocolError> {
        loop {
            let snapshot = self.updates.borrow_and_update().clone();
            if snapshot.committed > self.logical_offset as usize {
                return Ok(self.logical_offset);
            }
            if let Some(failure) = snapshot.failure {
                return Err(failure.protocol_error());
            }
            self.updates
                .changed()
                .await
                .map_err(|_| ProtocolError::PipelineClosed)?;
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProtocolError {
    #[error("the replica count must be 1, 3, or 5")]
    ReplicaCount,
    #[error("operation did not reach a replica quorum")]
    NoQuorum,
    #[error("writer is poisoned by an indeterminate record and must be recovered")]
    Poisoned,
    #[error("writer was fenced: {0}")]
    Fenced(String),
    #[error("recovery witnesses contain different bytes at record {record_index}")]
    ConflictingPrefix { record_index: usize },
    #[error("recovery prefix has {actual} records, expected at least {expected}")]
    RecoveryPrefixTooShort { expected: usize, actual: usize },
    #[error("recovered seal digest {actual} does not match committed digest {expected}")]
    SealDigestMismatch { expected: String, actual: String },
    #[error("recovered seal CRC32C {actual:08x} does not match committed CRC32C {expected:08x}")]
    SealCrc32cMismatch { expected: u32, actual: u32 },
    #[error("manifest register is invalid: {0}")]
    InvalidManifest(String),
    #[error(
        "the manifest segment directory is full: truncate the WAL to free \
         retained sealed segments before sealing again"
    )]
    SegmentDirectoryFull,
    #[error(transparent)]
    ManifestStore(#[from] crate::manifest_store::ManifestStoreError),
    #[error("manifest register is unavailable")]
    ManifestUnavailable,
    #[error("commit pipeline closed before reporting a result")]
    PipelineClosed,
    #[error("segment writer is sealed")]
    Finalized,
    #[error(transparent)]
    Record(#[from] RecordError),
    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
}

impl CanonicalPrefix {
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn into_records(self) -> Vec<RecordFrame> {
        self.records
    }

    fn truncate(&mut self, records: usize) {
        self.records.truncate(records);
        self.record_ends.truncate(records);
        self.bytes.truncate(self.committed_bytes_len(records));
    }

    fn committed_bytes_len(&self, records: usize) -> usize {
        records
            .checked_sub(1)
            .and_then(|index| self.record_ends.get(index).copied())
            .unwrap_or(0)
    }

    fn record_bytes(&self, index: usize) -> &[u8] {
        let start = index
            .checked_sub(1)
            .and_then(|previous| self.record_ends.get(previous).copied())
            .unwrap_or(0);
        &self.bytes[start..self.record_ends[index]]
    }

    fn from_snapshot(snapshot: &ReplicaSnapshot) -> Self {
        let (records, consumed) = RecordFrame::decode_complete_prefix(&snapshot.bytes);
        let mut record_ends = Vec::with_capacity(records.len());
        let mut end = 0usize;
        for record in &records {
            end += record.encode().expect("a decoded record must encode").len();
            record_ends.push(end);
        }
        Self {
            bytes: snapshot.bytes[..consumed].to_vec(),
            records,
            record_ends,
        }
    }
}

impl Default for AdmittedPrefix {
    fn default() -> Self {
        Self {
            records: 0,
            bytes: 0,
            digest: DigestState::Ready(Sha256::new()),
            crc32c: 0,
        }
    }
}

impl AdmittedPrefix {
    fn len(&self) -> usize {
        self.records
    }

    fn is_empty(&self) -> bool {
        self.records == 0
    }

    fn bytes_len(&self) -> usize {
        self.bytes
    }

    fn extend_metadata(&mut self, chunks: &[Bytes]) {
        for chunk in chunks {
            self.crc32c = crc32c::crc32c_append(self.crc32c, chunk);
            self.bytes += chunk.len();
            self.records += 1;
        }
    }

    fn queue_digest(&mut self, chunks: Arc<[Bytes]>) {
        let previous = std::mem::replace(&mut self.digest, DigestState::Ready(Sha256::new()));
        let pending = match previous {
            DigestState::Ready(mut hasher) => {
                let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<Arc<[Bytes]>>();
                let task = tokio::spawn(async move {
                    while let Some(chunks) = receiver.recv().await {
                        hasher = tokio::task::spawn_blocking(move || {
                            for chunk in chunks.iter() {
                                hasher.update(chunk);
                            }
                            hasher
                        })
                        .await
                        .expect("ordered digest task failed");
                    }
                    hasher
                });
                sender.send(chunks).expect("digest worker failed");
                DigestState::Pending { sender, task }
            }
            DigestState::Pending { sender, task } => {
                sender.send(chunks).expect("digest worker failed");
                DigestState::Pending { sender, task }
            }
        };
        self.digest = pending;
    }

    async fn digest(&mut self) -> String {
        let pending = std::mem::replace(&mut self.digest, DigestState::Ready(Sha256::new()));
        let hasher = match pending {
            DigestState::Ready(hasher) => hasher,
            DigestState::Pending { sender, task } => {
                drop(sender);
                task.await.expect("ordered digest task failed")
            }
        };
        let digest = digest_hex(hasher.clone().finalize());
        self.digest = DigestState::Ready(hasher);
        digest
    }

    async fn shutdown_digest(&mut self) {
        let digest = std::mem::replace(&mut self.digest, DigestState::Ready(Sha256::new()));
        if let DigestState::Pending { sender, task } = digest {
            drop(sender);
            let _ = task.await;
        }
    }

    fn crc32c(&self) -> u32 {
        self.crc32c
    }
}

impl Drop for AdmittedPrefix {
    fn drop(&mut self) {
        let digest = std::mem::replace(&mut self.digest, DigestState::Ready(Sha256::new()));
        if let DigestState::Pending { sender, task } = digest {
            drop(sender);
            task.abort();
        }
    }
}

impl CommitTracker {
    fn new(lanes: usize, quorum: usize, metrics: Arc<Metrics>) -> Arc<Self> {
        let snapshot = CommitSnapshot::default();
        let (updates, _) = watch::channel(snapshot);
        Arc::new(Self {
            quorum,
            state: Mutex::new(CommitState {
                boundaries: VecDeque::new(),
                admitted: 0,
                committed: 0,
                committed_bytes: 0,
                failure: None,
                lanes: (0..lanes).map(|_| LaneCommitState::default()).collect(),
            }),
            updates,
            metrics,
        })
    }

    fn activate_lane(&self, zone: usize, durable: i64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let lane = &mut state.lanes[zone];
        let previous_lag = lane_durable_lag(lane);
        lane.durable = durable.max(0);
        lane.finished = false;
        self.metrics
            .adjust_zone_durable_lag(zone, lane_durable_lag(lane) - previous_lag);
    }

    fn admitted_len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .admitted
    }

    fn committed_len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .committed
    }

    fn committed_bytes(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .committed_bytes
    }

    fn is_poisoned(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .failure
            .is_some()
    }

    fn subscribe(&self) -> watch::Receiver<CommitSnapshot> {
        self.updates.subscribe()
    }

    fn admit_window(&self, boundaries: &[i64], represented_zones: &[usize]) -> CommitRange {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let first_offset = state.admitted;
        let end = boundaries
            .last()
            .copied()
            .expect("an admitted window is non-empty");
        state.admitted += boundaries.len();
        state.boundaries.extend(boundaries.iter().copied());
        for &zone in represented_zones {
            let lane = &mut state.lanes[zone];
            let previous_lag = lane_durable_lag(lane);
            lane.represented_end = lane.represented_end.max(end);
            self.metrics
                .adjust_zone_durable_lag(zone, lane_durable_lag(lane) - previous_lag);
        }
        self.recompute_locked(&mut state);
        CommitRange {
            first_offset,
            end_offset: state.admitted,
            updates: self.subscribe(),
        }
    }

    fn publish_durable(&self, zone: usize, durable: i64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let lane = &mut state.lanes[zone];
        if durable <= lane.durable {
            return;
        }
        let previous_lag = lane_durable_lag(lane);
        lane.durable = durable;
        self.metrics
            .adjust_zone_durable_lag(zone, lane_durable_lag(lane) - previous_lag);
        self.recompute_locked(&mut state);
    }

    fn finish_lane(&self, zone: usize, error: Option<TransportError>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fence = error
            .as_ref()
            .filter(|error| error.code.fences_writer())
            .map(ToString::to_string);
        let lane = &mut state.lanes[zone];
        let previous_lag = lane_durable_lag(lane);
        lane.finished = true;
        if error.is_some() {
            lane.error = error;
        }
        self.metrics
            .adjust_zone_durable_lag(zone, lane_durable_lag(lane) - previous_lag);
        if let Some(fence) = fence {
            // A takeover fence is writer-wide, not a removable lane failure.
            // Publish it even when the just-confirmed boundary drained the queue.
            if state.failure.is_none() {
                state.failure = Some(CommitFailure::Fenced(fence));
                self.publish_locked(&state);
            }
            return;
        }
        self.recompute_locked(&mut state);
    }

    fn poison(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.failure.is_none() {
            state.failure = Some(CommitFailure::Poisoned);
            self.publish_locked(&state);
        }
    }

    fn recompute_locked(&self, state: &mut CommitState) {
        if state.failure.is_some() {
            return;
        }
        let quorum_watermark = quorum_durable_watermark(&state.lanes, self.quorum);
        let mut changed = false;
        while state
            .boundaries
            .front()
            .is_some_and(|boundary| *boundary <= quorum_watermark)
        {
            let boundary = state
                .boundaries
                .pop_front()
                .expect("front boundary was present");
            state.committed += 1;
            state.committed_bytes =
                usize::try_from(boundary).expect("record boundaries are nonnegative");
            changed = true;
        }

        if let Some(&oldest) = state.boundaries.front() {
            let possible = state
                .lanes
                .iter()
                .filter(|lane| {
                    lane.durable >= oldest || (!lane.finished && lane.represented_end >= oldest)
                })
                .count();
            if possible < self.quorum {
                state.failure = Some(select_commit_failure(&state.lanes, oldest));
                changed = true;
            }
        }
        if changed {
            self.publish_locked(state);
        }
    }

    fn publish_locked(&self, state: &CommitState) {
        self.updates.send_replace(CommitSnapshot {
            committed: state.committed,
            failure: state.failure.clone(),
        });
    }
}

impl Drop for CommitTracker {
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (zone, lane) in state.lanes.iter_mut().enumerate() {
            let lag = lane_durable_lag(lane);
            if lag != 0 {
                self.metrics.adjust_zone_durable_lag(zone, -lag);
                lane.finished = true;
            }
        }
    }
}

fn lane_durable_lag(lane: &LaneCommitState) -> i64 {
    if lane.finished {
        0
    } else {
        lane.represented_end.saturating_sub(lane.durable).max(0)
    }
}

fn quorum_durable_watermark(lanes: &[LaneCommitState], quorum: usize) -> i64 {
    let mut durables: Vec<_> = lanes.iter().map(|lane| lane.durable).collect();
    durables.sort_unstable();
    durables[durables.len() - quorum]
}

fn select_commit_failure(lanes: &[LaneCommitState], boundary: i64) -> CommitFailure {
    let represented = lanes.iter().filter(|lane| lane.represented_end >= boundary);
    let mut fenced = None;
    let mut terminal = None;
    for error in represented.filter_map(|lane| lane.error.clone()) {
        if error.code.fences_writer() {
            prefer_lower_zone(&mut fenced, error);
        } else if !error.code.transient() {
            prefer_lower_zone(&mut terminal, error);
        }
    }
    match (fenced, terminal) {
        (Some(error), _) => CommitFailure::Fenced(error.to_string()),
        (None, Some(error)) => CommitFailure::Transport(error),
        (None, None) => CommitFailure::Poisoned,
    }
}

impl CommitRange {
    pub(crate) fn first_offset(&self) -> usize {
        self.first_offset
    }

    pub(crate) fn end_offset(&self) -> usize {
        self.end_offset
    }

    #[cfg(test)]
    pub(crate) fn into_pending(self) -> Vec<PendingCommit> {
        (self.first_offset..self.end_offset)
            .map(|logical_offset| PendingCommit {
                logical_offset: logical_offset as u64,
                updates: self.updates.clone(),
            })
            .collect()
    }

    pub(crate) fn progress(&mut self) -> (usize, Option<ProtocolError>) {
        let snapshot = self.updates.borrow_and_update().clone();
        (
            snapshot.committed,
            snapshot.failure.map(|failure| failure.protocol_error()),
        )
    }

    pub(crate) async fn changed(&mut self) -> Result<(), ProtocolError> {
        self.updates
            .changed()
            .await
            .map_err(|_| ProtocolError::PipelineClosed)
    }
}

pub(crate) fn digest_bytes(bytes: &[u8]) -> String {
    digest_hex(Sha256::digest(bytes))
}

fn digest_hex(digest: impl AsRef<[u8]>) -> String {
    use std::fmt::Write;

    let digest = digest.as_ref();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

impl QuorumVolume {
    /// A volume whose every created or replaced object carries `metadata` —
    /// the constant format marker. Rewrites (canonical write-back, repair)
    /// replace custom metadata wholesale with the same constant; chain
    /// position lives in the manifest's segment directory, never on the
    /// object.
    pub fn with_metadata(
        replicas: Vec<Arc<dyn Replica>>,
        config: ClientConfig,
        metadata: HashMap<String, String>,
        metrics: Arc<Metrics>,
    ) -> Result<Self, ProtocolError> {
        if !SUPPORTED_REPLICA_COUNTS.contains(&replicas.len()) {
            return Err(ProtocolError::ReplicaCount);
        }
        Ok(Self {
            replicas,
            config,
            metadata,
            metrics,
        })
    }

    fn quorum(&self) -> usize {
        majority(self.replicas.len())
    }

    /// Conditionally create one new segment generation on a quorum.
    ///
    /// Only objects created by this call count toward writer eligibility. An
    /// `AlreadyExists` object never lets a racing process adopt that segment.
    pub async fn create_writer(&self) -> Result<Writer, ProtocolError> {
        let metadata = self.metadata.clone();
        let creates = join_all(
            self.replicas
                .iter()
                .map(|replica| create_session_with_retry(replica, metadata.clone(), &self.config)),
        )
        .await;
        let tokens: Vec<_> = creates.into_iter().flatten().collect();
        if tokens.len() < self.quorum() {
            return Err(ProtocolError::NoQuorum);
        }
        Ok(Writer::new(
            self.replicas.clone(),
            self.config.clone(),
            self.metadata.clone(),
            tokens,
            Arc::clone(&self.metrics),
        ))
    }

    /// Fence an existing segment, select a compatible recovery quorum, and
    /// rewrite its canonical prefix to the same witnesses. The returned tail
    /// contains the canonical bytes and the taken-over replica sessions needed
    /// for seal enforcement; application appends start in a new segment.
    pub async fn recover_for_seal(
        &self,
        expected_records: Option<usize>,
    ) -> Result<RecoveredTail, ProtocolError> {
        match self.recover_candidate(expected_records).await? {
            RecoveryCandidate::NonEmpty(tail) => Ok(tail),
            RecoveryCandidate::Empty { .. } | RecoveryCandidate::Absent => {
                Err(ProtocolError::RecoveryPrefixTooShort {
                    expected: expected_records.unwrap_or(1),
                    actual: 0,
                })
            }
        }
    }

    /// Fence before observing size, then recover the committed record prefix.
    ///
    /// `takeover_current` resolves the latest object generation explicitly,
    /// then opens that exact generation to revoke any stale stream and obtain
    /// authoritative `persisted_size`. The identity stat is tail-blind and
    /// never participates in prefix selection. Object bytes are read only after
    /// non-empty witnesses are frozen, because they are not readable while
    /// appendable.
    pub(crate) async fn recover_candidate(
        &self,
        expected_records: Option<usize>,
    ) -> Result<RecoveryCandidate, ProtocolError> {
        enum Observation {
            Live(AppendToken),
            Finalized(ReplicaSnapshot),
            Missing(usize),
        }

        let attempts = join_all(self.replicas.iter().map(|replica| {
            let replica = Arc::clone(replica);
            let config = self.config.clone();
            async move {
                match takeover_current_with_retry(&replica, &config).await {
                    Ok(token) => Ok::<_, ProtocolError>(Observation::Live(token)),
                    Err(error) if error.code == TransportCode::FailedPrecondition => {
                        let snapshot = snapshot_with_retry(&replica, &config).await?;
                        if valid_format(&snapshot.metadata) {
                            Ok(Observation::Finalized(snapshot))
                        } else {
                            Err(ProtocolError::NoQuorum)
                        }
                    }
                    Err(error) if error.code == TransportCode::NotFound => {
                        Ok(Observation::Missing(error.zone))
                    }
                    Err(error)
                        if error.code.transient()
                            || matches!(
                                error.code,
                                TransportCode::DataLoss | TransportCode::Ambiguous
                            ) =>
                    {
                        Err(ProtocolError::NoQuorum)
                    }
                    Err(error) => Err(error.into()),
                }
            }
        }))
        .await;

        let mut observations = Vec::new();
        for attempt in attempts {
            match attempt {
                Ok(observation) => observations.push(observation),
                Err(ProtocolError::NoQuorum) => {}
                Err(error) => return Err(error),
            }
        }
        if observations.len() < self.quorum() {
            return Err(ProtocolError::NoQuorum);
        }
        if observations
            .iter()
            .all(|observation| matches!(observation, Observation::Missing(_)))
        {
            return Ok(RecoveryCandidate::Absent);
        }

        let missing_zones: Vec<_> = observations
            .iter()
            .filter_map(|observation| match observation {
                Observation::Missing(zone) => Some(*zone),
                _ => None,
            })
            .collect();

        let mut sizes = observations
            .iter()
            .map(|observation| match observation {
                Observation::Live(token) => token.persisted_size,
                Observation::Finalized(snapshot) => snapshot.persisted_size,
                Observation::Missing(_) => 0,
            })
            .collect::<Vec<_>>();
        if sizes.len() < self.quorum() {
            return Err(ProtocolError::NoQuorum);
        }
        let max_observed_size = *sizes
            .iter()
            .max()
            .expect("a quorum supplied at least one size");
        // An acknowledged write quorum can hide at most N-k of its witnesses
        // among the unavailable replicas. Therefore a potentially acknowledged
        // offset must appear on at least Q-(N-k) of the k available sizes. This
        // is the Qth-largest size when all replicas answer, the maximum when
        // only a read quorum answers, and the corresponding intermediate order
        // statistic for larger replica sets. Failed reads can remove evidence,
        // but can never lower the selected offset below a quorum that may have
        // acknowledged it.
        let committed_size = select_recovery_size(&mut sizes, self.replicas.len())
            .expect("a reachable majority intersects every write quorum");

        if committed_size == 0 {
            let mut empty_witnesses = observations
                .iter()
                .filter(|observation| {
                    matches!(
                        observation,
                        Observation::Live(token) if token.persisted_size == 0
                    ) || matches!(
                        observation,
                        Observation::Finalized(snapshot) if snapshot.persisted_size == 0
                    )
                })
                .count();
            let mut tokens = observations
                .into_iter()
                .filter_map(|observation| match observation {
                    Observation::Live(token) if token.persisted_size == 0 => Some(token),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if tokens.len() < self.quorum() {
                let creates = join_all(missing_zones.into_iter().map(|zone| {
                    create_session_with_retry(
                        &self.replicas[zone],
                        self.metadata.clone(),
                        &self.config,
                    )
                }))
                .await;
                for token in creates.into_iter().flatten() {
                    empty_witnesses += 1;
                    tokens.push(token);
                }
            }
            if empty_witnesses < self.quorum() {
                return Err(ProtocolError::NoQuorum);
            }
            let reusable_writer = (tokens.len() == self.replicas.len()).then(|| {
                Box::new(Writer::new(
                    self.replicas.clone(),
                    self.config.clone(),
                    self.metadata.clone(),
                    tokens,
                    Arc::clone(&self.metrics),
                ))
            });
            return Ok(RecoveryCandidate::Empty { reusable_writer });
        }

        let mut live_tokens = Vec::new();
        let mut recovery_snapshots = Vec::new();
        for observation in observations {
            match observation {
                Observation::Live(token) => live_tokens.push(token),
                Observation::Finalized(snapshot) => recovery_snapshots.push(snapshot),
                Observation::Missing(_) => {}
            }
        }
        let frozen = join_all(live_tokens.into_iter().map(|mut token| {
            let replica = Arc::clone(&self.replicas[token.zone]);
            let config = self.config.clone();
            async move {
                let persisted_size = token.persisted_size;
                finalize_with_retry(&replica, &mut token, persisted_size, &config).await
            }
        }))
        .await;
        let frozen_zones = frozen
            .into_iter()
            .flatten()
            .map(|snapshot| snapshot.zone)
            .collect::<Vec<_>>();
        if frozen_zones.len() + recovery_snapshots.len() + missing_zones.len() < self.quorum() {
            return Err(ProtocolError::NoQuorum);
        }

        let committed_size = usize::try_from(committed_size).map_err(|_| {
            ProtocolError::InvalidManifest("persisted segment size does not fit in usize".into())
        })?;
        let max_observed_size = usize::try_from(max_observed_size).map_err(|_| {
            ProtocolError::InvalidManifest("persisted segment size does not fit in usize".into())
        })?;
        let reads = join_all(
            frozen_zones
                .into_iter()
                .map(|zone| snapshot_with_retry(&self.replicas[zone], &self.config)),
        )
        .await;
        for snapshot in reads.into_iter().flatten() {
            if valid_format(&snapshot.metadata) {
                recovery_snapshots.push(snapshot);
            }
        }
        for snapshot in &mut recovery_snapshots {
            snapshot
                .bytes
                .truncate(snapshot.bytes.len().min(committed_size));
        }
        recovery_snapshots.extend(missing_zones.into_iter().map(|zone| ReplicaSnapshot {
            zone,
            generation: 0,
            metageneration: 0,
            persisted_size: 0,
            finalized: true,
            crc32c: Some(crc32c::crc32c(&[])),
            metadata: self.metadata.clone(),
            bytes: Vec::new(),
        }));
        let (mut canonical, _) = select_canonical_quorum(&recovery_snapshots, self.quorum())?;
        let had_discarded_suffix = max_observed_size > canonical.bytes.len();
        if let Some(expected) = expected_records {
            if canonical.len() < expected {
                return Err(ProtocolError::RecoveryPrefixTooShort {
                    expected,
                    actual: canonical.len(),
                });
            }
            canonical.truncate(expected);
        }
        if canonical.len() == 0 {
            return Ok(RecoveryCandidate::Empty {
                reusable_writer: None,
            });
        }
        Ok(RecoveryCandidate::NonEmpty(RecoveredTail {
            canonical,
            had_discarded_suffix,
        }))
    }

    /// Install and finalize the canonical bytes on at least a quorum of
    /// replicas.
    ///
    /// Enforcement only: the seal decision must already be committed (through
    /// the manifest for the active segment, or implied by the following
    /// segment's base for chain repair). Replicas are processed with the
    /// shortest prefix first so the canonical bytes always remain durable in
    /// at least one object throughout the rewrite; the operation is
    /// idempotent and convergent under crashes and races because the bytes
    /// are fixed by the committed decision.
    pub async fn enforce_seal(&self, canonical: &CanonicalPrefix) -> Result<(), ProtocolError> {
        let data = Bytes::from(canonical.bytes.clone());
        let reads = join_all(
            self.replicas
                .iter()
                .map(|replica| snapshot_with_retry(replica, &self.config)),
        )
        .await;
        let mut witnesses: Vec<ReplicaSnapshot> = Vec::new();
        let mut missing: Vec<usize> = Vec::new();
        for (zone, read) in reads.into_iter().enumerate() {
            match read {
                Ok(snapshot) if valid_format(&snapshot.metadata) => witnesses.push(snapshot),
                Ok(_) => {}
                Err(error) if error.code == TransportCode::NotFound => missing.push(zone),
                Err(error) if error.code == TransportCode::DataLoss => {
                    match stat_with_retry(&self.replicas[zone], &self.config).await {
                        Ok(snapshot) if valid_format(&snapshot.metadata) => {
                            // `stat` is deliberately content-blind. Empty bytes
                            // force `enforce_witness` to guarded-replace the
                            // rotted generation from the canonical prefix.
                            witnesses.push(snapshot);
                        }
                        Ok(_) => {}
                        Err(stat_error) if stat_error.code == TransportCode::NotFound => {
                            missing.push(zone);
                        }
                        Err(stat_error) if stat_error.code.transient() => {}
                        Err(stat_error) => return Err(stat_error.into()),
                    }
                }
                Err(error) if error.code.transient() => {}
                Err(error) => return Err(error.into()),
            }
        }
        // shortest matching prefix first: the best copy is rewritten last
        witnesses.sort_by_key(|snapshot| {
            let shared = snapshot
                .bytes
                .iter()
                .zip(&canonical.bytes)
                .take_while(|(a, b)| a == b)
                .count();
            (shared == canonical.bytes.len() && snapshot.bytes.len() == canonical.bytes.len())
                as usize
                * canonical.bytes.len()
                + shared
        });
        let mut finalized = 0usize;
        for snapshot in witnesses {
            match self.enforce_witness(snapshot, &data).await {
                Ok(true) => finalized += 1,
                Ok(false) => {}
                Err(error) => return Err(error),
            }
        }
        for zone in missing {
            let replica = Arc::clone(&self.replicas[zone]);
            let Ok(created) =
                create_with_retry(&replica, self.metadata.clone(), &self.config).await
            else {
                continue;
            };
            match self.enforce_witness(created, &data).await {
                Ok(true) => finalized += 1,
                Ok(false) => {}
                Err(error) => return Err(error),
            }
        }
        if finalized >= self.quorum() {
            Ok(())
        } else {
            Err(ProtocolError::NoQuorum)
        }
    }

    async fn enforce_witness(
        &self,
        mut snapshot: ReplicaSnapshot,
        data: &Bytes,
    ) -> Result<bool, ProtocolError> {
        let zone = snapshot.zone;
        let replica = Arc::clone(&self.replicas[zone]);
        for _ in 0..=self.config.max_retries {
            if snapshot.finalized {
                if snapshot.bytes == data[..] {
                    return Ok(true);
                }
                // wrong finalized content: replace with a fresh generation
            } else if snapshot.bytes == data[..] {
                let Ok(mut token) = takeover_with_retry(&replica, &snapshot, &self.config).await
                else {
                    snapshot = snapshot_with_retry(&replica, &self.config).await?;
                    continue;
                };
                match finalize_with_retry(&replica, &mut token, data.len() as i64, &self.config)
                    .await
                {
                    Ok(_) => return Ok(true),
                    Err(error) if error.code == TransportCode::FailedPrecondition => {
                        snapshot = snapshot_with_retry(&replica, &self.config).await?;
                        continue;
                    }
                    Err(error) if error.code.transient() => return Ok(false),
                    Err(error) => return Err(error.into()),
                }
            }
            let mut token = match replace_with_retry(
                &replica,
                snapshot.clone(),
                data.clone(),
                self.metadata.clone(),
                &self.config,
            )
            .await
            {
                Ok(token) => token,
                Err(error) if error.code == TransportCode::FailedPrecondition => {
                    snapshot = snapshot_with_retry(&replica, &self.config).await?;
                    continue;
                }
                Err(error) if error.code.transient() => return Ok(false),
                Err(error) => return Err(error.into()),
            };
            match finalize_with_retry(&replica, &mut token, data.len() as i64, &self.config).await {
                Ok(_) => return Ok(true),
                Err(error) if error.code == TransportCode::FailedPrecondition => {
                    snapshot = snapshot_with_retry(&replica, &self.config).await?;
                }
                Err(error) if error.code.transient() => return Ok(false),
                Err(error) => return Err(error.into()),
            }
        }
        Ok(false)
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        for lane in self.lanes.iter_mut().filter_map(Option::take) {
            lane.done.abort();
        }
    }
}

impl Writer {
    fn new(
        replicas: Vec<Arc<dyn Replica>>,
        config: ClientConfig,
        metadata: HashMap<String, String>,
        tokens: Vec<AppendToken>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let mut by_zone: HashMap<usize, AppendToken> = tokens
            .into_iter()
            .map(|token| (token.zone, token))
            .collect();
        let commits = CommitTracker::new(
            replicas.len(),
            majority(replicas.len()),
            Arc::clone(&metrics),
        );
        let lanes = (0..replicas.len())
            .map(|zone| {
                by_zone.remove(&zone).map(|token| {
                    commits.activate_lane(zone, token.persisted_size);
                    let (work, rx) = tokio::sync::mpsc::unbounded_channel();
                    let replica = Arc::clone(&replicas[zone]);
                    let config = config.clone();
                    let metrics = Arc::clone(&metrics);
                    let budget = LaneBudget::new();
                    let stall_timeout = LaneStallTimeout::new();
                    let lane_commits = Arc::clone(&commits);
                    LaneHandle {
                        work,
                        done: tokio::spawn(run_lane(
                            replica,
                            token,
                            config,
                            metrics,
                            rx,
                            lane_commits,
                            Arc::clone(&stall_timeout),
                        )),
                        budget,
                        stall_timeout,
                    }
                })
            })
            .collect();
        Self {
            replicas,
            config,
            metadata,
            lanes,
            admitted: AdmittedPrefix::default(),
            commits,
            sealed: false,
            metrics,
        }
    }

    fn quorum(&self) -> usize {
        majority(self.replicas.len())
    }

    pub fn admitted_len(&self) -> usize {
        self.commits.admitted_len()
    }

    pub(crate) fn set_max_replica_lag_bytes(&mut self, limit: usize) {
        for lane in self.lanes.iter().flatten() {
            lane.budget.set_limit(limit);
        }
    }

    pub(crate) fn set_lane_stall_timeout(&mut self, timeout: Duration) {
        for lane in self.lanes.iter().flatten() {
            lane.stall_timeout.set(timeout);
        }
    }

    pub(crate) async fn shutdown_background_tasks(&mut self) {
        let lanes = std::mem::take(&mut self.lanes);
        let mut tasks = Vec::new();
        for lane in lanes.into_iter().flatten() {
            lane.done.abort();
            tasks.push(lane.done);
        }
        let _ = join_all(tasks).await;
        self.admitted.shutdown_digest().await;
        join_all(self.replicas.iter().map(|replica| replica.shutdown())).await;
    }

    pub fn committed_len(&self) -> usize {
        self.commits.committed_len()
    }

    pub fn is_poisoned(&self) -> bool {
        self.commits.is_poisoned()
    }

    pub fn physical_size(&self) -> usize {
        self.commits.committed_bytes()
    }

    /// SHA-256 of every admitted encoded record. Digest work is queued only
    /// after lane dispatch; rotation waits for the ordered worker after
    /// admission freezes, and the background fold waits for the commit
    /// watermark to reach the admitted record count.
    pub async fn seal_digest(&mut self) -> String {
        self.admitted.digest().await
    }

    /// Full-object CRC32C of every admitted encoded record. Admission freezes
    /// before the fold CAS, so this names the same exact byte range as
    /// [`Self::seal_digest`].
    pub fn seal_crc32c(&self) -> u32 {
        self.admitted.crc32c()
    }

    pub async fn enqueue_data_window(
        &mut self,
        records: Vec<RecordFrame>,
        on_attempted: AttemptedBytes,
    ) -> Result<CommitRange, ProtocolError> {
        if records.is_empty() {
            return Ok(CommitRange {
                first_offset: self.admitted_len(),
                end_offset: self.admitted_len(),
                updates: self.commits.subscribe(),
            });
        }
        if self.sealed {
            return Err(ProtocolError::Finalized);
        }
        if self.is_poisoned() {
            return Err(ProtocolError::Poisoned);
        }
        let chunks: Result<Vec<_>, _> = records.iter().map(RecordFrame::encode).collect();
        let chunks: Arc<[Bytes]> = chunks?.into();
        drop(records);
        let quorum = self.quorum();
        let active_lanes = self.lanes.iter().filter(|lane| lane.is_some()).count();
        if active_lanes < quorum {
            tracing::warn!(active_lanes, required_lanes = quorum, "WAL writer poisoned");
            self.commits.poison();
            return Err(ProtocolError::NoQuorum);
        }
        self.metrics.batches_sent.increment();
        let batch_bytes = chunks.iter().map(Bytes::len).sum::<usize>();
        let mut reservations = Vec::with_capacity(self.lanes.len());
        let mut retired_lanes = Vec::new();
        for zone in 0..self.lanes.len() {
            let reservation = self.lanes[zone]
                .as_ref()
                .and_then(|lane| lane.budget.try_reserve(batch_bytes));
            if reservation.is_none() && self.lanes[zone].is_some() {
                let lane = self.lanes[zone].take().expect("lane checked present");
                lane.done.abort();
                retired_lanes.push(lane.done);
                self.commits.finish_lane(zone, None);
                self.metrics.lane_capacity_drops.increment();
                tracing::warn!(
                    zone,
                    batch_bytes,
                    "replica lane exceeded its retained-byte budget"
                );
            }
            reservations.push(reservation);
        }
        let _ = join_all(retired_lanes).await;
        if reservations.iter().flatten().count() < quorum {
            self.commits.poison();
            return Err(ProtocolError::NoQuorum);
        }
        let start = self.admitted.bytes_len() as i64;
        let mut boundaries = Vec::with_capacity(chunks.len());
        let mut acc = start;
        for chunk in chunks.iter() {
            acc += chunk.len() as i64;
            boundaries.push(acc);
        }
        let boundaries: Arc<[i64]> = boundaries.into();
        self.admitted.extend_metadata(&chunks);
        let batch = Arc::new(BatchDescriptor {
            start,
            chunks: Arc::clone(&chunks),
            boundaries,
            on_attempted: Arc::clone(&on_attempted),
            bytes: batch_bytes,
            pending_lanes: AtomicUsize::new(reservations.iter().flatten().count()),
            packed_groups: std::sync::Mutex::new(BTreeMap::new()),
        });
        let mut represented_zones = Vec::with_capacity(self.lanes.len());
        for (zone, reservation) in reservations.iter_mut().enumerate() {
            if let (Some(lane), Some(reservation)) = (&self.lanes[zone], reservation.take()) {
                let lane_batch = LaneBatch::new(Arc::clone(&batch), reservation);
                if let Err(error) = lane.work.send(lane_batch) {
                    drop(error.0);
                    // the lane task already terminated; its death notice
                    // reached earlier batches, and this batch simply never
                    // gains this zone's support
                    let lane = self.lanes[zone]
                        .take()
                        .expect("failed lane send still owns its task handle");
                    let _ = lane.done.await;
                    self.commits.finish_lane(zone, None);
                } else {
                    represented_zones.push(zone);
                }
            }
        }
        // SHA-256 is ordered behind every preceding batch but starts only
        // after this batch is visible to all live lanes. Rotation is the sole
        // consumer that waits for the digest pipeline.
        self.admitted.queue_digest(chunks);
        Ok(self
            .commits
            .admit_window(&batch.boundaries, &represented_zones))
    }

    /// Finalize the rotated segment at exactly the committed bytes.
    ///
    /// Enforcement only: the caller has already committed the rotation view
    /// (advanced `tail_base` with the canonical digest) through the manifest
    /// quorum. The fast path finalizes this writer's own lanes; if fewer than
    /// a quorum survive, the fallback reconstructs the decided prefix from
    /// storage, verifies its digest, and installs it through
    /// [`QuorumVolume::enforce_seal`].
    ///
    /// This method is one-shot even though the fallback operation is
    /// idempotent: sealing takes and closes the live lane set before it can
    /// fail. A caller that needs retries must reconstruct from storage through
    /// the committed range and digest rather than calling `seal` again.
    pub async fn seal(&mut self) -> Result<SealReport, ProtocolError> {
        self.sealed = true;
        let total = self.admitted.len();
        if self.admitted.is_empty() || self.is_poisoned() {
            self.shutdown_background_tasks().await;
            return Err(ProtocolError::Poisoned);
        }
        let expected_digest = self.seal_digest().await;
        let expected_crc32c = self.seal_crc32c();
        let quorum = self.quorum();
        // Close every lane's work channel: each task flushes its in-flight
        // acknowledgments and yields the token at its durable tail. The abort
        // guard stops stragglers on cancellation or early return; the normal
        // exit below drains every lane join handle before returning.
        let lanes = std::mem::take(&mut self.lanes);
        let aborts = AbortLanesOnDrop(
            lanes
                .iter()
                .flatten()
                .map(|lane| lane.done.abort_handle())
                .collect(),
        );
        let mut draining: FuturesUnordered<_> = lanes
            .into_iter()
            .enumerate()
            .filter_map(|(zone, lane)| {
                lane.map(|lane| {
                    drop(lane.work);
                    let done = lane.done;
                    async move { done.await.ok().flatten().map(|token| (zone, token)) }
                })
            })
            .collect();
        let result = async {
            // Settle against the same segment-wide watermark used by normal
            // completions. Poison wins even if the durable watermark later moves:
            // sealing may not acknowledge across an indeterminate gap.
            let mut settling = self.commits.subscribe();
            loop {
                let snapshot = settling.borrow_and_update().clone();
                if snapshot.failure.is_some() {
                    return Err(ProtocolError::Poisoned);
                }
                if snapshot.committed >= total {
                    break;
                }
                settling
                    .changed()
                    .await
                    .map_err(|_| ProtocolError::PipelineClosed)?;
            }
            // The watermark above proves quorum durability for every admitted
            // record, but a per-record quorum may be stitched from different
            // zone pairs: finalization needs lanes whose own copy holds every
            // byte. Wait for a quorum of full drains (or for every lane to
            // settle), give stragglers one bounded grace, then abandon them to
            // targeted repair — a lagging lane may legally retain tens of MiB
            // of unacknowledged backlog, and the seal must not wait out a
            // drain the committed boundary does not need. An abandoned copy is
            // a prefix of the canonical bytes, which recovery and repair
            // already tolerate.
            let mut drained = Vec::new();
            while drained.len() < quorum {
                match draining.next().await {
                    Some(Some((zone, token))) => drained.push((zone, token)),
                    Some(None) => {}
                    None => break,
                }
            }
            if !draining.is_empty() {
                let grace = tokio::time::sleep(LANE_DRAIN_GRACE);
                tokio::pin!(grace);
                loop {
                    tokio::select! {
                        biased;
                        next = draining.next() => match next {
                            Some(Some((zone, token))) => drained.push((zone, token)),
                            Some(None) => {}
                            None => break,
                        },
                        () = &mut grace => break,
                    }
                }
            }
            let write_offset = self.physical_size() as i64;
            let finalizations = join_all(drained.into_iter().map(|(zone, mut token)| {
                let replica = Arc::clone(&self.replicas[zone]);
                let config = self.config.clone();
                async move {
                    finalize_with_retry(&replica, &mut token, write_offset, &config)
                        .await
                        .map(|snapshot| (zone, snapshot))
                }
            }))
            .await;
            let mut finalized = vec![None; self.replicas.len()];
            for result in finalizations {
                match result {
                    Ok((zone, snapshot)) => {
                        finalized[zone] = Some(snapshot);
                    }
                    Err(error) => {
                        tracing::warn!(
                            zone = error.zone,
                            code = ?error.code,
                            %error,
                            write_offset,
                            "live segment finalization failed"
                        );
                    }
                }
            }
            if finalized.iter().flatten().count() >= self.quorum() {
                return Ok(SealReport { finalized });
            }
            let volume = QuorumVolume::with_metadata(
                self.replicas.clone(),
                self.config.clone(),
                self.metadata.clone(),
                Arc::clone(&self.metrics),
            )?;
            let recovered = volume.recover_for_seal(Some(total)).await?;
            let actual_digest = recovered.digest();
            if actual_digest != expected_digest {
                return Err(ProtocolError::SealDigestMismatch {
                    expected: expected_digest,
                    actual: actual_digest,
                });
            }
            let actual_crc32c = recovered.crc32c();
            if actual_crc32c != expected_crc32c {
                return Err(ProtocolError::SealCrc32cMismatch {
                    expected: expected_crc32c,
                    actual: actual_crc32c,
                });
            }
            volume.enforce_seal(recovered.canonical()).await?;
            // Enforcement guarantees a sealed quorum, but does not promise that
            // every replica was reachable. Conservatively request targeted repair.
            Ok(SealReport::default())
        }
        .await;
        aborts.abort();
        while draining.next().await.is_some() {}
        self.shutdown_background_tasks().await;
        result
    }

    /// Poison this writer so no later completion can be acknowledged.
    #[cfg(test)]
    pub fn poison(&self) {
        self.commits.poison();
    }
}

fn prefer_lower_zone(current: &mut Option<TransportError>, candidate: TransportError) {
    if current
        .as_ref()
        .is_none_or(|existing| candidate.zone < existing.zone)
    {
        *current = Some(candidate);
    }
}

async fn snapshot_with_retry(
    replica: &Arc<dyn Replica>,
    config: &ClientConfig,
) -> Result<ReplicaSnapshot, TransportError> {
    let mut attempt = 0usize;
    loop {
        match replica.snapshot().await {
            Ok(snapshot) => return Ok(snapshot),
            Err(error) if error.code.transient() && attempt < config.max_retries => {
                retry_sleep(config, attempt).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn stat_with_retry(
    replica: &Arc<dyn Replica>,
    config: &ClientConfig,
) -> Result<ReplicaSnapshot, TransportError> {
    let mut attempt = 0usize;
    loop {
        match replica.stat().await {
            Ok(snapshot) => return Ok(snapshot),
            Err(error) if error.code.transient() && attempt < config.max_retries => {
                retry_sleep(config, attempt).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn create_with_retry(
    replica: &Arc<dyn Replica>,
    metadata: HashMap<String, String>,
    config: &ClientConfig,
) -> Result<ReplicaSnapshot, TransportError> {
    let mut attempt = 0usize;
    loop {
        match replica.create_appendable(metadata.clone()).await {
            Ok(snapshot) => return Ok(snapshot),
            Err(error) if error.code.transient() && attempt < config.max_retries => {
                retry_sleep(config, attempt).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn create_session_with_retry(
    replica: &Arc<dyn Replica>,
    metadata: HashMap<String, String>,
    config: &ClientConfig,
) -> Result<AppendToken, TransportError> {
    let mut attempt = 0usize;
    loop {
        match replica.create_append_session(metadata.clone()).await {
            Ok(token) => return Ok(token),
            Err(error) if error.code.transient() && attempt < config.max_retries => {
                retry_sleep(config, attempt).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn takeover_with_retry(
    replica: &Arc<dyn Replica>,
    observed: &ReplicaSnapshot,
    config: &ClientConfig,
) -> Result<AppendToken, TransportError> {
    let mut attempt = 0usize;
    loop {
        match replica.takeover(observed).await {
            Ok(token) => return Ok(token),
            Err(error) if error.code.transient() && attempt < config.max_retries => {
                retry_sleep(config, attempt).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn takeover_current_with_retry(
    replica: &Arc<dyn Replica>,
    config: &ClientConfig,
) -> Result<AppendToken, TransportError> {
    let mut attempt = 0usize;
    loop {
        match replica.takeover_current().await {
            Ok(token) => return Ok(token),
            Err(error) if error.code.transient() && attempt < config.max_retries => {
                retry_sleep(config, attempt).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn replace_with_retry(
    replica: &Arc<dyn Replica>,
    mut observed: ReplicaSnapshot,
    data: Bytes,
    metadata: HashMap<String, String>,
    config: &ClientConfig,
) -> Result<AppendToken, TransportError> {
    let mut attempt = 0usize;
    loop {
        match replica
            .replace_appendable(&observed, data.clone(), metadata.clone())
            .await
        {
            Ok(token) => return Ok(token),
            Err(error) if error.code.transient() && attempt < config.max_retries => {
                retry_sleep(config, attempt).await;
                attempt += 1;
                observed = snapshot_with_retry(replica, config).await?;
                if observed.bytes == data[..]
                    && observed.metadata == metadata
                    && !observed.finalized
                {
                    return Ok(AppendToken {
                        zone: observed.zone,
                        generation: Some(observed.generation),
                        metageneration: Some(observed.metageneration),
                        persisted_size: data.len() as i64,
                        write_handle: None,
                    });
                }
            }
            Err(error) => return Err(error),
        }
    }
}

/// Immutable data shared by every lane representing one admitted window.
struct BatchDescriptor {
    start: i64,
    chunks: Arc<[Bytes]>,
    boundaries: Arc<[i64]>,
    on_attempted: AttemptedBytes,
    bytes: usize,
    pending_lanes: AtomicUsize,
    packed_groups: Mutex<BTreeMap<i64, Arc<OnceLock<Arc<PackedAppend>>>>>,
}

impl BatchDescriptor {
    fn end(&self) -> i64 {
        self.boundaries
            .last()
            .copied()
            .expect("an admitted batch is non-empty")
    }

    fn lane_staged(&self) {
        let previous = self.pending_lanes.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "batch lane count underflow");
        if previous == 1 {
            self.packed_groups
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clear();
        }
    }
}

/// Lane-local ownership for one shared batch descriptor.
struct LaneBatch {
    batch: Arc<BatchDescriptor>,
    reservation: Option<Arc<LaneReservation>>,
    staged: bool,
}

impl LaneBatch {
    fn new(batch: Arc<BatchDescriptor>, reservation: Arc<LaneReservation>) -> Self {
        Self {
            batch,
            reservation: Some(reservation),
            staged: false,
        }
    }

    fn into_retained(mut self) -> RetainedBatch {
        let retained = RetainedBatch {
            batch: Arc::clone(&self.batch),
            next_chunk: 0,
            _reservation: self
                .reservation
                .take()
                .expect("an unstaged lane batch owns its reservation"),
        };
        self.staged = true;
        self.batch.lane_staged();
        retained
    }
}

impl Drop for LaneBatch {
    fn drop(&mut self) {
        if !self.staged {
            self.batch.lane_staged();
        }
    }
}

struct RetainedBatch {
    batch: Arc<BatchDescriptor>,
    next_chunk: usize,
    _reservation: Arc<LaneReservation>,
}

fn packed_group(batches: &[LaneBatch]) -> Arc<PackedAppend> {
    let first = &batches.first().expect("coalesced group is non-empty").batch;
    let end = batches
        .last()
        .expect("coalesced group is non-empty")
        .batch
        .end();
    let cell = {
        let mut groups = first
            .packed_groups
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(
            groups
                .entry(end)
                .or_insert_with(|| Arc::new(OnceLock::new())),
        )
    };
    Arc::clone(cell.get_or_init(|| {
        let chunks = batches
            .iter()
            .flat_map(|batch| batch.batch.chunks.iter().cloned())
            .collect();
        Arc::new(pack_append(chunks))
    }))
}

/// How long a seal waits for straggler lane drains once a quorum of lanes
/// has fully drained and the commit watermark has settled. In the healthy
/// case the last lane finishes within this window and gets finalized; a
/// deeply backlogged laggard is cut and left to targeted repair.
const LANE_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_millis(100);

/// Aborts the lane tasks a seal took ownership of when the seal exits by
/// any path. A finished task ignores the abort; an abandoned straggler must
/// stop appending so the next generation owns the object alone.
struct AbortLanesOnDrop(Vec<tokio::task::AbortHandle>);

impl AbortLanesOnDrop {
    fn abort(&self) {
        for lane in &self.0 {
            lane.abort();
        }
    }
}

impl Drop for AbortLanesOnDrop {
    fn drop(&mut self) {
        self.abort();
    }
}

/// The per-lane writer receives batches in offset order, snapshots and drains
/// everything currently queued, and sends the resulting group with a flush on
/// its final data message without waiting for earlier flush acknowledgments.
/// Durable-tail movement publishes one monotonic byte watermark. On a session
/// disturbance it resumes via the session handle and resends the retained
/// unacknowledged suffix; a fence or exhausted retries publishes one terminal
/// lane outcome. A lane that makes no durable progress for its configured
/// timeout is retired without recovery: changing sessions cannot prove that a
/// completely stationary durable tail is healthy, and the commit tracker must
/// promptly decide whether the remaining lanes still form a true quorum.
#[derive(Debug)]
enum LaneDeath {
    Stalled,
    Transport(TransportError),
}

impl LaneDeath {
    fn stalled(metrics: &Metrics) -> Self {
        metrics.lane_timeouts.increment();
        Self::Stalled
    }
}

struct LaneRuntime {
    replica: Arc<dyn Replica>,
    token: AppendToken,
    config: ClientConfig,
    metrics: Arc<Metrics>,
    commits: Arc<CommitTracker>,
    stall_timeout: Arc<LaneStallTimeout>,
    durable: i64,
    retained: VecDeque<RetainedBatch>,
    attempted: Option<AttemptedBytes>,
    monitor_session: bool,
    last_progress: tokio::time::Instant,
}

impl LaneRuntime {
    fn new(
        replica: Arc<dyn Replica>,
        token: AppendToken,
        config: ClientConfig,
        metrics: Arc<Metrics>,
        commits: Arc<CommitTracker>,
        stall_timeout: Arc<LaneStallTimeout>,
    ) -> Self {
        let durable = token.persisted_size;
        Self {
            replica,
            token,
            config,
            metrics,
            commits,
            stall_timeout,
            durable,
            retained: VecDeque::new(),
            attempted: None,
            // Keep observing an idle live stream so a trailing fence cannot
            // disappear merely because its persisted-size response drained
            // the retained suffix.
            monitor_session: true,
            last_progress: tokio::time::Instant::now(),
        }
    }

    fn zone(&self) -> usize {
        self.token.zone
    }

    fn stall_deadline(&self) -> tokio::time::Instant {
        self.last_progress + self.stall_timeout.get()
    }

    fn publish_advance(&mut self, change: LaneDurableChange) -> Result<bool, LaneDeath> {
        publish_lane_advance(
            change,
            self.zone(),
            &mut self.durable,
            &mut self.last_progress,
            &mut self.retained,
            &self.commits,
        )
    }

    /// Resolve an elapsed lane deadline against the durable-tail stream.
    ///
    /// A timeout is not itself proof of failure: the final observation may
    /// publish progress or a stream error. Return whether the caller still
    /// needs session recovery after applying that observation.
    async fn confirm_timeout(&mut self) -> Result<bool, LaneDeath> {
        let change = confirm_lane_stall(
            &self.replica,
            self.durable,
            self.stall_deadline(),
            &self.metrics,
        )
        .await?;
        let stream_failed = self.publish_advance(change)?;
        Ok(stream_failed || !self.retained.is_empty())
    }

    async fn stage(&mut self, batches: Vec<LaneBatch>) -> Result<bool, LaneDeath> {
        match tokio::time::timeout_at(
            self.stall_deadline(),
            stage_group(
                &self.replica,
                batches,
                &mut self.attempted,
                &mut self.retained,
            ),
        )
        .await
        {
            Ok(failed) => Ok(failed),
            Err(_) => self.confirm_timeout().await,
        }
    }
}

async fn run_lane(
    replica: Arc<dyn Replica>,
    token: AppendToken,
    config: ClientConfig,
    metrics: Arc<Metrics>,
    mut work: tokio::sync::mpsc::UnboundedReceiver<LaneBatch>,
    commits: Arc<CommitTracker>,
    stall_timeout: Arc<LaneStallTimeout>,
) -> Option<AppendToken> {
    let mut lane = LaneRuntime::new(replica, token, config, metrics, commits, stall_timeout);
    let mut closed = false;
    let death: Option<LaneDeath> = loop {
        if closed && lane.retained.is_empty() {
            break None;
        }
        tokio::select! {
            biased;
            // Progress must win ties with queued work. The writer reserves a
            // lane's retained bytes before dispatch; if a ready durable-tail
            // update sits behind an always-ready work queue, a healthy lane
            // looks stalled and is falsely dropped at its byte budget. `biased`
            // keeps DST deterministic, while the work arm still snapshots and
            // drains the ready queue so sustained producers cannot postpone a
            // flush indefinitely.
            changed = lane_progress(
                &lane.replica,
                lane.durable,
                !lane.retained.is_empty(),
                lane.stall_deadline(),
                &lane.metrics,
            ), if lane.monitor_session || !lane.retained.is_empty() => {
                match changed {
                    LaneProgress::Advanced(change) => {
                        match lane.publish_advance(change) {
                            Ok(false) => lane.monitor_session = true,
                            Ok(true) => {
                                if let Err(error) = lane.recover().await {
                                    break Some(error);
                                }
                                lane.monitor_session = true;
                            }
                            Err(error) => break Some(error),
                        }
                    }
                    LaneProgress::Stalled => {
                        break Some(LaneDeath::Stalled);
                    }
                    LaneProgress::Failed(error) if !error.code.transient() => {
                        break Some(LaneDeath::Transport(error));
                    }
                    LaneProgress::Failed(_) => {
                        lane.monitor_session = false;
                        if lane.retained.is_empty() {
                            continue;
                        }
                        if let Err(error) = lane.recover().await {
                            break Some(error);
                        }
                        lane.monitor_session = true;
                    }
                }
            }
            batch = work.recv(), if !closed => match batch {
                Some(batch) => {
                    // Snapshot and drain everything already queued, then flush
                    // on the final data message. Work arriving after the
                    // snapshot forms the next group, so sustained producers
                    // cannot postpone this flush indefinitely.
                    let mut batches = vec![batch];
                    let queued = work.len();
                    for _ in 0..queued {
                        match work.try_recv() {
                            Ok(batch) => batches.push(batch),
                            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                                closed = true;
                                break;
                            }
                        }
                    }
                    if lane.retained.is_empty() {
                        lane.last_progress = tokio::time::Instant::now();
                    }
                    let failed = match lane.stage(batches).await {
                        Ok(failed) => failed,
                        Err(error) => break Some(error),
                    };
                    if failed {
                        if let Err(error) = lane.recover().await {
                            break Some(error);
                        }
                    }
                    lane.monitor_session = true;
                }
                None => closed = true,
            },
        }
    };
    match death {
        None => {
            lane.commits.finish_lane(lane.zone(), None);
            lane.token.persisted_size = lane.durable;
            Some(lane.token)
        }
        Some(LaneDeath::Stalled) => {
            tracing::warn!(
                zone = lane.zone(),
                durable_offset = lane.durable,
                retained_chunks = retained_chunk_count(&lane.retained),
                stall_timeout_ms = lane.stall_timeout.get().as_millis(),
                "append lane shed after making no durable progress"
            );
            lane.commits.finish_lane(lane.zone(), None);
            lane.retained.clear();
            work.close();
            while work.try_recv().is_ok() {}
            None
        }
        Some(LaneDeath::Transport(error)) => {
            tracing::warn!(
                zone = error.zone,
                code = ?error.code,
                error = %error,
                durable_offset = lane.durable,
                retained_chunks = retained_chunk_count(&lane.retained),
                "append lane died"
            );
            tracing::debug!(
                zone = error.zone,
                code = ?error.code,
                message = error.message.as_str(),
                durable_offset = lane.durable,
                retained_chunks = retained_chunk_count(&lane.retained),
                "append lane dropped"
            );
            lane.commits.finish_lane(lane.zone(), Some(error));
            lane.retained.clear();
            work.close();
            while work.try_recv().is_ok() {}
            None
        }
    }
}

/// Stage one coalesced group onto the session: record every batch's chunks for
/// acknowledgment matching and resend, then send all group chunks together so
/// the transport flushes on the final data message. Returns whether the send
/// failed so the caller can run lane recovery, which also flushes its resend.
async fn stage_group(
    replica: &Arc<dyn Replica>,
    batches: Vec<LaneBatch>,
    attempted: &mut Option<AttemptedBytes>,
    retained: &mut VecDeque<RetainedBatch>,
) -> bool {
    let group_start = batches
        .first()
        .expect("coalesced group is non-empty")
        .batch
        .start;
    let packed = packed_group(&batches);
    for batch in batches {
        (batch.batch.on_attempted)(batch.batch.bytes as u64);
        *attempted = Some(Arc::clone(&batch.batch.on_attempted));
        retained.push_back(batch.into_retained());
    }
    replica
        .lane_send_packed(group_start, &packed)
        .await
        .is_err()
}

/// Advance each retained batch's byte cursor and release fully durable batches.
/// `partition_point` avoids per-record notification work when one persisted
/// offset covers a large coalesced group.
fn ack_through(durable: i64, retained: &mut VecDeque<RetainedBatch>) {
    while let Some(batch) = retained.front_mut() {
        batch.next_chunk = batch
            .batch
            .boundaries
            .partition_point(|boundary| *boundary <= durable)
            .max(batch.next_chunk);
        if batch.next_chunk == batch.batch.chunks.len() {
            retained.pop_front();
        } else {
            break;
        }
    }
}

fn retained_chunk_count(retained: &VecDeque<RetainedBatch>) -> usize {
    retained
        .iter()
        .map(|batch| batch.batch.chunks.len() - batch.next_chunk)
        .sum()
}

/// Wait for the durable tail to move past `seen` or for the live session to
/// fail. Idle sessions have no stall deadline, while retained writes must make
/// durable progress before their configured deadline.
enum LaneProgress {
    Advanced(LaneDurableChange),
    Stalled,
    Failed(TransportError),
}

async fn lane_progress(
    replica: &Arc<dyn Replica>,
    seen: i64,
    active: bool,
    stall_deadline: tokio::time::Instant,
    metrics: &Metrics,
) -> LaneProgress {
    loop {
        if !active {
            match replica.lane_durable_change(seen).await {
                Ok(change) if change.persisted_size > seen => {
                    return LaneProgress::Advanced(change);
                }
                Ok(_) => tokio::task::yield_now().await,
                Err(error) => return LaneProgress::Failed(error),
            }
            continue;
        }
        tokio::select! {
            biased;
            result = replica.lane_durable_change(seen) => match result {
                Ok(change) if change.persisted_size > seen => {
                    return LaneProgress::Advanced(change);
                }
                Ok(_) if tokio::time::Instant::now() < stall_deadline => {
                    tokio::task::yield_now().await;
                }
                Ok(_) => {
                    let _ = LaneDeath::stalled(metrics);
                    return LaneProgress::Stalled;
                }
                Err(error) => return LaneProgress::Failed(error),
            },
            _ = tokio::time::sleep_until(stall_deadline) => {
                let _ = LaneDeath::stalled(metrics);
                return LaneProgress::Stalled;
            }
        }
    }
}

fn publish_lane_progress(
    tail: i64,
    zone: usize,
    durable: &mut i64,
    last_progress: &mut tokio::time::Instant,
    retained: &mut VecDeque<RetainedBatch>,
    commits: &CommitTracker,
) {
    let previous = *durable;
    *durable = (*durable).max(tail);
    if *durable > previous {
        *last_progress = tokio::time::Instant::now();
    }
    ack_through(*durable, retained);
    commits.publish_durable(zone, *durable);
}

/// Publish physical progress first, then report whether a transient stream
/// failure needs recovery or a terminal error must stop the lane.
fn publish_lane_advance(
    change: LaneDurableChange,
    zone: usize,
    durable: &mut i64,
    last_progress: &mut tokio::time::Instant,
    retained: &mut VecDeque<RetainedBatch>,
    commits: &CommitTracker,
) -> Result<bool, LaneDeath> {
    publish_lane_progress(
        change.persisted_size,
        zone,
        durable,
        last_progress,
        retained,
        commits,
    );
    match change.error {
        None => Ok(false),
        Some(error) if error.code.transient() => Ok(true),
        Some(error) => Err(LaneDeath::Transport(error)),
    }
}

async fn confirm_lane_stall(
    replica: &Arc<dyn Replica>,
    seen: i64,
    stall_deadline: tokio::time::Instant,
    metrics: &Metrics,
) -> Result<LaneDurableChange, LaneDeath> {
    match lane_progress(replica, seen, true, stall_deadline, metrics).await {
        LaneProgress::Advanced(change) => Ok(change),
        LaneProgress::Stalled => Err(LaneDeath::Stalled),
        LaneProgress::Failed(error) if error.code.transient() => Err(LaneDeath::stalled(metrics)),
        LaneProgress::Failed(error) => Err(LaneDeath::Transport(error)),
    }
}

impl LaneRuntime {
    /// Re-learn the durable tail through a session resume and resend the
    /// retained unacknowledged suffix, slicing a partially durable chunk at
    /// the durable boundary. This never reads object bytes: one
    /// generation-guarded stream wrote every byte at these offsets, so the
    /// offsets identify our data.
    async fn recover(&mut self) -> Result<(), LaneDeath> {
        let mut attempt = 0usize;
        loop {
            self.metrics.lane_retries.increment();
            let deadline = self.stall_deadline();
            let resumed =
                tokio::time::timeout_at(deadline, self.replica.resume_tail(&mut self.token)).await;
            let resumed = match resumed {
                Ok(resumed) => resumed,
                Err(_) => {
                    if self.confirm_timeout().await? {
                        continue;
                    }
                    return Ok(());
                }
            };
            match resumed {
                Ok(tail) => {
                    publish_lane_progress(
                        tail,
                        self.zone(),
                        &mut self.durable,
                        &mut self.last_progress,
                        &mut self.retained,
                        &self.commits,
                    );
                    let mut suffix = Vec::with_capacity(retained_chunk_count(&self.retained));
                    for retained_batch in &self.retained {
                        for index in retained_batch.next_chunk..retained_batch.batch.chunks.len() {
                            let chunk = &retained_batch.batch.chunks[index];
                            if suffix.is_empty() {
                                let offset = index
                                    .checked_sub(1)
                                    .and_then(|previous| {
                                        retained_batch.batch.boundaries.get(previous).copied()
                                    })
                                    .unwrap_or(retained_batch.batch.start);
                                let skip = usize::try_from((self.durable - offset).max(0))
                                    .unwrap_or(chunk.len());
                                if skip >= chunk.len() {
                                    continue;
                                }
                                suffix.push(chunk.slice(skip..));
                            } else {
                                suffix.push(chunk.clone());
                            }
                        }
                    }
                    if suffix.is_empty() {
                        return Ok(());
                    }
                    let packed = pack_append(suffix);
                    let resend_bytes = packed.len();
                    tracing::debug!(
                        zone = self.zone(),
                        durable_offset = self.durable,
                        chunks = packed.chunks().len(),
                        bytes = resend_bytes,
                        attempt,
                        "resending append lane batch after recovery"
                    );
                    if let Some(attempted) = &self.attempted {
                        attempted(resend_bytes as u64);
                    }
                    let sent = tokio::time::timeout_at(
                        self.stall_deadline(),
                        self.replica.lane_send_packed(self.durable, &packed),
                    )
                    .await;
                    let sent = match sent {
                        Ok(sent) => sent,
                        Err(_) => {
                            if self.confirm_timeout().await? {
                                continue;
                            }
                            return Ok(());
                        }
                    };
                    match sent {
                        Ok(()) => return Ok(()),
                        Err(error)
                            if error.code.transient() && attempt < self.config.max_retries =>
                        {
                            if tokio::time::timeout_at(
                                self.stall_deadline(),
                                retry_sleep(&self.config, attempt),
                            )
                            .await
                            .is_err()
                            {
                                if self.confirm_timeout().await? {
                                    continue;
                                }
                                return Ok(());
                            }
                            attempt += 1;
                        }
                        Err(error) => return Err(LaneDeath::Transport(error)),
                    }
                }
                Err(error) if error.code.transient() && attempt < self.config.max_retries => {
                    if tokio::time::timeout_at(
                        self.stall_deadline(),
                        retry_sleep(&self.config, attempt),
                    )
                    .await
                    .is_err()
                    {
                        if self.confirm_timeout().await? {
                            continue;
                        }
                        return Ok(());
                    }
                    attempt += 1;
                }
                Err(error) => return Err(LaneDeath::Transport(error)),
            }
        }
    }
}

async fn finalize_with_retry(
    replica: &Arc<dyn Replica>,
    token: &mut AppendToken,
    write_offset: i64,
    config: &ClientConfig,
) -> Result<ReplicaSnapshot, TransportError> {
    let mut attempt = 0usize;
    loop {
        match replica.finalize(token, write_offset).await {
            Ok(snapshot) => return Ok(snapshot),
            Err(error)
                if error.code == TransportCode::FailedPrecondition || error.code.transient() =>
            {
                // A retained create stream has no generation identity until a
                // successful handle resume proves it. If its finish response
                // is ambiguous, canonical seal enforcement must resolve it.
                let Some(generation) = token.generation else {
                    return Err(error);
                };
                if error.code.transient() {
                    if attempt >= config.max_retries {
                        return Err(error);
                    }
                    retry_sleep(config, attempt).await;
                    attempt += 1;
                }
                // A finalized object's size is authoritative, so an
                // ambiguous finish response needs metadata only. Open-object
                // metrics remain tail-blind and therefore cannot falsely prove
                // that finalization landed.
                let snapshot = stat_with_retry(replica, config).await?;
                if snapshot.finalized
                    && snapshot.generation == generation
                    && snapshot.persisted_size == write_offset
                {
                    return Ok(snapshot);
                }
                if error.code == TransportCode::FailedPrecondition {
                    return Err(error);
                }
            }
            Err(error) => return Err(error),
        }
    }
}

pub(crate) fn retry_delay(config: &ClientConfig, attempt: usize) -> Duration {
    let multiplier = 1u32.checked_shl(attempt.min(16) as u32).unwrap_or(u32::MAX);
    config.retry_base.saturating_mul(multiplier)
}

pub(crate) async fn retry_sleep(config: &ClientConfig, attempt: usize) {
    tokio::time::sleep(retry_delay(config, attempt)).await;
}

/// Select the longest exact, mutually consistent well-formed prefix visible
/// on a quorum of recovery witnesses.
///
/// A candidate is any quorum-sized subset of the readable snapshots whose
/// well-formed prefixes agree pairwise on their overlap; its prefix is the
/// longest member's. The witness set must be quorum-sized because a committed
/// record is durable on a write quorum, and only a quorum-sized read subset
/// is guaranteed to intersect it — a smaller consistent set could miss
/// committed records entirely. A record visible on only one selected witness
/// is retained and then written back to every witness before sealing; this
/// promotion is required when only a quorum of zones is readable, since an
/// unavailable zone may hold another copy of a committed record. A divergent
/// minority lane is excluded by quorum agreement; different bytes among
/// equally long maximal candidates remain ambiguous and fail recovery.
pub(crate) fn canonical_prefix(
    snapshots: &[ReplicaSnapshot],
    quorum: usize,
) -> Result<Vec<RecordFrame>, ProtocolError> {
    select_canonical_quorum(snapshots, quorum).map(|(prefix, _)| prefix.into_records())
}

/// Ascending index combinations of size `quorum` out of `count` snapshots,
/// in lexicographic order (`count` is at most 5).
fn quorum_subsets(count: usize, quorum: usize) -> Vec<Vec<usize>> {
    fn extend(
        start: usize,
        count: usize,
        quorum: usize,
        current: &mut Vec<usize>,
        subsets: &mut Vec<Vec<usize>>,
    ) {
        if current.len() == quorum {
            subsets.push(current.clone());
            return;
        }
        for index in start..count {
            current.push(index);
            extend(index + 1, count, quorum, current, subsets);
            current.pop();
        }
    }
    let mut subsets = Vec::new();
    extend(
        0,
        count,
        quorum,
        &mut Vec::with_capacity(quorum),
        &mut subsets,
    );
    subsets
}

fn select_canonical_quorum(
    snapshots: &[ReplicaSnapshot],
    quorum: usize,
) -> Result<(CanonicalPrefix, Vec<ReplicaSnapshot>), ProtocolError> {
    if snapshots.len() < quorum {
        return Err(ProtocolError::NoQuorum);
    }
    let decoded: Vec<_> = snapshots
        .iter()
        .map(CanonicalPrefix::from_snapshot)
        .collect();
    let mut conflicts = vec![false; decoded.len() * decoded.len()];
    let mut first_conflict = None;
    for left in 0..decoded.len() {
        for right in left + 1..decoded.len() {
            let overlap = decoded[left].len().min(decoded[right].len());
            let conflict = (0..overlap).find(|index| {
                decoded[left].record_bytes(*index) != decoded[right].record_bytes(*index)
            });
            if let Some(index) = conflict {
                first_conflict.get_or_insert(index);
                conflicts[left * decoded.len() + right] = true;
            }
        }
    }
    let mut candidates = Vec::new();
    for subset in quorum_subsets(decoded.len(), quorum) {
        let consistent = subset.iter().enumerate().all(|(position, &left)| {
            subset[position + 1..]
                .iter()
                .all(|&right| !conflicts[left * decoded.len() + right])
        });
        if !consistent {
            continue;
        }
        let mut longest_member = subset[0];
        for &member in &subset[1..] {
            if decoded[member].len() > decoded[longest_member].len() {
                longest_member = member;
            }
        }
        candidates.push((
            decoded[longest_member].clone(),
            subset
                .iter()
                .map(|&member| snapshots[member].clone())
                .collect::<Vec<_>>(),
        ));
    }
    let longest = candidates
        .iter()
        .map(|(candidate, _)| candidate.len())
        .max()
        .ok_or(ProtocolError::ConflictingPrefix {
            record_index: first_conflict.unwrap_or(0),
        })?;
    let mut longest_candidates = candidates
        .into_iter()
        .filter(|(candidate, _)| candidate.len() == longest);
    let first = longest_candidates
        .next()
        .expect("longest length came from a candidate");
    let mut equivalent = vec![first];
    for candidate in longest_candidates {
        if candidate.0.bytes != equivalent[0].0.bytes {
            let record_index = (0..candidate.0.len())
                .find(|index| {
                    candidate.0.record_bytes(*index) != equivalent[0].0.record_bytes(*index)
                })
                .unwrap_or(0);
            return Err(ProtocolError::ConflictingPrefix { record_index });
        }
        equivalent.push(candidate);
    }
    Ok(equivalent
        .into_iter()
        .max_by(|(_, left_witnesses), (_, right_witnesses)| {
            let left_zones: Vec<_> = left_witnesses.iter().map(|copy| copy.zone).collect();
            let right_zones: Vec<_> = right_witnesses.iter().map(|copy| copy.zone).collect();
            right_zones.cmp(&left_zones)
        })
        .expect("at least one equivalent candidate remains"))
}

pub(crate) fn protocol_metadata() -> HashMap<String, String> {
    HashMap::from([(META_FORMAT.to_string(), FORMAT_VERSION.to_string())])
}

pub(crate) fn valid_format(metadata: &HashMap<String, String>) -> bool {
    metadata.get(META_FORMAT).map(String::as_str) == Some(FORMAT_VERSION)
}

#[cfg(test)]
fn encode_records(records: &[RecordFrame]) -> Result<Vec<u8>, RecordError> {
    let encoded: Result<Vec<_>, _> = records.iter().map(RecordFrame::encode).collect();
    Ok(encoded?.into_iter().flatten().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, VecDeque};
    use std::sync::Arc;

    use async_trait::async_trait;
    use tokio::sync::{mpsc, oneshot, watch, Mutex};

    use crate::metrics::{test_support::TestMetricsRecorder, Metrics};

    fn snapshot(zone: usize, records: &[RecordFrame]) -> ReplicaSnapshot {
        let bytes = encode_records(records).unwrap();
        ReplicaSnapshot {
            zone,
            generation: 1,
            metageneration: 1,
            persisted_size: 0,
            finalized: false,
            crc32c: Some(crc32c::crc32c(&bytes)),
            metadata: protocol_metadata(),
            bytes,
        }
    }

    fn record(value: &[u8]) -> RecordFrame {
        RecordFrame {
            payload: Bytes::copy_from_slice(value),
        }
    }

    fn batch_descriptor(
        start: i64,
        chunks: Vec<Bytes>,
        pending_lanes: usize,
    ) -> Arc<BatchDescriptor> {
        let mut end = start;
        let mut boundaries = Vec::with_capacity(chunks.len());
        for chunk in &chunks {
            end += chunk.len() as i64;
            boundaries.push(end);
        }
        let bytes = chunks.iter().map(Bytes::len).sum();
        Arc::new(BatchDescriptor {
            start,
            chunks: chunks.into(),
            boundaries: boundaries.into(),
            on_attempted: Arc::new(|_| {}),
            bytes,
            pending_lanes: AtomicUsize::new(pending_lanes),
            packed_groups: std::sync::Mutex::new(BTreeMap::new()),
        })
    }

    struct ScriptedLaneReplica {
        zone: usize,
        durable: watch::Sender<i64>,
        send_releases: Mutex<VecDeque<oneshot::Receiver<()>>>,
        sends: mpsc::UnboundedSender<i64>,
    }

    impl ScriptedLaneReplica {
        fn new(
            zone: usize,
            send_releases: VecDeque<oneshot::Receiver<()>>,
            sends: mpsc::UnboundedSender<i64>,
        ) -> Self {
            let (durable, _) = watch::channel(0);
            Self {
                zone,
                durable,
                send_releases: Mutex::new(send_releases),
                sends,
            }
        }

        fn error(&self, code: TransportCode, message: &str) -> TransportError {
            TransportError {
                zone: self.zone,
                code,
                message: message.into(),
            }
        }
    }

    struct ReaderTerminalReplica {
        zone: usize,
        reader_failed: watch::Sender<bool>,
        resume_calls: AtomicUsize,
    }

    struct StalledReplica {
        zone: usize,
    }

    impl ReaderTerminalReplica {
        fn new(zone: usize) -> Self {
            let (reader_failed, _) = watch::channel(false);
            Self {
                zone,
                reader_failed,
                resume_calls: AtomicUsize::new(0),
            }
        }

        fn error(&self) -> TransportError {
            TransportError {
                zone: self.zone,
                code: TransportCode::PermissionDenied,
                message: "async reader rejected append".into(),
            }
        }

        fn resume_calls(&self) -> usize {
            self.resume_calls.load(Ordering::SeqCst)
        }
    }

    struct SingleReplicaFactory {
        replica: Arc<dyn Replica>,
    }

    impl SingleReplicaFactory {
        fn new(replica: Arc<dyn Replica>) -> Self {
            Self { replica }
        }
    }

    #[async_trait]
    impl Replica for ScriptedLaneReplica {
        async fn snapshot(&self) -> Result<ReplicaSnapshot, TransportError> {
            panic!("snapshot is not used in this test")
        }

        async fn stat(&self) -> Result<ReplicaSnapshot, TransportError> {
            panic!("stat is not used in this test")
        }

        async fn create_appendable(
            &self,
            _metadata: HashMap<String, String>,
        ) -> Result<ReplicaSnapshot, TransportError> {
            panic!("create_appendable is not used in this test")
        }

        async fn create_append_session(
            &self,
            _metadata: HashMap<String, String>,
        ) -> Result<AppendToken, TransportError> {
            panic!("create_append_session is not used in this test")
        }

        async fn create_register(
            &self,
            _metadata: HashMap<String, String>,
        ) -> Result<ReplicaSnapshot, TransportError> {
            panic!("create_register is not used in this test")
        }

        async fn update_register(
            &self,
            _metageneration: i64,
            _metadata: HashMap<String, String>,
        ) -> Result<ReplicaSnapshot, TransportError> {
            panic!("update_register is not used in this test")
        }

        async fn resume_tail(&self, _token: &mut AppendToken) -> Result<i64, TransportError> {
            Err(self.error(
                TransportCode::Internal,
                "resume_tail should not run in this test",
            ))
        }

        async fn takeover(
            &self,
            _observed: &ReplicaSnapshot,
        ) -> Result<AppendToken, TransportError> {
            panic!("takeover is not used in this test")
        }

        async fn replace_appendable(
            &self,
            _observed: &ReplicaSnapshot,
            _data: Bytes,
            _metadata: HashMap<String, String>,
        ) -> Result<AppendToken, TransportError> {
            panic!("replace_appendable is not used in this test")
        }

        async fn append(
            &self,
            _token: &AppendToken,
            _write_offset: i64,
            _data: Vec<u8>,
        ) -> Result<i64, TransportError> {
            panic!("append is not used in this test")
        }

        async fn lane_send(
            &self,
            write_offset: i64,
            chunks: &[Bytes],
        ) -> Result<(), TransportError> {
            let end = write_offset + chunks.iter().map(|chunk| chunk.len() as i64).sum::<i64>();
            self.durable.send_replace(end);
            let _ = self.sends.send(end);
            let release = self.send_releases.lock().await.pop_front();
            if let Some(release) = release {
                let _ = release.await;
            }
            Ok(())
        }

        async fn lane_durable_change(
            &self,
            seen: i64,
        ) -> Result<LaneDurableChange, TransportError> {
            let mut durable = self.durable.subscribe();
            loop {
                let current = *durable.borrow_and_update();
                if current > seen {
                    return Ok(LaneDurableChange {
                        persisted_size: current,
                        error: None,
                    });
                }
                durable
                    .changed()
                    .await
                    .map_err(|_| self.error(TransportCode::Unavailable, "durable watch closed"))?;
            }
        }

        async fn delete(&self, _generation: i64) -> Result<(), TransportError> {
            panic!("delete is not used in this test")
        }

        async fn finalize(
            &self,
            _token: &mut AppendToken,
            _write_offset: i64,
        ) -> Result<ReplicaSnapshot, TransportError> {
            panic!("finalize is not used in this test")
        }
    }

    #[async_trait]
    impl Replica for ReaderTerminalReplica {
        async fn snapshot(&self) -> Result<ReplicaSnapshot, TransportError> {
            panic!("snapshot is not used in this test")
        }

        async fn stat(&self) -> Result<ReplicaSnapshot, TransportError> {
            panic!("stat is not used in this test")
        }

        async fn create_appendable(
            &self,
            _metadata: HashMap<String, String>,
        ) -> Result<ReplicaSnapshot, TransportError> {
            panic!("create_appendable is not used in this test")
        }

        async fn create_append_session(
            &self,
            _metadata: HashMap<String, String>,
        ) -> Result<AppendToken, TransportError> {
            Ok(AppendToken {
                zone: self.zone,
                generation: Some(1),
                metageneration: Some(1),
                persisted_size: 0,
                write_handle: None,
            })
        }

        async fn create_register(
            &self,
            _metadata: HashMap<String, String>,
        ) -> Result<ReplicaSnapshot, TransportError> {
            panic!("create_register is not used in this test")
        }

        async fn update_register(
            &self,
            _metageneration: i64,
            _metadata: HashMap<String, String>,
        ) -> Result<ReplicaSnapshot, TransportError> {
            panic!("update_register is not used in this test")
        }

        async fn resume_tail(&self, _token: &mut AppendToken) -> Result<i64, TransportError> {
            self.resume_calls.fetch_add(1, Ordering::SeqCst);
            Err(TransportError {
                zone: self.zone,
                code: TransportCode::Unavailable,
                message: "recovery must not run after a terminal reader error".into(),
            })
        }

        async fn takeover(
            &self,
            _observed: &ReplicaSnapshot,
        ) -> Result<AppendToken, TransportError> {
            panic!("takeover is not used in this test")
        }

        async fn replace_appendable(
            &self,
            _observed: &ReplicaSnapshot,
            _data: Bytes,
            _metadata: HashMap<String, String>,
        ) -> Result<AppendToken, TransportError> {
            panic!("replace_appendable is not used in this test")
        }

        async fn append(
            &self,
            _token: &AppendToken,
            _write_offset: i64,
            _data: Vec<u8>,
        ) -> Result<i64, TransportError> {
            panic!("append is not used in this test")
        }

        async fn lane_send(
            &self,
            _write_offset: i64,
            _chunks: &[Bytes],
        ) -> Result<(), TransportError> {
            self.reader_failed.send_replace(true);
            Ok(())
        }

        async fn lane_durable_change(
            &self,
            _seen: i64,
        ) -> Result<LaneDurableChange, TransportError> {
            let mut reader_failed = self.reader_failed.subscribe();
            loop {
                if *reader_failed.borrow_and_update() {
                    return Err(self.error());
                }
                reader_failed.changed().await.map_err(|_| TransportError {
                    zone: self.zone,
                    code: TransportCode::Unavailable,
                    message: "reader failure watch closed".into(),
                })?;
            }
        }

        async fn delete(&self, _generation: i64) -> Result<(), TransportError> {
            panic!("delete is not used in this test")
        }

        async fn finalize(
            &self,
            _token: &mut AppendToken,
            _write_offset: i64,
        ) -> Result<ReplicaSnapshot, TransportError> {
            panic!("finalize is not used in this test")
        }
    }

    #[async_trait]
    impl Replica for StalledReplica {
        async fn snapshot(&self) -> Result<ReplicaSnapshot, TransportError> {
            panic!("snapshot is not used in this test")
        }

        async fn stat(&self) -> Result<ReplicaSnapshot, TransportError> {
            panic!("stat is not used in this test")
        }

        async fn create_appendable(
            &self,
            _metadata: HashMap<String, String>,
        ) -> Result<ReplicaSnapshot, TransportError> {
            panic!("create_appendable is not used in this test")
        }

        async fn create_append_session(
            &self,
            _metadata: HashMap<String, String>,
        ) -> Result<AppendToken, TransportError> {
            Ok(AppendToken {
                zone: self.zone,
                generation: Some(1),
                metageneration: Some(1),
                persisted_size: 0,
                write_handle: None,
            })
        }

        async fn create_register(
            &self,
            _metadata: HashMap<String, String>,
        ) -> Result<ReplicaSnapshot, TransportError> {
            panic!("create_register is not used in this test")
        }

        async fn update_register(
            &self,
            _metageneration: i64,
            _metadata: HashMap<String, String>,
        ) -> Result<ReplicaSnapshot, TransportError> {
            panic!("update_register is not used in this test")
        }

        async fn resume_tail(&self, _token: &mut AppendToken) -> Result<i64, TransportError> {
            panic!("a no-progress timeout must shed instead of recovering the lane")
        }

        async fn takeover(
            &self,
            _observed: &ReplicaSnapshot,
        ) -> Result<AppendToken, TransportError> {
            panic!("takeover is not used in this test")
        }

        async fn replace_appendable(
            &self,
            _observed: &ReplicaSnapshot,
            _data: Bytes,
            _metadata: HashMap<String, String>,
        ) -> Result<AppendToken, TransportError> {
            panic!("replace_appendable is not used in this test")
        }

        async fn append(
            &self,
            _token: &AppendToken,
            _write_offset: i64,
            _data: Vec<u8>,
        ) -> Result<i64, TransportError> {
            panic!("append is not used in this test")
        }

        async fn lane_send(
            &self,
            _write_offset: i64,
            _chunks: &[Bytes],
        ) -> Result<(), TransportError> {
            Ok(())
        }

        async fn lane_durable_change(
            &self,
            _seen: i64,
        ) -> Result<LaneDurableChange, TransportError> {
            std::future::pending().await
        }

        async fn delete(&self, _generation: i64) -> Result<(), TransportError> {
            panic!("delete is not used in this test")
        }

        async fn finalize(
            &self,
            _token: &mut AppendToken,
            _write_offset: i64,
        ) -> Result<ReplicaSnapshot, TransportError> {
            panic!("finalize is not used in this test")
        }
    }

    #[async_trait]
    impl crate::transport::ReplicaFactory for SingleReplicaFactory {
        fn bucket_name(&self) -> &str {
            "single-replica"
        }

        fn replica(&self, _object: &str) -> Arc<dyn Replica> {
            self.replica.clone()
        }

        async fn list(
            &self,
            _prefix: &str,
        ) -> Result<Vec<crate::transport::ListedObject>, TransportError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn majority_matches_supported_widths() {
        assert_eq!(majority(1), 1);
        assert_eq!(majority(3), 2);
        assert_eq!(majority(5), 3);
        let lanes = |durables: &[i64]| {
            durables
                .iter()
                .map(|durable| LaneCommitState {
                    durable: *durable,
                    ..LaneCommitState::default()
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(quorum_durable_watermark(&lanes(&[7]), majority(1)), 7);
        assert_eq!(quorum_durable_watermark(&lanes(&[1, 9, 5]), majority(3)), 5);
        assert_eq!(
            quorum_durable_watermark(&lanes(&[1, 9, 5, 7, 3]), majority(5)),
            5
        );
    }

    #[tokio::test]
    async fn admitted_prefix_hashes_framed_bytes_in_order_off_path() {
        let first = record(b"first").encode().unwrap();
        let second = record(b"second").encode().unwrap();
        let third = record(b"third").encode().unwrap();
        let expected = [first.as_ref(), second.as_ref(), third.as_ref()].concat();

        let mut admitted = AdmittedPrefix::default();
        let first_batch: Arc<[Bytes]> = vec![first].into();
        admitted.extend_metadata(&first_batch);
        admitted.queue_digest(first_batch);
        let second_batch: Arc<[Bytes]> = vec![second, third].into();
        admitted.extend_metadata(&second_batch);
        admitted.queue_digest(second_batch);

        assert_eq!(admitted.len(), 3);
        assert_eq!(admitted.bytes_len(), expected.len());
        assert_eq!(admitted.digest().await, digest_bytes(&expected));
        assert_eq!(admitted.crc32c(), crc32c::crc32c(&expected));
    }

    #[test]
    fn matching_lane_groups_share_one_packed_wire_payload() {
        let first = batch_descriptor(0, vec![Bytes::from_static(b"first")], 2);
        let second = batch_descriptor(first.end(), vec![Bytes::from_static(b"second")], 2);
        let first_budget = LaneBudget::new();
        let second_budget = LaneBudget::new();
        let group_one = vec![
            LaneBatch::new(
                Arc::clone(&first),
                first_budget.try_reserve(first.bytes).unwrap(),
            ),
            LaneBatch::new(
                Arc::clone(&second),
                first_budget.try_reserve(second.bytes).unwrap(),
            ),
        ];
        let group_two = vec![
            LaneBatch::new(
                Arc::clone(&first),
                second_budget.try_reserve(first.bytes).unwrap(),
            ),
            LaneBatch::new(
                Arc::clone(&second),
                second_budget.try_reserve(second.bytes).unwrap(),
            ),
        ];

        let packed_one = packed_group(&group_one);
        let packed_two = packed_group(&group_two);

        assert!(Arc::ptr_eq(&packed_one, &packed_two));
        assert_eq!(
            packed_one.chunks(),
            &[Bytes::from_static(b"first"), Bytes::from_static(b"second")]
        );
        drop(group_one);
        drop(group_two);
        assert!(first
            .packed_groups
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty());
    }

    #[test]
    fn packed_wire_payload_preserves_offsets_bytes_and_checksums() {
        let first = Bytes::from(vec![7; 262_143]);
        let second = Bytes::from_static(b"xy");
        let packed = pack_append(vec![first.clone(), second.clone()]);
        let messages = packed.messages();

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].relative_offset, 0);
        assert_eq!(messages[0].content, first);
        assert_eq!(messages[0].crc32c, crc32c::crc32c(&messages[0].content));
        assert_eq!(messages[1].relative_offset, 262_143);
        assert_eq!(messages[1].content, second);
        assert_eq!(messages[1].crc32c, crc32c::crc32c(&messages[1].content));
    }

    #[test]
    fn quorum_subsets_enumerate_lexicographically() {
        assert_eq!(quorum_subsets(1, 1), vec![vec![0]]);
        assert_eq!(
            quorum_subsets(3, 2),
            vec![vec![0, 1], vec![0, 2], vec![1, 2]]
        );
        assert_eq!(quorum_subsets(5, 3).len(), 10);
        assert_eq!(quorum_subsets(5, 3)[0], vec![0, 1, 2]);
    }

    #[test]
    fn canonical_promotes_a_tail_visible_on_one_recovery_witness() {
        let first = record(b"first");
        let second = record(b"second");
        let snapshots = vec![
            snapshot(0, &[first.clone(), second.clone()]),
            snapshot(1, std::slice::from_ref(&first)),
        ];
        assert_eq!(
            canonical_prefix(&snapshots, 2).unwrap(),
            vec![first, second]
        );
    }

    #[test]
    fn canonical_rejects_conflicting_bytes_at_the_same_record() {
        let snapshots = vec![snapshot(0, &[record(b"a")]), snapshot(1, &[record(b"b")])];
        assert!(matches!(
            canonical_prefix(&snapshots, 2),
            Err(ProtocolError::ConflictingPrefix { record_index: 0 })
        ));
    }

    #[test]
    fn canonical_ignores_one_conflicting_lane_when_two_exact_copies_agree() {
        let good = record(b"good");
        let snapshots = vec![
            snapshot(0, &[record(b"bad")]),
            snapshot(1, std::slice::from_ref(&good)),
            snapshot(2, std::slice::from_ref(&good)),
        ];
        assert_eq!(canonical_prefix(&snapshots, 2).unwrap(), vec![good]);
    }

    #[test]
    fn canonical_rejects_equal_length_candidates_without_a_quorum_choice() {
        let snapshots = vec![
            snapshot(0, &[]),
            snapshot(1, &[record(b"left")]),
            snapshot(2, &[record(b"right")]),
        ];
        assert!(matches!(
            canonical_prefix(&snapshots, 2),
            Err(ProtocolError::ConflictingPrefix { record_index: 0 })
        ));
    }

    #[test]
    fn canonical_stops_at_a_partial_tail() {
        let first = record(b"first");
        let second = record(b"second");
        let mut damaged = encode_records(&[first.clone(), second]).unwrap();
        damaged.truncate(damaged.len() - 2);
        let snapshots = vec![
            ReplicaSnapshot {
                bytes: damaged,
                ..snapshot(0, &[])
            },
            snapshot(1, std::slice::from_ref(&first)),
        ];
        assert_eq!(canonical_prefix(&snapshots, 2).unwrap(), vec![first]);
    }

    #[test]
    fn canonical_accepts_a_single_replica_witness() {
        let first = record(b"first");
        let snapshots = vec![snapshot(0, std::slice::from_ref(&first))];
        assert_eq!(canonical_prefix(&snapshots, 1).unwrap(), vec![first]);
    }

    #[test]
    fn canonical_five_zone_quorum_requires_three_consistent_witnesses() {
        let good = record(b"good");
        let consistent = vec![
            snapshot(0, std::slice::from_ref(&good)),
            snapshot(1, std::slice::from_ref(&good)),
            snapshot(2, &[record(b"divergent")]),
            snapshot(3, std::slice::from_ref(&good)),
        ];
        assert_eq!(
            canonical_prefix(&consistent, 3).unwrap(),
            vec![good.clone()]
        );

        // two consistent witnesses out of five are not a read quorum: a
        // committed record could live only on the two unreachable zones
        // plus the divergent lane's pre-divergence prefix
        let insufficient = vec![
            snapshot(0, std::slice::from_ref(&good)),
            snapshot(1, std::slice::from_ref(&good)),
            snapshot(2, &[record(b"divergent")]),
        ];
        assert!(matches!(
            canonical_prefix(&insufficient, 3),
            Err(ProtocolError::ConflictingPrefix { .. })
        ));
    }

    #[test]
    fn canonical_five_zone_promotes_the_longest_member_of_the_quorum() {
        let first = record(b"first");
        let second = record(b"second");
        let snapshots = vec![
            snapshot(0, std::slice::from_ref(&first)),
            snapshot(1, &[first.clone(), second.clone()]),
            snapshot(2, &[]),
        ];
        assert_eq!(
            canonical_prefix(&snapshots, 3).unwrap(),
            vec![first, second]
        );
    }

    #[test]
    fn recovery_size_uses_the_available_quorum_intersection_rank() {
        let mut all_three = [10, 20, 20];
        assert_eq!(select_recovery_size(&mut all_three, 3), Some(20));

        let mut two_of_three = [10, 20];
        assert_eq!(select_recovery_size(&mut two_of_three, 3), Some(20));

        let mut four_of_five = [10, 20, 30, 40];
        assert_eq!(select_recovery_size(&mut four_of_five, 5), Some(30));

        let mut all_five = [10, 20, 30, 40, 50];
        assert_eq!(select_recovery_size(&mut all_five, 5), Some(30));
    }

    fn active_commit_tracker(lanes: usize) -> Arc<CommitTracker> {
        let recorder = TestMetricsRecorder::default();
        let metrics = Arc::new(Metrics::new(&recorder, lanes));
        let tracker = CommitTracker::new(lanes, majority(lanes), metrics);
        for zone in 0..lanes {
            tracker.activate_lane(zone, 0);
        }
        tracker
    }

    #[test]
    fn quorum_byte_watermark_evicts_resolved_record_boundaries() {
        let tracker = active_commit_tracker(3);
        let mut range = tracker.admit_window(&[10, 20, 30], &[0, 1, 2]);

        tracker.publish_durable(0, 30);
        assert_eq!(range.progress().0, 0);
        tracker.publish_durable(1, 20);
        assert_eq!(range.progress().0, 2);
        {
            let state = tracker
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert_eq!(state.committed_bytes, 20);
            assert_eq!(state.boundaries, VecDeque::from([30]));
        }

        tracker.publish_durable(2, 30);
        assert_eq!(range.progress().0, 3);
        assert!(tracker
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .boundaries
            .is_empty());
    }

    #[test]
    fn per_zone_durable_lag_tracks_admitted_and_persisted_bytes() {
        let recorder = Arc::new(TestMetricsRecorder::default());
        let metrics = Arc::new(Metrics::new(recorder.as_ref(), 1));
        let tracker = CommitTracker::new(1, 1, metrics);
        tracker.activate_lane(0, 0);
        let _range = tracker.admit_window(&[10, 20], &[0]);
        assert_eq!(
            recorder.labeled_gauge("chorus.wal.replica.durable_lag_bytes", &[("zone", "0")]),
            20
        );

        tracker.publish_durable(0, 10);
        assert_eq!(
            recorder.labeled_gauge("chorus.wal.replica.durable_lag_bytes", &[("zone", "0")]),
            10
        );
        tracker.finish_lane(0, None);
        assert_eq!(
            recorder.labeled_gauge("chorus.wal.replica.durable_lag_bytes", &[("zone", "0")]),
            0
        );
    }

    #[test]
    fn retired_lanes_cannot_support_future_admissions() {
        let tracker = active_commit_tracker(3);
        tracker.finish_lane(0, None);
        let mut range = tracker.admit_window(&[10], &[1, 2]);

        tracker.finish_lane(1, None);

        let (committed, failure) = range.progress();
        assert_eq!(committed, 0);
        assert!(matches!(failure, Some(ProtocolError::Poisoned)));
    }

    #[tokio::test]
    async fn durable_progress_resets_the_lane_stall_timeout() {
        let recorder = Arc::new(TestMetricsRecorder::default());
        let metrics = Arc::new(Metrics::new(recorder.as_ref(), 1));
        let (sends, _) = mpsc::unbounded_channel();
        let replica = Arc::new(ScriptedLaneReplica::new(0, VecDeque::new(), sends));
        let replica_for_progress: Arc<dyn Replica> = replica.clone();
        let updater = replica.clone();
        let updates = tokio::spawn(async move {
            for durable in [10, 20, 30] {
                tokio::time::sleep(Duration::from_millis(5)).await;
                updater.durable.send_replace(durable);
            }
        });
        let timeout = Duration::from_millis(100);
        let mut seen = 0;

        for expected in [10, 20, 30] {
            match lane_progress(
                &replica_for_progress,
                seen,
                true,
                tokio::time::Instant::now() + timeout,
                &metrics,
            )
            .await
            {
                LaneProgress::Advanced(change) => {
                    assert_eq!(change.persisted_size, expected);
                    assert!(change.error.is_none());
                    seen = change.persisted_size;
                }
                LaneProgress::Stalled => panic!("advancing lane was falsely shed"),
                LaneProgress::Failed(error) => panic!("advancing lane failed: {error}"),
            }
        }
        updates.await.unwrap();
        assert_eq!(recorder.counter("chorus.wal.lane.timeouts"), 0);
    }

    #[tokio::test]
    async fn ready_progress_wins_an_expired_operation_deadline() {
        let recorder = Arc::new(TestMetricsRecorder::default());
        let metrics = Arc::new(Metrics::new(recorder.as_ref(), 1));
        let tracker = CommitTracker::new(1, 1, Arc::clone(&metrics));
        tracker.activate_lane(0, 0);
        let (sends, _) = mpsc::unbounded_channel();
        let replica = Arc::new(ScriptedLaneReplica::new(0, VecDeque::new(), sends));
        replica.durable.send_replace(10);
        let replica: Arc<dyn Replica> = replica;
        let mut durable = 0;
        let mut last_progress = tokio::time::Instant::now() - Duration::from_secs(1);
        let mut retained = VecDeque::new();

        let change = confirm_lane_stall(&replica, durable, tokio::time::Instant::now(), &metrics)
            .await
            .expect("ready durable progress must win the deadline tie");
        publish_lane_advance(
            change,
            0,
            &mut durable,
            &mut last_progress,
            &mut retained,
            &tracker,
        )
        .expect("ready progress has no stream failure");

        assert_eq!(durable, 10);
        assert_eq!(recorder.counter("chorus.wal.lane.timeouts"), 0);
    }

    #[tokio::test]
    async fn fencing_lane_failure_stops_the_writer_after_publishing_progress() {
        let tracker = active_commit_tracker(1);
        let range = tracker.admit_window(&[10], &[0]);

        tracker.publish_durable(0, 10);
        tracker.finish_lane(
            0,
            Some(TransportError {
                zone: 0,
                code: TransportCode::FailedPrecondition,
                message: "newer writer took over".into(),
            }),
        );

        assert_eq!(range.into_pending().remove(0).wait().await.unwrap(), 0);
        assert!(tracker.is_poisoned());
        let mut updates = tracker.subscribe();
        let snapshot = updates.borrow_and_update().clone();
        assert!(matches!(snapshot.failure, Some(CommitFailure::Fenced(_))));
    }

    #[tokio::test]
    async fn pending_commits_follow_the_prefix_watermark_and_gap_poison() {
        let tracker = active_commit_tracker(3);
        let range = tracker.admit_window(&[10, 20], &[0, 1, 2]);
        let mut pending = range.into_pending();
        let second = pending.pop().expect("second pending commit");
        let first = pending.pop().expect("first pending commit");

        tracker.publish_durable(0, 20);
        tracker.publish_durable(1, 10);

        tracker.finish_lane(
            1,
            Some(TransportError {
                zone: 1,
                code: TransportCode::PermissionDenied,
                message: "terminal".into(),
            }),
        );
        tracker.finish_lane(
            2,
            Some(TransportError {
                zone: 2,
                code: TransportCode::Unavailable,
                message: "transient".into(),
            }),
        );
        tracker.publish_durable(2, 20);

        assert_eq!(first.wait().await.unwrap(), 0);
        assert!(matches!(
            second.wait().await,
            Err(ProtocolError::Transport(TransportError {
                zone: 1,
                code: TransportCode::PermissionDenied,
                ..
            }))
        ));
        assert_eq!(tracker.committed_len(), 1);
    }

    #[tokio::test]
    async fn ready_progress_releases_lane_budget_before_more_work_is_staged() {
        let recorder = Arc::new(TestMetricsRecorder::default());
        let metrics = Arc::new(Metrics::new(recorder.as_ref(), 1));
        let (first_send_release_tx, first_send_release_rx) = oneshot::channel();
        let (second_send_release_tx, second_send_release_rx) = oneshot::channel();
        let (sends_tx, mut sends_rx) = mpsc::unbounded_channel();
        let replica: Arc<dyn Replica> = Arc::new(ScriptedLaneReplica::new(
            0,
            VecDeque::from([first_send_release_rx, second_send_release_rx]),
            sends_tx,
        ));
        let mut writer = Writer::new(
            vec![replica],
            ClientConfig::default(),
            protocol_metadata(),
            vec![AppendToken {
                zone: 0,
                generation: Some(1),
                metageneration: Some(1),
                persisted_size: 0,
                write_handle: None,
            }],
            metrics,
        );
        let encoded = record(b"first").encode().unwrap().len();
        writer.set_max_replica_lag_bytes(encoded * 2);
        let attempted: AttemptedBytes = Arc::new(|_| {});

        let first = writer
            .enqueue_data_window(vec![record(b"first")], attempted.clone())
            .await
            .unwrap()
            .into_pending()
            .remove(0);
        assert_eq!(sends_rx.recv().await, Some(encoded as i64));

        let second = writer
            .enqueue_data_window(vec![record(b"other")], attempted.clone())
            .await
            .unwrap()
            .into_pending()
            .remove(0);
        first_send_release_tx
            .send(())
            .expect("the first staged group should still be blocked");
        assert_eq!(sends_rx.recv().await, Some((encoded * 2) as i64));

        let third = writer
            .enqueue_data_window(vec![record(b"third")], attempted)
            .await
            .unwrap()
            .into_pending()
            .remove(0);
        second_send_release_tx
            .send(())
            .expect("the second staged group should still be blocked");

        assert_eq!(first.wait().await.unwrap(), 0);
        assert_eq!(second.wait().await.unwrap(), 1);
        assert_eq!(third.wait().await.unwrap(), 2);
        assert_eq!(writer.committed_len(), 3);
        assert_eq!(recorder.counter("chorus.wal.lane.capacity_drops"), 0);
        assert!(!writer.is_poisoned());
    }

    #[tokio::test]
    async fn terminal_lane_failures_preserve_transport_errors_in_completions() {
        let replica = Arc::new(ReaderTerminalReplica::new(0));
        let factory: Arc<dyn crate::transport::ReplicaFactory> =
            Arc::new(SingleReplicaFactory::new(replica.clone()));
        let manifest_store =
            Arc::new(crate::manifest_store::test_support::InMemoryManifestStore::default());
        let volume = crate::segment::SegmentedVolume::new_with_factories_and_manifest_store(
            vec![factory],
            manifest_store,
            "terminal-completion",
            ClientConfig {
                max_retries: 0,
                retry_base: Duration::ZERO,
            },
        )
        .unwrap();
        let writer = volume.recover_writer().await.unwrap();
        let mut handle = crate::engine::WalEngine::start(
            writer,
            crate::WalEngineConfig {
                repair_interval: None,
                ..Default::default()
            },
        )
        .unwrap();
        let completion = handle
            .enqueue_append(
                crate::segment::WalSeqNo::ZERO,
                Bytes::from_static(b"terminal"),
            )
            .await
            .unwrap();
        let error = completion.await.unwrap_err();
        assert!(matches!(
            error,
            crate::Error::Transport {
                code: TransportCode::PermissionDenied,
                ..
            }
        ));
        assert_eq!(replica.resume_calls(), 0);
        let _ = tokio::time::timeout(Duration::from_secs(1), handle.shutdown())
            .await
            .expect("engine shutdown timed out");
    }

    #[tokio::test]
    async fn stalled_lane_poison_releases_blocked_admission() {
        let replica: Arc<dyn Replica> = Arc::new(StalledReplica { zone: 0 });
        let factory: Arc<dyn crate::transport::ReplicaFactory> =
            Arc::new(SingleReplicaFactory::new(replica));
        let manifest_store =
            Arc::new(crate::manifest_store::test_support::InMemoryManifestStore::default());
        let volume = crate::segment::SegmentedVolume::new_with_factories_and_manifest_store(
            vec![factory],
            manifest_store,
            "stalled-admission",
            ClientConfig {
                max_retries: 0,
                retry_base: Duration::ZERO,
            },
        )
        .unwrap();
        let writer = volume.recover_writer().await.unwrap();
        let payload = Bytes::from_static(b"stalled");
        let encoded_bytes = payload.len() + 4;
        let stall_timeout = Duration::from_millis(20);
        let mut handle = crate::engine::WalEngine::start(
            writer,
            crate::WalEngineConfig {
                queue_capacity: 2,
                max_record_bytes: payload.len(),
                pipeline_window_records: 1,
                max_inflight_bytes: encoded_bytes,
                max_replica_lag_bytes: encoded_bytes,
                lane_stall_timeout: stall_timeout,
                repair_interval: None,
                ..Default::default()
            },
        )
        .unwrap();
        let first = handle
            .enqueue_append(crate::segment::WalSeqNo::ZERO, payload.clone())
            .await
            .unwrap();

        let second = tokio::time::timeout(
            stall_timeout.saturating_mul(10),
            handle.enqueue_append(crate::segment::WalSeqNo::record(1), payload),
        )
        .await
        .expect("blocked admission did not wake after the writer poisoned");
        assert!(matches!(second, Err(crate::Error::Closed)));

        let first = tokio::time::timeout(stall_timeout.saturating_mul(10), first)
            .await
            .expect("admitted append did not receive terminal poison");
        assert!(matches!(first, Err(crate::Error::Poisoned)));
        let _ = handle.shutdown().await;
    }
}
