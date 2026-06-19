use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::future::join_all;
use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore};
use tokio::time::{Duration, Sleep};

use crate::error::Error;
use crate::manifest::ManifestUpdate;
use crate::metrics::Metrics;
use crate::record::RecordFrame;
use crate::segment::{
    PendingSwap, RecordCommitRange, RegisteredSpare, SegmentedWriter, TruncationReport, WalSeqNo,
};
use crate::transport::Replica;

const DEFAULT_PIPELINE_WINDOW_RECORDS: usize = 32;
const DEFAULT_QUEUE_CAPACITY: usize = DEFAULT_PIPELINE_WINDOW_RECORDS * 8;
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Clone, Debug)]
/// Capacity controls for the in-process transactional WAL pipeline.
pub struct WalEngineConfig {
    /// Maximum records waiting to enter the dispatched pipeline.
    ///
    /// This is one combined bound across the handle-to-engine channel and the
    /// engine's internal queue, not a separate allowance for each stage. The
    /// default is 256 records, eight times the default pipeline window.
    pub queue_capacity: usize,
    /// Maximum application payload bytes accepted in one record.
    pub max_record_bytes: usize,
    /// Maximum dispatched but not yet logically committed records. Slots are
    /// replenished individually as ordered quorum completions arrive.
    pub pipeline_window_records: usize,
    /// Maximum encoded WAL bytes admitted but not yet resolved through the
    /// ordered quorum completion stream. Admission waits for this byte budget
    /// before taking ownership of another record.
    pub max_inflight_bytes: usize,
    /// Maximum unacknowledged encoded bytes retained for one replica lane.
    /// A lane that falls farther behind is dropped so a healthy quorum can
    /// continue without retaining an unbounded retry suffix. Must be at
    /// least [`max_inflight_bytes`](Self::max_inflight_bytes).
    pub max_replica_lag_bytes: usize,
    /// Maximum interval in which a lane with retained writes may make no
    /// durable-tail progress before it is shed.
    ///
    /// Any increase in `persisted_size` resets the interval, so a slow but
    /// steadily advancing lane remains live. Shedding never changes the
    /// strict-majority quorum requirement: if the remaining lanes cannot cover
    /// an admitted record, the writer poisons instead of advancing its commit
    /// watermark. The default is five seconds.
    pub lane_stall_timeout: Duration,
    /// Target encoded size for automatic segment rotation.
    ///
    /// The engine checks this threshold between records, so a segment may exceed
    /// it by one complete encoded record. This is not a hard object-size limit.
    ///
    /// Treat this as the minimum segment-size floor for the single-pending
    /// design. With sustained encoded throughput `T` bytes/s and worst-case
    /// provision-plus-fold latency `L` seconds, choose at least `T * L` times
    /// an operational safety factor. If the active pending segment crosses
    /// this threshold before its refill is registered, dispatch pauses
    /// fail-closed rather than acknowledging an unregistered successor.
    pub max_segment_bytes: usize,
    /// Hard encoded-byte ceiling for one active segment object.
    ///
    /// The default is 4 GiB, deliberately far below GCS's 5 TiB object limit.
    /// Rotation normally occurs at [`max_segment_bytes`](Self::max_segment_bytes),
    /// but the manifest's bounded sealed-segment directory can defer that
    /// rotation until truncation removes retained entries. Admission stops
    /// cleanly at this ceiling with [`Error::ActiveSegmentFull`] instead of
    /// letting the active object grow toward the provider limit or poisoning
    /// the healthy writer. The ceiling must fit one maximum encoded record and
    /// be at least the advisory rotation target.
    pub max_active_segment_bytes: usize,
    /// Interval between background maintenance passes.
    ///
    /// Each startup and periodic pass runs the dead-incarnation sweep deferred
    /// by recovery, retries deletion tombstones below the committed floor, then
    /// repairs retained sealed segments. `None` disables periodic passes.
    /// Startup cleanup and repair, plus targeted repair after a degraded
    /// rotation, still run without timers.
    pub repair_interval: Option<Duration>,
    /// Maximum time graceful shutdown may spend draining accepted work and
    /// joining owned background tasks before aborting them.
    ///
    /// The default is five minutes, long enough for the default storage retry
    /// budget while still turning a wedged backend or task into a bounded error.
    pub shutdown_timeout: Duration,
}

impl Default for WalEngineConfig {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            max_record_bytes: 1024 * 1024,
            pipeline_window_records: DEFAULT_PIPELINE_WINDOW_RECORDS,
            max_inflight_bytes: 64 * 1024 * 1024,
            max_replica_lag_bytes: 64 * 1024 * 1024,
            lane_stall_timeout: crate::protocol::DEFAULT_LANE_STALL_TIMEOUT,
            max_segment_bytes: 256 * 1024 * 1024,
            max_active_segment_bytes: usize::try_from(4_u64 * 1024 * 1024 * 1024)
                .unwrap_or(usize::MAX),
            repair_interval: Some(Duration::from_secs(300)),
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }
}

impl WalEngineConfig {
    fn validate(&self) -> Result<(), Error> {
        if self.queue_capacity == 0 {
            return Err(Error::InvalidConfig("queue_capacity must be nonzero"));
        }
        if self.max_record_bytes == 0 {
            return Err(Error::InvalidConfig("max_record_bytes must be nonzero"));
        }
        if self.max_record_bytes > RecordFrame::MAX_PAYLOAD_BYTES {
            return Err(Error::InvalidConfig(
                "max_record_bytes exceeds the record format limit",
            ));
        }
        if self.pipeline_window_records == 0 {
            return Err(Error::InvalidConfig(
                "pipeline_window_records must be nonzero",
            ));
        }
        if self.max_inflight_bytes == 0 {
            return Err(Error::InvalidConfig("max_inflight_bytes must be nonzero"));
        }
        if self.max_replica_lag_bytes == 0 {
            return Err(Error::InvalidConfig(
                "max_replica_lag_bytes must be nonzero",
            ));
        }
        let max_encoded_record = self
            .max_record_bytes
            .checked_add(4)
            .ok_or(Error::InvalidConfig("max_record_bytes is too large"))?;
        if max_encoded_record > self.max_inflight_bytes {
            return Err(Error::InvalidConfig(
                "max_inflight_bytes must fit one maximum-size encoded record",
            ));
        }
        if self.max_replica_lag_bytes < self.max_inflight_bytes {
            return Err(Error::InvalidConfig(
                "max_replica_lag_bytes must be at least max_inflight_bytes",
            ));
        }
        if self.lane_stall_timeout == Duration::ZERO {
            return Err(Error::InvalidConfig("lane_stall_timeout must be nonzero"));
        }
        if self.max_inflight_bytes > Semaphore::MAX_PERMITS {
            return Err(Error::InvalidConfig(
                "max_inflight_bytes exceeds the semaphore limit",
            ));
        }
        if self.max_segment_bytes == 0 {
            return Err(Error::InvalidConfig("max_segment_bytes must be nonzero"));
        }
        if self.max_active_segment_bytes == 0 {
            return Err(Error::InvalidConfig(
                "max_active_segment_bytes must be nonzero",
            ));
        }
        if self.max_active_segment_bytes < max_encoded_record {
            return Err(Error::InvalidConfig(
                "max_active_segment_bytes must fit one maximum-size encoded record",
            ));
        }
        if self.max_active_segment_bytes < self.max_segment_bytes {
            return Err(Error::InvalidConfig(
                "max_active_segment_bytes must be at least max_segment_bytes",
            ));
        }
        if self.repair_interval == Some(Duration::ZERO) {
            return Err(Error::InvalidConfig("repair_interval must be nonzero"));
        }
        if self.shutdown_timeout == Duration::ZERO {
            return Err(Error::InvalidConfig("shutdown_timeout must be nonzero"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Durability evidence for one caller-numbered record.
///
/// Success means the record reached a strict-majority quorum and every preceding
/// sequence number committed.
pub struct AppendReceipt {
    /// Caller-assigned sequence number of the committed record.
    pub seqno: WalSeqNo,
}

impl AppendReceipt {
    /// Exclusive replay and checkpoint boundary after this append.
    pub fn next_seqno(&self) -> WalSeqNo {
        WalSeqNo::record(self.seqno.record_index + 1)
    }
}

/// Owned durability future returned after a record is admitted in sequence.
///
/// Dropping this value does not cancel the append. The WAL continues processing
/// admitted work so shutdown can drain it and recovery can preserve it. A
/// failure may preserve a representative terminal [`crate::TransportCode`] from
/// one failed lane. Use [`Error::may_have_committed`] to determine whether
/// recovery must resolve the record's durable outcome.
pub struct AppendCompletion {
    receiver: oneshot::Receiver<Result<AppendReceipt, Error>>,
}

impl Future for AppendCompletion {
    type Output = Result<AppendReceipt, Error>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(context) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Closed)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Starts the background append pipeline and maintenance task.
pub struct WalEngine;

/// Control plane for a running WAL engine.
///
/// This handle is intentionally single-owner and not cloneable. Calling
/// [`enqueue_append`](Self::enqueue_append) through `&mut self` gives the
/// database one linear admission point for its caller-assigned sequence
/// numbers, while returned [`AppendCompletion`] values remain independently
/// awaitable by the database's apply pipeline.
pub struct WalHandle {
    sender: mpsc::Sender<Command>,
    metrics: Arc<Metrics>,
    engine_task: tokio::task::JoinHandle<()>,
    provisioner_task: tokio::task::JoinHandle<()>,
    provisioner_shutdown: watch::Sender<bool>,
    maintenance: crate::maintenance::MaintenanceHandle,
    maintenance_task: tokio::task::JoinHandle<()>,
    rotation_recheck: mpsc::Sender<()>,
    next_seqno: u64,
    max_record_bytes: usize,
    max_inflight_bytes: usize,
    max_active_segment_bytes: usize,
    total_admitted_bytes: u128,
    active_capacity: watch::Receiver<ActiveSegmentCapacity>,
    inflight_bytes: Arc<Semaphore>,
    queue_slots: Arc<Semaphore>,
    shutdown_timeout: Duration,
}

struct AdmittedAppend {
    seqno: WalSeqNo,
    record: RecordFrame,
    payload_bytes: usize,
    encoded_bytes: usize,
    /// Cumulative encoded admission boundary after this record. The engine
    /// publishes the last boundary assigned to an old segment when it swaps,
    /// so the handle can charge every later queued record to the successor
    /// without inspecting or racing the command queue.
    admission_end_bytes: u128,
    /// Admission time for the commit-latency histogram. `tokio::time`
    /// follows the simulator's virtual clock under DST.
    admitted_at: tokio::time::Instant,
    _inflight_bytes: OwnedSemaphorePermit,
    queue_slot: Option<OwnedSemaphorePermit>,
    completion: oneshot::Sender<Result<AppendReceipt, Error>>,
}

enum Command {
    Append(AdmittedAppend),
    Shutdown { response: oneshot::Sender<()> },
}

struct CompletionBatch {
    commits: RecordCommitRange,
    appends: VecDeque<AdmittedAppend>,
}

struct EngineState {
    queue: VecDeque<Command>,
    completion_batches: VecDeque<CompletionBatch>,
    pending_records: usize,
    next_completion: u64,
    next_admission: u64,
    last_dispatched_admission_bytes: u128,
    input_closed: bool,
    shutdown_response: Option<oneshot::Sender<()>>,
}

impl EngineState {
    fn new(next_record_index: u64) -> Self {
        Self {
            queue: VecDeque::new(),
            completion_batches: VecDeque::new(),
            pending_records: 0,
            next_completion: next_record_index,
            next_admission: next_record_index,
            last_dispatched_admission_bytes: 0,
            input_closed: false,
            shutdown_response: None,
        }
    }

    fn drain_available(&mut self, receiver: &mut mpsc::Receiver<Command>) {
        loop {
            match receiver.try_recv() {
                Ok(command) => {
                    admit_command(command, &mut self.queue, &mut self.next_admission);
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.input_closed = true;
                    break;
                }
            }
        }
    }

    fn stop_admission(&mut self, receiver: &mut mpsc::Receiver<Command>) {
        receiver.close();
        while let Ok(command) = receiver.try_recv() {
            self.queue.push_back(command);
        }
    }

    fn fail_queued(
        &mut self,
        receiver: &mut mpsc::Receiver<Command>,
        error: Error,
        metrics: &Metrics,
    ) {
        self.stop_admission(receiver);
        fail_all(&mut self.queue, error, metrics);
        metrics.operation_failures.increment();
    }

    fn poison(&mut self, receiver: &mut mpsc::Receiver<Command>, metrics: &Metrics) {
        self.stop_admission(receiver);
        fail_completion_batches(&mut self.completion_batches, Error::Poisoned, metrics);
        fail_all(&mut self.queue, Error::Poisoned, metrics);
        metrics.operation_failures.increment();
    }
}

enum CompletionFailure {
    Append(AdmittedAppend, Error),
    Invariant(&'static str),
}

#[derive(Clone, Copy, Debug)]
struct ActiveSegmentCapacity {
    /// Cumulative engine-lifetime admission bytes preceding this segment.
    admission_start_bytes: u128,
    /// Bytes already present when the engine adopted this active segment.
    ///
    /// Normal recovery starts a fresh empty object. Tests and low-level users
    /// may start the engine around a writer that already committed records,
    /// which still need to count against the hard ceiling.
    existing_bytes: usize,
    /// Whether recovery can still add this active object to the manifest
    /// directory after it gains a committed record.
    seal_room: bool,
}

struct EngineControl {
    catalog: watch::Sender<Vec<crate::segment::SegmentDescriptor>>,
    maintenance: crate::maintenance::MaintenanceHandle,
    active_capacity: watch::Sender<ActiveSegmentCapacity>,
    queue_slots: Arc<Semaphore>,
    rotation_rechecks: mpsc::Receiver<()>,
    provision_requests: mpsc::Sender<ProvisionAttempt>,
    provision_results: mpsc::Receiver<Result<ProvisionOutcome, Error>>,
}

impl WalEngine {
    /// Start the background engine around an already recovered writer.
    ///
    /// Configuration limits must be nonzero and internally consistent: byte
    /// budgets must fit a maximum encoded record, replica lag must cover the
    /// in-flight budget, and the hard active-segment ceiling must cover the
    /// advisory rotation target. Invalid limits return [`Error::InvalidConfig`].
    /// This function requires a running Tokio runtime and returns immediately
    /// after spawning the task.
    pub fn start(mut writer: SegmentedWriter, config: WalEngineConfig) -> Result<WalHandle, Error> {
        config.validate()?;
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let metrics = writer.metrics();
        let next_seqno = writer.committed_record_end();
        let active_segment_bytes = writer.active_segment_bytes();
        let active_segment_seal_room = writer.active_segment_has_seal_room();
        if active_segment_bytes != 0 && !active_segment_seal_room {
            return Err(Error::SegmentDirectoryFull);
        }
        let max_record_bytes = config.max_record_bytes;
        let max_inflight_bytes = config.max_inflight_bytes;
        let max_active_segment_bytes = config.max_active_segment_bytes;
        let shutdown_timeout = config.shutdown_timeout;
        let inflight_bytes = Arc::new(Semaphore::new(max_inflight_bytes));
        let queue_slots = Arc::new(Semaphore::new(config.queue_capacity));
        writer.set_max_replica_lag_bytes(config.max_replica_lag_bytes);
        writer.set_lane_stall_timeout(config.lane_stall_timeout);
        // Maintenance (sealed-segment repair, floor-committed truncation)
        // runs on its own task, serialized internally, concurrent with the
        // engine: it shares no mutable writer state — only catalog snapshots
        // published through the watch channel and an epoch-free manifest
        // handle of its own.
        let (catalog_tx, catalog_rx) = watch::channel(writer.sealed_segments_snapshot());
        let (maintenance, maintenance_task) = crate::maintenance::start(
            writer.maintenance_config(config.repair_interval),
            catalog_rx,
            Arc::clone(&metrics),
        );
        let (active_capacity_tx, active_capacity) = watch::channel(ActiveSegmentCapacity {
            admission_start_bytes: 0,
            existing_bytes: active_segment_bytes,
            seal_room: active_segment_seal_room,
        });
        let (rotation_recheck, rotation_rechecks) = mpsc::channel(1);
        let (provision_requests, provision_attempts) = mpsc::channel::<ProvisionAttempt>(1);
        let (provision_results_tx, provision_results) = mpsc::channel(1);
        let (provisioner_shutdown, provisioner_shutdown_rx) = watch::channel(false);
        let provisioner_task = tokio::spawn(run_provisioner(
            provision_attempts,
            provision_results_tx,
            provisioner_shutdown_rx,
        ));
        tracing::info!(
            next_record_index = next_seqno,
            queue_capacity = config.queue_capacity,
            pipeline_window_records = config.pipeline_window_records,
            max_inflight_bytes = config.max_inflight_bytes,
            max_replica_lag_bytes = config.max_replica_lag_bytes,
            lane_stall_timeout_ms = config.lane_stall_timeout.as_millis(),
            max_segment_bytes = config.max_segment_bytes,
            max_active_segment_bytes = config.max_active_segment_bytes,
            "WAL engine started"
        );
        let engine_task = tokio::spawn(run_engine(
            writer,
            config,
            receiver,
            Arc::clone(&metrics),
            EngineControl {
                catalog: catalog_tx,
                maintenance: maintenance.clone(),
                active_capacity: active_capacity_tx,
                queue_slots: Arc::clone(&queue_slots),
                rotation_rechecks,
                provision_requests,
                provision_results,
            },
        ));
        Ok(WalHandle {
            sender,
            metrics,
            engine_task,
            provisioner_task,
            provisioner_shutdown,
            maintenance,
            maintenance_task,
            rotation_recheck,
            next_seqno,
            max_record_bytes,
            max_inflight_bytes,
            max_active_segment_bytes,
            total_admitted_bytes: 0,
            active_capacity,
            inflight_bytes,
            queue_slots,
            shutdown_timeout,
        })
    }
}

impl WalHandle {
    /// Admit one caller-numbered opaque record without waiting for durability.
    ///
    /// Before waiting, this method verifies that `seqno` is exactly the next
    /// contiguous sequence number and that the record fits the configured
    /// limits. If the manifest has no slot recovery could use to seal this
    /// active object, admission returns [`Error::SegmentDirectoryFull`]. If the
    /// active object is at its hard ceiling, admission returns
    /// [`Error::ActiveSegmentFull`]. Neither condition consumes `seqno`;
    /// truncation may free directory capacity, after which the same append can
    /// be retried. Otherwise this waits for encoded-byte admission capacity and
    /// a bounded queue slot. On success the WAL owns the record and the
    /// returned [`AppendCompletion`] may be moved to another task. Await that
    /// completion before applying the transaction to the database.
    ///
    /// The payload should be the database's complete encoded transaction batch.
    /// The WAL treats it as opaque bytes and never merges it with another call.
    /// Dropping the completion does not cancel admitted work. On an eventual
    /// completion error, use [`Error::may_have_committed`] before deciding
    /// whether recovery must resolve the sequence number.
    pub async fn enqueue_append(
        &mut self,
        seqno: WalSeqNo,
        record: Bytes,
    ) -> Result<AppendCompletion, Error> {
        if self.sender.is_closed() {
            return Err(Error::Closed);
        }
        if seqno.record_index != self.next_seqno {
            return Err(Error::OutOfOrder {
                expected: WalSeqNo::record(self.next_seqno),
                actual: seqno,
            });
        }
        if seqno.record_index == u64::MAX {
            return Err(Error::SequenceExhausted);
        }
        let payload_bytes = record.len();
        if payload_bytes > self.max_record_bytes {
            return Err(Error::RecordTooLarge {
                max: self.max_record_bytes,
                actual: payload_bytes,
            });
        }

        let record = RecordFrame { payload: record };
        let encoded_bytes = record.encoded_len().map_err(|_| Error::RecordTooLarge {
            max: self.max_record_bytes.min(RecordFrame::MAX_PAYLOAD_BYTES),
            actual: payload_bytes,
        })?;
        let capacity = *self.active_capacity.borrow_and_update();
        if !capacity.seal_room {
            return Err(Error::SegmentDirectoryFull);
        }
        let admitted_here = self
            .total_admitted_bytes
            .checked_sub(capacity.admission_start_bytes)
            .ok_or_else(|| {
                Error::Internal(
                    "active-segment admission boundary exceeds total admitted bytes".into(),
                )
            })?;
        let current_active = (capacity.existing_bytes as u128)
            .checked_add(admitted_here)
            .ok_or_else(|| Error::Internal("active-segment byte accounting overflowed".into()))?;
        let projected = current_active
            .checked_add(encoded_bytes as u128)
            .ok_or_else(|| Error::Internal("active-segment byte accounting overflowed".into()))?;
        if projected > self.max_active_segment_bytes as u128 {
            return Err(Error::ActiveSegmentFull {
                max: self.max_active_segment_bytes,
                current: usize::try_from(current_active).unwrap_or(usize::MAX),
                requested: encoded_bytes,
            });
        }
        // Sequence numbers cap an engine lifetime at fewer than 2^64 records
        // and every encoded record is u32-bounded, so u128 has ample headroom.
        // Keep the check anyway: protocol-invariant failures are errors, never
        // panics.
        let admission_end_bytes = self
            .total_admitted_bytes
            .checked_add(encoded_bytes as u128)
            .ok_or_else(|| Error::Internal("cumulative admission bytes overflowed".into()))?;
        let permits = u32::try_from(encoded_bytes)
            .map_err(|_| Error::Internal("encoded record length exceeds u32".into()))?;
        let inflight_bytes = Arc::clone(&self.inflight_bytes)
            .acquire_many_owned(permits)
            .await
            .map_err(|_| Error::Closed)?;
        let queue_slot = Arc::clone(&self.queue_slots)
            .acquire_owned()
            .await
            .map_err(|_| Error::Closed)?;
        self.metrics.max_inflight_bytes.update_max(
            self.max_inflight_bytes
                .saturating_sub(self.inflight_bytes.available_permits()) as u64,
        );
        let (completion, receiver) = oneshot::channel();
        self.sender
            .send(Command::Append(AdmittedAppend {
                seqno,
                record,
                payload_bytes,
                encoded_bytes,
                admission_end_bytes,
                admitted_at: tokio::time::Instant::now(),
                _inflight_bytes: inflight_bytes,
                queue_slot: Some(queue_slot),
                completion,
            }))
            .await
            .map_err(|_| Error::Closed)?;

        self.total_admitted_bytes = admission_end_bytes;
        self.next_seqno += 1;
        self.metrics.append_records.increment();
        self.metrics.append_bytes.add(payload_bytes as u64);
        Ok(AppendCompletion { receiver })
    }

    /// Advance the application checkpoint floor and delete eligible sealed
    /// segments.
    ///
    /// `floor` is the first record the database still needs. Call this only after
    /// the database has durably checkpointed every record before that boundary.
    /// The WAL deletes only whole sealed segments; it never truncates the active
    /// segment or deletes a segment containing `floor`.
    ///
    /// Deletion is best effort across zones. The returned report contains only
    /// deletion statistics; the database remains the authority for the durable
    /// checkpoint it supplied. Startup and periodic maintenance retry
    /// tombstones whose zones were unavailable during this call. A successful
    /// truncation also makes the engine re-read directory capacity, so an active
    /// segment held at [`Error::ActiveSegmentFull`] can rotate and resume
    /// admission once enough entries are removed.
    pub async fn truncate_before(&self, floor: WalSeqNo) -> Result<TruncationReport, Error> {
        let report = self.maintenance.truncate(floor).await?;
        // The maintenance manifest and writer manifest are independent
        // handles. One pending wake is enough to make the engine refresh its
        // copy after every completed truncation: if the slot is full, its
        // queued wake has not been received yet; otherwise this call fills it.
        let _ = self.rotation_recheck.try_send(());
        Ok(report)
    }

    /// Consume the admission handle, abort every owned task, and await their
    /// termination.
    ///
    /// Accepted completions may resolve as closed or ambiguous. Use
    /// [`shutdown`](Self::shutdown) when accepted work must drain gracefully.
    pub async fn abort(self) {
        let WalHandle {
            sender,
            engine_task,
            provisioner_task,
            provisioner_shutdown,
            maintenance,
            maintenance_task,
            shutdown_timeout,
            ..
        } = self;
        let mut engine_task = engine_task;
        let mut provisioner_task = provisioner_task;
        let mut maintenance_task = maintenance_task;
        let _ = provisioner_shutdown.send(true);
        engine_task.abort();
        maintenance_task.abort();
        drop(sender);
        drop(maintenance);
        if tokio::time::timeout(shutdown_timeout, async {
            let _ = tokio::join!(
                &mut engine_task,
                &mut provisioner_task,
                &mut maintenance_task
            );
        })
        .await
        .is_err()
        {
            engine_task.abort();
            provisioner_task.abort();
            maintenance_task.abort();
            let _ = tokio::join!(
                &mut engine_task,
                &mut provisioner_task,
                &mut maintenance_task
            );
            tracing::error!("WAL abort required forced cancellation after the shutdown deadline");
        }
    }

    /// Consume the admission handle, drain accepted work, finish every
    /// committed seal, and wait for all owned tasks to exit.
    ///
    pub async fn shutdown(self) -> Result<(), Error> {
        let WalHandle {
            sender,
            engine_task,
            provisioner_task,
            provisioner_shutdown,
            maintenance,
            maintenance_task,
            shutdown_timeout,
            ..
        } = self;
        let mut engine_task = engine_task;
        let mut provisioner_task = provisioner_task;
        let mut maintenance_task = maintenance_task;
        let maintenance_shutdown = maintenance.clone();
        let provisioner_timeout_shutdown = provisioner_shutdown.clone();
        let graceful = async {
            let (response, receiver) = oneshot::channel();
            let sent = sender.send(Command::Shutdown { response }).await;
            drop(sender);
            let acknowledged = if sent.is_ok() {
                receiver.await.is_ok()
            } else {
                false
            };
            // Provisioning is speculative. Once the engine has acknowledged
            // its drain, signal the worker, cancel its in-flight attempt through
            // the worker's select, and join both owned tasks.
            let _ = provisioner_shutdown.send(true);
            let (engine_result, provisioner_result) =
                tokio::join!(&mut engine_task, &mut provisioner_task);
            let engine_joined = engine_result.is_ok();
            let provisioner_joined = provisioner_result.is_ok();

            // Stop periodic work explicitly, then let maintenance drain every
            // queued seal before exiting. The dedicated signal cannot be starved
            // by an overdue repair tick.
            maintenance.shutdown();
            drop(maintenance);
            let maintenance_joined = (&mut maintenance_task).await.is_ok();

            acknowledged && engine_joined && provisioner_joined && maintenance_joined
        };

        match tokio::time::timeout(shutdown_timeout, graceful).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(Error::Closed),
            Err(_) => {
                tracing::error!(
                    ?shutdown_timeout,
                    "WAL graceful shutdown exceeded its deadline; aborting owned tasks"
                );
                maintenance_shutdown.shutdown();
                let _ = provisioner_timeout_shutdown.send(true);
                engine_task.abort();
                maintenance_task.abort();
                let cleanup = async {
                    let _ = tokio::join!(
                        &mut engine_task,
                        &mut provisioner_task,
                        &mut maintenance_task
                    );
                };
                if tokio::time::timeout(shutdown_timeout, cleanup)
                    .await
                    .is_err()
                {
                    engine_task.abort();
                    provisioner_task.abort();
                    maintenance_task.abort();
                    let _ = tokio::join!(
                        &mut engine_task,
                        &mut provisioner_task,
                        &mut maintenance_task
                    );
                    tracing::error!(
                        "WAL shutdown required forced cancellation after the cleanup deadline"
                    );
                }
                Err(Error::ShutdownTimeout {
                    timeout: shutdown_timeout,
                })
            }
        }
    }
}

async fn run_engine(
    mut writer: SegmentedWriter,
    config: WalEngineConfig,
    mut receiver: mpsc::Receiver<Command>,
    metrics: Arc<Metrics>,
    control: EngineControl,
) {
    let EngineControl {
        catalog,
        maintenance,
        active_capacity,
        queue_slots,
        mut rotation_rechecks,
        provision_requests,
        mut provision_results,
    } = control;
    let client_config = writer.client_config();
    let mut state = EngineState::new(writer.committed_record_end());
    let mut rotation =
        writer
            .take_recovered_fold()
            .map_or(Rotation::Idle, |swap| Rotation::Draining {
                swap: Box::new(swap),
                fold_attempts: 0,
                retry: None,
                fold_capacity_blocked: false,
                successor_due: false,
            });
    let mut rotation_rechecks_open = true;
    // Spare provisioning runs on a dedicated worker; its result channel is a
    // `select!` wake source, so a swap waiting on a slow spare can never park
    // the engine. `spare_requested` keeps at most one attempt outstanding.
    let mut spare_requested = false;
    let attempted_stats = Arc::clone(&metrics);
    let on_attempted: crate::protocol::AttemptedBytes = Arc::new(move |bytes| {
        attempted_stats.replica_bytes_attempted.add(bytes);
    });

    'engine: loop {
        metrics.rotation_state.set(rotation.metric_value());
        state.drain_available(&mut receiver);
        observe_queue_depth(&metrics, config.queue_capacity, &queue_slots);

        // Consume the segment commit watermark directly. Every batch is in
        // global record order, and each segment watermark is already a
        // contiguous prefix, so no second completion reorder is needed.
        // The active pending segment was registered before the swap, so its
        // acknowledgments do not wait for the later fold.
        let failed = 'completion: loop {
            let Some(batch) = state.completion_batches.front_mut() else {
                break None;
            };
            let (committed_end, failure) = batch.commits.progress();
            while batch
                .appends
                .front()
                .is_some_and(|append| append.seqno.record_index < committed_end)
            {
                let Some(append) = batch.appends.pop_front() else {
                    // The commit tracker and completion queue are separate
                    // state machines. If they ever disagree, poison the WAL
                    // rather than panicking the host process.
                    break 'completion Some(CompletionFailure::Invariant(
                        "commit watermark advanced without a queued append",
                    ));
                };
                if state.pending_records == 0 {
                    break 'completion Some(CompletionFailure::Invariant(
                        "completion queue exceeded the pending record count",
                    ));
                }
                debug_assert_eq!(append.seqno.record_index, state.next_completion);
                complete_append(append, &metrics);
                state.pending_records -= 1;
                state.next_completion += 1;
                rotation.mark_due(writer.rotation_due(config.max_segment_bytes));
            }
            if batch.appends.is_empty() {
                state.completion_batches.pop_front();
                continue;
            }
            if let Some(error) = failure {
                let Some(append) = batch.appends.pop_front() else {
                    break Some(CompletionFailure::Invariant(
                        "failed commit batch had no unresolved append",
                    ));
                };
                if state.pending_records == 0 {
                    break Some(CompletionFailure::Invariant(
                        "failed completion exceeded the pending record count",
                    ));
                }
                state.pending_records -= 1;
                break Some(CompletionFailure::Append(append, error));
            }
            break None;
        };
        if let Some(failure) = failed {
            match failure {
                CompletionFailure::Append(append, error) => {
                    tracing::warn!(
                        record_index = append.seqno.record_index,
                        %error,
                        "WAL engine poisoned by an indeterminate append"
                    );
                    fail_append(append, error, &metrics);
                }
                CompletionFailure::Invariant(message) => {
                    tracing::error!(message, "WAL engine completion invariant failed");
                }
            }
            state.poison(&mut receiver, &metrics);
            break 'engine;
        }

        // The dedicated worker owns all create/open and manifest work. Before
        // a rotation it provisions then confirms `pending`; after a swap it
        // provisions an unregistered refill, then atomically folds the old
        // tail and registers that refill.
        if !spare_requested {
            let attempt = if rotation.fold_ready(state.next_completion)
                && writer.unregistered_spare_ready()
            {
                let fold = match rotation
                    .swap()
                    .ok_or_else(|| Error::Internal("fold readiness without a swap".into()))
                    .and_then(|swap| writer.pending_fold_request(swap))
                {
                    Ok(fold) => fold,
                    Err(error) => {
                        state.fail_queued(&mut receiver, error, &metrics);
                        break 'engine;
                    }
                };
                let parts = match writer.provision_parts() {
                    Ok(parts) => parts,
                    Err(error) => {
                        state.fail_queued(&mut receiver, error, &metrics);
                        break 'engine;
                    }
                };
                Some(ProvisionAttempt {
                    future: Box::pin(async move {
                        crate::segment::fold_registered_pending(parts.manifest, fold)
                            .await
                            .map(ProvisionOutcome::Folded)
                    }),
                    replicas: Vec::new(),
                })
            } else if writer.spare_wanted() {
                let parts = match writer.provision_parts() {
                    Ok(parts) => parts,
                    Err(error) => {
                        state.fail_queued(&mut receiver, error, &metrics);
                        break 'engine;
                    }
                };
                let id = writer.next_segment_id();
                let replicas =
                    crate::segment::provision_replicas(&parts.factories, &parts.prefix, &id);
                if rotation.has_pending_fold() {
                    let cancellation_replicas = replicas.clone();
                    Some(ProvisionAttempt {
                        future: Box::pin(async move {
                            crate::segment::provision_spare_with_replicas(
                                replicas,
                                parts.client_config,
                                parts.max_replica_lag_bytes,
                                parts.lane_stall_timeout,
                                id,
                                parts.metrics,
                            )
                            .await
                            .map(|(id, writer)| ProvisionOutcome::Unregistered(id, writer))
                        }),
                        replicas: cancellation_replicas,
                    })
                } else {
                    let cancellation_replicas = replicas.clone();
                    Some(ProvisionAttempt {
                        future: Box::pin(async move {
                            crate::segment::provision_registered_spare_with_replicas(
                                parts, id, replicas,
                            )
                            .await
                            .map(|spare| ProvisionOutcome::Registered(Box::new(spare)))
                        }),
                        replicas: cancellation_replicas,
                    })
                }
            } else {
                None
            };
            if let Some(attempt) = attempt {
                if provision_requests.try_send(attempt).is_ok() {
                    spare_requested = true;
                    metrics.spare_provisioning_attempts.increment();
                }
            }
        }

        if rotation.swap_ready() && writer.spare_ready() && writer.swap_boundary_ready() {
            // Swap rotation, first half: once every admitted old-tail record is
            // committed, wait only for the already-dispatched digest pipeline
            // and route admissions to the spare in memory. There is no manifest
            // write, object creation, or stream open here. Pinning the successor
            // base to this committed boundary prevents a pending segment from
            // being spliced above an unresolved global sequence gap.
            match writer.begin_swap().await {
                Ok(Some(swap)) => {
                    rotation = Rotation::Draining {
                        swap: Box::new(swap),
                        fold_attempts: 0,
                        retry: None,
                        fold_capacity_blocked: false,
                        successor_due: false,
                    };
                    active_capacity.send_replace(ActiveSegmentCapacity {
                        admission_start_bytes: state.last_dispatched_admission_bytes,
                        existing_bytes: writer.active_segment_bytes(),
                        seal_room: writer.active_segment_has_seal_room(),
                    });
                    // The consumed pending slot now needs an unregistered
                    // refill. Re-enter the loop so the worker request is
                    // queued before the engine reaches its single wait point.
                    continue 'engine;
                }
                Ok(None) => rotation = Rotation::Idle,
                Err(error) => {
                    tracing::warn!(%error, "WAL engine rotation swap failed");
                    state.fail_queued(&mut receiver, error, &metrics);
                    break 'engine;
                }
            }
        }

        if state.pending_records == 0 && !matches!(state.queue.front(), Some(Command::Append(_))) {
            match state.queue.pop_front() {
                Some(Command::Shutdown { response }) => {
                    fail_all(&mut state.queue, Error::Closed, &metrics);
                    state.shutdown_response = Some(response);
                    break;
                }
                Some(Command::Append(_)) => unreachable!(),
                None => {}
            }
        }

        // Dispatch pauses when rotation is due without a confirmed pending
        // segment, including when the consumed pending segment itself crosses
        // the threshold before fold/refill completes.
        while !rotation.dispatch_paused()
            && matches!(state.queue.front(), Some(Command::Append(_)))
            && state.pending_records < config.pipeline_window_records
        {
            let outstanding = state.pending_records;
            let room = config.pipeline_window_records - outstanding;
            let mut appends = Vec::with_capacity(room);
            for _ in 0..room {
                if !matches!(state.queue.front(), Some(Command::Append(_))) {
                    break;
                }
                let Some(Command::Append(append)) = state.queue.pop_front() else {
                    unreachable!();
                };
                appends.push(append);
            }
            let records = appends.iter().map(|append| append.record.clone()).collect();
            let wal_record_bytes: u64 = appends
                .iter()
                .map(|append| append.encoded_bytes as u64)
                .sum();
            let batch_admission_end = appends
                .last()
                .map(|append| append.admission_end_bytes)
                .unwrap_or(state.last_dispatched_admission_bytes);
            match writer
                .enqueue_records_for_engine(records, Arc::clone(&on_attempted))
                .await
            {
                Ok(commits) => {
                    for append in &mut appends {
                        drop(append.queue_slot.take());
                    }
                    state.last_dispatched_admission_bytes = batch_admission_end;
                    if outstanding > 0 {
                        metrics.pipeline_refills.increment();
                    }
                    metrics.wal_record_bytes.add(wal_record_bytes);
                    let expected_first = appends
                        .first()
                        .map(|append| append.seqno.record_index)
                        .expect("an admitted engine batch is non-empty");
                    let expected_end = appends
                        .last()
                        .map(|append| append.seqno.record_index + 1)
                        .expect("an admitted engine batch is non-empty");
                    if commits.first_global_record_index() != expected_first
                        || commits.end_global_record_index() != expected_end
                    {
                        let error = Error::Internal(format!(
                            "WAL assigned records {}..{}, caller admitted {expected_first}..{expected_end}",
                            commits.first_global_record_index(),
                            commits.end_global_record_index()
                        ));
                        state.stop_admission(&mut receiver);
                        for append in appends {
                            fail_append(append, error.clone(), &metrics);
                        }
                        fail_all(&mut state.queue, error, &metrics);
                        metrics.operation_failures.increment();
                        break 'engine;
                    }
                    state.pending_records += appends.len();
                    state.completion_batches.push_back(CompletionBatch {
                        commits,
                        appends: appends.into(),
                    });
                    metrics
                        .max_inflight_records
                        .update_max(state.pending_records as u64);
                    rotation.mark_due(writer.rotation_due(config.max_segment_bytes));
                }
                Err(error) => {
                    tracing::warn!(%error, "WAL engine append pipeline failed");
                    state.stop_admission(&mut receiver);
                    for append in appends {
                        fail_append(append, error.clone(), &metrics);
                    }
                    metrics.operation_failures.increment();
                    fail_all(&mut state.queue, error, &metrics);
                    break 'engine;
                }
            }
        }
        observe_queue_depth(&metrics, config.queue_capacity, &queue_slots);

        if state.input_closed && state.queue.is_empty() && state.pending_records == 0 {
            break;
        }

        metrics.rotation_state.set(rotation.metric_value());
        // The single wait point: every event that can unblock the engine is a
        // branch here, so nothing the loop is waiting for can fail to wake it.
        //
        // `biased` makes the poll order deterministic: an unbiased `select!`
        // draws its starting branch from a thread-local RNG, so two replays
        // of one simulation seed could service simultaneously-ready arms in
        // different orders and diverge. Commit movement drains first, then the
        // rare rotation and provisioning wakes, then admission. The shared
        // record permits bound the channel and internal queue together.
        tokio::select! {
            biased;
            result = next_commit_update(&mut state.completion_batches), if state.pending_records != 0 => {
                if let Err(error) = result {
                    state.stop_admission(&mut receiver);
                    if let Some(append) = state.completion_batches
                        .front_mut()
                        .and_then(|batch| batch.appends.pop_front())
                    {
                        fail_append(append, error, &metrics);
                    }
                    fail_completion_batches(&mut state.completion_batches, Error::Poisoned, &metrics);
                    fail_all(&mut state.queue, Error::Poisoned, &metrics);
                    metrics.operation_failures.increment();
                    break 'engine;
                }
            }
            // Rotation wake sources share one arm (one `&mut rotation`
            // borrow). Seal enforcement and fold retry backoff must wake the
            // engine even when no append completion is in flight.
            event = async {
                match &mut rotation {
                    Rotation::Sealing { enforced } => RotationEvent::Sealed(enforced.await),
                    Rotation::Draining { retry: Some(retry), .. } => {
                        retry.as_mut().await;
                        RotationEvent::RetryElapsed
                    }
                    _ => unreachable!("rotation wait checked"),
                }
            }, if matches!(rotation, Rotation::Sealing { .. }) || rotation.retry_pending() => {
                match event {
                    RotationEvent::Sealed(Ok(())) => {
                        rotation = Rotation::Idle;
                        rotation.mark_due(writer.rotation_due(config.max_segment_bytes));
                    }
                    RotationEvent::Sealed(Err(_)) => {
                        tracing::warn!(
                            "seal enforcement exhausted retries; rotation disabled until restart"
                        );
                        rotation = Rotation::Disabled {
                            due: writer.rotation_due(config.max_segment_bytes),
                        };
                        metrics.operation_failures.increment();
                    }
                    RotationEvent::RetryElapsed => {
                        if let Rotation::Draining { retry, .. } = &mut rotation {
                            *retry = None;
                        }
                    }
                }
            }
            request = rotation_rechecks.recv(), if rotation_rechecks_open => {
                match request {
                    Some(()) => {
                        match writer.refresh_rotation_due(config.max_segment_bytes).await {
                            Ok(due) => {
                                rotation.release_fold_capacity_block();
                                rotation.mark_due(due);
                                active_capacity.send_modify(|capacity| {
                                    capacity.seal_room =
                                        writer.active_segment_has_seal_room();
                                });
                            }
                            Err(error) => {
                                tracing::warn!(
                                    %error,
                                    "failed to refresh rotation eligibility after truncation"
                                );
                                metrics.operation_failures.increment();
                            }
                        }
                    }
                    None => rotation_rechecks_open = false,
                }
            }
            // provisioning results arrive the same way: with every caller
            // blocked on a queued append no command wakes the loop, and a due
            // rotation must swap the moment its spare lands
            result = provision_results.recv() => {
                spare_requested = false;
                match result {
                    Some(Ok(ProvisionOutcome::Registered(spare))) => {
                        writer.adopt_registered_spare(*spare);
                    }
                    Some(Ok(ProvisionOutcome::Unregistered(id, spare))) => {
                        writer.adopt_unregistered_spare(id, spare);
                    }
                    Some(Ok(ProvisionOutcome::Folded(update))) => {
                        let taken = std::mem::replace(&mut rotation, Rotation::Idle);
                        let Rotation::Draining { swap, .. } = taken else {
                            tracing::error!("pending fold completed without a rotation");
                            state.fail_queued(&mut receiver, Error::Poisoned, &metrics);
                            break 'engine;
                        };
                        if let Err(error) = writer.confirm_fold(&swap, update) {
                            state.fail_queued(&mut receiver, error, &metrics);
                            break 'engine;
                        }
                        active_capacity.send_modify(|capacity| {
                            capacity.seal_room = writer.active_segment_has_seal_room();
                        });
                        let _ = catalog.send(writer.sealed_segments_snapshot());
                        if let Some(segment) = swap.into_segment() {
                            match maintenance.seal_segment(segment).await {
                                Ok(enforced) => rotation = Rotation::Sealing { enforced },
                                Err(error) => {
                                    tracing::warn!(
                                        %error,
                                        "maintenance task stopped before committed seal was queued"
                                    );
                                    rotation = Rotation::Disabled {
                                        due: writer.rotation_due(config.max_segment_bytes),
                                    };
                                    metrics.operation_failures.increment();
                                }
                            }
                        } else {
                            rotation = Rotation::Idle;
                            rotation.mark_due(writer.rotation_due(config.max_segment_bytes));
                        }
                    }
                    Some(Err(error)) => {
                        if matches!(error, Error::SegmentDirectoryFull)
                            && rotation.has_pending_fold()
                        {
                            // The consumed pending segment is already the
                            // active writer, so abandoning this fold would
                            // lose its manifest transition. Retrying cannot
                            // create capacity and a zero-delay test client can
                            // otherwise spin forever. Park the exact fold until
                            // truncate_before refreshes the directory and
                            // sends the rotation recheck above.
                            rotation.block_fold_on_capacity();
                            tracing::warn!(
                                "manifest directory is full; pending fold waits for truncation"
                            );
                        } else if matches!(error, Error::Poisoned | Error::Fenced(_)) {
                            metrics.spare_provisioning_failures.increment();
                            tracing::warn!(%error, "background rotation work was fenced");
                            state.fail_queued(&mut receiver, error, &metrics);
                            break 'engine;
                        } else {
                            metrics.spare_provisioning_failures.increment();
                            tracing::warn!(%error, "background rotation work failed; will retry");
                            if rotation.has_pending_fold() && writer.unregistered_spare_ready() {
                                rotation.arm_fold_retry(&client_config);
                            }
                        }
                    }
                    None => {
                        metrics.spare_provisioning_failures.increment();
                        tracing::warn!("spare provisioner stopped");
                        state.fail_queued(&mut receiver, Error::Closed, &metrics);
                        break 'engine;
                    }
                }
            }
            command = receiver.recv(), if !state.input_closed => {
                match command {
                    Some(command) => admit_command(
                        command,
                        &mut state.queue,
                        &mut state.next_admission,
                    ),
                    None => state.input_closed = true,
                }
            }
        }
    }
    writer.shutdown_background_tasks().await;
    rotation.shutdown_background_tasks().await;
    if let Some(response) = state.shutdown_response {
        let _ = response.send(());
        tracing::info!("WAL engine shut down");
    } else {
        tracing::warn!("WAL engine stopped without graceful shutdown");
    }
    metrics.queue_depth.set(0);
    metrics.rotation_state.set(0);
}

/// What the rotation wake arm of the engine's `select!` observed: the two
/// rotation states that wait on a future share one arm so only one `&mut
/// rotation` borrow exists across the `select!`.
enum RotationEvent {
    Sealed(Result<(), oneshot::error::RecvError>),
    RetryElapsed,
}

/// Rotation control state for one preregistered pending segment.
enum Rotation {
    /// Active segment within its advisory size budget.
    Idle,
    /// Budget crossed. Dispatch is fail-closed until a confirmed pending
    /// segment is available for the in-memory swap.
    Due,
    /// Admissions already route to the adopted spare; the swapped-out
    /// segment is draining toward its admitted end. The active successor was
    /// preregistered before the swap, so its acknowledgments are independent
    /// of the later background fold.
    Draining {
        swap: Box<PendingSwap>,
        fold_attempts: usize,
        retry: Option<Pin<Box<Sleep>>>,
        /// A fold rejected for directory capacity cannot make progress until
        /// application truncation removes retained entries. Keep the exact
        /// transition parked instead of retrying a permanent local predicate.
        fold_capacity_blocked: bool,
        /// The consumed pending segment itself crossed the rotation floor
        /// before fold/refill completed. With no second pending slot, dispatch
        /// pauses until the fold registers the refill.
        successor_due: bool,
    },
    /// The swapped-out segment's seal enforcement is in flight. The next
    /// swap would overwrite the manifest seal record that segment needs for
    /// recovery, so rotation holds until `enforced` resolves — at most one
    /// sealed segment ever lacks a finalized quorum, and it is always the
    /// manifest's current seal record.
    Sealing { enforced: oneshot::Receiver<()> },
    /// Live sealing and bounded idempotent reconstruction both failed: no
    /// further rotation until restart recovery enforces the segment named by
    /// the manifest's seal record.
    Disabled { due: bool },
}

impl Rotation {
    fn metric_value(&self) -> i64 {
        match self {
            Self::Idle => 0,
            Self::Due => 1,
            Self::Draining { .. } => 2,
            Self::Sealing { .. } => 3,
            Self::Disabled { .. } => 4,
        }
    }

    fn mark_due(&mut self, due: bool) {
        if !due {
            return;
        }
        match self {
            Rotation::Idle => *self = Rotation::Due,
            Rotation::Draining { successor_due, .. } => *successor_due = true,
            Rotation::Disabled { due: disabled_due } => *disabled_due = true,
            Rotation::Due | Rotation::Sealing { .. } => {}
        }
    }

    fn swap_ready(&self) -> bool {
        matches!(self, Rotation::Due)
    }

    fn fold_ready(&self, next_completion: u64) -> bool {
        matches!(
            self,
            Rotation::Draining {
                swap,
                retry: None,
                fold_capacity_blocked: false,
                ..
            }
                if next_completion > swap.end_record_index
        )
    }

    fn swap(&self) -> Option<&PendingSwap> {
        match self {
            Rotation::Draining { swap, .. } => Some(swap),
            _ => None,
        }
    }

    fn has_pending_fold(&self) -> bool {
        matches!(self, Rotation::Draining { .. })
    }

    fn retry_pending(&self) -> bool {
        matches!(self, Rotation::Draining { retry: Some(_), .. })
    }

    fn dispatch_paused(&self) -> bool {
        matches!(
            self,
            Rotation::Due
                | Rotation::Draining {
                    successor_due: true,
                    ..
                }
                | Rotation::Disabled { due: true }
        )
    }

    fn arm_fold_retry(&mut self, config: &crate::protocol::ClientConfig) {
        if let Rotation::Draining {
            fold_attempts,
            retry,
            ..
        } = self
        {
            let delay = crate::protocol::retry_delay(config, *fold_attempts);
            *fold_attempts = fold_attempts.saturating_add(1);
            *retry = Some(Box::pin(tokio::time::sleep(delay)));
        }
    }

    fn block_fold_on_capacity(&mut self) {
        if let Rotation::Draining {
            fold_capacity_blocked,
            ..
        } = self
        {
            *fold_capacity_blocked = true;
        }
    }

    fn release_fold_capacity_block(&mut self) {
        if let Rotation::Draining {
            fold_capacity_blocked,
            ..
        } = self
        {
            *fold_capacity_blocked = false;
        }
    }

    async fn shutdown_background_tasks(&mut self) {
        if let Rotation::Draining { swap, .. } = self {
            if let Some(writer) = &mut swap.writer {
                writer.shutdown_background_tasks().await;
            }
        }
    }
}

enum ProvisionOutcome {
    Registered(Box<RegisteredSpare>),
    Unregistered(String, crate::protocol::Writer),
    Folded(ManifestUpdate),
}

type ProvisionFuture = Pin<Box<dyn Future<Output = Result<ProvisionOutcome, Error>> + Send>>;

struct ProvisionAttempt {
    future: ProvisionFuture,
    replicas: Vec<Arc<dyn Replica>>,
}

/// Dedicated worker running spare-provisioning attempts one at a time. It
/// exists so the engine adopts spares through a channel — a `select!` wake
/// source — instead of polling a task handle whose completion could otherwise
/// arrive while no engine branch is waiting on it. Exits when the engine drops
/// the request sender.
async fn run_provisioner(
    mut requests: mpsc::Receiver<ProvisionAttempt>,
    results: mpsc::Sender<Result<ProvisionOutcome, Error>>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let attempt = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow_and_update() {
                    return;
                }
                continue;
            }
            attempt = requests.recv() => {
                let Some(attempt) = attempt else {
                    return;
                };
                attempt
            }
        };
        let ProvisionAttempt {
            mut future,
            replicas,
        } = attempt;
        let result = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow_and_update() {
                    None
                } else {
                    continue;
                }
            }
            result = &mut future => Some(result),
        };
        let Some(result) = result else {
            cancel_provision_attempt(future, replicas).await;
            return;
        };
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow_and_update() {
                    return;
                }
            }
            sent = results.send(result) => {
                if sent.is_err() {
                    return;
                }
            }
        }
    }
}

async fn cancel_provision_attempt(mut future: ProvisionFuture, replicas: Vec<Arc<dyn Replica>>) {
    if replicas.is_empty() {
        // A fold has no data-plane sessions to close. Drive its guarded
        // manifest CAS to a terminal result so shutdown does not return while
        // a submitted control-plane mutation is still unresolved.
        let _ = future.await;
        return;
    }
    let shutdown = join_all(replicas.iter().map(|replica| replica.shutdown()));
    let _ = tokio::join!(&mut future, shutdown);
}

async fn next_commit_update(batches: &mut VecDeque<CompletionBatch>) -> Result<(), Error> {
    // `pending_records` gates this call, but it is maintained independently
    // from the batch deque. Treat disagreement as poison so a protocol bug
    // shuts down this WAL without aborting the process.
    let batch = batches.front_mut().ok_or(Error::Poisoned)?;
    batch.commits.changed().await
}

fn observe_queue_depth(metrics: &Metrics, queue_capacity: usize, queue_slots: &Semaphore) {
    metrics
        .queue_depth
        .set_usize(queue_capacity.saturating_sub(queue_slots.available_permits()));
}

fn complete_append(append: AdmittedAppend, metrics: &Metrics) {
    metrics
        .append_commit_latency
        .record_duration(append.admitted_at.elapsed());
    let _ = append.completion.send(Ok(AppendReceipt {
        seqno: append.seqno,
    }));
    metrics.committed_records.increment();
    metrics.committed_bytes.add(append.payload_bytes as u64);
    metrics
        .committed_records_watermark
        .set_u64(append.seqno.record_index + 1);
}

fn admit_command(command: Command, queue: &mut VecDeque<Command>, next_admission: &mut u64) {
    match command {
        Command::Append(append) => {
            debug_assert_eq!(append.seqno.record_index, *next_admission);
            queue.push_back(Command::Append(append));
            *next_admission += 1;
        }
        Command::Shutdown { response } => {
            queue.push_back(Command::Shutdown { response });
        }
    }
}

fn fail_all(queue: &mut VecDeque<Command>, error: Error, metrics: &Metrics) {
    while let Some(command) = queue.pop_front() {
        match command {
            Command::Append(append) => fail_append(append, error.clone(), metrics),
            Command::Shutdown { response } => {
                let _ = response.send(());
            }
        }
    }
}

fn fail_completion_batches(
    batches: &mut VecDeque<CompletionBatch>,
    error: Error,
    metrics: &Metrics,
) {
    while let Some(mut batch) = batches.pop_front() {
        while let Some(append) = batch.appends.pop_front() {
            fail_append(append, error.clone(), metrics);
        }
    }
}

fn fail_append(append: AdmittedAppend, error: Error, metrics: &Metrics) {
    metrics.append_failures.increment();
    let _ = append.completion.send(Err(error));
}
