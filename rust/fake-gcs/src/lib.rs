use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{Stream, StreamExt};
use prost::Message;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Mutex};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Code, Request, Response, Status};

pub mod proto {
    #![allow(clippy::large_enum_variant)] // generated oneof variants
    tonic::include_proto!("google.storage.v2");
}

use proto::bidi_write_object_request::{Data, FirstMessage};
use proto::bidi_write_object_response::WriteStatus;
use proto::storage_server::{Storage, StorageServer};
use proto::{
    BidiWriteObjectRequest, BidiWriteObjectResponse, DeleteObjectRequest, Empty, GetObjectRequest,
    ListObjectsRequest, ListObjectsResponse, Object, ReadObjectRequest, ReadObjectResponse,
    Timestamp, UpdateObjectRequest,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Operation {
    Delete,
    Get,
    List,
    Read,
    Update,
    BidiWrite,
    BidiCreate,
    BidiTakeoverOpen,
    BidiResume,
    BidiAppendFlush,
    BidiFinalize,
    BidiGuardedReplace,
}

impl Operation {
    fn latency_domain(self) -> u64 {
        match self {
            Self::Delete => 0,
            Self::Get => 1,
            Self::List => 2,
            Self::Read => 3,
            Self::Update => 4,
            Self::BidiWrite => 5,
            Self::BidiCreate => 6,
            Self::BidiTakeoverOpen => 7,
            Self::BidiResume => 8,
            Self::BidiAppendFlush => 9,
            Self::BidiFinalize => 10,
            Self::BidiGuardedReplace => 11,
        }
    }
}

/// Deterministic latency range for one storage operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimulatedLatency {
    min: Duration,
    max: Duration,
}

impl SimulatedLatency {
    pub const fn fixed(delay: Duration) -> Self {
        Self {
            min: delay,
            max: delay,
        }
    }

    pub const fn between(min: Duration, max: Duration) -> Self {
        Self { min, max }
    }

    fn sample(self, seed: u64, operation: Operation, ordinal: u64) -> Duration {
        let min_ns = duration_nanos(self.min);
        let max_ns = duration_nanos(self.max);
        let (min_ns, max_ns) = if min_ns <= max_ns {
            (min_ns, max_ns)
        } else {
            (max_ns, min_ns)
        };
        let span = max_ns - min_ns;
        let sample = splitmix64(
            seed ^ operation
                .latency_domain()
                .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                ^ ordinal.wrapping_mul(0xbf58_476d_1ce4_e5b9),
        );
        let offset = if span == u64::MAX {
            sample
        } else {
            sample % (span + 1)
        };
        Duration::from_nanos(min_ns.saturating_add(offset))
    }
}

/// Persistent per-operation service latency.
///
/// Unlike `inject_delay`, these delays model normal service behavior and do
/// not count as observed faults. Each operation's samples depend only on the
/// profile seed and that operation's invocation ordinal.
#[derive(Clone, Debug, Default)]
pub struct LatencyProfile {
    seed: u64,
    operations: HashMap<Operation, SimulatedLatency>,
}

impl LatencyProfile {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            operations: HashMap::new(),
        }
    }

    pub fn with_operation(mut self, operation: Operation, latency: SimulatedLatency) -> Self {
        self.operations.insert(operation, latency);
        self
    }

    fn delay(&self, operation: Operation, ordinal: u64) -> Option<Duration> {
        self.operations
            .get(&operation)
            .copied()
            .map(|latency| latency.sample(self.seed, operation, ordinal))
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[derive(Clone, Default)]
pub struct FakeGcs {
    inner: Arc<Mutex<State>>,
    /// Wakes opens held by `inject_open_hold` when `release_open_holds` runs.
    hold_notify: Arc<tokio::sync::Notify>,
    /// Wakes flushes held by `inject_flush_hold` when `release_flush_holds`
    /// runs.
    flush_notify: Arc<tokio::sync::Notify>,
    /// Wakes finalization requests held by `inject_finalize_hold`.
    finalize_notify: Arc<tokio::sync::Notify>,
}

#[derive(Default)]
struct State {
    objects: HashMap<String, StoredObject>,
    next_generation: i64,
    next_stream_id: u64,
    crashed: bool,
    failures: HashMap<Operation, VecDeque<Code>>,
    response_losses: HashMap<Operation, VecDeque<Code>>,
    delays: HashMap<Operation, VecDeque<Duration>>,
    latency_profile: LatencyProfile,
    latency_counts: HashMap<Operation, u64>,
    redirects: HashMap<Operation, VecDeque<String>>,
    session_expiries: HashMap<Operation, u64>,
    stream_closes_after_response: HashMap<Operation, VecDeque<Code>>,
    mutation_throttles: HashMap<Operation, u64>,
    operation_counts: HashMap<Operation, u64>,
    operation_log: Vec<Operation>,
    observed_faults: u64,
    open_holds: u64,
    holds_generation: u64,
    /// Sessions with a stream id below this are parked at their next flush
    /// until `release_flush_holds`.
    flush_hold_below: Option<u64>,
    /// Flushes `permit_held_flushes` lets through the armed hold.
    flush_hold_passes: u64,
    finalize_hold: bool,
    partial_writes: VecDeque<usize>,
}

struct StoredObject {
    bucket: String,
    name: String,
    generation: i64,
    metageneration: i64,
    metadata: HashMap<String, String>,
    content_type: String,
    bytes: Vec<u8>,
    integrity_crc32c: u32,
    finalized: bool,
    active_stream: Option<u64>,
}

fn stream_handle(stream_id: u64) -> Vec<u8> {
    stream_id.to_be_bytes().to_vec()
}

fn sum_delay(left: Option<Duration>, right: Option<Duration>) -> Option<Duration> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left + right),
        (delay, None) | (None, delay) => delay,
    }
}

/// Result of opening an in-process bidi session via [`FakeGcs::sim_open`].
pub struct SimSessionOpen {
    /// The open response (persisted size / resource and write handle).
    pub response: proto::BidiWriteObjectResponse,
    /// `(append spec, stream id)` when an append session was opened (an
    /// appendable create, a takeover, or a handle resume).
    pub append: Option<(proto::AppendObjectSpec, u64)>,
}

pub struct RunningFake {
    pub endpoint: String,
    pub service: FakeGcs,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

/// Direct fake-service observation used by deterministic protocol tracing.
///
/// This bypasses RPC behavior deliberately: the production client still uses
/// tonic, while the simulator derives trace observations from the fake storage
/// state without importing client protocol internals.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectObservation {
    pub name: String,
    pub generation: i64,
    pub metageneration: i64,
    pub metadata: HashMap<String, String>,
    pub bytes: Vec<u8>,
    pub finalized: bool,
    pub reported_size: i64,
    pub crc32c: Option<u32>,
}

impl Drop for RunningFake {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl FakeGcs {
    pub fn with_latency(profile: LatencyProfile) -> Self {
        let state = State {
            latency_profile: profile,
            ..State::default()
        };
        Self {
            inner: Arc::new(Mutex::new(state)),
            ..Self::default()
        }
    }

    pub async fn start(self) -> std::io::Result<RunningFake> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let service = self.clone();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(StorageServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("fake GCS server");
        });
        Ok(RunningFake {
            endpoint: format!("http://{address}"),
            service: self,
            shutdown: Some(shutdown_tx),
            task: Some(task),
        })
    }

    /// Serve the fake over a caller-supplied incoming connection stream
    /// instead of a real TCP listener. The deterministic simulator feeds this
    /// a simulated-network accept loop; `start` keeps the real loopback path
    /// for integration tests, probes, and benches.
    pub async fn serve_with_incoming<I, IO, IE>(
        self,
        incoming: I,
    ) -> Result<(), tonic::transport::Error>
    where
        I: Stream<Item = Result<IO, IE>>,
        IO: tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + tonic::transport::server::Connected
            + Unpin
            + Send
            + 'static,
        IE: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        tonic::transport::Server::builder()
            .add_service(StorageServer::new(self))
            .serve_with_incoming(incoming)
            .await
    }

    pub async fn inject(&self, operation: Operation, code: Code) {
        self.inner
            .lock()
            .await
            .failures
            .entry(operation)
            .or_default()
            .push_back(code);
    }

    /// Remove every still-unconsumed queued fault for `operation`.
    pub async fn clear_injected_operation(&self, operation: Operation) -> usize {
        let mut state = self.inner.lock().await;
        let mut cleared = state
            .failures
            .remove(&operation)
            .map_or(0, |queue| queue.len());
        cleared += state
            .response_losses
            .remove(&operation)
            .map_or(0, |queue| queue.len());
        cleared += state
            .delays
            .remove(&operation)
            .map_or(0, |queue| queue.len());
        cleared += state
            .redirects
            .remove(&operation)
            .map_or(0, |queue| queue.len());
        cleared += state.session_expiries.remove(&operation).unwrap_or(0) as usize;
        cleared += state
            .stream_closes_after_response
            .remove(&operation)
            .map_or(0, |queue| queue.len());
        cleared += state.mutation_throttles.remove(&operation).unwrap_or(0) as usize;
        cleared
    }

    /// Apply the next request for `operation` normally, then fail the
    /// response with `code`: the ambiguous applied-then-lost outcome.
    /// Honored by `UpdateObject` and `DeleteObject`.
    pub async fn inject_response_lost(&self, operation: Operation, code: Code) {
        self.inner
            .lock()
            .await
            .response_losses
            .entry(operation)
            .or_default()
            .push_back(code);
    }

    pub async fn inject_delay(&self, operation: Operation, delay: Duration) {
        self.inner
            .lock()
            .await
            .delays
            .entry(operation)
            .or_default()
            .push_back(delay);
    }

    /// Redirect the next matching decoded bidi-write operation until the
    /// caller replays it with `routing_token`.
    pub async fn inject_redirect(&self, operation: Operation, routing_token: impl Into<String>) {
        self.inner
            .lock()
            .await
            .redirects
            .entry(operation)
            .or_default()
            .push_back(routing_token.into());
    }

    /// Revoke the append session addressed by the next matching decoded
    /// bidi-write operation, then fail that operation as an expired stream.
    pub async fn inject_session_expiry(&self, operation: Operation) {
        *self
            .inner
            .lock()
            .await
            .session_expiries
            .entry(operation)
            .or_default() += 1;
    }

    /// Apply the next matching bidi-write operation, emit its normal response,
    /// then close the stream with `code`. This models a persisted-size response
    /// coalesced with either a transient close or terminal writer fence.
    pub async fn inject_stream_close_after_response(&self, operation: Operation, code: Code) {
        self.inner
            .lock()
            .await
            .stream_closes_after_response
            .entry(operation)
            .or_default()
            .push_back(code);
    }

    /// Reject the next matching object mutation with RESOURCE_EXHAUSTED.
    ///
    /// The gate is released by consuming the one injected attempt, rather than
    /// by a clock, so retry behavior is deterministic under virtual time.
    pub async fn inject_mutation_throttle(&self, operation: Operation) {
        *self
            .inner
            .lock()
            .await
            .mutation_throttles
            .entry(operation)
            .or_default() += 1;
    }

    /// Number of injected faults that have reached an operation.
    pub async fn observed_fault_count(&self) -> u64 {
        self.inner.lock().await.observed_faults
    }

    /// Number of RPC attempts observed for one storage operation.
    pub async fn operation_count(&self, operation: Operation) -> u64 {
        self.inner
            .lock()
            .await
            .operation_counts
            .get(&operation)
            .copied()
            .unwrap_or(0)
    }

    /// Ordered decoded storage operations, excluding the generic bidi stream
    /// envelope that precedes every classified bidi request.
    pub async fn operation_log(&self) -> Vec<Operation> {
        self.inner.lock().await.operation_log.clone()
    }

    /// Reset operation counters without changing stored objects or failures.
    pub async fn reset_operation_counts(&self) {
        let mut state = self.inner.lock().await;
        state.operation_counts.clear();
        state.operation_log.clear();
        state.latency_counts.clear();
    }

    /// Hold the next appendable CREATE (a `WriteObjectSpec` with
    /// `appendable`) until `release_open_holds`. Session opens, resumes, and
    /// continuations are untouched: live opens are rate-limited object mutations
    /// and cost orders of magnitude more than a flush on an established
    /// session, and this is the only injection that preserves that asymmetry
    /// — a plain `BidiWrite` delay is consumed FIFO by whatever flush arrives
    /// first. An explicit release instead of a timer keeps traces
    /// deterministic: the open resumes at a point the harness chooses, not at
    /// a wall-clock race against concurrent appends.
    pub async fn inject_open_hold(&self) {
        self.inner.lock().await.open_holds += 1;
    }

    /// Release every open currently held by `inject_open_hold` and clear any
    /// unconsumed holds.
    pub async fn release_open_holds(&self) {
        let mut state = self.inner.lock().await;
        state.open_holds = 0;
        state.holds_generation += 1;
        drop(state);
        self.hold_notify.notify_waiters();
    }

    /// Park every flush — append continuations and resumed-session first
    /// messages — for sessions opened BEFORE this call, until
    /// `release_flush_holds`. Sessions opened afterwards (takeover fences,
    /// recovery write-backs, fresh provisioning) pass untouched, so a writer
    /// whose flushes are parked can be deposed and recovered while the hold
    /// stays armed. An explicit release keeps traces deterministic, and a
    /// release after a takeover leaves the held bytes unapplied: the parked
    /// flush fails against its revoked stream instead of mutating the object.
    pub async fn inject_flush_hold(&self) {
        let mut state = self.inner.lock().await;
        state.flush_hold_below = Some(state.next_stream_id + 1);
        state.flush_hold_passes = 0;
    }

    /// Let `passes` parked or arriving flushes through the armed hold,
    /// first come first served, leaving the hold armed for the rest.
    pub async fn permit_held_flushes(&self, passes: u64) {
        self.inner.lock().await.flush_hold_passes += passes;
        self.flush_notify.notify_waiters();
    }

    /// Release every flush currently parked by `inject_flush_hold`.
    pub async fn release_flush_holds(&self) {
        let mut state = self.inner.lock().await;
        state.flush_hold_below = None;
        state.flush_hold_passes = 0;
        drop(state);
        self.flush_notify.notify_waiters();
    }

    /// Park every finalization request, including requests on fresh recovery
    /// sessions, until `release_finalize_holds`.
    pub async fn inject_finalize_hold(&self) {
        self.inner.lock().await.finalize_hold = true;
    }

    /// Release every finalization request parked by `inject_finalize_hold`.
    pub async fn release_finalize_holds(&self) {
        self.inner.lock().await.finalize_hold = false;
        self.finalize_notify.notify_waiters();
    }

    async fn finalize_hold_gate(&self) {
        loop {
            let notified = self.finalize_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.inner.lock().await.finalize_hold {
                return;
            }
            notified.await;
        }
    }

    async fn flush_hold_gate(&self, stream_id: u64) {
        loop {
            let notified = self.flush_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.inner.lock().await;
                let held = state
                    .flush_hold_below
                    .is_some_and(|threshold| stream_id < threshold);
                if !held {
                    return;
                }
                if state.flush_hold_passes > 0 {
                    state.flush_hold_passes -= 1;
                    return;
                }
            }
            notified.await;
        }
    }

    /// Persist only this many bytes from the next append request, then return
    /// UNAVAILABLE before a response. This models an interrupted record tail.
    pub async fn inject_partial_write(&self, bytes: usize) {
        self.inner.lock().await.partial_writes.push_back(bytes);
    }

    pub async fn set_crashed(&self, crashed: bool) {
        self.inner.lock().await.crashed = crashed;
    }

    pub async fn corrupt_byte_for(&self, bucket: &str, object: &str, index: usize) -> bool {
        let mut state = self.inner.lock().await;
        let Some(stored) = state.objects.get_mut(&object_key(bucket, object)) else {
            return false;
        };
        let Some(byte) = stored.bytes.get_mut(index) else {
            return false;
        };
        *byte ^= 0xff;
        true
    }

    /// Replace one byte while updating the provider-owned object checksum.
    ///
    /// Unlike `corrupt_byte_for`, this models a valid same-size divergent
    /// object rather than undetected storage rot: reads succeed and metadata
    /// reports the checksum of the new bytes.
    pub async fn diverge_byte_for(&self, bucket: &str, object: &str, index: usize) -> bool {
        let mut state = self.inner.lock().await;
        let Some(stored) = state.objects.get_mut(&object_key(bucket, object)) else {
            return false;
        };
        let Some(byte) = stored.bytes.get_mut(index) else {
            return false;
        };
        *byte ^= 0xff;
        stored.integrity_crc32c = crc32c::crc32c(&stored.bytes);
        true
    }

    pub async fn raw_bytes_for(&self, bucket: &str, object: &str) -> Vec<u8> {
        self.inner
            .lock()
            .await
            .objects
            .get(&object_key(bucket, object))
            .map(|object| object.bytes.clone())
            .unwrap_or_default()
    }

    /// Return the size and finalization state exposed by `GetObject`.
    ///
    /// This is a deterministic-test observation hook. In particular, an open
    /// appendable object must report size zero even when its live stream has
    /// already persisted bytes.
    pub async fn reported_size_for(&self, bucket: &str, object: &str) -> Option<(i64, bool)> {
        self.inner
            .lock()
            .await
            .objects
            .get(&object_key(bucket, object))
            .map(|object| {
                let metadata = object.to_proto();
                (metadata.size, object.finalized)
            })
    }

    /// Observe every stored object under a bucket-local prefix.
    pub async fn observe_prefix(&self, bucket: &str, prefix: &str) -> Vec<ObjectObservation> {
        let state = self.inner.lock().await;
        let mut objects: Vec<_> = state
            .objects
            .values()
            .filter(|object| object.bucket == bucket && object.name.starts_with(prefix))
            .map(|object| {
                let metadata = object.to_proto();
                ObjectObservation {
                    name: object.name.clone(),
                    generation: object.generation,
                    metageneration: object.metageneration,
                    metadata: object.metadata.clone(),
                    bytes: object.bytes.clone(),
                    finalized: object.finalized,
                    reported_size: metadata.size,
                    crc32c: metadata
                        .checksums
                        .as_ref()
                        .and_then(|checksums| checksums.crc32c),
                }
            })
            .collect();
        objects.sort_by(|left, right| left.name.cmp(&right.name));
        objects
    }

    async fn resume_stream_id(&self, spec: &proto::AppendObjectSpec) -> Option<u64> {
        let state = self.inner.lock().await;
        let object = state.objects.get(&object_key(&spec.bucket, &spec.object))?;
        if object.generation != spec.generation {
            return None;
        }
        let stream_id = object.active_stream?;
        let handle = spec.write_handle.as_ref()?;
        (handle.handle == stream_handle(stream_id)).then_some(stream_id)
    }

    async fn append_spec_for_created(
        &self,
        bucket: String,
        object: String,
    ) -> Result<proto::AppendObjectSpec, Status> {
        let state = self.inner.lock().await;
        let stored = state
            .objects
            .get(&object_key(&bucket, &object))
            .ok_or_else(|| Status::not_found("created appendable object disappeared"))?;
        Ok(proto::AppendObjectSpec {
            bucket,
            object,
            generation: stored.generation,
            ..Default::default()
        })
    }

    async fn allocate_stream_id(&self) -> u64 {
        let mut state = self.inner.lock().await;
        state.next_stream_id += 1;
        state.next_stream_id
    }

    async fn before(&self, operation: Operation) -> Result<(), Status> {
        let delay = self.before_charge(operation).await?;
        Self::sleep_charged(delay).await;
        Ok(())
    }

    /// Fault-inject and compute the operation latency WITHOUT sleeping.
    ///
    /// `before` is exactly this followed by [`Self::sleep_charged`]. The
    /// in-process simulation transport uses the split so a fire-and-forget
    /// `lane_send` can charge the fault/latency immediately and defer the
    /// sleep to the durability observation, with no background reader task.
    async fn before_charge(
        &self,
        operation: Operation,
    ) -> Result<(Option<Duration>, Option<Duration>), Status> {
        let mut state = self.inner.lock().await;
        *state.operation_counts.entry(operation).or_default() += 1;
        if operation != Operation::BidiWrite {
            state.operation_log.push(operation);
        }
        if state.crashed {
            return Err(Status::unavailable("zonal node is crashed"));
        }
        if let Some(code) = state
            .failures
            .get_mut(&operation)
            .and_then(VecDeque::pop_front)
        {
            state.observed_faults += 1;
            return Err(Status::new(code, "injected fake GCS failure"));
        }
        let delay = state
            .delays
            .get_mut(&operation)
            .and_then(VecDeque::pop_front);
        if delay.is_some() {
            state.observed_faults += 1;
        }
        let ordinal = *state.latency_counts.entry(operation).or_default();
        *state.latency_counts.entry(operation).or_default() += 1;
        let service_delay = state.latency_profile.delay(operation, ordinal);
        Ok((delay, service_delay))
    }

    async fn sleep_charged(delay: (Option<Duration>, Option<Duration>)) {
        if let Some(delay) = delay.0 {
            tokio::time::sleep(delay).await;
        }
        if let Some(delay) = delay.1 {
            tokio::time::sleep(delay).await;
        }
    }

    async fn throttle_mutation(
        &self,
        operation: Operation,
        bucket: &str,
        object: &str,
    ) -> Result<(), Status> {
        let mut state = self.inner.lock().await;
        let Some(remaining) = state.mutation_throttles.get_mut(&operation) else {
            return Ok(());
        };
        if *remaining == 0 {
            return Ok(());
        }
        *remaining -= 1;
        state.observed_faults += 1;
        Err(Status::resource_exhausted(format!(
            "injected per-object mutation throttle for {}",
            object_key(bucket, object)
        )))
    }

    async fn before_bidi(
        &self,
        operation: Operation,
        routing_token: Option<&str>,
        object: Option<(&str, &str)>,
    ) -> Result<(), Status> {
        let delay = self
            .before_bidi_charge(operation, routing_token, object)
            .await?;
        Self::sleep_charged(delay).await;
        Ok(())
    }

    async fn before_bidi_charge(
        &self,
        operation: Operation,
        routing_token: Option<&str>,
        object: Option<(&str, &str)>,
    ) -> Result<(Option<Duration>, Option<Duration>), Status> {
        let (injected_delay, service_delay) = {
            let mut state = self.inner.lock().await;
            if operation != Operation::BidiWrite {
                *state.operation_counts.entry(operation).or_default() += 1;
                state.operation_log.push(operation);
            }
            if state.crashed {
                return Err(Status::unavailable("zonal node is crashed"));
            }

            let redirect_operation = if state
                .redirects
                .get(&operation)
                .is_some_and(|queue| !queue.is_empty())
            {
                Some(operation)
            } else if state
                .redirects
                .get(&Operation::BidiWrite)
                .is_some_and(|queue| !queue.is_empty())
            {
                Some(Operation::BidiWrite)
            } else {
                None
            };
            if let Some(redirect_operation) = redirect_operation {
                let token = state.redirects[&redirect_operation]
                    .front()
                    .expect("checked nonempty")
                    .clone();
                if routing_token == Some(token.as_str()) {
                    state
                        .redirects
                        .get_mut(&redirect_operation)
                        .expect("redirect queue exists")
                        .pop_front();
                } else {
                    state.observed_faults += 1;
                    return Err(redirect_status(token));
                }
            }

            let expiry_operation = if state
                .session_expiries
                .get(&operation)
                .is_some_and(|remaining| *remaining > 0)
            {
                Some(operation)
            } else if state
                .session_expiries
                .get(&Operation::BidiWrite)
                .is_some_and(|remaining| *remaining > 0)
            {
                Some(Operation::BidiWrite)
            } else {
                None
            };
            if let Some(expiry_operation) = expiry_operation {
                *state
                    .session_expiries
                    .get_mut(&expiry_operation)
                    .expect("expiry counter exists") -= 1;
                if let Some((bucket, object)) = object {
                    if let Some(stored) = state.objects.get_mut(&object_key(bucket, object)) {
                        stored.active_stream = None;
                    }
                }
                state.observed_faults += 1;
                return Err(Status::unavailable("injected append session expiry"));
            }

            let throttle_operation = if state
                .mutation_throttles
                .get(&operation)
                .is_some_and(|remaining| *remaining > 0)
            {
                Some(operation)
            } else if state
                .mutation_throttles
                .get(&Operation::BidiWrite)
                .is_some_and(|remaining| *remaining > 0)
            {
                Some(Operation::BidiWrite)
            } else {
                None
            };
            if let Some(throttle_operation) = throttle_operation {
                *state
                    .mutation_throttles
                    .get_mut(&throttle_operation)
                    .expect("throttle counter exists") -= 1;
                state.observed_faults += 1;
                return Err(Status::resource_exhausted(
                    "injected per-object mutation throttle",
                ));
            }

            if state
                .failures
                .get(&operation)
                .is_some_and(|queue| !queue.is_empty())
            {
                let code = state
                    .failures
                    .get_mut(&operation)
                    .and_then(VecDeque::pop_front)
                    .expect("failure queue is nonempty");
                state.observed_faults += 1;
                return Err(Status::new(code, "injected fake GCS failure"));
            }

            let delay = if state
                .delays
                .get(&operation)
                .is_some_and(|queue| !queue.is_empty())
            {
                state
                    .delays
                    .get_mut(&operation)
                    .and_then(VecDeque::pop_front)
            } else {
                None
            };
            if delay.is_some() {
                state.observed_faults += 1;
            }
            let ordinal = *state.latency_counts.entry(operation).or_default();
            *state.latency_counts.entry(operation).or_default() += 1;
            let service_delay = state.latency_profile.delay(operation, ordinal);
            (delay, service_delay)
        };
        Ok((injected_delay, service_delay))
    }

    // ----- In-process simulation transport API -------------------------
    //
    // These public methods let an in-memory `Replica` drive the fake directly,
    // with no spawned task, no h2/tonic, no TCP, and no background reader. They
    // reuse the exact byte/takeover/finalize/fault/latency/hold semantics of
    // the gRPC handler (`apply_bidi`, `apply_append_continuation`, `before`,
    // `before_bidi`, the hold gates), so a deterministic simulation transport
    // and the real gRPC transport agree on behavior.

    /// Open an in-process bidi session from one first message (appendable
    /// create, takeover open, or handle resume). Mirrors the open glue of the
    /// gRPC `bidi_write_object` handler without its response stream/spawn.
    pub async fn sim_open(
        &self,
        first: BidiWriteObjectRequest,
        routing_token: Option<&str>,
    ) -> Result<SimSessionOpen, Status> {
        self.before(Operation::BidiWrite).await?;
        let operation = classify_bidi_request(&first, false);
        let object = bidi_object(&first).map(|(b, o)| (b.to_string(), o.to_string()));
        self.before_bidi(
            operation,
            routing_token,
            object.as_ref().map(|(b, o)| (b.as_str(), o.as_str())),
        )
        .await?;

        let append_spec = match first.first_message.as_ref() {
            Some(FirstMessage::AppendObjectSpec(spec)) => Some(spec.clone()),
            _ => None,
        };
        let created_append = match first.first_message.as_ref() {
            Some(FirstMessage::WriteObjectSpec(spec)) if spec.appendable == Some(true) => spec
                .resource
                .as_ref()
                .map(|resource| (resource.bucket.clone(), resource.name.clone())),
            _ => None,
        };
        if created_append.is_some() {
            // Replicate the open-hold wait. The session is cancelled by drop,
            // so the streaming handler's tx.closed/incoming arms are not needed.
            let held_at = {
                let mut state = self.inner.lock().await;
                (state.open_holds > 0).then(|| {
                    state.open_holds -= 1;
                    state.holds_generation
                })
            };
            if let Some(generation) = held_at {
                loop {
                    let notified = self.hold_notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if self.inner.lock().await.holds_generation != generation {
                        break;
                    }
                    notified.await;
                }
            }
        }
        let opened = if let Some(spec) = append_spec.as_ref() {
            let stream_id = if spec.write_handle.is_some() {
                self.resume_stream_id(spec).await.ok_or_else(|| {
                    Status::failed_precondition("no active append session to resume")
                })?
            } else {
                self.allocate_stream_id().await
            };
            Some((spec.clone(), stream_id))
        } else {
            None
        };
        let create_stream_id = if created_append.is_some() {
            Some(self.allocate_stream_id().await)
        } else {
            None
        };
        let stream_id = opened
            .as_ref()
            .map(|(_, stream_id)| *stream_id)
            .or(create_stream_id);
        if let Some(stream_id) = stream_id {
            if operation == Operation::BidiFinalize {
                self.finalize_hold_gate().await;
            }
            self.flush_hold_gate(stream_id).await;
        }
        let response = self.apply_bidi(first, stream_id).await?;
        let append = if let Some((bucket, object)) = created_append {
            let spec = self.append_spec_for_created(bucket, object).await?;
            Some((spec, stream_id.expect("appendable create stream has an id")))
        } else {
            opened
        };
        self.close_stream_after_response(operation).await?;
        Ok(SimSessionOpen { response, append })
    }

    /// Apply one continuation (append or finalize) on an open in-process
    /// session, blocking for the operation's fault/latency. Used for the
    /// synchronous finalize path.
    pub async fn sim_continue(
        &self,
        spec: &proto::AppendObjectSpec,
        stream_id: u64,
        request: BidiWriteObjectRequest,
        routing_token: Option<&str>,
    ) -> Result<BidiWriteObjectResponse, Status> {
        // `sim_lane_apply` now sleeps the charged latency itself (before the
        // write) and returns any post-response stream close. A one-shot
        // continuation surfaces that close as an error, matching the prior
        // behaviour where the close propagated out of `sim_lane_apply`.
        let (response, close) = self
            .sim_lane_apply(spec, stream_id, request, routing_token)
            .await?;
        if let Some(status) = close {
            return Err(status);
        }
        Ok(response)
    }

    /// Apply one continuation synchronously, mirroring the gRPC bidi handler.
    /// The charged fault/latency is slept BEFORE the write mutates storage, and
    /// the post-response stream close (if injected) is returned alongside the
    /// applied response rather than discarding it. No background reader, no spawn.
    pub async fn sim_lane_apply(
        &self,
        spec: &proto::AppendObjectSpec,
        stream_id: u64,
        request: BidiWriteObjectRequest,
        routing_token: Option<&str>,
    ) -> Result<(BidiWriteObjectResponse, Option<Status>), Status> {
        let operation = classify_bidi_request(&request, true);
        let before_delay = self.before_charge(Operation::BidiWrite).await?;
        let bidi_delay = self
            .before_bidi_charge(operation, routing_token, Some((&spec.bucket, &spec.object)))
            .await?;
        // Match the gRPC handler: the charged latency elapses BEFORE the write
        // mutates storage, so a delayed flush is not visible to a concurrent
        // recovery until its latency passes.
        Self::sleep_charged((
            sum_delay(before_delay.0, bidi_delay.0),
            sum_delay(before_delay.1, bidi_delay.1),
        ))
        .await;
        if operation == Operation::BidiFinalize {
            self.finalize_hold_gate().await;
        }
        self.flush_hold_gate(stream_id).await;
        let response = self
            .apply_append_continuation(spec, stream_id, request)
            .await?;
        // The gRPC fake emits the response, THEN injects the stream close.
        // Surface the applied response together with the close error so the
        // lane reports durable progress alongside the fence, exactly as the
        // gRPC client folds persisted_size + error in one observation.
        let close = self.close_stream_after_response(operation).await.err();
        Ok((response, close))
    }

    /// Read the full object bytes for the in-process snapshot path, charging
    /// the `Read` fault/latency. Returns `DATA_LOSS`-style errors as the gRPC
    /// read path would by reusing `before`.
    pub async fn sim_read_bytes(
        &self,
        bucket: &str,
        object: &str,
        generation: i64,
    ) -> Result<Vec<u8>, Status> {
        self.before(Operation::Read).await?;
        let state = self.inner.lock().await;
        let stored = state
            .objects
            .get(&object_key(bucket, object))
            .ok_or_else(|| Status::not_found("object"))?;
        // Generation-bind the read like `read_object`'s `if_generation_match`,
        // so a replacement between the caller's metadata lookup and this read
        // cannot return a mixed snapshot (old metadata/generation, new bytes).
        if stored.generation != generation {
            return Err(Status::failed_precondition(
                "object generation changed before read",
            ));
        }
        // Mirror the content read's rot check: detected corruption fails the
        // read with DATA_LOSS (metadata stats still succeed), exactly as
        // `read_object` does, so recovery never trusts corrupted bytes.
        if crc32c::crc32c(&stored.bytes) != stored.integrity_crc32c {
            return Err(Status::data_loss("stored object CRC32C mismatch"));
        }
        Ok(stored.bytes.clone())
    }

    async fn close_stream_after_response(&self, operation: Operation) -> Result<(), Status> {
        let mut state = self.inner.lock().await;
        let Some(code) = state
            .stream_closes_after_response
            .get_mut(&operation)
            .and_then(VecDeque::pop_front)
        else {
            return Ok(());
        };
        state.observed_faults += 1;
        Err(Status::new(
            code,
            "injected stream close after persisted response",
        ))
    }

    /// In-process one-shot non-appendable create (the manifest register path),
    /// sharing `write_object_apply` with the streaming `write_object` so the
    /// simulation transport and the gRPC service agree. Charges the same fault
    /// and latency through `before`.
    pub async fn sim_write_object(
        &self,
        first: proto::WriteObjectRequest,
    ) -> Result<proto::WriteObjectResponse, Status> {
        self.before(Operation::BidiWrite).await?;
        let Some(proto::write_object_request::FirstMessage::WriteObjectSpec(spec)) =
            first.first_message
        else {
            return Err(Status::invalid_argument(
                "write_object requires a WriteObjectSpec first message",
            ));
        };
        if spec.appendable == Some(true) {
            return Err(Status::invalid_argument(
                "appendable objects require BidiWriteObject on a zonal bucket",
            ));
        }
        let resource = spec
            .resource
            .ok_or_else(|| Status::invalid_argument("resource"))?;
        let mut bytes = Vec::new();
        if let Some(proto::write_object_request::Data::ChecksummedData(data)) = &first.data {
            bytes.extend_from_slice(&data.content);
        }
        self.write_object_apply(
            resource,
            bytes,
            first.finish_write,
            spec.if_generation_match,
        )
        .await
    }

    async fn write_object_apply(
        &self,
        resource: proto::Object,
        bytes: Vec<u8>,
        finished: bool,
        if_generation_match: Option<i64>,
    ) -> Result<proto::WriteObjectResponse, Status> {
        if !finished {
            return Err(Status::invalid_argument(
                "one-shot write must set finish_write",
            ));
        }
        check_metadata_size(&resource.metadata)?;
        let mut state = self.inner.lock().await;
        let key = object_key(&resource.bucket, &resource.name);
        if state.objects.contains_key(&key) {
            if if_generation_match == Some(0) {
                return Err(Status::already_exists("object exists"));
            }
            return Err(Status::failed_precondition("generation mismatch"));
        }
        state.next_generation += 1;
        let stored = StoredObject {
            bucket: resource.bucket,
            name: resource.name,
            generation: state.next_generation,
            metageneration: 1,
            metadata: resource.metadata,
            content_type: resource.content_type,
            integrity_crc32c: crc32c::crc32c(&bytes),
            bytes,
            finalized: true,
            active_stream: None,
        };
        let response = proto::WriteObjectResponse {
            write_status: Some(proto::write_object_response::WriteStatus::Resource(
                stored.to_proto(),
            )),
        };
        state.objects.insert(key, stored);
        Ok(response)
    }
}

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

struct TaskResponseStream<T> {
    inner: ReceiverStream<Result<T, Status>>,
    task: tokio::task::JoinHandle<()>,
}

impl<T> Stream for TaskResponseStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().inner).poll_next(cx)
    }
}

impl<T> Drop for TaskResponseStream<T> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tonic::async_trait]
impl Storage for FakeGcs {
    type ReadObjectStream = ResponseStream<ReadObjectResponse>;
    type BidiWriteObjectStream = ResponseStream<BidiWriteObjectResponse>;

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<Empty>, Status> {
        self.before(Operation::Delete).await?;
        let request = request.into_inner();
        self.throttle_mutation(Operation::Delete, &request.bucket, &request.object)
            .await?;
        let mut state = self.inner.lock().await;
        let key = object_key(&request.bucket, &request.object);
        let object = state
            .objects
            .get(&key)
            .ok_or_else(|| Status::not_found("object"))?;
        if request.generation != 0 && request.generation != object.generation {
            return Err(Status::failed_precondition("generation mismatch"));
        }
        if request
            .if_generation_match
            .is_some_and(|value| value != object.generation)
            || request
                .if_metageneration_match
                .is_some_and(|value| value != object.metageneration)
        {
            return Err(Status::failed_precondition("delete precondition mismatch"));
        }
        state.objects.remove(&key);
        if let Some(code) = state
            .response_losses
            .get_mut(&Operation::Delete)
            .and_then(VecDeque::pop_front)
        {
            state.observed_faults += 1;
            return Err(Status::new(code, "injected response loss after apply"));
        }
        Ok(Response::new(Empty {}))
    }

    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<Object>, Status> {
        self.before(Operation::Get).await?;
        let request = request.into_inner();
        let state = self.inner.lock().await;
        let object = state
            .objects
            .get(&object_key(&request.bucket, &request.object))
            .ok_or_else(|| Status::not_found("object"))?;
        check_name(object, &request.bucket, &request.object)?;
        // Metadata stats are content-blind, exactly like real GCS: rot is
        // surfaced by ReadObject, never by GetObject.
        if let Some(generation) = request.if_generation_match {
            if generation != object.generation {
                return Err(Status::failed_precondition("generation mismatch"));
            }
        }
        if let Some(metageneration) = request.if_metageneration_match {
            if metageneration != object.metageneration {
                return Err(Status::failed_precondition("metageneration mismatch"));
            }
        }
        Ok(Response::new(object.to_proto()))
    }

    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        self.before(Operation::List).await?;
        let request = request.into_inner();
        let state = self.inner.lock().await;
        let mut objects: Vec<_> = state
            .objects
            .values()
            .filter(|object| {
                object.bucket == request.parent && object.name.starts_with(&request.prefix)
            })
            .map(StoredObject::to_proto)
            .collect();
        objects.sort_by(|left, right| left.name.cmp(&right.name));
        let start = request.page_token.parse::<usize>().unwrap_or(0);
        let page_size = if request.page_size <= 0 {
            1000
        } else {
            request.page_size.min(1000) as usize
        };
        let end = objects.len().min(start.saturating_add(page_size));
        let page = objects.get(start..end).unwrap_or_default().to_vec();
        let next_page_token = if end < objects.len() {
            end.to_string()
        } else {
            String::new()
        };
        Ok(Response::new(ListObjectsResponse {
            objects: page,
            prefixes: Vec::new(),
            next_page_token,
        }))
    }

    async fn read_object(
        &self,
        request: Request<ReadObjectRequest>,
    ) -> Result<Response<Self::ReadObjectStream>, Status> {
        self.before(Operation::Read).await?;
        let request = request.into_inner();
        let state = self.inner.lock().await;
        let object = state
            .objects
            .get(&object_key(&request.bucket, &request.object))
            .ok_or_else(|| Status::not_found("object"))?;
        check_name(object, &request.bucket, &request.object)?;
        if crc32c::crc32c(&object.bytes) != object.integrity_crc32c {
            // detected rot fails the content read; metadata stats still work
            return Err(Status::data_loss("stored object CRC32C mismatch"));
        }
        if request.generation != 0 && request.generation != object.generation {
            return Err(Status::failed_precondition("generation mismatch"));
        }
        if request
            .if_generation_match
            .is_some_and(|value| value != object.generation)
        {
            return Err(Status::failed_precondition("generation mismatch"));
        }
        if request
            .if_metageneration_match
            .is_some_and(|value| value != object.metageneration)
        {
            return Err(Status::failed_precondition("metageneration mismatch"));
        }
        let start = usize::try_from(request.read_offset.max(0)).unwrap_or(usize::MAX);
        if start > object.bytes.len() {
            return Err(Status::out_of_range("read offset"));
        }
        let limit = if request.read_limit <= 0 {
            object.bytes.len() - start
        } else {
            usize::try_from(request.read_limit).unwrap_or(usize::MAX)
        };
        let end = object.bytes.len().min(start.saturating_add(limit));
        let response = ReadObjectResponse {
            checksummed_data: Some(proto::ChecksummedData {
                content: object.bytes[start..end].to_vec(),
                crc32c: Some(crc32c::crc32c(&object.bytes[start..end])),
            }),
            metadata: Some(object.to_proto()),
        };
        Ok(Response::new(Box::pin(tokio_stream::iter([Ok(response)]))))
    }

    async fn update_object(
        &self,
        request: Request<UpdateObjectRequest>,
    ) -> Result<Response<Object>, Status> {
        self.before(Operation::Update).await?;
        let request = request.into_inner();
        let update = request
            .object
            .ok_or_else(|| Status::invalid_argument("object"))?;
        self.throttle_mutation(Operation::Update, &update.bucket, &update.name)
            .await?;
        check_metadata_size(&update.metadata)?;
        let mut state = self.inner.lock().await;
        let key = object_key(&update.bucket, &update.name);
        let object = state
            .objects
            .get_mut(&key)
            .ok_or_else(|| Status::not_found("object"))?;
        check_name(object, &update.bucket, &update.name)?;
        if request
            .if_generation_match
            .is_some_and(|value| value != object.generation)
            || request
                .if_metageneration_match
                .is_some_and(|value| value != object.metageneration)
        {
            return Err(Status::failed_precondition("CAS mismatch"));
        }
        object.metadata = update.metadata;
        object.metageneration += 1;
        let proto = object.to_proto();
        if let Some(code) = state
            .response_losses
            .get_mut(&Operation::Update)
            .and_then(VecDeque::pop_front)
        {
            // applied, response lost: indistinguishable to the caller from
            // a CAS that never happened
            state.observed_faults += 1;
            return Err(Status::new(code, "injected response loss after apply"));
        }
        Ok(Response::new(proto))
    }

    async fn write_object(
        &self,
        request: Request<tonic::Streaming<proto::WriteObjectRequest>>,
    ) -> Result<Response<proto::WriteObjectResponse>, Status> {
        // One-shot, non-appendable create: the path the manifest register
        // uses, since regional buckets reject appendable opens.
        self.before(Operation::BidiWrite).await?;
        let mut incoming = request.into_inner();
        let first = incoming
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty write stream"))??;
        let Some(proto::write_object_request::FirstMessage::WriteObjectSpec(spec)) =
            first.first_message
        else {
            return Err(Status::invalid_argument(
                "write_object requires a WriteObjectSpec first message",
            ));
        };
        if spec.appendable == Some(true) {
            return Err(Status::invalid_argument(
                "appendable objects require BidiWriteObject on a zonal bucket",
            ));
        }
        let resource = spec
            .resource
            .ok_or_else(|| Status::invalid_argument("resource"))?;
        let mut bytes = Vec::new();
        if let Some(proto::write_object_request::Data::ChecksummedData(data)) = &first.data {
            bytes.extend_from_slice(&data.content);
        }
        let mut finished = first.finish_write;
        while let Some(next) = incoming.next().await {
            let next = next?;
            if let Some(proto::write_object_request::Data::ChecksummedData(data)) = &next.data {
                bytes.extend_from_slice(&data.content);
            }
            finished |= next.finish_write;
        }
        self.write_object_apply(resource, bytes, finished, spec.if_generation_match)
            .await
            .map(Response::new)
    }

    async fn bidi_write_object(
        &self,
        request: Request<tonic::Streaming<BidiWriteObjectRequest>>,
    ) -> Result<Response<Self::BidiWriteObjectStream>, Status> {
        self.before(Operation::BidiWrite).await?;
        let routing_token = request_routing_token(request.metadata());
        let mut incoming = request.into_inner();
        let first = incoming
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty bidi stream"))??;
        let operation = classify_bidi_request(&first, false);
        self.before_bidi(operation, routing_token.as_deref(), bidi_object(&first))
            .await?;
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let service = self.clone();
        let task = tokio::spawn(async move {
            let mut opened_append = None;
            let result = async {
                let append_spec = match first.first_message.as_ref() {
                    Some(FirstMessage::AppendObjectSpec(spec)) => Some(spec.clone()),
                    _ => None,
                };
                let created_append = match first.first_message.as_ref() {
                    Some(FirstMessage::WriteObjectSpec(spec)) if spec.appendable == Some(true) => {
                        spec.resource
                            .as_ref()
                            .map(|resource| (resource.bucket.clone(), resource.name.clone()))
                    }
                    _ => None,
                };
                // An appendable CREATE is the expensive, rate-limited
                // mutation in the live service and the one await inside
                // spare provisioning: the only path `inject_open_hold`
                // holds back. Session opens (takeover fences), handle
                // resumes, and continuations stay fast so sealing and the
                // established lanes keep moving while a hold is pending.
                let new_open = created_append.is_some();
                if new_open {
                    let held_at = {
                        let mut state = service.inner.lock().await;
                        (state.open_holds > 0).then(|| {
                            state.open_holds -= 1;
                            state.holds_generation
                        })
                    };
                    if let Some(generation) = held_at {
                        loop {
                            let notified = service.hold_notify.notified();
                            tokio::pin!(notified);
                            notified.as_mut().enable();
                            if service.inner.lock().await.holds_generation != generation {
                                break;
                            }
                            tokio::select! {
                                _ = notified => {}
                                _ = tx.closed() => {
                                    return Err(Status::cancelled(
                                        "appendable create response stream closed",
                                    ));
                                }
                                request = incoming.next() => {
                                    return match request {
                                        None => Err(Status::cancelled(
                                            "appendable create request stream closed",
                                        )),
                                        Some(Err(status)) => Err(status),
                                        Some(Ok(_)) => Err(Status::invalid_argument(
                                            "appendable create continued before its open response",
                                        )),
                                    };
                                }
                            }
                        }
                    }
                }
                if let Some(spec) = append_spec.as_ref() {
                    let stream_id = if spec.write_handle.is_some() {
                        // a handle resumes the existing session; it is not a
                        // takeover and must not revoke the active stream
                        match service.resume_stream_id(spec).await {
                            Some(id) => id,
                            None => {
                                return Err(Status::failed_precondition(
                                    "no active append session to resume",
                                ))
                            }
                        }
                    } else {
                        service.allocate_stream_id().await
                    };
                    opened_append = Some((spec.clone(), stream_id));
                }
                let create_stream_id = if created_append.is_some() {
                    Some(service.allocate_stream_id().await)
                } else {
                    None
                };
                let stream_id = opened_append
                    .as_ref()
                    .map(|(_, stream_id)| *stream_id)
                    .or(create_stream_id);
                // a resumed session keeps its original stream id, so a flush
                // hold armed after the open parks it here; a takeover open
                // allocates a fresh id and passes
                if let Some(stream_id) = stream_id {
                    if operation == Operation::BidiFinalize {
                        service.finalize_hold_gate().await;
                    }
                    service.flush_hold_gate(stream_id).await;
                }
                let first_response = service.apply_bidi(first, stream_id).await?;
                if let Some((bucket, object)) = created_append {
                    let spec = service.append_spec_for_created(bucket, object).await?;
                    opened_append =
                        Some((spec, stream_id.expect("appendable create stream has an id")));
                }
                tx.send(Ok(first_response))
                    .await
                    .map_err(|_| Status::cancelled("client closed response stream"))?;
                service.close_stream_after_response(operation).await?;
                if let Some((spec, stream_id)) = opened_append.as_ref() {
                    while let Some(request) = incoming.next().await {
                        let request = request?;
                        let operation = classify_bidi_request(&request, true);
                        service.before(Operation::BidiWrite).await?;
                        service
                            .before_bidi(
                                operation,
                                routing_token.as_deref(),
                                Some((&spec.bucket, &spec.object)),
                            )
                            .await?;
                        if operation == Operation::BidiFinalize {
                            service.finalize_hold_gate().await;
                        }
                        service.flush_hold_gate(*stream_id).await;
                        let response = service
                            .apply_append_continuation(spec, *stream_id, request)
                            .await?;
                        tx.send(Ok(response))
                            .await
                            .map_err(|_| Status::cancelled("client closed response stream"))?;
                        service.close_stream_after_response(operation).await?;
                    }
                } else if incoming.next().await.is_some() {
                    return Err(Status::invalid_argument(
                        "non-appendable write accepts one request in the fake",
                    ));
                }
                Ok(())
            }
            .await;
            // The append session deliberately survives the stream
            // disconnect: the write handle resumes it, and only another
            // takeover open (or finalization) revokes it — matching the
            // live service, whose per-object mutation rate limit makes
            // handle resume the only viable steady-state append path.
            let _ = &opened_append;
            if let Err(status) = result {
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(Box::pin(TaskResponseStream {
            inner: ReceiverStream::new(rx),
            task,
        })))
    }
}

impl FakeGcs {
    async fn apply_bidi(
        &self,
        request: BidiWriteObjectRequest,
        stream_id: Option<u64>,
    ) -> Result<BidiWriteObjectResponse, Status> {
        let mut state = self.inner.lock().await;
        match request.first_message.as_ref() {
            Some(FirstMessage::WriteObjectSpec(spec)) => {
                let is_create = spec.if_generation_match == Some(0);
                let resource = spec
                    .resource
                    .clone()
                    .ok_or_else(|| Status::invalid_argument("resource"))?;
                let key = object_key(&resource.bucket, &resource.name);
                match state.objects.get(&key) {
                    Some(_) if spec.if_generation_match == Some(0) => {
                        return Err(Status::already_exists("object"));
                    }
                    Some(existing)
                        if spec
                            .if_generation_match
                            .is_some_and(|generation| generation != existing.generation)
                            || spec.if_metageneration_match.is_some_and(|metageneration| {
                                metageneration != existing.metageneration
                            }) =>
                    {
                        return Err(Status::failed_precondition(
                            "replacement precondition mismatch",
                        ));
                    }
                    None if spec.if_generation_match != Some(0) => {
                        return Err(Status::failed_precondition(
                            "replacement object does not exist",
                        ));
                    }
                    _ => {}
                }
                check_metadata_size(&resource.metadata)?;
                state.next_generation += 1;
                let mut stored = StoredObject {
                    bucket: resource.bucket,
                    name: resource.name,
                    generation: state.next_generation,
                    metageneration: 1,
                    metadata: resource.metadata,
                    content_type: resource.content_type,
                    bytes: Vec::new(),
                    integrity_crc32c: crc32c::crc32c(&[]),
                    finalized: false,
                    active_stream: None,
                };
                append_data(&mut stored, &request)?;
                if request.finish_write {
                    stored.finalized = true;
                } else {
                    // an appendable create IS an append-session open: the
                    // returned write handle resumes this session
                    stored.active_stream = Some(
                        stream_id.ok_or_else(|| Status::internal("append stream id missing"))?,
                    );
                }
                let write_handle = stored.active_stream.map(stream_handle);
                let response = if stored.finalized {
                    WriteStatus::Resource(stored.to_proto())
                } else if is_create {
                    // Real GCS identifies a newly created appendable object
                    // with an unfinalized resource. The client must not treat
                    // this opening resource as the later finish response.
                    WriteStatus::Resource(stored.to_session_proto())
                } else {
                    WriteStatus::PersistedSize(stored.bytes.len() as i64)
                };
                state.objects.insert(key, stored);
                Ok(BidiWriteObjectResponse {
                    write_status: Some(response),
                    write_handle: write_handle.map(|handle| proto::BidiWriteHandle { handle }),
                })
            }
            Some(FirstMessage::AppendObjectSpec(spec)) => {
                let key = object_key(&spec.bucket, &spec.object);
                let partial_write = state.partial_writes.pop_front();
                if partial_write.is_some() {
                    state.observed_faults += 1;
                }
                let object = state
                    .objects
                    .get_mut(&key)
                    .ok_or_else(|| Status::not_found("object"))?;
                check_name(object, &spec.bucket, &spec.object)?;
                if spec.generation != 0 && spec.generation != object.generation {
                    return Err(Status::failed_precondition("generation mismatch"));
                }
                if spec.write_handle.is_none()
                    && spec
                        .if_metageneration_match
                        .is_some_and(|value| value != object.metageneration)
                {
                    return Err(Status::failed_precondition("metageneration mismatch"));
                }
                if object.finalized {
                    return Err(Status::failed_precondition(
                        "The object has already been finalized.",
                    ));
                }
                if spec.write_handle.is_none() {
                    object.active_stream = Some(
                        stream_id.ok_or_else(|| Status::internal("append stream id missing"))?,
                    );
                }
                if let Some(bytes) = partial_write {
                    append_data_prefix(object, &request, bytes)?;
                    return Err(Status::unavailable(
                        "injected interruption after a partial append",
                    ));
                }
                append_data(object, &request)?;
                if request.finish_write {
                    object.finalized = true;
                    object.active_stream = None;
                    return Ok(BidiWriteObjectResponse {
                        write_status: Some(WriteStatus::Resource(object.to_proto())),
                        write_handle: None,
                    });
                }
                let write_status = if request.data.is_none() {
                    WriteStatus::Resource(object.to_session_proto())
                } else {
                    WriteStatus::PersistedSize(object.bytes.len() as i64)
                };
                Ok(BidiWriteObjectResponse {
                    write_status: Some(write_status),
                    write_handle: Some(proto::BidiWriteHandle {
                        handle: stream_handle(
                            stream_id
                                .ok_or_else(|| Status::internal("append stream id missing"))?,
                        ),
                    }),
                })
            }
            _ => Err(Status::invalid_argument("unsupported first message")),
        }
    }

    async fn apply_append_continuation(
        &self,
        spec: &proto::AppendObjectSpec,
        stream_id: u64,
        request: BidiWriteObjectRequest,
    ) -> Result<BidiWriteObjectResponse, Status> {
        if request.first_message.is_some() {
            return Err(Status::invalid_argument(
                "first_message is only valid on the first stream request",
            ));
        }
        let mut state = self.inner.lock().await;
        let partial_write = state.partial_writes.pop_front();
        if partial_write.is_some() {
            state.observed_faults += 1;
        }
        let object = state
            .objects
            .get_mut(&object_key(&spec.bucket, &spec.object))
            .ok_or_else(|| Status::not_found("object"))?;
        if object.active_stream != Some(stream_id) {
            return Err(Status::failed_precondition(
                "A different writer has become the exclusive writer of this object.",
            ));
        }
        if spec.generation != 0 && spec.generation != object.generation {
            return Err(Status::failed_precondition("generation mismatch"));
        }
        if object.finalized {
            return Err(Status::failed_precondition(
                "The object has already been finalized.",
            ));
        }
        if let Some(bytes) = partial_write {
            append_data_prefix(object, &request, bytes)?;
            return Err(Status::unavailable(
                "injected interruption after a partial append",
            ));
        }
        append_data(object, &request)?;
        if request.finish_write {
            object.finalized = true;
            object.active_stream = None;
            return Ok(BidiWriteObjectResponse {
                write_status: Some(WriteStatus::Resource(object.to_proto())),
                write_handle: None,
            });
        }
        Ok(BidiWriteObjectResponse {
            write_status: Some(WriteStatus::PersistedSize(object.bytes.len() as i64)),
            write_handle: Some(proto::BidiWriteHandle {
                handle: stream_handle(stream_id),
            }),
        })
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct RichStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

fn redirect_status(routing_token: String) -> Status {
    let message = "injected zonal write redirect".to_string();
    let redirect = proto::BidiWriteObjectRedirectedError {
        routing_token: Some(routing_token),
        write_handle: None,
        generation: None,
    };
    let rich = RichStatus {
        code: 10,
        message: message.clone(),
        details: vec![prost_types::Any {
            type_url: "type.googleapis.com/google.storage.v2.BidiWriteObjectRedirectedError".into(),
            value: redirect.encode_to_vec(),
        }],
    };
    Status::with_details(Code::Aborted, message, rich.encode_to_vec().into())
}

fn request_routing_token(metadata: &tonic::metadata::MetadataMap) -> Option<String> {
    metadata
        .get("x-goog-request-params")
        .and_then(|value| value.to_str().ok())
        .and_then(|params| {
            params
                .split('&')
                .find_map(|parameter| parameter.strip_prefix("routing_token=").map(str::to_string))
        })
}

fn classify_bidi_request(request: &BidiWriteObjectRequest, continuation: bool) -> Operation {
    if request.finish_write {
        return Operation::BidiFinalize;
    }
    if continuation {
        return Operation::BidiAppendFlush;
    }
    match request.first_message.as_ref() {
        Some(FirstMessage::WriteObjectSpec(spec)) if spec.if_generation_match == Some(0) => {
            Operation::BidiCreate
        }
        Some(FirstMessage::WriteObjectSpec(_)) => Operation::BidiGuardedReplace,
        Some(FirstMessage::AppendObjectSpec(spec)) if spec.write_handle.is_some() => {
            Operation::BidiResume
        }
        Some(FirstMessage::AppendObjectSpec(_)) => Operation::BidiTakeoverOpen,
        _ => Operation::BidiWrite,
    }
}

fn bidi_object(request: &BidiWriteObjectRequest) -> Option<(&str, &str)> {
    match request.first_message.as_ref()? {
        FirstMessage::WriteObjectSpec(spec) => {
            let resource = spec.resource.as_ref()?;
            Some((&resource.bucket, &resource.name))
        }
        FirstMessage::AppendObjectSpec(spec) => Some((&spec.bucket, &spec.object)),
        FirstMessage::UploadId(_) => None,
    }
}

fn append_data(object: &mut StoredObject, request: &BidiWriteObjectRequest) -> Result<(), Status> {
    append_data_prefix(object, request, usize::MAX)
}

fn append_data_prefix(
    object: &mut StoredObject,
    request: &BidiWriteObjectRequest,
    max_bytes: usize,
) -> Result<(), Status> {
    let offset = usize::try_from(request.write_offset)
        .map_err(|_| Status::out_of_range("negative write offset"))?;
    if offset > object.bytes.len() {
        return Err(Status::out_of_range("write offset past persisted size"));
    }
    let Some(Data::ChecksummedData(data)) = &request.data else {
        return Ok(());
    };
    if data
        .crc32c
        .is_some_and(|crc| crc != crc32c::crc32c(&data.content))
    {
        return Err(Status::data_loss("CRC32C mismatch"));
    }
    if offset < object.bytes.len() {
        let end = offset.saturating_add(data.content.len());
        if end <= object.bytes.len() && object.bytes[offset..end] == data.content {
            return Ok(());
        }
        return Err(Status::failed_precondition(
            "non-idempotent lower-offset retry",
        ));
    }
    let count = data.content.len().min(max_bytes);
    object.bytes.extend_from_slice(&data.content[..count]);
    object.integrity_crc32c = crc32c::crc32c(&object.bytes);
    Ok(())
}

fn check_name(object: &StoredObject, bucket: &str, name: &str) -> Result<(), Status> {
    if object.bucket == bucket && object.name == name {
        Ok(())
    } else {
        Err(Status::not_found("object"))
    }
}

fn object_key(bucket: &str, object: &str) -> String {
    format!("{bucket}\0{object}")
}

/// Real GCS caps an object's total custom metadata — keys plus values — at
/// 8 KiB. Enforcing it here keeps the manifest's segment-directory budget
/// honest in simulation instead of only in production.
fn check_metadata_size(metadata: &std::collections::HashMap<String, String>) -> Result<(), Status> {
    let total: usize = metadata
        .iter()
        .map(|(key, value)| key.len() + value.len())
        .sum();
    if total > 8192 {
        return Err(Status::invalid_argument(
            "custom metadata exceeds the 8 KiB limit",
        ));
    }
    Ok(())
}

impl StoredObject {
    fn to_session_proto(&self) -> Object {
        let mut object = self.to_proto();
        object.size = self.bytes.len() as i64;
        object
    }

    fn to_proto(&self) -> Object {
        Object {
            name: self.name.clone(),
            bucket: self.bucket.clone(),
            generation: self.generation,
            metageneration: self.metageneration,
            // Live GCS does not expose flushed appendable bytes through
            // GetObject.size until the object is finalized.
            size: if self.finalized {
                self.bytes.len() as i64
            } else {
                0
            },
            content_type: self.content_type.clone(),
            checksums: Some(proto::ObjectChecksums {
                crc32c: Some(self.integrity_crc32c),
                md5_hash: Vec::new(),
            }),
            metadata: self.metadata.clone(),
            finalize_time: self.finalized.then_some(Timestamp {
                seconds: 1,
                nanos: 0,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::bidi_write_object_request::FirstMessage;
    use proto::storage_client::StorageClient;

    #[test]
    fn latency_profile_is_seeded_per_operation_and_ordinal() {
        let latency = SimulatedLatency::between(Duration::from_millis(1), Duration::from_millis(2));
        let profile = LatencyProfile::new(393)
            .with_operation(Operation::BidiAppendFlush, latency)
            .with_operation(Operation::Update, latency);

        let appends: Vec<_> = (0..16)
            .map(|ordinal| profile.delay(Operation::BidiAppendFlush, ordinal).unwrap())
            .collect();
        let replay: Vec<_> = (0..16)
            .map(|ordinal| profile.delay(Operation::BidiAppendFlush, ordinal).unwrap())
            .collect();
        let updates: Vec<_> = (0..16)
            .map(|ordinal| profile.delay(Operation::Update, ordinal).unwrap())
            .collect();

        assert_eq!(appends, replay);
        assert_ne!(appends, updates);
        assert!(appends.iter().all(|delay| latency.min <= *delay));
        assert!(appends.iter().all(|delay| *delay <= latency.max));
        assert_eq!(profile.delay(Operation::Get, 0), None);
    }

    fn create_request(bucket: &str, object: &str) -> BidiWriteObjectRequest {
        BidiWriteObjectRequest {
            first_message: Some(FirstMessage::WriteObjectSpec(proto::WriteObjectSpec {
                resource: Some(Object {
                    bucket: bucket.into(),
                    name: object.into(),
                    ..Default::default()
                }),
                if_generation_match: Some(0),
                appendable: Some(true),
                ..Default::default()
            })),
            flush: true,
            state_lookup: true,
            ..Default::default()
        }
    }

    fn with_routing_token<T>(value: T, bucket: &str, token: Option<&str>) -> Request<T> {
        let mut request = Request::new(value);
        let params = token.map_or_else(
            || format!("bucket={bucket}"),
            |token| format!("bucket={bucket}&routing_token={token}"),
        );
        request
            .metadata_mut()
            .insert("x-goog-request-params", params.parse().unwrap());
        request
    }

    fn object() -> StoredObject {
        StoredObject {
            bucket: "bucket".into(),
            name: "object".into(),
            generation: 1,
            metageneration: 1,
            metadata: HashMap::new(),
            content_type: String::new(),
            bytes: Vec::new(),
            integrity_crc32c: crc32c::crc32c(&[]),
            finalized: false,
            active_stream: None,
        }
    }

    fn request(offset: i64, bytes: &[u8], crc: u32) -> BidiWriteObjectRequest {
        BidiWriteObjectRequest {
            write_offset: offset,
            data: Some(Data::ChecksummedData(proto::ChecksummedData {
                content: bytes.to_vec(),
                crc32c: Some(crc),
            })),
            ..Default::default()
        }
    }

    fn append_request(
        bucket: &str,
        object: &str,
        generation: i64,
        metageneration: i64,
        offset: i64,
        bytes: &[u8],
    ) -> BidiWriteObjectRequest {
        BidiWriteObjectRequest {
            first_message: Some(FirstMessage::AppendObjectSpec(proto::AppendObjectSpec {
                bucket: bucket.into(),
                object: object.into(),
                generation,
                if_metageneration_match: Some(metageneration),
                write_handle: None,
                ..Default::default()
            })),
            write_offset: offset,
            data: Some(Data::ChecksummedData(proto::ChecksummedData {
                content: bytes.to_vec(),
                crc32c: Some(crc32c::crc32c(bytes)),
            })),
            flush: true,
            state_lookup: true,
            ..Default::default()
        }
    }

    async fn create_appendable(
        client: &mut StorageClient<tonic::transport::Channel>,
        bucket: &str,
        object: &str,
    ) {
        let request = create_request(bucket, object);
        let mut responses = client
            .bidi_write_object(tokio_stream::iter([request]))
            .await
            .unwrap()
            .into_inner();
        responses.message().await.unwrap().unwrap();
    }

    #[test]
    fn semantic_bidi_keys_cover_first_messages_and_continuations() {
        let create = create_request("bucket", "object");
        assert_eq!(classify_bidi_request(&create, false), Operation::BidiCreate);

        let mut replace = create;
        let Some(FirstMessage::WriteObjectSpec(spec)) = replace.first_message.as_mut() else {
            unreachable!();
        };
        spec.if_generation_match = Some(7);
        assert_eq!(
            classify_bidi_request(&replace, false),
            Operation::BidiGuardedReplace
        );

        let mut takeover = append_request("bucket", "object", 7, 3, 0, b"x");
        assert_eq!(
            classify_bidi_request(&takeover, false),
            Operation::BidiTakeoverOpen
        );
        let Some(FirstMessage::AppendObjectSpec(spec)) = takeover.first_message.as_mut() else {
            unreachable!();
        };
        spec.write_handle = Some(proto::BidiWriteHandle {
            handle: stream_handle(9),
        });
        assert_eq!(
            classify_bidi_request(&takeover, false),
            Operation::BidiResume
        );

        let continuation = request(0, b"x", crc32c::crc32c(b"x"));
        assert_eq!(
            classify_bidi_request(&continuation, true),
            Operation::BidiAppendFlush
        );
        let mut finalize = continuation;
        finalize.finish_write = true;
        assert_eq!(
            classify_bidi_request(&finalize, true),
            Operation::BidiFinalize
        );
    }

    #[tokio::test]
    async fn redirect_requires_the_routing_token_before_replay() {
        let server = FakeGcs::default().start().await.unwrap();
        let mut client = StorageClient::connect(server.endpoint.clone())
            .await
            .unwrap();
        let bucket = "projects/_/buckets/zone-0";
        let token = "redirect-token";
        server
            .service
            .inject_redirect(Operation::BidiCreate, token)
            .await;
        let create = create_request(bucket, "redirected");

        let status = client
            .bidi_write_object(with_routing_token(
                tokio_stream::iter([create.clone()]),
                bucket,
                None,
            ))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::Aborted);
        let rich = RichStatus::decode(status.details()).unwrap();
        let redirect =
            proto::BidiWriteObjectRedirectedError::decode(rich.details[0].value.as_slice())
                .unwrap();
        assert_eq!(redirect.routing_token.as_deref(), Some(token));

        let mut replayed = client
            .bidi_write_object(with_routing_token(
                tokio_stream::iter([create]),
                bucket,
                Some(token),
            ))
            .await
            .unwrap()
            .into_inner();
        replayed.message().await.unwrap().unwrap();
        assert_eq!(
            server.service.operation_count(Operation::BidiCreate).await,
            2
        );
        assert_eq!(server.service.observed_fault_count().await, 1);
    }

    #[tokio::test]
    async fn injected_session_expiry_revokes_the_handle() {
        let server = FakeGcs::default().start().await.unwrap();
        let mut client = StorageClient::connect(server.endpoint.clone())
            .await
            .unwrap();
        let bucket = "projects/_/buckets/zone-0";
        let name = "expired";
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(create_request(bucket, name)).await.unwrap();
        let mut responses = client
            .bidi_write_object(ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        let opened = responses.message().await.unwrap().unwrap();
        let handle = opened.write_handle.unwrap();
        let object = client
            .get_object(GetObjectRequest {
                bucket: bucket.into(),
                object: name.into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();

        server
            .service
            .inject_session_expiry(Operation::BidiAppendFlush)
            .await;
        tx.send(request(0, b"x", crc32c::crc32c(b"x")))
            .await
            .unwrap();
        assert_eq!(
            responses.message().await.unwrap_err().code(),
            Code::Unavailable
        );

        let mut resume = append_request(
            bucket,
            name,
            object.generation,
            object.metageneration,
            0,
            b"x",
        );
        let Some(FirstMessage::AppendObjectSpec(spec)) = resume.first_message.as_mut() else {
            unreachable!();
        };
        spec.write_handle = Some(handle);
        let mut resumed = client
            .bidi_write_object(tokio_stream::iter([resume]))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            resumed.message().await.unwrap_err().code(),
            Code::FailedPrecondition
        );
    }

    #[tokio::test]
    async fn mutation_throttle_is_one_attempt_and_object_scoped() {
        let server = FakeGcs::default().start().await.unwrap();
        let mut client = StorageClient::connect(server.endpoint.clone())
            .await
            .unwrap();
        let bucket = "projects/_/buckets/zone-0";
        let name = "throttled";
        create_appendable(&mut client, bucket, name).await;
        let object = client
            .get_object(GetObjectRequest {
                bucket: bucket.into(),
                object: name.into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let update = UpdateObjectRequest {
            object: Some(Object {
                bucket: bucket.into(),
                name: name.into(),
                metadata: HashMap::from([("test".into(), "value".into())]),
                ..Default::default()
            }),
            if_generation_match: Some(object.generation),
            if_metageneration_match: Some(object.metageneration),
            ..Default::default()
        };
        server
            .service
            .inject_mutation_throttle(Operation::Update)
            .await;
        assert_eq!(
            client
                .update_object(update.clone())
                .await
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );
        let updated = client.update_object(update).await.unwrap().into_inner();
        assert_eq!(updated.metageneration, object.metageneration + 1);
    }

    #[tokio::test]
    async fn delete_can_apply_then_lose_its_response() {
        let server = FakeGcs::default().start().await.unwrap();
        let mut client = StorageClient::connect(server.endpoint.clone())
            .await
            .unwrap();
        let bucket = "projects/_/buckets/zone-0";
        let name = "delete-loss";
        create_appendable(&mut client, bucket, name).await;
        let object = client
            .get_object(GetObjectRequest {
                bucket: bucket.into(),
                object: name.into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let delete = DeleteObjectRequest {
            bucket: bucket.into(),
            object: name.into(),
            generation: object.generation,
            if_generation_match: Some(object.generation),
            ..Default::default()
        };
        server
            .service
            .inject_response_lost(Operation::Delete, Code::Unavailable)
            .await;
        assert_eq!(
            client
                .delete_object(delete.clone())
                .await
                .unwrap_err()
                .code(),
            Code::Unavailable
        );
        assert_eq!(
            client.delete_object(delete).await.unwrap_err().code(),
            Code::NotFound
        );
    }

    #[tokio::test]
    async fn appendable_create_stream_accepts_continuations_and_finish() {
        let server = FakeGcs::default().start().await.unwrap();
        let mut client = StorageClient::connect(server.endpoint.clone())
            .await
            .unwrap();
        let bucket = "projects/_/buckets/zone-0";
        let name = "create-stream";
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(BidiWriteObjectRequest {
            first_message: Some(FirstMessage::WriteObjectSpec(proto::WriteObjectSpec {
                resource: Some(Object {
                    bucket: bucket.into(),
                    name: name.into(),
                    ..Default::default()
                }),
                if_generation_match: Some(0),
                appendable: Some(true),
                ..Default::default()
            })),
            flush: true,
            state_lookup: true,
            ..Default::default()
        })
        .await
        .unwrap();
        let mut responses = client
            .bidi_write_object(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        let opened = responses.message().await.unwrap().unwrap();
        let Some(WriteStatus::Resource(resource)) = opened.write_status else {
            panic!("appendable create response omitted resource");
        };
        assert_eq!(resource.size, 0);
        assert!(resource.finalize_time.is_none());

        tx.send(BidiWriteObjectRequest {
            write_offset: 0,
            data: Some(Data::ChecksummedData(proto::ChecksummedData {
                content: b"abc".to_vec(),
                crc32c: Some(crc32c::crc32c(b"abc")),
            })),
            flush: true,
            state_lookup: true,
            ..Default::default()
        })
        .await
        .unwrap();
        assert!(matches!(
            responses.message().await.unwrap().unwrap().write_status,
            Some(WriteStatus::PersistedSize(3))
        ));

        tx.send(BidiWriteObjectRequest {
            write_offset: 3,
            finish_write: true,
            ..Default::default()
        })
        .await
        .unwrap();
        let finalized = responses.message().await.unwrap().unwrap();
        let Some(WriteStatus::Resource(resource)) = finalized.write_status else {
            panic!("finish response omitted resource");
        };
        assert_eq!(resource.size, 3);
        assert!(resource.finalize_time.is_some());
        assert_eq!(
            resource.checksums.and_then(|checksums| checksums.crc32c),
            Some(crc32c::crc32c(b"abc"))
        );

        let observed = client
            .get_object(GetObjectRequest {
                bucket: bucket.into(),
                object: name.into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            observed.checksums.and_then(|checksums| checksums.crc32c),
            Some(crc32c::crc32c(b"abc"))
        );
    }

    #[test]
    fn rejects_bad_crc() {
        let mut object = object();
        let error = append_data(&mut object, &request(0, b"abc", 7)).unwrap_err();
        assert_eq!(error.code(), Code::DataLoss);
        assert!(object.bytes.is_empty());
    }

    #[test]
    fn lower_offset_retry_must_match() {
        let mut object = object();
        append_data(&mut object, &request(0, b"abc", crc32c::crc32c(b"abc"))).unwrap();
        append_data(&mut object, &request(0, b"abc", crc32c::crc32c(b"abc"))).unwrap();
        let error =
            append_data(&mut object, &request(0, b"xyz", crc32c::crc32c(b"xyz"))).unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn metadata_cas_does_not_revoke_but_fresh_open_takes_over() {
        let server = FakeGcs::default().start().await.unwrap();
        let mut client = StorageClient::connect(server.endpoint.clone())
            .await
            .unwrap();
        let bucket = "projects/_/buckets/zone-0";
        let name = "takeover";
        create_appendable(&mut client, bucket, name).await;
        let object = client
            .get_object(GetObjectRequest {
                bucket: bucket.into(),
                object: name.into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();

        let (old_tx, old_rx) = tokio::sync::mpsc::channel(4);
        old_tx
            .send(append_request(
                bucket,
                name,
                object.generation,
                object.metageneration,
                0,
                b"a",
            ))
            .await
            .unwrap();
        let mut old_stream = client
            .bidi_write_object(ReceiverStream::new(old_rx))
            .await
            .unwrap()
            .into_inner();
        old_stream.message().await.unwrap().unwrap();

        let updated = client
            .update_object(UpdateObjectRequest {
                object: Some(Object {
                    bucket: bucket.into(),
                    name: name.into(),
                    generation: object.generation,
                    metadata: HashMap::from([("test_tag".into(), "new".into())]),
                    ..Default::default()
                }),
                if_generation_match: Some(object.generation),
                if_metageneration_match: Some(object.metageneration),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();

        old_tx
            .send(BidiWriteObjectRequest {
                write_offset: 1,
                data: Some(Data::ChecksummedData(proto::ChecksummedData {
                    content: b"b".to_vec(),
                    crc32c: Some(crc32c::crc32c(b"b")),
                })),
                flush: true,
                state_lookup: true,
                ..Default::default()
            })
            .await
            .unwrap();
        old_stream.message().await.unwrap().unwrap();

        let mut replacement = client
            .bidi_write_object(tokio_stream::iter([append_request(
                bucket,
                name,
                object.generation,
                updated.metageneration,
                2,
                b"c",
            )]))
            .await
            .unwrap()
            .into_inner();
        replacement.message().await.unwrap().unwrap();

        old_tx
            .send(BidiWriteObjectRequest {
                write_offset: 3,
                data: Some(Data::ChecksummedData(proto::ChecksummedData {
                    content: b"d".to_vec(),
                    crc32c: Some(crc32c::crc32c(b"d")),
                })),
                flush: true,
                state_lookup: true,
                ..Default::default()
            })
            .await
            .unwrap();
        let error = old_stream.message().await.unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert_eq!(
            error.message(),
            "A different writer has become the exclusive writer of this object."
        );

        let object = client
            .get_object(GetObjectRequest {
                bucket: bucket.into(),
                object: name.into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(object.size, 0);
        assert_eq!(server.service.raw_bytes_for(bucket, name).await, b"abc");
    }

    #[tokio::test]
    async fn metageneration_is_checked_only_on_a_handle_free_open() {
        let server = FakeGcs::default().start().await.unwrap();
        let mut client = StorageClient::connect(server.endpoint.clone())
            .await
            .unwrap();
        let bucket = "projects/_/buckets/zone-0";
        let name = "write-handle";
        create_appendable(&mut client, bucket, name).await;
        let object = client
            .get_object(GetObjectRequest {
                bucket: bucket.into(),
                object: name.into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let stale = object.metageneration + 1;

        let mut fresh = client
            .bidi_write_object(tokio_stream::iter([append_request(
                bucket,
                name,
                object.generation,
                stale,
                0,
                b"x",
            )]))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            fresh.message().await.unwrap_err().code(),
            Code::FailedPrecondition
        );

        let mut resumed = append_request(bucket, name, object.generation, stale, 0, b"y");
        let Some(FirstMessage::AppendObjectSpec(spec)) = resumed.first_message.as_mut() else {
            unreachable!();
        };
        let handle = {
            let state = server.service.inner.lock().await;
            let stored = state.objects.get(&object_key(bucket, name)).unwrap();
            stream_handle(stored.active_stream.unwrap())
        };
        spec.write_handle = Some(proto::BidiWriteHandle { handle });
        let mut responses = client
            .bidi_write_object(tokio_stream::iter([resumed]))
            .await
            .unwrap()
            .into_inner();
        responses.message().await.unwrap().unwrap();
        assert_eq!(server.service.raw_bytes_for(bucket, name).await, b"y");
    }

    #[tokio::test]
    async fn finalized_object_has_a_distinct_terminal_fence_and_accepts_metadata_cas() {
        let server = FakeGcs::default().start().await.unwrap();
        let mut client = StorageClient::connect(server.endpoint.clone())
            .await
            .unwrap();
        let bucket = "projects/_/buckets/zone-0";
        let name = "finalized";
        create_appendable(&mut client, bucket, name).await;
        let open = client
            .get_object(GetObjectRequest {
                bucket: bucket.into(),
                object: name.into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(open.size, 0);

        let mut finalize = append_request(
            bucket,
            name,
            open.generation,
            open.metageneration,
            0,
            b"sealed",
        );
        finalize.finish_write = true;
        let mut responses = client
            .bidi_write_object(tokio_stream::iter([finalize]))
            .await
            .unwrap()
            .into_inner();
        responses.message().await.unwrap().unwrap();

        let finalized = client
            .get_object(GetObjectRequest {
                bucket: bucket.into(),
                object: name.into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(finalized.size, 6);
        let updated = client
            .update_object(UpdateObjectRequest {
                object: Some(Object {
                    bucket: bucket.into(),
                    name: name.into(),
                    generation: finalized.generation,
                    metadata: HashMap::from([("chorus.probe".into(), "updated".into())]),
                    ..Default::default()
                }),
                if_generation_match: Some(finalized.generation),
                if_metageneration_match: Some(finalized.metageneration),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(updated.metageneration, finalized.metageneration + 1);

        let mut rejected = client
            .bidi_write_object(tokio_stream::iter([append_request(
                bucket,
                name,
                updated.generation,
                updated.metageneration,
                6,
                b"x",
            )]))
            .await
            .unwrap()
            .into_inner();
        let error = rejected.message().await.unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert_eq!(error.message(), "The object has already been finalized.");
    }

    #[tokio::test]
    async fn listing_is_immediate_lexical_and_paginated() {
        let server = FakeGcs::default().start().await.unwrap();
        let mut client = StorageClient::connect(server.endpoint.clone())
            .await
            .unwrap();
        let bucket = "projects/_/buckets/zone-0";
        create_appendable(&mut client, bucket, "wal/segments/00000000000000000010").await;
        create_appendable(&mut client, bucket, "wal/segments/00000000000000000002").await;

        let first = client
            .list_objects(ListObjectsRequest {
                parent: bucket.into(),
                prefix: "wal/segments/".into(),
                page_size: 1,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(first.objects[0].name, "wal/segments/00000000000000000002");
        assert!(!first.next_page_token.is_empty());

        let second = client
            .list_objects(ListObjectsRequest {
                parent: bucket.into(),
                prefix: "wal/segments/".into(),
                page_size: 1,
                page_token: first.next_page_token,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(second.objects[0].name, "wal/segments/00000000000000000010");
        assert!(second.next_page_token.is_empty());
    }
}
