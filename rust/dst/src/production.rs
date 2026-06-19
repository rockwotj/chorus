use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::rc::Rc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use chorus_client::{
    AppendCompletion, AppendReceipt, ClientConfig, CounterFn, GaugeFn, HistogramFn,
    MetricsRecorder, NoopMetricsRecorder, Recovery, ReplicaFactory, SegmentedVolume,
    UpDownCounterFn, WalEngineConfig, WalHandle, WalRecord, WalSeqNo,
};
// The lane-stall recheck scenario keeps the real gRPC-over-turmoil transport
// (it exercises stream-close behavior the in-memory transport does not model),
// so these gRPC-serving helpers are test-only.
#[cfg(test)]
use chorus_client::GrpcReplicaFactory;
use chorus_fake_gcs::{FakeGcs, LatencyProfile, ObjectObservation, Operation, SimulatedLatency};

use crate::sim_transport::InMemoryReplicaFactory;
use futures::future::join_all;
use futures::TryStreamExt;
use rand::prelude::IndexedRandom;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sha2::{Digest, Sha256};
use tonic::Code;

use crate::{trace_digest, validate_trace_structure, SimulationReport, TraceEvent};

const BUCKET_PREFIX: &str = "projects/_/buckets/zone-";
const PHASE_COUNT: usize = 16;
const BOUNDED_WAIT_TICKS: usize = 100_000;
const SETTLING_MARGIN_TICKS: usize = 64;

fn phase_deck(seed: u64, rng: &mut ChaCha8Rng) -> [u8; PHASE_COUNT] {
    let mut phases = std::array::from_fn(|phase| phase as u8);
    if seed != 0 {
        phases.shuffle(rng);
    }
    phases
}

/// Port every simulated fake GCS host serves gRPC on.
#[cfg(test)]
const SIM_GRPC_PORT: u16 = 9999;

/// One virtual tick — also the fixed simulated network latency. Polling
/// helpers sleep this long per probe so host timers and message delivery
/// actually advance between probes (a busy `yield_now` loop would keep the
/// simulated runtime from ever going idle, freezing virtual time).
const SIM_TICK: Duration = Duration::from_millis(1);

fn zone_latency_profile(seed: u64, zone: usize) -> LatencyProfile {
    let fast = SimulatedLatency::fixed(Duration::from_millis(2));
    let append = SimulatedLatency::between(Duration::from_millis(1), Duration::from_millis(2));
    let mutation = SimulatedLatency::between(Duration::from_millis(50), Duration::from_millis(95));
    LatencyProfile::new(seed ^ (zone as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))
        .with_operation(Operation::Delete, fast)
        .with_operation(Operation::Get, fast)
        .with_operation(Operation::List, fast)
        .with_operation(Operation::Read, fast)
        .with_operation(Operation::Update, mutation)
        .with_operation(
            Operation::BidiCreate,
            SimulatedLatency::fixed(Duration::from_millis(50)),
        )
        .with_operation(Operation::BidiTakeoverOpen, fast)
        .with_operation(Operation::BidiResume, fast)
        .with_operation(Operation::BidiAppendFlush, append)
        .with_operation(Operation::BidiFinalize, fast)
        .with_operation(Operation::BidiGuardedReplace, mutation)
}

fn manifest_latency_profile(seed: u64) -> LatencyProfile {
    let fast = SimulatedLatency::fixed(Duration::from_millis(2));
    let mutation = SimulatedLatency::between(Duration::from_millis(50), Duration::from_millis(95));
    LatencyProfile::new(seed ^ 0xd6e8_feb8_6659_fd93)
        .with_operation(Operation::Delete, fast)
        .with_operation(Operation::Get, fast)
        .with_operation(Operation::List, fast)
        .with_operation(Operation::Read, fast)
        .with_operation(Operation::Update, mutation)
        .with_operation(
            Operation::BidiWrite,
            SimulatedLatency::fixed(Duration::from_millis(50)),
        )
}

fn production_engine_config() -> WalEngineConfig {
    WalEngineConfig {
        queue_capacity: 4096,
        max_record_bytes: 16 * 1024,
        pipeline_window_records: 4,
        max_inflight_bytes: 64 * 1024,
        max_replica_lag_bytes: 64 * 1024,
        lane_stall_timeout: WalEngineConfig::default().lane_stall_timeout,
        max_segment_bytes: 700,
        max_active_segment_bytes: WalEngineConfig::default().max_active_segment_bytes,
        repair_interval: None,
        shutdown_timeout: Duration::from_secs(60),
    }
}

async fn wait_for_predicate<T, F, Fut>(description: &str, mut predicate: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    for _ in 0..BOUNDED_WAIT_TICKS {
        tokio::time::sleep(SIM_TICK).await;
        if let Some(value) = predicate().await? {
            return Ok(value);
        }
    }
    bail!("timed out after {BOUNDED_WAIT_TICKS} virtual ticks waiting for {description}")
}

async fn wait_for_settled_predicate<T, F, Fut>(description: &str, predicate: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    let value = wait_for_predicate(description, predicate).await?;
    advance_virtual_ticks(SETTLING_MARGIN_TICKS).await;
    Ok(value)
}

async fn advance_virtual_ticks(ticks: usize) {
    for _ in 0..ticks {
        tokio::time::sleep(SIM_TICK).await;
    }
}

/// Cap on one seed's virtual lifetime. Generous: a run that hits this is a
/// deadlocked run, and virtual time is nearly free.
const SIM_DURATION: Duration = Duration::from_secs(7 * 24 * 3600);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FaultTarget {
    Zone(usize),
    Manifest,
}

impl FaultTarget {
    fn trace_zone(self) -> Option<usize> {
        match self {
            Self::Zone(zone) => Some(zone),
            Self::Manifest => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum BackgroundFault {
    Transient {
        target: FaultTarget,
        operation: Operation,
        code: Code,
        attempts: u64,
    },
    ResponseLoss {
        target: FaultTarget,
        operation: Operation,
        code: Code,
    },
    Delay {
        target: FaultTarget,
        operation: Operation,
        ticks: u32,
    },
    Redirect {
        zone: usize,
        operation: Operation,
        routing_token: String,
    },
    SessionExpiry {
        zone: usize,
        operation: Operation,
    },
    MutationThrottle {
        target: FaultTarget,
        operation: Operation,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScheduledBackgroundFault {
    opportunity: u64,
    fault: BackgroundFault,
}

fn random_bidi_operation(rng: &mut ChaCha8Rng) -> Operation {
    *[
        Operation::BidiCreate,
        Operation::BidiTakeoverOpen,
        Operation::BidiResume,
        Operation::BidiAppendFlush,
        Operation::BidiFinalize,
        Operation::BidiGuardedReplace,
    ]
    .choose(rng)
    .expect("semantic operation list is nonempty")
}

fn random_open_operation(rng: &mut ChaCha8Rng) -> Operation {
    *[
        Operation::BidiCreate,
        Operation::BidiTakeoverOpen,
        Operation::BidiResume,
        Operation::BidiGuardedReplace,
    ]
    .choose(rng)
    .expect("open operation list is nonempty")
}

fn random_mutation_operation(rng: &mut ChaCha8Rng) -> Operation {
    *[
        Operation::Update,
        Operation::Delete,
        Operation::BidiCreate,
        Operation::BidiTakeoverOpen,
        Operation::BidiFinalize,
        Operation::BidiGuardedReplace,
    ]
    .choose(rng)
    .expect("mutation operation list is nonempty")
}

fn random_general_operation(rng: &mut ChaCha8Rng) -> Operation {
    *[
        Operation::Delete,
        Operation::Get,
        Operation::List,
        Operation::Read,
        Operation::Update,
        Operation::BidiWrite,
    ]
    .choose(rng)
    .expect("operation list is nonempty")
}

fn target_for(operation: Operation, rng: &mut ChaCha8Rng) -> FaultTarget {
    match operation {
        Operation::Update | Operation::BidiWrite if rng.random_bool(0.25) => FaultTarget::Manifest,
        _ => FaultTarget::Zone(rng.random_range(0..3)),
    }
}

fn random_transient_code(rng: &mut ChaCha8Rng) -> Code {
    if rng.random_bool(0.5) {
        Code::Unavailable
    } else {
        Code::DeadlineExceeded
    }
}

fn background_fault_budget(seed: u64, rng: &mut ChaCha8Rng) -> VecDeque<ScheduledBackgroundFault> {
    let burst_operation = random_bidi_operation(rng);
    let general_operation = random_general_operation(rng);
    let delay_operation = random_general_operation(rng);
    let redirect_operation = random_open_operation(rng);
    let expiry_operation = *[
        Operation::BidiAppendFlush,
        Operation::BidiResume,
        Operation::BidiFinalize,
    ]
    .choose(rng)
    .expect("expiry operation list is nonempty");
    let throttle_operation = random_mutation_operation(rng);
    let mut faults = vec![
        BackgroundFault::Transient {
            target: FaultTarget::Zone(rng.random_range(0..3)),
            operation: burst_operation,
            code: random_transient_code(rng),
            attempts: rng.random_range(1..=4),
        },
        BackgroundFault::Transient {
            target: target_for(general_operation, rng),
            operation: general_operation,
            code: random_transient_code(rng),
            attempts: 1,
        },
        BackgroundFault::ResponseLoss {
            target: FaultTarget::Manifest,
            operation: Operation::Update,
            code: Code::Unavailable,
        },
        BackgroundFault::ResponseLoss {
            target: FaultTarget::Zone(rng.random_range(0..3)),
            operation: Operation::Delete,
            code: Code::Unavailable,
        },
        BackgroundFault::Delay {
            target: target_for(delay_operation, rng),
            operation: delay_operation,
            ticks: rng.random_range(1..=4),
        },
        BackgroundFault::Redirect {
            zone: rng.random_range(0..3),
            operation: redirect_operation,
            routing_token: format!("dst-{seed:016x}-redirect"),
        },
        BackgroundFault::SessionExpiry {
            zone: rng.random_range(0..3),
            operation: expiry_operation,
        },
        BackgroundFault::MutationThrottle {
            target: target_for(throttle_operation, rng),
            operation: throttle_operation,
        },
    ];
    faults.shuffle(rng);

    let mut opportunity = 0u64;
    faults
        .into_iter()
        .map(|fault| {
            opportunity += rng.random_range(1..=2);
            ScheduledBackgroundFault { opportunity, fault }
        })
        .collect()
}

type MetricLabels = Vec<(String, String)>;
type GaugeMetrics = HashMap<(String, MetricLabels), Arc<AtomicI64>>;

#[derive(Default)]
struct HarnessMetrics {
    counters: Mutex<HashMap<String, Arc<AtomicU64>>>,
    gauges: Mutex<GaugeMetrics>,
}

impl HarnessMetrics {
    fn counter(&self, name: &str) -> u64 {
        self.counters.lock().unwrap()[name].load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn labeled_gauge(&self, name: &str, labels: &[(&str, &str)]) -> i64 {
        let key = (
            name.to_string(),
            labels
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
        );
        self.gauges.lock().unwrap()[&key].load(Ordering::Relaxed)
    }
}

struct HarnessCounter(Arc<AtomicU64>);

impl CounterFn for HarnessCounter {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }
}

struct HarnessGauge(Arc<AtomicI64>);

impl GaugeFn for HarnessGauge {
    fn set(&self, value: i64) {
        self.0.store(value, Ordering::Relaxed);
    }
}

struct NoopMetric;

impl GaugeFn for NoopMetric {
    fn set(&self, _value: i64) {}
}

impl UpDownCounterFn for NoopMetric {
    fn increment(&self, _value: i64) {}
}

impl HistogramFn for NoopMetric {
    fn record(&self, _value: f64) {}
}

impl MetricsRecorder for HarnessMetrics {
    fn register_counter(
        &self,
        name: &str,
        _description: &str,
        _labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        let metric = self
            .counters
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_default()
            .clone();
        Arc::new(HarnessCounter(metric))
    }

    fn register_gauge(
        &self,
        name: &str,
        _description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        let key = (
            name.to_string(),
            labels
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
        );
        let metric = self.gauges.lock().unwrap().entry(key).or_default().clone();
        Arc::new(HarnessGauge(metric))
    }

    fn register_up_down_counter(
        &self,
        _name: &str,
        _description: &str,
        _labels: &[(&str, &str)],
    ) -> Arc<dyn UpDownCounterFn> {
        Arc::new(NoopMetric)
    }

    fn register_histogram(
        &self,
        _name: &str,
        _description: &str,
        _labels: &[(&str, &str)],
        _boundaries: &[f64],
    ) -> Arc<dyn HistogramFn> {
        Arc::new(NoopMetric)
    }
}

#[derive(Clone, Debug)]
struct SegmentObservation {
    id: String,
    base_record_index: u64,
    end_record_index: Option<u64>,
    crc32c: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DirectoryObservation {
    id: String,
    base_record_index: u64,
    crc32c: u32,
}

#[derive(Clone, Debug)]
struct ManifestObservation {
    epoch: u64,
    tail_base: u64,
    tail_id: Option<String>,
    pending_id: Option<String>,
    seal_base: Option<u64>,
    seal_id: Option<String>,
    seal_digest: Option<u64>,
    truncation_floor: u64,
    /// The `chorus.segments` directory: committed seals not yet deleted
    /// from every zone, in base order.
    segments: Vec<DirectoryObservation>,
    /// The raw `chorus.segments` register value, used to mirror the engine's
    /// directory-capacity gate.
    segments_encoded: String,
}

/// Run one seed inside a turmoil simulation: simulated network, virtual
/// time. The three zone fakes and the regional manifest fake are turmoil
/// hosts serving tonic over the simulated network; the harness plus the real
/// production client run as the turmoil client host. Everything that can
/// order events — message latency, timers, turmoil's own RNG — is derived
/// from `seed`, so two invocations replay identically.
pub fn run_seed_with_latency(
    seed: u64,
    steps: u64,
    inject_latency: bool,
) -> Result<SimulationReport> {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(SIM_DURATION)
        .tick_duration(SIM_TICK)
        .min_message_latency(SIM_TICK)
        .max_message_latency(SIM_TICK)
        .rng_seed(seed)
        .build();
    let zones: Vec<FakeGcs> = (0..3)
        .map(|zone| {
            if inject_latency {
                FakeGcs::with_latency(zone_latency_profile(seed, zone))
            } else {
                FakeGcs::default()
            }
        })
        .collect();
    let manifest = if inject_latency {
        FakeGcs::with_latency(manifest_latency_profile(seed))
    } else {
        FakeGcs::default()
    };
    // turmoil runs single-threaded; the report leaves the client future
    // through a shared slot. The fakes are not served as network hosts: the
    // in-memory transport drives them directly inside the harness client.
    let slot: Rc<RefCell<Option<Result<SimulationReport>>>> = Rc::new(RefCell::new(None));
    let report = Rc::clone(&slot);
    sim.client("harness", async move {
        let outcome = async {
            ProductionHarness::new(seed, zones, manifest)
                .await?
                .run(steps.max(1))
                .await
        }
        .await;
        *report.borrow_mut() = Some(outcome);
        Ok(())
    });
    sim.run()
        .map_err(|error| anyhow!("simulation failed: {error}"))?;
    let outcome = slot
        .borrow_mut()
        .take()
        .context("harness completed without reporting")?;
    outcome
}

#[cfg(test)]
fn run_lane_stall_seed(seed: u64) -> Result<SimulationReport> {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(SIM_DURATION)
        .tick_duration(SIM_TICK)
        .min_message_latency(SIM_TICK)
        .max_message_latency(SIM_TICK)
        .rng_seed(seed)
        .build();
    let zones: Vec<FakeGcs> = (0..3).map(|_| FakeGcs::default()).collect();
    let manifest = FakeGcs::default();
    for (zone, service) in zones.iter().enumerate() {
        let service = service.clone();
        sim.host(format!("zone-{zone}"), move || serve_fake(service.clone()));
    }
    {
        let service = manifest.clone();
        sim.host("manifest", move || serve_fake(service.clone()));
    }
    let slot: Rc<RefCell<Option<Result<SimulationReport>>>> = Rc::new(RefCell::new(None));
    let report = Rc::clone(&slot);
    sim.client("harness", async move {
        let mut config = production_engine_config();
        config.pipeline_window_records = 32;
        config.max_replica_lag_bytes = 1024 * 1024;
        config.lane_stall_timeout = Duration::from_millis(50);
        config.max_segment_bytes = 16 * 1024 * 1024;
        let outcome = async {
            ProductionHarness::new_with_engine_config(seed, zones, manifest, config)
                .await?
                .run_lane_stall_scenario()
                .await
        }
        .await;
        *report.borrow_mut() = Some(outcome);
        Ok(())
    });
    sim.run()
        .map_err(|error| anyhow!("lane-stall simulation failed: {error}"))?;
    let outcome = slot
        .borrow_mut()
        .take()
        .context("lane-stall harness completed without reporting")?;
    outcome
}

#[cfg(test)]
fn run_lane_stall_recheck_seed(seed: u64, close_code: Code) -> Result<()> {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(SIM_DURATION)
        .tick_duration(SIM_TICK)
        .min_message_latency(SIM_TICK)
        .max_message_latency(SIM_TICK)
        .rng_seed(seed)
        .build();
    let zone = FakeGcs::default();
    let manifest = FakeGcs::default();
    {
        let service = zone.clone();
        sim.host("zone-0", move || serve_fake(service.clone()));
    }
    {
        let service = manifest;
        sim.host("manifest", move || serve_fake(service.clone()));
    }
    let slot: Rc<RefCell<Option<Result<()>>>> = Rc::new(RefCell::new(None));
    let result = Rc::clone(&slot);
    sim.client("harness", async move {
        let outcome = run_lane_stall_recheck_scenario(seed, zone, close_code).await;
        *result.borrow_mut() = Some(outcome);
        Ok(())
    });
    sim.run()
        .map_err(|error| anyhow!("lane-stall {close_code:?} simulation failed: {error}"))?;
    let outcome = slot
        .borrow_mut()
        .take()
        .context("lane-stall recheck harness completed without reporting")?;
    outcome
}

#[cfg(test)]
async fn run_lane_stall_recheck_scenario(seed: u64, zone: FakeGcs, close_code: Code) -> Result<()> {
    let bucket = format!("{BUCKET_PREFIX}0");
    let scenario = match close_code {
        Code::Unavailable => "transient",
        Code::FailedPrecondition => "fenced",
        _ => bail!("unsupported lane-stall close code {close_code:?}"),
    };
    let prefix = format!("dst/lane-stall-recheck/{scenario}/{seed}");
    let factory =
        GrpcReplicaFactory::from_channel(0, sim_channel("zone-0").await?, bucket.clone(), None);
    let manifest_factory = GrpcReplicaFactory::from_channel(
        1,
        sim_channel("manifest").await?,
        format!("{BUCKET_PREFIX}regional"),
        None,
    );
    let metrics = Arc::new(HarnessMetrics::default());
    let metrics_recorder: Arc<dyn MetricsRecorder> = metrics.clone();
    let volume = SegmentedVolume::new_with_metrics_recorder(
        vec![factory],
        manifest_factory,
        &prefix,
        ClientConfig {
            max_retries: 3,
            retry_base: Duration::ZERO,
        },
        metrics_recorder,
    )?;
    let mut recovery = volume.recover(WalSeqNo::ZERO).await?;
    while recovery.try_next().await?.is_some() {}
    let stall_timeout = Duration::from_millis(50);
    let mut handle = recovery
        .start(WalEngineConfig {
            queue_capacity: 16,
            max_record_bytes: 1024,
            pipeline_window_records: 4,
            max_inflight_bytes: 16 * 1024,
            max_replica_lag_bytes: 16 * 1024,
            lane_stall_timeout: stall_timeout,
            max_segment_bytes: 16 * 1024 * 1024,
            max_active_segment_bytes: WalEngineConfig::default().max_active_segment_bytes,
            repair_interval: None,
            shutdown_timeout: Duration::from_secs(60),
        })
        .await?;

    zone.inject_delay(
        Operation::BidiAppendFlush,
        stall_timeout.saturating_sub(SIM_TICK.saturating_mul(5)),
    )
    .await;
    zone.inject_stream_close_after_response(Operation::BidiAppendFlush, close_code)
        .await;

    let payload = Bytes::from(vec![b'x'; 512]);
    let expected = payload.len() + 4;
    let completion = handle
        .enqueue_append(WalSeqNo::ZERO, payload)
        .await
        .context("failed to admit the recheck record")?;
    tokio::time::timeout(stall_timeout.saturating_mul(10), completion)
        .await
        .context("recheck record did not complete")?
        .context("recheck record was spuriously poisoned")?;

    let object_prefix = format!("{prefix}/segments/");
    wait_for_predicate("the recheck record to persist", || {
        let zone = zone.clone();
        let bucket = bucket.clone();
        let object_prefix = object_prefix.clone();
        async move {
            let persisted = zone
                .observe_prefix(&bucket, &object_prefix)
                .await
                .into_iter()
                .map(|object| object.bytes.len())
                .max()
                .unwrap_or(0);
            Ok((persisted >= expected).then_some(()))
        }
    })
    .await?;
    if metrics.counter("chorus.wal.lane.timeouts") != 0 {
        bail!("the lane was shed after publishing deadline-edge progress");
    }
    if metrics.labeled_gauge("chorus.wal.replica.durable_lag_bytes", &[("zone", "0")]) != 0 {
        bail!("the deadline-edge persisted_size was not published");
    }

    let completion = handle
        .enqueue_append(WalSeqNo::record(1), Bytes::from_static(b"after-recheck"))
        .await
        .context("the rechecked lane stopped before reporting its terminal state")?;
    let result = tokio::time::timeout(stall_timeout.saturating_mul(10), completion)
        .await
        .context("post-recheck append did not complete")?;
    match close_code {
        Code::Unavailable => {
            result.context("post-recheck append was spuriously poisoned")?;
            if metrics.counter("chorus.wal.lane.timeouts") != 0 {
                bail!("the advancing lane was shed after the transient stream close");
            }
        }
        Code::FailedPrecondition => {
            let error = match result {
                Ok(_) => bail!("the fenced writer acknowledged a subsequent append"),
                Err(error) => error,
            };
            if !matches!(
                error,
                chorus_client::Error::Fenced(_)
                    | chorus_client::Error::Poisoned
                    | chorus_client::Error::Transport {
                        code: chorus_client::TransportCode::FailedPrecondition,
                        ..
                    }
            ) {
                bail!("the fenced writer returned a non-fencing error: {error}");
            }
        }
        _ => unreachable!("close code validated above"),
    }
    if close_code == Code::Unavailable {
        handle.shutdown().await?;
    } else {
        let _ = handle.shutdown().await;
    }
    Ok(())
}

/// Serve a fake GCS zone forever over the simulated network.
#[cfg(test)]
async fn serve_fake(service: FakeGcs) -> turmoil::Result {
    let listener = turmoil::net::TcpListener::bind((
        std::net::IpAddr::from(std::net::Ipv4Addr::UNSPECIFIED),
        SIM_GRPC_PORT,
    ))
    .await?;
    service
        .serve_with_incoming(async_stream::stream! {
            loop {
                yield listener.accept().await.map(|(stream, _)| sim_io::Accepted(stream));
            }
        })
        .await?;
    Ok(())
}

/// A tonic channel to a simulated host, dialed through turmoil's network.
#[cfg(test)]
async fn sim_channel(host: &str) -> Result<tonic::transport::Channel> {
    let endpoint = tonic::transport::Endpoint::new(format!("http://{host}:{SIM_GRPC_PORT}"))
        .with_context(|| format!("invalid simulated endpoint for {host}"))?;
    endpoint
        .connect_with_connector(sim_io::connector())
        .await
        .with_context(|| format!("failed to dial simulated host {host}"))
}

/// Adapters between turmoil's simulated TCP streams and the tonic/hyper I/O
/// traits, following turmoil's gRPC example: the server side wraps accepted
/// streams in a `Connected` `AsyncRead`/`AsyncWrite` newtype for
/// `serve_with_incoming`; the client side is a tower connector returning
/// `hyper_util::rt::TokioIo` for `connect_with_connector`.
#[cfg(test)]
mod sim_io {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use hyper::Uri;
    use hyper_util::rt::TokioIo;
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tonic::transport::server::{Connected, TcpConnectInfo};
    use tower::Service;
    use turmoil::net::TcpStream;

    pub struct Accepted(pub TcpStream);

    impl Connected for Accepted {
        type ConnectInfo = TcpConnectInfo;

        fn connect_info(&self) -> Self::ConnectInfo {
            Self::ConnectInfo {
                local_addr: self.0.local_addr().ok(),
                remote_addr: self.0.peer_addr().ok(),
            }
        }
    }

    impl AsyncRead for Accepted {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for Accepted {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            Pin::new(&mut self.0).poll_write(cx, buf)
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }

    type Fut = Pin<Box<dyn Future<Output = Result<TokioIo<TcpStream>, std::io::Error>> + Send>>;

    pub fn connector(
    ) -> impl Service<Uri, Response = TokioIo<TcpStream>, Error = std::io::Error, Future = Fut> + Clone
    {
        tower::service_fn(|uri: Uri| {
            Box::pin(async move {
                let authority = uri
                    .authority()
                    .ok_or_else(|| std::io::Error::other("endpoint uri lacks an authority"))?;
                let stream = TcpStream::connect(authority.as_str()).await?;
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }) as Fut
        })
    }
}

pub fn assert_deterministic(seed: u64, steps: u64) -> Result<SimulationReport> {
    assert_deterministic_with_latency(seed, steps, false)
}

pub fn assert_deterministic_with_latency(
    seed: u64,
    steps: u64,
    inject_latency: bool,
) -> Result<SimulationReport> {
    let first = run_seed_with_latency(seed, steps, inject_latency)?;
    let second = run_seed_with_latency(seed, steps, inject_latency)?;
    if first.digest != second.digest || first.events != second.events {
        let mismatch = first
            .events
            .iter()
            .zip(&second.events)
            .position(|(left, right)| left != right)
            .unwrap_or(first.events.len().min(second.events.len()));
        bail!(
            "production seed {seed} is not deterministic at event {mismatch}: first={:?}, second={:?}",
            first.events.get(mismatch),
            second.events.get(mismatch)
        );
    }
    Ok(first)
}

#[cfg(test)]
fn assert_lane_stall_deterministic(seed: u64) -> Result<SimulationReport> {
    let first = run_lane_stall_seed(seed)?;
    let second = run_lane_stall_seed(seed)?;
    if first.digest != second.digest || first.events != second.events {
        let mismatch = first
            .events
            .iter()
            .zip(&second.events)
            .position(|(left, right)| left != right)
            .unwrap_or(first.events.len().min(second.events.len()));
        bail!(
            "lane-stall seed {seed} is not deterministic at event {mismatch}: first={:?}, second={:?}",
            first.events.get(mismatch),
            second.events.get(mismatch)
        );
    }
    Ok(first)
}

struct ProductionHarness {
    seed: u64,
    rng: ChaCha8Rng,
    background_faults: VecDeque<ScheduledBackgroundFault>,
    fault_opportunity: u64,
    servers: Vec<FakeGcs>,
    crashed: [bool; 3],
    factories: Vec<Arc<dyn ReplicaFactory>>,
    manifest_server: FakeGcs,
    manifest_factory: Arc<dyn ReplicaFactory>,
    prefix: String,
    client_config: ClientConfig,
    engine_config: WalEngineConfig,
    handle: Option<WalHandle>,
    metrics: Option<Arc<HarnessMetrics>>,
    trace: Vec<TraceEvent>,
    sequence: u64,
    logical_time: u64,
    writer_incarnation: u64,
    epoch: u64,
    submitted: u64,
    next_seqno: u64,
    truncation_floor: u64,
    next_reader: u64,
    seen_records: HashSet<u64>,
    recovery_selected: HashSet<u64>,
    seen_seals: BTreeMap<u64, u64>,
    seen_quorum_enforced: HashSet<(String, u64)>,
    pending_gate_releases: VecDeque<(String, u64, u64)>,
    seen_gate_releases: HashSet<String>,
    observed_seal_metric: u64,
    seen_creates: HashSet<(u64, u64)>,
    seen_finalized: HashSet<(u64, u64)>,
    seen_get_sizes: HashSet<(u64, bool)>,
    seen_open: HashSet<(u64, u64)>,
    seen_manifest_views: HashSet<(u64, u64, u64, u64)>,
    seen_seal_decisions: HashSet<u64>,
    segment_writers: HashMap<(u64, u64), (u64, u64)>,
    record_writers: HashMap<u64, u64>,
    // Ordered distinct object ids observed at each segment base. The index
    // is the trace's generation number: recovery retiring an empty tail
    // name and committing a fresh id at the same base is two object names
    // — two generations — at one base, and the monitors key creators,
    // appenders, and recovered names by (segment, gen).
    segment_gens: HashMap<u64, Vec<String>>,
    trace_segment_ids: HashMap<String, u64>,
    next_trace_segment_id: u64,
}

impl ProductionHarness {
    async fn new(seed: u64, servers: Vec<FakeGcs>, manifest_server: FakeGcs) -> Result<Self> {
        Self::new_with_engine_config(seed, servers, manifest_server, production_engine_config())
            .await
    }

    async fn new_with_engine_config(
        seed: u64,
        servers: Vec<FakeGcs>,
        manifest_server: FakeGcs,
        engine_config: WalEngineConfig,
    ) -> Result<Self> {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let background_faults = background_fault_budget(seed, &mut rng);
        // In-memory transport: each replica drives its FakeGcs zone directly
        // (no tonic/h2/TCP, no served accept loops, no spawned per-session
        // reader), collapsing the concurrent-task soup so the simulation is
        // bit-reproducible while the client protocol runs unchanged.
        let mut factories: Vec<Arc<dyn ReplicaFactory>> = Vec::new();
        for (zone, server) in servers.iter().enumerate() {
            factories.push(Arc::new(InMemoryReplicaFactory::new(
                server.clone(),
                format!("{BUCKET_PREFIX}{zone}"),
                zone,
            )));
        }
        // the regional bucket hosting the manifest control register; its
        // availability is the provider's regional replication, so DST keeps
        // it reachable while zones crash and recover around it
        let manifest_factory: Arc<dyn ReplicaFactory> = Arc::new(InMemoryReplicaFactory::new(
            manifest_server.clone(),
            format!("{BUCKET_PREFIX}regional"),
            3,
        ));
        let mut harness = Self {
            seed,
            rng,
            background_faults,
            fault_opportunity: 0,
            servers,
            crashed: [false; 3],
            factories,
            manifest_server,
            manifest_factory,
            prefix: format!("dst/{seed}"),
            client_config: ClientConfig {
                max_retries: 3,
                retry_base: Duration::ZERO,
            },
            engine_config,
            handle: None,
            metrics: None,
            trace: Vec::new(),
            sequence: 0,
            logical_time: 0,
            writer_incarnation: 0,
            epoch: 0,
            submitted: 0,
            next_seqno: 0,
            truncation_floor: 0,
            next_reader: 1,
            seen_records: HashSet::new(),
            recovery_selected: HashSet::new(),
            seen_seals: BTreeMap::new(),
            seen_quorum_enforced: HashSet::new(),
            pending_gate_releases: VecDeque::new(),
            seen_gate_releases: HashSet::new(),
            observed_seal_metric: 0,
            seen_creates: HashSet::new(),
            seen_finalized: HashSet::new(),
            seen_get_sizes: HashSet::new(),
            seen_open: HashSet::new(),
            seen_manifest_views: HashSet::new(),
            seen_seal_decisions: HashSet::new(),
            segment_writers: HashMap::new(),
            record_writers: HashMap::new(),
            segment_gens: HashMap::new(),
            trace_segment_ids: HashMap::new(),
            next_trace_segment_id: 1,
        };
        harness.restart_engine().await?;
        Ok(harness)
    }

    async fn run(mut self, steps: u64) -> Result<SimulationReport> {
        if let Err(error) = self.run_phases(steps).await {
            // diagnostic aid: preserve the partial trace of a failed seed
            if let Ok(path) = std::env::var("CHORUS_DST_FAILURE_TRACE") {
                let mut lines = String::new();
                for event in &self.trace {
                    if let Ok(line) = serde_json::to_string(event) {
                        lines.push_str(&line);
                        lines.push('\n');
                    }
                }
                let _ = std::fs::write(&path, lines);
            }
            return Err(error);
        }
        self.finish(steps).await
    }

    #[cfg(test)]
    async fn run_lane_stall_scenario(mut self) -> Result<SimulationReport> {
        const STALLED_ZONE: usize = 2;
        const SLOW_PROGRESS_RECORDS: usize = 3;
        const BURST_RECORDS: usize = 16;

        let stall_timeout = self.engine_config.lane_stall_timeout;
        let tail_id = self
            .manifest_observation()
            .await?
            .tail_id
            .context("lane-stall scenario has no active tail")?;
        let object = self.segment_object(&tail_id);
        let mut stalled_zone_bytes = self.zone_bytes(STALLED_ZONE, &object).await;
        let mut slow_receipts = Vec::new();

        // Repeated sub-timeout delays make this lane slower than its peers while
        // still advancing persisted_size. Every observed advance must reset the
        // deadline; relative slowness alone is not a shedding condition.
        for _ in 0..SLOW_PROGRESS_RECORDS {
            self.servers[STALLED_ZONE]
                .inject_delay(Operation::BidiAppendFlush, stall_timeout / 2)
                .await;
            let completion = self.admit_padded(512).await?;
            let receipt = tokio::time::timeout(stall_timeout.saturating_mul(2), completion)
                .await
                .context("slow advancing lane blocked a quorum commit")??;
            slow_receipts.push(receipt);
            stalled_zone_bytes += 516;
            self.wait_for_zone_bytes(STALLED_ZONE, &object, stalled_zone_bytes)
                .await?;
        }
        if self
            .metrics
            .as_ref()
            .context("metrics recorder not installed")?
            .counter("chorus.wal.lane.timeouts")
            != 0
        {
            bail!("a steadily advancing lane was falsely shed");
        }
        self.observe_receipts(slow_receipts).await?;

        // Park this established lane's future flushes permanently. The other
        // two lanes remain healthy, so every admitted record must still commit
        // on a true 2-of-3 quorum without waiting for the stall deadline.
        self.servers[STALLED_ZONE].inject_flush_hold().await;
        self.emit("ProducerSpike", None, None, None, None, None, None);
        let stall_started = tokio::time::Instant::now();
        let mut completions = Vec::new();
        for _ in 0..BURST_RECORDS {
            completions.push(self.admit_padded(2048).await?);
        }
        let results = tokio::time::timeout(stall_timeout, join_all(completions))
            .await
            .context("healthy quorum commits waited for the stalled lane")?;
        let mut receipts = Vec::new();
        for result in results {
            receipts.push(result.context("healthy quorum append failed during one-lane stall")?);
        }
        self.observe_receipts(receipts).await?;

        wait_for_predicate("the no-progress lane to be time-shed", || {
            let metrics = self.metrics.as_ref().expect("metrics installed").clone();
            async move { Ok((metrics.counter("chorus.wal.lane.timeouts") == 1).then_some(())) }
        })
        .await?;
        let shed_elapsed = stall_started.elapsed();
        if shed_elapsed > stall_timeout.saturating_add(SIM_TICK.saturating_mul(16)) {
            bail!("stalled lane was shed after {shed_elapsed:?}, beyond timeout {stall_timeout:?}");
        }
        let metrics = self
            .metrics
            .as_ref()
            .context("metrics recorder not installed")?;
        if metrics.counter("chorus.wal.lane.capacity_drops") != 0 {
            bail!("lane hit the retained-byte limit before its stall timeout");
        }

        // Force the writer to observe the dead lane handle, then prove a later
        // append still commits on the two live lanes within the same bound.
        let completion = self.admit_padded(2048).await?;
        let receipt = tokio::time::timeout(stall_timeout, completion)
            .await
            .context("post-shed append exceeded the stall bound")??;
        self.observe_receipts(vec![receipt]).await?;
        let healthy_bytes = self
            .zone_bytes(0, &object)
            .await
            .min(self.zone_bytes(1, &object).await);
        if self.zone_bytes(STALLED_ZONE, &object).await >= healthy_bytes {
            bail!("held lane advanced with the healthy quorum instead of remaining stalled");
        }

        // Release the transport hold and restart through normal recovery. The
        // acknowledged prefix must replay intact from the two committing lanes;
        // the formerly stalled copy may contain any shorter exact prefix.
        self.servers[STALLED_ZONE].release_flush_holds().await;
        advance_virtual_ticks(SETTLING_MARGIN_TICKS).await;
        self.restart_engine().await?;

        let steps = self.submitted;
        self.finish(steps).await
    }

    async fn observed_fault_count(&self) -> u64 {
        let mut total = self.manifest_server.observed_fault_count().await;
        for server in &self.servers {
            total += server.observed_fault_count().await;
        }
        total
    }

    async fn arm_background_faults(&mut self) -> Vec<BackgroundFault> {
        self.fault_opportunity += 1;
        let mut armed = Vec::new();
        while self
            .background_faults
            .front()
            .is_some_and(|fault| fault.opportunity <= self.fault_opportunity)
        {
            let scheduled = self
                .background_faults
                .pop_front()
                .expect("checked nonempty");
            armed.push(scheduled.fault.clone());
            self.apply_background_fault(scheduled.fault).await;
        }
        armed
    }

    async fn apply_background_fault(&mut self, fault: BackgroundFault) {
        match fault {
            BackgroundFault::Transient {
                target,
                operation,
                code,
                attempts,
            } => {
                let service = self.fault_target(target);
                for _ in 0..attempts {
                    service.inject(operation, code).await;
                }
                let event = if code == Code::DeadlineExceeded {
                    "RpcDeadlineExceeded"
                } else {
                    "RpcUnavailable"
                };
                self.emit(event, target.trace_zone(), None, None, None, None, None);
            }
            BackgroundFault::ResponseLoss {
                target,
                operation,
                code,
            } => {
                self.fault_target(target)
                    .inject_response_lost(operation, code)
                    .await;
                self.emit(
                    "RpcDropped",
                    target.trace_zone(),
                    None,
                    None,
                    None,
                    None,
                    None,
                );
            }
            BackgroundFault::Delay {
                target,
                operation,
                ticks,
            } => {
                self.fault_target(target)
                    .inject_delay(operation, SIM_TICK.saturating_mul(ticks))
                    .await;
                self.emit(
                    "RpcDropped",
                    target.trace_zone(),
                    None,
                    None,
                    None,
                    None,
                    None,
                );
            }
            BackgroundFault::Redirect {
                zone,
                operation,
                routing_token,
            } => {
                self.servers[zone]
                    .inject_redirect(operation, routing_token)
                    .await;
                self.emit("RpcDropped", Some(zone), None, None, None, None, None);
            }
            BackgroundFault::SessionExpiry { zone, operation } => {
                self.servers[zone].inject_session_expiry(operation).await;
                self.emit("RpcDropped", Some(zone), None, None, None, None, None);
            }
            BackgroundFault::MutationThrottle { target, operation } => {
                self.fault_target(target)
                    .inject_mutation_throttle(operation)
                    .await;
                self.emit(
                    "RpcDropped",
                    target.trace_zone(),
                    None,
                    None,
                    None,
                    None,
                    None,
                );
            }
        }
    }

    fn fault_target(&self, target: FaultTarget) -> FakeGcs {
        match target {
            FaultTarget::Zone(zone) => self.servers[zone].clone(),
            FaultTarget::Manifest => self.manifest_server.clone(),
        }
    }

    async fn disarm_background_faults(&self, faults: &[BackgroundFault]) {
        for fault in faults {
            let (target, operation) = match fault {
                BackgroundFault::Transient {
                    target, operation, ..
                }
                | BackgroundFault::ResponseLoss {
                    target, operation, ..
                }
                | BackgroundFault::Delay {
                    target, operation, ..
                }
                | BackgroundFault::MutationThrottle { target, operation } => (*target, *operation),
                BackgroundFault::Redirect {
                    zone, operation, ..
                }
                | BackgroundFault::SessionExpiry { zone, operation } => {
                    (FaultTarget::Zone(*zone), *operation)
                }
            };
            self.fault_target(target)
                .clear_injected_operation(operation)
                .await;
        }
    }

    async fn restart_after_expected_fault(&mut self, mut allow_aftermath: bool) -> Result<()> {
        for attempt in 0..=4 {
            let observed_before = self.observed_fault_count().await;
            match self.restart_engine().await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let observed = self.observed_fault_count().await > observed_before;
                    if !is_expected_harness_fault(&error, observed || allow_aftermath) {
                        return Err(error);
                    }
                    allow_aftermath = false;
                    if attempt == 4 {
                        return Err(error).context(
                            "expected injected-fault recovery exhausted its bounded retries",
                        );
                    }
                }
            }
        }
        unreachable!("bounded recovery loop returns on success or final failure")
    }

    async fn recover_expected_phase_fault(&mut self) -> Result<()> {
        for zone in 0..3 {
            self.servers[zone].release_open_holds().await;
            self.servers[zone].release_flush_holds().await;
            self.servers[zone].release_finalize_holds().await;
            if self.crashed[zone] {
                self.servers[zone].set_crashed(false).await;
                self.crashed[zone] = false;
                self.emit("ZoneRestart", Some(zone), None, None, None, None, None);
            }
        }
        self.restart_after_expected_fault(true).await
    }

    async fn run_phases(&mut self, steps: u64) -> Result<()> {
        let mut round = 0u64;
        while self.submitted < steps {
            // One shuffled deck is one coverage epoch: every adversarial
            // phase runs exactly once, but no phase can quietly depend on the
            // predecessor that happened to precede it in the original fixed
            // schedule. Seed zero intentionally preserves that old ordering
            // as a debugging escape hatch for bisecting schedule regressions.
            let phases = phase_deck(self.seed, &mut self.rng);
            for phase in phases {
                if self.submitted >= steps {
                    break;
                }
                self.logical_time += 1;
                if std::env::var("CHORUS_DEBUG_LANES").is_ok() {
                    eprintln!(
                        "PHASE round={round} phase={phase} submitted={}",
                        self.submitted
                    );
                }
                let observed_before = self.observed_fault_count().await;
                let phase_result = match phase {
                    0 => {
                        self.inject_transient().await;
                        Ok(())
                    }
                    1 => self.write_with_zone_down().await,
                    2 => self.restart_with_zone_down().await,
                    3 => self.force_takeover().await,
                    4 => self.truncate().await,
                    5 => {
                        self.delayed_spare_provisioning(steps - self.submitted)
                            .await
                    }
                    6 => self.exercise_reduced_redundancy_corruption().await,
                    7 => {
                        self.inject_ambiguous_appends().await;
                        Ok(())
                    }
                    8 => self.ambiguous_manifest_cas().await,
                    9 => self.rot_active_tail().await,
                    10 => self.exercise_background_repair().await,
                    11 => {
                        self.inject_reordering().await;
                        Ok(())
                    }
                    12 => self.crash_in_swap_window(steps - self.submitted).await,
                    13 => self.stale_zone_empty_tail(steps - self.submitted).await,
                    14 => {
                        self.recover_past_unenforced_seal(steps - self.submitted)
                            .await
                    }
                    _ => self.racing_recoveries().await,
                };
                if let Err(error) = phase_result {
                    let observed = self.observed_fault_count().await > observed_before;
                    if is_expected_harness_fault(&error, observed) {
                        self.recover_expected_phase_fault().await.with_context(|| {
                            format!("round {round} phase {phase} expected-fault recovery")
                        })?;
                    } else {
                        return Err(error).with_context(|| format!("round {round} phase {phase}"));
                    }
                }
                // Adversarial phases may consume their whole fixed walk and
                // finish one record past the requested budget. The budget is
                // a lower bound for coverage, not an arithmetic invariant.
                let remaining = steps.saturating_sub(self.submitted);
                if remaining > 0 {
                    let spike = self.rng.random_range(4..=12);
                    let observed_before = self.observed_fault_count().await;
                    if let Err(error) = self.submit_spike(remaining.min(spike) as usize).await {
                        let observed = self.observed_fault_count().await > observed_before;
                        if is_expected_harness_fault(&error, observed) {
                            self.recover_expected_phase_fault().await.with_context(|| {
                                format!(
                                    "round {round} phase {phase} producer expected-fault recovery"
                                )
                            })?;
                        } else {
                            return Err(error).with_context(|| {
                                format!("round {round} phase {phase} producer spike")
                            });
                        }
                    }
                }
                round += 1;
            }
        }
        Ok(())
    }

    async fn finish(mut self, steps: u64) -> Result<SimulationReport> {
        if let Some(handle) = self.handle.take() {
            let shutdown_bound = self
                .engine_config
                .shutdown_timeout
                .saturating_mul(2)
                .saturating_add(Duration::from_secs(1));
            tokio::time::timeout(shutdown_bound, handle.shutdown())
                .await
                .context("final engine shutdown exceeded the harness bound")?
                .context("final engine shutdown")?;
        }
        self.audit().await?;
        validate_trace_structure(&self.trace).with_context(|| {
            format!(
                "tail={:?}",
                self.trace
                    .iter()
                    .rev()
                    .take(24)
                    .cloned()
                    .collect::<Vec<_>>()
            )
        })?;
        let digest = trace_digest(&self.trace)?;
        Ok(SimulationReport {
            seed: self.seed,
            steps,
            virtual_time_ms: self.logical_time,
            events: self.trace,
            digest,
            committed_records: self.seen_records.len() as u64,
            truncation_floor: self.truncation_floor,
        })
    }

    fn volume(&self, metrics_recorder: Arc<dyn MetricsRecorder>) -> SegmentedVolume {
        SegmentedVolume::new_with_dyn_factories_and_metrics_recorder(
            self.factories.to_vec(),
            self.manifest_factory.clone(),
            &self.prefix,
            self.client_config.clone(),
            metrics_recorder,
        )
        .expect("the simulation always binds three zones")
    }

    fn volume_at(&self, prefix: &str) -> SegmentedVolume {
        SegmentedVolume::new_with_dyn_factories_and_metrics_recorder(
            self.factories.to_vec(),
            self.manifest_factory.clone(),
            prefix,
            self.client_config.clone(),
            Arc::new(NoopMetricsRecorder),
        )
        .expect("the simulation always binds three zones")
    }

    async fn catalog(&self) -> Result<Vec<SegmentObservation>> {
        let prefix = format!("{}/segments/", self.prefix);
        // a fresh volume has no manifest yet — and no unstamped segments to
        // attribute either
        let manifest = self.manifest_observation().await.ok();
        let manifest = manifest.unwrap_or(ManifestObservation {
            epoch: 0,
            tail_base: 0,
            tail_id: None,
            pending_id: None,
            seal_base: None,
            seal_id: None,
            seal_digest: None,
            truncation_floor: 0,
            segments: Vec::new(),
            segments_encoded: String::new(),
        });
        let pending_base = if let Some(pending_id) = manifest.pending_id.as_deref() {
            let pending_records = self.candidate_record_count(pending_id).await.unwrap_or(0);
            if pending_records == 0 {
                None
            } else {
                let tail_records = match manifest.tail_id.as_deref() {
                    Some(tail_id) => self.candidate_record_count(tail_id).await.unwrap_or(0),
                    None => 0,
                };
                Some(
                    manifest
                        .tail_base
                        .checked_add(tail_records as u64)
                        .context("pending segment base overflowed u64")?,
                )
            }
        } else {
            None
        };
        let mut by_base: BTreeMap<u64, Vec<ObjectObservation>> = BTreeMap::new();
        for zone in 0..3 {
            if self.crashed[zone] {
                continue;
            }
            for object in self.servers[zone]
                .observe_prefix(&format!("{BUCKET_PREFIX}{zone}"), &prefix)
                .await
            {
                // Chain position lives in the manifest: the segment
                // directory names every committed seal, and tail_id names
                // the active segment. Anything else is an unswapped spare.
                let id = object
                    .name
                    .strip_prefix(&prefix)
                    .context("observed object lies outside the WAL prefix")?;
                let directory_base = manifest
                    .segments
                    .iter()
                    .find(|entry| entry.id == id)
                    .map(|entry| entry.base_record_index);
                let base = if let Some(base) = directory_base {
                    base
                } else if manifest.tail_id.as_deref() == Some(id) {
                    manifest.tail_base
                } else if manifest.pending_id.as_deref() == Some(id) {
                    let Some(base) = pending_base else {
                        continue;
                    };
                    base
                } else {
                    continue;
                };
                by_base.entry(base).or_default().push(object);
            }
        }
        let bases: Vec<_> = by_base.keys().copied().collect();
        let mut segments = Vec::with_capacity(bases.len());
        for (index, base_record_index) in bases.iter().copied().enumerate() {
            let copies = &by_base[&base_record_index];
            let id = copies[0]
                .name
                .strip_prefix(&prefix)
                .context("observed object lies outside the WAL prefix")?;
            if copies.iter().any(|copy| copy.name != copies[0].name) {
                bail!("multiple segment ids claim base {base_record_index}");
            }
            let crc32c = manifest
                .segments
                .iter()
                .find(|entry| entry.id == id)
                .map(|entry| entry.crc32c);
            let end_record_index = if let Some(next) = bases.get(index + 1) {
                Some(next - 1)
            } else {
                finalized_record_count(copies, crc32c).and_then(|count| {
                    u64::try_from(count)
                        .ok()
                        .and_then(|count| count.checked_sub(1))
                        .and_then(|span| base_record_index.checked_add(span))
                })
            };
            segments.push(SegmentObservation {
                id: id.to_string(),
                base_record_index,
                end_record_index,
                crc32c,
            });
        }
        Ok(segments)
    }

    async fn object_observations(&self, object: &str) -> Vec<(usize, ObjectObservation)> {
        let mut observations = Vec::new();
        for zone in 0..3 {
            if self.crashed[zone] {
                continue;
            }
            if let Some(observation) = self.servers[zone]
                .observe_prefix(&format!("{BUCKET_PREFIX}{zone}"), object)
                .await
                .into_iter()
                .find(|candidate| candidate.name == object)
            {
                observations.push((zone, observation));
            }
        }
        observations
    }

    async fn candidate_record_count(&self, id: &str) -> Option<usize> {
        let object = self.segment_object(id);
        let observations = self.object_observations(&object).await;
        let zones_up = (0..3).filter(|&zone| !self.crashed[zone]).count();
        if zones_up < 2 {
            return None;
        }
        let mut counts = observations
            .iter()
            .map(|(_, observation)| decode_complete_prefix(&observation.bytes).0.len())
            .collect::<Vec<_>>();
        counts.extend(std::iter::repeat_n(
            0,
            zones_up.saturating_sub(counts.len()),
        ));
        counts.sort_unstable();
        if zones_up == 3 {
            counts.get(counts.len().checked_sub(2)?).copied()
        } else {
            counts.last().copied()
        }
    }

    async fn manifest_observation(&self) -> Result<ManifestObservation> {
        self.manifest_observation_at(&self.prefix).await
    }

    async fn manifest_observation_at(&self, prefix: &str) -> Result<ManifestObservation> {
        let object = format!("{prefix}/manifest");
        let snapshot = self
            .manifest_server
            .observe_prefix(&format!("{BUCKET_PREFIX}regional"), &object)
            .await
            .into_iter()
            .find(|candidate| candidate.name == object)
            .context("manifest object is missing after a successful transition")?;
        if snapshot.metadata.get("chorus.format").map(String::as_str) != Some("1") {
            bail!("manifest object does not use chorus.format=1");
        }
        let parse_u64 = |key: &str| -> Result<u64> {
            snapshot
                .metadata
                .get(key)
                .with_context(|| format!("manifest lacks {key}"))?
                .parse()
                .with_context(|| format!("manifest has invalid {key}"))
        };
        let seal_base = snapshot
            .metadata
            .get("chorus.seal_base")
            .map(|value| {
                value
                    .parse()
                    .context("manifest has invalid chorus.seal_base")
            })
            .transpose()?;
        let seal_digest = snapshot
            .metadata
            .get("chorus.seal_digest")
            .map(|digest| {
                let prefix = digest
                    .get(..16)
                    .context("manifest seal digest is shorter than 64 bits")?;
                u64::from_str_radix(prefix, 16).context("manifest has invalid chorus.seal_digest")
            })
            .transpose()?;
        if seal_base.is_some() != seal_digest.is_some() {
            bail!("manifest seal base and digest are not both present");
        }
        let observation = ManifestObservation {
            epoch: parse_u64("chorus.epoch")?,
            tail_base: parse_u64("chorus.tail_base")?,
            tail_id: snapshot.metadata.get("chorus.tail_id").cloned(),
            pending_id: snapshot.metadata.get("chorus.pending_id").cloned(),
            seal_base,
            seal_id: snapshot.metadata.get("chorus.seal_id").cloned(),
            seal_digest,
            truncation_floor: parse_u64("chorus.trunc")?,
            segments: parse_segment_directory(
                snapshot
                    .metadata
                    .get("chorus.segments")
                    .context("manifest lacks chorus.segments")?,
            )?,
            segments_encoded: snapshot
                .metadata
                .get("chorus.segments")
                .cloned()
                .context("manifest lacks chorus.segments")?,
        };
        Ok(observation)
    }

    fn emit_committed_view(
        &mut self,
        epoch: u64,
        tail_base: u64,
        seal_base: u64,
        seal_digest: u64,
        truncation_floor: u64,
    ) {
        if !self
            .seen_manifest_views
            .insert((epoch, tail_base, seal_base, seal_digest))
        {
            return;
        }
        self.seen_seal_decisions.insert(seal_base);
        self.emit(
            "ViewCommitted",
            None,
            Some(seal_base),
            Some(tail_base),
            Some(seal_digest),
            Some(tail_base),
            None,
        );
        self.trace.last_mut().unwrap().truncation_floor = Some(truncation_floor);
    }

    fn emit_manifest_view(&mut self, manifest: &ManifestObservation) {
        let (Some(seal_base), Some(seal_digest)) = (manifest.seal_base, manifest.seal_digest)
        else {
            return;
        };
        self.emit_committed_view(
            manifest.epoch,
            manifest.tail_base,
            seal_base,
            seal_digest,
            manifest.truncation_floor,
        );
    }

    async fn directory_seal_digest(&self, entry: &DirectoryObservation) -> Result<u64> {
        let object = self.segment_object(&entry.id);
        let mut digests: HashMap<[u8; 32], usize> = HashMap::new();
        for (_, observation) in self.object_observations(&object).await {
            if !observation.finalized {
                continue;
            }
            if observation.crc32c != Some(entry.crc32c)
                || crc32c::crc32c(&observation.bytes) != entry.crc32c
            {
                continue;
            }
            let digest: [u8; 32] = Sha256::digest(&observation.bytes).into();
            *digests.entry(digest).or_default() += 1;
        }
        let digest = digests
            .into_iter()
            .find_map(|(digest, copies)| (copies >= 2).then_some(digest))
            .with_context(|| {
                format!(
                    "directory segment {} at base {} lacks an exact finalized digest quorum",
                    entry.id, entry.base_record_index
                )
            })?;
        Ok(u64::from_be_bytes(
            digest[..8].try_into().expect("SHA-256 prefix"),
        ))
    }

    async fn emit_manifest_views(&mut self, committed: &ManifestObservation) -> Result<()> {
        let mut emitted = false;
        for index in 0..committed.segments.len() {
            let entry = committed.segments[index].clone();
            if self.seen_seal_decisions.contains(&entry.base_record_index) {
                continue;
            }
            let tail_base = committed
                .segments
                .get(index + 1)
                .map_or(committed.tail_base, |next| next.base_record_index);
            let seal_digest = if committed.seal_base == Some(entry.base_record_index) {
                committed
                    .seal_digest
                    .context("current recovery seal lacks a digest")?
            } else {
                self.directory_seal_digest(&entry).await?
            };
            self.emit_committed_view(
                committed.epoch,
                tail_base,
                entry.base_record_index,
                seal_digest,
                committed.truncation_floor,
            );
            emitted = true;
        }
        if !emitted {
            self.emit_manifest_view(committed);
        }
        Ok(())
    }

    async fn emit_recovery_manifest_views(
        &mut self,
        adopted: Option<&ManifestObservation>,
        committed: &ManifestObservation,
    ) -> Result<()> {
        if let Some(adopted) = adopted {
            if committed.segments.len() < adopted.segments.len()
                || committed.segments[..adopted.segments.len()] != adopted.segments
            {
                bail!("recovery rewrote the adopted manifest directory");
            }
        }
        self.emit_manifest_views(committed).await
    }

    fn emit_directory_adoptions(&mut self, manifest: &ManifestObservation) -> Result<()> {
        let entry_count =
            u64::try_from(manifest.segments.len()).context("directory length does not fit u64")?;
        let current_seal_id = manifest
            .seal_id
            .as_deref()
            .map(|id| self.trace_segment_id(id));
        for (index, entry) in manifest.segments.iter().enumerate() {
            let next_base = manifest
                .segments
                .get(index + 1)
                .map_or(manifest.tail_base, |next| next.base_record_index);
            let end = next_base
                .checked_sub(1)
                .context("directory entry has no derivable end")?;
            let segment_id = self.trace_segment_id(&entry.id);
            self.emit(
                "DirectoryAdopted",
                None,
                Some(entry.base_record_index),
                None,
                None,
                Some(end),
                None,
            );
            let event = self.trace.last_mut().expect("just emitted");
            event.segment_id = Some(segment_id);
            event.current_seal_id = current_seal_id;
            event.tail_base = Some(manifest.tail_base);
            event.seal_base = manifest.seal_base;
            event.directory_index =
                Some(u64::try_from(index).context("directory index does not fit u64")?);
            event.directory_len = Some(entry_count);
            event.truncation_floor = Some(manifest.truncation_floor);
        }
        Ok(())
    }

    async fn restart_engine(&mut self) -> Result<()> {
        let checkpoint = self.truncation_floor;
        self.restart_engine_at(checkpoint).await
    }

    /// Restart recovery from a database checkpoint other than the committed
    /// truncation floor. The checkpoint positions replay and nothing else: a
    /// database that checkpointed past a sealed segment still must not strip
    /// that segment of its place in the chain.
    async fn restart_engine_at(&mut self, checkpoint: u64) -> Result<()> {
        if self.handle.is_some() {
            self.audit().await?;
        }
        if let Some(handle) = self.handle.take() {
            handle.abort().await;
            self.emit("WriterCrash", None, None, None, None, None, None);
        }
        self.writer_incarnation += 1;
        self.epoch = self.writer_incarnation;
        self.emit("WriterRestart", None, None, None, None, None, None);
        let recovery = self.observe_recovery_at(checkpoint).await?;
        self.next_seqno = recovery.end.record_index;
        let handle = recovery.start(self.engine_config.clone()).await?;
        // the recovery just minted a fresh active id, so the first audit to
        // observe it assigns this incarnation as its owner via gen keying
        self.handle = Some(handle);
        self.wait_for_repair_pass(1).await?;
        self.audit().await
    }

    async fn wait_for_repair_pass(&self, minimum: u64) -> Result<()> {
        for _ in 0..10_000 {
            self.handle.as_ref().context("engine not running")?;
            let passes = self
                .metrics
                .as_ref()
                .context("metrics recorder not installed")?
                .counter("chorus.wal.repair.passes");
            if passes >= minimum {
                return Ok(());
            }
            tokio::time::sleep(SIM_TICK).await;
        }
        bail!("background sealed repair pass did not complete")
    }

    async fn observe_recovery(&mut self) -> Result<Recovery> {
        let checkpoint = self.truncation_floor;
        self.observe_recovery_at(checkpoint).await
    }

    async fn observe_recovery_at(&mut self, checkpoint: u64) -> Result<Recovery> {
        let catalog = self.catalog().await?;
        // This is the snapshot recovery trusts as its chain authority. The
        // recovery stream may commit a new seal when it reaches EOF, so emit
        // adoption events from this pre-recovery view, not the later manifest.
        let adopted_manifest = self.manifest_observation().await.ok();
        let recovered_segment = catalog.last().cloned();
        if let Some(segment) = &recovered_segment {
            // recovery addresses the manifest's CURRENT name at this base;
            // a fresh generation it commits at the same base is a different
            // object name the recoverer may legitimately go on to append
            let recovered_gen = self.gen_for(segment.base_record_index, &segment.id);
            self.emit(
                "RecoveryStarted",
                None,
                Some(segment.base_record_index),
                None,
                None,
                None,
                None,
            );
            self.trace.last_mut().expect("just emitted").gen = Some(recovered_gen);
        }

        let metrics = Arc::new(HarnessMetrics::default());
        let metrics_recorder: Arc<dyn MetricsRecorder> = metrics.clone();
        let mut recovery = self
            .volume(metrics_recorder)
            .recover(WalSeqNo::record(checkpoint))
            .await?;
        self.metrics = Some(metrics);
        self.observed_seal_metric = 0;
        let claimed_manifest = self.manifest_observation().await?;
        self.epoch = claimed_manifest.epoch;
        self.emit("EpochClaimed", None, None, None, None, None, None);
        if let Some(adopted_manifest) = adopted_manifest.as_ref() {
            self.emit_directory_adoptions(adopted_manifest)?;
        }
        let start = recovery.from.record_index;
        let end = recovery.end.record_index;
        let mut records: BTreeMap<u64, WalRecord> = BTreeMap::new();
        while let Some(record) = recovery.try_next().await? {
            records.insert(record.seqno.record_index, record);
        }
        let committed_manifest = self.manifest_observation().await?;
        self.emit_recovery_manifest_views(adopted_manifest.as_ref(), &committed_manifest)
            .await?;
        for (record_index, record) in &records {
            let segment = catalog
                .iter()
                .rev()
                .find(|segment| segment.base_record_index <= *record_index)
                .context("recovered record has no containing segment")?
                .base_record_index;
            let recovered = recovered_record(record);
            self.emit(
                "RecoverySelected",
                None,
                Some(segment),
                Some(*record_index),
                Some(record_value(&recovered)),
                None,
                None,
            );
            // the monitor's RecoverySelected handler merges the record into
            // its formed map: a record first materialized by recovery
            // promotion (committed by repair, never acknowledged by its
            // writer) must not be re-announced as formed by a later audit,
            // though its quorum commit is still the audit's to report
            self.recovery_selected.insert(*record_index);
        }
        let reader = self.next_reader;
        self.next_reader += 1;
        self.emit(
            "ReplayOpened",
            None,
            None,
            Some(start),
            None,
            Some(end),
            Some(reader),
        );
        for record_index in records.keys() {
            let segment = catalog
                .iter()
                .rev()
                .find(|segment| segment.base_record_index <= *record_index)
                .context("replayed record has no containing segment")?
                .base_record_index;
            self.emit(
                "ReplayRecord",
                None,
                Some(segment),
                Some(*record_index),
                None,
                None,
                Some(reader),
            );
        }
        self.emit("ReplayClosed", None, None, None, None, None, Some(reader));

        if let Some(segment) = recovered_segment {
            if records
                .keys()
                .any(|record_index| *record_index >= segment.base_record_index)
            {
                let enforced = self
                    .catalog()
                    .await?
                    .into_iter()
                    .find(|candidate| candidate.id == segment.id)
                    .context("recovery did not retain its non-empty sealed segment")?;
                self.observe_canonical_support(&enforced, &records).await?;
            }
            self.emit(
                "RecoveryCompleted",
                None,
                Some(segment.base_record_index),
                Some(start),
                None,
                Some(end),
                None,
            );
        }
        Ok(recovery)
    }

    async fn observe_canonical_support(
        &mut self,
        segment: &SegmentObservation,
        records: &BTreeMap<u64, WalRecord>,
    ) -> Result<()> {
        let object = self.segment_object(&segment.id);
        let snapshots: Vec<_> = self
            .object_observations(&object)
            .await
            .into_iter()
            .filter(|(_, snapshot)| sealed_copy_is_healthy(snapshot, segment))
            .collect();
        for (record_index, record) in records {
            if *record_index < segment.base_record_index {
                continue;
            }
            let recovered = recovered_record(record);
            let value = record_value(&recovered);
            let offset = (*record_index - segment.base_record_index) as usize;
            let mut zones = Vec::new();
            for (zone, snapshot) in &snapshots {
                let (frames, _) = decode_complete_prefix(&snapshot.bytes);
                if frames.get(offset) == Some(&recovered) {
                    zones.push(*zone);
                }
            }
            if zones.len() < 2 {
                bail!("recovery did not persist record {record_index} on a finalized quorum");
            }
            zones.sort_unstable();
            for zone in zones {
                self.emit(
                    "CanonicalPersisted",
                    Some(zone),
                    Some(segment.base_record_index),
                    Some(*record_index),
                    Some(value),
                    None,
                    None,
                );
            }
        }
        Ok(())
    }

    async fn submit_spike(&mut self, count: usize) -> Result<()> {
        let (receipts, poisoned, faulted, directory_full) =
            self.submit_spike_without_audit(count).await?;
        self.observe_receipts(receipts).await?;
        if directory_full {
            // SegmentDirectoryFull is explicit admission backpressure: the
            // sequence number was not consumed and the writer remains healthy.
            // Model the database response by advancing its durable checkpoint,
            // restoring a temporarily unavailable zone if necessary, and
            // deleting enough retained entries for the two-slot swap reserve.
            self.restore_rotation_capacity().await?;
            return Ok(());
        }
        if poisoned || faulted {
            // the documented database reaction to a poisoned writer:
            // restart recovery, which realigns the next admissible sequence
            // number from the recovered end. A faulted-but-healthy spike also
            // restarts so the next exact-state trap begins with every lane
            // reconciled instead of inheriting legitimate degraded aftermath.
            self.restart_after_expected_fault(true).await?;
        }
        Ok(())
    }

    /// Submit a spike and await every completion. A quorum loss while two of
    /// three lanes are degraded legitimately poisons the writer (the
    /// protocol's documented behavior); that outcome is reported as
    /// `poisoned = true` together with the receipts that did commit, and the
    /// caller restarts recovery. Any other completion failure is fatal.
    async fn submit_spike_without_audit(
        &mut self,
        count: usize,
    ) -> Result<(Vec<AppendReceipt>, bool, bool, bool)> {
        let background_faults = self.arm_background_faults().await;
        let observed_before = self.observed_fault_count().await;
        self.emit("ProducerSpike", None, None, None, None, None, None);
        let mut completions = Vec::new();
        let mut admission_error = None;
        let mut admitted = 0usize;
        for index in 0..count {
            let value = self.submitted + index as u64;
            let seqno = self.next_seqno + index as u64;
            let result = self
                .handle
                .as_mut()
                .context("engine not running")?
                .enqueue_append(
                    WalSeqNo::record(seqno),
                    Bytes::from(format!("value-{value}")),
                )
                .await;
            match result {
                Ok(completion) => {
                    completions.push(completion);
                    self.record_writers.insert(seqno, self.writer_incarnation);
                    admitted += 1;
                }
                Err(error) => {
                    admission_error = Some((seqno, error));
                    break;
                }
            }
        }
        let results = join_all(completions).await;
        self.submitted += admitted as u64;
        self.next_seqno += admitted as u64;
        let observed = self.observed_fault_count().await > observed_before;
        self.disarm_background_faults(&background_faults).await;
        let mut receipts = Vec::new();
        let mut poisoned = false;
        let mut directory_full = false;
        if let Some((seqno, error)) = admission_error {
            if matches!(error, chorus_client::Error::SegmentDirectoryFull) {
                directory_full = true;
            } else if is_expected_client_fault(&error, observed) {
                poisoned = true;
            } else {
                return Err(error).with_context(|| format!("failed to admit record {seqno}"));
            }
        }
        for result in results {
            match result {
                Ok(receipt) => receipts.push(receipt),
                Err(error) if is_expected_client_fault(&error, observed) => {
                    poisoned = true;
                }
                Err(error) => {
                    return Err(error).context("append completion failed");
                }
            }
        }
        receipts.sort_by_key(|receipt| receipt.seqno);
        Ok((receipts, poisoned, observed, directory_full))
    }

    async fn observe_receipts(&mut self, receipts: Vec<AppendReceipt>) -> Result<()> {
        self.audit().await?;
        let catalog = self.catalog().await?;
        for receipt in receipts {
            let record = receipt.seqno.record_index;
            let segment = catalog
                .iter()
                .rev()
                .find(|segment| segment.base_record_index <= record)
                .context("acknowledged record has no segment")?
                .base_record_index;
            if !self.seen_records.contains(&record) {
                bail!("acknowledged record was not observed committed");
            }
            self.emit(
                "ProducerAcknowledged",
                None,
                Some(segment),
                Some(record),
                None,
                None,
                None,
            );
        }
        Ok(())
    }

    async fn audit(&mut self) -> Result<()> {
        let manifest = self.manifest_observation().await?;
        self.emit_manifest_views(&manifest).await?;
        let catalog = self.catalog().await?;
        for segment in &catalog {
            let base = segment.base_record_index;
            let gen = self.gen_for(base, &segment.id);
            let object = self.segment_object(&segment.id);
            let owner = *self
                .segment_writers
                .entry((base, gen))
                .or_insert((self.writer_incarnation, self.epoch));
            let mut snapshots = Vec::new();
            let mut reported_sizes = BTreeMap::new();
            for (zone, snapshot) in self.object_observations(&object).await {
                if let Some(reported) = self.servers[zone]
                    .reported_size_for(&format!("{BUCKET_PREFIX}{zone}"), &object)
                    .await
                {
                    reported_sizes.insert(zone, reported);
                }
                snapshots.push((zone, snapshot));
            }
            snapshots.sort_by_key(|(zone, _)| *zone);
            if snapshots.len() >= 2 {
                if self.seen_creates.insert((base, gen)) {
                    for (zone, _) in &snapshots {
                        self.emit_for(
                            owner,
                            "SegmentCreateAttempt",
                            Some(*zone),
                            Some(base),
                            None,
                            None,
                            None,
                            None,
                        );
                        self.emit_for(
                            owner,
                            "SegmentCreated",
                            Some(*zone),
                            Some(base),
                            None,
                            None,
                            None,
                            None,
                        );
                        self.trace.last_mut().expect("just emitted").gen = Some(gen);
                    }
                }
                if self.seen_open.insert((base, gen)) {
                    self.emit_for(
                        owner,
                        "SegmentOpened",
                        None,
                        Some(base),
                        None,
                        None,
                        None,
                        None,
                    );
                    self.trace.last_mut().expect("just emitted").gen = Some(gen);
                }
                if !self.seen_get_sizes.contains(&(base, false)) {
                    if let Some((zone, (reported_size, false))) = reported_sizes
                        .iter()
                        .map(|(zone, observation)| (*zone, *observation))
                        .find(|(_, (_, finalized))| !finalized)
                    {
                        self.seen_get_sizes.insert((base, false));
                        self.emit(
                            "GetSizeObserved",
                            Some(zone),
                            Some(base),
                            None,
                            None,
                            None,
                            None,
                        );
                        let event = self.trace.last_mut().expect("just emitted");
                        event.reported_size = Some(reported_size);
                        event.finalized = Some(false);
                    }
                }
            }

            let mut candidates: HashMap<(u64, u64), HashSet<usize>> = HashMap::new();
            for (zone, snapshot) in &snapshots {
                let (records, _) = decode_complete_prefix(&snapshot.bytes);
                for (offset, durable_record) in records.into_iter().enumerate() {
                    let record = base + offset as u64;
                    candidates
                        .entry((record, record_value(&durable_record)))
                        .or_default()
                        .insert(*zone);
                }
            }
            let mut committed: Vec<_> = candidates
                .into_iter()
                .filter(|(_, zones)| zones.len() >= 2)
                .collect();
            committed.sort_by_key(|((record, value), _)| (*record, *value));
            for ((record, value), zones) in committed {
                if !self.seen_records.insert(record) {
                    continue;
                }
                let writer = self.record_writers.get(&record).copied().unwrap_or(owner.0);
                // a record first materialized by a recovery's selection was
                // already merged into the monitors' formed state by
                // RecoverySelected; re-announcing it formed is a monitor
                // violation, but its quorum commit below is real
                if !self.recovery_selected.contains(&record) {
                    self.emit_for(
                        (writer, owner.1),
                        "RecordFormed",
                        None,
                        Some(base),
                        Some(record),
                        Some(value),
                        None,
                        None,
                    );
                }
                let mut zones: Vec<_> = zones.into_iter().collect();
                zones.sort_unstable();
                // Two witnesses are the complete commit proof. Omitting later
                // redundant copies keeps trace identity independent of whether
                // an unneeded third lane becomes visible at this audit.
                for zone in zones.into_iter().take(2) {
                    self.emit_for(
                        (writer, owner.1),
                        "RecordPersisted",
                        Some(zone),
                        Some(base),
                        Some(record),
                        Some(value),
                        None,
                        None,
                    );
                    self.trace.last_mut().expect("just emitted").gen = Some(gen);
                }
                self.emit_for(
                    (writer, owner.1),
                    "RecordCommitted",
                    None,
                    Some(base),
                    Some(record),
                    Some(value),
                    None,
                    None,
                );
            }

            let mut finalized: HashMap<(u64, Vec<u8>), Vec<usize>> = HashMap::new();
            for (zone, snapshot) in &snapshots {
                if !snapshot.finalized {
                    continue;
                }
                let Some(records) = decode_all(&snapshot.bytes) else {
                    continue;
                };
                if records.is_empty() {
                    continue;
                }
                let end = base + records.len() as u64 - 1;
                finalized
                    .entry((end, snapshot.bytes.clone()))
                    .or_default()
                    .push(*zone);
            }
            if let Some(((end, bytes), mut zones)) =
                finalized.into_iter().find(|(_, zones)| zones.len() >= 2)
            {
                zones.sort_unstable();
                if self.seen_quorum_enforced.insert((segment.id.clone(), end)) {
                    let segment_id = self.trace_segment_id(&segment.id);
                    self.emit(
                        "SealQuorumEnforced",
                        None,
                        Some(base),
                        None,
                        None,
                        Some(end),
                        None,
                    );
                    self.trace.last_mut().expect("just emitted").segment_id = Some(segment_id);
                    self.pending_gate_releases
                        .push_back((segment.id.clone(), base, end));
                }
                if self.seen_finalized.insert((base, end)) {
                    for zone in &zones {
                        self.emit(
                            "SegmentFinalized",
                            Some(*zone),
                            Some(base),
                            None,
                            None,
                            Some(end),
                            None,
                        );
                    }
                }
                if self.seen_get_sizes.insert((base, true)) {
                    let (zone, (reported_size, finalized)) = zones
                        .iter()
                        .filter_map(|zone| {
                            reported_sizes
                                .get(zone)
                                .copied()
                                .map(|observation| (*zone, observation))
                        })
                        .find(|(_, (_, finalized))| *finalized)
                        .context("finalized quorum lacks a GetObject observation")?;
                    if reported_size != bytes.len() as i64 {
                        bail!("finalized GetObject size differs from object bytes");
                    }
                    self.emit(
                        "GetSizeObserved",
                        Some(zone),
                        Some(base),
                        None,
                        None,
                        None,
                        None,
                    );
                    let event = self.trace.last_mut().expect("just emitted");
                    event.reported_size = Some(reported_size);
                    event.finalized = Some(finalized);
                }
                self.emit_logical_seal(&segment.id, base, end);
            }
        }
        self.emit_gate_releases();
        Ok(())
    }

    fn emit_logical_seal(&mut self, id: &str, base: u64, end: u64) {
        // Recovery may enforce a predecessor before the deferred fold
        // publishes its seal decision. Physical finalization is useful
        // evidence, but it is not a logical sealed-chain observation until
        // the manifest names the decision.
        let enforced = self
            .seen_quorum_enforced
            .iter()
            .any(|(seen_id, seen_end)| seen_id == id && *seen_end == end);
        if enforced
            && self.seen_seal_decisions.contains(&base)
            && self.seen_seals.insert(base, end).is_none()
        {
            self.emit(
                "SegmentSealed",
                None,
                Some(base),
                None,
                None,
                Some(end),
                None,
            );
        }
    }

    fn segment_object(&self, id: &str) -> String {
        format!("{}/segments/{id}", self.prefix)
    }

    /// The generation number of object `id` at segment `base`: the ordinal
    /// of the id among the distinct ids observed at that base, in
    /// observation order.
    fn gen_for(&mut self, base: u64, id: &str) -> u64 {
        let ids = self.segment_gens.entry(base).or_default();
        if let Some(index) = ids.iter().position(|known| known == id) {
            index as u64
        } else {
            ids.push(id.to_string());
            (ids.len() - 1) as u64
        }
    }

    fn trace_segment_id(&mut self, id: &str) -> u64 {
        if let Some(trace_id) = self.trace_segment_ids.get(id) {
            return *trace_id;
        }
        let trace_id = self.next_trace_segment_id;
        self.next_trace_segment_id += 1;
        self.trace_segment_ids.insert(id.to_string(), trace_id);
        trace_id
    }

    fn emit_gate_releases(&mut self) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        let observed = metrics.counter("chorus.wal.seal.segments");
        let newly_released = observed.saturating_sub(self.observed_seal_metric);
        self.observed_seal_metric = observed;
        for _ in 0..newly_released {
            while let Some((id, base, end)) = self.pending_gate_releases.pop_front() {
                if !self.seen_gate_releases.insert(id.clone()) {
                    continue;
                }
                let segment_id = self.trace_segment_id(&id);
                self.emit(
                    "RotationGateReleased",
                    None,
                    Some(base),
                    None,
                    None,
                    Some(end),
                    None,
                );
                self.trace.last_mut().expect("just emitted").segment_id = Some(segment_id);
                break;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_for(
        &mut self,
        identity: (u64, u64),
        event: &str,
        zone: Option<usize>,
        segment: Option<u64>,
        logical_offset: Option<u64>,
        value: Option<u64>,
        record_end: Option<u64>,
        reader: Option<u64>,
    ) {
        self.trace.push(TraceEvent {
            seq: self.sequence,
            time_ms: self.logical_time,
            event: event.into(),
            writer: identity.0,
            epoch: identity.1,
            zone,
            segment,
            gen: None,
            logical_offset,
            value,
            record_end,
            segment_id: None,
            current_seal_id: None,
            tail_base: None,
            seal_base: None,
            directory_index: None,
            directory_len: None,
            truncation_floor: None,
            reader,
            reported_size: None,
            finalized: None,
        });
        self.sequence += 1;
    }

    async fn write_with_zone_down(&mut self) -> Result<()> {
        let zone = self.rng.random_range(0..3);
        self.servers[zone].set_crashed(true).await;
        self.crashed[zone] = true;
        self.emit("ZoneCrash", Some(zone), None, None, None, None, None);
        let (receipts, _poisoned, _faulted, _directory_full) =
            self.submit_spike_without_audit(2).await?;
        self.servers[zone].set_crashed(false).await;
        self.crashed[zone] = false;
        self.emit("ZoneRestart", Some(zone), None, None, None, None, None);
        self.observe_receipts(receipts).await?;
        // the trailing restart recovers from a poisoned outcome as well
        self.restart_engine().await
    }

    async fn restart_with_zone_down(&mut self) -> Result<()> {
        let zone = self.rng.random_range(0..3);
        self.servers[zone].set_crashed(true).await;
        self.crashed[zone] = true;
        self.emit("ZoneCrash", Some(zone), None, None, None, None, None);
        self.restart_engine().await?;
        let (receipts, _poisoned, _faulted, _directory_full) =
            self.submit_spike_without_audit(1).await?;
        self.servers[zone].set_crashed(false).await;
        self.crashed[zone] = false;
        self.emit("ZoneRestart", Some(zone), None, None, None, None, None);
        self.observe_receipts(receipts).await?;
        self.restart_engine().await
    }

    async fn force_takeover(&mut self) -> Result<()> {
        self.audit().await?;
        let mut stale = self.handle.take().context("engine not running")?;
        self.writer_incarnation += 1;
        self.epoch = self.writer_incarnation;
        self.emit("WriterRestart", None, None, None, None, None, None);
        let replacement = self.observe_recovery().await?;
        let replacement_end = replacement.end.record_index;
        let stale_result = match stale
            .enqueue_append(
                WalSeqNo::record(self.next_seqno),
                Bytes::from_static(b"deposed"),
            )
            .await
        {
            Ok(completion) => completion.await,
            Err(error) => Err(error),
        };
        if stale_result.is_ok() {
            bail!("deposed production writer committed after takeover");
        }
        stale.abort().await;
        self.next_seqno = replacement_end;
        // the replacement recovery minted a fresh active id; the first
        // audit to observe it assigns this incarnation via gen keying
        self.handle = Some(replacement.start(self.engine_config.clone()).await?);
        self.wait_for_repair_pass(1).await?;
        self.audit().await
    }

    async fn truncate(&mut self) -> Result<()> {
        let catalog = self.catalog().await?;
        let Some(target_floor) = catalog
            .iter()
            .filter_map(|segment| segment.end_record_index.map(|end| end + 1))
            .find(|floor| *floor > self.truncation_floor)
        else {
            return Ok(());
        };
        self.emit("TruncationProposed", None, None, None, None, None, None);
        self.trace.last_mut().unwrap().truncation_floor = Some(target_floor);
        let before = catalog;
        let mut existing = HashSet::new();
        for segment in &before {
            let object = self.segment_object(&segment.id);
            for zone in 0..3 {
                if self.servers[zone]
                    .reported_size_for(&format!("{BUCKET_PREFIX}{zone}"), &object)
                    .await
                    .is_some()
                {
                    existing.insert((segment.base_record_index, zone));
                }
            }
        }
        let _report = self
            .handle
            .as_ref()
            .context("engine not running")?
            .truncate_before(WalSeqNo::record(target_floor))
            .await?;
        let manifest = self.manifest_observation().await?;
        if manifest.truncation_floor < target_floor {
            bail!(
                "truncate returned before committing floor {target_floor}; manifest holds {}",
                manifest.truncation_floor
            );
        }
        self.emit_manifest_view(&manifest);
        for segment in &before {
            if let Some(end) = segment.end_record_index {
                self.emit_logical_seal(&segment.id, segment.base_record_index, end);
            }
        }
        self.truncation_floor = manifest.truncation_floor;
        self.emit("FloorCommitted", None, None, None, None, None, None);
        self.trace.last_mut().unwrap().truncation_floor = Some(self.truncation_floor);
        for segment in before.into_iter().filter(|segment| {
            segment
                .end_record_index
                .is_some_and(|end| end < target_floor)
        }) {
            let object = self.segment_object(&segment.id);
            let mut deleted = Vec::new();
            for zone in 0..3 {
                if existing.contains(&(segment.base_record_index, zone))
                    && self.servers[zone]
                        .reported_size_for(&format!("{BUCKET_PREFIX}{zone}"), &object)
                        .await
                        .is_none()
                {
                    deleted.push(zone);
                }
            }
            if deleted.len() < 2 {
                bail!(
                    "truncation advanced past segment {} without deleting a quorum",
                    segment.base_record_index
                );
            }
            for zone in deleted {
                self.emit(
                    "SegmentDeleted",
                    Some(zone),
                    Some(segment.base_record_index),
                    None,
                    None,
                    segment.end_record_index,
                    None,
                );
                self.trace.last_mut().unwrap().truncation_floor = Some(target_floor);
            }
        }
        Ok(())
    }

    async fn restore_rotation_capacity(&mut self) -> Result<()> {
        for zone in 0..3 {
            if self.crashed[zone] {
                self.servers[zone].set_crashed(false).await;
                self.crashed[zone] = false;
                self.emit("ZoneRestart", Some(zone), None, None, None, None, None);
            }
        }
        for _ in 0..32 {
            let before = self.manifest_observation().await?;
            if chorus_client::dst_support::gcs_segment_directory_has_room(
                &before.segments_encoded,
                2,
            ) {
                return Ok(());
            }
            self.truncate().await?;
            let after = self.manifest_observation().await?;
            if after.segments.len() >= before.segments.len() {
                bail!(
                    "directory backpressure could not free a retained segment: \
                     entries={} floor={}",
                    after.segments.len(),
                    after.truncation_floor
                );
            }
        }
        bail!("directory backpressure did not restore the two-entry rotation reserve")
    }

    async fn exercise_background_repair(&mut self) -> Result<()> {
        let catalog = self.catalog().await?;
        let sealed: Vec<_> = catalog
            .into_iter()
            .filter(|segment| segment.end_record_index.is_some())
            .collect();
        self.restart_engine().await?;
        self.handle.as_ref().context("engine not running")?;
        let repair_passes = self
            .metrics
            .as_ref()
            .context("metrics recorder not installed")?
            .counter("chorus.wal.repair.passes");
        if repair_passes == 0 {
            bail!("engine restart did not run sealed repair")
        }
        for segment in sealed {
            let object = self.segment_object(&segment.id);
            for zone in 0..3 {
                let snapshot = self.servers[zone]
                    .observe_prefix(&format!("{BUCKET_PREFIX}{zone}"), &object)
                    .await
                    .into_iter()
                    .find(|candidate| candidate.name == object)
                    .context("sealed repair left a missing object")?;
                if !sealed_copy_is_healthy(&snapshot, &segment) {
                    bail!(
                        "sealed repair left zone {zone} segment {} unhealthy: \
                         expected_end={:?} expected_crc32c={:?} actual_bytes={} \
                         actual_records={:?} actual_finalized={} actual_crc32c={:?} \
                         computed_crc32c={:08x} format={:?}",
                        segment.base_record_index,
                        segment.end_record_index,
                        segment.crc32c,
                        snapshot.bytes.len(),
                        decode_all(&snapshot.bytes).map(|records| records.len()),
                        snapshot.finalized,
                        snapshot.crc32c,
                        crc32c::crc32c(&snapshot.bytes),
                        snapshot.metadata.get("chorus.format"),
                    );
                }
            }
        }
        Ok(())
    }

    async fn inject_transient(&mut self) {
        let zone = self.rng.random_range(0..3);
        let (code, event) = if self.rng.random_bool(0.5) {
            (Code::Unavailable, "RpcUnavailable")
        } else {
            (Code::DeadlineExceeded, "RpcDeadlineExceeded")
        };
        self.servers[zone].inject(Operation::BidiWrite, code).await;
        self.emit(event, Some(zone), None, None, None, None, None);
    }

    async fn inject_reordering(&mut self) {
        let zone = self.rng.random_range(0..3);
        self.servers[zone]
            .inject_delay(Operation::BidiWrite, Duration::from_millis(2))
            .await;
        self.emit("RpcDropped", Some(zone), None, None, None, None, None);
    }

    /// Adversarial check for single-pending exhaustion. The first oversized
    /// record consumes the one preregistered pending segment. Refill creates
    /// are held, so the second oversized record commits on that segment and
    /// makes another rotation due with no registered successor. Dispatch must
    /// then remain fail-closed until refill and fold complete.
    ///
    /// The trap walks the engine there one record at a time so every rotation
    /// decision lands at a chosen record instead of a byte-count race. Each
    /// record alone overflows the segment budget; `inject_open_hold` holds
    /// only appendable CREATES, exactly the expensive awaits inside refill
    /// provisioning. Records [1]+[2] consume and fill the registered pending
    /// segment while its replacement remains unavailable. Records [3..5] can
    /// only queue. Releasing the holds lets the provisioner create the refill,
    /// atomically fold the old tail and register it, and wake dispatch.
    async fn delayed_spare_provisioning(&mut self, remaining: u64) -> Result<()> {
        // The walk needs a healthy quorum and five oversized records;
        // degraded seeds exercise provisioning delay through other phases.
        if remaining < 5 || (0..3).any(|zone| self.crashed[zone]) {
            return self.submit_spike(remaining.min(8) as usize).await;
        }
        let manifest = self.manifest_observation().await?;
        if !chorus_client::dst_support::gcs_segment_directory_has_room(
            &manifest.segments_encoded,
            6,
        ) {
            return self.submit_spike(remaining.min(8) as usize).await;
        }
        let Some((tail, pending)) = self.wait_for_trap_baseline().await else {
            return self.submit_spike(remaining.min(8) as usize).await;
        };
        for zone in 0..3 {
            self.servers[zone].inject_open_hold().await;
            self.emit("RpcDropped", Some(zone), None, None, None, None, None);
        }
        self.emit("ProducerSpike", None, None, None, None, None, None);
        let payload_len = self.engine_config.max_segment_bytes + 96;
        let walk = async {
            let mut receipts = Vec::new();
            for _ in 0..2 {
                match self.settle_trap_record(payload_len, &mut receipts).await? {
                    TrapStep::Committed => {}
                    TrapStep::Poisoned => return Ok(TrapWalk::Poisoned(receipts)),
                }
            }
            self.wait_for_pre_fold_window(&tail, &pending).await?;
            // [3..5]: the parked engine can only queue these
            let mut parked_completions = Vec::new();
            for _ in 0..3 {
                parked_completions.push(self.admit_padded(payload_len).await?);
            }
            // let the engine absorb the queue wakes and park for good before
            // the release: from here its only legitimate wake is the
            // provisioning completion itself
            advance_virtual_ticks(SETTLING_MARGIN_TICKS).await;
            let mut parked = Box::pin(join_all(parked_completions));
            if let Ok(results) = tokio::time::timeout(SIM_TICK, &mut parked).await {
                for zone in 0..3 {
                    self.servers[zone].release_open_holds().await;
                }
                if results.iter().any(Result::is_ok) {
                    bail!("pending exhaustion acknowledged a queued record before refill");
                }
                return Ok(TrapWalk::Poisoned(receipts));
            }
            for zone in 0..3 {
                self.servers[zone].release_open_holds().await;
            }
            let mut poisoned = false;
            for result in parked.await {
                match result {
                    Ok(receipt) => receipts.push(receipt),
                    Err(chorus_client::Error::Poisoned)
                    | Err(chorus_client::Error::Fenced(_))
                    | Err(chorus_client::Error::Closed) => {
                        poisoned = true;
                    }
                    Err(error) => {
                        return Err(error).context("delayed-spare trap completion failed");
                    }
                }
            }
            Ok(if poisoned {
                TrapWalk::Poisoned(receipts)
            } else {
                TrapWalk::Committed(receipts)
            })
        };
        // A broken provisioning wake-up deadlocks the engine; fail the seed
        // instead of hanging the suite. The bound is virtual time — generous,
        // because the walk's bounded polling waits may consume virtual minutes
        // while costing almost nothing real.
        let walk = tokio::time::timeout(Duration::from_secs(600), walk)
            .await
            .context(
                "delayed-spare-provisioning spike stalled: \
                 engine no longer wakes on provisioning completion",
            )??;
        match walk {
            TrapWalk::Committed(receipts) => {
                self.wait_for_rotations_settled(Some(&tail)).await?;
                self.observe_receipts(receipts).await
            }
            TrapWalk::Poisoned(receipts) => self.abandon_trap(receipts).await,
        }
    }

    /// Admit one record whose payload is padded past the segment budget,
    /// carrying the spike bookkeeping `submit_spike_without_audit` does.
    async fn admit_padded(&mut self, payload_len: usize) -> Result<AppendCompletion> {
        let value = self.submitted;
        let seqno = self.next_seqno;
        let mut payload = format!("value-{value}").into_bytes();
        payload.resize(payload_len, b'x');
        let handle = self.handle.as_mut().context("engine not running")?;
        let completion = handle
            .enqueue_append(WalSeqNo::record(seqno), Bytes::from(payload))
            .await
            .with_context(|| format!("failed to admit record {seqno}"))?;
        self.record_writers.insert(seqno, self.writer_incarnation);
        self.submitted += 1;
        self.next_seqno += 1;
        Ok(completion)
    }

    /// Admit one padded record and await its commit, classifying a poisoned
    /// writer as a step outcome instead of a failure (a healthy-quorum seed
    /// should never poison here, but the documented reaction is restart, not
    /// a failed seed).
    async fn settle_trap_record(
        &mut self,
        payload_len: usize,
        receipts: &mut Vec<AppendReceipt>,
    ) -> Result<TrapStep> {
        let completion = match self.admit_padded(payload_len).await {
            Ok(completion) => completion,
            Err(error)
                if matches!(
                    error.downcast_ref::<chorus_client::Error>(),
                    Some(chorus_client::Error::Poisoned)
                        | Some(chorus_client::Error::Fenced(_))
                        | Some(chorus_client::Error::Closed)
                ) =>
            {
                return Ok(TrapStep::Poisoned)
            }
            Err(error) => return Err(error),
        };
        match completion.await {
            Ok(receipt) => {
                receipts.push(receipt);
                Ok(TrapStep::Committed)
            }
            Err(chorus_client::Error::Poisoned)
            | Err(chorus_client::Error::Fenced(_))
            | Err(chorus_client::Error::Closed) => Ok(TrapStep::Poisoned),
            Err(error) => Err(error).context("delayed-spare trap record failed"),
        }
    }

    /// The poisoned bail-out of the trap: release every hold, recover, and
    /// account for what did commit.
    async fn abandon_trap(&mut self, receipts: Vec<AppendReceipt>) -> Result<()> {
        for zone in 0..3 {
            self.servers[zone].release_open_holds().await;
        }
        self.observe_receipts(receipts).await?;
        self.restart_engine().await
    }

    /// Crash after the hot path has rotated into the preregistered pending
    /// segment and acknowledged records there, but before the background fold
    /// CAS. Refill creates are held to keep that window open. Recovery must
    /// walk `[tail, pending]`, replay both committed prefixes, and establish a
    /// fresh empty pending segment before append admission resumes.
    async fn crash_in_swap_window(&mut self, remaining: u64) -> Result<()> {
        if remaining < 3 || (0..3).any(|zone| self.crashed[zone]) {
            return self.submit_spike(remaining.min(8) as usize).await;
        }
        let Some((tail, pending)) = self.wait_for_trap_baseline().await else {
            return self.submit_spike(remaining.min(8) as usize).await;
        };
        for zone in 0..3 {
            self.servers[zone].inject_open_hold().await;
            self.emit("RpcDropped", Some(zone), None, None, None, None, None);
        }
        self.emit("ProducerSpike", None, None, None, None, None, None);
        let mut receipts = Vec::new();
        for payload_len in [self.engine_config.max_segment_bytes + 96, 64, 64] {
            match self.settle_trap_record(payload_len, &mut receipts).await? {
                TrapStep::Committed => {}
                TrapStep::Poisoned => return self.abandon_trap(receipts).await,
            }
        }
        self.wait_for_pre_fold_window(&tail, &pending).await?;
        let expected_end = self.next_seqno;
        self.observe_receipts(receipts).await?;
        let stale = self.handle.take().context("engine not running")?;
        stale.abort().await;
        self.emit("WriterCrash", None, None, None, None, None, None);
        for zone in 0..3 {
            self.servers[zone].release_open_holds().await;
        }
        self.restart_engine()
            .await
            .context("recovery wedged after the pre-fold pending crash")?;
        if self.next_seqno != expected_end {
            bail!(
                "pre-fold recovery lost acknowledged records: expected {expected_end}, recovered {}",
                self.next_seqno
            );
        }
        self.wait_for_rotations_settled(None).await?;
        let manifest = self.manifest_observation().await?;
        if !manifest.segments.iter().any(|entry| entry.id == pending) {
            bail!("recovery did not fold the acknowledged pending segment into the directory");
        }
        let fresh_pending = manifest
            .pending_id
            .as_deref()
            .context("recovery resumed without a registered pending segment")?;
        if fresh_pending == pending {
            bail!("recovery reused the consumed pending id instead of registering a fresh spare");
        }
        Ok(())
    }

    /// Adversarial check for empty-tail name retirement. A quorum that reads
    /// the tail empty cannot rule out a copy on the unreachable third zone
    /// carrying a dead incarnation's unacknowledged bytes. Recovery commits
    /// that decision through a CAS which retires the old id and names a fresh
    /// successor before discarding the witnesses. The stale copy then remains
    /// under a retired name no incarnation will append to, and a later recovery
    /// sweeps it as a dead writer's orphan.
    ///
    /// The trap builds the stale copy deliberately: a rotation makes the
    /// manifest tail a fresh empty object on all three zones, flush holds
    /// pin zones 1 and 2, and one unacknowledged record lands on zone 0
    /// alone. The writer crashes, zone 0 goes down holding the only copy,
    /// and recovery on zones 1 and 2 reads the tail empty — the retirement
    /// CAS must land before the witnesses are discarded. The replacement
    /// then commits real records at the phantom's offset on zones 1 and 2,
    /// zone 0 returns as zone 1 goes down, and the final recovery pairs
    /// zone 0 (stale) with zone 2 (live). The stale copy is an orphan under a
    /// retired name and is swept while zone 2's lone witness promotes; no
    /// current object id has incompatible witnesses.
    async fn stale_zone_empty_tail(&mut self, remaining: u64) -> Result<()> {
        // the walk needs a healthy quorum, one rotation record, one
        // sacrificial record, and two records for the replacement writer
        if remaining < 4 || (0..3).any(|zone| self.crashed[zone]) {
            return self.submit_spike(remaining.min(8) as usize).await;
        }
        if self.wait_for_trap_baseline().await.is_none() {
            return self.submit_spike(remaining.min(8) as usize).await;
        }
        // one oversized record forces a rotation: the manifest tail becomes
        // a fresh object, empty on all three zones
        self.emit("ProducerSpike", None, None, None, None, None, None);
        let mut receipts = Vec::new();
        match self
            .settle_trap_record(self.engine_config.max_segment_bytes + 96, &mut receipts)
            .await?
        {
            TrapStep::Committed => {}
            TrapStep::Poisoned => return self.abandon_trap(receipts).await,
        }
        // the swap the oversized record forced must land before the
        // baseline samples the manifest, or the walk starts on the tail the
        // engine is about to seal
        self.wait_for_rotations_settled(None).await?;
        let Some((tail, _pending)) = self.wait_for_trap_baseline().await else {
            self.observe_receipts(receipts).await?;
            return Ok(());
        };
        let tail_object = self.segment_object(&tail);
        self.observe_receipts(receipts).await?;
        // park zones 1 and 2; the phantom record reaches zone 0 alone
        for zone in 1..3 {
            self.servers[zone].inject_flush_hold().await;
            self.emit("RpcDropped", Some(zone), None, None, None, None, None);
        }
        self.emit("ProducerSpike", None, None, None, None, None, None);
        let committed_end = self.next_seqno;
        let phantom = self.admit_padded(64).await?;
        self.wait_for_zone_bytes(0, &tail_object, 1).await?;
        // crash with the phantom below quorum, then take down the one zone
        // that applied it
        let stale = self.handle.take().context("engine not running")?;
        stale.abort().await;
        self.emit("WriterCrash", None, None, None, None, None, None);
        if phantom.await.is_ok() {
            bail!("a record below quorum was acknowledged");
        }
        self.servers[0].set_crashed(true).await;
        self.crashed[0] = true;
        self.emit("ZoneCrash", Some(0), None, None, None, None, None);
        // zones 1 and 2 read the tail empty: the recovery must retire the
        // tail id it cannot prove empty before discarding the witnesses
        self.restart_engine()
            .await
            .context("empty-tail recovery wedged")?;
        if self.next_seqno != committed_end {
            bail!(
                "empty-tail recovery moved the committed end: expected {committed_end}, \
                 recovered {}",
                self.next_seqno
            );
        }
        let manifest = self.manifest_observation().await?;
        if manifest.tail_id.as_deref() == Some(tail.as_str()) {
            bail!("recovery kept the tail id it could not prove empty");
        }
        // released flushes fail against revoked streams and discarded
        // objects without mutating anything
        for zone in 1..3 {
            self.servers[zone].release_flush_holds().await;
        }
        // the replacement commits real records at the phantom's offset, on
        // zones 1 and 2 alone
        self.submit_spike(2).await?;
        // zone 0 returns holding the stale copy as zone 1 goes down:
        // recovery must pair the stale zone with one live witness
        self.servers[0].set_crashed(false).await;
        self.crashed[0] = false;
        self.emit("ZoneRestart", Some(0), None, None, None, None, None);
        self.servers[1].set_crashed(true).await;
        self.crashed[1] = true;
        self.emit("ZoneCrash", Some(1), None, None, None, None, None);
        self.restart_engine()
            .await
            .context("recovery wedged pairing the stale zone with a live witness")?;
        if self.next_seqno != committed_end + 2 {
            bail!(
                "the stale zone's retired-tail copy corrupted recovery: expected {}, \
                 recovered {}",
                committed_end + 2,
                self.next_seqno
            );
        }
        self.servers[1].set_crashed(false).await;
        self.crashed[1] = false;
        self.emit("ZoneRestart", Some(1), None, None, None, None, None);
        // The first sweep could not delete zone 1's empty retired copy while
        // that zone was down. Restart with every zone up so startup cleanup
        // removes every copy of the retired id and later baselines see a fully
        // provisioned engine.
        self.restart_engine().await?;
        wait_for_predicate("startup cleanup to remove every retired-tail copy", || {
            let harness = &*self;
            let object = tail_object.as_str();
            async move {
                Ok(harness
                    .object_observations(object)
                    .await
                    .is_empty()
                    .then_some(()))
            }
        })
        .await
    }

    /// Adversarial check that the database checkpoint positions replay but
    /// carries no authority over chain membership. The fold CAS commits the
    /// seal record — and its segment-directory entry — before maintenance
    /// finalizes the sealed object, so a crash in between leaves one chain
    /// member whose enforcement is still owed: exactly the one segment
    /// recovery enforces, the manifest's `seal_id`.
    ///
    /// The trap parks the seal's finalize request on two of the three zones,
    /// so the finalize lands on exactly one open copy and no newly finalized
    /// quorum exists. It crashes the writer inside the
    /// committed-but-unenforced window and recovers with a checkpoint PAST
    /// the segment's end. The assertion is that recovery kept the chain
    /// member — retention follows the committed truncation floor, not the
    /// caller's checkpoint — and enforced its seal to a finalized quorum,
    /// followed by a normal restart that replays across it.
    async fn recover_past_unenforced_seal(&mut self, remaining: u64) -> Result<()> {
        // the walk needs a healthy quorum and one rotation record
        if remaining < 2 || (0..3).any(|zone| self.crashed[zone]) {
            return self.submit_spike(remaining.min(8) as usize).await;
        }
        let Some((tail, _pending)) = self.wait_for_trap_baseline().await else {
            return self.submit_spike(remaining.min(8) as usize).await;
        };
        let tail_object = self.segment_object(&tail);
        // Settle a quorum at the most advanced observed length before arming.
        // One lane may legitimately remain behind a 2/3 commit forever.
        let mut applied = 0;
        for zone in 0..3 {
            applied = applied.max(self.zone_bytes(zone, &tail_object).await);
        }
        self.wait_for_quorum_zone_bytes(&tail_object, applied)
            .await?;
        let mut baseline_finalized = [false; 3];
        for (zone, snapshot) in self.object_observations(&tail_object).await {
            baseline_finalized[zone] = snapshot.finalized;
        }
        let Some(unheld_zone) = baseline_finalized.iter().position(|finalized| !finalized) else {
            return self.submit_spike(remaining.min(8) as usize).await;
        };
        let held_zones = (0..3)
            .filter(|&zone| zone != unheld_zone)
            .collect::<Vec<_>>();
        for &zone in &held_zones {
            self.servers[zone].inject_finalize_hold().await;
            self.emit("RpcDropped", Some(zone), None, None, None, None, None);
        }
        self.emit("ProducerSpike", None, None, None, None, None, None);
        let mut receipts = Vec::new();
        match self
            .settle_trap_record(self.engine_config.max_segment_bytes + 96, &mut receipts)
            .await?
        {
            TrapStep::Committed => {}
            TrapStep::Poisoned => {
                for &zone in &held_zones {
                    self.servers[zone].release_finalize_holds().await;
                }
                return self.abandon_trap(receipts).await;
            }
        }
        // the fold CAS lands — the manifest names the old tail as its seal
        // record — and the finalize reaches one open copy while the armed
        // holds park every other zone, short of a newly finalized quorum
        let manifest = self.wait_for_manifest_seal(&tail).await?;
        self.wait_for_zone_finalized(unheld_zone, &tail_object)
            .await?;
        self.observe_receipts(receipts).await?;
        // crash inside the committed-but-unenforced window
        let stale = self.handle.take().context("engine not running")?;
        stale.abort().await;
        self.emit("WriterCrash", None, None, None, None, None, None);
        for (zone, snapshot) in self.object_observations(&tail_object).await {
            if zone != unheld_zone && snapshot.finalized && !baseline_finalized[zone] {
                bail!("zone {zone} newly finalized a seal while finalization was held");
            }
        }
        for &zone in &held_zones {
            self.servers[zone].release_finalize_holds().await;
        }
        // Recover with the database checkpoint PAST the unenforced seal's
        // end. Chain retention follows the committed truncation floor, so
        // recovery still enforces the seal.
        let expected_end = self.next_seqno;
        if manifest.tail_base != expected_end {
            bail!(
                "swap committed tail base {}; expected {expected_end}",
                manifest.tail_base
            );
        }
        self.restart_engine_at(manifest.tail_base)
            .await
            .context("recovery wedged on a checkpoint past the unenforced seal")?;
        if self.next_seqno != expected_end {
            bail!(
                "checkpointed recovery moved the committed end: expected {expected_end}, \
                 recovered {}",
                self.next_seqno
            );
        }
        // recovery must enforce the seal to a finalized quorum: otherwise a
        // later recovery that cannot reach zone 0 could not satisfy the
        // committed seal at all
        let finalized = self
            .object_observations(&tail_object)
            .await
            .into_iter()
            .filter(|(_, snapshot)| snapshot.finalized)
            .count();
        if finalized < 2 {
            bail!("a checkpoint past the unenforced seal stripped it of enforcement");
        }
        // A normal restart replays across the recovered segment from the
        // committed floor: the chain member survived its vulnerable window.
        self.restart_engine()
            .await
            .context("replay across the recovered seal failed")?;
        if self.next_seqno != expected_end {
            bail!(
                "replay across the recovered seal lost records: expected {expected_end}, \
                 recovered {}",
                self.next_seqno
            );
        }
        Ok(())
    }

    /// Race an append already in flight on stale sessions against a new epoch.
    /// Recovery resolves each candidate's current generation, then fences that
    /// exact generation with one takeover open before using its returned
    /// persisted size. The metadata `Get` is tail-blind and supplies identity
    /// only; no content read may precede the takeover.
    async fn racing_recoveries(&mut self) -> Result<()> {
        if self.crashed.iter().any(|&down| down) {
            return Ok(());
        }
        self.audit().await?;
        let main = self.handle.take().context("engine not running")?;
        main.abort().await;
        self.emit("WriterCrash", None, None, None, None, None, None);

        let prefix = format!("{}/racing-recoveries/{}", self.prefix, self.logical_time);
        let initial_volume = self.volume_at(&prefix);
        let mut initial_recovery = initial_volume.recover(WalSeqNo::ZERO).await?;
        while initial_recovery.try_next().await?.is_some() {}
        let mut initial_writer = initial_recovery.start(self.engine_config.clone()).await?;
        let payload = Bytes::from(format!("racing-recoveries-{}", self.logical_time));
        let receipt = initial_writer
            .enqueue_append(WalSeqNo::ZERO, payload.clone())
            .await?
            .await?;
        if receipt.seqno != WalSeqNo::ZERO {
            bail!("racing-recoveries setup acknowledged an unexpected sequence number");
        }

        let initial_manifest = self.manifest_observation_at(&prefix).await?;
        initial_manifest
            .tail_id
            .as_deref()
            .context("racing-recoveries setup has no active tail")?;
        let pending = initial_manifest
            .pending_id
            .clone()
            .context("racing-recoveries setup has no registered pending segment")?;
        let pending_object = format!("{prefix}/segments/{pending}");
        wait_for_predicate(
            "the racing-recoveries pending segment to exist in every zone",
            || {
                let harness = &*self;
                let object = pending_object.as_str();
                async move {
                    let copies = harness.object_observations(object).await.len();
                    Ok((copies == 3).then_some(()))
                }
            },
        )
        .await?;
        for server in &self.servers {
            server.reset_operation_counts().await;
            server.inject_flush_hold().await;
        }
        let stale_completion = initial_writer
            .enqueue_append(WalSeqNo::record(1), Bytes::from_static(b"stale-race"))
            .await?;
        wait_for_predicate("the stale append to reach every held lane", || {
            let servers = self.servers.clone();
            async move {
                for server in servers {
                    if server.operation_count(Operation::BidiAppendFlush).await == 0 {
                        return Ok(None);
                    }
                }
                Ok(Some(()))
            }
        })
        .await?;
        for server in &self.servers {
            server.reset_operation_counts().await;
        }

        let mut recovery = self.volume_at(&prefix).recover(WalSeqNo::ZERO).await?;
        let recovered_end = recovery.end.record_index;
        let mut records = Vec::new();
        while let Some(record) = recovery.try_next().await? {
            records.push(record);
        }
        if recovered_end != receipt.next_seqno().record_index
            || records
                != [WalRecord {
                    seqno: WalSeqNo::ZERO,
                    payload,
                }]
        {
            bail!("takeover recovery lost the acknowledged setup record");
        }
        for (zone, server) in self.servers.iter().enumerate() {
            let takeovers = server.operation_count(Operation::BidiTakeoverOpen).await;
            let operations = server.operation_log().await;
            let first_takeover = operations
                .iter()
                .position(|operation| *operation == Operation::BidiTakeoverOpen);
            let first_read = operations
                .iter()
                .position(|operation| *operation == Operation::Read);
            if takeovers != 2
                || operations.first().copied() != Some(Operation::Get)
                || first_takeover != Some(1)
                || first_read
                    .is_some_and(|read| first_takeover.is_none_or(|takeover| read < takeover))
            {
                initial_writer.abort().await;
                bail!(
                    "zone {zone} recovery used takeovers={takeovers}, operations={operations:?}; \
                     expected current-generation lookup then takeover before any content read, \
                     with one takeover for each of [tail,pending]"
                );
            }
            server.release_flush_holds().await;
        }
        let mut replacement = recovery.start(self.engine_config.clone()).await?;
        if stale_completion.await.is_ok() {
            replacement.abort().await;
            initial_writer.abort().await;
            bail!("stale append committed after the new epoch's takeover fence");
        }
        replacement
            .enqueue_append(WalSeqNo::record(1), Bytes::from_static(b"winner"))
            .await?
            .await?;
        replacement.abort().await;
        initial_writer.abort().await;
        self.restart_engine().await
    }

    /// Block until the manifest's seal record names `seal`: the fold CAS
    /// committed.
    async fn wait_for_manifest_seal(&self, seal: &str) -> Result<ManifestObservation> {
        let description = format!("the fold CAS to commit seal record {seal}");
        wait_for_predicate(&description, || {
            let harness = self;
            async move {
                let manifest = harness.manifest_observation().await.ok();
                Ok(manifest.filter(|manifest| manifest.seal_id.as_deref() == Some(seal)))
            }
        })
        .await
    }

    /// Block until `zone`'s copy of `object` is finalized: the maintenance
    /// seal reached the one zone the trap left unparked.
    async fn wait_for_zone_finalized(&self, zone: usize, object: &str) -> Result<()> {
        let description = format!("the seal finalize to reach zone {zone} for {object}");
        wait_for_predicate(&description, || {
            let harness = self;
            async move {
                let finalized = harness
                    .object_observations(object)
                    .await
                    .into_iter()
                    .any(|(z, snapshot)| z == zone && snapshot.finalized);
                Ok(finalized.then_some(()))
            }
        })
        .await
    }

    /// The byte length `object` currently carries on `zone` (zero if absent).
    async fn zone_bytes(&self, zone: usize, object: &str) -> usize {
        self.servers[zone]
            .observe_prefix(&format!("{BUCKET_PREFIX}{zone}"), object)
            .await
            .into_iter()
            .find(|candidate| candidate.name == object)
            .map(|candidate| candidate.bytes.len())
            .unwrap_or(0)
    }

    /// Block until `object` carries at least `min_len` bytes on `zone`,
    /// returning the length observed when the threshold was met.
    async fn wait_for_zone_bytes(
        &self,
        zone: usize,
        object: &str,
        min_len: usize,
    ) -> Result<usize> {
        let description = format!("zone {zone} to apply {min_len} bytes to {object}");
        wait_for_predicate(&description, || {
            let harness = self;
            async move {
                let len = harness.zone_bytes(zone, object).await;
                Ok((len >= min_len).then_some(len))
            }
        })
        .await
    }

    /// Block until any read quorum carries at least `min_len` bytes.
    async fn wait_for_quorum_zone_bytes(&self, object: &str, min_len: usize) -> Result<()> {
        let description = format!("a zone quorum to apply {min_len} bytes to {object}");
        wait_for_predicate(&description, || {
            let harness = self;
            async move {
                let mut reached = 0usize;
                for zone in 0..3 {
                    reached += usize::from(harness.zone_bytes(zone, object).await >= min_len);
                }
                Ok((reached >= 2).then_some(()))
            }
        })
        .await
    }

    /// Block until acknowledged records are visible on a quorum of the
    /// consumed pending segment while the manifest still carries the pre-fold
    /// `[tail, pending]` pair.
    async fn wait_for_pre_fold_window(&self, tail: &str, pending: &str) -> Result<()> {
        let pending_object = self.segment_object(pending);
        wait_for_predicate("acknowledged records to reach pending before the fold", || {
            let harness = self;
            let pending_object = pending_object.as_str();
            async move {
                let Some(manifest) = harness.manifest_observation().await.ok() else {
                    return Ok(None);
                };
                if manifest.tail_id.as_deref() != Some(tail)
                    || manifest.pending_id.as_deref() != Some(pending)
                {
                    bail!("background fold landed before the held-refill trap observed its window");
                }
                let mut nonempty = 0usize;
                for zone in 0..3 {
                    nonempty += usize::from(
                        harness.zone_bytes(zone, pending_object).await > 0,
                    );
                }
                Ok((nonempty >= 2).then_some(()))
            }
        })
        .await
    }

    /// Block until the manifest's registered pending segment exists empty in
    /// every zone and the last committed seal, if any, shows a finalized
    /// quorum. The trap's holds must catch the refill spawned by its own
    /// rotation rather than an earlier preregistration still in flight.
    ///
    /// Returns `None` up front when the segment directory cannot hold two more
    /// entries. A swap-window crash forces recovery to seal both the old tail
    /// and the consumed pending, so the engine only admits a swap with room for
    /// two; with less room it defers the rotation and the trap's forced records
    /// would never reach pending. Directory room is replenished only by the
    /// `truncate` phase — never mid-trap, and rotations have already settled —
    /// so a tight directory will not loosen during the wait. Bail immediately
    /// and let the caller fall back to a plain spike instead of polling to a
    /// guaranteed timeout.
    async fn wait_for_trap_baseline(&self) -> Option<(String, String)> {
        if let Ok(manifest) = self.manifest_observation().await {
            if !chorus_client::dst_support::gcs_segment_directory_has_room(
                &manifest.segments_encoded,
                2,
            ) {
                return None;
            }
        }
        let prefix = format!("{}/segments/", self.prefix);
        wait_for_settled_predicate("a fully provisioned trap baseline", || {
            let harness = self;
            let prefix = prefix.as_str();
            async move {
                let Some(manifest) = harness.manifest_observation().await.ok() else {
                    return Ok(None);
                };
                let Some(tail_id) = manifest.tail_id.clone() else {
                    return Ok(None);
                };
                let Some(pending_id) = manifest.pending_id.clone() else {
                    return Ok(None);
                };
                let mut spare_zones = 0usize;
                let mut enforced_zones = 0usize;
                for zone in 0..3 {
                    for object in harness.servers[zone]
                        .observe_prefix(&format!("{BUCKET_PREFIX}{zone}"), prefix)
                        .await
                    {
                        let Some(id) = object.name.strip_prefix(prefix) else {
                            continue;
                        };
                        if id == pending_id && object.bytes.is_empty() && !object.finalized {
                            spare_zones += 1;
                        }
                        if manifest.seal_id.as_deref() == Some(id) && object.finalized {
                            enforced_zones += 1;
                        }
                    }
                }
                let ready = spare_zones == 3 && (manifest.seal_id.is_none() || enforced_zones >= 2);
                Ok(ready.then_some((tail_id, pending_id)))
            }
        })
        .await
        .ok()
    }

    /// Block until no rotation work is pending: the manifest tail is active,
    /// unfinalized, and below the rotation budget, the registered pending
    /// segment is empty, and every committed seal shows a finalized copy on a
    /// read quorum.
    ///
    /// Seals at or above `strict_floor` must show a finalized copy in
    /// *every* uncrashed zone, not just a quorum: healthy fast-path seals
    /// finalize all drained lanes, so the third copy always lands and the
    /// trace scan's unfinalized-size branch would otherwise race it.
    /// Recovery-sealed segments below the floor stop at quorum forever
    /// (with repairs disabled), so the strict bar cannot apply to them.
    async fn wait_for_rotations_settled(&self, strict_floor: Option<&str>) -> Result<()> {
        let prefix = format!("{}/segments/", self.prefix);
        wait_for_predicate("maintenance to settle committed rotation work", || {
            let harness = self;
            let prefix = prefix.as_str();
            async move {
                let Some(manifest) = harness.manifest_observation().await.ok() else {
                    return Ok(None);
                };
                let mut finalized: HashMap<String, usize> = HashMap::new();
                let mut pending: HashSet<String> = HashSet::new();
                let mut tail_bytes = 0usize;
                let mut tail_finalized = 0usize;
                let mut pending_bytes = 0usize;
                for zone in 0..3 {
                    if harness.crashed[zone] {
                        continue;
                    }
                    for object in harness.servers[zone]
                        .observe_prefix(&format!("{BUCKET_PREFIX}{zone}"), prefix)
                        .await
                    {
                        let Some(id) = object.name.strip_prefix(prefix) else {
                            continue;
                        };
                        let committed_seal = manifest.segments.iter().any(|entry| entry.id == id)
                            || manifest.seal_id.as_deref() == Some(id);
                        if committed_seal {
                            pending.insert(id.to_string());
                        }
                        if object.finalized {
                            *finalized.entry(id.to_string()).or_default() += 1;
                        }
                        if manifest.tail_id.as_deref() == Some(id) {
                            tail_bytes = tail_bytes.max(object.bytes.len());
                            tail_finalized += usize::from(object.finalized);
                        }
                        if manifest.pending_id.as_deref() == Some(id) {
                            pending_bytes = pending_bytes.max(object.bytes.len());
                        }
                    }
                }
                if manifest.pending_id.is_none()
                    || pending_bytes != 0
                    || tail_finalized != 0
                    || (tail_bytes >= harness.engine_config.max_segment_bytes
                        && chorus_client::dst_support::gcs_segment_directory_has_room(
                            &manifest.segments_encoded,
                            2,
                        ))
                {
                    return Ok(None);
                }
                let zones_up = (0..3).filter(|&zone| !harness.crashed[zone]).count();
                for id in &pending {
                    // segment ids are fixed-width hex, so lexicographic order
                    // matches numeric order within an epoch
                    let strict = strict_floor.is_some_and(|floor| id.as_str() >= floor);
                    let need = if strict { zones_up } else { 2 };
                    if finalized.get(id).copied().unwrap_or(0) < need {
                        return Ok(None);
                    }
                }
                Ok(Some(()))
            }
        })
        .await
    }

    async fn exercise_reduced_redundancy_corruption(&mut self) -> Result<()> {
        let prefix = format!("{}/corruption/{}", self.prefix, self.logical_time);
        let volume = SegmentedVolume::new_with_dyn_factories_and_metrics_recorder(
            self.factories.to_vec(),
            self.manifest_factory.clone(),
            &prefix,
            self.client_config.clone(),
            Arc::new(NoopMetricsRecorder),
        )
        .expect("the simulation always binds three zones");
        if std::env::var("CHORUS_DEBUG_LANES").is_ok() {
            eprintln!("EXERCISE-START");
        }
        self.servers[2].set_crashed(true).await;
        self.crashed[2] = true;
        let mut recovery = volume.recover(WalSeqNo::ZERO).await?;
        while recovery.try_next().await?.is_some() {}
        let mut writer = recovery
            .start(WalEngineConfig {
                repair_interval: None,
                ..Default::default()
            })
            .await?;
        writer
            .enqueue_append(WalSeqNo::ZERO, Bytes::from_static(b"reduced-redundancy"))
            .await?
            .await?;
        writer.abort().await;
        // the active segment is unstamped (base lives in the manifest), so
        // the record's segment is the one non-empty object under the prefix
        let object = self.servers[0]
            .observe_prefix(&format!("{BUCKET_PREFIX}0"), &format!("{prefix}/segments/"))
            .await
            .into_iter()
            .find(|object| !object.bytes.is_empty())
            .context("reduced-redundancy segment is missing")?
            .name;
        for zone in 0..2 {
            self.servers[zone]
                .corrupt_byte_for(&format!("{BUCKET_PREFIX}{zone}"), &object, 0)
                .await;
            self.emit(
                "DiskCorrupted",
                Some(zone),
                Some(0),
                Some(0),
                None,
                None,
                None,
            );
        }
        self.servers[2].set_crashed(false).await;
        self.crashed[2] = false;
        if volume.recover(WalSeqNo::ZERO).await.is_ok() {
            bail!("recovery accepted corruption of both committing copies");
        }
        if std::env::var("CHORUS_DEBUG_LANES").is_ok() {
            eprintln!("EXERCISE-END");
        }
        Ok(())
    }

    /// Arm applied-then-lost and torn-tail outcomes on upcoming appends: the
    /// fake persists all (an ambiguous, maybe-committed record) or a prefix
    /// (an interrupted record tail) of the next append on the chosen zones,
    /// then fails the response. The producer spike that follows drives the
    /// writer through lane resume, poisoning, and recovery promotion of a
    /// tail the writer never saw acknowledged.
    async fn inject_ambiguous_appends(&mut self) {
        let arms = self.rng.random_range(1..=2);
        for _ in 0..arms {
            let zone = self.rng.random_range(0..3);
            if self.crashed[zone] {
                continue;
            }
            let bytes = if self.rng.random_bool(0.5) {
                usize::MAX // full record persisted, response lost
            } else {
                self.rng.random_range(1..32) // torn tail
            };
            self.servers[zone].inject_partial_write(bytes).await;
        }
    }

    /// The recovery epoch claim (or view commit) CAS applies on the regional
    /// manifest but its response is lost. The client must recognize its own
    /// applied claim on re-read instead of treating the register as
    /// contended — and must never double-claim an epoch.
    async fn ambiguous_manifest_cas(&mut self) -> Result<()> {
        self.manifest_server
            .inject_response_lost(Operation::Update, Code::Unavailable)
            .await;
        self.restart_engine().await
    }

    /// CRC-rot the newest segment's last byte on one zone, then restart.
    /// Recovery's selection must treat the rotted lane as a short lane,
    /// choose the canonical tail from the two healthy copies, and heal the
    /// rotted replica by write-back — without losing any acknowledged
    /// record (the audit inside restart_engine checks exactly that).
    async fn rot_active_tail(&mut self) -> Result<()> {
        if self.crashed.iter().any(|&down| down) {
            // tolerate-one-bad-lane needs the other two reachable
            return Ok(());
        }
        self.wait_for_rotations_settled(None).await?;
        let zone = self.rng.random_range(0..3);
        let bucket = format!("{BUCKET_PREFIX}{zone}");
        let segments = self.servers[zone]
            .observe_prefix(&bucket, &format!("{}/segments/", self.prefix))
            .await;
        // the active segment carries no base stamp; the manifest names it
        let manifest = self.manifest_observation().await?;
        let prefix = format!("{}/segments/", self.prefix);
        let Some(tail_id) = manifest.tail_id else {
            return Ok(());
        };
        let base = manifest.tail_base;
        let Some(newest) = segments
            .iter()
            .find(|segment| segment.name.strip_prefix(&prefix) == Some(tail_id.as_str()))
        else {
            return Ok(());
        };
        if newest.bytes.is_empty() {
            return Ok(());
        }
        let index = newest.bytes.len() - 1;
        self.servers[zone]
            .corrupt_byte_for(&bucket, &newest.name, index)
            .await;
        self.emit(
            "DiskCorrupted",
            Some(zone),
            Some(base),
            None,
            None,
            None,
            None,
        );
        self.restart_engine().await
    }

    #[allow(clippy::too_many_arguments)]
    fn emit(
        &mut self,
        event: &str,
        zone: Option<usize>,
        segment: Option<u64>,
        logical_offset: Option<u64>,
        value: Option<u64>,
        record_end: Option<u64>,
        reader: Option<u64>,
    ) {
        self.emit_for(
            (self.writer_incarnation, self.epoch),
            event,
            zone,
            segment,
            logical_offset,
            value,
            record_end,
            reader,
        );
    }
}

fn is_expected_client_fault(error: &chorus_client::Error, injected_fault_observed: bool) -> bool {
    match error {
        chorus_client::Error::Poisoned
        | chorus_client::Error::Fenced(_)
        | chorus_client::Error::Closed => true,
        chorus_client::Error::NoReadQuorum => injected_fault_observed,
        chorus_client::Error::Transport { code, .. } => {
            is_expected_transport_code(*code, injected_fault_observed)
        }
        _ => false,
    }
}

fn is_expected_transport_code(
    code: chorus_client::TransportCode,
    injected_fault_observed: bool,
) -> bool {
    injected_fault_observed && (code.transient() || code.fences_writer())
}

fn is_expected_harness_fault(error: &anyhow::Error, injected_fault_observed: bool) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<chorus_client::Error>()
            .is_some_and(|error| is_expected_client_fault(error, injected_fault_observed))
    })
}

fn recovered_record(record: &WalRecord) -> Vec<u8> {
    encode_record(&record.payload)
}

/// Decode the `chorus.segments` value: comma-separated `id:base:crc32c`
/// entries.
fn parse_segment_directory(value: &str) -> Result<Vec<DirectoryObservation>> {
    let mut entries = Vec::new();
    if value.is_empty() {
        return Ok(entries);
    }
    for part in value.split(',') {
        let mut fields = part.split(':');
        let id = fields
            .next()
            .with_context(|| format!("manifest directory entry {part} lacks an id"))?;
        let base = fields
            .next()
            .with_context(|| format!("manifest directory entry {part} lacks a base"))?;
        let crc = fields
            .next()
            .with_context(|| format!("manifest directory entry {part} lacks a CRC32C"))?;
        if crc.len() != 8
            || !crc
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            bail!("manifest directory entry {part} has an invalid CRC32C");
        }
        let crc32c = u32::from_str_radix(crc, 16)
            .with_context(|| format!("manifest directory entry {part} has an invalid CRC32C"))?;
        if fields.next().is_some() {
            bail!("manifest directory entry {part} has too many fields");
        }
        if id.is_empty() {
            bail!("manifest directory entry {part} lacks an id");
        }
        let base: u64 = base
            .parse()
            .with_context(|| format!("manifest directory entry {part} has an invalid base"))?;
        entries.push(DirectoryObservation {
            id: id.to_string(),
            base_record_index: base,
            crc32c,
        });
    }
    Ok(entries)
}

fn record_value(encoded: &[u8]) -> u64 {
    let digest = Sha256::digest(encoded);
    u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 prefix"))
}

fn sealed_copy_is_healthy(snapshot: &ObjectObservation, segment: &SegmentObservation) -> bool {
    let Some(end) = segment.end_record_index else {
        return false;
    };
    let Some(record_count) = end
        .checked_sub(segment.base_record_index)
        .and_then(|span| span.checked_add(1))
    else {
        return false;
    };
    snapshot.finalized
        && snapshot.metadata.get("chorus.format").map(String::as_str) == Some("1")
        && decode_all(&snapshot.bytes).is_some_and(|records| records.len() as u64 == record_count)
        && segment.crc32c.is_none_or(|expected| {
            snapshot.crc32c == Some(expected) && crc32c::crc32c(&snapshot.bytes) == expected
        })
}

/// Outcome of one awaited record in the delayed-spare trap walk.
enum TrapStep {
    Committed,
    Poisoned,
}

/// Outcome of the whole delayed-spare trap walk.
enum TrapWalk {
    Committed(Vec<AppendReceipt>),
    Poisoned(Vec<AppendReceipt>),
}

fn encode_record(payload: &[u8]) -> Vec<u8> {
    let total_len = u32::try_from(payload.len() + 4).expect("DST record fits u32");
    let mut encoded = Vec::with_capacity(total_len as usize);
    encoded.extend_from_slice(&total_len.to_be_bytes());
    encoded.extend_from_slice(payload);
    encoded
}

fn decode_complete_prefix(mut bytes: &[u8]) -> (Vec<Vec<u8>>, usize) {
    let mut records = Vec::new();
    let mut consumed = 0;
    while bytes.len() >= 4 {
        let total_len = u32::from_be_bytes(bytes[..4].try_into().expect("four bytes")) as usize;
        if total_len < 4 || total_len > bytes.len() {
            break;
        }
        records.push(bytes[..total_len].to_vec());
        consumed += total_len;
        bytes = &bytes[total_len..];
    }
    (records, consumed)
}

fn decode_all(bytes: &[u8]) -> Option<Vec<Vec<u8>>> {
    let (records, consumed) = decode_complete_prefix(bytes);
    (consumed == bytes.len()).then_some(records)
}

fn finalized_record_count(
    objects: &[ObjectObservation],
    expected_crc32c: Option<u32>,
) -> Option<usize> {
    let mut support: HashMap<&[u8], usize> = HashMap::new();
    for object in objects.iter().filter(|object| object.finalized) {
        if expected_crc32c.is_some_and(|expected| {
            object.crc32c != Some(expected) || crc32c::crc32c(&object.bytes) != expected
        }) {
            continue;
        }
        if decode_all(&object.bytes).is_some_and(|records| !records.is_empty()) {
            *support.entry(&object.bytes).or_default() += 1;
        }
    }
    support
        .into_iter()
        .find(|(_, copies)| *copies >= 2)
        .and_then(|(bytes, _)| decode_all(bytes).map(|records| records.len()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_directory_parser_requires_checksummed_entries() {
        assert!(parse_segment_directory("segment:0").is_err());
        let entries = parse_segment_directory("sealed-a:0:01234567,sealed-b:5:1234abcd")
            .expect("checksummed directory");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id, "sealed-a");
        assert_eq!(entries[0].base_record_index, 0);
        assert_eq!(entries[0].crc32c, 0x0123_4567);
        assert_eq!(entries[1].id, "sealed-b");
        assert_eq!(entries[1].base_record_index, 5);
        assert_eq!(entries[1].crc32c, 0x1234_abcd);
    }

    #[test]
    fn segment_directory_parser_rejects_noncanonical_crc32c() {
        assert!(parse_segment_directory("segment:0:123").is_err());
        assert!(parse_segment_directory("segment:0:1234ABCD").is_err());
        assert!(parse_segment_directory("segment:0:1234abcg").is_err());
        assert!(parse_segment_directory("segment:0:1234abcd:extra").is_err());
    }

    #[test]
    fn checksummed_segment_health_requires_committed_object_crc32c() {
        let bytes = encode_record(b"record");
        let crc32c = crc32c::crc32c(&bytes);
        let segment = SegmentObservation {
            id: "segment".into(),
            base_record_index: 0,
            end_record_index: Some(0),
            crc32c: Some(crc32c),
        };
        let mut snapshot = ObjectObservation {
            name: "segment".into(),
            generation: 1,
            metageneration: 1,
            metadata: HashMap::from([("chorus.format".into(), "1".into())]),
            bytes,
            finalized: true,
            reported_size: 10,
            crc32c: Some(crc32c),
        };
        assert!(sealed_copy_is_healthy(&snapshot, &segment));
        snapshot.crc32c = Some(crc32c ^ 1);
        assert!(!sealed_copy_is_healthy(&snapshot, &segment));
    }

    #[test]
    fn seed_zero_preserves_the_fixed_phase_order() {
        let mut rng = ChaCha8Rng::seed_from_u64(0);
        assert_eq!(
            phase_deck(0, &mut rng),
            std::array::from_fn(|phase| phase as u8)
        );
    }

    #[test]
    fn shuffled_phase_epochs_are_complete_and_advance() {
        let mut rng = ChaCha8Rng::seed_from_u64(17);
        let first = phase_deck(17, &mut rng);
        let second = phase_deck(17, &mut rng);
        assert_ne!(first, second);

        for deck in [first, second] {
            let mut sorted = deck;
            sorted.sort_unstable();
            assert_eq!(sorted, std::array::from_fn(|phase| phase as u8));
        }
    }

    #[test]
    fn background_fault_budget_is_seeded_finite_and_covers_new_faults() {
        let mut first_rng = ChaCha8Rng::seed_from_u64(41);
        let mut second_rng = ChaCha8Rng::seed_from_u64(41);
        let first = background_fault_budget(41, &mut first_rng);
        let second = background_fault_budget(41, &mut second_rng);
        assert_eq!(first, second);
        assert_eq!(first.len(), 8);
        assert!(first
            .iter()
            .map(|scheduled| scheduled.opportunity)
            .is_sorted());
        assert!(first
            .iter()
            .any(|scheduled| matches!(scheduled.fault, BackgroundFault::Transient { .. })));
        assert!(first
            .iter()
            .any(|scheduled| matches!(scheduled.fault, BackgroundFault::ResponseLoss { .. })));
        assert!(first
            .iter()
            .any(|scheduled| matches!(scheduled.fault, BackgroundFault::Delay { .. })));
        assert!(first
            .iter()
            .any(|scheduled| matches!(scheduled.fault, BackgroundFault::Redirect { .. })));
        assert!(first
            .iter()
            .any(|scheduled| matches!(scheduled.fault, BackgroundFault::SessionExpiry { .. })));
        assert!(first
            .iter()
            .any(|scheduled| matches!(scheduled.fault, BackgroundFault::MutationThrottle { .. })));
        assert!(first.iter().any(|scheduled| matches!(
            scheduled.fault,
            BackgroundFault::ResponseLoss {
                operation: Operation::Delete,
                ..
            }
        )));
    }

    #[test]
    fn expected_fault_triage_keeps_protocol_failures_fatal() {
        assert!(!is_expected_transport_code(
            chorus_client::TransportCode::ResourceExhausted,
            false
        ));
        assert!(is_expected_transport_code(
            chorus_client::TransportCode::ResourceExhausted,
            true
        ));
        assert!(is_expected_client_fault(
            &chorus_client::Error::Poisoned,
            false
        ));
        assert!(!is_expected_client_fault(
            &chorus_client::Error::Internal("safety violation".into()),
            true
        ));
    }

    #[test]
    fn production_client_seed_is_deterministic() {
        assert_deterministic(3, 48).unwrap();
    }

    #[test]
    fn production_seed_crosses_manifest_directory_capacity_without_livelock() {
        let report = assert_deterministic(0, 1_450).unwrap();
        assert!(report.events.iter().any(|event| {
            event.event == "DirectoryAdopted" && event.directory_len.is_some_and(|len| len >= 150)
        }));
    }

    #[test]
    fn production_client_seed_is_deterministic_with_service_latency() {
        assert_deterministic_with_latency(3, 48, true).unwrap();
    }

    #[test]
    fn stalled_lane_is_time_shed_without_shedding_slow_progress() {
        assert_lane_stall_deterministic(73).unwrap();
    }

    #[test]
    fn deadline_recheck_publishes_progress_before_transient_close() {
        run_lane_stall_recheck_seed(97, Code::Unavailable).unwrap();
        run_lane_stall_recheck_seed(97, Code::Unavailable).unwrap();
    }

    #[test]
    fn durable_progress_before_terminal_close_fences_writer() {
        run_lane_stall_recheck_seed(101, Code::FailedPrecondition).unwrap();
        run_lane_stall_recheck_seed(101, Code::FailedPrecondition).unwrap();
    }
}
