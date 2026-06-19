//! Recovery benchmark for the zonal GCS quorum WAL.
//!
//! Each iteration populates a fresh prefix with `--populate-records` records,
//! using a segment size tuned to produce roughly `--target-sealed-segments`
//! sealed segments, then cleanly shuts the writer down (leaving an unsealed
//! active tail). It then runs one cold `recover()` and times the recovery
//! phases: epoch claim, prepare (directory adoption + committed-seal
//! enforcement + appendable-candidate takeover), replay, and start. Replay size
//! is controlled by `--replay-records` (the recovery checkpoint is set to
//! `populate_records - replay_records`).
//!
//! Sweep replay size by varying `--replay-records`; sweep sealed-segment count
//! by varying `--target-sealed-segments`. Output is JSON with per-phase latency
//! percentiles across iterations.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use chorus_client::{
    BearerAuth, ClientConfig, CounterFn, GaugeFn, GrpcReplicaFactory, HistogramFn, MetricsRecorder,
    RefreshingAuthConfig, SegmentedVolume, UpDownCounterFn, WalEngineConfig, WalHandle, WalSeqNo,
};
use clap::Parser;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use hdrhistogram::Histogram;
use tokio::time::Instant;
use tracing_subscriber::EnvFilter;

#[derive(Default)]
struct BenchMetrics {
    counters: Mutex<HashMap<String, Arc<AtomicU64>>>,
    gauges: Mutex<HashMap<String, Arc<AtomicI64>>>,
}

impl BenchMetrics {
    fn counter(&self, name: &str) -> u64 {
        self.counters
            .lock()
            .unwrap()
            .get(name)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

struct BenchCounter(Arc<AtomicU64>);

impl CounterFn for BenchCounter {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }
}

struct BenchGauge(Arc<AtomicI64>);

impl GaugeFn for BenchGauge {
    fn set(&self, value: i64) {
        self.0.store(value, Ordering::Relaxed);
    }
}

struct NoopMetric;

impl UpDownCounterFn for NoopMetric {
    fn increment(&self, _value: i64) {}
}

impl HistogramFn for NoopMetric {
    fn record(&self, _value: f64) {}
}

impl MetricsRecorder for BenchMetrics {
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
        Arc::new(BenchCounter(metric))
    }

    fn register_gauge(
        &self,
        name: &str,
        _description: &str,
        _labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        let metric = self
            .gauges
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_default()
            .clone();
        Arc::new(BenchGauge(metric))
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

#[derive(Parser, Debug)]
#[command(about = "Recovery-time benchmark for the zonal GCS quorum WAL")]
struct Args {
    #[arg(long, value_delimiter = ',', num_args = 1..=5)]
    endpoints: Vec<String>,
    #[arg(long, value_delimiter = ',', num_args = 1..=5)]
    buckets: Vec<String>,
    /// Endpoint of the regional bucket hosting the manifest register.
    #[arg(long)]
    manifest_endpoint: String,
    /// Full v2 resource name of the regional manifest bucket.
    #[arg(long)]
    manifest_bucket: String,
    /// Base object prefix; each iteration appends a unique sub-prefix.
    #[arg(long)]
    prefix: String,
    #[arg(long, env = "GCS_BEARER_TOKEN")]
    bearer_token: Option<String>,
    #[arg(long, conflicts_with = "bearer_token")]
    anonymous: bool,
    /// Records written into each iteration's log before recovery.
    #[arg(long, default_value_t = 50_000)]
    populate_records: u64,
    /// Approximate number of sealed segments to create during population.
    /// Zero disables rotation (one large active segment).
    #[arg(long, default_value_t = 8)]
    target_sealed_segments: u64,
    /// Records to replay during recovery; the checkpoint is set to
    /// `populate_records - replay_records`. Zero replays nothing; a value at or
    /// above `populate_records` replays the whole log.
    #[arg(long)]
    replay_records: Option<u64>,
    /// Independent cold recoveries measured, each on its own fresh prefix.
    #[arg(long, default_value_t = 5)]
    iterations: u64,
    #[arg(long, default_value_t = 4096)]
    payload_bytes: usize,
    /// Pipeline window used only to populate the log quickly.
    #[arg(long, default_value_t = 256)]
    populate_window: usize,
    #[arg(long, default_value_t = 4)]
    worker_threads: usize,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    if args.buckets.len() != args.endpoints.len() {
        bail!("--endpoints and --buckets must list the same number of zones");
    }
    if !matches!(args.endpoints.len(), 1 | 3 | 5) {
        bail!("1, 3, or 5 endpoints and buckets are required");
    }
    if args.populate_records == 0 || args.iterations == 0 || args.payload_bytes == 0 {
        bail!("--populate-records, --iterations, and --payload-bytes must be positive");
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.worker_threads)
        .enable_all()
        .build()?
        .block_on(run(args))
}

/// Append `count` records starting at `start_seqno` through a pipeline window.
async fn append_records(
    handle: &mut WalHandle,
    start_seqno: u64,
    count: u64,
    payload: &Bytes,
    window: usize,
) -> Result<()> {
    let target = start_seqno + count;
    let mut next = start_seqno;
    let mut inflight = FuturesUnordered::new();
    while inflight.len() < window && next < target {
        let completion = handle
            .enqueue_append(WalSeqNo::record(next), payload.clone())
            .await?;
        next += 1;
        inflight.push(completion);
    }
    while let Some(result) = inflight.next().await {
        result?;
        if next < target {
            let completion = handle
                .enqueue_append(WalSeqNo::record(next), payload.clone())
                .await?;
            next += 1;
            inflight.push(completion);
        }
    }
    Ok(())
}

fn engine_config(max_segment_bytes: usize) -> WalEngineConfig {
    WalEngineConfig {
        max_segment_bytes,
        // Keep the hard ceiling above the rotation target so rotation is driven
        // by the target, not the ceiling.
        max_active_segment_bytes: max_segment_bytes
            .saturating_mul(8)
            .max(WalEngineConfig::default().max_active_segment_bytes),
        repair_interval: None,
        ..WalEngineConfig::default()
    }
}

struct PhaseHistograms {
    epoch_claim: Histogram<u64>,
    prepare: Histogram<u64>,
    replay: Histogram<u64>,
    start: Histogram<u64>,
    total: Histogram<u64>,
}

impl PhaseHistograms {
    fn new() -> Result<Self> {
        let new = || Histogram::<u64>::new_with_bounds(1, 600_000_000, 3);
        Ok(Self {
            epoch_claim: new()?,
            prepare: new()?,
            replay: new()?,
            start: new()?,
            total: new()?,
        })
    }
}

fn summarize(histogram: &Histogram<u64>) -> serde_json::Value {
    serde_json::json!({
        "p50": histogram.value_at_quantile(0.50),
        "p99": histogram.value_at_quantile(0.99),
        "p99_9": histogram.value_at_quantile(0.999),
        "max": histogram.max(),
        "mean": histogram.mean().round() as u64,
    })
}

async fn run(args: Args) -> Result<()> {
    let auth = if args.anonymous {
        None
    } else if let Some(token) = &args.bearer_token {
        Some(BearerAuth::static_token(token.clone()))
    } else {
        Some(
            BearerAuth::google_adc(RefreshingAuthConfig::default())
                .await
                .context("load Google Application Default Credentials")?,
        )
    };
    let zones = args.endpoints.len();
    let mut factories = Vec::with_capacity(zones);
    for zone in 0..zones {
        let factory = match &auth {
            Some(auth) => {
                GrpcReplicaFactory::connect_with_auth(
                    zone,
                    &args.endpoints[zone],
                    args.buckets[zone].clone(),
                    auth.clone(),
                )
                .await
            }
            None => {
                GrpcReplicaFactory::connect(
                    zone,
                    &args.endpoints[zone],
                    args.buckets[zone].clone(),
                    None,
                )
                .await
            }
        }
        .with_context(|| format!("connect zone {zone}"))?;
        factories.push(factory);
    }
    let manifest_factory = match &auth {
        Some(auth) => {
            GrpcReplicaFactory::connect_with_auth(
                zones,
                &args.manifest_endpoint,
                args.manifest_bucket.clone(),
                auth.clone(),
            )
            .await
        }
        None => {
            GrpcReplicaFactory::connect(
                zones,
                &args.manifest_endpoint,
                args.manifest_bucket.clone(),
                None,
            )
            .await
        }
    }
    .context("connect regional manifest bucket")?;

    let payload = Bytes::from(vec![0x5a; args.payload_bytes]);
    let encoded_record = args.payload_bytes + 4;
    let max_segment_bytes = if args.target_sealed_segments == 0 {
        // No rotation: one active segment large enough to hold the whole log.
        (args.populate_records as usize)
            .saturating_mul(encoded_record)
            .saturating_mul(2)
            .max(WalEngineConfig::default().max_segment_bytes)
    } else {
        let per_segment = (args.populate_records / (args.target_sealed_segments + 1)).max(1);
        ((per_segment as usize).saturating_mul(encoded_record)).max(encoded_record)
    };
    let replay_records = args.replay_records.unwrap_or(args.populate_records);
    let checkpoint = args.populate_records.saturating_sub(replay_records);

    let metrics = Arc::new(BenchMetrics::default());
    let metrics_recorder: Arc<dyn MetricsRecorder> = metrics.clone();

    let mut phases = PhaseHistograms::new()?;
    let mut sealed_counts: Vec<u64> = Vec::new();
    let mut replayed_records: Vec<u64> = Vec::new();
    let mut cas_attempts: Vec<u64> = Vec::new();
    let mut segments_sealed: Vec<u64> = Vec::new();

    for iteration in 0..args.iterations {
        let prefix = format!("{}/i{iteration:03}", args.prefix);
        let volume = SegmentedVolume::new_with_metrics_recorder(
            factories.clone(),
            manifest_factory.clone(),
            &prefix,
            ClientConfig::default(),
            metrics_recorder.clone(),
        )?;

        // --- populate -------------------------------------------------------
        let mut bootstrap = volume.recover(WalSeqNo::ZERO).await?;
        while let Some(record) = bootstrap.next().await {
            record?;
        }
        let mut handle = bootstrap.start(engine_config(max_segment_bytes)).await?;
        append_records(
            &mut handle,
            0,
            args.populate_records,
            &payload,
            args.populate_window,
        )
        .await
        .context("populate log")?;
        handle
            .shutdown()
            .await
            .context("shutdown populated writer")?;

        // --- measure one cold recovery -------------------------------------
        let cas_before = metrics.counter("chorus.wal.manifest.cas_attempts");
        let sealed_before = metrics.counter("chorus.wal.seal.segments");

        let total_started = Instant::now();
        let mut recovery = volume
            .recover(WalSeqNo::record(checkpoint))
            .await
            .context("cold recovery")?;
        // Epoch-claim and prepare durations are captured inside the client and
        // exposed on the recovery object; read them before `start()` moves it.
        let epoch_claim = recovery.timings.epoch_claim;
        let prepare = recovery.timings.prepare;
        let sealed_count = recovery.sealed_segment_count() as u64;
        let replay_span = recovery
            .end
            .record_index
            .saturating_sub(recovery.from.record_index);

        let replay_started = Instant::now();
        while let Some(record) = recovery.next().await {
            record?;
        }
        let replay = replay_started.elapsed();

        let start_started = Instant::now();
        let recovered = recovery.start(engine_config(max_segment_bytes)).await?;
        let start = start_started.elapsed();
        let total = total_started.elapsed();

        recovered
            .shutdown()
            .await
            .context("shutdown recovered writer")?;

        record_us(&mut phases.epoch_claim, epoch_claim)?;
        record_us(&mut phases.prepare, prepare)?;
        record_us(&mut phases.replay, replay)?;
        record_us(&mut phases.start, start)?;
        record_us(&mut phases.total, total)?;
        sealed_counts.push(sealed_count);
        replayed_records.push(replay_span);
        cas_attempts.push(metrics.counter("chorus.wal.manifest.cas_attempts") - cas_before);
        segments_sealed.push(metrics.counter("chorus.wal.seal.segments") - sealed_before);
    }

    let avg = |v: &[u64]| -> f64 {
        if v.is_empty() {
            0.0
        } else {
            v.iter().sum::<u64>() as f64 / v.len() as f64
        }
    };

    let report = serde_json::json!({
        "benchmark": "recovery",
        "iterations": args.iterations,
        "populate_records": args.populate_records,
        "target_sealed_segments": args.target_sealed_segments,
        "observed_sealed_segments_avg": avg(&sealed_counts),
        "replay_records_target": replay_records,
        "replayed_records_avg": avg(&replayed_records),
        "checkpoint": checkpoint,
        "manifest_cas_attempts_avg": avg(&cas_attempts),
        "segments_sealed_avg": avg(&segments_sealed),
        "phase_latency_us": {
            "epoch_claim": summarize(&phases.epoch_claim),
            "prepare": summarize(&phases.prepare),
            "replay": summarize(&phases.replay),
            "start": summarize(&phases.start),
            "total": summarize(&phases.total),
        },
        "configuration": {
            "payload_bytes": args.payload_bytes,
            "max_segment_bytes": max_segment_bytes,
            "populate_window": args.populate_window,
            "worker_threads": args.worker_threads,
        }
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn record_us(histogram: &mut Histogram<u64>, duration: Duration) -> Result<()> {
    histogram.record((duration.as_micros().max(1) as u64).min(599_000_000))?;
    Ok(())
}
