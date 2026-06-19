use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::Parser;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use hdrhistogram::Histogram;
use walrus_rust::{FsyncSchedule, ReadConsistency, Walrus};

const OPEN_LOOP_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);
const TOPIC: &str = "disk-wal-bench";
const WAL_NAMESPACE: &str = "disk-wal-bench";

type AppendCompletion = BoxFuture<'static, Result<()>>;
type TimedAppendCompletion = BoxFuture<'static, (Instant, Instant, Result<()>)>;

#[derive(Parser, Debug)]
#[command(about = "Benchmark a walrus-rust disk WAL with durable synchronous appends")]
struct Args {
    /// Directory on the target disk in which walrus-rust stores the benchmark WAL.
    #[arg(long)]
    data_dir: PathBuf,
    #[arg(long, default_value_t = 60)]
    duration_seconds: u64,
    /// Maximum concurrent durable appends; --outstanding-appends is an alias.
    #[arg(long, visible_alias = "outstanding-appends", default_value_t = 32)]
    pipeline_window: usize,
    /// Fixed open-loop arrival rate in records per second; zero uses closed-loop mode.
    #[arg(long, default_value_t = 0.0, value_name = "RECORDS_PER_SECOND")]
    arrival_rate: f64,
    #[arg(long, default_value_t = 4096)]
    payload_bytes: usize,
    #[arg(long, default_value_t = 1_048_576)]
    max_record_bytes: usize,
    #[arg(long, default_value_t = 4)]
    worker_threads: usize,
}

#[derive(Default)]
struct WorkloadStats {
    completed_appends: u64,
    scheduled_appends: u64,
    outstanding_cap_waits: u64,
    drain_timed_out: bool,
    undrained_appends: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    validate(&args)?;
    std::fs::create_dir_all(&args.data_dir)
        .with_context(|| format!("create data directory {}", args.data_dir.display()))?;

    // walrus-rust reads its data root from the environment during construction.
    // Set it before starting the Tokio or walrus background threads.
    std::env::set_var("WALRUS_DATA_DIR", &args.data_dir);
    std::env::set_var("WALRUS_QUIET", "1");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.worker_threads)
        .max_blocking_threads(args.worker_threads)
        .enable_all()
        .build()?;
    let result = runtime.block_on(run(args));
    runtime.shutdown_timeout(Duration::from_secs(1));
    result
}

fn validate(args: &Args) -> Result<()> {
    if !args.arrival_rate.is_finite() || args.arrival_rate < 0.0 {
        bail!("--arrival-rate must be a finite non-negative number");
    }
    if args.payload_bytes == 0
        || args.duration_seconds == 0
        || args.max_record_bytes == 0
        || args.pipeline_window == 0
        || args.worker_threads == 0
    {
        bail!("duration and all size/concurrency counts must be positive");
    }
    if args.payload_bytes > args.max_record_bytes {
        bail!(
            "--payload-bytes ({}) exceeds --max-record-bytes ({})",
            args.payload_bytes,
            args.max_record_bytes
        );
    }
    Ok(())
}

fn submit_append(wal: Arc<Walrus>, payload: Arc<Vec<u8>>) -> AppendCompletion {
    let task = tokio::task::spawn_blocking(move || wal.append_for_topic(TOPIC, payload.as_slice()));
    async move {
        task.await
            .context("walrus append worker panicked")?
            .context("append record to walrus WAL")
    }
    .boxed()
}

fn record_open_loop_completion(
    completed: Option<(Instant, Instant, Result<()>)>,
    latency: &mut Histogram<u64>,
    stats: &mut WorkloadStats,
) -> Result<()> {
    let Some((scheduled_send_time, completed_at, result)) = completed else {
        bail!("open-loop completion set ended unexpectedly");
    };
    result?;
    let elapsed = completed_at.saturating_duration_since(scheduled_send_time);
    latency.record((elapsed.as_micros().max(1) as u64).min(599_000_000))?;
    stats.completed_appends += 1;
    Ok(())
}

async fn run_open_loop(
    wal: Arc<Walrus>,
    payload: Arc<Vec<u8>>,
    duration: Duration,
    arrival_rate: f64,
    outstanding_cap: usize,
    latency: &mut Histogram<u64>,
) -> Result<(Instant, WorkloadStats)> {
    let started = Instant::now();
    let deadline = started + duration;
    let drain_deadline = deadline + OPEN_LOOP_DRAIN_TIMEOUT;
    let mut stats = WorkloadStats::default();
    let mut inflight: FuturesUnordered<TimedAppendCompletion> = FuturesUnordered::new();
    let drain_timeout = tokio::time::sleep_until(drain_deadline.into());
    tokio::pin!(drain_timeout);

    while Instant::now() < deadline {
        let scheduled_offset_seconds = stats.scheduled_appends as f64 / arrival_rate;
        if scheduled_offset_seconds >= duration.as_secs_f64() {
            break;
        }
        // Preserve the absolute intended send time through admission and fsync
        // so queueing and storage stalls remain visible in the histogram.
        let scheduled_send_time = started + Duration::from_secs_f64(scheduled_offset_seconds);
        let send_time = tokio::time::sleep_until(scheduled_send_time.into());
        tokio::pin!(send_time);
        loop {
            tokio::select! {
                biased;
                completed = inflight.next(), if !inflight.is_empty() => {
                    record_open_loop_completion(completed, latency, &mut stats)?;
                }
                _ = &mut send_time => break,
            }
        }

        if inflight.len() >= outstanding_cap {
            stats.outstanding_cap_waits = stats.outstanding_cap_waits.saturating_add(1);
        }
        while inflight.len() >= outstanding_cap {
            tokio::select! {
                biased;
                completed = inflight.next() => {
                    record_open_loop_completion(completed, latency, &mut stats)?;
                }
                _ = &mut drain_timeout => {
                    stats.drain_timed_out = true;
                    stats.undrained_appends = inflight.len();
                    return Ok((started, stats));
                }
            }
        }

        let completion = submit_append(wal.clone(), payload.clone());
        stats.scheduled_appends += 1;
        inflight.push(
            async move {
                let result = completion.await;
                (scheduled_send_time, Instant::now(), result)
            }
            .boxed(),
        );
    }

    while !inflight.is_empty() {
        tokio::select! {
            biased;
            completed = inflight.next() => {
                record_open_loop_completion(completed, latency, &mut stats)?;
            }
            _ = &mut drain_timeout => {
                stats.drain_timed_out = true;
                stats.undrained_appends = inflight.len();
                break;
            }
        }
    }
    Ok((started, stats))
}

async fn run_closed_loop(
    wal: Arc<Walrus>,
    payload: Arc<Vec<u8>>,
    duration: Duration,
    pipeline_window: usize,
    latency: &mut Histogram<u64>,
) -> Result<(Instant, WorkloadStats)> {
    let started = Instant::now();
    let deadline = started + duration;
    let mut inflight: FuturesUnordered<BoxFuture<'static, (Instant, Result<()>)>> =
        FuturesUnordered::new();

    while inflight.len() < pipeline_window {
        let request_started = Instant::now();
        let completion = submit_append(wal.clone(), payload.clone());
        inflight.push(async move { (request_started, completion.await) }.boxed());
    }

    let mut completed_appends = 0u64;
    while let Some((request_started, result)) = inflight.next().await {
        result?;
        latency.record((request_started.elapsed().as_micros().max(1) as u64).min(599_000_000))?;
        completed_appends += 1;
        if Instant::now() < deadline {
            let request_started = Instant::now();
            let completion = submit_append(wal.clone(), payload.clone());
            inflight.push(async move { (request_started, completion.await) }.boxed());
        }
    }

    Ok((
        started,
        WorkloadStats {
            completed_appends,
            scheduled_appends: completed_appends,
            ..WorkloadStats::default()
        },
    ))
}

async fn run(args: Args) -> Result<()> {
    let wal = Arc::new(
        Walrus::with_consistency_and_schedule_for_key(
            WAL_NAMESPACE,
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::SyncEach,
        )
        .context("open walrus WAL with SyncEach durability")?,
    );
    let payload = Arc::new(vec![0x5a; args.payload_bytes]);
    let duration = Duration::from_secs(args.duration_seconds);
    let mut latency = Histogram::<u64>::new_with_bounds(1, 600_000_000, 3)?;

    let (mode, started, workload) = if args.arrival_rate > 0.0 {
        let (started, workload) = run_open_loop(
            wal,
            payload,
            duration,
            args.arrival_rate,
            args.pipeline_window,
            &mut latency,
        )
        .await?;
        ("open_loop", started, workload)
    } else {
        let (started, workload) =
            run_closed_loop(wal, payload, duration, args.pipeline_window, &mut latency).await?;
        ("closed_loop", started, workload)
    };

    let elapsed = started.elapsed().as_secs_f64();
    let completed_appends = workload.completed_appends;
    let record_iops = completed_appends as f64 / elapsed;
    let payload_mib = completed_appends as f64 * args.payload_bytes as f64 / (1024.0 * 1024.0);
    let logical_fsyncs = completed_appends;
    let records_per_fsync = if logical_fsyncs == 0 { 0.0 } else { 1.0 };
    let report = serde_json::json!({
        "backend": "walrus-fsync",
        "fsync_mode": "sync_each",
        "mode": mode,
        "arrival_rate_target": args.arrival_rate,
        "achieved_rate": record_iops,
        "duration_seconds": elapsed,
        "completed_appends": completed_appends,
        "scheduled_appends": workload.scheduled_appends,
        "committed_records": completed_appends,
        "fsyncs": logical_fsyncs,
        "fsyncs_per_second": logical_fsyncs as f64 / elapsed,
        "records_per_fsync": records_per_fsync,
        "records_per_persist": records_per_fsync,
        "drain_timed_out": workload.drain_timed_out,
        "undrained_appends": workload.undrained_appends,
        "outstanding_cap_waits": workload.outstanding_cap_waits,
        "append_iops": record_iops,
        "record_iops": record_iops,
        "payload_mib_per_second": payload_mib / elapsed,
        "payload_mib": payload_mib,
        "latency_us": {
            "p50": latency.value_at_quantile(0.50),
            "p99": latency.value_at_quantile(0.99),
            "p99_9": latency.value_at_quantile(0.999),
            "max": latency.max(),
        },
        "configuration": {
            "data_dir": args.data_dir,
            "wal_namespace": WAL_NAMESPACE,
            "topic": TOPIC,
            "outstanding_appends": args.pipeline_window,
            "arrival_rate": args.arrival_rate,
            "open_loop_outstanding_cap": args.pipeline_window,
            "payload_bytes": args.payload_bytes,
            "max_record_bytes": args.max_record_bytes,
            "pipeline_window": args.pipeline_window,
            "worker_threads": args.worker_threads,
        }
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
