use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use bytes::Bytes;
use chorus_client::{
    AppendReceipt, ClientConfig, Error, MetricsRecorder, SegmentedVolume, WalEngineConfig,
    WalHandle, WalSeqNo,
};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use hdrhistogram::Histogram;

use super::BenchMetrics;
use crate::ConnectedStorage;

const OPEN_LOOP_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

type TimedAppendCompletion = BoxFuture<'static, (Instant, Instant, Result<AppendReceipt, Error>)>;

#[derive(clap::Args, Debug)]
pub(crate) struct AppendArgs {
    #[arg(long, default_value_t = 60)]
    duration_seconds: u64,
    #[arg(long, default_value_t = 128)]
    outstanding_appends: usize,
    /// Fixed open-loop arrival rate in records per second; zero uses closed-loop mode.
    #[arg(long, default_value_t = 0.0, value_name = "RECORDS_PER_SECOND")]
    arrival_rate: f64,
    #[arg(long, default_value_t = 4096)]
    payload_bytes: usize,
    #[arg(long, default_value_t = 1_048_576)]
    max_record_bytes: usize,
    #[arg(long, default_value_t = 32)]
    pipeline_window: usize,
    #[arg(long, default_value_t = 67_108_864)]
    max_inflight_bytes: usize,
    #[arg(long, default_value_t = 67_108_864)]
    max_replica_lag_bytes: usize,
    #[arg(long, default_value_t = 5_000)]
    lane_stall_timeout_ms: u64,
    #[arg(long, default_value_t = 256)]
    queue_capacity: usize,
    #[arg(long, default_value_t = 268_435_456)]
    segment_bytes: usize,
    #[arg(long, default_value_t = 4)]
    worker_threads: usize,
}

impl AppendArgs {
    pub(crate) fn worker_threads(&self) -> usize {
        self.worker_threads
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if !self.arrival_rate.is_finite() || self.arrival_rate < 0.0 {
            bail!("--arrival-rate must be a finite non-negative number");
        }
        if self.outstanding_appends == 0
            || self.payload_bytes == 0
            || self.duration_seconds == 0
            || self.max_record_bytes == 0
            || self.pipeline_window == 0
            || self.max_inflight_bytes == 0
            || self.max_replica_lag_bytes == 0
            || self.lane_stall_timeout_ms == 0
            || self.queue_capacity == 0
            || self.segment_bytes == 0
            || self.worker_threads == 0
        {
            bail!("duration and all size/concurrency counts must be positive");
        }
        Ok(())
    }
}

#[derive(Default)]
struct WorkloadStats {
    completed_appends: u64,
    scheduled_appends: u64,
    outstanding_cap_waits: u64,
    drain_timed_out: bool,
    undrained_appends: usize,
}

fn record_open_loop_completion(
    completed: Option<(Instant, Instant, Result<AppendReceipt, Error>)>,
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
    handle: &mut WalHandle,
    next_seqno: &mut u64,
    payload: &Bytes,
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
        // Keep the intended absolute send time through admission and commit so
        // backpressure and stalls remain visible in the latency histogram.
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

        let enqueue = handle.enqueue_append(WalSeqNo::record(*next_seqno), payload.clone());
        tokio::pin!(enqueue);
        let completion = loop {
            tokio::select! {
                biased;
                completed = inflight.next(), if !inflight.is_empty() => {
                    record_open_loop_completion(completed, latency, &mut stats)?;
                }
                result = &mut enqueue => break result?,
                _ = &mut drain_timeout => {
                    stats.drain_timed_out = true;
                    stats.undrained_appends = inflight.len();
                    return Ok((started, stats));
                }
            }
        };
        *next_seqno += 1;
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

pub(crate) async fn run(storage: ConnectedStorage, prefix: String, args: AppendArgs) -> Result<()> {
    let metrics = Arc::new(BenchMetrics::default());
    let metrics_recorder: Arc<dyn MetricsRecorder> = metrics.clone();
    let volume = SegmentedVolume::new_with_metrics_recorder(
        storage.factories,
        storage.manifest_factory,
        prefix,
        ClientConfig::default(),
        metrics_recorder,
    )?;
    let mut recovery = volume.recover(WalSeqNo::ZERO).await?;
    let mut next_seqno = recovery.end.record_index;
    while let Some(record) = recovery.next().await {
        record?;
    }
    let mut handle = recovery
        .start(WalEngineConfig {
            queue_capacity: args.queue_capacity,
            max_record_bytes: args.max_record_bytes,
            pipeline_window_records: args.pipeline_window,
            max_inflight_bytes: args.max_inflight_bytes,
            max_replica_lag_bytes: args.max_replica_lag_bytes,
            lane_stall_timeout: Duration::from_millis(args.lane_stall_timeout_ms),
            max_segment_bytes: args.segment_bytes,
            max_active_segment_bytes: WalEngineConfig::default().max_active_segment_bytes,
            repair_interval: Some(Duration::from_secs(300)),
            shutdown_timeout: WalEngineConfig::default().shutdown_timeout,
        })
        .await?;

    let payload = Bytes::from(vec![0x5a; args.payload_bytes]);
    // The engine can hold this many accepted records across its dispatch queue
    // and active pipeline; waiting at the same bound prevents benchmark growth.
    let open_loop_outstanding_cap = args.queue_capacity.saturating_add(args.pipeline_window);
    let (mode, started, workload, latency) = if args.arrival_rate > 0.0 {
        let mut latency = Histogram::<u64>::new_with_bounds(1, 600_000_000, 3)?;
        let (started, workload) = run_open_loop(
            &mut handle,
            &mut next_seqno,
            &payload,
            Duration::from_secs(args.duration_seconds),
            args.arrival_rate,
            open_loop_outstanding_cap,
            &mut latency,
        )
        .await?;
        ("open_loop", started, workload, latency)
    } else {
        let deadline = Instant::now() + Duration::from_secs(args.duration_seconds);
        let started = Instant::now();
        let mut inflight: FuturesUnordered<
            BoxFuture<
                'static,
                (
                    Instant,
                    Result<chorus_client::AppendReceipt, chorus_client::Error>,
                ),
            >,
        > = FuturesUnordered::new();
        while inflight.len() < args.outstanding_appends {
            let request_started = Instant::now();
            let completion = handle
                .enqueue_append(WalSeqNo::record(next_seqno), payload.clone())
                .await?;
            next_seqno += 1;
            inflight.push(async move { (request_started, completion.await) }.boxed());
        }

        let mut latency = Histogram::<u64>::new_with_bounds(1, 600_000_000, 3)?;
        let mut completed_appends = 0u64;
        while let Some((request_started, result)) = inflight.next().await {
            result?;
            latency
                .record((request_started.elapsed().as_micros().max(1) as u64).min(599_000_000))?;
            completed_appends += 1;
            if Instant::now() < deadline {
                let request_started = Instant::now();
                let completion = handle
                    .enqueue_append(WalSeqNo::record(next_seqno), payload.clone())
                    .await?;
                next_seqno += 1;
                inflight.push(async move { (request_started, completion.await) }.boxed());
            }
        }
        let workload = WorkloadStats {
            completed_appends,
            scheduled_appends: completed_appends,
            ..WorkloadStats::default()
        };
        ("closed_loop", started, workload, latency)
    };
    let elapsed = started.elapsed().as_secs_f64();
    let completed_appends = workload.completed_appends;
    let record_iops = completed_appends as f64 / elapsed;
    let committed_payload_bytes = metrics.counter("chorus.wal.append.committed_bytes");
    let committed_records = metrics.counter("chorus.wal.append.committed_records");
    let batches_sent = metrics.counter("chorus.wal.batch.sent");
    let replica_bytes_attempted = metrics.counter("chorus.wal.replica.bytes_attempted");
    let payload_mib = committed_payload_bytes as f64 / (1024.0 * 1024.0);
    let records_per_persist = if batches_sent == 0 {
        0.0
    } else {
        committed_records as f64 / batches_sent as f64
    };
    let write_amplification = if committed_payload_bytes == 0 {
        0.0
    } else {
        replica_bytes_attempted as f64 / committed_payload_bytes as f64
    };
    let report = serde_json::json!({
        "mode": mode,
        "arrival_rate_target": args.arrival_rate,
        "achieved_rate": record_iops,
        "duration_seconds": elapsed,
        "completed_appends": completed_appends,
        "scheduled_appends": workload.scheduled_appends,
        "committed_records": committed_records,
        "batches_sent": batches_sent,
        "records_per_persist": records_per_persist,
        "drain_timed_out": workload.drain_timed_out,
        "undrained_appends": workload.undrained_appends,
        "outstanding_cap_waits": workload.outstanding_cap_waits,
        "append_iops": completed_appends as f64 / elapsed,
        "record_iops": record_iops,
        "payload_mib_per_second": payload_mib / elapsed,
        "payload_mib": payload_mib,
        "wal_record_bytes": metrics.counter("chorus.wal.append.encoded_bytes"),
        "replica_bytes_attempted": replica_bytes_attempted,
        "write_amplification": write_amplification,
        "max_inflight_records": metrics.gauge("chorus.wal.pipeline.max_inflight_records"),
        "max_inflight_bytes": metrics.gauge("chorus.wal.pipeline.max_inflight_bytes"),
        "lane_capacity_drops": metrics.counter("chorus.wal.lane.capacity_drops"),
        "lane_timeouts": metrics.counter("chorus.wal.lane.timeouts"),
        "pipeline_refills": metrics.counter("chorus.wal.pipeline.refills"),
        "latency_us": {
            "p50": latency.value_at_quantile(0.50),
            "p99": latency.value_at_quantile(0.99),
            "p99_9": latency.value_at_quantile(0.999),
            "max": latency.max(),
        },
        "configuration": {
            "outstanding_appends": args.outstanding_appends,
            "arrival_rate": args.arrival_rate,
            "open_loop_outstanding_cap": open_loop_outstanding_cap,
            "payload_bytes": args.payload_bytes,
            "max_record_bytes": args.max_record_bytes,
            "pipeline_window": args.pipeline_window,
            "max_inflight_bytes": args.max_inflight_bytes,
            "max_replica_lag_bytes": args.max_replica_lag_bytes,
            "lane_stall_timeout_ms": args.lane_stall_timeout_ms,
            "worker_threads": args.worker_threads,
            "segment_bytes": args.segment_bytes,
        }
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    if workload.drain_timed_out {
        handle.abort().await;
    } else {
        handle.shutdown().await?;
    }
    Ok(())
}
