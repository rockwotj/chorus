//! Live writer-plus-readonly-follower benchmark.
//!
//! The workload remains in one appendable segment. Each writer completion is
//! timestamped, and the independent follower consumes the same record through
//! quorum bidirectional reads. The latency histogram therefore measures
//! commit completion to subscriber delivery without segment-rotation delay.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use chorus_client::{
    ClientConfig, MetricsRecorder, ReadOnlyConfig, ReadOnlyFollower, SegmentedVolume,
    WalEngineConfig, WalHandle, WalSeqNo,
};
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use hdrhistogram::Histogram;
use tokio::sync::Notify;

use super::BenchMetrics;
use crate::ConnectedStorage;

#[derive(clap::Args, Debug)]
pub(crate) struct ReadOnlyArgs {
    /// Number of records to append and deliver through the active segment.
    #[arg(long, default_value_t = 4096)]
    records: usize,
    #[arg(long, default_value_t = 4096)]
    payload_bytes: usize,
    #[arg(long, default_value_t = 64)]
    pipeline_window: usize,
    /// Delay between readonly active-tail polls.
    #[arg(long, default_value_t = 100)]
    poll_interval_ms: u64,
    /// Maximum wait for writer completion and follower catch-up.
    #[arg(long, default_value_t = 180)]
    timeout_seconds: u64,
    #[arg(long, default_value_t = 4)]
    worker_threads: usize,
}

impl ReadOnlyArgs {
    pub(crate) fn worker_threads(&self) -> usize {
        self.worker_threads
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.records == 0
            || self.payload_bytes == 0
            || self.pipeline_window == 0
            || self.poll_interval_ms == 0
            || self.timeout_seconds == 0
            || self.worker_threads == 0
        {
            bail!("all readonly benchmark counts and intervals must be positive");
        }
        self.payload_bytes
            .checked_add(4)
            .and_then(|encoded| encoded.checked_mul(self.records))
            .and_then(|bytes| bytes.checked_mul(2))
            .context("readonly benchmark active segment size overflowed usize")?;
        Ok(())
    }
}

#[derive(Default)]
struct CommitTimeline {
    committed_at: Mutex<HashMap<u64, Instant>>,
    notify: Notify,
}

impl CommitTimeline {
    fn publish(&self, record_index: u64, at: Instant) {
        self.committed_at.lock().unwrap().insert(record_index, at);
        self.notify.notify_waiters();
    }

    async fn take(&self, record_index: u64) -> Instant {
        loop {
            let notified = self.notify.notified();
            if let Some(at) = self.committed_at.lock().unwrap().remove(&record_index) {
                return at;
            }
            notified.await;
        }
    }
}

struct SubscriberStats {
    records: u64,
    commit_to_subscribe: Histogram<u64>,
}

impl SubscriberStats {
    fn new() -> Result<Self> {
        Ok(Self {
            records: 0,
            commit_to_subscribe: Histogram::new_with_bounds(1, 600_000_000, 3)?,
        })
    }
}

async fn append_records(
    handle: &mut WalHandle,
    count: u64,
    payload: &Bytes,
    window: usize,
    commits: &CommitTimeline,
) -> Result<()> {
    let mut next = 0u64;
    let mut inflight = FuturesUnordered::new();
    while inflight.len() < window && next < count {
        inflight.push(
            handle
                .enqueue_append(WalSeqNo::record(next), payload.clone())
                .await?,
        );
        next += 1;
    }
    while let Some(result) = inflight.next().await {
        let receipt = result?;
        commits.publish(receipt.seqno.record_index, Instant::now());
        if next < count {
            inflight.push(
                handle
                    .enqueue_append(WalSeqNo::record(next), payload.clone())
                    .await?,
            );
            next += 1;
        }
    }
    Ok(())
}

async fn run_subscriber(
    mut follower: ReadOnlyFollower,
    commits: Arc<CommitTimeline>,
    target: u64,
) -> Result<SubscriberStats> {
    let mut stats = SubscriberStats::new()?;
    let mut next = follower.from.record_index;
    while next < target {
        let record = follower
            .try_next()
            .await?
            .context("readonly follower ended unexpectedly")?;
        if record.seqno.record_index != next {
            bail!(
                "readonly follower returned {}, expected {next}",
                record.seqno.record_index
            );
        }
        let observed_at = Instant::now();
        let committed_at = commits.take(next).await;
        record_us(
            &mut stats.commit_to_subscribe,
            observed_at.saturating_duration_since(committed_at),
        )?;
        stats.records += 1;
        next += 1;
    }
    Ok(stats)
}

pub(crate) async fn run(
    storage: ConnectedStorage,
    prefix: String,
    args: ReadOnlyArgs,
) -> Result<()> {
    let metrics = Arc::new(BenchMetrics::default());
    let metrics_recorder: Arc<dyn MetricsRecorder> = metrics.clone();
    let writer_volume = SegmentedVolume::new_with_metrics_recorder(
        storage.factories.clone(),
        storage.manifest_factory.clone(),
        &prefix,
        ClientConfig::default(),
        metrics_recorder,
    )?;
    let reader_volume = SegmentedVolume::new(
        storage.factories,
        storage.manifest_factory,
        &prefix,
        ClientConfig::default(),
    )?;

    let mut recovery = writer_volume.recover(WalSeqNo::ZERO).await?;
    if recovery.end != WalSeqNo::ZERO {
        bail!("readonly benchmark requires an unused prefix");
    }
    while recovery.try_next().await?.is_some() {}

    let active_segment_bytes = (args.payload_bytes + 4)
        .checked_mul(args.records)
        .and_then(|bytes| bytes.checked_mul(2))
        .context("readonly benchmark active segment size overflowed usize")?;
    let mut writer = recovery
        .start(WalEngineConfig {
            queue_capacity: args.pipeline_window.saturating_mul(8),
            max_record_bytes: args
                .payload_bytes
                .max(WalEngineConfig::default().max_record_bytes),
            pipeline_window_records: args.pipeline_window,
            max_inflight_bytes: WalEngineConfig::default().max_inflight_bytes,
            max_replica_lag_bytes: WalEngineConfig::default().max_replica_lag_bytes,
            lane_stall_timeout: WalEngineConfig::default().lane_stall_timeout,
            max_segment_bytes: active_segment_bytes,
            max_active_segment_bytes: active_segment_bytes,
            repair_interval: None,
            shutdown_timeout: WalEngineConfig::default().shutdown_timeout,
        })
        .await?;

    let follower = reader_volume
        .open_readonly_with_config(
            WalSeqNo::ZERO,
            ReadOnlyConfig {
                poll_interval: Duration::from_millis(args.poll_interval_ms),
            },
        )
        .await?;
    let commits = Arc::new(CommitTimeline::default());
    let subscriber_commits = Arc::clone(&commits);
    let target = u64::try_from(args.records).context("record count overflowed u64")?;
    let subscriber =
        tokio::spawn(async move { run_subscriber(follower, subscriber_commits, target).await });

    let payload = Bytes::from(vec![0x5a; args.payload_bytes]);
    let benchmark_started = Instant::now();
    let writer_started = Instant::now();
    let timeout = Duration::from_secs(args.timeout_seconds);
    tokio::time::timeout(
        timeout,
        append_records(
            &mut writer,
            target,
            &payload,
            args.pipeline_window,
            &commits,
        ),
    )
    .await
    .context("timed out writing readonly benchmark records")??;
    let writer_elapsed = writer_started.elapsed();

    let subscriber_stats = tokio::time::timeout(timeout, subscriber)
        .await
        .context("timed out waiting for readonly follower")??
        .context("readonly follower task failed")?;
    let total_elapsed = benchmark_started.elapsed();
    writer.shutdown().await?;

    if metrics.rotation_count() != 0 {
        bail!(
            "readonly active-tail benchmark unexpectedly rotated {} segments",
            metrics.rotation_count()
        );
    }

    let payload_mib = args.payload_bytes as f64 / (1024.0 * 1024.0);
    let report = serde_json::json!({
        "benchmark": "readonly-active-tail",
        "writer": {
            "committed_records": target,
            "duration_seconds": writer_elapsed.as_secs_f64(),
            "record_iops": target as f64 / writer_elapsed.as_secs_f64(),
            "payload_mib_per_second":
                target as f64 * payload_mib / writer_elapsed.as_secs_f64(),
        },
        "subscriber": {
            "delivered_records": subscriber_stats.records,
            "end_to_end_duration_seconds": total_elapsed.as_secs_f64(),
            "record_iops": target as f64 / total_elapsed.as_secs_f64(),
            "payload_mib_per_second":
                target as f64 * payload_mib / total_elapsed.as_secs_f64(),
        },
        "commit_to_subscribe_latency_us":
            summarize(&subscriber_stats.commit_to_subscribe),
        "configuration": {
            "records": args.records,
            "payload_bytes": args.payload_bytes,
            "pipeline_window": args.pipeline_window,
            "active_segment_bytes": active_segment_bytes,
            "poll_interval_ms": args.poll_interval_ms,
            "worker_threads": args.worker_threads,
        }
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn summarize(histogram: &Histogram<u64>) -> serde_json::Value {
    serde_json::json!({
        "samples": histogram.len(),
        "p50": histogram.value_at_quantile(0.50),
        "p90": histogram.value_at_quantile(0.90),
        "p99": histogram.value_at_quantile(0.99),
        "p99_9": histogram.value_at_quantile(0.999),
        "max": histogram.max(),
        "mean": histogram.mean().round() as u64,
    })
}

fn record_us(histogram: &mut Histogram<u64>, duration: Duration) -> Result<()> {
    histogram.record((duration.as_micros().max(1) as u64).min(599_000_000))?;
    Ok(())
}
