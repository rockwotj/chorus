//! Backend-neutral metrics recorder and WAL metric handles.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// Handle for a monotonically increasing metric.
pub trait CounterFn: Send + Sync {
    /// Increase the counter by `value`.
    fn increment(&self, value: u64);
}

/// Handle for an absolute point-in-time metric.
pub trait GaugeFn: Send + Sync {
    /// Set the gauge to `value`.
    fn set(&self, value: i64);
}

/// Handle for an additive metric that may increase or decrease.
pub trait UpDownCounterFn: Send + Sync {
    /// Change the counter by `value`.
    fn increment(&self, value: i64);
}

/// Handle for a sampled distribution.
pub trait HistogramFn: Send + Sync {
    /// Record one observation.
    fn record(&self, value: f64);
}

/// Registers backend-owned metric handles used directly by Chorus.
///
/// Labels are fixed when a handle is registered. Implementations should return
/// another handle for the same backend time series when a name and label set is
/// registered more than once, allowing several WAL volumes to share a registry.
///
/// Most Chorus metrics currently register with an empty label set; the
/// per-replica durable-lag gauge adds only `zone`. Applications that construct
/// several volumes over one backend recorder should therefore pass each volume
/// a small recorder adapter that injects a stable `wal` or `volume` label into
/// every registration while preserving labels supplied here. Passing the same
/// recorder directly makes different volumes aggregate indistinguishably.
pub trait MetricsRecorder: Send + Sync {
    /// Register a monotonically increasing counter.
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn>;

    /// Register an absolute point-in-time gauge.
    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn>;

    /// Register an additive counter that may increase or decrease.
    fn register_up_down_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn UpDownCounterFn>;

    /// Register a histogram with the requested bucket boundaries.
    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
        boundaries: &[f64],
    ) -> Arc<dyn HistogramFn>;
}

/// Recorder used when the application does not configure metrics.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopMetricsRecorder;

#[derive(Debug)]
struct NoopMetric;

impl CounterFn for NoopMetric {
    fn increment(&self, _value: u64) {}
}

impl GaugeFn for NoopMetric {
    fn set(&self, _value: i64) {}
}

impl UpDownCounterFn for NoopMetric {
    fn increment(&self, _value: i64) {}
}

impl HistogramFn for NoopMetric {
    fn record(&self, _value: f64) {}
}

impl MetricsRecorder for NoopMetricsRecorder {
    fn register_counter(
        &self,
        _name: &str,
        _description: &str,
        _labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        Arc::new(NoopMetric)
    }

    fn register_gauge(
        &self,
        _name: &str,
        _description: &str,
        _labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        Arc::new(NoopMetric)
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

pub(crate) struct Counter(Arc<dyn CounterFn>);

impl Counter {
    fn register(recorder: &dyn MetricsRecorder, name: &str, description: &str) -> Self {
        Self(recorder.register_counter(name, description, &[]))
    }

    pub(crate) fn increment(&self) {
        self.add(1);
    }

    pub(crate) fn add(&self, value: u64) {
        self.0.increment(value);
    }
}

pub(crate) struct Gauge(Arc<dyn GaugeFn>);

impl Gauge {
    fn register(recorder: &dyn MetricsRecorder, name: &str, description: &str) -> Self {
        Self(recorder.register_gauge(name, description, &[]))
    }

    fn register_with_labels(
        recorder: &dyn MetricsRecorder,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Self {
        Self(recorder.register_gauge(name, description, labels))
    }

    pub(crate) fn set(&self, value: i64) {
        self.0.set(value);
    }

    pub(crate) fn set_u64(&self, value: u64) {
        self.set(i64::try_from(value).unwrap_or(i64::MAX));
    }

    pub(crate) fn set_usize(&self, value: usize) {
        self.set(i64::try_from(value).unwrap_or(i64::MAX));
    }
}

/// Crate-private aggregation for gauges with several concurrent contributors.
///
/// Segment rotation briefly overlaps old and successor writers. Keeping the
/// per-writer contributions here avoids exposing bookkeeping callbacks through
/// the public [`MetricsRecorder`] API while presenting one operator gauge per
/// zone.
pub(crate) struct AggregateGauge {
    total: AtomicI64,
    gauge: Gauge,
}

impl AggregateGauge {
    fn register(
        recorder: &dyn MetricsRecorder,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Self {
        Self {
            total: AtomicI64::new(0),
            gauge: Gauge::register_with_labels(recorder, name, description, labels),
        }
    }

    pub(crate) fn add(&self, value: i64) {
        let total = self.total.fetch_add(value, Ordering::Relaxed) + value;
        self.gauge.set(total.max(0));
        let current = self.total.load(Ordering::Relaxed);
        if current != total {
            self.gauge.set(current.max(0));
        }
    }
}

pub(crate) struct UpDownCounter(Arc<dyn UpDownCounterFn>);

impl UpDownCounter {
    fn register(recorder: &dyn MetricsRecorder, name: &str, description: &str) -> Self {
        Self(recorder.register_up_down_counter(name, description, &[]))
    }

    pub(crate) fn add(&self, value: i64) {
        self.0.increment(value);
    }
}

/// Prometheus-style latency buckets in seconds, wide enough to span a fast
/// zonal append acknowledgment and a slow regional seal alike.
const LATENCY_SECONDS_BOUNDARIES: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

pub(crate) struct Histogram(Arc<dyn HistogramFn>);

impl Histogram {
    fn register(recorder: &dyn MetricsRecorder, name: &str, description: &str) -> Self {
        Self(recorder.register_histogram(name, description, &[], LATENCY_SECONDS_BOUNDARIES))
    }

    pub(crate) fn record_duration(&self, duration: std::time::Duration) {
        self.0.record(duration.as_secs_f64());
    }
}

pub(crate) struct HighWaterGauge {
    high_water: AtomicI64,
    gauge: Gauge,
}

impl HighWaterGauge {
    fn register(recorder: &dyn MetricsRecorder, name: &str, description: &str) -> Self {
        Self {
            high_water: AtomicI64::new(0),
            gauge: Gauge::register(recorder, name, description),
        }
    }

    pub(crate) fn update_max(&self, value: u64) {
        let value = i64::try_from(value).unwrap_or(i64::MAX);
        if self.high_water.fetch_max(value, Ordering::Relaxed) < value {
            self.gauge.set(value);
            let current = self.high_water.load(Ordering::Relaxed);
            if current > value {
                self.gauge.set(current);
            }
        }
    }
}

/// Handles for all metrics emitted by one WAL volume.
pub(crate) struct Metrics {
    pub(crate) append_records: Counter,
    pub(crate) append_bytes: Counter,
    pub(crate) append_failures: Counter,
    pub(crate) committed_records: Counter,
    pub(crate) committed_bytes: Counter,
    pub(crate) batches_sent: Counter,
    pub(crate) lane_retries: Counter,
    pub(crate) lane_timeouts: Counter,
    pub(crate) rotations_completed: Counter,
    pub(crate) spare_provisioning_attempts: Counter,
    pub(crate) spare_provisioning_failures: Counter,
    pub(crate) segments_sealed: Counter,
    pub(crate) seal_enforcement_retries: Counter,
    pub(crate) manifest_cas_attempts: Counter,
    pub(crate) manifest_cas_conflicts: Counter,
    pub(crate) repair_passes: Counter,
    pub(crate) repair_objects_repaired: Counter,
    pub(crate) repair_transient_skips: Counter,
    pub(crate) repair_failures: Counter,
    pub(crate) recoveries_run: Counter,
    pub(crate) recovery_segments_adopted: Counter,
    pub(crate) orphan_objects_deleted: Counter,
    pub(crate) orphan_sweeps_deferred: Counter,
    pub(crate) truncation_cycles: Counter,
    pub(crate) operation_failures: Counter,
    pub(crate) max_inflight_records: HighWaterGauge,
    pub(crate) max_inflight_bytes: HighWaterGauge,
    pub(crate) pipeline_refills: Counter,
    pub(crate) lane_capacity_drops: Counter,
    pub(crate) wal_record_bytes: Counter,
    pub(crate) replica_bytes_attempted: Counter,
    pub(crate) open_segments: UpDownCounter,
    pub(crate) committed_records_watermark: Gauge,
    pub(crate) queue_depth: Gauge,
    zone_durable_lag: Vec<AggregateGauge>,
    pub(crate) rotation_state: Gauge,
    maintenance_queue_depth: AggregateGauge,
    pub(crate) manifest_directory_bytes: Gauge,
    pub(crate) append_commit_latency: Histogram,
    pub(crate) manifest_cas_latency: Histogram,
    pub(crate) seal_duration: Histogram,
}

impl Metrics {
    pub(crate) fn new(recorder: &dyn MetricsRecorder, replica_count: usize) -> Self {
        macro_rules! counter {
            ($name:literal, $description:literal) => {
                Counter::register(recorder, $name, $description)
            };
        }
        macro_rules! gauge {
            ($name:literal, $description:literal) => {
                Gauge::register(recorder, $name, $description)
            };
        }
        macro_rules! high_water {
            ($name:literal, $description:literal) => {
                HighWaterGauge::register(recorder, $name, $description)
            };
        }
        macro_rules! histogram {
            ($name:literal, $description:literal) => {
                Histogram::register(recorder, $name, $description)
            };
        }

        let zone_durable_lag = (0..replica_count)
            .map(|zone| {
                let zone = zone.to_string();
                AggregateGauge::register(
                    recorder,
                    "chorus.wal.replica.durable_lag_bytes",
                    "Encoded admitted bytes not yet durable in this replica zone",
                    &[("zone", zone.as_str())],
                )
            })
            .collect();

        Self {
            append_records: counter!(
                "chorus.wal.append.records",
                "Application records admitted for append"
            ),
            append_bytes: counter!(
                "chorus.wal.append.bytes",
                "Application payload bytes admitted for append"
            ),
            append_failures: counter!(
                "chorus.wal.append.failures",
                "Admitted appends that completed with an error"
            ),
            committed_records: counter!(
                "chorus.wal.append.committed_records",
                "Records committed in contiguous sequence order"
            ),
            committed_bytes: counter!(
                "chorus.wal.append.committed_bytes",
                "Application payload bytes committed in contiguous sequence order"
            ),
            batches_sent: counter!(
                "chorus.wal.batch.sent",
                "Logical record batches submitted to replication"
            ),
            lane_retries: counter!(
                "chorus.wal.lane.retries",
                "Replica lane recovery or resend attempts"
            ),
            lane_timeouts: counter!(
                "chorus.wal.lane.timeouts",
                "Replica lanes shed after making no durable progress before their timeout"
            ),
            rotations_completed: counter!(
                "chorus.wal.rotation.completed",
                "Active-segment rotations completed"
            ),
            spare_provisioning_attempts: counter!(
                "chorus.wal.rotation.spare_provisioning_attempts",
                "Background spare provisioning attempts"
            ),
            spare_provisioning_failures: counter!(
                "chorus.wal.rotation.spare_provisioning_failures",
                "Background spare provisioning failures"
            ),
            segments_sealed: counter!(
                "chorus.wal.seal.segments",
                "Segments whose committed seal was enforced"
            ),
            seal_enforcement_retries: counter!(
                "chorus.wal.seal.enforcement_retries",
                "Committed-seal enforcement retry attempts started by maintenance"
            ),
            manifest_cas_attempts: counter!(
                "chorus.wal.manifest.cas_attempts",
                "Manifest compare-and-swap requests sent to storage"
            ),
            manifest_cas_conflicts: counter!(
                "chorus.wal.manifest.cas_conflicts",
                "Manifest compare-and-swap precondition conflicts"
            ),
            repair_passes: counter!(
                "chorus.wal.repair.passes",
                "Sealed-segment repair passes completed or skipped after an error"
            ),
            repair_objects_repaired: counter!(
                "chorus.wal.repair.objects_repaired",
                "Missing or divergent sealed objects repaired"
            ),
            repair_transient_skips: counter!(
                "chorus.wal.repair.transient_skips",
                "Repair targets skipped after transient failures"
            ),
            repair_failures: counter!(
                "chorus.wal.repair.failures",
                "Repair passes aborted by an error"
            ),
            recoveries_run: counter!("chorus.wal.recovery.runs", "Recovery attempts started"),
            recovery_segments_adopted: counter!(
                "chorus.wal.recovery.segments_adopted",
                "Sealed segments adopted by recovery"
            ),
            orphan_objects_deleted: counter!(
                "chorus.wal.orphan.objects_deleted",
                "Dead-incarnation segment-object copies deleted by maintenance"
            ),
            orphan_sweeps_deferred: counter!(
                "chorus.wal.orphan.sweeps_deferred",
                "Dead-incarnation sweeps deferred after transient or terminal storage failures"
            ),
            truncation_cycles: counter!(
                "chorus.wal.truncation.cycles",
                "Application-triggered truncation cycles completed"
            ),
            operation_failures: counter!(
                "chorus.wal.operation.failures",
                "Engine and maintenance operations completed with errors"
            ),
            max_inflight_records: high_water!(
                "chorus.wal.pipeline.max_inflight_records",
                "High-water mark of dispatched uncommitted records"
            ),
            max_inflight_bytes: high_water!(
                "chorus.wal.pipeline.max_inflight_bytes",
                "High-water mark of admitted unresolved encoded bytes"
            ),
            pipeline_refills: counter!(
                "chorus.wal.pipeline.refills",
                "Pipeline refills before all prior records completed"
            ),
            lane_capacity_drops: counter!(
                "chorus.wal.lane.capacity_drops",
                "Replica lanes dropped after exceeding their retained-byte budget"
            ),
            wal_record_bytes: counter!(
                "chorus.wal.append.encoded_bytes",
                "Encoded durable record bytes generated before replication"
            ),
            replica_bytes_attempted: counter!(
                "chorus.wal.replica.bytes_attempted",
                "Encoded bytes handed to replica transports including retries"
            ),
            open_segments: UpDownCounter::register(
                recorder,
                "chorus.wal.catalog.open_segments",
                "Active appendable segments owned by this client",
            ),
            committed_records_watermark: gauge!(
                "chorus.wal.append.committed_watermark",
                "Exclusive committed record boundary"
            ),
            queue_depth: gauge!(
                "chorus.wal.pipeline.queue_depth",
                "Appends waiting across the admission channel and engine queue"
            ),
            zone_durable_lag,
            rotation_state: gauge!(
                "chorus.wal.rotation.state",
                "Rotation gate state: 0 idle, 1 due, 2 draining, 3 sealing, 4 disabled"
            ),
            maintenance_queue_depth: AggregateGauge::register(
                recorder,
                "chorus.wal.maintenance.queue_depth",
                "Maintenance requests waiting for execution",
                &[],
            ),
            manifest_directory_bytes: gauge!(
                "chorus.wal.manifest.directory_bytes",
                "Encoded bytes used by the sealed segment directory"
            ),
            append_commit_latency: histogram!(
                "chorus.wal.append.commit_latency_seconds",
                "Seconds from append admission to contiguous quorum commit"
            ),
            manifest_cas_latency: histogram!(
                "chorus.wal.manifest.cas_latency_seconds",
                "Latency of one manifest compare-and-swap request"
            ),
            seal_duration: histogram!(
                "chorus.wal.seal.duration_seconds",
                "Seconds to finalize a swapped-out segment"
            ),
        }
    }

    pub(crate) fn adjust_zone_durable_lag(&self, zone: usize, delta: i64) {
        if let Some(gauge) = self.zone_durable_lag.get(zone) {
            gauge.add(delta);
        }
    }

    pub(crate) fn adjust_maintenance_queue_depth(&self, delta: i64) {
        self.maintenance_queue_depth.add(delta);
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use super::{CounterFn, GaugeFn, HistogramFn, MetricsRecorder, UpDownCounterFn};

    #[derive(Default)]
    pub(crate) struct TestMetricsRecorder {
        counters: Mutex<HashMap<String, Arc<AtomicU64>>>,
        gauges: Mutex<HashMap<String, Arc<AtomicI64>>>,
        up_down_counters: Mutex<HashMap<String, Arc<AtomicI64>>>,
        histograms: Mutex<HashMap<String, Arc<Mutex<Vec<f64>>>>>,
    }

    impl TestMetricsRecorder {
        pub(crate) fn counter(&self, name: &str) -> u64 {
            self.counters.lock().unwrap()[name].load(Ordering::Relaxed)
        }

        pub(crate) fn gauge(&self, name: &str) -> i64 {
            self.gauges.lock().unwrap()[name].load(Ordering::Relaxed)
        }

        pub(crate) fn labeled_gauge(&self, name: &str, labels: &[(&str, &str)]) -> i64 {
            self.gauges.lock().unwrap()[&metric_key(name, labels)].load(Ordering::Relaxed)
        }

        pub(crate) fn up_down_counter(&self, name: &str) -> i64 {
            self.up_down_counters.lock().unwrap()[name].load(Ordering::Relaxed)
        }

        pub(crate) fn histogram_samples(&self, name: &str) -> usize {
            self.histograms.lock().unwrap()[name].lock().unwrap().len()
        }
    }

    struct TestCounter(Arc<AtomicU64>);

    impl CounterFn for TestCounter {
        fn increment(&self, value: u64) {
            self.0.fetch_add(value, Ordering::Relaxed);
        }
    }

    struct TestGauge(Arc<AtomicI64>);

    impl GaugeFn for TestGauge {
        fn set(&self, value: i64) {
            self.0.store(value, Ordering::Relaxed);
        }
    }

    struct TestUpDownCounter(Arc<AtomicI64>);

    impl UpDownCounterFn for TestUpDownCounter {
        fn increment(&self, value: i64) {
            self.0.fetch_add(value, Ordering::Relaxed);
        }
    }

    struct TestHistogram(Arc<Mutex<Vec<f64>>>);

    impl HistogramFn for TestHistogram {
        fn record(&self, value: f64) {
            self.0.lock().unwrap().push(value);
        }
    }

    impl MetricsRecorder for TestMetricsRecorder {
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
            Arc::new(TestCounter(metric))
        }

        fn register_gauge(
            &self,
            name: &str,
            _description: &str,
            labels: &[(&str, &str)],
        ) -> Arc<dyn GaugeFn> {
            let key = metric_key(name, labels);
            let metric = self.gauges.lock().unwrap().entry(key).or_default().clone();
            Arc::new(TestGauge(metric))
        }

        fn register_up_down_counter(
            &self,
            name: &str,
            _description: &str,
            _labels: &[(&str, &str)],
        ) -> Arc<dyn UpDownCounterFn> {
            let metric = self
                .up_down_counters
                .lock()
                .unwrap()
                .entry(name.to_string())
                .or_default()
                .clone();
            Arc::new(TestUpDownCounter(metric))
        }

        fn register_histogram(
            &self,
            name: &str,
            _description: &str,
            _labels: &[(&str, &str)],
            _boundaries: &[f64],
        ) -> Arc<dyn HistogramFn> {
            let metric = self
                .histograms
                .lock()
                .unwrap()
                .entry(name.to_string())
                .or_default()
                .clone();
            Arc::new(TestHistogram(metric))
        }
    }

    fn metric_key(name: &str, labels: &[(&str, &str)]) -> String {
        labels
            .iter()
            .fold(name.to_string(), |mut key, (name, value)| {
                key.push('{');
                key.push_str(name);
                key.push('=');
                key.push_str(value);
                key.push('}');
                key
            })
    }
}
