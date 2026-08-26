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

    fn register_with_labels(
        recorder: &dyn MetricsRecorder,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Self {
        Self(recorder.register_counter(name, description, labels))
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

/// Every `TransportCode`, so a failure counter exists for each and a code that
/// never fires is visibly zero rather than absent.
const TRANSPORT_FAILURE_CODES: &[&str] = &[
    "NotFound",
    "AlreadyExists",
    "InvalidArgument",
    "FailedPrecondition",
    "Aborted",
    "OutOfRange",
    "ResourceExhausted",
    "Unimplemented",
    "DataLoss",
    "Ambiguous",
    "Unauthenticated",
    "PermissionDenied",
    "Unavailable",
    "DeadlineExceeded",
    "Internal",
];

/// Latency buckets spanning fast zonal acknowledgments and slow regional seals.
const LATENCY_SECONDS_BOUNDARIES: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

pub(crate) struct Histogram(Arc<dyn HistogramFn>);

impl Histogram {
    fn register(recorder: &dyn MetricsRecorder, name: &str, description: &str) -> Self {
        Self(recorder.register_histogram(name, description, &[], LATENCY_SECONDS_BOUNDARIES))
    }

    fn register_with_labels(
        recorder: &dyn MetricsRecorder,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Self {
        Self(recorder.register_histogram(name, description, labels, LATENCY_SECONDS_BOUNDARIES))
    }

    pub(crate) fn record_duration(&self, duration: std::time::Duration) {
        self.0.record(duration.as_secs_f64());
    }
}

/// Handles for all metrics emitted by one WAL volume.
///
/// Four event counters are available only to deterministic simulations and
/// unit tests: they synchronize fault injection and check protocol coverage,
/// rather than describing production health.
pub(crate) struct Metrics {
    pub(crate) append_failures: Counter,
    pub(crate) committed_records: Counter,
    pub(crate) committed_bytes: Counter,
    #[cfg(any(test, feature = "dst-support"))]
    pub(crate) lane_timeouts: Counter,
    #[cfg(any(test, feature = "dst-support"))]
    pub(crate) segments_sealed: Counter,
    #[cfg(any(test, feature = "dst-support"))]
    pub(crate) repair_passes: Counter,
    /// Latency and failure codes of timed quorum-path replica operations.
    pub(crate) rpc: TransportRpcMetrics,
    #[cfg(any(test, feature = "dst-support"))]
    pub(crate) lane_capacity_drops: Counter,
    pub(crate) queue_depth: Gauge,
    zone_durable_lag: Vec<AggregateGauge>,
    maintenance_queue_depth: AggregateGauge,
    pub(crate) manifest_directory_bytes: Gauge,
    pub(crate) append_commit_latency: Histogram,
    pub(crate) manifest_cas_latency: Histogram,
    pub(crate) seal_duration: Histogram,
}

/// One histogram per provider operation, plus failures keyed by the code the
/// provider returned. `ResourceExhausted` in particular is a per-object
/// mutation-rate limit rather than a client fault, and is invisible in a
/// latency figure alone.
pub(crate) struct TransportRpcMetrics {
    pub(crate) snapshot: Histogram,
    pub(crate) stat: Histogram,
    pub(crate) create_appendable: Histogram,
    pub(crate) create_append_session: Histogram,
    pub(crate) resume_tail: Histogram,
    pub(crate) takeover: Histogram,
    pub(crate) replace_appendable: Histogram,
    pub(crate) finalize: Histogram,
    failures: Vec<(&'static str, Counter)>,
}

impl TransportRpcMetrics {
    fn register(recorder: &dyn MetricsRecorder) -> Self {
        let rpc = |op: &str| {
            Histogram::register_with_labels(
                recorder,
                "chorus.wal.transport.rpc_seconds",
                "Latency of one quorum-path replica operation",
                &[("op", op)],
            )
        };
        let failures = TRANSPORT_FAILURE_CODES
            .iter()
            .map(|code| {
                (
                    *code,
                    Counter::register_with_labels(
                        recorder,
                        "chorus.wal.transport.rpc_failures",
                        "Provider RPCs that returned an error, by provider code",
                        &[("code", code)],
                    ),
                )
            })
            .collect();
        Self {
            snapshot: rpc("snapshot"),
            stat: rpc("stat"),
            create_appendable: rpc("create_appendable"),
            create_append_session: rpc("create_append_session"),
            resume_tail: rpc("resume_tail"),
            takeover: rpc("takeover"),
            replace_appendable: rpc("replace_appendable"),
            finalize: rpc("finalize"),
            failures,
        }
    }

    pub(crate) fn record_failure(&self, code: &str) {
        if let Some((_, counter)) = self.failures.iter().find(|(name, _)| *name == code) {
            counter.increment();
        }
    }
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
            #[cfg(any(test, feature = "dst-support"))]
            lane_timeouts: counter!(
                "chorus.wal.lane.timeouts",
                "Replica lanes shed after making no durable progress before their timeout"
            ),
            #[cfg(any(test, feature = "dst-support"))]
            segments_sealed: counter!(
                "chorus.wal.seal.segments",
                "Segments whose committed seal was enforced"
            ),
            #[cfg(any(test, feature = "dst-support"))]
            repair_passes: counter!(
                "chorus.wal.repair.passes",
                "Sealed-segment repair passes completed or skipped after an error"
            ),
            #[cfg(any(test, feature = "dst-support"))]
            lane_capacity_drops: counter!(
                "chorus.wal.lane.capacity_drops",
                "Replica lanes dropped after exceeding their retained-byte budget"
            ),
            queue_depth: gauge!(
                "chorus.wal.pipeline.queue_depth",
                "Appends waiting across the admission channel and engine queue"
            ),
            zone_durable_lag,
            rpc: TransportRpcMetrics::register(recorder),
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
