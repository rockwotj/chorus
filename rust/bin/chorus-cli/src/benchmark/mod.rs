use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use chorus_client::{CounterFn, GaugeFn, HistogramFn, MetricsRecorder, UpDownCounterFn};

pub(crate) mod append;
pub(crate) mod readonly;
pub(crate) mod recovery;

#[derive(Default)]
pub(crate) struct BenchMetrics {
    counters: Mutex<HashMap<String, Arc<AtomicU64>>>,
    gauges: Mutex<HashMap<String, Arc<AtomicI64>>>,
}

impl BenchMetrics {
    pub(crate) fn counter(&self, name: &str) -> u64 {
        self.counters
            .lock()
            .unwrap()
            .get(name)
            .map(|counter| counter.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub(crate) fn gauge(&self, name: &str) -> i64 {
        self.gauges
            .lock()
            .unwrap()
            .get(name)
            .map(|gauge| gauge.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub(crate) fn rotation_count(&self) -> usize {
        self.counter("chorus.wal.rotation.completed") as usize
    }
}

struct BenchCounter {
    counter: Arc<AtomicU64>,
}

impl CounterFn for BenchCounter {
    fn increment(&self, value: u64) {
        self.counter.fetch_add(value, Ordering::SeqCst);
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
        Arc::new(BenchCounter { counter: metric })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_metrics_count_rotations() {
        let metrics = BenchMetrics::default();
        let rotations = metrics.register_counter("chorus.wal.rotation.completed", "test", &[]);

        rotations.increment(1);
        rotations.increment(1);

        assert_eq!(metrics.rotation_count(), 2);
    }
}
