use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use chorus_client::{CounterFn, GaugeFn, HistogramFn, MetricsRecorder, UpDownCounterFn};

pub(crate) mod append;
pub(crate) mod readonly;
pub(crate) mod recovery;

#[derive(Default)]
pub(crate) struct BenchMetrics {
    counters: Mutex<HashMap<String, Arc<AtomicU64>>>,
    histogram_samples: Mutex<HashMap<String, Arc<AtomicU64>>>,
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

    /// Number of values recorded into `name`.
    pub(crate) fn histogram_samples(&self, name: &str) -> u64 {
        self.histogram_samples
            .lock()
            .unwrap()
            .get(name)
            .map(|samples| samples.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Segments sealed so far. Every rotation seals the segment it rotates
    /// away from, so a non-zero count means the active segment moved.
    pub(crate) fn seal_count(&self) -> u64 {
        self.histogram_samples("chorus.wal.seal.duration_seconds")
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

struct BenchHistogram {
    samples: Arc<AtomicU64>,
}

impl HistogramFn for BenchHistogram {
    fn record(&self, _value: f64) {
        self.samples.fetch_add(1, Ordering::SeqCst);
    }
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
        name: &str,
        _description: &str,
        _labels: &[(&str, &str)],
        _boundaries: &[f64],
    ) -> Arc<dyn HistogramFn> {
        let samples = self
            .histogram_samples
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_default()
            .clone();
        Arc::new(BenchHistogram { samples })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_metrics_count_seals() {
        let metrics = BenchMetrics::default();
        let seals =
            metrics.register_histogram("chorus.wal.seal.duration_seconds", "test", &[], &[]);

        assert_eq!(metrics.seal_count(), 0);
        seals.record(0.5);
        seals.record(1.5);

        assert_eq!(metrics.seal_count(), 2);
    }
}
