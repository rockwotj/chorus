use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use chorus_client::{
    ClientConfig, CounterFn, GaugeFn, GrpcReplicaFactory, HistogramFn, MetricsRecorder,
    NoopMetricsRecorder, SegmentedVolume, UpDownCounterFn, WalEngineConfig, WalSeqNo,
};
use chorus_fake_gcs::FakeGcs;
use futures::TryStreamExt;

async fn volume(
    prefix: &str,
    recorder: Arc<dyn MetricsRecorder>,
) -> (Vec<chorus_fake_gcs::RunningFake>, SegmentedVolume) {
    let mut servers = Vec::new();
    let mut factories = Vec::new();
    for zone in 0..3 {
        let server = FakeGcs::default().start().await.unwrap();
        let factory = GrpcReplicaFactory::connect(
            zone,
            &server.endpoint,
            format!("projects/_/buckets/zone-{zone}"),
            None,
        )
        .await
        .unwrap();
        servers.push(server);
        factories.push(factory);
    }

    let regional = FakeGcs::default().start().await.unwrap();
    let manifest_factory =
        GrpcReplicaFactory::connect(3, &regional.endpoint, "projects/_/buckets/regional", None)
            .await
            .unwrap();
    servers.push(regional);

    let volume = SegmentedVolume::new_with_metrics_recorder(
        factories,
        manifest_factory,
        prefix,
        ClientConfig {
            max_retries: 3,
            retry_base: Duration::ZERO,
        },
        recorder,
    )
    .unwrap();
    (servers, volume)
}

#[tokio::test]
async fn public_api_appends_and_replays_records() {
    let (_servers, volume) = volume("public-api-wal", Arc::new(NoopMetricsRecorder)).await;
    let mut recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    assert_eq!(recovery.end, WalSeqNo::ZERO);
    assert!(recovery.try_next().await.unwrap().is_none());

    let mut handle = recovery.start(WalEngineConfig::default()).await.unwrap();
    for (record_index, payload) in [b"one".as_slice(), b"two", b"three"]
        .into_iter()
        .enumerate()
    {
        let receipt = handle
            .enqueue_append(
                WalSeqNo::record(record_index as u64),
                Bytes::copy_from_slice(payload),
            )
            .await
            .unwrap()
            .await
            .unwrap();
        assert_eq!(receipt.seqno, WalSeqNo::record(record_index as u64));
    }
    tokio::time::timeout(Duration::from_secs(10), handle.shutdown())
        .await
        .expect("engine shutdown timed out")
        .unwrap();

    let recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    assert_eq!(recovery.end, WalSeqNo::record(3));
    let records = recovery.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.payload.as_ref())
            .collect::<Vec<_>>(),
        vec![b"one".as_slice(), b"two", b"three"]
    );
    assert_eq!(records[2].next_seqno(), WalSeqNo::record(3));
}

#[derive(Default)]
struct Registrations(Mutex<BTreeSet<(String, &'static str)>>);

impl MetricsRecorder for Registrations {
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        self.0.lock().unwrap().insert((name.into(), "counter"));
        NoopMetricsRecorder.register_counter(name, description, labels)
    }

    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        self.0.lock().unwrap().insert((name.into(), "gauge"));
        NoopMetricsRecorder.register_gauge(name, description, labels)
    }

    fn register_up_down_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn UpDownCounterFn> {
        self.0
            .lock()
            .unwrap()
            .insert((name.into(), "up_down_counter"));
        NoopMetricsRecorder.register_up_down_counter(name, description, labels)
    }

    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
        boundaries: &[f64],
    ) -> Arc<dyn HistogramFn> {
        self.0.lock().unwrap().insert((name.into(), "histogram"));
        if name == "chorus.wal.transport.rpc_seconds" {
            assert!(matches!(
                labels,
                [(
                    "op",
                    "snapshot"
                        | "stat"
                        | "create_appendable"
                        | "create_append_session"
                        | "resume_tail"
                        | "takeover"
                        | "replace_appendable"
                        | "finalize"
                )]
            ));
        }
        NoopMetricsRecorder.register_histogram(name, description, labels, boundaries)
    }
}

// Integration tests compile the library without cfg(test), so this checks the
// real production surface and separately verifies the DST feature boundary.
#[tokio::test]
async fn metric_surface_contains_only_core_and_enabled_dst_instruments() {
    let recorder = Arc::new(Registrations::default());
    let (_servers, _volume) = volume("metric-surface", recorder.clone()).await;
    let core = [
        ("append.committed_records", "counter"),
        ("append.committed_bytes", "counter"),
        ("append.failures", "counter"),
        ("transport.rpc_failures", "counter"),
        ("pipeline.queue_depth", "gauge"),
        ("maintenance.queue_depth", "gauge"),
        ("replica.durable_lag_bytes", "gauge"),
        ("manifest.directory_bytes", "gauge"),
        ("append.commit_latency_seconds", "histogram"),
        ("transport.rpc_seconds", "histogram"),
        ("manifest.cas_latency_seconds", "histogram"),
        ("seal.duration_seconds", "histogram"),
    ];
    let dst = if cfg!(feature = "dst-support") {
        &[
            ("lane.timeouts", "counter"),
            ("lane.capacity_drops", "counter"),
            ("repair.passes", "counter"),
            ("seal.segments", "counter"),
        ][..]
    } else {
        &[][..]
    };
    let expected = core
        .iter()
        .chain(dst)
        .map(|(name, kind)| (format!("chorus.wal.{name}"), *kind))
        .collect();
    assert_eq!(*recorder.0.lock().unwrap(), expected);
}
