use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::engine::WalEngine;
use crate::metrics::test_support::TestMetricsRecorder;
use crate::record::RecordFrame;
use crate::segment::{segment_object, SegmentedWriter};
use crate::transport::{ReplicaFactory, TransportCode};
use crate::{
    ClientConfig, Error, GrpcReplicaFactory, SegmentedVolume, WalEngineConfig, WalHandle,
    WalRecord, WalSeqNo,
};
use chorus_fake_gcs::{FakeGcs, LatencyProfile, Operation, SimulatedLatency};
use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use tonic::Code;

async fn factory_cluster() -> (
    Vec<chorus_fake_gcs::RunningFake>,
    Vec<Arc<dyn ReplicaFactory>>,
    Arc<dyn ReplicaFactory>,
) {
    factory_cluster_of(3).await
}

async fn factory_cluster_of(
    zones: usize,
) -> (
    Vec<chorus_fake_gcs::RunningFake>,
    Vec<Arc<dyn ReplicaFactory>>,
    Arc<dyn ReplicaFactory>,
) {
    let mut servers = Vec::new();
    let mut factories: Vec<Arc<dyn ReplicaFactory>> = Vec::new();
    for zone in 0..zones {
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
        factories.push(Arc::new(factory));
    }
    // the regional bucket hosting the manifest control register
    let regional = FakeGcs::default().start().await.unwrap();
    let manifest_factory: Arc<dyn ReplicaFactory> = Arc::new(
        GrpcReplicaFactory::connect(
            zones,
            &regional.endpoint,
            "projects/_/buckets/regional".to_string(),
            None,
        )
        .await
        .unwrap(),
    );
    servers.push(regional);
    (servers, factories, manifest_factory)
}

fn zonal_latency(seed: u64) -> LatencyProfile {
    let fast = SimulatedLatency::fixed(Duration::from_millis(2));
    let mutation = SimulatedLatency::between(Duration::from_millis(50), Duration::from_millis(95));
    LatencyProfile::new(seed)
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
        .with_operation(
            Operation::BidiAppendFlush,
            SimulatedLatency::between(Duration::from_millis(1), Duration::from_millis(2)),
        )
        .with_operation(Operation::BidiFinalize, fast)
        .with_operation(Operation::BidiGuardedReplace, mutation)
}

fn regional_latency(seed: u64) -> LatencyProfile {
    LatencyProfile::new(seed)
        .with_operation(
            Operation::BidiWrite,
            SimulatedLatency::fixed(Duration::from_millis(50)),
        )
        .with_operation(
            Operation::Get,
            SimulatedLatency::fixed(Duration::from_millis(2)),
        )
        .with_operation(
            Operation::Update,
            SimulatedLatency::between(Duration::from_millis(50), Duration::from_millis(95)),
        )
}

async fn latency_factory_cluster() -> (
    Vec<chorus_fake_gcs::RunningFake>,
    Vec<Arc<dyn ReplicaFactory>>,
    Arc<dyn ReplicaFactory>,
) {
    let mut servers = Vec::new();
    let mut factories: Vec<Arc<dyn ReplicaFactory>> = Vec::new();
    for zone in 0..3 {
        let server = FakeGcs::with_latency(zonal_latency(393 + zone as u64))
            .start()
            .await
            .unwrap();
        let factory = GrpcReplicaFactory::connect(
            zone,
            &server.endpoint,
            format!("projects/_/buckets/zone-{zone}"),
            None,
        )
        .await
        .unwrap();
        servers.push(server);
        factories.push(Arc::new(factory));
    }
    let regional = FakeGcs::with_latency(regional_latency(397))
        .start()
        .await
        .unwrap();
    let manifest_factory: Arc<dyn ReplicaFactory> = Arc::new(
        GrpcReplicaFactory::connect(
            3,
            &regional.endpoint,
            "projects/_/buckets/regional".to_string(),
            None,
        )
        .await
        .unwrap(),
    );
    servers.push(regional);
    (servers, factories, manifest_factory)
}

fn test_config() -> ClientConfig {
    ClientConfig {
        max_retries: 3,
        retry_base: Duration::ZERO,
    }
}

async fn shutdown_engine(handle: WalHandle) {
    tokio::time::timeout(Duration::from_secs(10), handle.shutdown())
        .await
        .expect("WAL engine shutdown timed out")
        .unwrap();
}

fn record(payload: &[u8]) -> RecordFrame {
    RecordFrame {
        payload: bytes::Bytes::copy_from_slice(payload),
    }
}

fn no_attempted_bytes() -> crate::protocol::AttemptedBytes {
    Arc::new(|_| {})
}

fn volume(
    factories: Vec<Arc<dyn ReplicaFactory>>,
    manifest_factory: Arc<dyn ReplicaFactory>,
    prefix: &str,
) -> SegmentedVolume {
    SegmentedVolume::new_with_factories(factories, manifest_factory, prefix, test_config())
        .expect("test clusters use a supported replica count")
}

fn volume_with_metrics(
    factories: Vec<Arc<dyn ReplicaFactory>>,
    manifest_factory: Arc<dyn ReplicaFactory>,
    prefix: &str,
) -> (SegmentedVolume, Arc<TestMetricsRecorder>) {
    let recorder = Arc::new(TestMetricsRecorder::default());
    let metrics_recorder: Arc<dyn crate::MetricsRecorder> = recorder.clone();
    let volume = SegmentedVolume::new_with_factories_and_metrics_recorder(
        factories,
        manifest_factory,
        prefix,
        test_config(),
        metrics_recorder,
    )
    .expect("test clusters use a supported replica count");
    (volume, recorder)
}

async fn append_one(writer: &mut SegmentedWriter, payload: &[u8]) -> u64 {
    writer
        .enqueue_records(vec![record(payload)], no_attempted_bytes())
        .await
        .unwrap()
        .remove(0)
        .wait()
        .await
        .unwrap()
}

async fn recover_records(
    volume: &SegmentedVolume,
    checkpoint: WalSeqNo,
) -> (WalSeqNo, Vec<WalRecord>) {
    let recovery = volume.recover(checkpoint).await.unwrap();
    let end = recovery.end;
    let records = recovery.try_collect().await.unwrap();
    (end, records)
}

async fn manifest_frontier_ids(
    manifest_factory: &Arc<dyn ReplicaFactory>,
    prefix: &str,
) -> (String, String) {
    let manifest = manifest_factory
        .replica(&format!("{prefix}/manifest"))
        .stat()
        .await
        .unwrap();
    let tail = manifest
        .metadata
        .get("chorus.tail_id")
        .cloned()
        .expect("claimed manifest must name a tail");
    let pending = manifest
        .metadata
        .get("chorus.pending_id")
        .cloned()
        .expect("recovered writer must preregister a pending segment");
    (tail, pending)
}

async fn append_raw_bytes(factory: &Arc<dyn ReplicaFactory>, object: &str, bytes: Vec<u8>) {
    let replica = factory.replica(object);
    let observed = replica.stat().await.unwrap();
    let mut token = replica.takeover(&observed).await.unwrap();
    let offset = token.persisted_size;
    let end = offset + bytes.len() as i64;
    token.persisted_size = replica.append(&token, offset, bytes).await.unwrap();
    assert_eq!(token.persisted_size, end);
}

async fn append_raw_record(factory: &Arc<dyn ReplicaFactory>, object: &str, payload: &[u8]) {
    append_raw_bytes(factory, object, record(payload).encode().unwrap().to_vec()).await;
}

async fn append_partial_raw_record(
    factory: &Arc<dyn ReplicaFactory>,
    object: &str,
    payload: &[u8],
) {
    let mut encoded = record(payload).encode().unwrap().to_vec();
    encoded.pop().expect("test record has a non-empty encoding");
    append_raw_bytes(factory, object, encoded).await;
}

/// Chain position lives in the manifest's segment directory; tests resolve
/// a sealed base to its object id through the register, then check the
/// zone's listing for that copy.
async fn segment_objects_for_base(
    factory: &Arc<dyn ReplicaFactory>,
    manifest_factory: &Arc<dyn ReplicaFactory>,
    prefix: &str,
    base: u64,
) -> Vec<String> {
    let manifest = manifest_factory
        .replica(&format!("{prefix}/manifest"))
        .stat()
        .await
        .unwrap();
    let directory = manifest
        .metadata
        .get("chorus.segments")
        .cloned()
        .unwrap_or_default();
    let Some(id) = directory
        .split(',')
        .filter(|entry| !entry.is_empty())
        .find_map(|entry| {
            let mut fields = entry.split(':');
            let id = fields.next()?;
            let entry_base = fields.next()?;
            (entry_base == base.to_string()).then(|| id.to_string())
        })
    else {
        return Vec::new();
    };
    let name = segment_object(prefix, &id);
    factory
        .list(&format!("{prefix}/segments/"))
        .await
        .unwrap()
        .into_iter()
        .filter(|object| object.name == name)
        .map(|object| object.name)
        .collect()
}

async fn segment_object_for_base(
    factory: &Arc<dyn ReplicaFactory>,
    manifest_factory: &Arc<dyn ReplicaFactory>,
    prefix: &str,
    base: u64,
) -> String {
    let objects = segment_objects_for_base(factory, manifest_factory, prefix, base).await;
    assert_eq!(objects.len(), 1, "expected one segment at base {base}");
    objects.into_iter().next().unwrap()
}

/// The active segment carries no `chorus.base` stamp (its base lives in the
/// manifest), so tests find it through the manifest's tail id.
async fn active_segment_objects(
    factory: &Arc<dyn ReplicaFactory>,
    manifest_factory: &Arc<dyn ReplicaFactory>,
    prefix: &str,
) -> Vec<String> {
    let manifest = manifest_factory
        .replica(&format!("{prefix}/manifest"))
        .snapshot()
        .await
        .unwrap();
    let Some(tail_id) = manifest.metadata.get("chorus.tail_id") else {
        return Vec::new();
    };
    let name = segment_object(prefix, tail_id);
    factory
        .list(&format!("{prefix}/segments/"))
        .await
        .unwrap()
        .into_iter()
        .filter(|object| object.name == name)
        .map(|object| object.name)
        .collect()
}

async fn active_segment_object(
    factory: &Arc<dyn ReplicaFactory>,
    manifest_factory: &Arc<dyn ReplicaFactory>,
    prefix: &str,
) -> String {
    let objects = active_segment_objects(factory, manifest_factory, prefix).await;
    assert_eq!(objects.len(), 1, "expected the active segment on this zone");
    objects.into_iter().next().unwrap()
}

async fn wait_for_counter(metrics: &TestMetricsRecorder, name: &str, minimum: u64) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while metrics.counter(name) < minimum {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{name} never reached {minimum}"));
}

async fn wait_for_repair_passes(metrics: &TestMetricsRecorder, minimum: u64) {
    wait_for_counter(metrics, "chorus.wal.repair.passes", minimum).await;
}

mod engine;
mod maintenance;
mod recovery;
mod topology;
mod transport;
mod writer;
