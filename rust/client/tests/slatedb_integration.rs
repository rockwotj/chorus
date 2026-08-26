#![cfg(feature = "slatedb")]

// Exercise the library as a downstream application: no private adapter helpers,
// cfg(test) initialization path, WalReader, or WalAdmin implementation.
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chorus_client::{
    slatedb::ChorusWal, ClientConfig, GrpcReplicaFactory, SegmentedVolume, WalEngineConfig,
};
use chorus_fake_gcs::{FakeGcs, LatencyProfile, Operation, RunningFake, SimulatedLatency};
use slatedb::config::{
    CloseOptions, FlushOptions, FlushType, GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions, Settings,
};
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::{Db, GarbageCollectorBuilder, WriteBatch};

const PREFIX: &str = "public-slatedb";
const KEY_COUNT: u64 = 97;
type Model = BTreeMap<Bytes, Bytes>;

struct Fixture {
    servers: Vec<RunningFake>,
    store: Arc<dyn ObjectStore>,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_slow_zone(false).await
    }

    async fn with_slow_zone(slow_zone: bool) -> Self {
        let mut servers = Vec::new();
        for zone in 0..4 {
            let service = if slow_zone && zone == 2 {
                FakeGcs::with_latency(
                    LatencyProfile::new(7)
                        .with_operation(
                            Operation::BidiCreate,
                            SimulatedLatency::fixed(Duration::from_millis(5)),
                        )
                        .with_operation(
                            Operation::BidiFinalize,
                            SimulatedLatency::fixed(Duration::from_millis(3)),
                        ),
                )
            } else {
                FakeGcs::default()
            };
            servers.push(service.start().await.unwrap());
        }
        Self {
            servers,
            store: Arc::new(InMemory::new()),
        }
    }

    async fn open(&self) -> (Db, GarbageCollectorBuilder<&'static str>) {
        // Recreate transports, volume, initializer, and GC registry on every
        // open, so replay cannot succeed by retaining a previous writer's state.
        let mut factories = Vec::new();
        for (zone, server) in self.servers.iter().enumerate() {
            factories.push(
                GrpcReplicaFactory::connect(
                    zone,
                    &server.endpoint,
                    format!("projects/_/buckets/zone-{zone}"),
                    None,
                )
                .await
                .unwrap(),
            );
        }
        let regional = factories.pop().unwrap();
        let volume = SegmentedVolume::new(
            factories,
            regional,
            PREFIX,
            ClientConfig {
                max_retries: 3,
                retry_base: Duration::ZERO,
            },
        )
        .unwrap();
        let wal = ChorusWal::with_config(
            volume,
            WalEngineConfig {
                max_segment_bytes: 1024,
                repair_interval: None,
                shutdown_timeout: Duration::from_secs(5),
                ..WalEngineConfig::default()
            },
        );
        let gc_builder = || {
            GarbageCollectorBuilder::new("db", self.store.clone())
                .with_wal_gc(Arc::new(wal.clone()))
                .with_options(GarbageCollectorOptions {
                    manifest_options: None,
                    wal_options: Some(GarbageCollectorDirectoryOptions {
                        interval: None,
                        min_age: Duration::ZERO,
                        dry_run: false,
                    }),
                    wal_fence_options: None,
                    compacted_options: None,
                    compactions_options: None,
                    detach_options: None,
                    ..GarbageCollectorOptions::default()
                })
        };
        let collector = gc_builder();
        let db = Db::builder("db", self.store.clone())
            .with_settings(Settings {
                flush_interval: None,
                compactor_options: None,
                garbage_collector_options: None,
                l0_max_ssts: 64,
                l0_max_ssts_per_key: 64,
                ..Settings::default()
            })
            .with_wal_writer(Box::new(wal.clone()))
            .with_gc_builder(gc_builder())
            .build()
            .await
            .unwrap();
        (db, collector)
    }

    async fn manifest(&self) -> HashMap<String, String> {
        let objects = self.servers[3]
            .service
            .observe_prefix("projects/_/buckets/zone-3", PREFIX)
            .await;
        assert_eq!(objects.len(), 1);
        objects[0].metadata.clone()
    }

    async fn segments(&self, zone: usize) -> BTreeSet<String> {
        self.servers[zone]
            .service
            .observe_prefix(
                &format!("projects/_/buckets/zone-{zone}"),
                &format!("{PREFIX}/segments/"),
            )
            .await
            .into_iter()
            .map(|object| object.name)
            .collect()
    }
}

fn key(index: u64) -> Bytes {
    Bytes::from(format!("key-{index:03}"))
}

async fn write_batches(db: &Db, model: &mut Model, state: &mut u64, batches: usize) {
    for _ in 0..batches {
        let mut batch = WriteBatch::new();
        for _ in 0..4 {
            // Fixed seeds make the mixed overwrite/delete workload reproducible.
            *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let key = key((*state >> 32) % KEY_COUNT);
            if state.is_multiple_of(5) {
                batch.delete(&key);
                model.remove(&key);
            } else {
                let value = Bytes::from(vec![(*state >> 24) as u8; (*state >> 40) as usize % 192]);
                batch.put(&key, &value);
                model.insert(key, value);
            }
        }
        db.write(batch)
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
    }
}

async fn verify(db: &Db, model: &Model) {
    for index in 0..KEY_COUNT {
        let key = key(index);
        assert_eq!(db.get(&key).await.unwrap(), model.get(&key).cloned());
    }
    let mut scan = db.scan(..).await.unwrap();
    let mut actual = Vec::new();
    while let Some(row) = scan.next().await.unwrap() {
        actual.push((row.key, row.value));
    }
    assert_eq!(actual, model.clone().into_iter().collect::<Vec<_>>());

    let mut scan = db.scan(key(20)..key(50)).await.unwrap();
    let mut actual = Vec::new();
    while let Some(row) = scan.next().await.unwrap() {
        actual.push((row.key, row.value));
    }
    assert_eq!(
        actual,
        model
            .range(key(20)..key(50))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>()
    );
}

async fn flush_l0(db: &Db) {
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
}

async fn close(db: &Db) {
    // Leave the suffix only in Chorus, forcing WAL replay on the next open.
    db.close_with_options(CloseOptions::default().with_flush_type(None))
        .await
        .unwrap();
}

// Object counts are not comparable across replicas: speculative provisioning
// and catch-up can leave different numbers of objects in each zone. Instead,
// check the exact sealed objects whose deletion the committed floor authorizes.
fn collected_objects(manifest: &HashMap<String, String>, floor: u64) -> BTreeSet<String> {
    let entries: Vec<_> = manifest["chorus.segments"]
        .split(',')
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            let mut fields = entry.split(':');
            let id = fields.next().unwrap();
            let base = fields.next().unwrap().parse::<u64>().unwrap();
            (id, base)
        })
        .collect();
    let tail = manifest["chorus.tail_base"].parse().unwrap();
    entries
        .iter()
        .enumerate()
        .filter(|(index, _)| entries.get(index + 1).map_or(tail, |entry| entry.1) <= floor)
        .map(|(_, (id, _))| format!("{PREFIX}/segments/{id}"))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_api_matches_model_across_gc_and_fresh_client_reopens() {
    tokio::time::timeout(Duration::from_secs(60), async {
        for seed in [1, 7, 42, 0xdead_beef] {
            // Exercise asymmetric provisioning/finalization as well as fast
            // replicas; per-zone object counts need not agree at GC boundaries.
            let fixture = Fixture::with_slow_zone(seed == 7).await;
            let mut model = Model::new();
            let mut state = seed;
            let mut previous_floor = 0;
            for _ in 0..4 {
                let (db, collector) = fixture.open().await;
                let collector = collector.build();
                verify(&db, &model).await;
                write_batches(&db, &mut model, &mut state, 40).await;
                flush_l0(&db).await;
                let manifest = fixture.manifest().await;
                collector.run_gc_once().await;
                let collected = fixture.manifest().await;
                let floor: u64 = collected["chorus.trunc"].parse().unwrap();
                assert!(floor > previous_floor, "GC did not advance: {collected:?}");
                previous_floor = floor;
                assert_eq!(manifest["chorus.epoch"], collected["chorus.epoch"]);
                assert_eq!(manifest["chorus.owner"], collected["chorus.owner"]);
                let deleted = collected_objects(&manifest, floor);
                assert!(!deleted.is_empty(), "GC must reclaim physical objects");
                for zone in 0..3 {
                    let remaining = fixture.segments(zone).await;
                    assert!(
                        deleted.is_disjoint(&remaining),
                        "zone {zone}: {remaining:?}"
                    );
                }
                write_batches(&db, &mut model, &mut state, 8).await;
                verify(&db, &model).await;
                close(&db).await;
            }
            let (db, _) = fixture.open().await;
            verify(&db, &model).await;
            close(&db).await;
        }
    })
    .await
    .expect("public SlateDB lifecycle test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_api_survives_zone_loss_gc_and_writer_takeover() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let fixture = Fixture::new().await;
        let mut model = Model::new();
        let mut state = 123;
        let (old, collector) = fixture.open().await;
        let collector = collector.build();
        write_batches(&old, &mut model, &mut state, 32).await;
        flush_l0(&old).await;
        let before = fixture.segments(0).await;

        fixture.servers[2].service.set_crashed(true).await;
        write_batches(&old, &mut model, &mut state, 16).await;
        collector.run_gc_once().await;
        let deleted: BTreeSet<_> = before
            .difference(&fixture.segments(0).await)
            .cloned()
            .collect();
        assert!(!deleted.is_empty(), "GC must reclaim the flushed prefix");
        assert!(deleted.is_subset(&fixture.segments(2).await));
        verify(&old, &model).await;

        // Take ownership without closing the previous Db, while a zone is down.
        let (new, collector) = fixture.open().await;
        let collector = collector.build();
        verify(&new, &model).await;
        let stale_write = match old.put(b"stale-writer", b"must-not-commit").await {
            Ok(handle) => handle.await_durable().await,
            Err(error) => Err(error),
        };
        assert!(
            stale_write.is_err(),
            "the fenced writer acknowledged a write"
        );
        let _ = old
            .close_with_options(CloseOptions::default().with_flush_type(None))
            .await;
        write_batches(&new, &mut model, &mut state, 16).await;

        fixture.servers[2].service.set_crashed(false).await;
        collector.run_gc_once().await;
        assert!(deleted.is_disjoint(&fixture.segments(2).await));
        verify(&new, &model).await;
        close(&new).await;
        let (reopened, _) = fixture.open().await;
        verify(&reopened, &model).await;
        assert_eq!(reopened.get(b"stale-writer").await.unwrap(), None);
        close(&reopened).await;
    })
    .await
    .expect("public SlateDB fault test timed out");
}
