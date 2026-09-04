#![cfg(feature = "slatedb")]

// Exercise the library as a downstream application: no private adapter helpers,
// cfg(test) initialization path or WalAdmin implementation.
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chorus_client::{
    slatedb::ChorusWal, ClientConfig, GrpcReplicaFactory, ReadOnlyConfig, SegmentedVolume,
    WalEngineConfig,
};
use chorus_fake_gcs::{FakeGcs, LatencyProfile, Operation, RunningFake, SimulatedLatency};
use slatedb::admin::AdminBuilder;
use slatedb::config::{
    CheckpointOptions, CheckpointScope, CloseOptions, DbReaderOptions, FlushOptions, FlushType,
    GarbageCollectorDirectoryOptions, GarbageCollectorOptions, Settings,
};
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::{Db, DbReader, DbReaderMode, GarbageCollectorBuilder, WriteBatch};

#[path = "slatedb_integration/startup_gc.rs"]
mod startup_gc;

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

    async fn wal(&self) -> ChorusWal {
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
        ChorusWal::with_config(
            volume,
            WalEngineConfig {
                max_segment_bytes: 1024,
                repair_interval: None,
                shutdown_timeout: Duration::from_secs(5),
                ..WalEngineConfig::default()
            },
        )
        .with_reader_config(ReadOnlyConfig {
            poll_interval: Duration::from_millis(5),
            manifest_poll_interval: Duration::from_millis(5),
        })
    }

    async fn open_reader(&self, mode: DbReaderMode) -> DbReader {
        DbReader::builder("db", self.store.clone())
            .with_reader_mode(mode)
            .with_wal_reader(Arc::new(self.wal().await))
            .with_options(DbReaderOptions {
                manifest_poll_interval: Duration::from_millis(10),
                ..DbReaderOptions::default()
            })
            .build()
            .await
            .unwrap()
    }

    async fn open(&self) -> (Db, GarbageCollectorBuilder<&'static str>) {
        let wal = self.wal().await;
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

async fn verify_reader(reader: &DbReader, model: &Model) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let mut scan = reader.scan(..).await.unwrap();
            let mut actual = Model::new();
            while let Some(row) = scan.next().await.unwrap() {
                actual.insert(row.key, row.value);
            }
            if &actual == model {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("DbReader did not catch up with the reference model");
    for index in 0..KEY_COUNT {
        let key = key(index);
        assert_eq!(reader.get(&key).await.unwrap(), model.get(&key).cloned());
    }
    let mut scan = reader.scan(key(20)..key(50)).await.unwrap();
    let mut actual = Vec::new();
    while let Some(row) = scan.next().await.unwrap() {
        actual.push((row.key, row.value));
    }
    assert_eq!(
        actual,
        model
            .range(key(20)..key(50))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn public_db_readers_follow_active_writes_gc_and_writer_takeover() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let fixture = Fixture::new().await;
        let (old, _) = fixture.open().await;
        let epoch = fixture.manifest().await["chorus.epoch"].clone();
        let latest = fixture.open_reader(DbReaderMode::FollowLatest).await;
        let managed = fixture.open_reader(DbReaderMode::ManagedCheckpoint).await;
        assert_eq!(fixture.manifest().await["chorus.epoch"], epoch);
        let mut model = Model::new();
        let mut state = 0x9a37_u64;
        // First batch stays in the active tail and is never flushed to L0.
        write_batches(&old, &mut model, &mut state, 1).await;
        verify_reader(&latest, &model).await;
        verify_reader(&managed, &model).await;
        write_batches(&old, &mut model, &mut state, 16).await;
        verify_reader(&latest, &model).await;
        verify_reader(&managed, &model).await;

        fixture.servers[2].service.set_crashed(true).await;
        let (new, collector) = fixture.open().await;
        let collector = collector.build();
        write_batches(&new, &mut model, &mut state, 8).await;
        verify_reader(&latest, &model).await;
        verify_reader(&managed, &model).await;
        fixture.servers[2].service.set_crashed(false).await;
        flush_l0(&new).await;
        collector.run_gc_once().await;
        write_batches(&new, &mut model, &mut state, 8).await;
        verify_reader(&latest, &model).await;
        verify_reader(&managed, &model).await;
        let fresh = fixture.open_reader(DbReaderMode::FollowLatest).await;
        verify_reader(&fresh, &model).await;
        fresh.close().await.unwrap();
        latest.close().await.unwrap();
        managed.close().await.unwrap();
        collector.run_gc_once().await;
        assert!(
            fixture.manifest().await["chorus.trunc"]
                .parse::<u64>()
                .unwrap()
                > 0
        );
        let _ = old
            .close_with_options(CloseOptions::default().with_flush_type(None))
            .await;
        close(&new).await;
    })
    .await
    .expect("public DbReader lifecycle test timed out");
}

#[tokio::test]
async fn public_checkpoint_reader_excludes_later_writes_and_survives_gc() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let fixture = Fixture::new().await;
        let (db, collector) = fixture.open().await;
        let collector = collector.build();
        let mut model = Model::new();
        let mut state = 0xc105_u64;
        write_batches(&db, &mut model, &mut state, 12).await;
        let checkpoint = db
            .create_checkpoint(CheckpointScope::Durable, &CheckpointOptions::default())
            .await
            .unwrap();
        let pinned_model = model.clone();
        let pinned = fixture
            .open_reader(DbReaderMode::Checkpoint(checkpoint.id))
            .await;
        verify_reader(&pinned, &pinned_model).await;
        write_batches(&db, &mut model, &mut state, 12).await;
        flush_l0(&db).await;
        collector.run_gc_once().await;
        assert_ne!(model, pinned_model);
        verify_reader(&pinned, &pinned_model).await;
        // Opening a fresh process at the old checkpoint must also work after GC.
        let reopened = fixture
            .open_reader(DbReaderMode::Checkpoint(checkpoint.id))
            .await;
        verify_reader(&reopened, &pinned_model).await;
        let latest = fixture.open_reader(DbReaderMode::FollowLatest).await;
        verify_reader(&latest, &model).await;
        latest.close().await.unwrap();
        reopened.close().await.unwrap();
        pinned.close().await.unwrap();
        close(&db).await;
    })
    .await
    .expect("public checkpoint reader test timed out");
}

#[tokio::test]
async fn public_wal_only_checkpoint_survives_gc_and_fresh_reader_reopen() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let fixture = Fixture::new().await;
        let (first, _) = fixture.open().await;
        let mut model = Model::new();
        let mut state = 0xc105_u64;
        write_batches(&first, &mut model, &mut state, 12).await;
        first
            .close_with_options(CloseOptions::default().with_flush_type(Some(FlushType::Wal)))
            .await
            .unwrap();
        let admin = AdminBuilder::new("db", fixture.store.clone()).build();
        let checkpoint = admin
            .create_detached_checkpoint(&CheckpointOptions::default())
            .await
            .unwrap();
        let manifest = admin
            .read_manifest(Some(checkpoint.manifest_id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(manifest.replay_after_wal_id(), 0);
        assert_eq!(manifest.next_wal_sst_id(), 13);
        assert!(manifest.l0().is_empty());
        let pinned_model = model.clone();
        let (db, collector) = fixture.open().await;
        write_batches(&db, &mut model, &mut state, 12).await;
        flush_l0(&db).await;
        let collector = collector.build();
        collector.run_gc_once().await;
        assert_eq!(fixture.manifest().await["chorus.trunc"], "0");
        let reopened = fixture
            .open_reader(DbReaderMode::Checkpoint(checkpoint.id))
            .await;
        verify_reader(&reopened, &pinned_model).await;
        reopened.close().await.unwrap();
        admin.delete_checkpoint(checkpoint.id).await.unwrap();
        collector.run_gc_once().await;
        assert!(
            fixture.manifest().await["chorus.trunc"]
                .parse::<u64>()
                .unwrap()
                > 0
        );
        close(&db).await;
    })
    .await
    .expect("WAL-only checkpoint retention test timed out");
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
