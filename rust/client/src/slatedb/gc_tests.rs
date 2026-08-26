use super::*;
use chorus_fake_gcs::proto::{storage_server::Storage, GetObjectRequest, Timestamp};
use slatedb::admin::AdminBuilder;
use slatedb::config::{
    CheckpointOptions, CheckpointScope, GarbageCollectorDirectoryOptions, GarbageCollectorOptions,
};
use slatedb::wal::{WalFileRange, WalGc};
use slatedb::GarbageCollectorBuilder;
use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::time::SystemTime;

fn timestamp(time: SystemTime) -> Timestamp {
    let time = time.duration_since(std::time::UNIX_EPOCH).unwrap();
    Timestamp {
        seconds: time.as_secs() as i64,
        nanos: time.subsec_nanos() as i32,
    }
}

async fn first_segment(servers: &[RunningFake]) -> String {
    let metadata = manifest(servers).await;
    let id = metadata["chorus.segments"].split(':').next().unwrap();
    format!("slatedb-test/segments/{id}")
}

async fn manifest(servers: &[RunningFake]) -> HashMap<String, String> {
    let objects = servers[3]
        .service
        .observe_prefix("projects/_/buckets/zone-3", "slatedb-test")
        .await;
    assert_eq!(objects.len(), 1);
    objects[0].metadata.clone()
}

async fn floor(servers: &[RunningFake]) -> u64 {
    manifest(servers).await["chorus.trunc"].parse().unwrap()
}

async fn names(servers: &[RunningFake], zone: usize) -> HashSet<String> {
    servers[zone]
        .service
        .observe_prefix(
            &format!("projects/_/buckets/zone-{zone}"),
            "slatedb-test/segments/",
        )
        .await
        .into_iter()
        .map(|object| object.name)
        .collect()
}

fn retained(first: u64) -> Vec<WalFileRange> {
    vec![WalFileRange(Bound::Included(first), Bound::Unbounded)]
}

async fn initialize(wal: &ChorusWal, checkpoint: u64) -> WriterInitResult {
    let recovery = wal
        .volume
        .recover(WalSeqNo::record(checkpoint))
        .await
        .unwrap();
    let mut result =
        writer_and_replay_with_gc(recovery, wal.config.clone(), wal.gc.clone()).unwrap();
    replay(&mut result).await;
    result
}

async fn seeded(checkpoint: u64) -> (Vec<RunningFake>, ChorusWal, WriterInitResult) {
    seeded_at(checkpoint, Timestamp::default()).await
}

async fn seeded_at(
    checkpoint: u64,
    now: Timestamp,
) -> (Vec<RunningFake>, ChorusWal, WriterInitResult) {
    let (servers, volume) = volume().await;
    for server in &servers {
        server.service.set_clock(now).await;
    }
    let wal = ChorusWal::with_config(
        volume,
        WalEngineConfig {
            max_segment_bytes: 100,
            ..config()
        },
    );
    let mut writer = initialize(&wal, 0).await;
    for seq in 1..=16 {
        writer.wal_writer.append(&[row(seq)]).await.unwrap();
        writer.wal_writer.flush().await.unwrap().await.unwrap();
    }
    writer.wal_writer.close().await.unwrap();
    let writer = initialize(&wal, checkpoint).await;
    // Also waits behind startup repair/sweeps before taking physical snapshots.
    wal.collect(retained(0), Duration::ZERO, false)
        .await
        .unwrap();
    (servers, wal, writer)
}

#[tokio::test]
async fn gc_retention_can_precede_the_writer_replay_checkpoint() {
    let (servers, wal, mut writer) = seeded(12).await;
    let before = names(&servers, 0).await;
    let old_manifest = manifest(&servers).await;
    wal.collect(retained(6), Duration::ZERO, true)
        .await
        .unwrap();
    assert_eq!(floor(&servers).await, 0);
    assert_eq!(names(&servers, 0).await, before);
    wal.collect(retained(6), Duration::ZERO, false)
        .await
        .unwrap();
    let collected = floor(&servers).await;
    assert!(collected > 0 && collected <= 5, "floor={collected}");
    assert!(names(&servers, 0).await.len() < before.len());
    let new_manifest = manifest(&servers).await;
    assert_eq!(old_manifest["chorus.epoch"], new_manifest["chorus.epoch"]);
    assert_eq!(old_manifest["chorus.owner"], new_manifest["chorus.owner"]);
    writer.wal_writer.append(&[row(17)]).await.unwrap();
    writer.wal_writer.flush().await.unwrap().await.unwrap();
    writer.wal_writer.close().await.unwrap();
    // The old retained checkpoint still replays, despite opening the previous
    // writer at a newer checkpoint. No physical segment was partially deleted.
    let mut recovered = direct_writer(&wal.volume, 5).await;
    let batches = replay(&mut recovered).await;
    assert_eq!(batches.len(), 12);
    assert_eq!(batches[0].rows, vec![row(6)]);
    assert_eq!(batches.last().unwrap().rows, vec![row(17)]);
    recovered.wal_writer.close().await.unwrap();
}

#[tokio::test]
async fn gc_uses_object_age_on_first_eligible_pass_after_restart_and_dry_run() {
    let (servers, wal, mut writer) = seeded(0).await;
    let age = Duration::from_secs(3600);
    let before = names(&servers, 0).await;
    wal.collect(retained(0), age, false).await.unwrap();
    writer.wal_writer.close().await.unwrap();
    // A new initializer discards all process-local GC state. Old storage
    // timestamps still authorize deletion on its first eligible pass.
    let fresh = ChorusWal::with_config(wal.volume.clone(), wal.config.clone());
    let mut writer = initialize(&fresh, 0).await;
    fresh
        .collect(retained(0), Duration::ZERO, false)
        .await
        .unwrap();
    let snapshot = manifest(&servers).await;
    fresh.collect(retained(13), age, true).await.unwrap();
    assert_eq!(manifest(&servers).await, snapshot);
    assert!(before.is_subset(&names(&servers, 0).await));
    fresh.collect(retained(13), age, false).await.unwrap();
    assert!(floor(&servers).await > 0);
    assert!(!before.is_subset(&names(&servers, 0).await));
    writer.wal_writer.close().await.unwrap();
}

#[tokio::test]
async fn gc_defers_young_objects_even_when_they_are_unreferenced() {
    let (servers, wal, mut writer) = seeded_at(0, timestamp(SystemTime::now())).await;
    let age = Duration::from_secs(3600);
    let before = names(&servers, 0).await;
    wal.collect(retained(13), age, true).await.unwrap();
    wal.collect(retained(13), age, false).await.unwrap();
    assert_eq!(floor(&servers).await, 0);
    assert_eq!(names(&servers, 0).await, before);
    writer.wal_writer.close().await.unwrap();
}

#[tokio::test]
async fn gc_requires_old_valid_timestamps_on_every_existing_copy() {
    let (servers, wal, mut writer) = seeded(0).await;
    let age = Duration::from_secs(3600);
    let object = first_segment(&servers).await;
    let bucket = "projects/_/buckets/zone-2";
    let before = names(&servers, 2).await;
    for time in [
        None,
        Some(Timestamp {
            seconds: 0,
            nanos: -1,
        }),
        Some(Timestamp {
            seconds: 0,
            nanos: 1_000_000_000,
        }),
        Some(Timestamp {
            seconds: i64::MAX,
            nanos: 0,
        }),
        Some(timestamp(SystemTime::now())),
        Some(timestamp(SystemTime::now() + age)),
    ] {
        servers[2]
            .service
            .set_object_update_time(bucket, &object, time)
            .await;
        wal.collect(retained(13), age, true).await.unwrap();
        wal.collect(retained(13), age, false).await.unwrap();
        assert_eq!(floor(&servers).await, 0, "timestamp {time:?}");
        assert_eq!(names(&servers, 2).await, before);
    }
    servers[2]
        .service
        .set_object_update_time(bucket, &object, Some(Timestamp::default()))
        .await;
    wal.collect(retained(13), age, false).await.unwrap();
    assert!(floor(&servers).await > 0);
    writer.wal_writer.close().await.unwrap();
}

#[tokio::test]
async fn gc_defers_age_authorization_until_an_unavailable_zone_returns() {
    let (servers, wal, mut writer) = seeded(0).await;
    let age = Duration::from_secs(3600);
    let before = names(&servers, 0).await;
    servers[2].service.set_crashed(true).await;
    wal.collect(retained(13), age, false).await.unwrap();
    assert_eq!(floor(&servers).await, 0);
    assert_eq!(names(&servers, 0).await, before);
    servers[2].service.set_crashed(false).await;
    wal.collect(retained(13), age, false).await.unwrap();
    assert!(floor(&servers).await > 0);
    writer.wal_writer.close().await.unwrap();
}

#[tokio::test]
async fn gc_rechecks_the_storage_age_of_a_repaired_replica() {
    let (servers, wal, mut writer) = seeded(0).await;
    let object = first_segment(&servers).await;
    let bucket = "projects/_/buckets/zone-2";
    let request = || {
        tonic::Request::new(GetObjectRequest {
            bucket: bucket.into(),
            object: object.clone(),
            ..Default::default()
        })
    };
    let original = servers[2]
        .service
        .get_object(request())
        .await
        .unwrap()
        .into_inner();
    writer.wal_writer.close().await.unwrap();
    let repaired_at = timestamp(SystemTime::now());
    servers[2].service.set_clock(repaired_at).await;
    assert!(
        servers[2]
            .service
            .diverge_byte_for(bucket, &object, 0)
            .await
    );
    let mut writer = initialize(&wal, 0).await;
    // Queue behind startup repair without granting deletion authority.
    wal.collect(retained(0), Duration::ZERO, false)
        .await
        .unwrap();
    let repaired = servers[2]
        .service
        .get_object(request())
        .await
        .unwrap()
        .into_inner();
    assert_ne!(repaired.generation, original.generation);
    assert_eq!(repaired.update_time, Some(repaired_at));
    wal.collect(retained(13), Duration::from_secs(3600), false)
        .await
        .unwrap();
    assert_eq!(
        floor(&servers).await,
        0,
        "the youngest copy must protect the prefix"
    );
    assert!(names(&servers, 0).await.contains(&object));
    writer.wal_writer.close().await.unwrap();
}

#[tokio::test]
async fn gc_leaves_active_tail_and_retries_deletion_on_a_returning_zone() {
    let (servers, wal, mut writer) = seeded(0).await;
    let before = names(&servers, 2).await;
    servers[2].service.set_crashed(true).await;
    wal.collect(retained(13), Duration::ZERO, false)
        .await
        .unwrap();
    assert!(floor(&servers).await > 0);
    let deleted: HashSet<_> = before
        .difference(&names(&servers, 0).await)
        .cloned()
        .collect();
    assert!(!deleted.is_empty());
    assert!(deleted.is_subset(&names(&servers, 2).await));
    servers[2].service.set_crashed(false).await;
    wal.collect(retained(13), Duration::ZERO, false)
        .await
        .unwrap();
    assert!(deleted.is_disjoint(&names(&servers, 2).await));
    // An empty retention set still grants no permission to delete active or
    // speculative objects, only the sealed prefix already present at this pass.
    let active = manifest(&servers).await["chorus.tail_id"].clone();
    wal.collect(vec![], Duration::ZERO, false).await.unwrap();
    assert!(names(&servers, 0)
        .await
        .contains(&format!("slatedb-test/segments/{active}")));
    writer.wal_writer.append(&[row(17)]).await.unwrap();
    writer.wal_writer.flush().await.unwrap().await.unwrap();
    writer.wal_writer.close().await.unwrap();
    let mut recovered = direct_writer(&wal.volume, 16).await;
    assert_eq!(replay(&mut recovered).await[0].rows, vec![row(17)]);
    recovered.wal_writer.close().await.unwrap();
}

#[tokio::test]
async fn gc_is_available_before_first_append_and_rebinds_after_close() {
    let (servers, volume) = volume().await;
    let wal = ChorusWal::new(volume);
    assert!(matches!(
        wal.collect(retained(0), Duration::ZERO, false).await,
        Err(WalError::Closed)
    ));
    let mut writer = initialize(&wal, 0).await;
    wal.collect(retained(0), Duration::ZERO, false)
        .await
        .unwrap();
    let epoch = manifest(&servers).await["chorus.epoch"].clone();
    writer.wal_writer.close().await.unwrap();
    assert!(matches!(
        wal.collect(retained(0), Duration::ZERO, false).await,
        Err(WalError::Closed)
    ));
    assert_eq!(epoch, manifest(&servers).await["chorus.epoch"]);
    let mut next = initialize(&wal, 0).await;
    wal.collect(retained(0), Duration::ZERO, false)
        .await
        .unwrap();
    next.wal_writer.close().await.unwrap();
}

#[tokio::test]
async fn closing_a_fenced_writer_does_not_disconnect_its_successors_gc() {
    let (servers, wal, mut older) = seeded(0).await;
    let mut newer = initialize(&wal, 12).await;
    let _ = older.wal_writer.close().await;
    let epoch = manifest(&servers).await["chorus.epoch"].clone();
    wal.collect(retained(13), Duration::ZERO, false)
        .await
        .unwrap();
    assert!(floor(&servers).await > 0);
    assert_eq!(epoch, manifest(&servers).await["chorus.epoch"]);
    newer.wal_writer.append(&[row(17)]).await.unwrap();
    newer.wal_writer.flush().await.unwrap().await.unwrap();
    newer.wal_writer.close().await.unwrap();
}

fn gc_options(interval: Option<Duration>) -> GarbageCollectorOptions {
    GarbageCollectorOptions {
        manifest_options: None,
        wal_options: Some(GarbageCollectorDirectoryOptions {
            interval,
            min_age: Duration::ZERO,
            dry_run: false,
        }),
        wal_fence_options: None,
        compacted_options: None,
        compactions_options: None,
        detach_options: None,
        ..GarbageCollectorOptions::default()
    }
}

async fn open_with_gc(
    wal: &ChorusWal,
    store: Arc<dyn ObjectStore>,
    interval: Option<Duration>,
) -> Db {
    let gc = GarbageCollectorBuilder::new("db", store.clone())
        .with_wal_gc(Arc::new(wal.clone()))
        .with_options(gc_options(interval));
    Db::builder("db", store)
        .with_settings(Settings {
            flush_interval: None,
            compactor_options: None,
            garbage_collector_options: None,
            ..Settings::default()
        })
        .with_wal_writer(Box::new(wal.clone()))
        .with_gc_builder(gc)
        .build()
        .await
        .unwrap()
}

async fn put_range(db: &Db, first: u64, end: u64) {
    for index in first..end {
        db.put(format!("key-{index}"), [42; 100])
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn slatedb_scheduled_gc_reclaims_storage_while_the_database_keeps_writing() {
    let (servers, volume) = volume().await;
    let wal = ChorusWal::with_config(volume.clone(), config());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_with_gc(&wal, store.clone(), Some(Duration::from_millis(10))).await;
    put_range(&db, 0, 16).await;
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while floor(&servers).await == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    put_range(&db, 16, 20).await;
    assert_eq!(
        db.get(b"key-0").await.unwrap(),
        Some(Bytes::from(vec![42; 100]))
    );
    close(&db).await;
    let reopened = open(&volume, store).await;
    for index in 0..20 {
        assert_eq!(
            reopened.get(format!("key-{index}")).await.unwrap(),
            Some(Bytes::from(vec![42; 100]))
        );
    }
    close(&reopened).await;
}

#[tokio::test]
async fn slatedb_gc_respects_an_actual_retained_checkpoint_then_reclaims_it() {
    let (servers, volume) = volume().await;
    let wal = ChorusWal::with_config(volume.clone(), config());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_with_gc(&wal, store.clone(), None).await;
    put_range(&db, 0, 8).await;
    let checkpoint = db
        .create_checkpoint(CheckpointScope::Durable, &CheckpointOptions::default())
        .await
        .unwrap();
    let admin = AdminBuilder::new("db", store.clone()).build();
    let checkpoint_manifest = admin
        .read_manifest(Some(checkpoint.manifest_id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint_manifest.replay_after_wal_id(), 0);
    assert!(
        checkpoint_manifest.next_wal_sst_id() > 1,
        "checkpoint must retain WAL records"
    );
    put_range(&db, 8, 16).await;
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    // Reopen at the newer L0 checkpoint while retaining the old WAL-dependent one.
    close(&db).await;
    let db = open_with_gc(&wal, store.clone(), None).await;
    let collector = GarbageCollectorBuilder::new("db", store)
        .with_wal_gc(Arc::new(wal.clone()))
        .with_options(gc_options(None))
        .build();
    collector.run_gc_once().await;
    assert_eq!(floor(&servers).await, 0);
    admin.delete_checkpoint(checkpoint.id).await.unwrap();
    collector.run_gc_once().await;
    assert!(floor(&servers).await > 0);
    put_range(&db, 16, 17).await;
    close(&db).await;
}

#[tokio::test]
async fn gc_keeps_live_history_bounded_across_more_than_one_directory_of_rotations() {
    let (servers, volume) = volume().await;
    let wal = ChorusWal::with_config(
        volume.clone(),
        WalEngineConfig {
            max_segment_bytes: 100,
            ..config()
        },
    );
    let mut writer = initialize(&wal, 0).await;
    let mut all_objects = HashSet::new();
    for seq in 1..=600 {
        writer.wal_writer.append(&[row(seq)]).await.unwrap();
        writer.wal_writer.flush().await.unwrap().await.unwrap();
        if seq % 8 == 0 {
            let current = names(&servers, 0).await;
            all_objects.extend(current);
            wal.collect(retained(seq.saturating_sub(16)), Duration::ZERO, false)
                .await
                .unwrap();
            assert!(names(&servers, 0).await.len() < 30);
        }
    }
    assert!(
        all_objects.len() > 150,
        "exercise more than one manifest directory worth of segments"
    );
    assert!(floor(&servers).await > 550);
    writer.wal_writer.close().await.unwrap();
    let mut recovered = direct_writer(&volume, 584).await;
    let batches = replay(&mut recovered).await;
    assert_eq!(batches.len(), 16);
    assert_eq!(batches[0].rows, vec![row(585)]);
    assert_eq!(batches[15].rows, vec![row(600)]);
    recovered.wal_writer.close().await.unwrap();
}
