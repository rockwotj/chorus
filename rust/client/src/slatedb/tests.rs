use super::*;
use bytes::Bytes;
use chorus_fake_gcs::{FakeGcs, RunningFake};
use slatedb::config::{CloseOptions, FlushOptions, FlushType, Settings};
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::{Db, ValueDeletable, WriteBatch};
use std::time::Duration;

#[path = "gc_tests.rs"]
mod gc_tests;
#[path = "progress_tests.rs"]
mod progress_tests;
#[path = "reader_tests.rs"]
mod reader_tests;

async fn volume() -> (Vec<RunningFake>, SegmentedVolume) {
    let mut servers = Vec::new();
    let mut factories = Vec::new();
    for zone in 0..4 {
        let server = FakeGcs::default().start().await.unwrap();
        factories.push(
            crate::GrpcReplicaFactory::connect(
                zone,
                &server.endpoint,
                format!("projects/_/buckets/zone-{zone}"),
                None,
            )
            .await
            .unwrap(),
        );
        servers.push(server);
    }
    let manifest = factories.pop().unwrap();
    let volume = SegmentedVolume::new(
        factories,
        manifest,
        "slatedb-test",
        crate::ClientConfig {
            max_retries: 3,
            retry_base: Duration::ZERO,
        },
    )
    .unwrap();
    (servers, volume)
}

fn config() -> WalEngineConfig {
    WalEngineConfig {
        max_segment_bytes: 256,
        repair_interval: None,
        shutdown_timeout: Duration::from_secs(5),
        ..WalEngineConfig::default()
    }
}

fn row(seq: u64) -> RowEntry {
    RowEntry {
        key: Bytes::from(format!("key-{seq}")),
        value: ValueDeletable::Value(Bytes::from(format!("value-{seq}"))),
        seq,
        create_ts: Some(123),
        expire_ts: None,
    }
}

async fn direct_writer(volume: &SegmentedVolume, checkpoint: u64) -> WriterInitResult {
    let recovery = volume.recover(WalSeqNo::record(checkpoint)).await.unwrap();
    writer_and_replay(recovery, config()).unwrap()
}

async fn replay(result: &mut WriterInitResult) -> Vec<WalRows> {
    let mut rows = Vec::new();
    while let Some(batch) = result.replay_iterator.next().await.unwrap() {
        rows.push(batch);
    }
    rows
}

async fn open(volume: &SegmentedVolume, store: Arc<dyn ObjectStore>) -> Db {
    Db::builder("db", store)
        .with_settings(Settings {
            flush_interval: None,
            compactor_options: None,
            garbage_collector_options: None,
            ..Settings::default()
        })
        .with_wal_writer(Box::new(ChorusWal::with_config(volume.clone(), config())))
        .build()
        .await
        .unwrap()
}

async fn close(db: &Db) {
    tokio::time::timeout(
        Duration::from_secs(10),
        db.close_with_options(CloseOptions::default().with_flush_type(None)),
    )
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test]
async fn slatedb_reopens_atomic_batches_and_deletes_without_l0_flush() {
    let (_servers, volume) = volume().await;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(&volume, store.clone()).await;
    db.put(b"removed", b"old")
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    let mut batch = WriteBatch::new();
    batch.put(b"first", b"one");
    batch.put(b"second", b"two");
    batch.delete(b"removed");
    db.write(batch)
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    close(&db).await;

    // Inspect the actual persisted batch boundary, not just the DB's memtable.
    let mut recovered = direct_writer(&volume, 0).await;
    let batches = replay(&mut recovered).await;
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].last_consumed_wal_file_id, 1);
    assert_eq!(batches[1].last_consumed_wal_file_id, 2);
    assert_eq!(batches[1].rows.len(), 3);
    assert!(batches[1]
        .rows
        .iter()
        .all(|row| row.seq == batches[1].rows[0].seq));
    let previous_seq = batches[1].rows[0].seq;
    recovered.wal_writer.close().await.unwrap();

    let db = open(&volume, store).await;
    assert_eq!(
        db.get(b"first").await.unwrap(),
        Some(Bytes::from_static(b"one"))
    );
    assert_eq!(
        db.get(b"second").await.unwrap(),
        Some(Bytes::from_static(b"two"))
    );
    assert_eq!(db.get(b"removed").await.unwrap(), None);
    db.put(b"third", b"three")
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    close(&db).await;
    let mut recovered = direct_writer(&volume, 2).await;
    let batches = replay(&mut recovered).await;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].last_consumed_wal_file_id, 3);
    assert!(batches[0].rows[0].seq > previous_seq);
    recovered.wal_writer.close().await.unwrap();
}

#[tokio::test]
async fn slatedb_replays_only_the_suffix_after_an_l0_checkpoint() {
    let (_servers, volume) = volume().await;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(&volume, store.clone()).await;
    db.put(b"key", b"checkpointed")
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    db.put(b"key", b"wal-only")
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    close(&db).await;
    let db = open(&volume, store).await;
    assert_eq!(
        db.get(b"key").await.unwrap(),
        Some(Bytes::from_static(b"wal-only"))
    );
    close(&db).await;
    // Startup never deletes older records: a retained checkpoint may need them.
    let mut recovered = direct_writer(&volume, 0).await;
    assert_eq!(replay(&mut recovered).await.len(), 2);
    recovered.wal_writer.close().await.unwrap();
}

#[tokio::test]
async fn takeover_fences_old_writer_and_recovers_durable_prefix() {
    let (_servers, volume) = volume().await;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let old = open(&volume, store.clone()).await;
    old.put(b"key", b"old")
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    let new = open(&volume, store).await;
    assert_eq!(
        new.get(b"key").await.unwrap(),
        Some(Bytes::from_static(b"old"))
    );
    new.put(b"key", b"new")
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    let stale_write = tokio::time::timeout(Duration::from_secs(10), async {
        match old.put(b"stale", b"must-not-commit").await {
            Ok(handle) => handle.await_durable().await,
            Err(error) => Err(error),
        }
    })
    .await
    .unwrap();
    assert!(stale_write.is_err());
    let _ = old
        .close_with_options(CloseOptions::default().with_flush_type(None))
        .await;
    close(&new).await;
    let mut recovered = direct_writer(&volume, 0).await;
    let batches = replay(&mut recovered).await;
    assert_eq!(batches.len(), 2);
    assert!(batches
        .iter()
        .flat_map(|batch| &batch.rows)
        .all(|row| row.key != b"stale"[..]));
    recovered.wal_writer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipelined_appends_notify_only_after_quorum_and_flush_is_a_barrier() {
    let (servers, volume) = volume().await;
    let recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    let mut result = writer_and_replay(
        recovery,
        WalEngineConfig {
            max_segment_bytes: 1024 * 1024,
            ..config()
        },
    )
    .unwrap();
    assert!(replay(&mut result).await.is_empty());
    // Holds target existing sessions, so start the lazy writer first.
    result.wal_writer.append(&[row(1)]).await.unwrap();
    result.wal_writer.flush().await.unwrap().await.unwrap();
    for server in &servers[..3] {
        server.service.inject_flush_hold().await;
    }
    let observer = result.wal_writer.observer();
    let callback_observer = result.wal_writer.observer();
    let events = Arc::new(Mutex::new(Vec::new()));
    let received = events.clone();
    observer
        .subscribe(Arc::new(move |event| {
            // Reentrant status reads must not deadlock callbacks.
            let _ = callback_observer.status();
            received.lock().unwrap().push(event);
        }))
        .unwrap();
    for seq in 2..=4 {
        result.wal_writer.append(&[row(seq)]).await.unwrap();
    }
    let mut barrier = result.wal_writer.flush().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut barrier)
            .await
            .is_err()
    );
    assert_eq!(observer.status().unwrap().last_flushed_wal_id, 1);
    assert_eq!(observer.status().unwrap().buffered_wal_entries_count, 3);
    assert_eq!(observer.status().unwrap().estimated_bytes, 0);
    servers[0].service.release_flush_holds().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut barrier)
            .await
            .is_err()
    );
    servers[1].service.release_flush_holds().await;
    tokio::time::timeout(Duration::from_secs(5), barrier)
        .await
        .unwrap()
        .unwrap();
    let status = observer.status().unwrap();
    assert_eq!(status.last_flushed_wal_id, 4);
    assert_eq!(status.last_flushed_seq, Some(4));
    assert_eq!(status.estimated_bytes, 0);
    assert_eq!(status.buffered_wal_entries_count, 0);
    let completed_barrier = result.wal_writer.flush().await.unwrap();
    servers[2].service.release_flush_holds().await;
    result.wal_writer.close().await.unwrap();
    completed_barrier.await.unwrap();
    result.wal_writer.close().await.unwrap();
    let status = observer.status().unwrap_err();
    assert!(matches!(status.closed_reason, Some(WalError::Closed)));
    assert!(observer.subscribe(Arc::new(|_| {})).is_err());
    let events = events.lock().unwrap();
    let mut previous = 1;
    for event in &events[..events.len() - 1] {
        let WalEvent::WalFlushed(status) = event else {
            panic!("unexpected close before the final durable prefix");
        };
        assert!(status.last_flushed_wal_id > previous);
        previous = status.last_flushed_wal_id;
    }
    assert_eq!(previous, 4);
    assert!(matches!(events.last(), Some(WalEvent::WalClosed(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quorum_failure_closes_observer_and_does_not_advance_durability() {
    let (servers, volume) = volume().await;
    let mut result = direct_writer(&volume, 0).await;
    replay(&mut result).await;
    result.wal_writer.append(&[row(1)]).await.unwrap();
    result.wal_writer.flush().await.unwrap().await.unwrap();
    for server in &servers[..2] {
        server.service.set_crashed(true).await;
    }
    if result.wal_writer.append(&[row(2)]).await.is_ok() {
        if let Ok(barrier) = result.wal_writer.flush().await {
            assert!(tokio::time::timeout(Duration::from_secs(10), barrier)
                .await
                .unwrap()
                .is_err());
        }
    }
    let status = result.wal_writer.status().unwrap_err();
    assert_eq!(status.last_flushed_wal_id, 1);
    assert_eq!(status.last_flushed_seq, Some(1));
    assert!(!matches!(status.closed_reason, Some(WalError::Closed)));
    assert!(result.wal_writer.append(&[row(3)]).await.is_err());
    let _ = result.wal_writer.close().await;
    for server in &servers[..2] {
        server.service.set_crashed(false).await;
    }
    let mut recovered = direct_writer(&volume, 0).await;
    let batches = replay(&mut recovered).await;
    assert!((1..=2).contains(&batches.len()));
    assert_eq!(batches[0].rows, vec![row(1)]);
    if batches.len() == 2 {
        // The failed append is allowed to replay; the rejected later append is not.
        assert_eq!(batches[1].rows, vec![row(2)]);
    }
    recovered.wal_writer.close().await.unwrap();
}

#[tokio::test]
async fn incomplete_replay_and_oversized_batches_fail_closed() {
    let (_servers, volume) = volume().await;
    let mut result = direct_writer(&volume, 0).await;
    assert!(result.wal_writer.append(&[row(1)]).await.is_err());
    assert!(result.wal_writer.status().is_err());
    let _ = result.wal_writer.close().await;

    let recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    let mut result = writer_and_replay(
        recovery,
        WalEngineConfig {
            max_record_bytes: 32,
            ..config()
        },
    )
    .unwrap();
    replay(&mut result).await;
    assert!(result.wal_writer.append(&[row(1)]).await.is_err());
    assert_eq!(
        result.wal_writer.status().unwrap_err().last_flushed_wal_id,
        0
    );
    let _ = result.wal_writer.close().await;
}

#[tokio::test]
async fn corrupt_or_foreign_records_fail_replay_without_starting_writer() {
    let (_servers, volume) = volume().await;
    let mut recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    assert!(recovery.try_next().await.unwrap().is_none());
    let mut handle = recovery.start(config()).await.unwrap();
    handle
        .enqueue_append(WalSeqNo::ZERO, Bytes::from_static(b"not a SlateDB record"))
        .await
        .unwrap()
        .await
        .unwrap();
    handle.shutdown().await.unwrap();
    let mut result = direct_writer(&volume, 0).await;
    assert!(matches!(
        result.replay_iterator.next().await,
        Err(WalError::DataError(_))
    ));
    assert!(matches!(
        result.replay_iterator.next().await,
        Err(WalError::DataError(_))
    ));
    assert!(result.wal_writer.append(&[row(1)]).await.is_err());
    let _ = result.wal_writer.close().await;
}

#[tokio::test]
async fn close_without_writes_shuts_down_the_replayed_engine() {
    let (_servers, volume) = volume().await;
    let mut result = direct_writer(&volume, 0).await;
    replay(&mut result).await;
    result.wal_writer.flush().await.unwrap().await.unwrap();
    result.wal_writer.close().await.unwrap();
    assert_eq!(
        result.wal_writer.status().unwrap_err().last_flushed_wal_id,
        0
    );
}

#[tokio::test]
async fn a_flush_barrier_does_not_include_later_appends() {
    let (servers, volume) = volume().await;
    let mut result = direct_writer(&volume, 0).await;
    replay(&mut result).await;
    result.wal_writer.append(&[row(1)]).await.unwrap();
    result.wal_writer.flush().await.unwrap().await.unwrap();
    let earlier = result.wal_writer.flush().await.unwrap();
    for server in &servers[..3] {
        server.service.inject_flush_hold().await;
    }
    result.wal_writer.append(&[row(2)]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), earlier)
        .await
        .unwrap()
        .unwrap();
    let mut later = result.wal_writer.flush().await.unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(30), &mut later)
        .await
        .is_err());
    for server in &servers[..3] {
        server.service.release_flush_holds().await;
    }
    result.wal_writer.close().await.unwrap();
    later.await.unwrap();
    assert_eq!(
        result.wal_writer.status().unwrap_err().last_flushed_wal_id,
        2
    );
}

struct PausedInit {
    wal: ChorusWal,
    entered: Arc<tokio::sync::Notify>,
    resume: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl WriterInit for PausedInit {
    async fn fence_and_init(
        &self,
        manifest: &mut WriterManifest,
    ) -> Result<WriterInitResult, WalError> {
        self.entered.notify_one();
        self.resume.notified().await;
        self.wal.fence_and_init(manifest).await
    }
}

#[tokio::test]
async fn stale_initializer_cannot_start_after_a_newer_database_writer() {
    let (_servers, volume) = volume().await;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let init = PausedInit {
        wal: ChorusWal::with_config(volume.clone(), config()),
        entered: entered.clone(),
        resume: resume.clone(),
    };
    let older_store = store.clone();
    let older = tokio::spawn(async move {
        Db::builder("db", older_store)
            .with_wal_writer(Box::new(init))
            .build()
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let newer = open(&volume, store).await;
    newer
        .put(b"key", b"newer")
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    resume.notify_one();
    assert!(tokio::time::timeout(Duration::from_secs(5), older)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    // The stale initialization is rejected before it fences the newer Chorus
    // writer, so the surviving database can still make progress.
    newer
        .put(b"key", b"still-newer")
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    close(&newer).await;
}

#[tokio::test]
async fn drop_closes_observers_and_pending_barriers() {
    let (servers, volume) = volume().await;
    let mut result = direct_writer(&volume, 0).await;
    replay(&mut result).await;
    result.wal_writer.append(&[row(1)]).await.unwrap();
    result.wal_writer.flush().await.unwrap().await.unwrap();
    for server in &servers[..3] {
        server.service.inject_flush_hold().await;
    }
    result.wal_writer.append(&[row(2)]).await.unwrap();
    let barrier = result.wal_writer.flush().await.unwrap();
    let observer = result.wal_writer.observer();
    drop(result);
    assert!(matches!(
        observer.status().unwrap_err().closed_reason,
        Some(WalError::Closed)
    ));
    assert!(tokio::time::timeout(Duration::from_secs(1), barrier)
        .await
        .unwrap()
        .is_err());
    for server in &servers[..3] {
        server.service.release_flush_holds().await;
    }
}

#[test]
fn observer_callbacks_can_close_reentrantly_without_reordering_events() {
    let observer = Observer::new(0);
    let reentrant = observer.clone();
    let events = Arc::new(Mutex::new(Vec::new()));
    let received = events.clone();
    observer
        .subscribe(Arc::new(move |event| {
            if matches!(event, WalEvent::WalFlushed(_)) {
                reentrant.close(WalError::Closed);
            }
            received.lock().unwrap().push(event);
        }))
        .unwrap();
    observer.notify(|state| {
        Some((
            WalEvent::WalFlushed(state.status.clone()),
            state.listeners.clone(),
        ))
    });
    let events = events.lock().unwrap();
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0], WalEvent::WalFlushed(_)));
    assert!(matches!(events[1], WalEvent::WalClosed(_)));
    assert!(observer.state.lock().unwrap().listeners.is_empty());
}
