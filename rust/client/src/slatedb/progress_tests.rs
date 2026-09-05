use super::*;
use crate::record::RecordFrame;

fn complete(updates: &watch::Sender<progress::Progress>, wal_id: u64, seq: u64, rows: u128) {
    progress::completion(
        updates.clone(),
        progress::Position {
            wal_id,
            seq,
            total_rows: rows,
        },
    )(Ok(crate::AppendReceipt {
        seqno: WalSeqNo::record(wal_id - 1),
    }));
}

#[tokio::test]
async fn coalesced_progress_keeps_the_durable_prefix_before_failure() {
    let observer = Observer::new(10);
    observer.admitted(9);
    let earlier = observer.barrier(11);
    let latest = observer.barrier(13);
    let failed = observer.barrier(14);
    let events = Arc::new(Mutex::new(Vec::new()));
    let recorded = events.clone();
    observer
        .subscribe(Arc::new(move |event| recorded.lock().unwrap().push(event)))
        .unwrap();
    let (updates, receiver) = watch::channel(progress::Progress::default());
    // No consumer runs until all these updates, including failure, coalesce.
    // WAL IDs and application sequence numbers deliberately are not equal.
    complete(&updates, 11, 100, 2);
    complete(&updates, 12, 500, 5);
    complete(&updates, 13, 900, 7);
    progress::completion(
        updates.clone(),
        progress::Position {
            wal_id: 14,
            seq: 1200,
            total_rows: 9,
        },
    )(Err(Error::NoQuorum));
    // Later teardown must not overwrite the useful first error or prefix.
    updates.send_modify(|progress| progress.fail(WalError::Closed));
    progress::forward(receiver, observer.clone()).await;
    earlier.await.unwrap();
    latest.await.unwrap();
    assert!(failed.await.is_err());
    let status = observer.status().unwrap_err();
    assert_eq!(status.last_flushed_wal_id, 13);
    assert_eq!(status.last_flushed_seq, Some(900));
    assert_eq!(status.buffered_wal_entries_count, 2);
    assert!(!matches!(status.closed_reason, Some(WalError::Closed)));
    let events = events.lock().unwrap();
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], WalEvent::WalFlushed(s) if s.last_flushed_wal_id == 13));
    assert!(matches!(&events[1], WalEvent::WalClosed(s) if s.last_flushed_wal_id == 13));
}

#[test]
fn progress_can_arrive_before_admission_accounting_without_regressing() {
    let observer = Observer::new(0);
    observer.committed(&progress::Position {
        wal_id: 2,
        seq: 500,
        total_rows: 7,
    });
    observer.admitted(3);
    assert_eq!(observer.status().unwrap().buffered_wal_entries_count, 0);
    observer.admitted(7);
    observer.admitted(9);
    observer.committed(&progress::Position {
        wal_id: 1,
        seq: 100,
        total_rows: 3,
    });
    let status = observer.status().unwrap();
    assert_eq!(status.last_flushed_wal_id, 2);
    assert_eq!(status.last_flushed_seq, Some(500));
    assert_eq!(status.buffered_wal_entries_count, 2);
    assert_eq!(status.estimated_bytes, 0);
}

#[tokio::test]
async fn notification_shutdown_drains_the_last_unseen_position() {
    let observer = Observer::new(0);
    observer.admitted(5);
    let barrier = observer.barrier(2);
    let (updates, receiver) = watch::channel(progress::Progress::default());
    complete(&updates, 1, 100, 2);
    complete(&updates, 2, 900, 5);
    drop(updates);
    progress::forward(receiver, observer.clone()).await;
    barrier.await.unwrap();
    assert_eq!(observer.status().unwrap().last_flushed_seq, Some(900));
    assert_eq!(observer.status().unwrap().buffered_wal_entries_count, 0);
}

// Start the real adapter admission path without its notification consumer, so
// tests can stall observation deterministically without blocking runtime threads.
async fn writer_without_consumer(
    volume: &SegmentedVolume,
    config: WalEngineConfig,
) -> (Writer, watch::Receiver<progress::Progress>) {
    let mut recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    assert!(recovery.try_next().await.unwrap().is_none());
    let handle = recovery.start(config.clone()).await.unwrap();
    let (updates, receiver) = watch::channel(progress::Progress::default());
    (
        Writer {
            ready: None,
            handle: Some(handle),
            progress: Some(updates),
            notifications: None,
            runtime: Some(tokio::runtime::Handle::current()),
            observer: Observer::new(0),
            next_index: 0,
            last_admitted_seq: None,
            total_rows: 0,
            config,
        },
        receiver,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_progress_consumer_does_not_limit_small_write_admission() {
    let (servers, volume) = volume().await;
    let cfg = WalEngineConfig {
        max_segment_bytes: 1024 * 1024,
        ..config()
    };
    let (mut writer, mut progress) = writer_without_consumer(&volume, cfg).await;
    writer.append(&[row(100)]).await.unwrap();
    progress.wait_for(|p| p.committed.is_some()).await.unwrap();
    for server in &servers[..3] {
        server.service.inject_flush_hold().await;
    }
    // The old 64-ticket cap would block this loop before it finished, although
    // these batches use only a small fraction of the engine's byte budgets.
    tokio::time::timeout(Duration::from_secs(5), async {
        for index in 2..=257 {
            let seq = index * 100;
            let rows = vec![row(seq); if index % 2 == 0 { 2 } else { 1 }];
            writer.append(&rows).await.unwrap();
        }
    })
    .await
    .expect("admission was limited by unconsumed completion notifications");
    assert_eq!(progress.borrow().committed.unwrap().wal_id, 1);
    assert_eq!(writer.status().unwrap().last_flushed_wal_id, 0);
    assert_eq!(writer.status().unwrap().estimated_bytes, 0);
    let barrier = writer.flush().await.unwrap();
    for server in &servers[..3] {
        server.service.release_flush_holds().await;
    }
    tokio::time::timeout(
        Duration::from_secs(5),
        progress.wait_for(|p| p.committed.is_some_and(|position| position.wal_id == 257)),
    )
    .await
    .unwrap()
    .unwrap();
    // Durability continued while the adapter consumed zero notifications.
    let final_position = progress.borrow().committed.unwrap();
    assert_eq!(final_position.seq, 25700);
    assert_eq!(final_position.total_rows, 385);
    writer.notifications = Some(tokio::spawn(progress::forward(
        progress,
        writer.observer.clone(),
    )));
    tokio::time::timeout(Duration::from_secs(5), barrier)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(writer.status().unwrap().buffered_wal_entries_count, 0);
    writer.close().await.unwrap();
    assert_eq!(writer.status().unwrap_err().last_flushed_seq, Some(25700));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_bytes_block_admission_and_cancelled_wait_does_not_consume_an_id() {
    let (servers, volume) = volume().await;
    let rows = [row(100)];
    let payload_bytes = codec::encode(&rows, 1024).unwrap().len();
    let encoded_bytes = payload_bytes + RecordFrame::HEADER_LEN;
    let cfg = WalEngineConfig {
        max_record_bytes: payload_bytes,
        queue_capacity_bytes: encoded_bytes,
        max_inflight_bytes: encoded_bytes,
        pipeline_window_bytes: encoded_bytes,
        max_segment_bytes: 1024 * 1024,
        ..config()
    };
    let (mut writer, mut progress) = writer_without_consumer(&volume, cfg).await;
    writer.append(&rows).await.unwrap();
    progress.wait_for(|p| p.committed.is_some()).await.unwrap();
    for server in &servers[..3] {
        server.service.inject_flush_hold().await;
    }
    writer.append(&[row(200)]).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), writer.append(&[row(300)]))
            .await
            .is_err()
    );
    assert_eq!(writer.next_index, 2);
    assert_eq!(writer.total_rows, 2);
    assert!(progress.borrow().failure.is_none());
    for server in &servers[..3] {
        server.service.release_flush_holds().await;
    }
    // Engine completion, not notification consumption, releases byte capacity.
    tokio::time::timeout(Duration::from_secs(5), writer.append(&[row(300)]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(writer.next_index, 3);
    writer.notifications = Some(tokio::spawn(progress::forward(
        progress,
        writer.observer.clone(),
    )));
    writer.close().await.unwrap();
    let status = writer.status().unwrap_err();
    assert_eq!(status.last_flushed_wal_id, 3);
    assert_eq!(status.last_flushed_seq, Some(300));
    assert_eq!(status.buffered_wal_entries_count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_abort_publishes_failure_without_an_individual_ticket() {
    let (servers, volume) = volume().await;
    let (mut writer, mut progress) = writer_without_consumer(&volume, config()).await;
    writer.append(&[row(100)]).await.unwrap();
    progress.wait_for(|p| p.committed.is_some()).await.unwrap();
    for server in &servers[..3] {
        server.service.inject_flush_hold().await;
    }
    writer.append(&[row(500)]).await.unwrap();
    writer.handle.take().unwrap().abort().await;
    let snapshot = progress.borrow().clone();
    assert_eq!(snapshot.committed.unwrap().wal_id, 1);
    assert!(matches!(snapshot.failure, Some(WalError::Closed)));
    progress::forward(progress, writer.observer.clone()).await;
    assert_eq!(writer.status().unwrap_err().last_flushed_seq, Some(100));
    assert!(writer.observer.barrier(2).await.is_err());
    for server in &servers[..3] {
        server.service.release_flush_holds().await;
    }
    writer.close().await.unwrap();
}

#[tokio::test]
async fn admission_failure_publishes_unseen_durable_progress_before_closing() {
    let (_servers, volume) = volume().await;
    let (mut writer, mut progress) = writer_without_consumer(&volume, config()).await;
    writer.append(&[row(100)]).await.unwrap();
    let barrier = writer.flush().await.unwrap();
    progress.wait_for(|p| p.committed.is_some()).await.unwrap();
    assert_eq!(writer.status().unwrap().last_flushed_wal_id, 0);
    // A sequence regression fails admission before another engine command.
    // The failure path must retain the success the consumer has not seen yet.
    assert!(writer.append(&[row(100)]).await.is_err());
    barrier.await.unwrap();
    let status = writer.status().unwrap_err();
    assert_eq!(status.last_flushed_wal_id, 1);
    assert_eq!(status.last_flushed_seq, Some(100));
    assert_eq!(status.buffered_wal_entries_count, 0);
    assert!(writer.close().await.is_err());
}

#[tokio::test]
async fn disconnected_notification_consumer_rejects_further_admission() {
    let (_servers, volume) = volume().await;
    let (mut writer, receiver) = writer_without_consumer(&volume, config()).await;
    drop(receiver);
    assert!(writer.append(&[row(100)]).await.is_err());
    assert_eq!(writer.next_index, 0);
    assert_eq!(writer.total_rows, 0);
    assert!(writer.status().is_err());
    assert!(writer.close().await.is_err());
}
