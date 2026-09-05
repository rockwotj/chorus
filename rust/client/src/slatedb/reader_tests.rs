use super::*;
use slatedb::wal::{WalFileRange, WalGc, WalReader};
use std::ops::Bound;

fn reader(volume: &SegmentedVolume) -> ChorusWal {
    ChorusWal::new(volume.clone()).with_reader_config(ReadOnlyConfig {
        poll_interval: Duration::from_millis(5),
        manifest_poll_interval: Duration::from_millis(5),
    })
}

async fn next(iterator: &mut dyn WalIterator) -> Result<Option<WalRows>, WalError> {
    tokio::time::timeout(Duration::from_secs(5), iterator.next())
        .await
        .expect("WAL reader did not make progress")
}

#[tokio::test]
async fn reader_does_not_initialize_storage_and_handles_an_empty_writer() {
    let (servers, volume) = volume().await;
    let wal = reader(&volume);
    assert!(matches!(
        wal.last_wal_file_id(0).await,
        Err(WalError::Unavailable(_))
    ));
    assert!(wal.iterator((1..).into()).await.is_err());
    assert!(servers[3]
        .service
        .observe_prefix("projects/_/buckets/zone-3", "slatedb-test")
        .await
        .is_empty());

    let mut writer = direct_writer(&volume, 0).await;
    replay(&mut writer).await;
    assert_eq!(wal.last_wal_file_id(0).await.unwrap(), 0);
    // The caller's replay watermark is a lower bound, not a request to read it.
    assert_eq!(wal.last_wal_file_id(9).await.unwrap(), 9);
    let mut live = wal.iterator((1..).into()).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(25), live.next())
        .await
        .is_err());
    writer.wal_writer.append(&[row(1)]).await.unwrap();
    writer.wal_writer.flush().await.unwrap().await.unwrap();
    assert_eq!(
        next(live.as_mut())
            .await
            .unwrap()
            .unwrap()
            .last_consumed_wal_file_id,
        1
    );
    writer.wal_writer.close().await.unwrap();
}

#[tokio::test]
async fn bounded_reader_translates_ranges_and_terminates_without_an_extra_write() {
    let (servers, volume) = volume().await;
    let mut writer = direct_writer(&volume, 0).await;
    replay(&mut writer).await;
    for seq in 1..=5 {
        writer.wal_writer.append(&[row(seq)]).await.unwrap();
        writer.wal_writer.flush().await.unwrap().await.unwrap();
    }
    writer.wal_writer.close().await.unwrap();
    let wal = reader(&volume);
    assert_eq!(wal.last_wal_file_id(0).await.unwrap(), 5);
    for (range, expected) in [
        (WalFileRange(Bound::Included(0), Bound::Included(0)), vec![]),
        ((0..1).into(), vec![]),
        ((1..1).into(), vec![]),
        ((1..6).into(), vec![1, 2, 3, 4, 5]),
        (
            WalFileRange(Bound::Included(2), Bound::Included(4)),
            vec![2, 3, 4],
        ),
        (
            WalFileRange(Bound::Excluded(2), Bound::Included(4)),
            vec![3, 4],
        ),
        (
            WalFileRange(Bound::Excluded(4), Bound::Excluded(6)),
            vec![5],
        ),
        (WalFileRange(Bound::Included(5), Bound::Included(2)), vec![]),
        (
            WalFileRange(Bound::Included(u64::MAX), Bound::Excluded(u64::MAX)),
            vec![],
        ),
    ] {
        let mut iterator = wal.iterator(range).await.unwrap();
        let mut ids = Vec::new();
        while let Some(batch) = next(iterator.as_mut()).await.unwrap() {
            assert_eq!(batch.rows, vec![row(batch.last_consumed_wal_file_id)]);
            ids.push(batch.last_consumed_wal_file_id);
        }
        assert_eq!(ids, expected);
        assert!(next(iterator.as_mut()).await.unwrap().is_none());
    }
    for range in [
        WalFileRange(Bound::Unbounded, Bound::Unbounded),
        WalFileRange(Bound::Excluded(u64::MAX), Bound::Unbounded),
    ] {
        assert!(matches!(
            wal.iterator(range).await,
            Err(WalError::InternalError(_))
        ));
    }
    // A finite range must not hang waiting for nonexistent/future records.
    assert!(matches!(
        wal.iterator((1..7).into()).await,
        Err(WalError::Unavailable(_))
    ));
    assert!(matches!(
        wal.iterator(WalFileRange(Bound::Included(1), Bound::Included(u64::MAX)))
            .await,
        Err(WalError::Unavailable(_))
    ));

    // Sealed history has manifest-authenticated bytes and can be served by
    // one matching replica even when the unrelated active tail has no quorum.
    // Recovery publishes the closed writer's last segment into the directory.
    let mut successor = direct_writer(&volume, 5).await;
    assert!(replay(&mut successor).await.is_empty());
    successor.wal_writer.close().await.unwrap();
    servers[1].service.set_crashed(true).await;
    servers[2].service.set_crashed(true).await;
    assert!(matches!(
        wal.last_wal_file_id(0).await,
        Err(WalError::Unavailable(_))
    ));
    let mut sealed = wal.iterator((1..6).into()).await.unwrap();
    for id in 1..=5 {
        assert_eq!(
            next(sealed.as_mut())
                .await
                .unwrap()
                .unwrap()
                .last_consumed_wal_file_id,
            id
        );
    }
    assert!(next(sealed.as_mut()).await.unwrap().is_none());
}

#[tokio::test]
async fn trailing_reader_waits_for_quorum_and_preserves_atomic_batches_after_cancellation() {
    let (servers, volume) = volume().await;
    let recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    let mut writer = writer_and_replay(
        recovery,
        WalEngineConfig {
            max_segment_bytes: 16 * 1024 * 1024,
            ..config()
        },
    )
    .unwrap();
    replay(&mut writer).await;
    writer.wal_writer.append(&[row(1)]).await.unwrap();
    writer.wal_writer.flush().await.unwrap().await.unwrap();
    // Wait for all three lazy streams to open before holding their flushes.
    for (zone, server) in servers[..3].iter().enumerate() {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if server
                    .service
                    .observe_prefix(
                        &format!("projects/_/buckets/zone-{zone}"),
                        "slatedb-test/segments/",
                    )
                    .await
                    .iter()
                    .any(|object| !object.bytes.is_empty())
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    let wal = reader(&volume);
    let mut live = wal
        .iterator(WalFileRange(Bound::Excluded(1), Bound::Unbounded))
        .await
        .unwrap();
    for server in &servers[1..3] {
        server.service.inject_flush_hold().await;
    }
    let mut deleted = row(2);
    deleted.key = Bytes::from_static(b"deleted");
    deleted.value = ValueDeletable::Tombstone;
    let batch = vec![row(2), deleted];
    writer.wal_writer.append(&batch).await.unwrap();
    let completion = writer.wal_writer.flush().await.unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(50), live.next())
        .await
        .is_err());
    assert_eq!(wal.last_wal_file_id(1).await.unwrap(), 1);
    servers[1].service.release_flush_holds().await;
    completion.await.unwrap();
    let observed = next(live.as_mut()).await.unwrap().unwrap();
    assert_eq!(observed.last_consumed_wal_file_id, 2);
    assert_eq!(observed.rows, batch);
    assert!(tokio::time::timeout(Duration::from_millis(25), live.next())
        .await
        .is_err());
    // A fast lagging replica must not make a bounded read reject a batch
    // already durable on the other two replicas. Wait for the slower member
    // of that quorum rather than accepting the first shorter common prefix.
    servers[1]
        .service
        .inject_delay(
            chorus_fake_gcs::Operation::BidiRead,
            Duration::from_millis(30),
        )
        .await;
    assert_eq!(wal.last_wal_file_id(0).await.unwrap(), 2);
    servers[1]
        .service
        .inject_delay(
            chorus_fake_gcs::Operation::BidiRead,
            Duration::from_millis(30),
        )
        .await;
    let mut bounded = wal.iterator((1..3).into()).await.unwrap();
    assert_eq!(
        next(bounded.as_mut()).await.unwrap().unwrap().rows,
        vec![row(1)]
    );
    assert_eq!(next(bounded.as_mut()).await.unwrap().unwrap().rows, batch);
    assert!(next(bounded.as_mut()).await.unwrap().is_none());
    servers[2].service.release_flush_holds().await;
    writer.wal_writer.append(&[row(3)]).await.unwrap();
    writer.wal_writer.flush().await.unwrap().await.unwrap();
    assert_eq!(
        next(live.as_mut()).await.unwrap().unwrap().rows,
        vec![row(3)]
    );
    writer.wal_writer.close().await.unwrap();
}

#[tokio::test]
async fn trailing_reader_survives_rotation_and_writer_takeover() {
    let (_servers, volume) = volume().await;
    let mut writer = direct_writer(&volume, 0).await;
    replay(&mut writer).await;
    let wal = reader(&volume);
    let mut live = wal.iterator((1..).into()).await.unwrap();
    for seq in 1..=12 {
        writer.wal_writer.append(&[row(seq)]).await.unwrap();
        writer.wal_writer.flush().await.unwrap().await.unwrap();
        assert_eq!(
            next(live.as_mut()).await.unwrap().unwrap().rows,
            vec![row(seq)]
        );
    }
    let mut replacement = direct_writer(&volume, 0).await;
    assert_eq!(replay(&mut replacement).await.len(), 12);
    replacement.wal_writer.append(&[row(13)]).await.unwrap();
    replacement.wal_writer.flush().await.unwrap().await.unwrap();
    assert_eq!(
        next(live.as_mut()).await.unwrap().unwrap().rows,
        vec![row(13)]
    );
    let _ = writer.wal_writer.close().await;
    replacement.wal_writer.close().await.unwrap();
}

#[tokio::test]
async fn gc_overtaking_a_reader_reports_truncation_and_allows_checkpoint_resume() {
    let (servers, volume) = volume().await;
    let wal = reader(&volume);
    let recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    let mut writer = writer_and_replay_with_gc(recovery, config(), wal.gc.clone()).unwrap();
    replay(&mut writer).await;
    for seq in 1..=16 {
        writer.wal_writer.append(&[row(seq)]).await.unwrap();
        writer.wal_writer.flush().await.unwrap().await.unwrap();
    }
    writer.wal_writer.close().await.unwrap();
    let recovery = volume.recover(WalSeqNo::record(16)).await.unwrap();
    let mut writer = writer_and_replay_with_gc(recovery, config(), wal.gc.clone()).unwrap();
    replay(&mut writer).await;
    let mut stale = wal.iterator((1..).into()).await.unwrap();
    wal.collect(vec![(9..).into()], Duration::ZERO, false)
        .await
        .unwrap();
    let objects = servers[3]
        .service
        .observe_prefix("projects/_/buckets/zone-3", "slatedb-test")
        .await;
    let floor: u64 = objects[0].metadata["chorus.trunc"].parse().unwrap();
    assert!(floor > 0 && floor <= 8);
    assert!(matches!(
        next(stale.as_mut()).await,
        Err(WalError::WalTruncated(1))
    ));
    assert!(matches!(
        next(stale.as_mut()).await,
        Err(WalError::WalTruncated(1))
    ));
    assert!(matches!(
        wal.iterator((1..).into()).await,
        Err(WalError::WalTruncated(1))
    ));
    assert!(matches!(
        wal.last_wal_file_id(0).await,
        Err(WalError::WalTruncated(1))
    ));
    let mut resumed = wal.iterator((floor + 1..17).into()).await.unwrap();
    for id in floor + 1..=16 {
        assert_eq!(
            next(resumed.as_mut())
                .await
                .unwrap()
                .unwrap()
                .last_consumed_wal_file_id,
            id
        );
    }
    assert!(next(resumed.as_mut()).await.unwrap().is_none());
    writer.wal_writer.close().await.unwrap();
}

#[tokio::test]
async fn reader_latches_corruption_and_sequence_regression() {
    for corrupt in [false, true] {
        let (_servers, volume) = volume().await;
        let mut recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
        assert!(recovery.try_next().await.unwrap().is_none());
        let mut handle = recovery.start(config()).await.unwrap();
        let payload = codec::encode(&[row(1)], 1024).unwrap();
        handle
            .enqueue_append(WalSeqNo::ZERO, payload.clone())
            .await
            .unwrap()
            .await
            .unwrap();
        handle
            .enqueue_append(
                WalSeqNo::record(1),
                if corrupt {
                    Bytes::from_static(b"foreign")
                } else {
                    payload
                },
            )
            .await
            .unwrap()
            .await
            .unwrap();
        let wal = reader(&volume);
        let mut iterator = wal.iterator((1..).into()).await.unwrap();
        assert_eq!(
            next(iterator.as_mut()).await.unwrap().unwrap().rows,
            vec![row(1)]
        );
        assert!(matches!(
            next(iterator.as_mut()).await,
            Err(WalError::DataError(_))
        ));
        assert!(matches!(
            next(iterator.as_mut()).await,
            Err(WalError::DataError(_))
        ));
        handle.shutdown().await.unwrap();
    }
}
