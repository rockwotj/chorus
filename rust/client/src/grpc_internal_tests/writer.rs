use super::*;

#[tokio::test]
async fn quorum_append_rotate_finalizes_exact_segment_bytes() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories.clone(), manifest_factory.clone(), "seal-wal");
    let mut writer = volume.recover_writer().await.unwrap();
    assert_eq!(append_one(&mut writer, b"alpha").await, 0);
    assert_eq!(append_one(&mut writer, b"beta").await, 1);
    writer.rotate().await.unwrap();

    let manifest = manifest_factory
        .replica("seal-wal/manifest")
        .stat()
        .await
        .unwrap();
    let entry = manifest.metadata["chorus.segments"]
        .split(',')
        .next()
        .expect("sealed directory entry");
    let fields: Vec<_> = entry.split(':').collect();
    assert_eq!(fields.len(), 3);
    assert_eq!(fields[1], "0");
    assert_eq!(fields[2].len(), 8);
    assert!(fields[2]
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
    let committed_crc32c = u32::from_str_radix(fields[2], 16).unwrap();

    let mut sealed = 0;
    for factory in factories {
        let object = segment_object_for_base(&factory, &manifest_factory, "seal-wal", 0).await;
        let snapshot = factory.replica(&object).snapshot().await.unwrap();
        if snapshot.finalized {
            sealed += 1;
            assert_eq!(snapshot.metadata["chorus.format"], "1");
            assert_eq!(RecordFrame::decode_all(&snapshot.bytes).unwrap().len(), 2);
            assert!(!snapshot.metadata.contains_key("chorus.owner"));
            assert_eq!(snapshot.crc32c, Some(committed_crc32c));
            assert_eq!(crc32c::crc32c(&snapshot.bytes), committed_crc32c);
        }
    }
    assert!(sealed >= 2);
    assert_eq!(writer.active_segment_base(), 2);
}

#[tokio::test]
async fn rotation_recovers_bytes_when_live_finalization_loses_quorum() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories.clone(),
        manifest_factory.clone(),
        "seal-recovery-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    assert_eq!(append_one(&mut writer, b"alpha").await, 0);
    assert_eq!(append_one(&mut writer, b"beta").await, 1);

    // Fail the finish message on two retained create streams. Their tokens
    // have no generation identity, so the fast path cannot retry them and
    // must reconstruct the committed prefix from storage.
    servers[0]
        .service
        .inject(Operation::BidiFinalize, Code::Unavailable)
        .await;
    servers[1]
        .service
        .inject(Operation::BidiFinalize, Code::Unavailable)
        .await;

    let mut reads_before = 0;
    for server in &servers[..3] {
        reads_before += server.service.operation_count(Operation::Read).await;
    }
    writer.rotate().await.unwrap();
    let mut reads_after = 0;
    for server in &servers[..3] {
        reads_after += server.service.operation_count(Operation::Read).await;
    }
    assert!(
        reads_after > reads_before,
        "degraded finalization did not reconstruct the segment from storage"
    );

    let mut finalized = 0;
    for factory in factories {
        let object =
            segment_object_for_base(&factory, &manifest_factory, "seal-recovery-wal", 0).await;
        let snapshot = factory.replica(&object).snapshot().await.unwrap();
        if snapshot.finalized {
            finalized += 1;
            assert_eq!(RecordFrame::decode_all(&snapshot.bytes).unwrap().len(), 2);
        }
    }
    assert!(finalized >= 2);
}

#[tokio::test]
async fn one_crashed_zone_does_not_block_a_write_quorum() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory.clone(), "degraded-write-wal");
    let mut writer = volume.recover_writer().await.unwrap();
    servers[2].service.set_crashed(true).await;
    assert_eq!(append_one(&mut writer, b"survives").await, 0);
}

#[tokio::test]
async fn delayed_third_lane_does_not_block_ordered_quorum_progress() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories.clone(), manifest_factory.clone(), "slow-lane-wal");
    let mut writer = volume.recover_writer().await.unwrap();
    servers[2]
        .service
        .inject_delay(Operation::BidiWrite, Duration::from_secs(1))
        .await;
    tokio::time::timeout(
        Duration::from_millis(250),
        append_one(&mut writer, b"first"),
    )
    .await
    .expect("healthy quorum must not wait for zone 2");
    tokio::time::timeout(
        Duration::from_millis(250),
        append_one(&mut writer, b"second"),
    )
    .await
    .expect("the quorum pipeline must keep moving");

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let object =
                active_segment_object(&factories[2], &manifest_factory, "slow-lane-wal").await;
            let snapshot = factories[2].replica(&object).snapshot().await.unwrap();
            if RecordFrame::decode_all(&snapshot.bytes).unwrap().len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn seal_abandons_a_deeply_delayed_lane_after_a_quorum_drains() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories.clone(),
        manifest_factory.clone(),
        "laggard-seal-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"first").await;
    // zone 2's next append flush stalls far past the seal's drain grace; the
    // record still commits on the two healthy zones
    servers[2]
        .service
        .inject_delay(Operation::BidiWrite, Duration::from_secs(30))
        .await;
    append_one(&mut writer, b"second").await;
    tokio::time::timeout(Duration::from_secs(3), writer.rotate())
        .await
        .expect("the seal must not wait out a deeply backlogged lane")
        .unwrap();
    assert_eq!(writer.active_segment_base(), 2);

    // the abandoned copy is a prefix of the canonical bytes; the two
    // finalized copies carry the full sealed segment
    let mut finalized = 0;
    for factory in factories.iter().take(2) {
        let object =
            segment_object_for_base(factory, &manifest_factory, "laggard-seal-wal", 0).await;
        let snapshot = factory.replica(&object).snapshot().await.unwrap();
        if snapshot.finalized {
            finalized += 1;
            assert_eq!(RecordFrame::decode_all(&snapshot.bytes).unwrap().len(), 2);
        }
    }
    assert_eq!(finalized, 2, "the healthy zones must seal without zone 2");
}

#[tokio::test]
async fn pipeline_returns_per_record_quorum_futures() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory.clone(), "pipeline-wal");
    let mut writer = volume.recover_writer().await.unwrap();
    servers[2]
        .service
        .inject_delay(Operation::BidiWrite, Duration::from_secs(1))
        .await;
    let pending = writer
        .enqueue_records(
            vec![record(b"a"), record(b"b"), record(b"c")],
            no_attempted_bytes(),
        )
        .await
        .unwrap();
    let offsets = tokio::time::timeout(Duration::from_millis(250), async {
        futures::future::join_all(pending.into_iter().map(|pending| pending.wait()))
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
    })
    .await
    .unwrap();
    assert_eq!(offsets, vec![0, 1, 2]);
}
