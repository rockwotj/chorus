use super::*;

#[tokio::test]
async fn pipeline_resumes_a_trailing_partial_record() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories.clone(),
        manifest_factory.clone(),
        "partial-record-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    servers[0].service.inject_partial_write(7).await;
    servers[1].service.inject_partial_write(11).await;
    append_one(&mut writer, b"resumable-tail").await;
    for factory in factories.into_iter().take(2) {
        let object = active_segment_object(&factory, &manifest_factory, "partial-record-wal").await;
        let snapshot = factory.replica(&object).snapshot().await.unwrap();
        assert_eq!(
            RecordFrame::decode_all(&snapshot.bytes).unwrap()[0]
                .payload
                .as_ref(),
            b"resumable-tail"
        );
    }
}

#[tokio::test]
async fn retries_transient_create_and_append_failures() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    servers[0]
        .service
        .inject(Operation::BidiWrite, Code::Unavailable)
        .await;
    let volume = volume(factories, manifest_factory.clone(), "retry-wal");
    let mut writer = volume.recover_writer().await.unwrap();
    servers[0]
        .service
        .inject(Operation::BidiWrite, Code::DeadlineExceeded)
        .await;
    servers[1]
        .service
        .inject(Operation::BidiWrite, Code::Unavailable)
        .await;
    assert_eq!(append_one(&mut writer, b"retried").await, 0);
}

#[tokio::test]
async fn single_replica_wal_appends_rotates_and_recovers() {
    let (_servers, factories, manifest_factory) = factory_cluster_of(1).await;
    let volume = volume(factories.clone(), manifest_factory.clone(), "single-wal");
    let mut writer = volume.recover_writer().await.unwrap();
    assert_eq!(append_one(&mut writer, b"alpha").await, 0);
    assert_eq!(append_one(&mut writer, b"beta").await, 1);
    writer.rotate().await.unwrap();
    assert_eq!(append_one(&mut writer, b"gamma").await, 2);
    drop(writer);

    let (end, records) = recover_records(&volume, WalSeqNo::ZERO).await;
    assert_eq!(end, WalSeqNo::record(3));
    let payloads: Vec<_> = records
        .iter()
        .map(|record| record.payload.clone())
        .collect();
    assert_eq!(payloads, vec![b"alpha".as_slice(), b"beta", b"gamma"]);

    let object = segment_object_for_base(&factories[0], &manifest_factory, "single-wal", 0).await;
    let snapshot = factories[0].replica(&object).snapshot().await.unwrap();
    assert!(snapshot.finalized);
    assert_eq!(RecordFrame::decode_all(&snapshot.bytes).unwrap().len(), 2);
}

#[tokio::test]
async fn five_zone_wal_commits_with_two_zones_crashed() {
    let (servers, factories, manifest_factory) = factory_cluster_of(5).await;
    let volume = volume(factories.clone(), manifest_factory.clone(), "five-zone-wal");
    let mut writer = volume.recover_writer().await.unwrap();
    servers[3].service.set_crashed(true).await;
    servers[4].service.set_crashed(true).await;
    assert_eq!(append_one(&mut writer, b"survives-two-faults").await, 0);
    writer.rotate().await.unwrap();

    let mut sealed = 0;
    for factory in factories.iter().take(3) {
        let object = segment_object_for_base(factory, &manifest_factory, "five-zone-wal", 0).await;
        let snapshot = factory.replica(&object).snapshot().await.unwrap();
        if snapshot.finalized {
            sealed += 1;
            assert_eq!(RecordFrame::decode_all(&snapshot.bytes).unwrap().len(), 1);
        }
    }
    assert!(sealed >= 3, "a five-zone seal needs a three-zone quorum");
}

#[tokio::test]
async fn five_zone_recovery_with_two_zones_down_preserves_committed_data() {
    let (servers, factories, manifest_factory) = factory_cluster_of(5).await;
    let volume = volume(
        factories,
        manifest_factory.clone(),
        "five-zone-recovery-wal",
    );
    let mut first = volume.recover_writer().await.unwrap();
    append_one(&mut first, b"committed").await;
    drop(first);

    // a different pair of zones is down for recovery than for the write
    servers[0].service.set_crashed(true).await;
    servers[1].service.set_crashed(true).await;
    let (end, records) = recover_records(&volume, WalSeqNo::ZERO).await;
    assert_eq!(end, WalSeqNo::record(1));
    assert_eq!(records[0].payload, b"committed".as_slice());
}

#[tokio::test]
async fn five_zone_writes_block_without_a_three_zone_quorum() {
    let (servers, factories, manifest_factory) = factory_cluster_of(5).await;
    let volume = volume(factories, manifest_factory.clone(), "five-zone-floor-wal");
    let mut writer = volume.recover_writer().await.unwrap();
    for server in &servers[2..5] {
        server.service.set_crashed(true).await;
    }
    let pending = writer
        .enqueue_records(vec![record(b"no-quorum")], no_attempted_bytes())
        .await
        .unwrap()
        .remove(0);
    assert!(pending.wait().await.is_err());
}

#[tokio::test]
async fn opening_a_wal_with_a_different_bucket_derived_replica_count_is_refused() {
    let (_servers, factories, manifest_factory) = factory_cluster_of(5).await;
    let three = volume(
        factories[..3].to_vec(),
        manifest_factory.clone(),
        "replica-count-wal",
    );
    let mut writer = three.recover_writer().await.unwrap();
    append_one(&mut writer, b"three-zone-history").await;
    drop(writer);

    // the same prefix opened as a five-zone volume must refuse: every quorum
    // decision in the WAL's history was taken against three-zone membership
    let five = volume(factories, manifest_factory.clone(), "replica-count-wal");
    let error = match five.recover(WalSeqNo::ZERO).await {
        Ok(_) => panic!("a five-zone open of a three-zone WAL must fail"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("buckets"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn opening_a_wal_with_reordered_or_duplicate_buckets_is_refused() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let original = volume(
        factories.clone(),
        manifest_factory.clone(),
        "bucket-binding-wal",
    );
    let mut writer = original.recover_writer().await.unwrap();
    append_one(&mut writer, b"bound-history").await;
    drop(writer);

    let reordered = volume(
        vec![
            factories[1].clone(),
            factories[0].clone(),
            factories[2].clone(),
        ],
        manifest_factory.clone(),
        "bucket-binding-wal",
    );
    let error = match reordered.recover(WalSeqNo::ZERO).await {
        Ok(_) => panic!("a reordered replica set must not open the WAL"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("buckets"),
        "unexpected error: {error}"
    );

    let duplicated = volume(
        vec![
            factories[0].clone(),
            factories[1].clone(),
            factories[1].clone(),
        ],
        manifest_factory,
        "bucket-binding-wal",
    );
    let error = match duplicated.recover(WalSeqNo::ZERO).await {
        Ok(_) => panic!("a replica set with a duplicate bucket must not open the WAL"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("bucket"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn wal_runs_against_a_custom_manifest_store() {
    let (_servers, factories, _manifest_factory) = factory_cluster().await;
    let store: Arc<dyn crate::ManifestStore> =
        Arc::new(crate::manifest_store::test_support::InMemoryManifestStore::default());
    let volume = SegmentedVolume::new_with_factories_and_manifest_store(
        factories.clone(),
        Arc::clone(&store),
        "custom-store-wal",
        test_config(),
    )
    .unwrap();
    let mut writer = volume.recover_writer().await.unwrap();
    assert_eq!(append_one(&mut writer, b"alpha").await, 0);
    assert_eq!(append_one(&mut writer, b"beta").await, 1);
    writer.rotate().await.unwrap();
    assert_eq!(append_one(&mut writer, b"gamma").await, 2);
    drop(writer);

    // a second recovery through the same register claims a fresh epoch and
    // replays everything the first incarnation committed
    let volume = SegmentedVolume::new_with_factories_and_manifest_store(
        factories,
        store,
        "custom-store-wal",
        test_config(),
    )
    .unwrap();
    let (end, records) = recover_records(&volume, WalSeqNo::ZERO).await;
    assert_eq!(end, WalSeqNo::record(3));
    let payloads: Vec<_> = records
        .iter()
        .map(|record| record.payload.clone())
        .collect();
    assert_eq!(payloads, vec![b"alpha".as_slice(), b"beta", b"gamma"]);
}
