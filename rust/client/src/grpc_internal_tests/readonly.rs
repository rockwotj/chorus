use super::*;
use crate::ReadOnlyConfig;

fn readonly_config() -> ReadOnlyConfig {
    ReadOnlyConfig {
        poll_interval: Duration::from_millis(10),
        manifest_poll_interval: Duration::from_millis(10),
        ..ReadOnlyConfig::default()
    }
}

#[tokio::test]
async fn readonly_open_does_not_initialize_the_wal() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories,
        manifest_factory.clone(),
        "readonly-uninitialized-wal",
    );

    assert!(matches!(
        volume
            .open_readonly_with_config(WalSeqNo::ZERO, readonly_config())
            .await,
        Err(Error::Uninitialized)
    ));
    let error = manifest_factory
        .replica("readonly-uninitialized-wal/manifest")
        .stat()
        .await
        .expect_err("readonly open must not create the manifest");
    assert_eq!(error.code, TransportCode::NotFound);
}

#[tokio::test]
async fn readonly_open_reports_uninitialized_before_the_first_manifest_claim() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let prefix = "readonly-unclaimed-wal";
    let manifest_replica = manifest_factory.replica(&format!("{prefix}/manifest"));
    let mut manifest = crate::manifest::Manifest::open(
        Arc::new(crate::manifest_store::GcsManifestStore::new(
            manifest_replica.clone(),
        )),
        test_config(),
        Arc::new(crate::metrics::Metrics::new(
            &crate::NoopMetricsRecorder,
            factories.len(),
        )),
        factories.len(),
        factories
            .iter()
            .map(|factory| factory.bucket_name().to_string())
            .collect(),
    )
    .await
    .unwrap();
    assert_eq!(manifest.record().epoch, 0);
    assert!(manifest.record().tail_id.is_none());
    let before = manifest_replica.stat().await.unwrap();
    let volume = volume(factories, manifest_factory, prefix);
    assert!(matches!(
        volume
            .open_readonly_with_config(WalSeqNo::ZERO, readonly_config())
            .await,
        Err(Error::Uninitialized)
    ));
    let after = manifest_replica.stat().await.unwrap();
    assert_eq!(after.metageneration, before.metageneration);
    assert_eq!(after.metadata, before.metadata);

    manifest.claim().await.unwrap();
    assert!(volume
        .open_readonly_with_config(WalSeqNo::ZERO, readonly_config())
        .await
        .is_ok());
}

#[tokio::test]
async fn readonly_active_tail_survives_one_replaced_generation() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let prefix = "readonly-replaced-generation-wal";
    let volume = volume(factories.clone(), manifest_factory.clone(), prefix);
    let writer = volume.recover_writer().await.unwrap();
    drop(writer);
    let (tail, _) = manifest_frontier_ids(&manifest_factory, prefix).await;
    let object = segment_object(prefix, &tail);
    for factory in &factories {
        append_raw_record(factory, &object, b"first").await;
    }

    // Only zones 0 and 1 can form the first quorum, so the follower must
    // observe zone 0's original generation before its replacement.
    servers[2].service.set_crashed(true).await;
    let mut follower = volume
        .open_readonly_with_config(WalSeqNo::ZERO, readonly_config())
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(5), follower.try_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.seqno, WalSeqNo::ZERO);
    assert_eq!(first.payload, b"first".as_slice());

    let replica = factories[0].replica(&object);
    let original = replica.snapshot().await.unwrap();
    let mut replacement = original.bytes.to_vec();
    replacement.extend_from_slice(&record(b"minority-only").encode().unwrap());
    let replaced = replica
        .replace_appendable(&original, replacement.into(), original.metadata.clone())
        .await
        .unwrap();
    assert_ne!(replaced.generation.unwrap(), original.generation);
    servers[0].service.reset_operation_counts().await;

    // With no new majority record, polling must survive the old session's
    // failure, reopen it, and observe the replacement generation.
    assert!(
        tokio::time::timeout(Duration::from_secs(1), follower.try_next())
            .await
            .is_err()
    );
    assert!(
        servers[0]
            .service
            .operation_count(Operation::BidiRead)
            .await
            > 0
    );
    assert!(servers[0].service.operation_count(Operation::Read).await > 2);

    servers[2].service.set_crashed(false).await;
    for factory in &factories[1..] {
        append_raw_record(factory, &object, b"second").await;
        append_raw_record(factory, &object, b"third").await;
    }
    for (index, payload) in [(1, b"second".as_slice()), (2, b"third".as_slice())] {
        let next = tokio::time::timeout(Duration::from_secs(5), follower.try_next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(next.seqno, WalSeqNo::record(index));
        assert_eq!(next.payload, payload);
    }
}

#[tokio::test]
async fn readonly_follower_tracks_active_records_across_rotation_without_fencing_the_writer() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let writer_volume = volume(
        factories.clone(),
        manifest_factory.clone(),
        "readonly-follow-rotations-wal",
    );
    let reader_volume = volume(factories, manifest_factory, "readonly-follow-rotations-wal");
    let mut writer = writer_volume.recover_writer().await.unwrap();
    let mut follower = reader_volume
        .open_readonly_with_config(WalSeqNo::ZERO, readonly_config())
        .await
        .unwrap();

    append_one(&mut writer, b"first").await;
    for server in &servers {
        server.service.reset_operation_counts().await;
    }
    let first = tokio::time::timeout(Duration::from_secs(5), follower.try_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.seqno, WalSeqNo::ZERO);
    assert_eq!(first.payload, b"first".as_slice());
    assert_readonly_operations(&servers).await;

    writer.rotate().await.unwrap();
    append_one(&mut writer, b"second").await;
    for server in &servers {
        server.service.reset_operation_counts().await;
    }
    let second = tokio::time::timeout(Duration::from_secs(5), follower.try_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(second.seqno, WalSeqNo::record(1));
    assert_eq!(second.payload, b"second".as_slice());
    assert_readonly_operations(&servers).await;

    assert_eq!(append_one(&mut writer, b"writer-still-live").await, 2);
}

async fn assert_readonly_operations(servers: &[chorus_fake_gcs::RunningFake]) {
    for server in servers {
        assert_eq!(
            server
                .service
                .operation_count(Operation::BidiTakeoverOpen)
                .await,
            0,
            "readonly following must never open a takeover stream"
        );
    }
    let mut bidi_reads = 0;
    for server in &servers[..3] {
        bidi_reads += server.service.operation_count(Operation::BidiRead).await;
    }
    assert!(
        bidi_reads > 0,
        "readonly following must use bidirectional reads for the active tail"
    );
    assert_eq!(
        servers[3].service.operation_count(Operation::Update).await,
        0,
        "readonly following must never update the manifest"
    );
}

#[tokio::test]
async fn readonly_active_tail_requires_a_quorum_and_complete_frames() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let prefix = "readonly-active-quorum-wal";
    let volume = volume(factories.clone(), manifest_factory.clone(), prefix);
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"committed").await;

    let mut follower = volume
        .open_readonly_with_config(WalSeqNo::ZERO, readonly_config())
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(5), follower.try_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.payload, b"committed".as_slice());

    let (tail, _) = manifest_frontier_ids(&manifest_factory, prefix).await;
    let object = segment_object(prefix, &tail);
    append_raw_record(&factories[0], &object, b"minority").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), follower.try_next())
            .await
            .is_err(),
        "a record visible in only one zone must not be published"
    );
    append_raw_record(&factories[1], &object, b"minority").await;
    let minority = tokio::time::timeout(Duration::from_secs(5), follower.try_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(minority.payload, b"minority".as_slice());

    append_partial_raw_record(&factories[0], &object, b"partial").await;
    append_partial_raw_record(&factories[1], &object, b"partial").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), follower.try_next())
            .await
            .is_err(),
        "a quorum-visible partial frame must not be published"
    );
    let encoded = record(b"partial").encode().unwrap();
    let final_byte = vec![*encoded.last().unwrap()];
    append_raw_bytes(&factories[0], &object, final_byte.clone()).await;
    append_raw_bytes(&factories[1], &object, final_byte).await;
    let partial = tokio::time::timeout(Duration::from_secs(5), follower.try_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(partial.payload, b"partial".as_slice());
}

#[tokio::test]
async fn readonly_active_tail_returns_after_the_first_matching_quorum() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let writer_volume = volume(
        factories.clone(),
        manifest_factory.clone(),
        "readonly-raced-quorum-wal",
    );
    let reader_volume = volume(factories, manifest_factory, "readonly-raced-quorum-wal");
    let mut writer = writer_volume.recover_writer().await.unwrap();
    let mut follower = reader_volume
        .open_readonly_with_config(WalSeqNo::ZERO, readonly_config())
        .await
        .unwrap();

    append_one(&mut writer, b"first").await;
    servers[2]
        .service
        .inject_delay(Operation::BidiRead, Duration::from_secs(2))
        .await;
    let first = tokio::time::timeout(Duration::from_millis(500), follower.try_next())
        .await
        .expect("a slow third replica must not delay a matching majority")
        .unwrap()
        .unwrap();
    assert_eq!(first.payload, b"first".as_slice());

    append_one(&mut writer, b"second").await;
    let second = tokio::time::timeout(Duration::from_millis(500), follower.try_next())
        .await
        .expect("the fast majority must keep polling while the third read is outstanding")
        .unwrap()
        .unwrap();
    assert_eq!(second.payload, b"second".as_slice());

    for server in &servers[..3] {
        assert_eq!(
            server.service.operation_count(Operation::BidiRead).await,
            1,
            "successive range requests must reuse one BidiReadObject RPC per replica"
        );
    }
}

#[tokio::test]
async fn readonly_active_tail_does_not_wait_for_manifest_refresh() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let writer_volume = volume(
        factories.clone(),
        manifest_factory.clone(),
        "readonly-independent-manifest-wal",
    );
    let reader_volume = volume(
        factories,
        manifest_factory,
        "readonly-independent-manifest-wal",
    );
    let mut writer = writer_volume.recover_writer().await.unwrap();
    let mut follower = reader_volume
        .open_readonly_with_config(WalSeqNo::ZERO, readonly_config())
        .await
        .unwrap();

    for server in &servers {
        server.service.reset_operation_counts().await;
    }
    servers[3]
        .service
        .inject_delay(Operation::Get, Duration::from_secs(2))
        .await;

    append_one(&mut writer, b"first").await;
    let first = tokio::time::timeout(Duration::from_millis(500), follower.try_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.payload, b"first".as_slice());
    tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if servers[3].service.operation_count(Operation::Get).await > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the independent manifest refresh did not start");

    append_one(&mut writer, b"second").await;
    let second = tokio::time::timeout(Duration::from_millis(500), follower.try_next())
        .await
        .expect("an outstanding manifest GET must not delay active-tail reads")
        .unwrap()
        .unwrap();
    assert_eq!(second.payload, b"second".as_slice());
}

#[tokio::test]
async fn readonly_follower_reports_when_truncation_overtakes_it() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory, "readonly-lagged-wal");
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"soon-deleted").await;
    writer.rotate().await.unwrap();

    let mut follower = volume
        .open_readonly_with_config(WalSeqNo::ZERO, readonly_config())
        .await
        .unwrap();
    writer.truncate_before(WalSeqNo::record(1)).await.unwrap();

    let error = tokio::time::timeout(Duration::from_secs(5), follower.try_next())
        .await
        .unwrap()
        .expect_err("the follower must not skip truncated history");
    assert!(matches!(
        error,
        Error::ReadOnlyLagged {
            next: WalSeqNo { record_index: 0 },
            truncation_floor: WalSeqNo { record_index: 1 },
        }
    ));

    assert!(matches!(
        volume
            .open_readonly_with_config(WalSeqNo::ZERO, readonly_config())
            .await,
        Err(Error::ReadOnlyLagged {
            next: WalSeqNo { record_index: 0 },
            truncation_floor: WalSeqNo { record_index: 1 },
        })
    ));
}

#[tokio::test]
async fn readonly_open_rejects_a_zero_poll_interval() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory, "readonly-config-wal");

    for config in [
        ReadOnlyConfig {
            poll_interval: Duration::ZERO,
            ..readonly_config()
        },
        ReadOnlyConfig {
            manifest_poll_interval: Duration::ZERO,
            ..readonly_config()
        },
    ] {
        assert!(matches!(
            volume
                .open_readonly_with_config(WalSeqNo::ZERO, config)
                .await,
            Err(Error::InvalidConfig(_))
        ));
    }
}
