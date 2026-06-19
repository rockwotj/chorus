use super::*;

#[tokio::test]
async fn maintenance_sweeps_dead_spares_without_failing_recovery() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let (volume, metrics) = volume_with_metrics(
        factories.clone(),
        manifest_factory.clone(),
        "spare-sweep-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"zero").await;
    let protected_tail = writer
        .catalog()
        .last()
        .expect("writer has an active tail")
        .id
        .clone();
    drop(writer);

    // A dead incarnation's pre-provisioned spare (epoch 1, never swapped
    // in), and an object whose id claims a *newer* epoch than the recovery
    // below will hold — standing in for a racing recovery's fresh segment.
    let provisioning_metrics = Arc::new(crate::metrics::Metrics::new(
        &crate::metrics::NoopMetricsRecorder,
        factories.len(),
    ));
    let dead_spare = crate::segment::segment_id(1, 7);
    let newer = crate::segment::segment_id(99, 0);
    for id in [&dead_spare, &newer] {
        crate::segment::provision_spare(
            factories.clone(),
            "spare-sweep-wal".to_string(),
            test_config(),
            usize::MAX,
            crate::protocol::DEFAULT_LANE_STALL_TIMEOUT,
            id.clone(),
            Arc::clone(&provisioning_metrics),
        )
        .await
        .unwrap();
    }

    for server in &servers {
        server.service.reset_operation_counts().await;
    }
    // A terminal IAM-style delete error must be maintenance-only. Recovery and
    // replay finish without issuing any bucket listing.
    servers[0]
        .service
        .inject(Operation::Delete, Code::PermissionDenied)
        .await;
    let mut recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    (&mut recovery).try_collect::<Vec<_>>().await.unwrap();
    for server in &servers[..3] {
        assert_eq!(server.service.operation_count(Operation::List).await, 0);
    }
    let handle = recovery
        .start(WalEngineConfig {
            repair_interval: Some(Duration::from_millis(20)),
            ..Default::default()
        })
        .await
        .expect("orphan deletion failure must not fail recovery");
    wait_for_counter(&metrics, "chorus.wal.orphan.sweeps_deferred", 1).await;
    wait_for_counter(&metrics, "chorus.wal.orphan.objects_deleted", 3).await;

    for factory in &factories {
        assert_eq!(
            factory
                .replica(&segment_object("spare-sweep-wal", &dead_spare))
                .stat()
                .await
                .unwrap_err()
                .code,
            TransportCode::NotFound,
            "the dead incarnation's spare must be swept"
        );
        factory
            .replica(&segment_object("spare-sweep-wal", &protected_tail))
            .stat()
            .await
            .expect("the manifest tail from the sweep snapshot must survive");
        factory
            .replica(&segment_object("spare-sweep-wal", &newer))
            .stat()
            .await
            .expect("an id claiming a newer epoch must survive the sweep");
    }
    shutdown_engine(handle).await;
}
