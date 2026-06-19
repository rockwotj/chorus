use super::*;

#[tokio::test]
async fn caller_numbered_appends_admit_in_order_and_refill_the_pipeline() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let (volume, metrics) = volume_with_metrics(factories, manifest_factory.clone(), "engine-wal");
    let mut handle = WalEngine::start(
        volume.recover_writer().await.unwrap(),
        WalEngineConfig {
            max_record_bytes: 64 * 1024,
            pipeline_window_records: 8,
            ..Default::default()
        },
    )
    .unwrap();
    servers[0]
        .service
        .inject_delay(Operation::BidiWrite, Duration::from_millis(50))
        .await;
    servers[1]
        .service
        .inject_delay(Operation::BidiWrite, Duration::from_millis(50))
        .await;

    assert!(matches!(
        handle
            .enqueue_append(
                WalSeqNo::record(1),
                bytes::Bytes::from_static(b"out-of-order"),
            )
            .await,
        Err(Error::OutOfOrder {
            expected: WalSeqNo { record_index: 0 },
            actual: WalSeqNo { record_index: 1 },
        })
    ));
    assert!(matches!(
        handle
            .enqueue_append(WalSeqNo::ZERO, bytes::Bytes::from(vec![0; 64 * 1024 + 1]),)
            .await,
        Err(Error::RecordTooLarge {
            max: 65536,
            actual: 65537,
        })
    ));

    let first = tokio::time::timeout(
        Duration::from_millis(10),
        handle.enqueue_append(
            WalSeqNo::ZERO,
            bytes::Bytes::from_static(b"encoded-transaction-0"),
        ),
    )
    .await
    .expect("admission must not wait for GCS durability")
    .unwrap();
    assert!(matches!(
        handle
            .enqueue_append(WalSeqNo::ZERO, bytes::Bytes::from_static(b"duplicate"),)
            .await,
        Err(Error::OutOfOrder { .. })
    ));

    let mut completions = vec![first];
    for seqno in 1..64 {
        completions.push(
            handle
                .enqueue_append(
                    WalSeqNo::record(seqno),
                    bytes::Bytes::from(format!("transaction-{seqno}")),
                )
                .await
                .unwrap(),
        );
    }
    for (seqno, completion) in completions.into_iter().enumerate() {
        let receipt = completion.await.unwrap();
        assert_eq!(receipt.seqno, WalSeqNo::record(seqno as u64));
        assert_eq!(receipt.next_seqno(), WalSeqNo::record(seqno as u64 + 1));
    }
    assert_eq!(metrics.counter("chorus.wal.append.committed_records"), 64);
    // No refill-count assertion: the engine drains completion bursts and
    // refills in batches, so a small fast pipeline may never refill while
    // records are still outstanding.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn latency_injected_pipeline_keeps_many_commits_in_flight_across_rotation() {
    let (servers, factories, manifest_factory) = latency_factory_cluster().await;
    let (volume, metrics) =
        volume_with_metrics(factories, manifest_factory, "latency-pipeline-wal");
    let mut handle = WalEngine::start(
        volume.recover_writer().await.unwrap(),
        WalEngineConfig {
            queue_capacity: 256,
            max_record_bytes: 4096,
            pipeline_window_records: 64,
            max_inflight_bytes: 64 * 1024 * 1024,
            max_replica_lag_bytes: 64 * 1024 * 1024,
            max_segment_bytes: 1024 * 1024,
            repair_interval: None,
            ..Default::default()
        },
    )
    .unwrap();
    const OFFERED_RATE: f64 = 1_500.0;
    const RECORDS: u64 = 3_000;
    let started = tokio::time::Instant::now();
    let mut completions: FuturesUnordered<crate::AppendCompletion> = FuturesUnordered::new();
    let payload = bytes::Bytes::from(vec![0x5a; 4096]);
    let workload = tokio::time::timeout(Duration::from_secs(10), async {
        for seqno in 0..RECORDS {
            let send_at = started + Duration::from_secs_f64(seqno as f64 / OFFERED_RATE);
            let delay = tokio::time::sleep_until(send_at);
            tokio::pin!(delay);
            loop {
                tokio::select! {
                    biased;
                    completed = completions.next(), if !completions.is_empty() => {
                        completed.unwrap().unwrap();
                    }
                    _ = &mut delay => break,
                }
            }
            let completion = {
                let enqueue = handle.enqueue_append(WalSeqNo::record(seqno), payload.clone());
                tokio::pin!(enqueue);
                loop {
                    tokio::select! {
                        biased;
                        completed = completions.next(), if !completions.is_empty() => {
                            completed.unwrap().unwrap();
                        }
                        result = &mut enqueue => break result,
                    }
                }
            };
            let completion = match completion {
                Ok(completion) => completion,
                Err(error) => {
                    panic!("latency pipeline admission failed at {seqno}: {error}");
                }
            };
            completions.push(completion);
        }
        while let Some(result) = completions.next().await {
            result.unwrap();
        }
    })
    .await;
    let elapsed = started.elapsed();
    let completed = metrics.counter("chorus.wal.append.committed_records");
    let records_per_second = completed as f64 / elapsed.as_secs_f64();
    if workload.is_err() {
        for (index, server) in servers.iter().enumerate() {
            eprintln!(
                "server {index}: create={} append={} finalize={} get={} update={}",
                server.service.operation_count(Operation::BidiCreate).await,
                server
                    .service
                    .operation_count(Operation::BidiAppendFlush)
                    .await,
                server
                    .service
                    .operation_count(Operation::BidiFinalize)
                    .await,
                server.service.operation_count(Operation::Get).await,
                server.service.operation_count(Operation::Update).await,
            );
        }
        panic!(
            "latency pipeline timed out: completed={completed}/{RECORDS} rate={records_per_second:.1}/s rotation_state={} rotations={} max_inflight={} provision_attempts={} provision_failures={} operation_failures={} manifest_cas={} manifest_conflicts={}",
            metrics.gauge("chorus.wal.rotation.state"),
            metrics.counter("chorus.wal.rotation.completed"),
            metrics.gauge("chorus.wal.pipeline.max_inflight_records"),
            metrics.counter("chorus.wal.rotation.spare_provisioning_attempts"),
            metrics.counter("chorus.wal.rotation.spare_provisioning_failures"),
            metrics.counter("chorus.wal.operation.failures"),
            metrics.counter("chorus.wal.manifest.cas_attempts"),
            metrics.counter("chorus.wal.manifest.cas_conflicts"),
        );
    }
    let creates = servers[0]
        .service
        .operation_count(Operation::BidiCreate)
        .await;
    let folds = servers[3].service.operation_count(Operation::Update).await;
    eprintln!(
        "latency pipeline: records={RECORDS} elapsed={elapsed:?} rate={records_per_second:.1}/s max_inflight={} creates_per_zone={creates} manifest_updates={folds}",
        metrics.gauge("chorus.wal.pipeline.max_inflight_records"),
    );
    assert!(
        metrics.gauge("chorus.wal.pipeline.max_inflight_records") >= 32,
        "commit path did not fill a healthy pipeline"
    );
    assert!(
        records_per_second >= OFFERED_RATE * 0.75,
        "latency-injected throughput collapsed to {records_per_second:.1}/s"
    );
    shutdown_engine(handle).await;
    assert!(
        metrics.counter("chorus.wal.rotation.completed") >= 2,
        "loaded run did not complete multiple rotations"
    );
    assert_eq!(
        metrics.counter("chorus.wal.operation.failures"),
        0,
        "loaded run encountered an internal operation failure"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_depth_one_makes_progress_across_latency_injected_rotation() {
    let (servers, factories, manifest_factory) = latency_factory_cluster().await;
    let (volume, metrics) = volume_with_metrics(factories, manifest_factory, "latency-qd1-wal");
    let mut handle = WalEngine::start(
        volume.recover_writer().await.unwrap(),
        WalEngineConfig {
            queue_capacity: 1,
            max_record_bytes: 4096,
            pipeline_window_records: 1,
            max_inflight_bytes: 4100,
            max_replica_lag_bytes: 64 * 1024 * 1024,
            max_segment_bytes: 128 * 1024,
            repair_interval: None,
            ..Default::default()
        },
    )
    .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        // A 4 KiB payload occupies 4,100 encoded bytes. Ninety-seven
        // queue-depth-one appends fill the first two successors and then
        // require the second background refill before dispatch can continue.
        for seqno in 0..97 {
            handle
                .enqueue_append(
                    WalSeqNo::record(seqno),
                    bytes::Bytes::from(vec![0x5a; 4096]),
                )
                .await
                .unwrap()
                .await
                .unwrap();
        }
        while metrics.counter("chorus.wal.rotation.completed") < 2
            || metrics.counter("chorus.wal.seal.segments") < 2
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    if result.is_err() {
        let manifest_updates = servers[3].service.operation_count(Operation::Update).await;
        panic!(
            "queue-depth-one append wedged under service latency: rotation_state={} rotations={} provisions={} failures={} manifest_updates={}",
            metrics.gauge("chorus.wal.rotation.state"),
            metrics.counter("chorus.wal.rotation.completed"),
            metrics.counter("chorus.wal.rotation.spare_provisioning_attempts"),
            metrics.counter("chorus.wal.rotation.spare_provisioning_failures"),
            manifest_updates,
        );
    }
    shutdown_engine(handle).await;
    let rotations = metrics.counter("chorus.wal.rotation.completed");
    eprintln!(
        "latency qd1: rotations={rotations} creates_per_zone={} manifest_updates={} operation_failures={} repair_failures={} seal_retries={} spare_failures={} append_failures={}",
        servers[0]
            .service
            .operation_count(Operation::BidiCreate)
            .await,
        servers[3].service.operation_count(Operation::Update).await,
        metrics.counter("chorus.wal.operation.failures"),
        metrics.counter("chorus.wal.repair.failures"),
        metrics.counter("chorus.wal.seal.enforcement_retries"),
        metrics.counter("chorus.wal.rotation.spare_provisioning_failures"),
        metrics.counter("chorus.wal.append.failures"),
    );
    assert!(
        rotations >= 2,
        "queue-depth-one run completed only {rotations} rotations"
    );
    assert_eq!(
        metrics.counter("chorus.wal.operation.failures"),
        0,
        "queue-depth-one run encountered an internal operation failure"
    );
}

#[tokio::test]
async fn engine_configuration_is_validated_without_panicking() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory.clone(), "invalid-config-wal");
    let writer = volume.recover_writer().await.unwrap();
    assert!(matches!(
        WalEngine::start(
            writer,
            WalEngineConfig {
                max_segment_bytes: 0,
                ..Default::default()
            },
        ),
        Err(Error::InvalidConfig("max_segment_bytes must be nonzero"))
    ));

    let writer = volume.recover_writer().await.unwrap();
    assert!(matches!(
        WalEngine::start(
            writer,
            WalEngineConfig {
                max_active_segment_bytes: 0,
                ..Default::default()
            },
        ),
        Err(Error::InvalidConfig(
            "max_active_segment_bytes must be nonzero"
        ))
    ));

    let writer = volume.recover_writer().await.unwrap();
    assert!(matches!(
        WalEngine::start(
            writer,
            WalEngineConfig {
                max_record_bytes: 1,
                max_inflight_bytes: 5,
                max_replica_lag_bytes: 5,
                max_segment_bytes: 6,
                max_active_segment_bytes: 5,
                ..Default::default()
            },
        ),
        Err(Error::InvalidConfig(
            "max_active_segment_bytes must be at least max_segment_bytes"
        ))
    ));

    let writer = volume.recover_writer().await.unwrap();
    assert!(matches!(
        WalEngine::start(
            writer,
            WalEngineConfig {
                repair_interval: Some(Duration::ZERO),
                ..Default::default()
            },
        ),
        Err(Error::InvalidConfig("repair_interval must be nonzero"))
    ));

    let writer = volume.recover_writer().await.unwrap();
    assert!(matches!(
        WalEngine::start(
            writer,
            WalEngineConfig {
                lane_stall_timeout: Duration::ZERO,
                ..Default::default()
            },
        ),
        Err(Error::InvalidConfig("lane_stall_timeout must be nonzero"))
    ));

    let writer = volume.recover_writer().await.unwrap();
    assert!(matches!(
        WalEngine::start(
            writer,
            WalEngineConfig {
                shutdown_timeout: Duration::ZERO,
                ..Default::default()
            },
        ),
        Err(Error::InvalidConfig("shutdown_timeout must be nonzero"))
    ));

    let writer = volume.recover_writer().await.unwrap();
    assert!(matches!(
        WalEngine::start(
            writer,
            WalEngineConfig {
                max_record_bytes: 1024,
                max_inflight_bytes: 1027,
                ..Default::default()
            },
        ),
        Err(Error::InvalidConfig(
            "max_inflight_bytes must fit one maximum-size encoded record"
        ))
    ));

    let writer = volume.recover_writer().await.unwrap();
    assert!(matches!(
        WalEngine::start(
            writer,
            WalEngineConfig {
                max_record_bytes: 1024,
                max_inflight_bytes: 2048,
                max_replica_lag_bytes: 1024,
                ..Default::default()
            },
        ),
        Err(Error::InvalidConfig(
            "max_replica_lag_bytes must be at least max_inflight_bytes"
        ))
    ));
}

#[tokio::test]
async fn active_segment_ceiling_backpressures_cleanly_until_truncation_frees_rotation() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory, "active-segment-ceiling-wal");
    let mut writer = volume.recover_writer().await.unwrap();

    // Fill the real register directory with one-record sealed segments. The
    // final record stays in the active segment because directory_has_room(1)
    // becomes false; this is the retention-cap -> rotation-deferral edge the
    // hard ceiling exists to bound.
    let mut rotations = 0usize;
    loop {
        append_one(&mut writer, b"x").await;
        if !writer.rotation_due(1) {
            break;
        }
        writer.rotate().await.unwrap();
        rotations += 1;
        assert!(rotations < 512, "manifest directory never reached its cap");
    }
    assert!(rotations > 0);
    let active_base = writer.active_segment_base();
    let next = WalSeqNo::record(writer.committed_record_end());
    let mut handle = WalEngine::start(
        writer,
        WalEngineConfig {
            max_record_bytes: 1,
            max_inflight_bytes: 5,
            max_replica_lag_bytes: 5,
            max_segment_bytes: 1,
            max_active_segment_bytes: 5,
            repair_interval: None,
            ..Default::default()
        },
    )
    .unwrap();

    assert!(matches!(
        handle
            .enqueue_append(next, bytes::Bytes::from_static(b"y"))
            .await,
        Err(Error::ActiveSegmentFull {
            max: 5,
            current: 5,
            requested: 5,
        })
    ));
    // The error is admission backpressure: the same sequence number remains
    // valid and the engine is still serving maintenance.
    assert!(matches!(
        handle
            .enqueue_append(next, bytes::Bytes::from_static(b"y"))
            .await,
        Err(Error::ActiveSegmentFull { .. })
    ));

    let report = handle
        .truncate_before(WalSeqNo::record(active_base))
        .await
        .unwrap();
    assert!(report.deleted_segments > 0);
    let completion = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match handle
                .enqueue_append(next, bytes::Bytes::from_static(b"y"))
                .await
            {
                Err(Error::ActiveSegmentFull { .. }) => tokio::task::yield_now().await,
                result => break result,
            }
        }
    })
    .await
    .expect("rotation did not resume after truncation freed directory capacity")
    .unwrap();
    completion.await.unwrap();
    shutdown_engine(handle).await;
}

#[tokio::test]
async fn admission_requires_manifest_room_to_seal_the_active_segment() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory, "active-seal-slot-wal");
    let mut writer = volume.recover_writer().await.unwrap();

    // Rotation reserves two entries so a swap-window crash can seal both
    // appendable candidates. Consume rotations until only the final one-entry
    // recovery reserve remains, then seal the current tail into that slot.
    loop {
        append_one(&mut writer, b"x").await;
        if !writer.rotation_due(1) {
            break;
        }
        writer.rotate().await.unwrap();
    }
    writer.rotate().await.unwrap();
    let active_base = writer.active_segment_base();
    let next = WalSeqNo::record(writer.committed_record_end());
    let mut handle = WalEngine::start(
        writer,
        WalEngineConfig {
            repair_interval: None,
            ..Default::default()
        },
    )
    .unwrap();

    assert!(matches!(
        handle
            .enqueue_append(next, bytes::Bytes::from_static(b"unsafe"))
            .await,
        Err(Error::SegmentDirectoryFull)
    ));

    let report = handle
        .truncate_before(WalSeqNo::record(active_base))
        .await
        .unwrap();
    assert!(report.deleted_segments > 0);
    let completion = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match handle
                .enqueue_append(next, bytes::Bytes::from_static(b"safe"))
                .await
            {
                Err(Error::SegmentDirectoryFull) => tokio::task::yield_now().await,
                result => break result,
            }
        }
    })
    .await
    .expect("admission did not resume after truncation freed a seal slot")
    .unwrap();
    completion.await.unwrap();
    shutdown_engine(handle).await;
}

#[tokio::test]
async fn admission_waits_for_the_encoded_inflight_byte_budget() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let (volume, metrics) = volume_with_metrics(factories, manifest_factory, "byte-budget-wal");
    let mut handle = WalEngine::start(
        volume.recover_writer().await.unwrap(),
        WalEngineConfig {
            max_record_bytes: 5,
            max_inflight_bytes: 9,
            max_replica_lag_bytes: 9,
            ..Default::default()
        },
    )
    .unwrap();
    servers[0].service.inject_flush_hold().await;
    servers[1].service.inject_flush_hold().await;

    let first = handle
        .enqueue_append(WalSeqNo::ZERO, bytes::Bytes::from_static(b"first"))
        .await
        .unwrap();
    let second = {
        let admission =
            handle.enqueue_append(WalSeqNo::record(1), bytes::Bytes::from_static(b"other"));
        tokio::pin!(admission);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), admission.as_mut())
                .await
                .is_err()
        );

        servers[0].service.release_flush_holds().await;
        servers[1].service.release_flush_holds().await;
        tokio::time::timeout(Duration::from_secs(1), admission)
            .await
            .expect("quorum completion must release the byte budget")
            .unwrap()
    };
    first.await.unwrap();
    second.await.unwrap();
    assert_eq!(metrics.gauge("chorus.wal.pipeline.max_inflight_bytes"), 9);
    shutdown_engine(handle).await;
}

#[tokio::test]
async fn queue_capacity_bounds_channel_and_engine_queue_together() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let (volume, metrics) =
        volume_with_metrics(factories, manifest_factory, "record-queue-budget-wal");
    let mut handle = WalEngine::start(
        volume.recover_writer().await.unwrap(),
        WalEngineConfig {
            queue_capacity: 2,
            pipeline_window_records: 1,
            repair_interval: None,
            ..Default::default()
        },
    )
    .unwrap();
    servers[0].service.inject_flush_hold().await;
    servers[1].service.inject_flush_hold().await;

    let first = handle
        .enqueue_append(WalSeqNo::ZERO, bytes::Bytes::from_static(b"first"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while metrics.gauge("chorus.wal.pipeline.max_inflight_records") != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the first record never entered the pipeline");

    let second = handle
        .enqueue_append(WalSeqNo::record(1), bytes::Bytes::from_static(b"second"))
        .await
        .unwrap();
    let third = handle
        .enqueue_append(WalSeqNo::record(2), bytes::Bytes::from_static(b"third"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while metrics.gauge("chorus.wal.pipeline.queue_depth") != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the combined queue never reached its configured capacity");

    let fourth = {
        let fourth =
            handle.enqueue_append(WalSeqNo::record(3), bytes::Bytes::from_static(b"fourth"));
        tokio::pin!(fourth);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), fourth.as_mut())
                .await
                .is_err(),
            "a third waiting record exceeded the combined queue capacity"
        );
        assert_eq!(metrics.gauge("chorus.wal.pipeline.queue_depth"), 2);

        servers[0].service.release_flush_holds().await;
        servers[1].service.release_flush_holds().await;
        tokio::time::timeout(Duration::from_secs(1), fourth)
            .await
            .expect("queue capacity was not returned after pipeline dispatch")
            .unwrap()
    };
    for completion in [first, second, third, fourth] {
        completion.await.unwrap();
    }
    shutdown_engine(handle).await;
}

#[tokio::test]
async fn lagging_replica_is_dropped_at_its_retained_byte_budget() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let (volume, metrics) =
        volume_with_metrics(factories, manifest_factory, "lane-byte-budget-wal");
    let mut handle = WalEngine::start(
        volume.recover_writer().await.unwrap(),
        WalEngineConfig {
            max_record_bytes: 5,
            max_inflight_bytes: 9,
            max_replica_lag_bytes: 9,
            ..Default::default()
        },
    )
    .unwrap();
    servers[2].service.inject_flush_hold().await;

    handle
        .enqueue_append(WalSeqNo::ZERO, bytes::Bytes::from_static(b"first"))
        .await
        .unwrap()
        .await
        .unwrap();
    handle
        .enqueue_append(WalSeqNo::record(1), bytes::Bytes::from_static(b"other"))
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(metrics.counter("chorus.wal.lane.capacity_drops"), 1);

    servers[2].service.release_flush_holds().await;
    shutdown_engine(handle).await;
}

#[tokio::test]
async fn attempted_byte_metrics_include_transport_retries() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let (volume, metrics) =
        volume_with_metrics(factories, manifest_factory.clone(), "attempted-bytes-wal");
    let mut recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    while recovery.try_next().await.unwrap().is_some() {}
    let mut handle = recovery
        .start(WalEngineConfig {
            max_segment_bytes: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    servers[0]
        .service
        .inject(Operation::BidiWrite, Code::Unavailable)
        .await;
    handle
        .enqueue_append(WalSeqNo::ZERO, bytes::Bytes::from_static(b"retry"))
        .await
        .unwrap()
        .await
        .unwrap();

    // The injected lane failure retries after a backoff on a zone the
    // commit quorum never waited for; poll until the retried chunk's bytes
    // are attempted. The wait is wall-clock-bounded like
    // `wait_for_repair_passes`, not iteration-bounded.
    tokio::time::timeout(Duration::from_secs(10), async {
        while metrics.counter("chorus.wal.replica.bytes_attempted") < 9 * 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the zone 0 lane never attempted the retried chunk");
    handle.truncate_before(WalSeqNo::ZERO).await.unwrap();
    assert_eq!(metrics.counter("chorus.wal.append.encoded_bytes"), 9);
    assert_eq!(metrics.counter("chorus.wal.replica.bytes_attempted"), 9 * 4);
    assert!(metrics.counter("chorus.wal.lane.retries") >= 1);
    shutdown_engine(handle).await;
}

#[tokio::test]
async fn clean_rotation_avoids_followup_content_reads() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory, "rotation-rpc-wal");
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"sealed").await;
    for server in &servers {
        server.service.reset_operation_counts().await;
    }

    writer.rotate().await.unwrap();

    let regional = &servers[3].service;
    assert_eq!(regional.operation_count(Operation::Update).await, 1);
    assert_eq!(regional.operation_count(Operation::Get).await, 0);
    assert_eq!(regional.operation_count(Operation::Read).await, 0);
    for server in &servers[..3] {
        assert_eq!(server.service.operation_count(Operation::Get).await, 0);
        assert_eq!(server.service.operation_count(Operation::Read).await, 0);
        // segment objects are never mutated after creation: chain position
        // lives in the manifest directory, so no seal-time metadata CAS
        assert_eq!(server.service.operation_count(Operation::Update).await, 0);
        assert_eq!(
            server.service.operation_count(Operation::BidiWrite).await,
            2
        );
    }
}

#[tokio::test]
async fn pending_rotation_flip_uses_no_storage_rpc() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory, "rotation-flip-rpc-wal");
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"sealed").await;
    assert!(writer.spare_ready());
    for server in &servers {
        server.service.reset_operation_counts().await;
    }

    let swap = writer.begin_swap().await.unwrap().unwrap();

    for server in &servers {
        for operation in [
            Operation::Get,
            Operation::Read,
            Operation::Update,
            Operation::BidiCreate,
            Operation::BidiTakeoverOpen,
            Operation::BidiAppendFlush,
            Operation::BidiFinalize,
        ] {
            assert_eq!(
                server.service.operation_count(operation).await,
                0,
                "in-memory rotation issued {operation:?}"
            );
        }
    }
    drop(swap);
}

#[tokio::test]
async fn rotation_refuses_to_cross_an_uncommitted_tail_gap() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory, "rotation-tail-gap-wal");
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"r0").await;

    for server in &servers[..3] {
        server.service.inject_flush_hold().await;
    }
    servers[0].service.permit_held_flushes(1).await;
    let pending = writer
        .enqueue_records(vec![record(b"r1")], no_attempted_bytes())
        .await
        .unwrap()
        .remove(0);

    // r1 may become durable on A, but B and C cannot advance the contiguous
    // commit watermark. Rotation must keep the successor base at the committed
    // boundary instead of deriving it from the two admitted records.
    let swap = writer.begin_swap().await;
    for server in &servers[..3] {
        server.service.release_flush_holds().await;
    }
    pending.wait().await.unwrap();
    assert!(
        matches!(swap, Err(Error::Internal(_))),
        "rotation crossed an admitted-but-uncommitted tail record"
    );
}

#[tokio::test]
async fn recovery_walks_tail_then_pending_after_pre_fold_crash() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories,
        manifest_factory,
        "pre-fold-tail-pending-recovery-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"old-tail").await;
    let swap = writer.begin_swap().await.unwrap().unwrap();
    append_one(&mut writer, b"pending-tail").await;
    drop(swap);
    drop(writer);
    for server in &servers[..3] {
        server.service.reset_operation_counts().await;
    }

    let recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    assert_eq!(recovery.end, WalSeqNo::record(2));
    for server in &servers[..3] {
        // Both manifest candidates are fenced on every zone via exact-generation
        // takeover. An exact-generation open is impossible without first
        // resolving the generation, so this also witnesses generation-before-
        // takeover. (Canonical content is fetched from a single witness per
        // candidate rather than a full quorum, so per-zone read counts are
        // intentionally asymmetric and no longer asserted.)
        assert_eq!(
            server
                .service
                .operation_count(Operation::BidiTakeoverOpen)
                .await,
            2,
            "recovery did not fence both manifest candidates"
        );
        // Each candidate's current generation is resolved on every zone (a
        // metadata Get) before its exact-generation takeover.
        assert!(
            server.service.operation_count(Operation::Get).await >= 2,
            "recovery did not resolve each candidate's generation on every zone"
        );
    }
    let records = recovery.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.payload.as_ref())
            .collect::<Vec<_>>(),
        vec![b"old-tail".as_slice(), b"pending-tail".as_slice()]
    );
}

#[tokio::test]
async fn recovery_walks_tail_then_pending_with_third_zone_unavailable() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories,
        manifest_factory,
        "pre-fold-two-zone-recovery-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();

    // A and B acknowledge both sides of the in-memory flip. C remains
    // unavailable through the crash and recovery.
    servers[2].service.set_crashed(true).await;
    append_one(&mut writer, b"old-tail").await;
    let swap = writer.begin_swap().await.unwrap().unwrap();
    append_one(&mut writer, b"pending-tail").await;
    drop(swap);
    drop(writer);

    let (end, records) = recover_records(&volume, WalSeqNo::ZERO).await;
    assert_eq!(end, WalSeqNo::record(2));
    assert_eq!(
        records
            .iter()
            .map(|record| record.payload.as_ref())
            .collect::<Vec<_>>(),
        vec![b"old-tail".as_slice(), b"pending-tail".as_slice()]
    );
}

#[tokio::test]
async fn recovery_promotes_longer_tail_and_pending_prefixes_from_one_quorum_witness() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories,
        manifest_factory,
        "pre-fold-lagging-quorum-recovery-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();

    // A and C acknowledge both records while B remains at the empty prefixes.
    servers[1].service.set_crashed(true).await;
    append_one(&mut writer, b"old-tail").await;
    let swap = writer.begin_swap().await.unwrap().unwrap();
    append_one(&mut writer, b"pending-tail").await;
    drop(swap);
    drop(writer);

    // Recover through A and the lagging B. Quorum intersection requires
    // promoting A's longer compatible prefix for both manifest candidates.
    servers[1].service.set_crashed(false).await;
    servers[2].service.set_crashed(true).await;
    let (end, records) = recover_records(&volume, WalSeqNo::ZERO).await;
    assert_eq!(end, WalSeqNo::record(2));
    assert_eq!(
        records
            .iter()
            .map(|record| record.payload.as_ref())
            .collect::<Vec<_>>(),
        vec![b"old-tail".as_slice(), b"pending-tail".as_slice()]
    );
}

#[tokio::test]
async fn recovery_treats_a_partial_first_record_as_an_empty_frontier() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let prefix = "partial-first-record-recovery-wal";
    let volume = volume(factories.clone(), manifest_factory.clone(), prefix);
    let writer = volume.recover_writer().await.unwrap();
    let (tail_id, _) = manifest_frontier_ids(&manifest_factory, prefix).await;
    let tail_object = segment_object(prefix, &tail_id);

    // A has positive durable bytes but no complete record. B remains empty and
    // C is unavailable, so the reachable quorum's committed record frontier is
    // still empty even though byte-size selection first chooses A's tail.
    append_partial_raw_record(&factories[0], &tail_object, b"torn-first-record").await;
    drop(writer);
    servers[2].service.set_crashed(true).await;

    let mut recovered = volume.recover_writer().await.unwrap();
    assert_eq!(recovered.active_segment_base(), 0);
    let catalog = recovered.catalog();
    assert_eq!(catalog.len(), 1);
    assert_eq!(catalog[0].end_record_index, None);
    assert_ne!(catalog[0].id, tail_id);
    assert_eq!(
        append_one(&mut recovered, b"first-complete-record").await,
        0
    );

    let manifest = manifest_factory
        .replica(&format!("{prefix}/manifest"))
        .stat()
        .await
        .unwrap();
    assert_ne!(manifest.metadata.get("chorus.tail_id"), Some(&tail_id));
    assert_eq!(
        manifest.metadata.get("chorus.segments").map(String::as_str),
        Some("")
    );
}

#[tokio::test]
async fn recovery_accepts_a_live_and_finalized_empty_quorum() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let prefix = "finalized-empty-recovery-wal";
    let volume = volume(factories.clone(), manifest_factory.clone(), prefix);
    let writer = volume.recover_writer().await.unwrap();
    let (tail_id, _) = manifest_frontier_ids(&manifest_factory, prefix).await;
    let tail_object = segment_object(prefix, &tail_id);

    // Model a prior recovery that finalized B at the empty boundary before
    // crashing. A remains a live empty witness and C is unavailable.
    let replica_b = factories[1].replica(&tail_object);
    let mut token_b = replica_b.takeover_current().await.unwrap();
    assert_eq!(token_b.persisted_size, 0);
    let finalized_b = replica_b.finalize(&mut token_b, 0).await.unwrap();
    assert!(finalized_b.finalized);
    assert_eq!(finalized_b.persisted_size, 0);
    drop(writer);
    servers[2].service.set_crashed(true).await;

    let mut recovered = volume.recover_writer().await.unwrap();
    assert_eq!(recovered.active_segment_base(), 0);
    let catalog = recovered.catalog();
    assert_eq!(catalog.len(), 1);
    assert_eq!(catalog[0].end_record_index, None);
    assert_ne!(catalog[0].id, tail_id);
    assert_eq!(append_one(&mut recovered, b"first-after-empty").await, 0);

    let manifest = manifest_factory
        .replica(&format!("{prefix}/manifest"))
        .stat()
        .await
        .unwrap();
    assert_ne!(manifest.metadata.get("chorus.tail_id"), Some(&tail_id));
    assert_eq!(
        manifest.metadata.get("chorus.segments").map(String::as_str),
        Some("")
    );
}

#[tokio::test]
async fn recovery_retains_a_finalized_size_when_its_redundant_read_fails() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let prefix = "finalized-size-reread-failure-wal";
    let volume = volume(factories.clone(), manifest_factory.clone(), prefix);
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"r0").await;

    // r1 is acknowledged on B and C while A remains at the shorter prefix K.
    servers[0].service.set_crashed(true).await;
    append_one(&mut writer, b"r1").await;
    servers[0].service.set_crashed(false).await;
    let (tail_id, _) = manifest_frontier_ids(&manifest_factory, prefix).await;
    let tail_object = segment_object(prefix, &tail_id);
    drop(writer);

    // A prior partial recovery finalized B at L. The next recovery observes
    // that finalized snapshot successfully, then its old redundant size read
    // fails after all three takeover observations have been counted.
    let replica_b = factories[1].replica(&tail_object);
    let mut token_b = replica_b.takeover_current().await.unwrap();
    let size_b = token_b.persisted_size;
    replica_b.finalize(&mut token_b, size_b).await.unwrap();
    for server in &servers[..3] {
        server.service.reset_operation_counts().await;
    }
    servers[1]
        .service
        .inject_delay(Operation::Read, Duration::from_millis(100))
        .await;

    let recovering = {
        let volume = volume.clone();
        tokio::spawn(async move { recover_records(&volume, WalSeqNo::ZERO).await })
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        while servers[1].service.operation_count(Operation::Read).await == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("recovery never obtained B's first finalized snapshot");
    for _ in 0..=test_config().max_retries {
        servers[1]
            .service
            .inject(Operation::Get, Code::Unavailable)
            .await;
    }

    let (end, records) = recovering.await.unwrap();
    assert_eq!(end, WalSeqNo::record(2));
    assert_eq!(
        records
            .iter()
            .map(|record| record.payload.as_ref())
            .collect::<Vec<_>>(),
        vec![b"r0".as_slice(), b"r1".as_slice()]
    );
}

#[tokio::test]
async fn recovery_discards_pending_records_above_an_uncommitted_tail_gap() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let prefix = "pending-above-tail-gap-wal";
    let volume = volume(factories.clone(), manifest_factory.clone(), prefix);
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"r0").await;
    let (tail_id, pending_id) = manifest_frontier_ids(&manifest_factory, prefix).await;
    let tail_object = segment_object(prefix, &tail_id);
    let pending_object = segment_object(prefix, &pending_id);

    // A has durable bytes for a torn r1 below pending r2. With C unavailable,
    // recovery can still positively observe the old-tail hole through A while
    // B supplies the pending record. This is a gap, not simple replica lag.
    append_partial_raw_record(&factories[0], &tail_object, b"r1").await;
    append_raw_record(&factories[1], &pending_object, b"r2").await;
    append_raw_record(&factories[2], &pending_object, b"r2").await;
    drop(writer);
    servers[2].service.set_crashed(true).await;

    let (end, records) = recover_records(&volume, WalSeqNo::ZERO).await;
    assert_eq!(end, WalSeqNo::record(1));
    assert_eq!(
        records
            .iter()
            .map(|record| record.payload.as_ref())
            .collect::<Vec<_>>(),
        vec![b"r0".as_slice()]
    );
}

#[tokio::test]
async fn failed_gap_discard_fold_preserves_tail_evidence_for_retry() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let prefix = "pending-gap-fold-retry-wal";
    let volume = volume(factories.clone(), manifest_factory.clone(), prefix);
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"r0").await;
    let (tail_id, pending_id) = manifest_frontier_ids(&manifest_factory, prefix).await;
    let tail_object = segment_object(prefix, &tail_id);
    let pending_object = segment_object(prefix, &pending_id);

    append_raw_record(&factories[0], &tail_object, b"r1").await;
    append_raw_record(&factories[1], &pending_object, b"r2").await;
    append_raw_record(&factories[2], &pending_object, b"r2").await;
    drop(writer);

    // Pause after the recovery claim, then fail every fold CAS attempt. The
    // old-tail suffix is the durable evidence that pending is speculative, so
    // recovery must not truncate it before the manifest excludes pending.
    // Synchronize on the candidate takeover: it runs on every zone after the
    // epoch claim and before the fold, independent of which single zone the
    // canonical content is later read from.
    servers[2]
        .service
        .inject_delay(Operation::BidiTakeoverOpen, Duration::from_millis(100))
        .await;
    let first_recovery = {
        let volume = volume.clone();
        tokio::spawn(async move { volume.recover(WalSeqNo::ZERO).await })
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        while servers[2]
            .service
            .operation_count(Operation::BidiTakeoverOpen)
            .await
            == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("recovery did not reach the delayed candidate takeover");
    // Manifest transforms have a separate bounded CAS loop. Queue more faults
    // than that loop can consume, then clear the remainder before retrying.
    for _ in 0..32 {
        servers[3]
            .service
            .inject(Operation::Update, Code::Unavailable)
            .await;
    }
    assert!(
        first_recovery.await.unwrap().is_err(),
        "the injected manifest fold failure unexpectedly succeeded"
    );
    servers[3]
        .service
        .clear_injected_operation(Operation::Update)
        .await;

    let (end, records) = recover_records(&volume, WalSeqNo::ZERO).await;
    assert_eq!(end, WalSeqNo::record(1));
    assert_eq!(
        records
            .iter()
            .map(|record| record.payload.as_ref())
            .collect::<Vec<_>>(),
        vec![b"r0".as_slice()]
    );
}

#[tokio::test]
async fn ambiguous_not_found_takeover_cannot_retire_a_manifest_tail() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let prefix = "ambiguous-not-found-tail-wal";
    let volume = volume(factories, manifest_factory.clone(), prefix);
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"acked").await;
    let (tail_id, _) = manifest_frontier_ids(&manifest_factory, prefix).await;
    drop(writer);

    // The object exists and carries acknowledged data, but the takeover
    // selector reports NOT_FOUND. Recovery must treat this as ambiguous rather
    // than quorum-confirmed absence and must not replace the manifest tail.
    for server in &servers[..3] {
        server
            .service
            .inject(Operation::BidiTakeoverOpen, Code::NotFound)
            .await;
    }
    let recovery = volume.recover(WalSeqNo::ZERO).await;
    let mut takeover_counts = Vec::new();
    for server in &servers[..3] {
        takeover_counts.push(
            server
                .service
                .operation_count(Operation::BidiTakeoverOpen)
                .await,
        );
    }
    assert!(
        recovery.is_err(),
        "ambiguous takeover unexpectedly recovered; takeover counts {takeover_counts:?}"
    );

    let manifest = manifest_factory
        .replica(&format!("{prefix}/manifest"))
        .stat()
        .await
        .unwrap();
    assert_eq!(
        manifest.metadata.get("chorus.tail_id"),
        Some(&tail_id),
        "ambiguous takeover replaced the manifest-named tail"
    );
}

#[tokio::test]
async fn pending_exhaustion_backpressures_until_refill_is_registered() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory, "pending-exhaustion-wal");
    let writer = volume.recover_writer().await.unwrap();
    for server in &servers[..3] {
        server.service.inject_open_hold().await;
    }
    let mut handle = WalEngine::start(
        writer,
        WalEngineConfig {
            max_segment_bytes: 1,
            repair_interval: None,
            ..Default::default()
        },
    )
    .unwrap();

    handle
        .enqueue_append(WalSeqNo::ZERO, bytes::Bytes::from_static(b"old-tail"))
        .await
        .unwrap()
        .await
        .unwrap();
    handle
        .enqueue_append(
            WalSeqNo::record(1),
            bytes::Bytes::from_static(b"pending-tail"),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    let blocked = handle
        .enqueue_append(
            WalSeqNo::record(2),
            bytes::Bytes::from_static(b"after-refill"),
        )
        .await
        .unwrap();
    tokio::pin!(blocked);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), blocked.as_mut())
            .await
            .is_err(),
        "append escaped while the single pending slot was exhausted"
    );

    for server in &servers[..3] {
        server.service.release_open_holds().await;
    }
    blocked.await.unwrap();
    shutdown_engine(handle).await;
}

#[tokio::test]
async fn rotation_targets_repair_at_a_copy_missing_finalization() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let (volume, metrics) = volume_with_metrics(
        factories.clone(),
        manifest_factory.clone(),
        "targeted-finalize-repair-wal",
    );
    let mut recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    while recovery.try_next().await.unwrap().is_some() {}
    let mut handle = recovery
        .start(WalEngineConfig {
            max_segment_bytes: 1,
            repair_interval: None,
            ..Default::default()
        })
        .await
        .unwrap();

    // A crashed zone makes the rotation seal finalize only a quorum: the
    // engine must schedule a targeted repair pass for the one degraded
    // copy (which records a transient skip while the zone stays down).
    servers[2].service.set_crashed(true).await;
    handle
        .enqueue_append(WalSeqNo::ZERO, bytes::Bytes::from_static(b"sealed"))
        .await
        .unwrap()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while metrics.counter("chorus.wal.repair.passes") < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the degraded rotation never scheduled its targeted repair pass");
    assert!(metrics.counter("chorus.wal.repair.transient_skips") >= 1);
    assert_eq!(metrics.counter("chorus.wal.seal.segments"), 1);

    servers[2].service.set_crashed(false).await;
    shutdown_engine(handle).await;
}

#[tokio::test]
async fn transient_seal_enforcement_failure_retries_without_gating_rotation() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let metrics = Arc::new(TestMetricsRecorder::default());
    let metrics_recorder: Arc<dyn crate::MetricsRecorder> = metrics.clone();
    // The shared test config has zero backoff, which can exhaust all outer
    // maintenance retries before this task observes the first retry counter.
    // A small test-only delay leaves the retry in flight while quorum returns.
    let volume = SegmentedVolume::new_with_factories_and_metrics_recorder(
        factories,
        manifest_factory,
        "retry-seal-enforcement-wal",
        ClientConfig {
            retry_base: Duration::from_millis(100),
            ..test_config()
        },
        metrics_recorder,
    )
    .expect("test clusters use a supported replica count");
    let writer = volume.recover_writer().await.unwrap();

    servers[3].service.reset_operation_counts().await;
    servers[3]
        .service
        .inject_delay(Operation::Update, Duration::from_millis(500))
        .await;
    let mut handle = WalEngine::start(
        writer,
        WalEngineConfig {
            max_segment_bytes: 1,
            repair_interval: None,
            ..Default::default()
        },
    )
    .unwrap();
    handle
        .enqueue_append(WalSeqNo::ZERO, bytes::Bytes::from_static(b"first"))
        .await
        .unwrap()
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        while servers[3].service.operation_count(Operation::Update).await == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background fold did not reach the manifest update");
    for zone in [0usize, 1] {
        servers[zone].service.set_crashed(true).await;
    }

    wait_for_counter(&metrics, "chorus.wal.seal.enforcement_retries", 1).await;
    for zone in [0usize, 1] {
        servers[zone].service.set_crashed(false).await;
    }
    wait_for_counter(&metrics, "chorus.wal.seal.segments", 1).await;

    // A dropped enforcement sender would put the engine in Rotation::Disabled:
    // this record would commit into the successor but never produce another
    // seal. Observing the second seal proves the transient failure did not
    // become a gate-until-restart availability cliff.
    handle
        .enqueue_append(WalSeqNo::record(1), bytes::Bytes::from_static(b"second"))
        .await
        .unwrap()
        .await
        .unwrap();
    wait_for_counter(&metrics, "chorus.wal.seal.segments", 2).await;
    shutdown_engine(handle).await;
}

#[tokio::test]
async fn recorder_receives_write_and_seal_metrics() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let (volume, metrics) =
        volume_with_metrics(factories, manifest_factory, "metrics-write-seal-wal");
    let mut recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    while recovery.try_next().await.unwrap().is_some() {}
    let mut handle = recovery
        .start(WalEngineConfig {
            max_segment_bytes: 1,
            repair_interval: None,
            ..Default::default()
        })
        .await
        .unwrap();
    handle
        .enqueue_append(WalSeqNo::ZERO, bytes::Bytes::from_static(b"sealed"))
        .await
        .unwrap()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while metrics.counter("chorus.wal.seal.segments") == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("automatic rotation did not seal a segment");

    assert_eq!(metrics.counter("chorus.wal.append.records"), 1);
    assert_eq!(metrics.counter("chorus.wal.append.bytes"), 6);
    assert_eq!(metrics.counter("chorus.wal.append.committed_records"), 1);
    assert_eq!(metrics.gauge("chorus.wal.append.committed_watermark"), 1);
    assert_eq!(metrics.counter("chorus.wal.batch.sent"), 1);
    assert!(
        metrics.counter("chorus.wal.append.encoded_bytes")
            > metrics.counter("chorus.wal.append.bytes")
    );
    assert_eq!(metrics.counter("chorus.wal.rotation.completed"), 1);
    assert_eq!(metrics.counter("chorus.wal.seal.segments"), 1);
    assert!(metrics.counter("chorus.wal.manifest.cas_attempts") >= 2);
    assert_eq!(metrics.counter("chorus.wal.repair.passes"), 1);
    assert_eq!(
        metrics.up_down_counter("chorus.wal.catalog.open_segments"),
        1
    );
    assert_eq!(
        metrics.histogram_samples("chorus.wal.append.commit_latency_seconds"),
        1
    );
    assert_eq!(
        metrics.histogram_samples("chorus.wal.manifest.cas_latency_seconds"),
        metrics.counter("chorus.wal.manifest.cas_attempts") as usize
    );
    assert_eq!(
        metrics.histogram_samples("chorus.wal.seal.duration_seconds"),
        1
    );
    shutdown_engine(handle).await;
}

#[tokio::test]
async fn indeterminate_record_poisons_and_closes_the_engine() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory.clone(), "poison-wal");
    let mut handle = WalEngine::start(
        volume.recover_writer().await.unwrap(),
        WalEngineConfig {
            pipeline_window_records: 3,
            ..Default::default()
        },
    )
    .unwrap();
    servers[1].service.set_crashed(true).await;
    servers[2].service.set_crashed(true).await;
    let mut completions = Vec::new();
    for seqno in 0..8 {
        completions.push(
            handle
                .enqueue_append(
                    WalSeqNo::record(seqno),
                    bytes::Bytes::from(format!("record-{seqno}")),
                )
                .await
                .unwrap(),
        );
    }
    for completion in completions {
        assert!(matches!(completion.await, Err(Error::Poisoned)));
    }
    assert!(matches!(
        handle
            .enqueue_append(
                WalSeqNo::record(8),
                bytes::Bytes::from_static(b"after-poison"),
            )
            .await,
        Err(Error::Closed)
    ));
}

#[tokio::test]
async fn graceful_shutdown_consumes_handle_and_drains_accepted_work() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory.clone(), "shutdown-wal");
    let mut handle = WalEngine::start(
        volume.recover_writer().await.unwrap(),
        WalEngineConfig::default(),
    )
    .unwrap();
    servers[2]
        .service
        .inject_delay(Operation::BidiWrite, Duration::from_millis(50))
        .await;
    let dropped = handle
        .enqueue_append(WalSeqNo::ZERO, bytes::Bytes::from_static(b"accepted"))
        .await
        .unwrap();
    drop(dropped);
    let accepted = handle
        .enqueue_append(
            WalSeqNo::record(1),
            bytes::Bytes::from_static(b"also-accepted"),
        )
        .await
        .unwrap();
    shutdown_engine(handle).await;
    assert!(accepted.await.is_ok());
}

#[tokio::test]
async fn shutdown_leaves_no_owned_tasks_running() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let prefix = "shutdown-owned-tasks-wal";
    let volume = volume(factories.clone(), manifest_factory, prefix);
    let writer = volume.recover_writer().await.unwrap();

    // Keep maintenance and post-swap refill creation busy. Shutdown must
    // cancel both and join every owned task instead of waiting for the injected
    // storage delays or detaching work.
    servers[3]
        .service
        .inject_delay(Operation::Get, Duration::from_millis(150))
        .await;
    for server in &servers[..3] {
        server.service.reset_operation_counts().await;
        server.service.inject_open_hold().await;
    }

    let handle = WalEngine::start(
        writer,
        WalEngineConfig {
            max_segment_bytes: 1,
            repair_interval: None,
            ..Default::default()
        },
    )
    .unwrap();
    let mut handle = handle;
    handle
        .enqueue_append(WalSeqNo::ZERO, bytes::Bytes::from_static(b"rotate"))
        .await
        .unwrap()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let mut provisioning_started = true;
            for server in &servers[..3] {
                provisioning_started &=
                    server.service.operation_count(Operation::BidiWrite).await > 0;
            }
            if provisioning_started && servers[3].service.operation_count(Operation::Get).await > 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned tasks never entered their delayed storage operations");

    tokio::time::timeout(Duration::from_secs(2), handle.shutdown())
        .await
        .expect("shutdown did not cancel and join the provisioner")
        .unwrap();

    for server in &servers[..3] {
        server.service.release_open_holds().await;
    }
    tokio::time::sleep(Duration::from_millis(550)).await;
    for factory in &factories {
        assert_eq!(
            factory
                .list(&format!("{prefix}/segments/"))
                .await
                .unwrap()
                .len(),
            2,
            "a detached provisioner created the held refill after shutdown"
        );
    }
}

#[tokio::test]
async fn shutdown_deadline_aborts_a_stuck_append_pipeline() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory, "shutdown-deadline-wal");
    let mut handle = WalEngine::start(
        volume.recover_writer().await.unwrap(),
        WalEngineConfig {
            repair_interval: None,
            shutdown_timeout: Duration::from_millis(50),
            ..Default::default()
        },
    )
    .unwrap();

    for server in &servers[..3] {
        server
            .service
            .inject_delay(Operation::BidiAppendFlush, Duration::from_secs(5))
            .await;
    }
    let completion = handle
        .enqueue_append(WalSeqNo::ZERO, bytes::Bytes::from_static(b"stuck"))
        .await
        .unwrap();
    drop(completion);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let mut delayed = 0usize;
            for server in &servers[..3] {
                delayed += usize::from(
                    server
                        .service
                        .operation_count(Operation::BidiAppendFlush)
                        .await
                        > 0,
                );
            }
            if delayed >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("append lanes never entered the delayed flushes");

    let error = tokio::time::timeout(Duration::from_secs(1), handle.shutdown())
        .await
        .expect("shutdown exceeded its configured deadline and cleanup bound")
        .unwrap_err();
    assert!(
        matches!(error, Error::ShutdownTimeout { timeout } if timeout == Duration::from_millis(50))
    );
}
