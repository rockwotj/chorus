use super::*;

#[tokio::test]
async fn recovery_does_not_promote_a_tail_below_the_second_largest_size() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory.clone(), "promote-wal");
    let mut first = volume.recover_writer().await.unwrap();
    servers[1].service.set_crashed(true).await;
    servers[2].service.set_crashed(true).await;
    let pending = first
        .enqueue_records(vec![record(b"maybe-committed")], no_attempted_bytes())
        .await
        .unwrap()
        .remove(0);
    assert!(pending.wait().await.is_err());
    drop(first);

    servers[1].service.set_crashed(false).await;
    servers[2].service.set_crashed(false).await;
    let (end, records) = recover_records(&volume, WalSeqNo::ZERO).await;
    assert_eq!(end, WalSeqNo::ZERO);
    assert!(records.is_empty());
}

#[tokio::test]
async fn recovery_with_one_zone_down_preserves_committed_data() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    servers[2].service.set_crashed(true).await;
    let volume = volume(factories, manifest_factory.clone(), "two-zone-recovery-wal");
    let mut first = volume.recover_writer().await.unwrap();
    append_one(&mut first, b"committed").await;
    drop(first);

    servers[0].service.set_crashed(true).await;
    servers[2].service.set_crashed(false).await;
    let (_, records) = recover_records(&volume, WalSeqNo::ZERO).await;
    assert_eq!(records[0].payload, b"committed".as_slice());
}

#[tokio::test]
async fn recovery_ignores_one_corrupt_open_lane() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories.clone(),
        manifest_factory.clone(),
        "corrupt-recovery-wal",
    );
    let mut first = volume.recover_writer().await.unwrap();
    append_one(&mut first, b"committed").await;
    let object =
        active_segment_object(&factories[0], &manifest_factory, "corrupt-recovery-wal").await;
    assert!(
        servers[0]
            .service
            .corrupt_byte_for("projects/_/buckets/zone-0", &object, 0,)
            .await
    );
    drop(first);

    let (_, records) = recover_records(&volume, WalSeqNo::ZERO).await;
    assert_eq!(records[0].payload, b"committed".as_slice());
    let healed = factories[0].replica(&object).snapshot().await.unwrap();
    assert_eq!(
        RecordFrame::decode_all(&healed.bytes).unwrap()[0].payload,
        b"committed".as_slice()
    );
}

#[tokio::test]
async fn takeover_seals_old_segment_and_deposes_old_writer() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory.clone(), "takeover-wal");
    let mut first = volume.recover_writer().await.unwrap();
    append_one(&mut first, b"committed").await;

    let recovered = volume.recover_writer().await.unwrap();
    assert_eq!(recovered.active_segment_base(), 1);
    let stale = first
        .enqueue_records(vec![record(b"stale")], no_attempted_bytes())
        .await;
    let error = match stale {
        Ok(mut pending) => pending.remove(0).wait().await.unwrap_err(),
        Err(error) => error,
    };
    assert!(matches!(error, Error::Fenced(_)), "{error}");
}

#[tokio::test]
async fn manifest_takeover_fences_background_pending_fold() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories,
        manifest_factory,
        "pending-swap-manifest-fence-wal",
    );
    let mut stale = volume.recover_writer().await.unwrap();
    assert_eq!(append_one(&mut stale, b"committed-before-fence").await, 0);

    assert!(stale.spare_ready());
    let swap = stale.begin_swap().await.unwrap().unwrap();
    swap.writer.as_ref().unwrap().poison();
    assert!(swap.writer.as_ref().unwrap().is_poisoned());

    // Recovery claims the manifest epoch before its zonal takeovers. The
    // stale background worker can no longer fold the old tail or register a
    // refill after that claim.
    let mut replacement = volume.recover_writer().await.unwrap();
    let parts = stale.provision_parts().unwrap();
    let manifest = parts.manifest.clone();
    let refill_id = stale.next_segment_id();
    let (refill_id, refill) = crate::segment::provision_spare(
        parts.factories,
        parts.prefix,
        parts.client_config,
        parts.max_replica_lag_bytes,
        parts.lane_stall_timeout,
        refill_id,
        parts.metrics,
    )
    .await
    .unwrap();
    stale.adopt_unregistered_spare(refill_id, refill);
    let fold = stale.pending_fold_request(&swap).unwrap();
    let error = crate::segment::fold_registered_pending(manifest, fold)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Fenced(_)), "{error}");

    let stale_error = match stale
        .enqueue_records(vec![record(b"stale-successor")], no_attempted_bytes())
        .await
    {
        Ok(mut pending) => pending.remove(0).wait().await.unwrap_err(),
        Err(error) => error,
    };
    assert!(
        matches!(stale_error, Error::Poisoned | Error::Fenced(_)),
        "{stale_error}"
    );
    assert_eq!(append_one(&mut replacement, b"new-owner").await, 1);
}

#[tokio::test]
async fn recovery_derives_the_end_of_a_finalized_highest_segment() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories.clone(),
        manifest_factory.clone(),
        "finalized-highest-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"sealed-highest").await;
    writer.rotate().await.unwrap();
    let active_id = writer
        .catalog()
        .iter()
        .find(|segment| segment.base_record_index == 1)
        .unwrap()
        .id
        .clone();
    drop(writer);

    let active = segment_object("finalized-highest-wal", &active_id);
    for factory in &factories {
        let replica = factory.replica(&active);
        let generation = replica.snapshot().await.unwrap().generation;
        replica.delete(generation).await.unwrap();
    }

    let recovered = volume.recover_writer().await.unwrap();
    assert_eq!(recovered.active_segment_base(), 1);
    assert_eq!(recovered.catalog()[0].end_record_index, Some(0));
}

#[tokio::test]
async fn recovery_completes_a_partial_seal_with_only_two_zones_reachable() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory.clone(), "partial-seal-wal");
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"survives-partial-seal").await;
    servers[1].service.set_crashed(true).await;
    servers[2].service.set_crashed(true).await;
    assert!(writer.rotate().await.is_err());
    drop(writer);

    servers[1].service.set_crashed(false).await;
    let recovered = volume.recover_writer().await.unwrap();
    assert_eq!(recovered.active_segment_base(), 1);
    assert_eq!(recovered.catalog()[0].end_record_index, Some(0));
}

#[tokio::test]
async fn recovery_does_not_reject_a_noncanonical_witness_crc32c() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories.clone(),
        manifest_factory.clone(),
        "witness-crc-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"first").await;
    append_one(&mut writer, b"second").await;
    writer.rotate().await.unwrap();
    let segment = writer.catalog()[0].clone();
    let object = segment_object("witness-crc-wal", &segment.id);

    let replica = factories[0].replica(&object);
    let observed = replica.snapshot().await.unwrap();
    let records = RecordFrame::decode_all(&observed.bytes).unwrap();
    let short = records[0].encode().unwrap();
    let mut token = replica
        .replace_appendable(
            &observed,
            short.clone(),
            crate::protocol::protocol_metadata(),
        )
        .await
        .unwrap();
    let divergent = replica
        .finalize(&mut token, short.len() as i64)
        .await
        .unwrap();
    assert!(divergent.finalized);
    assert_ne!(divergent.crc32c, segment.crc32c);
    drop(writer);

    let recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    let records = recovery.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.payload.as_ref())
            .collect::<Vec<_>>(),
        vec![b"first".as_slice(), b"second".as_slice()]
    );
}

#[tokio::test]
async fn recovery_repairs_a_lagging_prior_segment_before_the_active_segment() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory.clone(), "lagging-seal-wal");
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"first").await;
    servers[2].service.set_crashed(true).await;
    writer.rotate().await.unwrap();
    append_one(&mut writer, b"second").await;
    drop(writer);

    servers[2].service.set_crashed(false).await;
    servers[1].service.set_crashed(true).await;
    let (end, records) = recover_records(&volume, WalSeqNo::ZERO).await;
    assert_eq!(end, WalSeqNo::record(2));
    assert_eq!(
        records
            .iter()
            .map(|record| record.payload.as_ref())
            .collect::<Vec<_>>(),
        vec![b"first".as_slice(), b"second".as_slice()]
    );
}

#[tokio::test]
async fn recovery_adopts_a_historical_segment_without_reading_it() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories.clone(),
        manifest_factory.clone(),
        "startup-historical-adopt-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"historical").await;
    writer.rotate().await.unwrap();
    append_one(&mut writer, b"current-seal").await;
    writer.rotate().await.unwrap();

    let historical = writer.catalog()[0].clone();
    assert_ne!(
        Some(historical.id.as_str()),
        writer.catalog().last().map(|segment| segment.id.as_str()),
        "the test must damage an older directory entry, not the current seal"
    );
    // Every copy of the oldest entry disappears. The committed directory still
    // names its range and CRC32C, so recovery has everything it needs to adopt
    // the chain; a startup that consulted storage instead could not proceed.
    let object = segment_object("startup-historical-adopt-wal", &historical.id);
    for factory in &factories {
        let replica = factory.replica(&object);
        let generation = replica.snapshot().await.unwrap().generation;
        replica.delete(generation).await.unwrap();
    }
    drop(writer);

    let recovered = volume
        .recover_writer_from(WalSeqNo::record(2))
        .await
        .unwrap();
    assert_eq!(recovered.active_segment_base(), 2);
    assert_eq!(
        recovered.catalog()[0].id,
        historical.id,
        "the unreadable entry stays in the chain for background repair"
    );
}

#[tokio::test]
async fn maintenance_restores_a_historical_segment_recovery_left_damaged() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let (volume, metrics) = volume_with_metrics(
        factories.clone(),
        manifest_factory.clone(),
        "startup-historical-repair-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"historical").await;
    writer.rotate().await.unwrap();
    append_one(&mut writer, b"current-seal").await;
    writer.rotate().await.unwrap();

    let historical = writer.catalog()[0].clone();
    let object = segment_object("startup-historical-repair-wal", &historical.id);
    let missing = factories[1].replica(&object);
    let generation = missing.snapshot().await.unwrap().generation;
    missing.delete(generation).await.unwrap();
    drop(writer);

    let recovered = volume.recover_writer().await.unwrap();
    assert_eq!(recovered.active_segment_base(), 2);
    assert_eq!(
        missing.snapshot().await.unwrap_err().code,
        TransportCode::NotFound,
        "recovery must not spend startup time restoring sealed history"
    );

    let handle = WalEngine::start(
        recovered,
        WalEngineConfig {
            repair_interval: None,
            ..Default::default()
        },
    )
    .unwrap();
    wait_for_repair_passes(&metrics, 1).await;
    assert_eq!(metrics.counter("chorus.wal.repair.objects_repaired"), 1);
    assert_eq!(
        metrics.counter("chorus.wal.repair.segments_without_source"),
        0
    );

    let repaired = missing.snapshot().await.unwrap();
    assert!(repaired.finalized);
    assert_eq!(repaired.crc32c, historical.crc32c);
    assert_eq!(crc32c::crc32c(&repaired.bytes), historical.crc32c.unwrap());
    shutdown_engine(handle).await;
}

#[tokio::test]
async fn replay_reads_past_a_rotted_historical_copy() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories.clone(),
        manifest_factory.clone(),
        "startup-historical-rot-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"historical").await;
    writer.rotate().await.unwrap();
    append_one(&mut writer, b"current-seal").await;
    writer.rotate().await.unwrap();

    let historical = writer.catalog()[0].clone();
    let object = segment_object("startup-historical-rot-wal", &historical.id);
    // Rot leaves the recorded checksum intact and fails the content read, so no
    // listing or stat can see it. The read path is what notices: it verifies
    // every copy it consumes against the committed CRC32C and takes the next
    // one.
    assert!(
        servers[1]
            .service
            .corrupt_byte_for("projects/_/buckets/zone-1", &object, 0)
            .await
    );
    drop(writer);

    let (end, records) = recover_records(&volume, WalSeqNo::ZERO).await;
    assert_eq!(end, WalSeqNo::record(2));
    assert_eq!(records[0].payload, b"historical".as_slice());
    assert_eq!(records[1].payload, b"current-seal".as_slice());
}

#[tokio::test]
async fn rejoined_zone_receives_only_immutable_sealed_segment_repair() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    servers[2].service.set_crashed(true).await;
    let volume = volume(
        factories.clone(),
        manifest_factory.clone(),
        "sealed-repair-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"sealed-copy").await;
    writer.rotate().await.unwrap();
    assert_eq!(writer.active_segment_base(), 1);
    let sealed_object = segment_object("sealed-repair-wal", &writer.catalog()[0].id);

    servers[2].service.set_crashed(false).await;
    let report = writer.repair_sealed_segments().await.unwrap();
    assert_eq!(report.objects_repaired, 1);
    let repaired = factories[2]
        .replica(&sealed_object)
        .snapshot()
        .await
        .unwrap();
    assert!(repaired.finalized);
    assert_eq!(RecordFrame::decode_all(&repaired.bytes).unwrap().len(), 1);
    assert!(
        segment_objects_for_base(&factories[2], &manifest_factory, "sealed-repair-wal", 1)
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn repair_uses_the_listing_crc32c_to_target_same_size_divergence() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories.clone(),
        manifest_factory.clone(),
        "crc-repair-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"sealed-copy").await;
    writer.rotate().await.unwrap();
    let segment = writer.catalog()[0].clone();
    let sealed_object = segment_object("crc-repair-wal", &segment.id);
    let canonical = factories[0]
        .replica(&sealed_object)
        .snapshot()
        .await
        .unwrap();
    assert_eq!(canonical.crc32c, segment.crc32c);
    assert!(
        servers[2]
            .service
            .diverge_byte_for(
                "projects/_/buckets/zone-2",
                &sealed_object,
                canonical.bytes.len() - 1,
            )
            .await
    );
    let divergent = factories[2].replica(&sealed_object).stat().await.unwrap();
    assert_ne!(divergent.crc32c, segment.crc32c);
    assert_eq!(divergent.persisted_size, canonical.persisted_size);

    for server in &servers[..3] {
        server.service.reset_operation_counts().await;
    }
    let report = writer.repair_sealed_segments().await.unwrap();
    assert_eq!(report.objects_repaired, 1);
    assert_eq!(report.objects_already_healthy, 2);
    assert_eq!(servers[0].service.operation_count(Operation::Read).await, 1);
    assert_eq!(servers[1].service.operation_count(Operation::Read).await, 0);
    assert!(servers[2].service.operation_count(Operation::Read).await >= 1);
    // One listing per zone answers the health question for the whole chain, so
    // an untouched copy costs no per-object metadata read at all.
    for server in &servers[..3] {
        assert_eq!(server.service.operation_count(Operation::List).await, 1);
    }
    assert_eq!(servers[1].service.operation_count(Operation::Get).await, 0);

    let repaired = factories[2]
        .replica(&sealed_object)
        .snapshot()
        .await
        .unwrap();
    assert_eq!(repaired.bytes, canonical.bytes);
    assert_eq!(repaired.crc32c, segment.crc32c);
}

#[tokio::test]
async fn repair_lists_each_zone_once_regardless_of_retained_history() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories.clone(),
        manifest_factory.clone(),
        "repair-listing-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    for payload in [
        b"first".as_slice(),
        b"second".as_slice(),
        b"third".as_slice(),
    ] {
        append_one(&mut writer, payload).await;
        writer.rotate().await.unwrap();
    }
    // three sealed entries plus the open frontier the last rotation installed
    assert_eq!(writer.catalog().len(), 4);
    let damaged = writer.catalog()[1].clone();
    let object = segment_object("repair-listing-wal", &damaged.id);
    let missing = factories[2].replica(&object);
    let generation = missing.snapshot().await.unwrap().generation;
    missing.delete(generation).await.unwrap();

    for server in &servers[..3] {
        server.service.reset_operation_counts().await;
    }
    let report = writer.repair_sealed_segments().await.unwrap();
    assert_eq!(report.segments_examined, 3);
    assert_eq!(report.objects_repaired, 1);
    assert_eq!(report.objects_already_healthy, 8);
    assert_eq!(report.segments_without_source, 0);
    for server in &servers[..3] {
        assert_eq!(server.service.operation_count(Operation::List).await, 1);
    }
    // Only the one segment that needs a source is read at all, and a zone whose
    // copies are all intact answers no per-object question whatsoever.
    assert_eq!(servers[0].service.operation_count(Operation::Read).await, 1);
    assert_eq!(servers[1].service.operation_count(Operation::Read).await, 0);
    assert_eq!(servers[1].service.operation_count(Operation::Get).await, 0);
}

#[tokio::test]
async fn repair_reports_a_segment_with_no_verifiable_copy_and_keeps_going() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories.clone(),
        manifest_factory.clone(),
        "repair-lost-segment-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    for payload in [b"lost".as_slice(), b"kept".as_slice()] {
        append_one(&mut writer, payload).await;
        writer.rotate().await.unwrap();
    }
    let lost = writer.catalog()[0].clone();
    let kept = writer.catalog()[1].clone();
    let lost_object = segment_object("repair-lost-segment-wal", &lost.id);
    for factory in &factories {
        let replica = factory.replica(&lost_object);
        let generation = replica.snapshot().await.unwrap().generation;
        replica.delete(generation).await.unwrap();
    }
    let kept_object = segment_object("repair-lost-segment-wal", &kept.id);
    let missing = factories[2].replica(&kept_object);
    let generation = missing.snapshot().await.unwrap().generation;
    missing.delete(generation).await.unwrap();

    // Nothing else inspects sealed history, so an unrecoverable entry must be
    // counted rather than swallowed, and must not stop the entries after it
    // from being restored.
    let report = writer.repair_sealed_segments().await.unwrap();
    assert_eq!(report.segments_without_source, 1);
    assert_eq!(report.objects_repaired, 1);
    assert!(missing.snapshot().await.unwrap().finalized);
}

/// Maintenance runs on its own task; poll until `minimum` passes complete.
/// The wait is wall-clock-bounded, not iteration-bounded: the pass makes
/// real loopback RPCs, so a slow machine needs time, not yields.
#[tokio::test]
async fn engine_start_repairs_rejoined_zone_and_reports_metrics() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    servers[2].service.set_crashed(true).await;
    let (volume, metrics) = volume_with_metrics(
        factories.clone(),
        manifest_factory.clone(),
        "engine-start-repair-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"sealed-copy").await;
    writer.rotate().await.unwrap();
    let sealed_object = segment_object("engine-start-repair-wal", &writer.catalog()[0].id);

    servers[2].service.set_crashed(false).await;
    let mut handle = WalEngine::start(
        writer,
        WalEngineConfig {
            repair_interval: None,
            ..Default::default()
        },
    )
    .unwrap();
    handle
        .enqueue_append(
            WalSeqNo::record(1),
            bytes::Bytes::from_static(b"active-copy"),
        )
        .await
        .unwrap()
        .await
        .unwrap();

    wait_for_repair_passes(&metrics, 1).await;
    assert_eq!(metrics.counter("chorus.wal.repair.passes"), 1);
    assert_eq!(metrics.counter("chorus.wal.repair.objects_repaired"), 1);
    assert_eq!(metrics.counter("chorus.wal.repair.failures"), 0);
    let repaired = factories[2]
        .replica(&sealed_object)
        .snapshot()
        .await
        .unwrap();
    assert!(repaired.finalized);
    shutdown_engine(handle).await;
}

#[tokio::test]
async fn repair_pass_failure_does_not_fail_appends() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let (volume, metrics) = volume_with_metrics(factories, manifest_factory, "repair-failure-wal");
    let writer = volume.recover_writer().await.unwrap();
    servers[3].service.set_crashed(true).await;
    let mut handle = WalEngine::start(
        writer,
        WalEngineConfig {
            repair_interval: None,
            ..Default::default()
        },
    )
    .unwrap();

    handle
        .enqueue_append(WalSeqNo::ZERO, bytes::Bytes::from_static(b"commits"))
        .await
        .unwrap()
        .await
        .unwrap();
    wait_for_repair_passes(&metrics, 1).await;
    assert_eq!(metrics.counter("chorus.wal.repair.passes"), 1);
    assert_eq!(metrics.counter("chorus.wal.repair.failures"), 1);
    assert_eq!(metrics.counter("chorus.wal.append.committed_records"), 1);
    shutdown_engine(handle).await;
}

#[tokio::test]
async fn rotation_and_truncation_use_only_segment_objects() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories.clone(), manifest_factory.clone(), "truncate-wal");
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"old").await;
    writer.rotate().await.unwrap();
    let sealed_id = writer.catalog()[0].id.clone();
    let sealed_object = segment_object("truncate-wal", &sealed_id);
    for server in &servers {
        server.service.reset_operation_counts().await;
    }
    let report = writer.truncate_before(WalSeqNo::record(1)).await.unwrap();
    assert_eq!(report.deleted_segments, 1);
    for server in &servers[..3] {
        assert_eq!(server.service.operation_count(Operation::Get).await, 1);
        assert_eq!(server.service.operation_count(Operation::Read).await, 0);
        assert_eq!(server.service.operation_count(Operation::Delete).await, 1);
    }
    drop(writer);
    assert!(matches!(
        volume.recover(WalSeqNo::ZERO).await,
        Err(Error::InvalidCatalog(_))
    ));
    let mut recovery = volume.recover_from_committed_floor().await.unwrap();
    assert_eq!(recovery.from, WalSeqNo::record(1));
    assert_eq!(recovery.end, WalSeqNo::record(1));
    assert!(recovery.try_next().await.unwrap().is_none());
    for factory in &factories {
        assert_eq!(
            factory
                .replica(&sealed_object)
                .snapshot()
                .await
                .unwrap_err()
                .code,
            TransportCode::NotFound
        );
    }
}

#[tokio::test]
async fn checkpoint_aware_recovery_ignores_returning_old_copies() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories.clone(),
        manifest_factory.clone(),
        "checkpoint-floor-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    for payload in [b"zero".as_slice(), b"one".as_slice()] {
        append_one(&mut writer, payload).await;
        writer.rotate().await.unwrap();
    }
    let old_objects: Vec<_> = writer
        .catalog()
        .iter()
        .filter(|segment| segment.base_record_index < 2)
        .map(|segment| segment_object("checkpoint-floor-wal", &segment.id))
        .collect();

    servers[2].service.set_crashed(true).await;
    writer.truncate_before(WalSeqNo::record(2)).await.unwrap();
    drop(writer);
    servers[2].service.set_crashed(false).await;
    servers[0].service.set_crashed(true).await;

    let mut recovered = volume
        .recover_writer_from(WalSeqNo::record(2))
        .await
        .unwrap();
    assert_eq!(recovered.active_segment_base(), 2);
    assert_eq!(recovered.checkpoint_floor(), 2);
    recovered
        .truncate_before(WalSeqNo::record(2))
        .await
        .unwrap();
    for object in old_objects {
        assert_eq!(
            factories[2]
                .replica(&object)
                .snapshot()
                .await
                .unwrap_err()
                .code,
            TransportCode::NotFound
        );
    }
}

#[tokio::test]
async fn periodic_maintenance_sweeps_returned_zone_tombstones_without_new_truncation() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let (volume, metrics) = volume_with_metrics(
        factories.clone(),
        manifest_factory.clone(),
        "periodic-tombstone-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"old").await;
    writer.rotate().await.unwrap();
    let sealed = writer.catalog()[0].clone();
    let object = segment_object("periodic-tombstone-wal", &sealed.id);

    // The floor commits and the reachable quorum deletes its copies, but zone
    // 2 sleeps through the pass. Its directory entry must remain as the
    // retryable tombstone authority.
    servers[2].service.set_crashed(true).await;
    writer.truncate_before(WalSeqNo::record(1)).await.unwrap();
    let manifest = manifest_factory
        .replica("periodic-tombstone-wal/manifest")
        .stat()
        .await
        .unwrap();
    assert!(
        manifest.metadata["chorus.segments"].contains(&sealed.id),
        "partially deleted segment left no tombstone"
    );

    let handle = WalEngine::start(
        writer,
        WalEngineConfig {
            repair_interval: Some(Duration::from_millis(20)),
            ..Default::default()
        },
    )
    .unwrap();
    // Startup cleanup must run while the zone is still down and leave the
    // tombstone intact. No application truncation call occurs after this point.
    wait_for_repair_passes(&metrics, 1).await;
    let manifest = manifest_factory
        .replica("periodic-tombstone-wal/manifest")
        .stat()
        .await
        .unwrap();
    assert!(manifest.metadata["chorus.segments"].contains(&sealed.id));

    servers[2].service.set_crashed(false).await;
    let passes_before = metrics.counter("chorus.wal.repair.passes");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let manifest = manifest_factory
                .replica("periodic-tombstone-wal/manifest")
                .stat()
                .await
                .unwrap();
            if !manifest.metadata["chorus.segments"].contains(&sealed.id) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("periodic maintenance never removed the returned-zone tombstone");
    wait_for_repair_passes(&metrics, passes_before + 1).await;
    assert_eq!(
        factories[2]
            .replica(&object)
            .snapshot()
            .await
            .unwrap_err()
            .code,
        TransportCode::NotFound
    );
    shutdown_engine(handle).await;
}

#[tokio::test]
async fn startup_replay_preserves_record_order_and_sequence_numbers() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory.clone(), "replay-wal");
    let mut writer = volume.recover_writer().await.unwrap();
    let pending = writer
        .enqueue_records(
            vec![record(b"a"), record(b"b"), record(b"c")],
            no_attempted_bytes(),
        )
        .await
        .unwrap();
    for pending in pending {
        pending.wait().await.unwrap();
    }
    drop(writer);
    let (_, records) = recover_records(&volume, WalSeqNo::ZERO).await;
    assert_eq!(
        records
            .iter()
            .map(|record| record.payload.as_ref())
            .collect::<Vec<_>>(),
        vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
    );
    assert_eq!(records[0].next_seqno(), WalSeqNo::record(1));
    assert_eq!(records[1].next_seqno(), WalSeqNo::record(2));
}

#[tokio::test]
async fn startup_replay_must_finish_before_appends_start() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let (volume, metrics) = volume_with_metrics(
        factories.clone(),
        manifest_factory.clone(),
        "startup-replay-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"recover-me").await;
    drop(writer);

    let recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    assert!(matches!(
        recovery.start(WalEngineConfig::default()).await,
        Err(Error::RecoveryIncomplete)
    ));
    let mut recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    assert_eq!(recovery.end, WalSeqNo::record(1));
    for factory in &factories {
        // Recovery has already fenced and finalized the manifest tail. The
        // replay capability still prevents append admission until `start`.
        assert_eq!(
            active_segment_objects(factory, &manifest_factory, "startup-replay-wal")
                .await
                .len(),
            1
        );
    }
    let records: Vec<_> = (&mut recovery).try_collect().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, b"recover-me".as_slice());
    let handle = recovery.start(WalEngineConfig::default()).await.unwrap();
    assert_eq!(metrics.counter("chorus.wal.append.committed_records"), 0);
    for factory in &factories {
        assert_eq!(
            active_segment_objects(factory, &manifest_factory, "startup-replay-wal")
                .await
                .len(),
            1
        );
    }
    shutdown_engine(handle).await;
}

#[tokio::test]
async fn checksummed_replay_reads_one_copy_per_sealed_segment() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory, "crc-replay-wal");
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"first").await;
    writer.rotate().await.unwrap();
    append_one(&mut writer, b"second").await;
    writer.rotate().await.unwrap();
    drop(writer);

    let recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    for server in &servers {
        server.service.reset_operation_counts().await;
    }
    let records = recovery.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.payload.as_ref())
            .collect::<Vec<_>>(),
        vec![b"first".as_slice(), b"second".as_slice()]
    );
    assert_eq!(servers[0].service.operation_count(Operation::Read).await, 2);
    assert_eq!(servers[1].service.operation_count(Operation::Read).await, 0);
    assert_eq!(servers[2].service.operation_count(Operation::Read).await, 0);
}

#[tokio::test]
async fn dropping_recovery_replay_early_requires_fresh_recovery() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(factories, manifest_factory.clone(), "startup-drop-wal");
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"one").await;
    append_one(&mut writer, b"two").await;
    drop(writer);

    let mut recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    assert!(recovery.try_next().await.unwrap().is_some());
    assert!(matches!(
        recovery.start(WalEngineConfig::default()).await,
        Err(Error::RecoveryIncomplete)
    ));
    let mut recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    (&mut recovery).try_collect::<Vec<_>>().await.unwrap();
    let handle = recovery.start(WalEngineConfig::default()).await.unwrap();
    shutdown_engine(handle).await;
}

#[tokio::test]
async fn failed_recovery_replay_cannot_start_append_admission() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories.clone(),
        manifest_factory.clone(),
        "failed-startup-replay-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"recover-me").await;
    drop(writer);

    let mut recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    for (zone, server) in servers.iter().enumerate().take(3) {
        let object = active_segment_objects(
            &factories[zone],
            &manifest_factory,
            "failed-startup-replay-wal",
        )
        .await
        .into_iter()
        .next()
        .expect("recovered predecessor object");
        assert!(
            server
                .service
                .corrupt_byte_for(&format!("projects/_/buckets/zone-{zone}"), &object, 0,)
                .await
        );
    }
    assert!(recovery.try_next().await.is_err());
    assert!(recovery.try_next().await.unwrap().is_none());
    assert!(matches!(
        recovery.start(WalEngineConfig::default()).await,
        Err(Error::RecoveryIncomplete)
    ));
}

#[tokio::test]
async fn competing_recoveries_race_only_when_creating_the_next_segment() {
    let (_servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories,
        manifest_factory.clone(),
        "competing-recovery-start-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"shared-history").await;
    drop(writer);

    let mut first = volume.recover(WalSeqNo::ZERO).await.unwrap();
    (&mut first).try_collect::<Vec<_>>().await.unwrap();
    let mut second = volume.recover(WalSeqNo::ZERO).await.unwrap();
    (&mut second).try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(first.end, second.end);

    let (first, second) = tokio::join!(
        first.start(WalEngineConfig::default()),
        second.start(WalEngineConfig::default()),
    );
    assert_ne!(first.is_ok(), second.is_ok());
    if let Ok(handle) = first {
        shutdown_engine(handle).await;
    }
    if let Ok(handle) = second {
        shutdown_engine(handle).await;
    }
}

#[tokio::test]
async fn startup_replay_uses_gcs_integrity_and_ignores_one_corrupt_replica() {
    let (servers, factories, manifest_factory) = factory_cluster().await;
    let volume = volume(
        factories.clone(),
        manifest_factory.clone(),
        "corrupt-replay-wal",
    );
    let mut writer = volume.recover_writer().await.unwrap();
    append_one(&mut writer, b"sealed").await;
    writer.rotate().await.unwrap();
    let object =
        segment_object_for_base(&factories[2], &manifest_factory, "corrupt-replay-wal", 0).await;
    assert!(
        servers[2]
            .service
            .corrupt_byte_for("projects/_/buckets/zone-2", &object, 0,)
            .await
    );
    drop(writer);
    let (_, records) = recover_records(&volume, WalSeqNo::ZERO).await;
    assert_eq!(records[0].payload, b"sealed".as_slice());
}
