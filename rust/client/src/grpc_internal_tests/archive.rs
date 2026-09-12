use super::*;
use crate::archive::{
    bytes_stream, read_all, tests::MemoryArchive, ArchiveConfig, ArchiveObjectRef,
};
use crate::manifest::Manifest;
use crate::{
    ArchivePolicy, ArchiveStore, GcsArchiveStore, GcsBodyManifestStore, ManifestStore,
    ManifestStoreError, ManifestVersion, VersionedManifest,
};
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

struct PausedFrontierManifest {
    inner: Arc<dyn ManifestStore>,
    old_tail: String,
    armed: AtomicBool,
    entered: Notify,
    resume: Notify,
    conflicts: AtomicUsize,
}

#[async_trait::async_trait]
impl ManifestStore for PausedFrontierManifest {
    fn max_directory_bytes(&self) -> usize {
        self.inner.max_directory_bytes()
    }

    async fn read(&self) -> Result<Option<VersionedManifest>, ManifestStoreError> {
        self.inner.read().await
    }

    async fn create(
        &self,
        fields: HashMap<String, String>,
    ) -> Result<VersionedManifest, ManifestStoreError> {
        self.inner.create(fields).await
    }

    async fn update(
        &self,
        version: ManifestVersion,
        fields: HashMap<String, String>,
    ) -> Result<VersionedManifest, ManifestStoreError> {
        // Claim preserves the tail. Its later replacement uses a recovery
        // catalog already adopted from that claimed manifest snapshot.
        if fields.get("chorus.tail_id") != Some(&self.old_tail)
            && self.armed.swap(false, Ordering::SeqCst)
        {
            self.entered.notify_one();
            self.resume.notified().await;
        }
        let result = self.inner.update(version, fields).await;
        if matches!(result, Err(ManifestStoreError::Conflict)) {
            self.conflicts.fetch_add(1, Ordering::SeqCst);
        }
        result
    }
}

async fn archive_during_recovery(initially_archived: bool, checkpoint: u64) {
    let (_servers, factories, regional) = factory_cluster().await;
    let prefix = "archive-recovery-snapshot";
    let archive = Arc::new(MemoryArchive::default());
    let policy = ArchivePolicy {
        keep_sealed_segments: 1,
    };
    let volume = volume(factories.clone(), regional.clone(), prefix)
        .with_archive(archive.clone(), policy)
        .unwrap();
    let mut writer = volume.recover_writer().await.unwrap();
    for value in [b"a", b"b", b"c", b"d"] {
        append_one(&mut writer, value).await;
        writer.rotate().await.unwrap();
    }
    drop(writer);
    let mut manifest = manifest_for(&regional, prefix).await;
    let config = ArchiveConfig {
        store: archive.clone(),
        policy,
    };
    if initially_archived {
        assert!(
            crate::segment::archive_one(&config, &factories, prefix, &mut manifest)
                .await
                .unwrap()
        );
        crate::segment::cleanup_archived_one(&config, &factories, prefix, &mut manifest)
            .await
            .unwrap();
    }
    let old_tail = manifest.record().tail_id.clone().unwrap();
    // A missing empty frontier forces a CAS during recovery preparation.
    for factory in &factories {
        let replica = factory.replica(&segment_object(prefix, &old_tail));
        replica
            .delete(replica.stat().await.unwrap().generation)
            .await
            .unwrap();
    }
    let paused = Arc::new(PausedFrontierManifest {
        inner: manifest.store(),
        old_tail,
        armed: AtomicBool::new(true),
        entered: Notify::new(),
        resume: Notify::new(),
        conflicts: AtomicUsize::new(0),
    });
    let volume = SegmentedVolume::new_with_factories_and_manifest_store(
        factories.clone(),
        paused.clone(),
        prefix,
        test_config(),
    )
    .unwrap()
    .with_archive(archive, policy)
    .unwrap();
    let recover = volume.recover(WalSeqNo::record(checkpoint));
    let publish = async {
        paused.entered.notified().await;
        // Move an entry out of the adopted hot catalog and remove every hot
        // copy. Replay must resolve its exact identity through archive fallback.
        assert!(
            crate::segment::archive_one(&config, &factories, prefix, &mut manifest)
                .await
                .unwrap()
        );
        crate::segment::cleanup_archived_one(&config, &factories, prefix, &mut manifest)
            .await
            .unwrap();
        paused.resume.notify_one();
    };
    let (recovery, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(recover, publish)
    })
    .await
    .expect("recovery did not reach and retry the frontier CAS");
    assert!(paused.conflicts.load(Ordering::SeqCst) > 0);
    let mut recovery = recovery.unwrap();
    assert_eq!(recovery.end, WalSeqNo::record(4));
    let records = (&mut recovery).try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.seqno.record_index)
            .collect::<Vec<_>>(),
        (checkpoint..4).collect::<Vec<_>>(),
        "initially_archived={initially_archived}, checkpoint={checkpoint}"
    );
    for record in records {
        assert_eq!(
            record.payload.as_ref(),
            &[b'a' + record.seqno.record_index as u8]
        );
    }
    shutdown_engine(recovery.start(WalEngineConfig::default()).await.unwrap()).await;
}

#[tokio::test]
async fn archive_recovery_keeps_adopted_empty_root_during_publication() {
    archive_during_recovery(false, 0).await;
}

#[tokio::test]
async fn archive_recovery_keeps_adopted_root_during_publication() {
    for checkpoint in [0, 1, 2] {
        archive_during_recovery(true, checkpoint).await;
    }
}

async fn manifest_for(factory: &Arc<dyn ReplicaFactory>, prefix: &str) -> Manifest {
    Manifest::open(
        Arc::new(crate::manifest_store::GcsManifestStore::new(
            factory.replica(&format!("{prefix}/manifest")),
        )),
        test_config(),
        Arc::new(crate::metrics::Metrics::new(&crate::NoopMetricsRecorder, 3)),
        3,
        (0..3).map(|i| format!("zone-{i}")).collect(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn archive_eviction_preserves_recovery_and_stale_readers() {
    let (servers, factories, regional) = factory_cluster().await;
    let prefix = "archive-recovery";
    let archive_factory =
        GrpcReplicaFactory::connect(3, &servers[3].endpoint, "projects/_/buckets/regional", None)
            .await
            .unwrap();
    let store =
        Arc::new(GcsArchiveStore::new(archive_factory, format!("{prefix}/archive")).unwrap());
    let policy = ArchivePolicy {
        keep_sealed_segments: 1,
    };
    let plain = volume(factories.clone(), regional.clone(), prefix);
    assert!(plain
        .clone()
        .with_archive(
            store.clone(),
            ArchivePolicy {
                keep_sealed_segments: 0
            }
        )
        .is_err());
    assert!(plain
        .clone()
        .with_archive(
            store.clone(),
            ArchivePolicy {
                keep_sealed_segments: usize::MAX
            }
        )
        .is_err());
    let volume = plain.clone().with_archive(store.clone(), policy).unwrap();
    let mut writer = volume.recover_writer().await.unwrap();
    for value in [b"a", b"b", b"c"] {
        append_one(&mut writer, value).await;
        writer.rotate().await.unwrap();
    }
    drop(writer);
    // The reader caches a hot-only manifest, then loses those zonal objects.
    let mut follower = volume.open_readonly(WalSeqNo::ZERO).await.unwrap();
    let mut manifest = manifest_for(&regional, prefix).await;
    let ids: Vec<_> = manifest
        .record()
        .segments
        .iter()
        .map(|entry| entry.id.clone())
        .collect();
    let config = ArchiveConfig {
        store: store.clone(),
        policy,
    };
    assert!(
        crate::segment::archive_one(&config, &factories, prefix, &mut manifest)
            .await
            .unwrap()
    );
    assert!(
        crate::segment::archive_one(&config, &factories, prefix, &mut manifest)
            .await
            .unwrap()
    );
    assert!(
        !crate::segment::archive_one(&config, &factories, prefix, &mut manifest)
            .await
            .unwrap()
    );
    for _ in 0..2 {
        crate::segment::cleanup_archived_one(&config, &factories, prefix, &mut manifest)
            .await
            .unwrap();
    }
    assert_eq!(manifest.record().trunc, 0);
    assert_eq!(manifest.record().segments.len(), 1);
    assert_eq!(manifest.record().archive.as_ref().unwrap().cleaned, 2);
    for factory in &factories {
        for id in &ids[..2] {
            assert_eq!(
                factory
                    .replica(&segment_object(prefix, id))
                    .stat()
                    .await
                    .unwrap_err()
                    .code,
                TransportCode::NotFound
            );
        }
    }
    for (index, expected) in [b"a", b"b", b"c"].iter().enumerate() {
        let record = tokio::time::timeout(Duration::from_secs(5), follower.try_next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(record.seqno.record_index, index as u64);
        assert_eq!(record.payload.as_ref(), *expected);
    }
    drop(follower);
    assert!(matches!(
        plain.recover(WalSeqNo::ZERO).await,
        Err(Error::InvalidConfig(_))
    ));
    let mut recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    let records = (&mut recovery).try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        records
            .iter()
            .map(|r| r.payload.clone())
            .collect::<Vec<_>>(),
        vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c")
        ]
    );
    let handle = recovery.start(WalEngineConfig::default()).await.unwrap();
    handle.truncate_before(WalSeqNo::record(3)).await.unwrap();
    let mut historical = volume.open_readonly(WalSeqNo::ZERO).await.unwrap();
    assert_eq!(
        historical
            .try_next()
            .await
            .unwrap()
            .unwrap()
            .payload
            .as_ref(),
        b"a"
    );
    drop(historical);
    shutdown_engine(handle).await;
}

#[tokio::test]
async fn archive_failure_and_unavailable_zone_do_not_lose_history_or_hold_slots() {
    let (servers, factories, regional) = factory_cluster().await;
    let prefix = "archive-faults";
    let store = Arc::new(MemoryArchive::default());
    let policy = ArchivePolicy {
        keep_sealed_segments: 1,
    };
    let volume = volume(factories.clone(), regional.clone(), prefix)
        .with_archive(store.clone(), policy)
        .unwrap();
    let mut writer = volume.recover_writer().await.unwrap();
    for _ in 0..3 {
        append_one(&mut writer, b"a").await;
        writer.rotate().await.unwrap();
    }
    drop(writer);
    let mut manifest = manifest_for(&regional, prefix).await;
    let config = ArchiveConfig {
        store: store.clone(),
        policy,
    };
    let before = manifest.record().clone();
    store.lose_write_response.store(true, Ordering::SeqCst);
    assert!(
        crate::segment::archive_one(&config, &factories, prefix, &mut manifest)
            .await
            .is_err()
    );
    assert_eq!(manifest.record(), &before);
    store.unavailable.store(true, Ordering::SeqCst);
    assert!(
        crate::segment::archive_one(&config, &factories, prefix, &mut manifest)
            .await
            .is_err()
    );
    assert_eq!(manifest.record(), &before);
    store.unavailable.store(false, Ordering::SeqCst);
    assert!(
        crate::segment::archive_one(&config, &factories, prefix, &mut manifest)
            .await
            .unwrap()
    );
    servers[0].service.set_crashed(true).await;
    crate::segment::cleanup_archived_one(&config, &factories, prefix, &mut manifest)
        .await
        .unwrap();
    assert_eq!(manifest.record().segments.len(), 2);
    assert_eq!(manifest.record().archive.as_ref().unwrap().cleaned, 0);
    servers[0].service.set_crashed(false).await;
    crate::segment::cleanup_archived_one(&config, &factories, prefix, &mut manifest)
        .await
        .unwrap();
    assert_eq!(manifest.record().archive.as_ref().unwrap().cleaned, 1);
}

#[tokio::test]
async fn archive_gcs_streams_immutable_objects_and_body_manifest_cas() {
    let server = FakeGcs::default().start().await.unwrap();
    let factory =
        GrpcReplicaFactory::connect(0, &server.endpoint, "projects/_/buckets/archive", None)
            .await
            .unwrap();
    let store = GcsArchiveStore::new(factory.clone(), "wal").unwrap();
    let bytes = Bytes::from(vec![37; 700_000]);
    let object = ArchiveObjectRef::for_bytes("segments", &bytes);
    store
        .put_if_absent(&object, bytes_stream(bytes.clone()))
        .await
        .unwrap();
    store
        .put_if_absent(&object, bytes_stream(bytes.clone()))
        .await
        .unwrap();
    assert_eq!(read_all(&store, &object).await.unwrap(), bytes);
    let chunks = store
        .read(&object, Some(123..456))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(chunks.concat(), bytes[123..456]);
    let mut conflicting = ArchiveObjectRef::for_bytes("segments", b"different");
    conflicting.key = object.key.clone();
    assert!(matches!(
        store
            .put_if_absent(&conflicting, bytes_stream(Bytes::from_static(b"different")))
            .await,
        Err(crate::ArchiveError::Corrupt(_))
    ));
    let corrupt_bytes = Bytes::from_static(b"genuine-corruption-error");
    let corrupt = ArchiveObjectRef::for_bytes("segments", &corrupt_bytes);
    server
        .service
        .inject(Operation::BidiWrite, Code::DataLoss)
        .await;
    assert!(matches!(
        store
            .put_if_absent(&corrupt, bytes_stream(corrupt_bytes.clone()))
            .await,
        Err(crate::ArchiveError::Corrupt(_))
    ));
    store
        .put_if_absent(&corrupt, bytes_stream(corrupt_bytes))
        .await
        .unwrap();
    let manifest = GcsBodyManifestStore::new(factory, "manifest", 100_000).unwrap();
    let first = manifest
        .create(HashMap::from([("payload".into(), "x".repeat(9000))]))
        .await
        .unwrap();
    assert_eq!(manifest.read().await.unwrap().unwrap().fields, first.fields);
    let second = manifest
        .update(
            first.version,
            HashMap::from([("payload".into(), "new".into())]),
        )
        .await
        .unwrap();
    assert_ne!(first.version, second.version);
    assert!(matches!(
        manifest.update(first.version, first.fields).await,
        Err(crate::ManifestStoreError::Conflict)
    ));
}

#[tokio::test]
async fn archive_policy_relieves_full_body_manifest_without_raising_floor() {
    let (servers, factories, _) = factory_cluster().await;
    let regional =
        GrpcReplicaFactory::connect(3, &servers[3].endpoint, "projects/_/buckets/regional", None)
            .await
            .unwrap();
    let manifest_store =
        Arc::new(GcsBodyManifestStore::new(regional, "archive-auto/manifest", 512).unwrap());
    let archive = Arc::new(MemoryArchive::default());
    let volume = SegmentedVolume::new_with_factories_and_manifest_store(
        factories,
        manifest_store.clone(),
        "archive-auto",
        test_config(),
    )
    .unwrap()
    .with_archive(
        archive.clone(),
        ArchivePolicy {
            keep_sealed_segments: 1,
        },
    )
    .unwrap();
    let mut writer = volume.recover_writer().await.unwrap();
    let mut rotations = 0;
    loop {
        append_one(&mut writer, b"x").await;
        if !writer.rotation_due(1) {
            break;
        }
        writer.rotate().await.unwrap();
        rotations += 1;
        assert!(rotations < 16);
    }
    assert!(rotations >= 2);
    let next = writer.committed_record_end();
    archive.unavailable.store(true, Ordering::SeqCst);
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
            .enqueue_append(WalSeqNo::record(next), Bytes::from_static(b"y"))
            .await,
        Err(Error::ActiveSegmentFull { .. })
    ));
    archive.unavailable.store(false, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(15), async {
        for index in next..next + 20 {
            loop {
                match handle
                    .enqueue_append(WalSeqNo::record(index), Bytes::from_static(b"y"))
                    .await
                {
                    Err(Error::ActiveSegmentFull { .. }) => {
                        tokio::time::sleep(Duration::from_millis(10)).await
                    }
                    result => {
                        result.unwrap().await.unwrap();
                        break;
                    }
                }
            }
        }
    })
    .await
    .expect("archival did not relieve directory backpressure");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let state = match manifest_store.read().await {
                Err(crate::ManifestStoreError::Unavailable(_)) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                result => result.unwrap().unwrap(),
            };
            assert_eq!(state.fields["chorus.trunc"], "0");
            if !state.fields["chorus.segments"].contains(',') {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("hot retention target did not converge");
    shutdown_engine(handle).await;
    let mut recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    let records = (&mut recovery).try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(records.len() as u64, next + 20);
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record.seqno.record_index, index as u64);
    }
}

#[tokio::test]
async fn archive_publication_revalidates_parent_and_preserves_checkpoint() {
    let (_servers, factories, regional) = factory_cluster().await;
    let store = Arc::new(MemoryArchive::default());
    let policy = ArchivePolicy {
        keep_sealed_segments: 1,
    };
    let prefix = "archive-cas";
    let volume = volume(factories.clone(), regional.clone(), prefix)
        .with_archive(store.clone(), policy)
        .unwrap();
    let mut writer = volume.recover_writer().await.unwrap();
    for _ in 0..3 {
        append_one(&mut writer, b"a").await;
        writer.rotate().await.unwrap();
    }
    drop(writer);
    let mut stale = manifest_for(&regional, prefix).await;
    let first = stale.record().segments[0].clone();
    let bytes = record(b"a").encode().unwrap();
    let entry = crate::archive::ArchivedSegment {
        id: first.id,
        start: 0,
        end: 1,
        crc32c: first.crc32c,
        object: ArchiveObjectRef::for_bytes("segments", &bytes),
    };
    store
        .put_if_absent(&entry.object, bytes_stream(bytes))
        .await
        .unwrap();
    let root = crate::archive::ArchiveCatalog::new(store.clone())
        .prepare_append(None, entry.clone())
        .await
        .unwrap();
    let mut other = manifest_for(&regional, prefix).await;
    other.raise_trunc(3).await.unwrap();
    let mut wrong = entry.clone();
    wrong.end = 2;
    assert!(!stale.publish_archive(None, &root, &wrong).await.unwrap());
    assert!(stale.publish_archive(None, &root, &entry).await.unwrap());
    assert_eq!(stale.record().trunc, 3);
    assert!(other.publish_archive(None, &root, &entry).await.unwrap());
    let config = ArchiveConfig { store, policy };
    assert!(
        crate::segment::archive_one(&config, &factories, prefix, &mut other)
            .await
            .unwrap()
    );
    // An idempotent success from a cached view is harmless: it cannot write
    // the old root back. Refresh before checking a genuinely stale parent.
    stale.refreshed_record().await.unwrap();
    assert!(!stale.publish_archive(None, &root, &entry).await.unwrap());
    assert_eq!(
        stale
            .record()
            .archive
            .as_ref()
            .unwrap()
            .root
            .as_ref()
            .unwrap()
            .end,
        2
    );
    assert_eq!(stale.record().trunc, 3);
}
