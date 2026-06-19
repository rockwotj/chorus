use std::time::Duration;

use bytes::Bytes;
use chorus_client::{ClientConfig, GrpcReplicaFactory, SegmentedVolume, WalEngineConfig, WalSeqNo};
use chorus_fake_gcs::FakeGcs;
use futures::TryStreamExt;

async fn volume(prefix: &str) -> (Vec<chorus_fake_gcs::RunningFake>, SegmentedVolume) {
    let mut servers = Vec::new();
    let mut factories = Vec::new();
    for zone in 0..3 {
        let server = FakeGcs::default().start().await.unwrap();
        let factory = GrpcReplicaFactory::connect(
            zone,
            &server.endpoint,
            format!("projects/_/buckets/zone-{zone}"),
            None,
        )
        .await
        .unwrap();
        servers.push(server);
        factories.push(factory);
    }

    let regional = FakeGcs::default().start().await.unwrap();
    let manifest_factory =
        GrpcReplicaFactory::connect(3, &regional.endpoint, "projects/_/buckets/regional", None)
            .await
            .unwrap();
    servers.push(regional);

    let volume = SegmentedVolume::new(
        factories,
        manifest_factory,
        prefix,
        ClientConfig {
            max_retries: 3,
            retry_base: Duration::ZERO,
        },
    )
    .unwrap();
    (servers, volume)
}

#[tokio::test]
async fn public_api_appends_and_replays_records() {
    let (_servers, volume) = volume("public-api-wal").await;
    let mut recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    assert_eq!(recovery.end, WalSeqNo::ZERO);
    assert!(recovery.try_next().await.unwrap().is_none());

    let mut handle = recovery.start(WalEngineConfig::default()).await.unwrap();
    for (record_index, payload) in [b"one".as_slice(), b"two", b"three"]
        .into_iter()
        .enumerate()
    {
        let receipt = handle
            .enqueue_append(
                WalSeqNo::record(record_index as u64),
                Bytes::copy_from_slice(payload),
            )
            .await
            .unwrap()
            .await
            .unwrap();
        assert_eq!(receipt.seqno, WalSeqNo::record(record_index as u64));
    }
    tokio::time::timeout(Duration::from_secs(10), handle.shutdown())
        .await
        .expect("engine shutdown timed out")
        .unwrap();

    let recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
    assert_eq!(recovery.end, WalSeqNo::record(3));
    let records = recovery.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.payload.as_ref())
            .collect::<Vec<_>>(),
        vec![b"one".as_slice(), b"two", b"three"]
    );
    assert_eq!(records[2].next_seqno(), WalSeqNo::record(3));
}
