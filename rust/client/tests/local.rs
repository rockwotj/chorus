#![cfg(feature = "local")]

use bytes::Bytes;
use chorus_client::{ClientConfig, SegmentedVolume, WalEngineConfig, WalSeqNo};
use futures::TryStreamExt;

/// Append records, restart, and replay them from a local directory.
#[tokio::test]
async fn local_volume_replays_after_restart() {
    let dir = std::env::temp_dir().join(format!("chorus-local-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let config = WalEngineConfig {
        max_segment_bytes: 512,
        ..WalEngineConfig::default()
    };

    let mut expected = Vec::new();
    for _restart in 0..3 {
        let volume = SegmentedVolume::new_local(&dir, "wal", ClientConfig::default()).unwrap();
        let mut recovery = volume.recover(WalSeqNo::ZERO).await.unwrap();
        let mut replayed = Vec::new();
        while let Some(record) = recovery.try_next().await.unwrap() {
            replayed.push(record.payload);
        }
        assert_eq!(replayed, expected);

        let mut next = recovery.end;
        let mut wal = recovery.start(config.clone()).await.unwrap();
        for _ in 0..20 {
            let payload = Bytes::from(format!("record {}", next.record_index));
            let completion = wal.enqueue_append(next, payload.clone()).await.unwrap();
            completion.await.unwrap();
            expected.push(payload);
            next = WalSeqNo::record(next.record_index + 1);
        }
        wal.shutdown().await.unwrap();
    }
    std::fs::remove_dir_all(&dir).unwrap();
}
