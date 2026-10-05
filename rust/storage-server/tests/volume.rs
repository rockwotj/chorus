//! End to end: a `SegmentedVolume` replicated over three storage nodes, with
//! an in-memory manifest register.

#[macro_use]
mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chorus_client::dst_support::gcs_max_directory_bytes;
use chorus_client::{
    ClientConfig, ManifestStore, ManifestStoreError, ManifestVersion, SegmentedVolume,
    TcpReplicaFactory, VersionedManifest, WalEngineConfig, WalHandle, WalSeqNo,
};
use common::{Backing, TestNode};
use futures::TryStreamExt;

const PREFIX: &str = "db/wal";
const STEP: Duration = Duration::from_secs(60);

/// A compare-and-set register in process memory.
#[derive(Default)]
struct MemoryManifestStore {
    register: Mutex<Option<VersionedManifest>>,
}

#[async_trait]
impl ManifestStore for MemoryManifestStore {
    fn max_directory_bytes(&self) -> usize {
        gcs_max_directory_bytes()
    }

    async fn read(&self) -> Result<Option<VersionedManifest>, ManifestStoreError> {
        Ok(self.register.lock().unwrap().clone())
    }

    async fn create(
        &self,
        fields: HashMap<String, String>,
    ) -> Result<VersionedManifest, ManifestStoreError> {
        let mut register = self.register.lock().unwrap();
        if register.is_some() {
            return Err(ManifestStoreError::AlreadyExists);
        }
        let created = VersionedManifest {
            version: ManifestVersion(1),
            fields,
        };
        *register = Some(created.clone());
        Ok(created)
    }

    async fn update(
        &self,
        version: ManifestVersion,
        fields: HashMap<String, String>,
    ) -> Result<VersionedManifest, ManifestStoreError> {
        let mut register = self.register.lock().unwrap();
        match register.as_ref() {
            Some(current) if current.version == version => {
                let updated = VersionedManifest {
                    version: ManifestVersion(version.0 + 1),
                    fields,
                };
                *register = Some(updated.clone());
                Ok(updated)
            }
            _ => Err(ManifestStoreError::Conflict),
        }
    }
}

struct Cluster {
    nodes: Vec<TestNode>,
    manifest: Arc<MemoryManifestStore>,
}

impl Cluster {
    fn start(backing: Backing) -> Self {
        Self {
            nodes: (0..3)
                .map(|i| TestNode::start_with(&format!("node-{i}"), backing))
                .collect(),
            manifest: Arc::default(),
        }
    }

    /// Stop all three (disk) nodes and start them again on their data
    /// directories. The manifest register lives on in this process.
    fn restart(&mut self) {
        self.nodes = std::mem::take(&mut self.nodes)
            .into_iter()
            .map(TestNode::restart)
            .collect();
    }

    /// A fresh client process: new connections, same nodes and register.
    async fn volume(&self) -> SegmentedVolume {
        let mut factories = Vec::new();
        for (zone, node) in self.nodes.iter().enumerate() {
            factories.push(
                TcpReplicaFactory::connect(node.addr(), format!("zone-{zone}"), zone)
                    .await
                    .expect("connect"),
            );
        }
        SegmentedVolume::new_tcp(
            factories,
            self.manifest.clone(),
            PREFIX,
            ClientConfig::default(),
        )
        .expect("volume")
    }

    /// Recover from the start of the log; returns the replayed payloads and
    /// the started writer.
    async fn recover(&self) -> (Vec<Bytes>, WalSeqNo, WalHandle) {
        let volume = self.volume().await;
        let mut recovery = tokio::time::timeout(STEP, volume.recover(WalSeqNo::record(0)))
            .await
            .expect("recover in time")
            .expect("recover");
        let end = recovery.end;
        let mut replayed = Vec::new();
        while let Some(record) = recovery.try_next().await.expect("replay") {
            replayed.push(record.payload);
        }
        let wal = recovery
            .start(WalEngineConfig::default())
            .await
            .expect("start");
        (replayed, end, wal)
    }
}

async fn append_all(wal: &mut WalHandle, from: WalSeqNo, payloads: &[Bytes]) -> WalSeqNo {
    let mut completions = Vec::new();
    let mut next = from;
    for payload in payloads {
        completions.push(wal.enqueue_append(next, payload.clone()).await.unwrap());
        next = WalSeqNo::record(next.record_index + 1);
    }
    for completion in completions {
        tokio::time::timeout(STEP, completion)
            .await
            .expect("commit in time")
            .expect("commit");
    }
    next
}

fn records(range: std::ops::Range<usize>) -> Vec<Bytes> {
    range
        .map(|i| Bytes::from(format!("record-{i}-{}", "x".repeat(i * 37))))
        .collect()
}

async fn wal_over_three_storage_nodes(backing: Backing) {
    let cluster = Cluster::start(backing);

    // First writer: an empty log.
    let (replayed, end, mut wal) = cluster.recover().await;
    assert!(replayed.is_empty());
    let first = records(0..5);
    let next = append_all(&mut wal, end, &first).await;
    assert_eq!(next, WalSeqNo::record(5));
    wal.shutdown().await.expect("clean shutdown");

    // Second writer: replays the sealed history, appends more, and then
    // "crashes" (dropped without shutdown) with an open segment.
    let (replayed, end, mut wal) = cluster.recover().await;
    assert_eq!(replayed, first);
    assert_eq!(end, WalSeqNo::record(5));
    let second = records(5..9);
    append_all(&mut wal, end, &second).await;
    drop(wal);

    // Third writer: recovery fences the open segment and replays everything.
    let (replayed, end, wal) = cluster.recover().await;
    assert_eq!(replayed, [first, second].concat());
    assert_eq!(end, WalSeqNo::record(9));
    wal.shutdown().await.expect("clean shutdown");
}

for_each_backing!(wal_over_three_storage_nodes);

#[tokio::test(flavor = "multi_thread")]
async fn wal_survives_restarting_every_node() {
    let mut cluster = Cluster::start(Backing::Disk);

    // A sealed history plus an open segment left by a crashed writer.
    let (_, end, mut wal) = cluster.recover().await;
    let first = records(0..6);
    let next = append_all(&mut wal, end, &first).await;
    wal.shutdown().await.expect("clean shutdown");
    let (replayed, end, mut wal) = cluster.recover().await;
    assert_eq!(replayed, first);
    assert_eq!(end, next);
    let second = records(6..11);
    append_all(&mut wal, end, &second).await;
    drop(wal);

    // Every node restarts from its data directory.
    cluster.restart();

    // Every acknowledged record is still there; the log goes on.
    let (replayed, end, mut wal) = cluster.recover().await;
    let acknowledged = [first, second].concat();
    assert_eq!(replayed, acknowledged);
    assert_eq!(end, WalSeqNo::record(11));
    let third = records(11..14);
    append_all(&mut wal, end, &third).await;
    wal.shutdown().await.expect("clean shutdown");

    // And once more, after a clean shutdown.
    cluster.restart();
    let (replayed, end, wal) = cluster.recover().await;
    assert_eq!(replayed, [acknowledged, third].concat());
    assert_eq!(end, WalSeqNo::record(14));
    wal.shutdown().await.expect("clean shutdown");
}
