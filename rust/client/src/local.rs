//! Local-filesystem storage for development and testing.
//!
//! [`SegmentedVolume::new_local`](crate::SegmentedVolume::new_local) runs a
//! single-replica WAL out of one directory. Each object is a data file under
//! `objects/` plus a JSON metadata file under `meta/`; the manifest register is
//! a JSON file under `manifests/`. All operations in a process share one lock.
//!
//! This backend is for running Chorus on a laptop. It does not fsync, does not
//! coordinate between processes, and does not model provider faults.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::manifest_store::{
    ManifestStore, ManifestStoreError, ManifestVersion, VersionedManifest,
};
use crate::transport::{
    AppendToken, LaneDurableChange, ListedObject, PackedAppend, Replica, ReplicaFactory,
    ReplicaRangeRead, ReplicaSnapshot, TransportCode, TransportError,
};

/// Bucket name recorded in the manifest's `chorus.buckets` binding.
pub(crate) const LOCAL_BUCKET: &str = "projects/_/buckets/local";

static LOCK: Mutex<()> = Mutex::new(());

fn lock() -> MutexGuard<'static, ()> {
    LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Generations and session ids. Seeded from the clock so values keep
/// increasing across process restarts.
fn next_id() -> i64 {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let _ = NEXT.compare_exchange(
        0,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(1, |elapsed| elapsed.as_micros() as u64),
        Ordering::Relaxed,
        Ordering::Relaxed,
    );
    NEXT.fetch_add(1, Ordering::Relaxed) as i64
}

fn err(code: TransportCode, message: impl Into<String>) -> TransportError {
    TransportError {
        zone: 0,
        code,
        message: message.into(),
    }
}

fn io_err(error: std::io::Error) -> TransportError {
    err(TransportCode::Internal, error.to_string())
}

#[derive(Clone, Serialize, Deserialize)]
struct ObjectMeta {
    generation: i64,
    metageneration: i64,
    finalized: bool,
    /// Session id of the most recent append open. A newer open fences older
    /// sessions.
    session: i64,
    metadata: HashMap<String, String>,
}

/// One object's data and metadata file paths.
struct ObjectFiles {
    data: PathBuf,
    meta: PathBuf,
}

impl ObjectFiles {
    fn new(root: &Path, object: &str) -> Self {
        Self {
            data: root.join("objects").join(object),
            meta: root.join("meta").join(format!("{object}.json")),
        }
    }

    fn load(&self) -> Result<Option<ObjectMeta>, TransportError> {
        match std::fs::read(&self.meta) {
            Ok(raw) => serde_json::from_slice(&raw)
                .map(Some)
                .map_err(|error| err(TransportCode::DataLoss, error.to_string())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io_err(error)),
        }
    }

    fn load_existing(&self) -> Result<ObjectMeta, TransportError> {
        self.load()?
            .ok_or_else(|| err(TransportCode::NotFound, "object not found"))
    }

    fn store(&self, meta: &ObjectMeta) -> Result<(), TransportError> {
        write_file(
            &self.meta,
            &serde_json::to_vec(meta).expect("meta serializes"),
        )
    }

    fn read_data(&self) -> Result<Vec<u8>, TransportError> {
        std::fs::read(&self.data).map_err(io_err)
    }

    fn data_len(&self) -> Result<i64, TransportError> {
        Ok(std::fs::metadata(&self.data).map_err(io_err)?.len() as i64)
    }
}

fn write_file(path: &Path, contents: &[u8]) -> Result<(), TransportError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(io_err)?;
    }
    std::fs::write(path, contents).map_err(io_err)
}

fn snapshot(meta: ObjectMeta, bytes: Vec<u8>, size: i64) -> ReplicaSnapshot {
    ReplicaSnapshot {
        zone: 0,
        generation: meta.generation,
        metageneration: meta.metageneration,
        persisted_size: size,
        finalized: meta.finalized,
        crc32c: meta.finalized.then(|| crc32c::crc32c(&bytes)),
        metadata: meta.metadata,
        bytes,
    }
}

/// Single-zone replica factory over a local directory.
pub(crate) struct LocalReplicaFactory {
    root: PathBuf,
}

impl LocalReplicaFactory {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self { root }
    }
}

#[async_trait]
impl ReplicaFactory for LocalReplicaFactory {
    fn bucket_name(&self) -> &str {
        LOCAL_BUCKET
    }

    fn replica(&self, object: &str) -> Arc<dyn Replica> {
        Arc::new(LocalReplica {
            files: ObjectFiles::new(&self.root, object),
            session: Mutex::new(None),
        })
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ListedObject>, TransportError> {
        let _guard = lock();
        let meta_root = self.root.join("meta");
        let mut pending = vec![meta_root.clone()];
        let mut listed = Vec::new();
        while let Some(dir) = pending.pop() {
            let entries = match std::fs::read_dir(&dir) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(io_err(error)),
            };
            for entry in entries {
                let path = entry.map_err(io_err)?.path();
                if path.is_dir() {
                    pending.push(path);
                    continue;
                }
                let relative = path.strip_prefix(&meta_root).expect("walked under meta");
                let Some(name) = relative
                    .to_str()
                    .and_then(|name| name.strip_suffix(".json"))
                else {
                    continue;
                };
                if !name.starts_with(prefix) {
                    continue;
                }
                let files = ObjectFiles::new(&self.root, name);
                let Some(meta) = files.load()? else {
                    continue;
                };
                let bytes = files.read_data()?;
                let last_modified = std::fs::metadata(&path)
                    .and_then(|metadata| metadata.modified())
                    .ok();
                listed.push(ListedObject {
                    zone: 0,
                    name: name.to_string(),
                    generation: meta.generation,
                    size: bytes.len() as i64,
                    finalized: meta.finalized,
                    last_modified,
                    crc32c: meta.finalized.then(|| crc32c::crc32c(&bytes)),
                    metadata: meta.metadata,
                });
            }
        }
        listed.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(listed)
    }
}

/// The append session this handle opened, if it is still live.
struct Session {
    id: i64,
    durable: watch::Sender<i64>,
}

struct LocalReplica {
    files: ObjectFiles,
    session: Mutex<Option<Session>>,
}

impl LocalReplica {
    fn session(&self) -> MutexGuard<'_, Option<Session>> {
        self.session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Record `id` as this handle's live session and build its token.
    fn open_session(&self, id: i64, meta: &ObjectMeta, persisted_size: i64) -> AppendToken {
        *self.session() = Some(Session {
            id,
            durable: watch::Sender::new(persisted_size),
        });
        AppendToken {
            zone: 0,
            generation: Some(meta.generation),
            metageneration: Some(meta.metageneration),
            persisted_size,
            write_handle: Some(Bytes::copy_from_slice(&id.to_be_bytes())),
        }
    }

    fn append(&self, write_offset: i64, packed: &PackedAppend) -> Result<(), TransportError> {
        let _guard = lock();
        let session = self.session();
        let session = session
            .as_ref()
            .ok_or_else(|| err(TransportCode::FailedPrecondition, "no live append session"))?;
        let meta = self.files.load_existing()?;
        if meta.finalized || meta.session != session.id {
            return Err(err(
                TransportCode::FailedPrecondition,
                "append session was fenced",
            ));
        }
        let len = self.files.data_len()?;
        if write_offset != len {
            return Err(err(
                TransportCode::OutOfRange,
                format!("write offset {write_offset} does not match object length {len}"),
            ));
        }
        let mut data = Vec::with_capacity(packed.len());
        for message in packed.messages() {
            data.extend_from_slice(&message.content);
        }
        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&self.files.data)
            .and_then(|mut file| file.write_all(&data))
            .map_err(io_err)?;
        session.durable.send_replace(len + data.len() as i64);
        Ok(())
    }
}

#[async_trait]
impl Replica for LocalReplica {
    async fn snapshot(&self) -> Result<ReplicaSnapshot, TransportError> {
        let _guard = lock();
        let meta = self.files.load_existing()?;
        let bytes = self.files.read_data()?;
        let size = bytes.len() as i64;
        Ok(snapshot(meta, bytes, size))
    }

    async fn read_range(&self, offset: i64) -> Result<ReplicaRangeRead, TransportError> {
        let _guard = lock();
        let meta = self.files.load_existing()?;
        let bytes = self.files.read_data()?;
        let start = usize::try_from(offset)
            .ok()
            .filter(|start| *start <= bytes.len())
            .ok_or_else(|| err(TransportCode::OutOfRange, "read offset past object end"))?;
        Ok(ReplicaRangeRead {
            zone: 0,
            generation: meta.generation,
            bytes: bytes[start..].to_vec(),
        })
    }

    async fn stat(&self) -> Result<ReplicaSnapshot, TransportError> {
        let _guard = lock();
        let meta = self.files.load_existing()?;
        let bytes = self.files.read_data()?;
        let size = bytes.len() as i64;
        let mut snapshot = snapshot(meta, bytes, size);
        snapshot.bytes = Vec::new();
        Ok(snapshot)
    }

    async fn create_append_session(
        &self,
        metadata: HashMap<String, String>,
    ) -> Result<AppendToken, TransportError> {
        let _guard = lock();
        if self.files.load()?.is_some() {
            return Err(err(TransportCode::AlreadyExists, "object already exists"));
        }
        let id = next_id();
        let meta = ObjectMeta {
            generation: id,
            metageneration: 1,
            finalized: false,
            session: id,
            metadata,
        };
        write_file(&self.files.data, &[])?;
        self.files.store(&meta)?;
        Ok(self.open_session(id, &meta, 0))
    }

    async fn resume_tail(&self, token: &mut AppendToken) -> Result<i64, TransportError> {
        let _guard = lock();
        let meta = self.files.load_existing()?;
        let handle_id = token
            .write_handle
            .as_ref()
            .and_then(|handle| <[u8; 8]>::try_from(handle.as_ref()).ok())
            .map(i64::from_be_bytes);
        if meta.finalized || handle_id != Some(meta.session) {
            return Err(err(
                TransportCode::FailedPrecondition,
                "append session was fenced",
            ));
        }
        let len = self.files.data_len()?;
        *token = self.open_session(meta.session, &meta, len);
        Ok(len)
    }

    async fn takeover(&self, observed: &ReplicaSnapshot) -> Result<AppendToken, TransportError> {
        let _guard = lock();
        let mut meta = self.files.load_existing()?;
        if meta.finalized || meta.generation != observed.generation {
            return Err(err(
                TransportCode::FailedPrecondition,
                "takeover precondition failed",
            ));
        }
        meta.session = next_id();
        self.files.store(&meta)?;
        let len = self.files.data_len()?;
        Ok(self.open_session(meta.session, &meta, len))
    }

    async fn replace_appendable(
        &self,
        observed: Option<&ReplicaSnapshot>,
        data: Bytes,
        metadata: HashMap<String, String>,
    ) -> Result<AppendToken, TransportError> {
        let _guard = lock();
        let current = self.files.load()?;
        let matches = match (&current, observed) {
            (None, None) => true,
            (Some(current), Some(observed)) => {
                current.generation == observed.generation
                    && current.metageneration == observed.metageneration
            }
            _ => false,
        };
        if !matches {
            return Err(err(
                TransportCode::FailedPrecondition,
                "replace precondition failed",
            ));
        }
        let id = next_id();
        let meta = ObjectMeta {
            generation: id,
            metageneration: 1,
            finalized: false,
            session: id,
            metadata,
        };
        write_file(&self.files.data, &data)?;
        self.files.store(&meta)?;
        Ok(self.open_session(id, &meta, data.len() as i64))
    }

    async fn lane_send(
        &self,
        write_offset: i64,
        packed: &PackedAppend,
    ) -> Result<(), TransportError> {
        self.append(write_offset, packed)
    }

    async fn lane_send_unflushed(
        &self,
        write_offset: i64,
        packed: &PackedAppend,
    ) -> Result<(), TransportError> {
        self.append(write_offset, packed)
    }

    async fn lane_flush(&self, _write_offset: i64) -> Result<(), TransportError> {
        Ok(())
    }

    async fn lane_durable_change(&self, seen: i64) -> Result<LaneDurableChange, TransportError> {
        let mut durable = self
            .session()
            .as_ref()
            .map(|session| session.durable.subscribe())
            .ok_or_else(|| err(TransportCode::FailedPrecondition, "no live append session"))?;
        let persisted_size = *durable
            .wait_for(|durable| *durable > seen)
            .await
            .map_err(|_| err(TransportCode::FailedPrecondition, "append session closed"))?;
        Ok(LaneDurableChange {
            persisted_size,
            error: None,
        })
    }

    async fn delete(&self, generation: i64) -> Result<(), TransportError> {
        let _guard = lock();
        let meta = self.files.load_existing()?;
        if meta.generation != generation {
            return Err(err(
                TransportCode::FailedPrecondition,
                "delete generation mismatch",
            ));
        }
        std::fs::remove_file(&self.files.meta).map_err(io_err)?;
        std::fs::remove_file(&self.files.data).map_err(io_err)
    }

    async fn finalize(
        &self,
        token: &mut AppendToken,
        write_offset: i64,
    ) -> Result<ReplicaSnapshot, TransportError> {
        let _guard = lock();
        let mut meta = self.files.load_existing()?;
        let bytes = self.files.read_data()?;
        let size = bytes.len() as i64;
        if size != write_offset {
            return Err(err(
                TransportCode::DataLoss,
                format!("finalize at {write_offset} but object length is {size}"),
            ));
        }
        if !meta.finalized {
            meta.finalized = true;
            meta.metageneration += 1;
            self.files.store(&meta)?;
        }
        *self.session() = None;
        token.generation = Some(meta.generation);
        token.metageneration = Some(meta.metageneration);
        Ok(snapshot(meta, bytes, size))
    }

    async fn shutdown(&self) {
        *self.session() = None;
    }
}

#[derive(Serialize, Deserialize)]
struct StoredManifest {
    version: u64,
    fields: HashMap<String, String>,
}

/// Manifest register stored as one JSON file.
pub(crate) struct LocalManifestStore {
    path: PathBuf,
}

impl LocalManifestStore {
    pub(crate) fn new(root: &Path, prefix: &str) -> Self {
        Self {
            path: root.join("manifests").join(format!("{prefix}.json")),
        }
    }

    fn load(&self) -> Result<Option<StoredManifest>, ManifestStoreError> {
        match std::fs::read(&self.path) {
            Ok(raw) => serde_json::from_slice(&raw)
                .map(Some)
                .map_err(|error| ManifestStoreError::Backend(error.to_string())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(ManifestStoreError::Backend(error.to_string())),
        }
    }

    fn store(&self, stored: StoredManifest) -> Result<VersionedManifest, ManifestStoreError> {
        write_file(
            &self.path,
            &serde_json::to_vec(&stored).expect("manifest serializes"),
        )
        .map_err(|error| ManifestStoreError::Backend(error.message))?;
        Ok(VersionedManifest {
            version: ManifestVersion(stored.version),
            fields: stored.fields,
        })
    }
}

#[async_trait]
impl ManifestStore for LocalManifestStore {
    fn max_directory_bytes(&self) -> usize {
        64 * 1024
    }

    async fn read(&self) -> Result<Option<VersionedManifest>, ManifestStoreError> {
        let _guard = lock();
        Ok(self.load()?.map(|stored| VersionedManifest {
            version: ManifestVersion(stored.version),
            fields: stored.fields,
        }))
    }

    async fn create(
        &self,
        fields: HashMap<String, String>,
    ) -> Result<VersionedManifest, ManifestStoreError> {
        let _guard = lock();
        if self.load()?.is_some() {
            return Err(ManifestStoreError::AlreadyExists);
        }
        self.store(StoredManifest { version: 1, fields })
    }

    async fn update(
        &self,
        version: ManifestVersion,
        fields: HashMap<String, String>,
    ) -> Result<VersionedManifest, ManifestStoreError> {
        let _guard = lock();
        let current = self
            .load()?
            .ok_or_else(|| ManifestStoreError::Backend("the register was never created".into()))?;
        if current.version != version.0 {
            return Err(ManifestStoreError::Conflict);
        }
        self.store(StoredManifest {
            version: version.0 + 1,
            fields,
        })
    }
}
