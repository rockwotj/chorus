//! Immutable archive storage and a copy-on-write, sequence-ordered catalog.
//!
//! Only the control manifest publishes roots. Uploading an object or preparing
//! a root does not authorize removal of any hot copy. Catalog pages and WAL
//! objects share the same backend and are never overwritten.

use std::{ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{future::BoxFuture, stream::BoxStream, FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// A lazily consumed archive read or upload. Dropping it cancels owned work.
pub type ArchiveByteStream = BoxStream<'static, Result<Bytes, ArchiveError>>;

/// Stable, namespace-relative immutable object identity and integrity metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArchiveObjectRef {
    /// Object key relative to the store's namespace.
    pub key: String,
    /// Exact complete-object byte length.
    pub byte_len: u64,
    /// SHA-256 of the complete immutable object.
    pub sha256: [u8; 32],
}

impl ArchiveObjectRef {
    pub(crate) fn for_bytes(kind: &str, bytes: &[u8]) -> Self {
        let sha256: [u8; 32] = Sha256::digest(bytes).into();
        let hash: String = sha256.iter().map(|b| format!("{b:02x}")).collect();
        Self {
            key: format!("{kind}/{hash}"),
            byte_len: bytes.len() as u64,
            sha256,
        }
    }

    pub(crate) fn verify(&self, bytes: &[u8]) -> Result<(), ArchiveError> {
        if self.byte_len != bytes.len() as u64
            || self.sha256 != <[u8; 32]>::from(Sha256::digest(bytes))
        {
            return Err(ArchiveError::Corrupt(self.key.clone()));
        }
        Ok(())
    }
}

/// Archive failures never authorize eviction of hot data.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ArchiveError {
    /// An object referenced by a committed catalog is missing.
    #[error("archive object not found: {0}")]
    NotFound(String),
    /// Retryable service failure, possibly after a successful write.
    #[error("archive unavailable: {0}")]
    Unavailable(String),
    /// Invalid bytes, catalog structure, or conflicting immutable content.
    #[error("archive integrity or immutable-key conflict: {0}")]
    Corrupt(String),
    /// A terminal configuration, authentication, or backend failure.
    #[error("archive backend: {0}")]
    Backend(String),
}

/// Immutable storage shared by archive data and catalog pages.
///
/// `namespace` is a stable identity for the backing bucket AND prefix; it must
/// not change across restarts. Writes atomically publish complete objects.
/// Success means durable, immediately readable bytes matching the reference.
/// Retrying identical bytes is idempotent; different bytes must never replace
/// an existing key. Errors may follow successful publication. Callers retry
/// with a fresh upload stream and the same reference. Reads must not silently
/// truncate or substitute missing data. Dropping futures/streams must cancel
/// their owned tasks. No listing or deletion is needed for normal operation.
#[async_trait]
pub trait ArchiveStore: Send + Sync {
    /// Stable backing-store and prefix identity, persisted in the manifest.
    fn namespace(&self) -> &str;
    /// Atomically publish the bytes or validate an identical existing object.
    async fn put_if_absent(
        &self,
        object: &ArchiveObjectRef,
        contents: ArchiveByteStream,
    ) -> Result<(), ArchiveError>;
    /// Byte ranges are half-open. `None` requests the entire object. Partial
    /// reads do not prove a full-object digest; Chorus currently verifies whole
    /// segments before exposing any records.
    async fn read(
        &self,
        object: &ArchiveObjectRef,
        byte_range: Option<Range<u64>>,
    ) -> Result<ArchiveByteStream, ArchiveError>;
}

/// Automatic hot placement policy; archived history is retained indefinitely.
#[derive(Clone, Copy, Debug)]
pub struct ArchivePolicy {
    /// Target hot sealed count. Must be at least one: recovery still enforces
    /// the most recent seal. The target plus pipeline headroom must fit the
    /// manifest's directory budget. Active/pending objects do not count. Failures may
    /// temporarily exceed the target; actual directory capacity remains hard.
    pub keep_sealed_segments: usize,
}

#[derive(Clone)]
pub(crate) struct ArchiveConfig {
    pub store: Arc<dyn ArchiveStore>,
    pub policy: ArchivePolicy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ArchiveState {
    pub namespace: String,
    /// History predating archive activation is not promised, even when a
    /// containing archived segment happens to include some older records.
    pub start: u64,
    pub root: Option<ArchiveRoot>,
    /// Exclusive boundary whose redundant zonal copies are all confirmed gone.
    pub cleaned: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ArchivedSegment {
    pub id: String,
    pub start: u64,
    pub end: u64,
    pub crc32c: u32,
    pub object: ArchiveObjectRef,
}

/// A range- and height-qualified node reference. Format 1 is a bounded B+ tree.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ArchiveRoot {
    pub format: u32,
    pub height: u32,
    pub start: u64,
    pub end: u64,
    pub object: ArchiveObjectRef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Page {
    Leaf(Vec<ArchivedSegment>),
    Branch(Vec<ArchiveRoot>),
}

const FANOUT: usize = 32;
const MAX_PAGE_BYTES: u64 = 64 * 1024;
const MAX_HEIGHT: u32 = 16;

pub(crate) fn valid_namespace(namespace: &str) -> bool {
    // Bound the serialized form too: control characters can expand sixfold.
    !namespace.is_empty() && serde_json::to_string(namespace).is_ok_and(|s| s.len() <= 514)
}

pub(crate) fn bytes_stream(bytes: Bytes) -> ArchiveByteStream {
    futures::stream::once(async { Ok(bytes) }).boxed()
}

pub(crate) async fn read_all(
    store: &dyn ArchiveStore,
    object: &ArchiveObjectRef,
) -> Result<Bytes, ArchiveError> {
    let mut stream = store.read(object, None).await?;
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if (bytes.len() as u64).saturating_add(chunk.len() as u64) > object.byte_len {
            return Err(ArchiveError::Corrupt(object.key.clone()));
        }
        bytes.extend_from_slice(&chunk);
    }
    object.verify(&bytes)?;
    Ok(bytes.into())
}

#[derive(Clone)]
pub(crate) struct ArchiveCatalog {
    store: Arc<dyn ArchiveStore>,
}

impl ArchiveCatalog {
    pub fn new(store: Arc<dyn ArchiveStore>) -> Self {
        Self { store }
    }

    async fn load(&self, root: &ArchiveRoot) -> Result<Page, ArchiveError> {
        if root.format != 1
            || root.height > MAX_HEIGHT
            || root.start >= root.end
            || root.object.byte_len > MAX_PAGE_BYTES
        {
            return Err(ArchiveError::Corrupt("invalid catalog root".into()));
        }
        let bytes = read_all(self.store.as_ref(), &root.object).await?;
        let page: Page =
            serde_json::from_slice(&bytes).map_err(|e| ArchiveError::Corrupt(e.to_string()))?;
        let mut next = root.start;
        let count = match &page {
            Page::Leaf(entries) if root.height == 0 => {
                for entry in entries {
                    if entry.start != next || entry.start >= entry.end {
                        return Err(ArchiveError::Corrupt("noncontiguous archive leaf".into()));
                    }
                    next = entry.end;
                }
                entries.len()
            }
            Page::Branch(children) if root.height > 0 => {
                for child in children {
                    if child.format != 1
                        || child.height.checked_add(1) != Some(root.height)
                        || child.start != next
                        || child.start >= child.end
                    {
                        return Err(ArchiveError::Corrupt("invalid archive branch".into()));
                    }
                    next = child.end;
                }
                children.len()
            }
            _ => return Err(ArchiveError::Corrupt("catalog height mismatch".into())),
        };
        if count == 0 || count > FANOUT || next != root.end {
            return Err(ArchiveError::Corrupt("invalid catalog page bounds".into()));
        }
        Ok(page)
    }

    async fn write(&self, page: Page, height: u32) -> Result<ArchiveRoot, ArchiveError> {
        let (start, end) = match &page {
            Page::Leaf(entries) => (entries[0].start, entries.last().unwrap().end),
            Page::Branch(children) => (children[0].start, children.last().unwrap().end),
        };
        let bytes = Bytes::from(
            serde_json::to_vec(&page).map_err(|e| ArchiveError::Backend(e.to_string()))?,
        );
        if bytes.len() as u64 > MAX_PAGE_BYTES || height > MAX_HEIGHT {
            return Err(ArchiveError::Backend("archive catalog page limit".into()));
        }
        let object = ArchiveObjectRef::for_bytes("catalog", &bytes);
        self.store
            .put_if_absent(&object, bytes_stream(bytes))
            .await?;
        Ok(ArchiveRoot {
            format: 1,
            height,
            start,
            end,
            object,
        })
    }

    fn append<'a>(
        &'a self,
        root: &'a ArchiveRoot,
        entry: ArchivedSegment,
    ) -> BoxFuture<'a, Result<Vec<ArchiveRoot>, ArchiveError>> {
        async move {
            let page = match self.load(root).await? {
                Page::Leaf(mut entries) => {
                    entries.push(entry);
                    if entries.len() > FANOUT {
                        let last = entries.pop().unwrap();
                        return Ok(vec![
                            root.clone(),
                            self.write(Page::Leaf(vec![last]), 0).await?,
                        ]);
                    }
                    Page::Leaf(entries)
                }
                Page::Branch(mut children) => {
                    let last = children.pop().unwrap();
                    children.extend(self.append(&last, entry).await?);
                    if children.len() > FANOUT {
                        let right = children.split_off(FANOUT);
                        return Ok(vec![
                            self.write(Page::Branch(children), root.height).await?,
                            self.write(Page::Branch(right), root.height).await?,
                        ]);
                    }
                    Page::Branch(children)
                }
            };
            Ok(vec![self.write(page, root.height).await?])
        }
        .boxed()
    }

    /// Prepares a new immutable root. No control state or hot data is changed.
    pub async fn prepare_append(
        &self,
        previous: Option<&ArchiveRoot>,
        entry: ArchivedSegment,
    ) -> Result<ArchiveRoot, ArchiveError> {
        if entry.start >= entry.end || previous.is_some_and(|root| root.end != entry.start) {
            return Err(ArchiveError::Corrupt(
                "archive append is not contiguous".into(),
            ));
        }
        match previous {
            None => self.write(Page::Leaf(vec![entry]), 0).await,
            Some(root) => {
                let mut children = self.append(root, entry).await?;
                if children.len() == 1 {
                    Ok(children.remove(0))
                } else {
                    self.write(Page::Branch(children), root.height + 1).await
                }
            }
        }
    }

    /// Traversal retains at most FANOUT siblings per level, never all history.
    pub fn scan_from(
        &self,
        root: ArchiveRoot,
        start: u64,
    ) -> BoxStream<'static, Result<ArchivedSegment, ArchiveError>> {
        let catalog = self.clone();
        async_stream::try_stream! {
            let mut stack = vec![root];
            while let Some(node) = stack.pop() {
                if node.end <= start { continue; }
                match catalog.load(&node).await? {
                    Page::Leaf(entries) => for entry in entries {
                        if entry.end > start { yield entry; }
                    },
                    Page::Branch(children) => stack.extend(children.into_iter().rev()),
                }
            }
        }
        .boxed()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use futures::TryStreamExt;
    use std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Mutex,
        },
    };

    #[derive(Default)]
    pub(crate) struct MemoryArchive {
        pub objects: Mutex<HashMap<String, Bytes>>,
        pub unavailable: AtomicBool,
        pub lose_write_response: AtomicBool,
        pub reads: AtomicUsize,
    }
    #[async_trait]
    impl ArchiveStore for MemoryArchive {
        fn namespace(&self) -> &str {
            "memory/archive"
        }
        async fn put_if_absent(
            &self,
            object: &ArchiveObjectRef,
            mut contents: ArchiveByteStream,
        ) -> Result<(), ArchiveError> {
            if self.unavailable.load(Ordering::SeqCst) {
                return Err(ArchiveError::Unavailable("injected".into()));
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = contents.next().await {
                bytes.extend_from_slice(&chunk?);
            }
            object.verify(&bytes)?;
            let mut objects = self.objects.lock().unwrap();
            if let Some(existing) = objects.get(&object.key) {
                object.verify(existing)?;
            } else {
                objects.insert(object.key.clone(), bytes.into());
            }
            if self.lose_write_response.swap(false, Ordering::SeqCst) {
                return Err(ArchiveError::Unavailable("lost response".into()));
            }
            Ok(())
        }
        async fn read(
            &self,
            object: &ArchiveObjectRef,
            byte_range: Option<Range<u64>>,
        ) -> Result<ArchiveByteStream, ArchiveError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if self.unavailable.load(Ordering::SeqCst) {
                return Err(ArchiveError::Unavailable("injected".into()));
            }
            let bytes = self
                .objects
                .lock()
                .unwrap()
                .get(&object.key)
                .cloned()
                .ok_or_else(|| ArchiveError::NotFound(object.key.clone()))?;
            let range = byte_range.unwrap_or(0..bytes.len() as u64);
            if range.start > range.end || range.end > bytes.len() as u64 {
                return Err(ArchiveError::Backend("invalid range".into()));
            }
            Ok(bytes_stream(
                bytes.slice(range.start as usize..range.end as usize),
            ))
        }
    }

    fn entry(index: u64) -> ArchivedSegment {
        ArchivedSegment {
            id: index.to_string(),
            start: index * 2,
            end: index * 2 + 2,
            crc32c: 0,
            object: ArchiveObjectRef::for_bytes("segments", &index.to_be_bytes()),
        }
    }

    #[tokio::test]
    async fn archive_catalog_splits_seek_lazily_and_preserves_old_roots() {
        let store = Arc::new(MemoryArchive::default());
        let catalog = ArchiveCatalog::new(store.clone());
        let mut root = None;
        let mut old = None;
        for i in 0..1100 {
            root = Some(
                catalog
                    .prepare_append(root.as_ref(), entry(i))
                    .await
                    .unwrap(),
            );
            if i == 20 {
                old = root.clone();
            }
        }
        let root = root.unwrap();
        assert_eq!(root.height, 2);
        store.reads.store(0, Ordering::SeqCst);
        let mut stream = catalog.scan_from(root.clone(), 2198);
        assert_eq!(store.reads.load(Ordering::SeqCst), 0);
        assert_eq!(stream.try_next().await.unwrap().unwrap(), entry(1099));
        assert!(stream.try_next().await.unwrap().is_none());
        assert_eq!(store.reads.load(Ordering::SeqCst), 3);
        let historical = catalog
            .scan_from(old.unwrap(), 0)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(historical.len(), 21);
        let all = catalog
            .scan_from(root, 0)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(all, (0..1100).map(entry).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn archive_catalog_rejects_gaps_corruption_and_missing_pages() {
        let store = Arc::new(MemoryArchive::default());
        let catalog = ArchiveCatalog::new(store.clone());
        let root = catalog.prepare_append(None, entry(0)).await.unwrap();
        assert!(catalog.prepare_append(Some(&root), entry(2)).await.is_err());
        store
            .objects
            .lock()
            .unwrap()
            .insert(root.object.key.clone(), Bytes::from_static(b"corrupt"));
        assert!(matches!(
            catalog.scan_from(root.clone(), 0).try_next().await,
            Err(ArchiveError::Corrupt(_))
        ));
        store.objects.lock().unwrap().remove(&root.object.key);
        assert!(matches!(
            catalog.scan_from(root, 0).try_next().await,
            Err(ArchiveError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn archive_upload_lost_response_retries_identical_content() {
        let store = MemoryArchive::default();
        let bytes = Bytes::from_static(b"immutable");
        let object = ArchiveObjectRef::for_bytes("segments", &bytes);
        store.lose_write_response.store(true, Ordering::SeqCst);
        assert!(store
            .put_if_absent(&object, bytes_stream(bytes.clone()))
            .await
            .is_err());
        store
            .put_if_absent(&object, bytes_stream(bytes.clone()))
            .await
            .unwrap();
        assert_eq!(read_all(&store, &object).await.unwrap(), bytes);
        assert!(store
            .put_if_absent(&object, bytes_stream(Bytes::from_static(b"different")))
            .await
            .is_err());
    }
}
