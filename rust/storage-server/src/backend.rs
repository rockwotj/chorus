//! Persistence boundary under the coordination layer.
//!
//! [`crate::store::Store`] owns every coordination decision (generations,
//! epochs, sessions, offsets, preconditions) and calls a [`Backend`] only to
//! make bytes and metadata durable and to read them back. The store
//! serializes all operations on one object through a per-object lock, so a
//! backend never sees two concurrent calls for the same [`ObjectKey`]; calls
//! for different objects may interleave at any await point.
//!
//! [`MemoryBackend`] keeps everything in process memory and treats every
//! write as durable once it returns. A disk backend implements the same trait
//! with compio file I/O.

use std::cell::RefCell;
use std::collections::HashMap;
use std::io;

/// Location of an object: bucket namespace plus object name.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectKey {
    /// Bucket namespace.
    pub bucket: String,
    /// Object name within the bucket.
    pub name: String,
}

impl ObjectKey {
    /// Build a key.
    pub fn new(bucket: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            name: name.into(),
        }
    }
}

/// The durable metadata record of one object generation (the `.meta` commit
/// point of a disk backend). Byte counts of an unfinalized object live in the
/// data file, not here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMeta {
    /// Node-unique, strictly increasing generation.
    pub generation: i64,
    /// Starts at 1; bumped by finalize.
    pub metageneration: i64,
    /// Custom metadata.
    pub metadata: HashMap<String, String>,
    /// Whether the generation is finalized (immutable).
    pub finalized: bool,
    /// Final length; meaningful only when `finalized`.
    pub finalized_size: u64,
    /// CRC32C of the final bytes; meaningful only when `finalized`.
    pub finalized_crc32c: u32,
    /// Writer-exclusivity counter; never decreases for a name.
    pub writer_epoch: u64,
    /// Last data or metadata change, nanoseconds since the Unix epoch.
    pub last_modified_unix_nanos: i64,
}

/// One object found by [`Backend::recover`].
#[derive(Clone, Debug)]
pub struct RecoveredObject {
    /// Where it lives.
    pub key: ObjectKey,
    /// Its committed metadata.
    pub meta: ObjectMeta,
    /// Durable byte length (the data file length for an unfinalized object).
    pub durable_size: u64,
    /// CRC32C of the first `durable_size` bytes.
    pub durable_crc32c: u32,
}

/// Everything a backend found at startup.
#[derive(Clone, Debug, Default)]
pub struct Recovered {
    /// Greatest generation ever handed out by this node (including deleted
    /// objects), so a restart never reuses one.
    pub last_generation: i64,
    /// Live objects.
    pub objects: Vec<RecoveredObject>,
}

/// Durable storage of object bytes and metadata.
///
/// Every method is awaited by the store while it holds the object's lock, so
/// calls for one key are strictly sequential and arrive in request order.
/// A method returns only once its effect is in place; methods documented as
/// durable must not return before an fsync/fdatasync covers the change.
/// Errors surface to the client as `Internal`.
///
/// The runtime is single-threaded, so futures need not be `Send`.
#[allow(async_fn_in_trait)]
pub trait Backend {
    /// Load the durable state at startup.
    async fn recover(&self) -> io::Result<Recovered>;

    /// Durably create generation `meta.generation` of `key` holding exactly
    /// `data`, then discard any older generation of `key`. The metadata is
    /// the commit point: after a crash either the old generation or the new
    /// one (with all of `data`) is visible.
    async fn create(&self, key: &ObjectKey, meta: &ObjectMeta, data: Vec<u8>) -> io::Result<()>;

    /// Durably replace the metadata of the current generation (epoch bump,
    /// finalize). For a finalize the store has already called [`Self::sync`].
    async fn commit_meta(&self, key: &ObjectKey, meta: &ObjectMeta) -> io::Result<()>;

    /// Write `data` at `offset` of the generation's bytes. `offset` is always
    /// the current accepted length (appends only). Not necessarily durable.
    async fn write(
        &self,
        key: &ObjectKey,
        generation: i64,
        offset: u64,
        data: Vec<u8>,
    ) -> io::Result<()>;

    /// Make every byte written so far to the generation durable.
    async fn sync(&self, key: &ObjectKey, generation: i64) -> io::Result<()>;

    /// Read `len` bytes at `offset` of the generation. The range is always
    /// within what was written.
    async fn read(
        &self,
        key: &ObjectKey,
        generation: i64,
        offset: u64,
        len: usize,
    ) -> io::Result<Vec<u8>>;

    /// Durably delete the generation (bytes and metadata).
    async fn delete(&self, key: &ObjectKey, generation: i64) -> io::Result<()>;
}

/// A backend that keeps everything in memory; every write is "durable" as
/// soon as it returns. Nothing survives a restart.
#[derive(Debug, Default)]
pub struct MemoryBackend {
    data: RefCell<HashMap<(ObjectKey, i64), Vec<u8>>>,
}

impl MemoryBackend {
    /// An empty backend.
    pub fn new() -> Self {
        Self::default()
    }

    fn missing(key: &ObjectKey, generation: i64) -> io::Error {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("no data for {}/{}#{generation}", key.bucket, key.name),
        )
    }
}

impl Backend for MemoryBackend {
    async fn recover(&self) -> io::Result<Recovered> {
        Ok(Recovered::default())
    }

    async fn create(&self, key: &ObjectKey, meta: &ObjectMeta, data: Vec<u8>) -> io::Result<()> {
        let mut map = self.data.borrow_mut();
        map.retain(|(k, _), _| k != key);
        map.insert((key.clone(), meta.generation), data);
        Ok(())
    }

    async fn commit_meta(&self, _key: &ObjectKey, _meta: &ObjectMeta) -> io::Result<()> {
        Ok(())
    }

    async fn write(
        &self,
        key: &ObjectKey,
        generation: i64,
        offset: u64,
        data: Vec<u8>,
    ) -> io::Result<()> {
        let mut map = self.data.borrow_mut();
        let bytes = map
            .get_mut(&(key.clone(), generation))
            .ok_or_else(|| Self::missing(key, generation))?;
        let offset = offset as usize;
        if offset > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "write beyond the end of the data",
            ));
        }
        bytes.truncate(offset);
        bytes.extend_from_slice(&data);
        Ok(())
    }

    async fn sync(&self, _key: &ObjectKey, _generation: i64) -> io::Result<()> {
        Ok(())
    }

    async fn read(
        &self,
        key: &ObjectKey,
        generation: i64,
        offset: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        let map = self.data.borrow();
        let bytes = map
            .get(&(key.clone(), generation))
            .ok_or_else(|| Self::missing(key, generation))?;
        let start = offset as usize;
        bytes
            .get(start..start + len)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "read past the end"))
    }

    async fn delete(&self, key: &ObjectKey, generation: i64) -> io::Result<()> {
        self.data.borrow_mut().remove(&(key.clone(), generation));
        Ok(())
    }
}
