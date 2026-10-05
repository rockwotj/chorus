//! [`DiskBackend`]: object bytes and metadata in plain files, through
//! compio's io_uring file APIs.
//!
//! # Layout
//!
//! ```text
//! <data-dir>/node.meta                              node id + generation high-water mark
//! <data-dir>/<bucket-stem>/<object-stem>.meta       committed ObjectMeta (+ real names)
//! <data-dir>/<bucket-stem>/<object-stem>.<gen>.data the generation's bytes
//! ```
//!
//! Stems come from [`names::stem`] (reversible percent-encoding, or a hash for
//! empty and over-long names). Records use the checksummed rkyv framing of
//! [`record`].
//!
//! # Commit points
//!
//! Every metadata change is a "rename trick": write `<file>.tmp`, fsync it,
//! rename it over `<file>`, fsync the directory. The object's `.meta` is the
//! commit point of create, replace, epoch bumps and finalize:
//!
//! - **create** (also replace): reserve the generation in `node.meta` if it is
//!   above the high-water mark; write `<stem>.<gen>.data`, fsync it and the
//!   directory; commit `.meta`; then unlink the previous generation's data
//!   file. A crash before the `.meta` rename leaves the old object (or none)
//!   plus an orphan data file that recovery deletes; after it, the new
//!   generation with all its bytes.
//! - **append**: positional write at the tail through a cached handle; `sync`
//!   is `fdatasync`. Recovery takes an unfinalized object's durable size from
//!   its data file length and recomputes its CRC32C.
//! - **finalize**: the store syncs the data, then the `.meta` rename with
//!   `finalized = true` is the finalize marker.
//! - **delete**: unlink `.meta`, fsync the directory, then unlink the data
//!   file (and fsync again). A crash in between leaves an orphan data file,
//!   never a `.meta` naming a missing file.
//!
//! The generation counter is durable: `create` raises the high-water mark in
//! `node.meta` (in steps of [`GENERATION_RESERVE`], so most creates need no
//! extra write) before any file of the new generation exists, and recovery
//! reports it, so a restart never reuses a generation, even of a deleted
//! object.

pub mod names;
pub mod record;

#[cfg(test)]
mod tests;

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};

use compio::buf::{BufResult, IntoInner, IoBuf};
use compio::fs::{File, OpenOptions};
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use futures::lock::Mutex;

use crate::backend::{Backend, ObjectKey, ObjectMeta, Recovered, RecoveredObject};
use record::{NodeRecord, ObjectRecord};

/// File name of the node record in the data directory.
pub const NODE_META: &str = "node.meta";
/// How far past a new generation the durable high-water mark is raised, so
/// `node.meta` is rewritten at most about once per this many microseconds of
/// wall-clock generations.
pub const GENERATION_RESERVE: i64 = 10_000_000;
/// Read size when recomputing a data file's checksum at startup.
const RECOVERY_READ_BYTES: usize = 1024 * 1024;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn context(error: io::Error, what: impl std::fmt::Display) -> io::Error {
    io::Error::new(error.kind(), format!("{what}: {error}"))
}

/// Remove a file; a missing file is not an error.
async fn remove_if_exists(path: &Path) -> io::Result<()> {
    match compio::fs::remove_file(path).await {
        Err(error) if error.kind() != io::ErrorKind::NotFound => {
            Err(context(error, format_args!("remove {}", path.display())))
        }
        _ => Ok(()),
    }
}

/// fsync a directory so entries created, renamed or removed in it persist.
async fn sync_dir(dir: &Path) -> io::Result<()> {
    let file = File::open(dir)
        .await
        .map_err(|e| context(e, format_args!("open dir {}", dir.display())))?;
    file.sync_all()
        .await
        .map_err(|e| context(e, format_args!("fsync dir {}", dir.display())))
}

/// Durably replace `dir/name` with `bytes` (write tmp, fsync, rename, fsync
/// the directory).
async fn write_atomically(dir: &Path, name: &str, bytes: Vec<u8>) -> io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    let target = dir.join(name);
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)
        .await
        .map_err(|e| context(e, format_args!("create {}", tmp.display())))?;
    let BufResult(result, _) = (&file).write_all_at(bytes, 0).await;
    result.map_err(|e| context(e, format_args!("write {}", tmp.display())))?;
    file.sync_all()
        .await
        .map_err(|e| context(e, format_args!("fsync {}", tmp.display())))?;
    drop(file);
    compio::fs::rename(&tmp, &target)
        .await
        .map_err(|e| context(e, format_args!("rename {}", tmp.display())))?;
    sync_dir(dir).await
}

/// Read `len` bytes at `offset` of `file`.
async fn read_exact(file: &File, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    if len == 0 {
        return Ok(Vec::new());
    }
    let buf = Vec::with_capacity(len).slice(0..len);
    let BufResult(result, buf) = file.read_exact_at(buf, offset).await;
    result?;
    Ok(buf.into_inner())
}

/// Length and CRC32C of a whole file, read through compio.
async fn checksum_file(path: &Path) -> io::Result<(u64, u32)> {
    let file = File::open(path).await?;
    let len = file.metadata().await?.len();
    let mut crc = 0;
    let mut offset = 0;
    while offset < len {
        let chunk = (len - offset).min(RECOVERY_READ_BYTES as u64) as usize;
        let bytes = read_exact(&file, offset, chunk).await?;
        crc = crc32c::crc32c_append(crc, &bytes);
        offset += chunk as u64;
    }
    Ok((len, crc))
}

/// The files found in one bucket directory, by object stem.
#[derive(Default)]
struct BucketScan {
    metas: Vec<(String, PathBuf)>,
    data: Vec<(String, i64, PathBuf)>,
    temps: Vec<PathBuf>,
}

fn scan_bucket(dir: &Path) -> io::Result<BucketScan> {
    let mut scan = BucketScan::default();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let Some(file_name) = entry.file_name().to_str().map(str::to_string) else {
            tracing::warn!(path = %path.display(), "ignoring a non-UTF-8 file name");
            continue;
        };
        if file_name.ends_with(".tmp") {
            scan.temps.push(path);
        } else if let Some(stem) = file_name.strip_suffix(".meta") {
            if names::is_stem(stem) {
                scan.metas.push((stem.to_string(), path));
            } else {
                tracing::warn!(path = %path.display(), "ignoring an unexpected file");
            }
        } else if let Some((stem, generation)) = file_name
            .strip_suffix(".data")
            .and_then(|rest| rest.rsplit_once('.'))
            .and_then(|(stem, gen)| Some((stem, gen.parse::<i64>().ok()?)))
            .filter(|(stem, _)| names::is_stem(stem))
        {
            scan.data.push((stem.to_string(), generation, path));
        } else {
            tracing::warn!(path = %path.display(), "ignoring an unexpected file");
        }
    }
    Ok(scan)
}

/// Persistence in plain files under one data directory. See the module docs
/// for the layout and the crash-consistency argument.
///
/// Open write handles are cached per `(key, generation)` and closed on
/// finalize, replace and delete. All methods must run on a compio runtime.
#[derive(Debug)]
pub struct DiskBackend {
    root: PathBuf,
    node_id: String,
    /// Durable generation high-water mark (`node.meta`).
    high_water: Cell<i64>,
    /// Serializes rewrites of `node.meta`.
    node_lock: Mutex<()>,
    /// Current generation of every live object (to find the data file a
    /// replace or delete discards).
    current: RefCell<HashMap<ObjectKey, i64>>,
    /// Cached read-write handles of data files.
    files: RefCell<HashMap<(ObjectKey, i64), File>>,
    /// Bucket directories known to exist durably.
    bucket_dirs: RefCell<HashSet<String>>,
}

impl DiskBackend {
    /// Open (creating if needed) the data directory `root` and its
    /// `node.meta`. `node_id` names a new node; for an existing directory it
    /// must match the stored id (`None` accepts the stored one). A new node
    /// without a requested id gets a generated one.
    pub async fn open(root: impl Into<PathBuf>, node_id: Option<String>) -> io::Result<Self> {
        let root = root.into();
        compio::fs::create_dir_all(&root)
            .await
            .map_err(|e| context(e, format_args!("create {}", root.display())))?;
        remove_if_exists(&root.join(format!("{NODE_META}.tmp"))).await?;
        let path = root.join(NODE_META);
        let record = match compio::fs::read(&path).await {
            Ok(bytes) => {
                let record = record::decode_node(&bytes)
                    .map_err(|e| context(e, format_args!("{}", path.display())))?;
                if let Some(requested) = node_id.filter(|id| *id != record.node_id) {
                    return Err(invalid(format!(
                        "{} belongs to node {:?}, not {requested:?}",
                        root.display(),
                        record.node_id
                    )));
                }
                record
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let record = NodeRecord {
                    node_id: node_id.unwrap_or_else(generated_node_id),
                    generation_high_water: 0,
                };
                write_atomically(&root, NODE_META, record::encode_node(&record)?).await?;
                // The data directory itself may be new.
                if let Some(parent) = root.parent().filter(|p| !p.as_os_str().is_empty()) {
                    sync_dir(parent).await?;
                }
                record
            }
            Err(error) => return Err(context(error, format_args!("read {}", path.display()))),
        };
        Ok(Self {
            root,
            node_id: record.node_id,
            high_water: Cell::new(record.generation_high_water),
            node_lock: Mutex::new(()),
            current: RefCell::default(),
            files: RefCell::default(),
            bucket_dirs: RefCell::default(),
        })
    }

    /// The node id stored in `node.meta`.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// The data directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Durable generation high-water mark.
    pub fn generation_high_water(&self) -> i64 {
        self.high_water.get()
    }

    /// Number of cached open data-file handles (for tests and diagnostics).
    pub fn open_handles(&self) -> usize {
        self.files.borrow().len()
    }

    fn bucket_dir(&self, bucket: &str) -> PathBuf {
        self.root.join(names::stem(bucket))
    }

    /// The `.meta` file name of `key` within its bucket directory.
    pub fn meta_file_name(key: &ObjectKey) -> String {
        format!("{}.meta", names::stem(&key.name))
    }

    /// The data file name of `key`'s `generation` within its bucket directory.
    pub fn data_file_name(key: &ObjectKey, generation: i64) -> String {
        format!("{}.{generation}.data", names::stem(&key.name))
    }

    /// Full path of a data file.
    pub fn data_path(&self, key: &ObjectKey, generation: i64) -> PathBuf {
        self.bucket_dir(&key.bucket)
            .join(Self::data_file_name(key, generation))
    }

    /// Full path of a `.meta` file.
    pub fn meta_path(&self, key: &ObjectKey) -> PathBuf {
        self.bucket_dir(&key.bucket).join(Self::meta_file_name(key))
    }

    /// Make sure `bucket`'s directory exists durably.
    async fn ensure_bucket_dir(&self, bucket: &str) -> io::Result<PathBuf> {
        let dir = self.bucket_dir(bucket);
        if !self.bucket_dirs.borrow().contains(bucket) {
            compio::fs::create_dir_all(&dir)
                .await
                .map_err(|e| context(e, format_args!("create {}", dir.display())))?;
            sync_dir(&self.root).await?;
            self.bucket_dirs.borrow_mut().insert(bucket.to_string());
        }
        Ok(dir)
    }

    /// Raise the durable high-water mark to cover `generation`.
    async fn reserve_generation(&self, generation: i64) -> io::Result<()> {
        if generation <= self.high_water.get() {
            return Ok(());
        }
        let _guard = self.node_lock.lock().await;
        if generation <= self.high_water.get() {
            return Ok(());
        }
        let high_water = generation.saturating_add(GENERATION_RESERVE);
        let record = NodeRecord {
            node_id: self.node_id.clone(),
            generation_high_water: high_water,
        };
        write_atomically(&self.root, NODE_META, record::encode_node(&record)?).await?;
        self.high_water.set(high_water);
        Ok(())
    }

    async fn write_meta(&self, key: &ObjectKey, meta: &ObjectMeta) -> io::Result<()> {
        let bytes = record::encode_object(&ObjectRecord::new(key, meta))?;
        let dir = self.ensure_bucket_dir(&key.bucket).await?;
        write_atomically(&dir, &Self::meta_file_name(key), bytes).await
    }

    /// The cached handle of a data file, opening (not creating) it if needed.
    async fn handle(&self, key: &ObjectKey, generation: i64) -> io::Result<File> {
        let cache_key = (key.clone(), generation);
        if let Some(file) = self.files.borrow().get(&cache_key) {
            return Ok(file.clone());
        }
        let path = self.data_path(key, generation);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|e| context(e, format_args!("open {}", path.display())))?;
        // Cache only the current generation's handle; a stale generation
        // (a sync racing a replace) is used once and closed.
        if self.current.borrow().get(key) == Some(&generation) {
            self.files.borrow_mut().insert(cache_key, file.clone());
        }
        Ok(file)
    }

    /// Drop cached handles of `key`, except `keep`.
    fn close_handles(&self, key: &ObjectKey, keep: Option<i64>) {
        self.files
            .borrow_mut()
            .retain(|(k, generation), _| k != key || Some(*generation) == keep);
    }

    /// Load one bucket directory (see [`Backend::recover`]).
    async fn recover_bucket(
        &self,
        dir_stem: &str,
        dir: &Path,
        recovered: &mut Recovered,
    ) -> io::Result<usize> {
        let scan =
            scan_bucket(dir).map_err(|e| context(e, format_args!("scan {}", dir.display())))?;
        let mut removed = 0;
        for tmp in &scan.temps {
            tracing::info!(path = %tmp.display(), "removing an uncommitted temporary file");
            remove_if_exists(tmp).await?;
            removed += 1;
        }
        let mut live: HashMap<String, i64> = HashMap::new();
        let mut bucket_name = None;
        for (stem, path) in &scan.metas {
            let bytes = compio::fs::read(path)
                .await
                .map_err(|e| context(e, format_args!("read {}", path.display())))?;
            let (key, meta) = record::decode_object(&bytes)
                .map_err(|e| context(e, format_args!("corrupt {}", path.display())))?
                .into_parts();
            if names::stem(&key.bucket) != dir_stem || names::stem(&key.name) != *stem {
                return Err(invalid(format!(
                    "{} holds {}/{}, which belongs elsewhere",
                    path.display(),
                    key.bucket,
                    key.name
                )));
            }
            let data_path = dir.join(Self::data_file_name(&key, meta.generation));
            let (durable_size, durable_crc32c) = if meta.finalized {
                let len = compio::fs::metadata(&data_path)
                    .await
                    .map_err(|e| context(e, format_args!("stat {}", data_path.display())))?
                    .len();
                if len != meta.finalized_size {
                    return Err(invalid(format!(
                        "{} is {len} bytes but its object was finalized at {}",
                        data_path.display(),
                        meta.finalized_size
                    )));
                }
                (meta.finalized_size, meta.finalized_crc32c)
            } else {
                checksum_file(&data_path)
                    .await
                    .map_err(|e| context(e, format_args!("checksum {}", data_path.display())))?
            };
            recovered.last_generation = recovered.last_generation.max(meta.generation);
            live.insert(stem.clone(), meta.generation);
            bucket_name = Some(key.bucket.clone());
            self.current
                .borrow_mut()
                .insert(key.clone(), meta.generation);
            recovered.objects.push(RecoveredObject {
                key,
                meta,
                durable_size,
                durable_crc32c,
            });
        }
        for (stem, generation, path) in &scan.data {
            recovered.last_generation = recovered.last_generation.max(*generation);
            if live.get(stem) != Some(generation) {
                tracing::info!(path = %path.display(), "removing an orphan data file");
                remove_if_exists(path).await?;
                removed += 1;
            }
        }
        if removed > 0 {
            sync_dir(dir).await?;
        }
        if let Some(bucket) = bucket_name {
            self.bucket_dirs.borrow_mut().insert(bucket);
        }
        Ok(removed)
    }
}

fn generated_node_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("node-{:x}-{:x}", std::process::id(), nanos)
}

impl Backend for DiskBackend {
    /// Scan every bucket directory: delete `*.tmp` files, load and verify
    /// every `.meta`, take an unfinalized object's durable size and CRC32C
    /// from its data file, check a finalized object's length, and delete
    /// data files that no `.meta` references. Inconsistent state that a
    /// crash cannot produce (a corrupt `.meta`, a missing data file, a
    /// finalized file of the wrong length) fails startup.
    async fn recover(&self) -> io::Result<Recovered> {
        let mut recovered = Recovered {
            last_generation: self.high_water.get(),
            objects: Vec::new(),
        };
        let mut dirs = Vec::new();
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            match entry.file_name().to_str() {
                Some(stem) if names::is_stem(stem) => dirs.push((stem.to_string(), entry.path())),
                _ => tracing::warn!(path = %entry.path().display(), "ignoring a foreign directory"),
            }
        }
        dirs.sort();
        let mut removed = 0;
        for (stem, dir) in &dirs {
            removed += self.recover_bucket(stem, dir, &mut recovered).await?;
        }
        let unfinalized = recovered
            .objects
            .iter()
            .filter(|object| !object.meta.finalized)
            .count();
        tracing::info!(
            data_dir = %self.root.display(),
            buckets = dirs.len(),
            objects = recovered.objects.len(),
            unfinalized,
            removed_files = removed,
            last_generation = recovered.last_generation,
            "disk recovery complete"
        );
        Ok(recovered)
    }

    async fn create(&self, key: &ObjectKey, meta: &ObjectMeta, data: Vec<u8>) -> io::Result<()> {
        let generation = meta.generation;
        self.reserve_generation(generation).await?;
        let dir = self.ensure_bucket_dir(&key.bucket).await?;
        let path = dir.join(Self::data_file_name(key, generation));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await
            .map_err(|e| context(e, format_args!("create {}", path.display())))?;
        if !data.is_empty() {
            let BufResult(result, _) = (&file).write_all_at(data, 0).await;
            result.map_err(|e| context(e, format_args!("write {}", path.display())))?;
        }
        file.sync_all()
            .await
            .map_err(|e| context(e, format_args!("fsync {}", path.display())))?;
        // The data file's directory entry must be durable before a .meta can
        // name it.
        sync_dir(&dir).await?;
        self.write_meta(key, meta).await?;
        // Committed. The previous generation is garbage now; recovery
        // removes it if this cleanup does not finish.
        let previous = self.current.borrow_mut().insert(key.clone(), generation);
        self.close_handles(key, None);
        self.files
            .borrow_mut()
            .insert((key.clone(), generation), file);
        if let Some(previous) = previous.filter(|&p| p != generation) {
            let old = dir.join(Self::data_file_name(key, previous));
            if let Err(error) = remove_if_exists(&old).await {
                tracing::warn!(%error, "cannot remove a replaced data file");
            }
        }
        Ok(())
    }

    async fn commit_meta(&self, key: &ObjectKey, meta: &ObjectMeta) -> io::Result<()> {
        self.write_meta(key, meta).await?;
        if meta.finalized {
            // Immutable from now on; reads open the file on demand.
            self.close_handles(key, None);
        }
        Ok(())
    }

    async fn write(
        &self,
        key: &ObjectKey,
        generation: i64,
        offset: u64,
        data: Vec<u8>,
    ) -> io::Result<()> {
        let file = self.handle(key, generation).await?;
        let BufResult(result, _) = (&file).write_all_at(data, offset).await;
        if let Err(error) = result {
            // A partial write must not later pass for appended bytes (recovery
            // trusts the file length): cut the file back to the accepted tail.
            if let Err(truncate) = file.set_len(offset).await {
                tracing::warn!(%truncate, "cannot truncate after a failed write");
            }
            return Err(context(
                error,
                format_args!("write {}", self.data_path(key, generation).display()),
            ));
        }
        Ok(())
    }

    async fn sync(&self, key: &ObjectKey, generation: i64) -> io::Result<()> {
        let file = self.handle(key, generation).await?;
        file.sync_data().await.map_err(|e| {
            context(
                e,
                format_args!("fdatasync {}", self.data_path(key, generation).display()),
            )
        })
    }

    async fn read(
        &self,
        key: &ObjectKey,
        generation: i64,
        offset: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let cached = self.files.borrow().get(&(key.clone(), generation)).cloned();
        let path = self.data_path(key, generation);
        let file = match cached {
            Some(file) => file,
            None => File::open(&path)
                .await
                .map_err(|e| context(e, format_args!("open {}", path.display())))?,
        };
        read_exact(&file, offset, len)
            .await
            .map_err(|e| context(e, format_args!("read {}", path.display())))
    }

    async fn delete(&self, key: &ObjectKey, generation: i64) -> io::Result<()> {
        self.close_handles(key, None);
        let dir = self.bucket_dir(&key.bucket);
        // The .meta goes first (durably), so a crash never leaves a .meta
        // naming a missing data file.
        remove_if_exists(&dir.join(Self::meta_file_name(key))).await?;
        sync_dir(&dir).await?;
        self.current.borrow_mut().remove(key);
        remove_if_exists(&dir.join(Self::data_file_name(key, generation))).await?;
        sync_dir(&dir).await
    }
}
