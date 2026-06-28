//! The manifest register: the Chorus control plane.
//!
//! The register is one tiny versioned document behind the pluggable
//! [`crate::ManifestStore`] interface. The default backend keeps it as
//! object metadata on one `<prefix>/manifest` object in a **regional** GCS
//! bucket: created once with `if_generation_match=0` and a constant empty
//! body, afterwards mutated only by metageneration-guarded `UpdateObject`. GCS gives every bucket strongly consistent reads and
//! preconditions, and a regional bucket is replicated across zones by the
//! provider, so that single object is a linearizable, zone-fault-tolerant
//! compare-and-swap register. Every control-plane decision the log takes —
//! granting a writer epoch, committing a recovery view, preregistering or
//! folding a pending segment, sealing a segment, and advancing the truncation
//! floor — is one CAS on the register, which makes each decision unique
//! without any client-side quorum or consensus round.
//! A single preregistered successor (`chorus.pending_id`) keeps that CAS out
//! of the append rotation path: rotation consumes the already-created object
//! in memory, then background maintenance folds the sealed tail and refills
//! the pending slot in one off-path CAS.
//!
//! The register is never deleted or recreated, so its history cannot be
//! erased by data repair. The data plane (segment appends, acknowledgments,
//! takeover and finalization fencing) stays entirely on the zonal Rapid
//! buckets; the register sees only rare control transitions where its
//! latency is irrelevant.
//!
//! The register also carries the sealed segment directory
//! (`chorus.segments`): every committed seal joins it in the seal's own CAS,
//! and truncation removes an entry only after the segment's copy is
//! confirmed deleted on every zone. The directory is the chain authority —
//! recovery adopts it without listing buckets or re-reading sealed bytes —
//! and its byte budget caps how many sealed segments the WAL retains.

use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;

use crate::manifest_store::{ManifestStore, ManifestStoreError, ManifestVersion};
use crate::metrics::Metrics;
use crate::protocol::{retry_sleep, ClientConfig, ProtocolError, SUPPORTED_REPLICA_COUNTS};

/// Object name of the control register, relative to the WAL prefix.
pub(crate) const MANIFEST_OBJECT: &str = "manifest";

const META_FORMAT: &str = "chorus.format";
/// The one supported register format. Structural validation does the real
/// gating: a register without the authoritative `chorus.segments` directory,
/// a directory entry that lacks its CRC32C, or the ordered `chorus.buckets`
/// replica binding is rejected at decode rather than migrated. Development
/// builds with those older shapes carried this same marker, so old binaries
/// must not share a register with new ones.
const FORMAT_VERSION: &str = "1";
const META_EPOCH: &str = "chorus.epoch";
const META_OWNER: &str = "chorus.owner";
const META_TAIL_BASE: &str = "chorus.tail_base";
const META_TAIL_ID: &str = "chorus.tail_id";
const META_PENDING_ID: &str = "chorus.pending_id";
const META_SEAL_ID: &str = "chorus.seal_id";
const META_SEAL_BASE: &str = "chorus.seal_base";
const META_SEAL_DIGEST: &str = "chorus.seal_digest";
const META_TRUNC: &str = "chorus.trunc";
const META_SEGMENTS: &str = "chorus.segments";
const META_BUCKETS: &str = "chorus.buckets";

const MAX_CAS_ROUNDS: usize = 16;

#[derive(Clone, Debug, Eq, PartialEq)]
/// One sealed chain member recorded in the register's segment directory.
/// Ends derive from contiguity — each entry ends one record before the next
/// entry's base, and the last entry ends at `tail_base - 1` — so the encoding
/// cannot disagree with the chain. Every entry carries the full-object
/// checksum of its exact sealed bytes.
pub(crate) struct DirectoryEntry {
    /// Opaque segment object id under `<prefix>/segments/`.
    pub id: String,
    /// First global record index of the segment.
    pub base: u64,
    /// Full-object CRC32C of the sealed bytes.
    pub crc32c: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// The manifest register contents.
pub(crate) struct ManifestRecord {
    /// Highest writer epoch granted.
    pub epoch: u64,
    /// Random incarnation id the epoch was granted to.
    pub owner: String,
    /// Base record index of the active appendable segment.
    pub tail_base: u64,
    /// Opaque object id of the active appendable segment. It is absent only in
    /// the uninitialized register; recovery commits a bootstrap id before
    /// conditionally creating the first segment.
    pub tail_id: Option<String>,
    /// One pre-created, stream-open successor authorized for the next
    /// rotation. Legacy registers omit the field and decode as no pending
    /// segment.
    pub pending_id: Option<String>,
    /// Base of the most recently sealed segment, absent until the first seal.
    pub seal_base: Option<u64>,
    /// Opaque object id of the most recently sealed segment.
    pub seal_id: Option<String>,
    /// SHA-256 hex digest of that segment's exact sealed bytes.
    pub seal_digest: Option<String>,
    /// Truncation floor: the first record index the log still retains.
    pub trunc: u64,
    /// The sealed segment directory, ordered by base: every committed seal
    /// that has not yet been deleted from all zones. Entries whose derived
    /// end lies below `trunc` are tombstones — chain history already
    /// truncated, retained only until every zonal copy is confirmed gone, so
    /// startup and periodic maintenance sweep a zone that slept through the
    /// original deletion pass without relisting the bucket.
    pub segments: Vec<DirectoryEntry>,
    /// Ordered zonal bucket names. The list length is the replica count used
    /// for quorum membership, so the field is required and exact identity and
    /// order are checked on every open.
    pub buckets: Vec<String>,
}

fn validate_bucket_names(buckets: &[String]) -> Result<(), ProtocolError> {
    if buckets.is_empty() {
        return Err(ProtocolError::InvalidManifest(
            "chorus.buckets must not be empty".into(),
        ));
    }
    if !SUPPORTED_REPLICA_COUNTS.contains(&buckets.len()) {
        return Err(ProtocolError::InvalidManifest(format!(
            "chorus.buckets has unsupported replica count {}; expected 1, 3, or 5",
            buckets.len()
        )));
    }
    let mut unique = HashSet::new();
    for bucket in buckets {
        if bucket.is_empty() || bucket.contains(',') {
            return Err(ProtocolError::InvalidManifest(format!(
                "invalid bucket name {bucket:?}"
            )));
        }
        if !unique.insert(bucket.as_str()) {
            return Err(ProtocolError::InvalidManifest(format!(
                "chorus.buckets repeats bucket {bucket}"
            )));
        }
    }
    Ok(())
}

impl ManifestRecord {
    fn initial(buckets: Vec<String>) -> Self {
        Self {
            epoch: 0,
            owner: String::new(),
            tail_base: 0,
            tail_id: None,
            pending_id: None,
            seal_base: None,
            seal_id: None,
            seal_digest: None,
            trunc: 0,
            segments: Vec::new(),
            buckets,
        }
    }

    /// Encoded `chorus.segments` value: `id:base:crc32c` entries joined by
    /// commas, in base order. CRC32C is exactly eight lowercase hexadecimal
    /// characters.
    fn encode_segments(segments: &[DirectoryEntry]) -> String {
        segments
            .iter()
            .map(|entry| format!("{}:{}:{:08x}", entry.id, entry.base, entry.crc32c))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn decode_segments(value: &str) -> Result<Vec<DirectoryEntry>, ProtocolError> {
        let mut segments = Vec::new();
        if value.is_empty() {
            return Ok(segments);
        }
        for part in value.split(',') {
            let invalid =
                || ProtocolError::InvalidManifest(format!("invalid chorus.segments entry {part}"));
            let mut fields = part.split(':');
            let id = fields.next().ok_or_else(invalid)?;
            let base = fields.next().ok_or_else(invalid)?;
            let crc = fields.next().ok_or_else(invalid)?;
            if crc.len() != 8
                || !crc
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(invalid());
            }
            let crc32c = u32::from_str_radix(crc, 16).map_err(|_| invalid())?;
            if fields.next().is_some() {
                return Err(invalid());
            }
            if id.is_empty() {
                return Err(invalid());
            }
            let base: u64 = base.parse().map_err(|_| invalid())?;
            if segments
                .last()
                .is_some_and(|previous: &DirectoryEntry| previous.base >= base)
            {
                return Err(ProtocolError::InvalidManifest(
                    "chorus.segments bases must be strictly increasing".into(),
                ));
            }
            segments.push(DirectoryEntry {
                id: id.to_string(),
                base,
                crc32c,
            });
        }
        Ok(segments)
    }

    fn decode_buckets(value: &str) -> Result<Vec<String>, ProtocolError> {
        if value.is_empty() {
            return Err(ProtocolError::InvalidManifest(
                "chorus.buckets must not be empty".into(),
            ));
        }
        Ok(value.split(',').map(str::to_string).collect())
    }

    fn encode(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::from([
            (META_FORMAT.to_string(), FORMAT_VERSION.to_string()),
            (META_EPOCH.to_string(), self.epoch.to_string()),
            (META_OWNER.to_string(), self.owner.clone()),
            (META_TAIL_BASE.to_string(), self.tail_base.to_string()),
            (META_TRUNC.to_string(), self.trunc.to_string()),
            (
                META_SEGMENTS.to_string(),
                Self::encode_segments(&self.segments),
            ),
            (META_BUCKETS.to_string(), self.buckets.join(",")),
        ]);
        if let Some(tail_id) = &self.tail_id {
            metadata.insert(META_TAIL_ID.to_string(), tail_id.clone());
        }
        if let Some(pending_id) = &self.pending_id {
            metadata.insert(META_PENDING_ID.to_string(), pending_id.clone());
        }
        if let Some(seal_base) = self.seal_base {
            metadata.insert(META_SEAL_BASE.to_string(), seal_base.to_string());
        }
        if let Some(seal_id) = &self.seal_id {
            metadata.insert(META_SEAL_ID.to_string(), seal_id.clone());
        }
        if let Some(digest) = &self.seal_digest {
            metadata.insert(META_SEAL_DIGEST.to_string(), digest.clone());
        }
        metadata
    }

    fn decode(metadata: &HashMap<String, String>) -> Result<Self, ProtocolError> {
        if metadata.get(META_FORMAT).map(String::as_str) != Some(FORMAT_VERSION) {
            return Err(ProtocolError::InvalidManifest(
                "manifest does not use chorus.format=1".into(),
            ));
        }
        let segments = Self::decode_segments(metadata.get(META_SEGMENTS).ok_or_else(|| {
            ProtocolError::InvalidManifest("manifest lacks chorus.segments".into())
        })?)?;
        let field = |key: &str| -> Result<u64, ProtocolError> {
            metadata
                .get(key)
                .ok_or_else(|| ProtocolError::InvalidManifest(format!("manifest lacks {key}")))?
                .parse()
                .map_err(|_| ProtocolError::InvalidManifest(format!("invalid {key}")))
        };
        let seal_base = metadata
            .get(META_SEAL_BASE)
            .map(|value| {
                value
                    .parse()
                    .map_err(|_| ProtocolError::InvalidManifest("invalid chorus.seal_base".into()))
            })
            .transpose()?;
        let seal_digest = metadata.get(META_SEAL_DIGEST).cloned();
        let buckets = Self::decode_buckets(metadata.get(META_BUCKETS).ok_or_else(|| {
            ProtocolError::InvalidManifest("manifest lacks chorus.buckets".into())
        })?)?;
        let record = Self {
            epoch: field(META_EPOCH)?,
            owner: metadata.get(META_OWNER).cloned().unwrap_or_default(),
            tail_base: field(META_TAIL_BASE)?,
            tail_id: metadata.get(META_TAIL_ID).cloned(),
            pending_id: metadata.get(META_PENDING_ID).cloned(),
            seal_base,
            seal_id: metadata.get(META_SEAL_ID).cloned(),
            seal_digest,
            trunc: field(META_TRUNC)?,
            segments,
            buckets,
        };
        record.validate()?;
        Ok(record)
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        let invalid = |message: String| ProtocolError::InvalidManifest(message);
        if self.seal_base.is_some() != self.seal_digest.is_some() {
            return Err(invalid(
                "seal_base and seal_digest must appear together".into(),
            ));
        }
        // Truncation may advance past tail_base while the active segment remains unrotated.
        if self.epoch > 0 && self.tail_id.is_none() {
            return Err(invalid(
                "chorus.tail_id must be present once chorus.epoch is nonzero".into(),
            ));
        }
        let mut ids = HashSet::new();
        let mut previous_base = None;
        for entry in &self.segments {
            if previous_base.is_some_and(|base| base >= entry.base) {
                return Err(invalid(
                    "chorus.segments bases must be strictly increasing".into(),
                ));
            }
            if entry.base >= self.tail_base {
                return Err(invalid(format!(
                    "chorus.segments entry {} has base {} at or past chorus.tail_base {}",
                    entry.id, entry.base, self.tail_base
                )));
            }
            if !ids.insert(entry.id.as_str()) {
                return Err(invalid(format!(
                    "chorus.segments repeats segment id {}",
                    entry.id
                )));
            }
            previous_base = Some(entry.base);
        }
        if self
            .tail_id
            .as_deref()
            .is_some_and(|tail_id| ids.contains(tail_id))
        {
            return Err(invalid(
                "chorus.tail_id collides with a sealed segment id".into(),
            ));
        }
        if let Some(pending_id) = self.pending_id.as_deref() {
            if pending_id.is_empty() || pending_id.contains(':') || pending_id.contains(',') {
                return Err(invalid(format!("invalid chorus.pending_id {pending_id:?}")));
            }
            if self.tail_id.as_deref() == Some(pending_id) {
                return Err(invalid(
                    "chorus.pending_id collides with chorus.tail_id".into(),
                ));
            }
            if ids.contains(pending_id) {
                return Err(invalid(
                    "chorus.pending_id collides with a sealed segment id".into(),
                ));
            }
        }
        if self.tail_base > self.trunc {
            if let Some(last) = self.segments.last() {
                if self.seal_id.as_deref() != Some(last.id.as_str())
                    || self.seal_base != Some(last.base)
                {
                    return Err(invalid(format!(
                        "chorus.segments ends with {}:{} but the seal record names {:?}:{:?}",
                        last.id, last.base, self.seal_id, self.seal_base
                    )));
                }
            }
        }
        validate_bucket_names(&self.buckets)?;
        Ok(())
    }

    fn validate_removals(
        &self,
        ids: &HashSet<String>,
        witnessed_floor: u64,
    ) -> Result<(), ProtocolError> {
        if witnessed_floor > self.trunc {
            return Err(ProtocolError::InvalidManifest(format!(
                "segment removal floor {witnessed_floor} exceeds committed truncation floor {}",
                self.trunc
            )));
        }
        for (index, entry) in self.segments.iter().enumerate() {
            if !ids.contains(&entry.id) {
                continue;
            }
            let next_base = self
                .segments
                .get(index + 1)
                .map_or(self.tail_base, |next| next.base);
            let end = next_base.checked_sub(1).ok_or_else(|| {
                ProtocolError::InvalidManifest(format!(
                    "cannot derive an end for segment {} at base {}",
                    entry.id, entry.base
                ))
            })?;
            if end >= witnessed_floor {
                return Err(ProtocolError::InvalidManifest(format!(
                    "cannot remove segment {} ending at {end} at truncation floor {witnessed_floor}",
                    entry.id
                )));
            }
        }
        Ok(())
    }

    /// Whether the directory can take `additional` more entries within the
    /// backend-provided byte budget. Sized against the worst case (a
    /// full-width id and base) so the answer cannot flip between the check and
    /// the fold CAS that relies on it.
    fn directory_has_room(&self, additional: usize, max_directory_bytes: usize) -> bool {
        directory_has_room_for(
            Self::encode_segments(&self.segments).len(),
            additional,
            max_directory_bytes,
        )
    }
}

/// A random 128-bit incarnation id. Uniqueness is the only requirement; the
/// entropy comes from the OS through `RandomState`'s per-instance seeding, so
/// no clock enters protocol logic.
pub(crate) fn incarnation_id() -> String {
    let mut id = String::with_capacity(32);
    for word in 0..2u64 {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(word);
        id.push_str(&format!("{:016x}", hasher.finish()));
    }
    id
}

/// Whether a directory whose entries currently encode to `current_encoded_len`
/// bytes can take `additional` more entries within `max_directory_bytes`. Sized
/// against the worst case (a full-width id and base) so the answer cannot flip
/// between a capacity check and the fold CAS that relies on it.
pub(crate) fn directory_has_room_for(
    current_encoded_len: usize,
    additional: usize,
    max_directory_bytes: usize,
) -> bool {
    // `{epoch:016x}-{seq:08x}` id (the seq pads to 8 hex digits but is only
    // debug-asserted below u32::MAX, so budget a full u64), a u64 base in
    // decimal, an eight-digit CRC32C, and separators
    const WORST_CASE_ENTRY_BYTES: usize = 16 + 1 + 16 + 1 + 20 + 1 + 8 + 1;
    additional
        .checked_mul(WORST_CASE_ENTRY_BYTES)
        .and_then(|reserved| current_encoded_len.checked_add(reserved))
        .is_some_and(|required| required <= max_directory_bytes)
}

/// A fresh segment id under one claimed epoch.
pub(crate) fn segment_id(epoch: u64, seq: u64) -> String {
    debug_assert!(seq <= u64::from(u32::MAX));
    format!("{epoch:016x}-{seq:08x}")
}

/// The manifest register plus this process's claim state.
pub(crate) struct Manifest {
    store: Arc<dyn ManifestStore>,
    config: ClientConfig,
    owner: String,
    epoch: u64,
    bucket_names: Vec<String>,
    max_directory_bytes: usize,
    cache: (ManifestVersion, ManifestRecord),
    metrics: Arc<Metrics>,
}

#[derive(Clone)]
/// Claim-bound capability used by background provisioning and fold work.
///
/// Each operation opens a fresh register view and rechecks `(epoch, owner)`
/// inside its CAS transform. A stale worker therefore fences instead of
/// publishing state for a later incarnation.
pub(crate) struct ManifestAccess {
    store: Arc<dyn ManifestStore>,
    config: ClientConfig,
    owner: String,
    epoch: u64,
    bucket_names: Vec<String>,
    max_directory_bytes: usize,
    cache: (ManifestVersion, ManifestRecord),
    metrics: Arc<Metrics>,
}

#[derive(Clone, Debug)]
/// A register state returned by an off-path manifest mutation.
pub(crate) struct ManifestUpdate {
    version: ManifestVersion,
    record: ManifestRecord,
}

#[derive(Clone)]
/// Inputs to the atomic fold/refill transition after a pending segment is
/// consumed by rotation.
pub(crate) struct PendingFold {
    pub old_tail_id: String,
    pub old_tail_base: u64,
    pub old_tail_end: u64,
    pub old_tail_digest: String,
    pub old_tail_crc32c: u32,
    pub consumed_pending_id: String,
    /// Tail id installed by the fold. Live rotation uses the consumed pending
    /// id; recovery may install a fresh id when a quorum-only empty pending
    /// observation cannot safely reuse the old name.
    pub successor_tail_id: String,
    pub refill_pending_id: String,
}

enum CasTransform<T> {
    Done(T),
    Update {
        record: Box<ManifestRecord>,
        value: T,
    },
}

impl Manifest {
    /// Read the register, conditionally creating it with the constant
    /// initial record when missing. `replica_count` is the configured number
    /// of zonal data factories; it must equal the required `chorus.buckets`
    /// length because every quorum decision in the WAL's history is taken
    /// against that ordered membership.
    pub async fn open(
        store: Arc<dyn ManifestStore>,
        config: ClientConfig,
        metrics: Arc<Metrics>,
        replica_count: usize,
        bucket_names: Vec<String>,
    ) -> Result<Self, ProtocolError> {
        validate_bucket_names(&bucket_names)?;
        if replica_count != bucket_names.len() {
            return Err(ProtocolError::InvalidManifest(format!(
                "volume has {replica_count} replica factories but {} bucket identities",
                bucket_names.len()
            )));
        }
        let max_directory_bytes = store.max_directory_bytes();
        let initial = ManifestRecord::initial(bucket_names.clone());
        initial.validate()?;
        let mut manifest = Self {
            store,
            config,
            owner: incarnation_id(),
            epoch: 0,
            bucket_names,
            max_directory_bytes,
            // `refresh` replaces this before `open` returns. Keeping a
            // structurally valid cache makes the post-open invariant
            // type-enforced instead of relying on `expect` in protocol paths.
            cache: (ManifestVersion(0), initial),
            metrics,
        };
        manifest.refresh().await?;
        Ok(manifest)
    }

    /// Read an existing register without creating it when absent.
    ///
    /// This is the readonly-open path. It deliberately has no write fallback:
    /// a caller using it can only observe a register initialized by a writer.
    pub(crate) async fn open_existing(
        store: Arc<dyn ManifestStore>,
        config: ClientConfig,
        metrics: Arc<Metrics>,
        replica_count: usize,
        bucket_names: Vec<String>,
    ) -> Result<Option<Self>, ProtocolError> {
        validate_bucket_names(&bucket_names)?;
        if replica_count != bucket_names.len() {
            return Err(ProtocolError::InvalidManifest(format!(
                "volume has {replica_count} replica factories but {} bucket identities",
                bucket_names.len()
            )));
        }
        let max_directory_bytes = store.max_directory_bytes();
        let initial = ManifestRecord::initial(bucket_names.clone());
        initial.validate()?;
        let mut manifest = Self {
            store,
            config,
            owner: incarnation_id(),
            epoch: 0,
            bucket_names,
            max_directory_bytes,
            cache: (ManifestVersion(0), initial),
            metrics,
        };
        if manifest.refresh_existing().await? {
            Ok(Some(manifest))
        } else {
            Ok(None)
        }
    }

    fn validate_configuration(&self, record: &ManifestRecord) -> Result<(), ProtocolError> {
        if record.buckets.len() != self.bucket_names.len() {
            return Err(ProtocolError::InvalidManifest(format!(
                "manifest records {} zonal buckets but the volume is configured with {} replicas",
                record.buckets.len(),
                self.bucket_names.len()
            )));
        }
        if record.buckets != self.bucket_names {
            return Err(ProtocolError::InvalidManifest(format!(
                "manifest records buckets {:?} but the volume is configured with {:?}",
                record.buckets, self.bucket_names
            )));
        }
        Ok(())
    }

    async fn refresh(&mut self) -> Result<(), ProtocolError> {
        for attempt in 0..=self.config.max_retries {
            match self.store.read().await {
                Ok(Some(state)) => {
                    let record = ManifestRecord::decode(&state.fields)?;
                    self.validate_configuration(&record)?;
                    self.install_cache(state.version, record);
                    return Ok(());
                }
                Ok(None) => {
                    let initial = ManifestRecord::initial(self.bucket_names.clone());
                    initial.validate()?;
                    match self.store.create(initial.encode()).await {
                        Ok(state) => {
                            self.install_cache(state.version, initial);
                            return Ok(());
                        }
                        Err(ManifestStoreError::AlreadyExists | ManifestStoreError::Conflict) => {
                            continue
                        }
                        Err(ManifestStoreError::Unavailable(_)) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                Err(ManifestStoreError::Unavailable(_)) => {}
                Err(error) => return Err(error.into()),
            }
            retry_sleep(&self.config, attempt).await;
        }
        Err(ProtocolError::ManifestUnavailable)
    }

    async fn refresh_existing(&mut self) -> Result<bool, ProtocolError> {
        for attempt in 0..=self.config.max_retries {
            match self.store.read().await {
                Ok(Some(state)) => {
                    let record = ManifestRecord::decode(&state.fields)?;
                    self.validate_configuration(&record)?;
                    self.install_cache(state.version, record);
                    return Ok(true);
                }
                Ok(None) => return Ok(false),
                Err(ManifestStoreError::Unavailable(_)) => {}
                Err(error) => return Err(error.into()),
            }
            retry_sleep(&self.config, attempt).await;
        }
        Err(ProtocolError::ManifestUnavailable)
    }

    /// The current register contents.
    pub fn record(&self) -> &ManifestRecord {
        &self.cache.1
    }

    /// A cloneable, claim-bound handle for work that must not block the
    /// append engine.
    pub(crate) fn off_path_access(&self) -> Result<ManifestAccess, ProtocolError> {
        if self.epoch == 0 {
            return Err(ProtocolError::Fenced(
                "manifest background access without a claim".into(),
            ));
        }
        Ok(ManifestAccess {
            store: Arc::clone(&self.store),
            config: self.config.clone(),
            owner: self.owner.clone(),
            epoch: self.epoch,
            bucket_names: self.bucket_names.clone(),
            max_directory_bytes: self.max_directory_bytes,
            cache: self.cache.clone(),
            metrics: Arc::clone(&self.metrics),
        })
    }

    /// Adopt a state returned by an off-path mutation without allowing a
    /// delayed worker result to regress the local cache.
    pub(crate) fn install_update(&mut self, update: ManifestUpdate) {
        if update.version.0 >= self.cache.0 .0 {
            self.install_cache(update.version, update.record);
        }
    }

    fn install_cache(&mut self, version: ManifestVersion, record: ManifestRecord) {
        self.metrics
            .manifest_directory_bytes
            .set_usize(ManifestRecord::encode_segments(&record.segments).len());
        self.cache = (version, record);
    }

    /// One guarded CAS request, timed for the latency histogram.
    async fn timed_update(
        &self,
        version: ManifestVersion,
        fields: HashMap<String, String>,
    ) -> Result<ManifestVersion, ManifestStoreError> {
        let started = tokio::time::Instant::now();
        let result = self.store.update(version, fields).await;
        self.metrics
            .manifest_cas_latency
            .record_duration(started.elapsed());
        result.map(|state| state.version)
    }

    /// Apply one typed record transform through the manifest CAS register.
    ///
    /// Every mutator shares the same conflict refresh, retry exhaustion, metric
    /// accounting, validation, and cache installation. The closure contains
    /// only operation-specific protocol checks and the candidate mutation.
    async fn cas_transform<T>(
        &mut self,
        exhausted: ProtocolError,
        mut transform: impl FnMut(&ManifestRecord) -> Result<CasTransform<T>, ProtocolError>,
    ) -> Result<T, ProtocolError> {
        for _ in 0..MAX_CAS_ROUNDS {
            let (version, record) = self.cache.clone();
            let (next, value) = match transform(&record)? {
                CasTransform::Done(value) => return Ok(value),
                CasTransform::Update { record, value } => (*record, value),
            };
            next.validate()?;
            self.metrics.manifest_cas_attempts.increment();
            match self.timed_update(version, next.encode()).await {
                Ok(updated) => {
                    self.install_cache(updated, next);
                    return Ok(value);
                }
                Err(
                    error @ (ManifestStoreError::Conflict | ManifestStoreError::Unavailable(_)),
                ) => {
                    if matches!(error, ManifestStoreError::Conflict) {
                        self.metrics.manifest_cas_conflicts.increment();
                    }
                    self.refresh().await?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(exhausted)
    }

    /// Claim a fresh epoch: one CAS raising `epoch` to a value the register
    /// has never granted. The register linearizes claims, so at most one
    /// incarnation ever owns an epoch.
    pub async fn claim(&mut self) -> Result<(), ProtocolError> {
        let owner = self.owner.clone();
        let candidate = self
            .cas_transform(
                ProtocolError::Fenced(
                    "manifest epoch claim did not converge under contention".into(),
                ),
                |record| {
                    let candidate = record.epoch.checked_add(1).ok_or_else(|| {
                        ProtocolError::InvalidManifest("epoch overflowed u64".into())
                    })?;
                    let mut claimed = record.clone();
                    claimed.epoch = candidate;
                    claimed.owner = owner.clone();
                    if claimed.tail_id.is_none() {
                        claimed.tail_id = Some(segment_id(candidate, 0));
                    }
                    Ok(CasTransform::Update {
                        record: Box::new(claimed),
                        value: candidate,
                    })
                },
            )
            .await?;
        self.epoch = candidate;
        Ok(())
    }

    /// Commit a view: `tail_base` and the most recent seal, in one CAS that
    /// re-validates `(epoch, owner)` under the metageneration guard. Fails
    /// with [`ProtocolError::Fenced`] once a higher epoch has been granted.
    ///
    /// The same CAS maintains the segment directory: a commit that moves
    /// `seal_id` to a new id is a seal decision, and the sealed segment's
    /// `(id, base, crc32c)` joins `chorus.segments` in that single write — the
    /// directory is exactly the set of committed seals not yet deleted from
    /// every zone, at no additional RPC. A full directory fails the seal
    /// with [`ProtocolError::SegmentDirectoryFull`]; the engine defers
    /// rotation before reaching that state, and truncation frees entries.
    #[cfg(test)]
    pub async fn commit_view(
        &mut self,
        tail_base: u64,
        tail_id: Option<String>,
        seal_base: Option<u64>,
        seal_id: Option<String>,
        seal_digest: Option<String>,
        new_seal_crc32c: Option<u32>,
    ) -> Result<(), ProtocolError> {
        if self.epoch == 0 {
            return Err(ProtocolError::Fenced("view commit without a claim".into()));
        }
        let epoch = self.epoch;
        let owner = self.owner.clone();
        let max_directory_bytes = self.max_directory_bytes;
        self.cas_transform(ProtocolError::ManifestUnavailable, |record| {
            if record.epoch != epoch || record.owner != owner {
                return Err(ProtocolError::Fenced(
                    "a higher epoch claimed the manifest".into(),
                ));
            }
            if record.tail_base == tail_base
                && record.tail_id == tail_id
                && record.seal_base == seal_base
                && record.seal_id == seal_id
                && record.seal_digest == seal_digest
            {
                if let Some(expected_crc32c) = new_seal_crc32c {
                    let committed_crc32c = record
                        .segments
                        .last()
                        .filter(|entry| {
                            Some(entry.id.as_str()) == seal_id.as_deref()
                                && Some(entry.base) == seal_base
                        })
                        .map(|entry| entry.crc32c);
                    if committed_crc32c != Some(expected_crc32c) {
                        return Err(ProtocolError::InvalidManifest(format!(
                            "committed seal checksum {committed_crc32c:?} differs from retry {expected_crc32c:08x}"
                        )));
                    }
                }
                return Ok(CasTransform::Done(()));
            }
            let mut next = record.clone();
            next.tail_base = tail_base;
            next.tail_id = tail_id.clone();
            next.seal_base = seal_base;
            next.seal_id = seal_id.clone();
            next.seal_digest = seal_digest.clone();
            if let Some(new_seal_id) = seal_id
                .clone()
                .filter(|new_seal_id| Some(new_seal_id) != record.seal_id.as_ref())
            {
                if !next.directory_has_room(1, max_directory_bytes) {
                    return Err(ProtocolError::SegmentDirectoryFull);
                }
                let entry = DirectoryEntry {
                    id: new_seal_id,
                    base: seal_base.ok_or_else(|| {
                        ProtocolError::InvalidManifest("seal commit without a seal_base".into())
                    })?,
                    crc32c: new_seal_crc32c.ok_or_else(|| {
                        ProtocolError::InvalidManifest("new seal commit without a CRC32C".into())
                    })?,
                };
                if next
                    .segments
                    .last()
                    .is_some_and(|previous| previous.base >= entry.base)
                {
                    return Err(ProtocolError::InvalidManifest(
                        "sealed segment base regresses the directory".into(),
                    ));
                }
                next.segments.push(entry);
            } else if new_seal_crc32c.is_some() {
                return Err(ProtocolError::InvalidManifest(
                    "CRC32C supplied without a new seal".into(),
                ));
            }
            Ok(CasTransform::Update {
                record: Box::new(next),
                value: (),
            })
        })
        .await
    }

    /// Authorize one already-created successor. The segment is not eligible
    /// for rotation until this CAS succeeds or a retry re-reads the same id.
    pub(crate) async fn register_pending(
        &mut self,
        pending_id: String,
    ) -> Result<(), ProtocolError> {
        self.require_claim()?;
        let epoch = self.epoch;
        let owner = self.owner.clone();
        self.cas_transform(ProtocolError::ManifestUnavailable, |record| {
            Self::check_claim(record, epoch, &owner)?;
            match record.pending_id.as_deref() {
                Some(committed) if committed == pending_id => {
                    return Ok(CasTransform::Done(()));
                }
                Some(committed) => {
                    return Err(ProtocolError::Fenced(format!(
                        "pending segment slot already contains {committed}"
                    )));
                }
                None => {}
            }
            let mut next = record.clone();
            next.pending_id = Some(pending_id.clone());
            Ok(CasTransform::Update {
                record: Box::new(next),
                value: (),
            })
        })
        .await
    }

    /// Replace an absent or otherwise unusable empty frontier with two newly
    /// created objects. No seal is recorded because no committed record is
    /// skipped.
    pub(crate) async fn replace_empty_frontier(
        &mut self,
        expected_tail_id: Option<&str>,
        expected_pending_id: Option<&str>,
        tail_base: u64,
        active_id: String,
        pending_id: String,
    ) -> Result<(), ProtocolError> {
        self.require_claim()?;
        let epoch = self.epoch;
        let owner = self.owner.clone();
        self.cas_transform(ProtocolError::ManifestUnavailable, |record| {
            Self::check_claim(record, epoch, &owner)?;
            if record.tail_id.as_deref() == Some(active_id.as_str())
                && record.pending_id.as_deref() == Some(pending_id.as_str())
                && record.tail_base == tail_base
            {
                return Ok(CasTransform::Done(()));
            }
            if record.tail_id.as_deref() != expected_tail_id
                || record.pending_id.as_deref() != expected_pending_id
                || record.tail_base != tail_base
            {
                return Err(ProtocolError::Fenced(
                    "manifest no longer matches the empty frontier".into(),
                ));
            }
            let mut next = record.clone();
            next.tail_id = Some(active_id.clone());
            next.pending_id = Some(pending_id.clone());
            Ok(CasTransform::Update {
                record: Box::new(next),
                value: (),
            })
        })
        .await
    }

    /// Atomically fold the consumed tail into the sealed directory, advance
    /// the active tail, and refill the single pending slot.
    pub(crate) async fn fold_pending(&mut self, fold: &PendingFold) -> Result<(), ProtocolError> {
        self.require_claim()?;
        let epoch = self.epoch;
        let owner = self.owner.clone();
        let max_directory_bytes = self.max_directory_bytes;
        self.cas_transform(ProtocolError::ManifestUnavailable, |record| {
            Self::check_claim(record, epoch, &owner)?;
            if record.tail_id.as_deref() == Some(fold.successor_tail_id.as_str())
                && record.tail_base == fold.old_tail_end.saturating_add(1)
                && record.pending_id.as_deref() == Some(fold.refill_pending_id.as_str())
                && record.seal_id.as_deref() == Some(fold.old_tail_id.as_str())
                && record.seal_base == Some(fold.old_tail_base)
                && record.seal_digest.as_deref() == Some(fold.old_tail_digest.as_str())
            {
                let committed_crc32c = record
                    .segments
                    .last()
                    .filter(|entry| {
                        entry.id == fold.old_tail_id && entry.base == fold.old_tail_base
                    })
                    .map(|entry| entry.crc32c);
                if committed_crc32c != Some(fold.old_tail_crc32c) {
                    return Err(ProtocolError::InvalidManifest(format!(
                        "committed fold checksum {committed_crc32c:?} differs from retry {:08x}",
                        fold.old_tail_crc32c
                    )));
                }
                return Ok(CasTransform::Done(()));
            }
            if record.tail_id.as_deref() != Some(fold.old_tail_id.as_str())
                || record.tail_base != fold.old_tail_base
                || record.pending_id.as_deref() != Some(fold.consumed_pending_id.as_str())
            {
                return Err(ProtocolError::Fenced(
                    "manifest no longer matches the pending fold".into(),
                ));
            }
            if fold.old_tail_end.checked_add(1).is_none() {
                return Err(ProtocolError::InvalidManifest(
                    "pending fold tail end overflowed u64".into(),
                ));
            }
            if !record.directory_has_room(1, max_directory_bytes) {
                return Err(ProtocolError::SegmentDirectoryFull);
            }
            let mut next = record.clone();
            next.segments.push(DirectoryEntry {
                id: fold.old_tail_id.clone(),
                base: fold.old_tail_base,
                crc32c: fold.old_tail_crc32c,
            });
            next.tail_base = fold.old_tail_end + 1;
            next.tail_id = Some(fold.successor_tail_id.clone());
            next.pending_id = Some(fold.refill_pending_id.clone());
            next.seal_base = Some(fold.old_tail_base);
            next.seal_id = Some(fold.old_tail_id.clone());
            next.seal_digest = Some(fold.old_tail_digest.clone());
            Ok(CasTransform::Update {
                record: Box::new(next),
                value: (),
            })
        })
        .await
    }

    fn require_claim(&self) -> Result<(), ProtocolError> {
        if self.epoch == 0 {
            return Err(ProtocolError::Fenced(
                "manifest mutation without a claim".into(),
            ));
        }
        Ok(())
    }

    fn check_claim(record: &ManifestRecord, epoch: u64, owner: &str) -> Result<(), ProtocolError> {
        if record.epoch != epoch || record.owner != owner {
            return Err(ProtocolError::Fenced(
                "a higher epoch claimed the manifest".into(),
            ));
        }
        Ok(())
    }

    /// Raise the truncation floor monotonically. Any role may do this; the
    /// CAS preserves every other field.
    pub async fn raise_trunc(&mut self, floor: u64) -> Result<(), ProtocolError> {
        self.cas_transform(ProtocolError::ManifestUnavailable, |record| {
            if record.trunc >= floor {
                return Ok(CasTransform::Done(()));
            }
            let mut next = record.clone();
            next.trunc = floor;
            Ok(CasTransform::Update {
                record: Box::new(next),
                value: (),
            })
        })
        .await
    }

    /// Drop directory entries whose every zonal copy is confirmed deleted.
    /// Epoch-free like [`Self::raise_trunc`] — the truncator discipline only
    /// removes entries wholly below `witnessed_floor`, which must itself be no
    /// higher than the current committed floor. The caller must also have
    /// confirmed every zonal copy absent. Rechecking derived ends against each
    /// CAS retry prevents a future caller from dropping reachable history. Ids
    /// already absent are fine: a racing pass removed them first.
    pub async fn remove_segments(
        &mut self,
        ids: &HashSet<String>,
        witnessed_floor: u64,
    ) -> Result<(), ProtocolError> {
        if ids.is_empty() {
            return Ok(());
        }
        self.cas_transform(ProtocolError::ManifestUnavailable, |record| {
            record.validate_removals(ids, witnessed_floor)?;
            if !record.segments.iter().any(|entry| ids.contains(&entry.id)) {
                return Ok(CasTransform::Done(()));
            }
            let mut next = record.clone();
            next.segments.retain(|entry| !ids.contains(&entry.id));
            Ok(CasTransform::Update {
                record: Box::new(next),
                value: (),
            })
        })
        .await
    }

    /// The register's store handle (for epoch-free readers like the
    /// maintenance task to open their own register view).
    pub(crate) fn store(&self) -> Arc<dyn ManifestStore> {
        Arc::clone(&self.store)
    }

    pub(crate) fn bucket_names(&self) -> &[String] {
        &self.bucket_names
    }

    /// Whether the current directory can reserve entries within this
    /// manifest store's captured capacity.
    pub(crate) fn directory_has_room(&self, additional: usize) -> bool {
        self.record()
            .directory_has_room(additional, self.max_directory_bytes)
    }

    /// Re-read the register and return its current contents.
    pub async fn refreshed_record(&mut self) -> Result<ManifestRecord, ProtocolError> {
        self.refresh().await?;
        Ok(self.record().clone())
    }

    /// Re-read an existing register without ever creating a missing one.
    pub(crate) async fn refreshed_existing_record(
        &mut self,
    ) -> Result<Option<ManifestRecord>, ProtocolError> {
        if self.refresh_existing().await? {
            Ok(Some(self.record().clone()))
        } else {
            Ok(None)
        }
    }

    /// Re-read the register and confirm this process still holds the epoch.
    pub async fn validate_owner(&mut self) -> Result<(), ProtocolError> {
        self.refresh().await?;
        let record = self.record();
        if record.epoch != self.epoch || record.owner != self.owner {
            return Err(ProtocolError::Fenced(
                "a higher epoch claimed the manifest".into(),
            ));
        }
        Ok(())
    }
}

impl ManifestAccess {
    async fn open_claimed(&self) -> Result<Manifest, ProtocolError> {
        let manifest = Manifest {
            store: Arc::clone(&self.store),
            config: self.config.clone(),
            owner: self.owner.clone(),
            epoch: self.epoch,
            bucket_names: self.bucket_names.clone(),
            max_directory_bytes: self.max_directory_bytes,
            cache: self.cache.clone(),
            metrics: Arc::clone(&self.metrics),
        };
        Ok(manifest)
    }

    pub(crate) async fn register_pending(
        &self,
        pending_id: String,
    ) -> Result<ManifestUpdate, ProtocolError> {
        let mut manifest = self.open_claimed().await?;
        manifest.register_pending(pending_id).await?;
        Ok(ManifestUpdate {
            version: manifest.cache.0,
            record: manifest.cache.1,
        })
    }

    pub(crate) async fn fold_pending(
        &self,
        fold: PendingFold,
    ) -> Result<ManifestUpdate, ProtocolError> {
        let mut manifest = self.open_claimed().await?;
        manifest.fold_pending(&fold).await?;
        Ok(ManifestUpdate {
            version: manifest.cache.0,
            record: manifest.cache.1,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::manifest_store::{test_support::InMemoryManifestStore, GCS_MAX_DIRECTORY_BYTES};
    use crate::metrics::NoopMetricsRecorder;

    fn test_buckets(replicas: u64) -> Vec<String> {
        (0..replicas).map(|zone| format!("zone-{zone}")).collect()
    }

    fn initial_record(replicas: u64) -> ManifestRecord {
        ManifestRecord::initial(test_buckets(replicas))
    }

    fn valid_record() -> ManifestRecord {
        ManifestRecord {
            epoch: 7,
            owner: "abc".into(),
            tail_base: 10,
            tail_id: Some("tail".into()),
            pending_id: Some("pending".into()),
            seal_base: Some(5),
            seal_id: Some("sealed-b".into()),
            seal_digest: Some("d".repeat(64)),
            trunc: 0,
            segments: vec![
                DirectoryEntry {
                    id: "sealed-a".into(),
                    base: 0,
                    crc32c: 0x0123_4567,
                },
                DirectoryEntry {
                    id: "sealed-b".into(),
                    base: 5,
                    crc32c: 0x1234_abcd,
                },
            ],
            buckets: test_buckets(3),
        }
    }

    #[test]
    fn record_roundtrip() {
        let record = valid_record();
        assert_eq!(
            ManifestRecord::decode(&record.encode()).expect("roundtrip"),
            record
        );
    }

    #[test]
    fn manifest_without_a_segment_directory_is_rejected() {
        let mut metadata = initial_record(3).encode();
        metadata.remove("chorus.segments");
        assert!(ManifestRecord::decode(&metadata).is_err());
    }

    #[test]
    fn directory_decode_rejects_unordered_and_malformed_entries() {
        assert!(ManifestRecord::decode_segments("a:5:00000001,b:5:00000002").is_err());
        assert!(ManifestRecord::decode_segments("a:5:00000001,b:4:00000002").is_err());
        assert!(ManifestRecord::decode_segments(":5:00000000").is_err());
        assert!(ManifestRecord::decode_segments("a").is_err());
        assert!(ManifestRecord::decode_segments("a:x:00000000").is_err());
        assert!(matches!(
            ManifestRecord::decode_segments("a:0"),
            Err(ProtocolError::InvalidManifest(_))
        ));
        assert!(ManifestRecord::decode_segments("a:0:123").is_err());
        assert!(ManifestRecord::decode_segments("a:0:1234ABCD").is_err());
        assert!(ManifestRecord::decode_segments("a:0:1234abcg").is_err());
        assert!(ManifestRecord::decode_segments("a:0:1234abcd:extra").is_err());
        assert_eq!(
            ManifestRecord::decode_segments("").expect("empty"),
            Vec::new()
        );
    }

    #[test]
    fn directory_encoding_roundtrips_checksummed_entries() {
        let entries = vec![
            DirectoryEntry {
                id: "sealed-a".into(),
                base: 0,
                crc32c: 0x0123_4567,
            },
            DirectoryEntry {
                id: "sealed-b".into(),
                base: 5,
                crc32c: 0x1234_abcd,
            },
        ];
        let encoded = ManifestRecord::encode_segments(&entries);
        assert_eq!(encoded, "sealed-a:0:01234567,sealed-b:5:1234abcd");
        assert_eq!(
            ManifestRecord::decode_segments(&encoded).expect("checksummed directory"),
            entries
        );
    }

    #[test]
    fn manifest_decode_accepts_truncation_past_tail() {
        let mut record = valid_record();
        record.trunc = 11;
        assert_eq!(
            ManifestRecord::decode(&record.encode()).expect("truncation past active tail"),
            record
        );
    }

    #[test]
    fn manifest_decode_rejects_directory_base_at_tail() {
        let mut metadata = valid_record().encode();
        metadata.insert(
            META_SEGMENTS.into(),
            "sealed-a:0:01234567,sealed-b:10:1234abcd".into(),
        );
        metadata.insert(META_SEAL_BASE.into(), "10".into());
        assert!(ManifestRecord::decode(&metadata).is_err());
    }

    #[test]
    fn manifest_decode_rejects_duplicate_directory_ids() {
        let mut metadata = valid_record().encode();
        metadata.insert(
            META_SEGMENTS.into(),
            "sealed-a:0:01234567,sealed-a:5:1234abcd".into(),
        );
        metadata.insert(META_SEAL_ID.into(), "sealed-a".into());
        assert!(ManifestRecord::decode(&metadata).is_err());
    }

    #[test]
    fn manifest_decode_rejects_tail_id_in_directory() {
        let mut metadata = valid_record().encode();
        metadata.insert(META_TAIL_ID.into(), "sealed-b".into());
        assert!(ManifestRecord::decode(&metadata).is_err());
    }

    #[test]
    fn legacy_manifest_without_pending_id_decodes_as_none() {
        let mut metadata = valid_record().encode();
        metadata.remove(META_PENDING_ID);
        assert!(ManifestRecord::decode(&metadata)
            .expect("legacy manifest")
            .pending_id
            .is_none());
    }

    #[test]
    fn manifest_decode_rejects_pending_collisions_and_malformed_ids() {
        let mut metadata = valid_record().encode();
        metadata.insert(META_PENDING_ID.into(), "tail".into());
        assert!(ManifestRecord::decode(&metadata).is_err());

        let mut metadata = valid_record().encode();
        metadata.insert(META_PENDING_ID.into(), "sealed-a".into());
        assert!(ManifestRecord::decode(&metadata).is_err());

        let mut metadata = valid_record().encode();
        metadata.insert(META_PENDING_ID.into(), String::new());
        assert!(ManifestRecord::decode(&metadata).is_err());

        let mut metadata = valid_record().encode();
        metadata.insert(META_PENDING_ID.into(), "bad,id".into());
        assert!(ManifestRecord::decode(&metadata).is_err());
    }

    #[test]
    fn manifest_decode_rejects_seal_not_matching_last_directory_entry() {
        let mut metadata = valid_record().encode();
        metadata.insert(META_SEAL_ID.into(), "sealed-a".into());
        metadata.insert(META_SEAL_BASE.into(), "0".into());
        assert!(ManifestRecord::decode(&metadata).is_err());

        let mut metadata = valid_record().encode();
        metadata.insert(META_SEAL_BASE.into(), "4".into());
        assert!(ManifestRecord::decode(&metadata).is_err());
    }

    #[test]
    fn manifest_decode_rejects_nonzero_epoch_without_tail_id() {
        let mut metadata = valid_record().encode();
        metadata.remove(META_TAIL_ID);
        assert!(ManifestRecord::decode(&metadata).is_err());
    }

    #[test]
    fn all_truncated_directory_may_not_end_at_current_seal() {
        let mut record = valid_record();
        record.trunc = record.tail_base;
        record.seal_base = Some(9);
        record.seal_id = Some("already-removed".into());
        assert_eq!(
            ManifestRecord::decode(&record.encode()).expect("all-truncated manifest"),
            record
        );
    }

    #[test]
    fn directory_room_tracks_the_byte_budget() {
        let mut record = initial_record(3);
        let max_directory_bytes = InMemoryManifestStore::default().max_directory_bytes();
        assert!(record.directory_has_room(1, max_directory_bytes));
        let mut segments = Vec::new();
        let mut base = 0;
        while ManifestRecord::encode_segments(&segments).len() <= max_directory_bytes {
            segments.push(DirectoryEntry {
                id: format!("{:016x}-{:08x}", 1, segments.len()),
                base,
                crc32c: base as u32,
            });
            base += 1;
        }
        record.segments = segments;
        assert!(!record.directory_has_room(1, max_directory_bytes));
    }

    #[tokio::test]
    async fn higher_capacity_store_admits_more_directory_entries() {
        let store = Arc::new(InMemoryManifestStore::default());
        let larger_budget = store.max_directory_bytes();
        assert!(larger_budget > GCS_MAX_DIRECTORY_BYTES);

        let mut record = initial_record(3);
        record.tail_id = Some("tail".into());
        while record.directory_has_room(1, GCS_MAX_DIRECTORY_BYTES) {
            let base = record.segments.len() as u64;
            let id = format!("{:016x}-{base:08x}", 1);
            record.segments.push(DirectoryEntry {
                id: id.clone(),
                base,
                crc32c: base as u32,
            });
            record.tail_base = base + 1;
            record.seal_base = Some(base);
            record.seal_id = Some(id);
            record.seal_digest = Some("d".repeat(64));
        }
        assert!(!record.directory_has_room(1, GCS_MAX_DIRECTORY_BYTES));
        assert!(record.directory_has_room(1, larger_budget));

        store
            .create(record.encode())
            .await
            .expect("seed higher-capacity manifest");
        let store: Arc<dyn ManifestStore> = store;
        let metrics = Arc::new(Metrics::new(&NoopMetricsRecorder, 3));
        let manifest = Manifest::open(
            store,
            ClientConfig {
                max_retries: 0,
                retry_base: Duration::ZERO,
            },
            metrics,
            3,
            test_buckets(3),
        )
        .await
        .expect("open higher-capacity manifest");

        assert!(manifest.directory_has_room(1));
    }

    #[test]
    fn initial_record_roundtrip_has_no_seal() {
        let initial = initial_record(3);
        let encoded = initial.encode();
        // the on-wire marker is part of the format contract; pin the
        // literal so a constant edit cannot drift silently
        assert_eq!(encoded.get("chorus.format").map(String::as_str), Some("1"));
        assert_eq!(
            encoded.get(META_BUCKETS).map(String::as_str),
            Some("zone-0,zone-1,zone-2")
        );
        let decoded = ManifestRecord::decode(&encoded).expect("roundtrip");
        assert_eq!(decoded, initial);
        assert!(decoded.seal_base.is_none());
        assert!(decoded.pending_id.is_none());
    }

    #[test]
    fn replica_count_derives_from_required_bucket_binding() {
        let metadata = initial_record(5).encode();
        assert!(!metadata.contains_key("chorus.replicas"));
        let decoded = ManifestRecord::decode(&metadata).expect("five-replica manifest");
        assert_eq!(decoded.buckets.len(), 5);
    }

    #[test]
    fn manifest_without_bucket_binding_is_rejected() {
        let mut metadata = initial_record(3).encode();
        metadata.remove(META_BUCKETS);
        assert!(matches!(
            ManifestRecord::decode(&metadata),
            Err(ProtocolError::InvalidManifest(_))
        ));
    }

    #[test]
    fn manifest_rejects_empty_or_unsupported_bucket_counts() {
        let mut metadata = initial_record(3).encode();
        metadata.insert(META_BUCKETS.into(), String::new());
        assert!(matches!(
            ManifestRecord::decode(&metadata),
            Err(ProtocolError::InvalidManifest(_))
        ));

        metadata.insert(META_BUCKETS.into(), "zone-0,zone-1".into());
        assert!(matches!(
            ManifestRecord::decode(&metadata),
            Err(ProtocolError::InvalidManifest(_))
        ));
    }

    #[tokio::test]
    async fn remove_segments_rejects_entry_not_below_witnessed_floor() {
        let mut record = valid_record();
        record.trunc = 4;
        let buckets = record.buckets.clone();
        let store = Arc::new(InMemoryManifestStore::default());
        store
            .create(record.encode())
            .await
            .expect("seed manifest store");
        let store: Arc<dyn ManifestStore> = store;
        let metrics = Arc::new(Metrics::new(&NoopMetricsRecorder, 3));
        let mut manifest = Manifest::open(
            store,
            ClientConfig {
                max_retries: 0,
                retry_base: Duration::ZERO,
            },
            metrics,
            3,
            buckets,
        )
        .await
        .expect("open seeded manifest");
        let ids = HashSet::from(["sealed-a".to_string()]);

        assert!(matches!(
            manifest.remove_segments(&ids, 4).await,
            Err(ProtocolError::InvalidManifest(_))
        ));
        assert_eq!(manifest.record().segments, record.segments);
    }

    #[tokio::test]
    async fn new_seal_commit_requires_and_persists_crc32c() {
        let store = Arc::new(InMemoryManifestStore::default());
        let store: Arc<dyn ManifestStore> = store;
        let metrics = Arc::new(Metrics::new(&NoopMetricsRecorder, 3));
        let mut manifest = Manifest::open(
            store,
            ClientConfig {
                max_retries: 0,
                retry_base: Duration::ZERO,
            },
            metrics,
            3,
            test_buckets(3),
        )
        .await
        .expect("open manifest");
        manifest.claim().await.expect("claim manifest");
        let sealed_id = manifest
            .record()
            .tail_id
            .clone()
            .expect("claim names a tail");
        let successor = segment_id(manifest.epoch, 1);
        let digest = "d".repeat(64);

        assert!(matches!(
            manifest
                .commit_view(
                    1,
                    Some(successor.clone()),
                    Some(0),
                    Some(sealed_id.clone()),
                    Some(digest.clone()),
                    None,
                )
                .await,
            Err(ProtocolError::InvalidManifest(_))
        ));

        manifest
            .commit_view(
                1,
                Some(successor.clone()),
                Some(0),
                Some(sealed_id.clone()),
                Some(digest.clone()),
                Some(0x1234_abcd),
            )
            .await
            .expect("commit checksummed seal");
        assert_eq!(
            manifest.record().segments,
            vec![DirectoryEntry {
                id: sealed_id.clone(),
                base: 0,
                crc32c: 0x1234_abcd,
            }]
        );
        manifest
            .commit_view(
                1,
                Some(successor.clone()),
                Some(0),
                Some(sealed_id.clone()),
                Some(digest.clone()),
                Some(0x1234_abcd),
            )
            .await
            .expect("identical seal retry");
        assert!(matches!(
            manifest
                .commit_view(
                    1,
                    Some(successor),
                    Some(0),
                    Some(sealed_id),
                    Some(digest),
                    Some(0xfeed_beef),
                )
                .await,
            Err(ProtocolError::InvalidManifest(_))
        ));
    }

    #[test]
    fn incarnation_ids_are_unique_enough() {
        assert_ne!(incarnation_id(), incarnation_id());
        assert_eq!(incarnation_id().len(), 32);
    }
}
