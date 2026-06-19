//! Pluggable storage for the manifest control register.
//!
//! The register is one tiny versioned document holding the WAL's control
//! fields (writer epoch, tail and seal records, truncation floor). Every
//! protocol decision is a compare-and-swap on it, so the backend must be a
//! linearizable CAS register — nothing more. The default implementation
//! stores the register as object metadata in a regional GCS bucket; any
//! system offering strongly consistent reads and an atomic conditional
//! update (Firestore, Spanner, a SQL row with optimistic locking) can stand
//! in through [`ManifestStore`].

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::transport::{Replica, ReplicaSnapshot, TransportCode};

/// Opaque optimistic-concurrency token for one observed register state.
///
/// A backend supplies whatever counter expresses "the register has not
/// changed since this read" — the GCS implementation uses the object
/// metageneration (the register is never deleted or recreated, so its
/// generation is constant), a SQL implementation might use a row version.
/// Callers only thread the token from a read into the conditional update
/// that follows it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManifestVersion(pub u64);

/// One consistent read of the register: its fields and the version token
/// guarding the next conditional update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionedManifest {
    /// Token for the conditional update that follows this read.
    pub version: ManifestVersion,
    /// The register's string fields (the `chorus.*` keys).
    pub fields: HashMap<String, String>,
}

/// Why a register operation did not produce a new state.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ManifestStoreError {
    /// The register changed since the witnessed version; re-read and decide
    /// again.
    #[error("the manifest register changed since it was read")]
    Conflict,
    /// A concurrent creator initialized the register first; re-read.
    #[error("the manifest register already exists")]
    AlreadyExists,
    /// Transient backend trouble; the operation may be retried as-is.
    #[error("manifest store unavailable: {0}")]
    Unavailable(String),
    /// Terminal backend failure.
    #[error("manifest store: {0}")]
    Backend(String),
}

/// A linearizable compare-and-swap register holding the manifest.
///
/// Required semantics, on which every protocol decision rests:
///
/// - [`read`](Self::read) is strongly consistent: it observes every update
///   that completed before it started.
/// - [`create`](Self::create) initializes the register exactly once;
///   concurrent creators lose with [`ManifestStoreError::AlreadyExists`].
/// - [`update`](Self::update) atomically replaces the fields if and only if
///   the register still carries the witnessed version, and fails with
///   [`ManifestStoreError::Conflict`] otherwise. Updates are totally
///   ordered.
///
/// An update whose response is lost may have applied; callers re-read and
/// re-decide, so implementations must not retry internally in ways that
/// could apply one logical update twice under a stale version.
///
/// A Firestore document, Spanner row, or SQL row is the intended extension
/// point when an operator wants to move beyond GCS object-metadata constraints.
/// Each backend reports its own sealed-directory byte budget, so a
/// higher-capacity store can retain more directory entries than GCS metadata.
#[async_trait]
pub trait ManifestStore: Send + Sync {
    /// Maximum encoded byte length accepted for the `chorus.segments` value.
    ///
    /// This is a backend capacity property, not a protocol constant. The
    /// client reserves worst-case room before committing another directory
    /// entry and returns `SegmentDirectoryFull` rather than exceeding it.
    fn max_directory_bytes(&self) -> usize;

    /// Read the register; `None` means it has never been created.
    async fn read(&self) -> Result<Option<VersionedManifest>, ManifestStoreError>;

    /// Create the register with `fields` if it does not exist.
    async fn create(
        &self,
        fields: HashMap<String, String>,
    ) -> Result<VersionedManifest, ManifestStoreError>;

    /// Replace the fields if the register still carries `version`.
    async fn update(
        &self,
        version: ManifestVersion,
        fields: HashMap<String, String>,
    ) -> Result<VersionedManifest, ManifestStoreError>;
}

/// The default register backend: object metadata on one regional GCS
/// object, guarded by a metageneration precondition.
pub(crate) struct GcsManifestStore {
    replica: Arc<dyn Replica>,
}

/// GCS caps all custom object metadata at roughly 8 KiB. The fixed manifest
/// fields stay below 500 bytes, leaving this conservative budget for the
/// encoded sealed-segment directory.
pub(crate) const GCS_MAX_DIRECTORY_BYTES: usize = 6144;

impl GcsManifestStore {
    pub(crate) fn new(replica: Arc<dyn Replica>) -> Self {
        Self { replica }
    }

    fn versioned(snapshot: ReplicaSnapshot) -> VersionedManifest {
        VersionedManifest {
            version: ManifestVersion(snapshot.metageneration as u64),
            fields: snapshot.metadata,
        }
    }
}

fn store_error(error: crate::transport::TransportError) -> ManifestStoreError {
    match error.code {
        TransportCode::FailedPrecondition => ManifestStoreError::Conflict,
        TransportCode::AlreadyExists => ManifestStoreError::AlreadyExists,
        code if code.transient() => ManifestStoreError::Unavailable(error.to_string()),
        _ => ManifestStoreError::Backend(error.to_string()),
    }
}

#[async_trait]
impl ManifestStore for GcsManifestStore {
    fn max_directory_bytes(&self) -> usize {
        GCS_MAX_DIRECTORY_BYTES
    }

    async fn read(&self) -> Result<Option<VersionedManifest>, ManifestStoreError> {
        // The register body is permanently empty; all state and CAS
        // preconditions live in object metadata, so a stat is the read.
        match self.replica.stat().await {
            Ok(snapshot) => Ok(Some(Self::versioned(snapshot))),
            Err(error) if error.code == TransportCode::NotFound => Ok(None),
            Err(error) => Err(store_error(error)),
        }
    }

    async fn create(
        &self,
        fields: HashMap<String, String>,
    ) -> Result<VersionedManifest, ManifestStoreError> {
        match self.replica.create_register(fields).await {
            Ok(snapshot) => Ok(Self::versioned(snapshot)),
            Err(error) => Err(store_error(error)),
        }
    }

    async fn update(
        &self,
        version: ManifestVersion,
        fields: HashMap<String, String>,
    ) -> Result<VersionedManifest, ManifestStoreError> {
        match self.replica.update_register(version.0 as i64, fields).await {
            Ok(snapshot) => Ok(Self::versioned(snapshot)),
            Err(error) => Err(store_error(error)),
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::{ManifestStore, ManifestStoreError, ManifestVersion, VersionedManifest};

    /// A process-local register with exact CAS semantics, standing in for a
    /// Firestore/Spanner/SQL backend in tests.
    #[derive(Default)]
    pub(crate) struct InMemoryManifestStore {
        state: Mutex<Option<(u64, HashMap<String, String>)>>,
    }

    #[async_trait]
    impl ManifestStore for InMemoryManifestStore {
        fn max_directory_bytes(&self) -> usize {
            64 * 1024
        }

        async fn read(&self) -> Result<Option<VersionedManifest>, ManifestStoreError> {
            Ok(self
                .state
                .lock()
                .unwrap()
                .as_ref()
                .map(|(version, fields)| VersionedManifest {
                    version: ManifestVersion(*version),
                    fields: fields.clone(),
                }))
        }

        async fn create(
            &self,
            fields: HashMap<String, String>,
        ) -> Result<VersionedManifest, ManifestStoreError> {
            let mut state = self.state.lock().unwrap();
            if state.is_some() {
                return Err(ManifestStoreError::AlreadyExists);
            }
            *state = Some((1, fields.clone()));
            Ok(VersionedManifest {
                version: ManifestVersion(1),
                fields,
            })
        }

        async fn update(
            &self,
            version: ManifestVersion,
            fields: HashMap<String, String>,
        ) -> Result<VersionedManifest, ManifestStoreError> {
            let mut state = self.state.lock().unwrap();
            let Some((current, stored)) = state.as_mut() else {
                return Err(ManifestStoreError::Backend(
                    "the register was never created".into(),
                ));
            };
            if ManifestVersion(*current) != version {
                return Err(ManifestStoreError::Conflict);
            }
            *current += 1;
            *stored = fields.clone();
            Ok(VersionedManifest {
                version: ManifestVersion(*current),
                fields,
            })
        }
    }

    #[tokio::test]
    async fn in_memory_store_enforces_cas_semantics() {
        let store = InMemoryManifestStore::default();
        assert!(store.read().await.unwrap().is_none());
        let initial = store
            .create(HashMap::from([("k".to_string(), "1".to_string())]))
            .await
            .unwrap();
        assert!(matches!(
            store.create(HashMap::new()).await,
            Err(ManifestStoreError::AlreadyExists)
        ));
        let updated = store
            .update(
                initial.version,
                HashMap::from([("k".to_string(), "2".to_string())]),
            )
            .await
            .unwrap();
        assert!(matches!(
            store.update(initial.version, HashMap::new()).await,
            Err(ManifestStoreError::Conflict)
        ));
        assert_eq!(
            store.read().await.unwrap().unwrap().version,
            updated.version
        );
    }
}
