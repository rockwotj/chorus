//! Ordinary regional-object I/O; no zonal append or takeover semantics.
use super::*;
use crate::archive::{ArchiveByteStream, ArchiveError, ArchiveObjectRef, ArchiveStore};
use crate::manifest_store::{
    ManifestStore, ManifestStoreError, ManifestVersion, VersionedManifest,
};
use futures::{future::BoxFuture, FutureExt, StreamExt};
use googleapis_tonic_google_storage_v2::google::storage::v2::ReadObjectRequest;
use sha2::{Digest, Sha256};
use std::ops::Range;

fn bind(factory: &GrpcReplicaFactory, object: String) -> GrpcReplica {
    GrpcReplica {
        zone: factory.zone,
        bucket: factory.bucket.clone(),
        object,
        auth: factory.auth.clone(),
        client: factory.client.clone(),
        routing_token: factory.routing_token.clone(),
        read_session: Arc::new(SessionMutex::new(None)),
        session: Arc::new(SessionSlot::new()),
    }
}

fn archive_error(error: impl std::fmt::Display) -> ArchiveError {
    ArchiveError::Backend(error.to_string())
}
fn status_error(status: Status) -> ArchiveError {
    match status.code() {
        Code::NotFound => ArchiveError::NotFound(status.to_string()),
        Code::Unavailable
        | Code::DeadlineExceeded
        | Code::ResourceExhausted
        | Code::Aborted
        | Code::FailedPrecondition => ArchiveError::Unavailable(status.to_string()),
        Code::DataLoss => ArchiveError::Corrupt(status.to_string()),
        _ => archive_error(status),
    }
}

fn manifest_read_error(error: ArchiveError) -> ManifestStoreError {
    match error {
        // A generation-pinned read can race replacement of an unversioned
        // object after stat. Re-read the new generation, never mix bodies.
        ArchiveError::Unavailable(message) | ArchiveError::NotFound(message) => {
            ManifestStoreError::Unavailable(message)
        }
        error => ManifestStoreError::Backend(error.to_string()),
    }
}

struct UploadTask(tokio::task::JoinHandle<()>);
impl Drop for UploadTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

enum WriteError {
    Conflict,
    Archive(ArchiveError),
}

impl From<ArchiveError> for WriteError {
    fn from(error: ArchiveError) -> Self {
        Self::Archive(error)
    }
}

async fn stat(replica: &GrpcReplica) -> Result<Object, Status> {
    let request = replica
        .request(GetObjectRequest {
            bucket: replica.bucket.clone(),
            object: replica.object.clone(),
            ..Default::default()
        })
        .map_err(|e| Status::internal(e.to_string()))?;
    replica
        .client
        .clone()
        .get_object(request)
        .await
        .map(|response| response.into_inner())
}

async fn read_bytes(
    replica: &GrpcReplica,
    object: Object,
    range: Range<u64>,
) -> Result<ArchiveByteStream, ArchiveError> {
    if range.start > range.end || range.end > object.size as u64 || object.size < 0 {
        return Err(ArchiveError::Backend("invalid object byte range".into()));
    }
    if range.is_empty() {
        return Ok(futures::stream::empty().boxed());
    }
    let request = replica
        .request(ReadObjectRequest {
            bucket: replica.bucket.clone(),
            object: replica.object.clone(),
            generation: object.generation,
            read_offset: range.start as i64,
            read_limit: (range.end - range.start) as i64,
            ..Default::default()
        })
        .map_err(archive_error)?;
    let mut responses = replica
        .client
        .clone()
        .read_object(request)
        .await
        .map_err(status_error)?
        .into_inner();
    Ok(async_stream::try_stream! {
        let mut received = 0u64;
        while let Some(response) = responses.message().await.map_err(status_error)? {
            if let Some(data) = response.checksummed_data {
                if data.crc32c.is_some_and(|crc| crc32c::crc32c(&data.content) != crc) {
                    Err(ArchiveError::Corrupt("archive response checksum".into()))?;
                }
                received = received.checked_add(data.content.len() as u64).ok_or_else(|| ArchiveError::Corrupt("archive length overflow".into()))?;
                if received > range.end - range.start { Err(ArchiveError::Corrupt("archive response overrun".into()))?; }
                yield data.content;
            }
        }
        if received != range.end - range.start { Err(ArchiveError::Corrupt("short archive read".into()))?; }
    }.boxed())
}

fn write_bytes(
    replica: GrpcReplica,
    generation: i64,
    expected: ArchiveObjectRef,
    mut contents: ArchiveByteStream,
) -> BoxFuture<'static, Result<Object, WriteError>> {
    async move {
    let resource = Object { bucket: replica.bucket.clone(), name: replica.object.clone(), content_type: "application/octet-stream".into(), ..Default::default() };
    let upload_error = Arc::new(std::sync::Mutex::new(None));
    let stream_error = upload_error.clone();
    let requests = async_stream::stream! {
        yield WriteObjectRequest { first_message: Some(write_object_request::FirstMessage::WriteObjectSpec(WriteObjectSpec {
            resource: Some(resource), if_generation_match: Some(generation), ..Default::default()
        })), ..Default::default() };
        let mut offset = 0u64;
        let mut digest = Sha256::new();
        while let Some(chunk) = contents.next().await {
            let chunk = match chunk { Ok(chunk) => chunk, Err(error) => { *stream_error.lock().unwrap() = Some(error); return; } };
            let mut position = 0;
            while position < chunk.len() {
                let end = (position + 256 * 1024).min(chunk.len());
                let bytes = chunk.slice(position..end);
                if offset.saturating_add(bytes.len() as u64) > expected.byte_len {
                    *stream_error.lock().unwrap() = Some(ArchiveError::Corrupt("upload exceeds reference length".into())); return;
                }
                digest.update(&bytes);
                yield WriteObjectRequest { write_offset: offset as i64, data: Some(write_object_request::Data::ChecksummedData(ChecksummedData {
                    content: bytes.clone(), crc32c: Some(crc32c::crc32c(&bytes))
                })), ..Default::default() };
                offset += bytes.len() as u64;
                position = end;
            }
        }
        if offset != expected.byte_len || <[u8;32]>::from(digest.finalize()) != expected.sha256 {
            *stream_error.lock().unwrap() = Some(ArchiveError::Corrupt("upload does not match reference".into())); return;
        }
        yield WriteObjectRequest { write_offset: offset as i64, finish_write: true, ..Default::default() };
    };
    let (tx, rx) = mpsc::channel(2);
    let mut requests = requests.boxed();
    let _upload = UploadTask(tokio::spawn(async move {
        while let Some(request) = requests.next().await {
            if tx.send(request).await.is_err() { break; }
        }
    }));
    let request = replica.request(ReceiverStream::new(rx)).map_err(archive_error)?;
    let result = replica.client.clone().write_object(request).await;
    if let Some(error) = upload_error.lock().unwrap().take() { return Err(error.into()); }
    let response = result.map_err(|status| {
        if matches!(status.code(), Code::AlreadyExists | Code::FailedPrecondition) {
            WriteError::Conflict
        } else { WriteError::Archive(status_error(status)) }
    })?.into_inner();
    match response.write_status {
        Some(write_object_response::WriteStatus::Resource(object)) => Ok(object),
        _ => Err(ArchiveError::Unavailable("write response omitted object identity".into()).into()),
    }
    }.boxed()
}

/// Immutable WAL/catalog objects in an ordinary regional GCS bucket.
#[derive(Clone)]
pub struct GcsArchiveStore {
    factory: GrpcReplicaFactory,
    prefix: String,
    namespace: String,
}

impl GcsArchiveStore {
    /// Bind a regional bucket connection to one WAL's archive prefix.
    pub fn new(factory: GrpcReplicaFactory, prefix: impl Into<String>) -> Result<Self, Error> {
        let prefix = prefix.into().trim_matches('/').to_string();
        if prefix.is_empty() {
            return Err(Error::InvalidConfig("archive prefix must not be empty"));
        }
        let namespace = format!("{}/{}", factory.bucket, prefix);
        if !crate::archive::valid_namespace(&namespace) {
            return Err(Error::InvalidConfig(
                "archive namespace exceeds its encoded byte limit",
            ));
        }
        Ok(Self {
            factory,
            prefix,
            namespace,
        })
    }

    fn replica(&self, object: &ArchiveObjectRef) -> Result<GrpcReplica, ArchiveError> {
        if object.key.is_empty()
            || object.key.starts_with('/')
            || object
                .key
                .split('/')
                .any(|part| part == ".." || part.is_empty())
            || object.byte_len > i64::MAX as u64
        {
            return Err(archive_error("invalid archive object reference"));
        }
        Ok(bind(
            &self.factory,
            format!("{}/{}", self.prefix, object.key),
        ))
    }
}

#[async_trait]
impl ArchiveStore for GcsArchiveStore {
    fn namespace(&self) -> &str {
        &self.namespace
    }
    async fn put_if_absent(
        &self,
        object: &ArchiveObjectRef,
        contents: ArchiveByteStream,
    ) -> Result<(), ArchiveError> {
        let replica = self.replica(object)?;
        match write_bytes(replica, 0, object.clone(), contents).await {
            Ok(_) => Ok(()),
            Err(WriteError::Conflict) => {
                // Check the entire existing object, not merely user metadata.
                crate::archive::read_all(self, object).await?;
                Ok(())
            }
            Err(WriteError::Archive(error)) => Err(error),
        }
    }
    async fn read(
        &self,
        object: &ArchiveObjectRef,
        byte_range: Option<Range<u64>>,
    ) -> Result<ArchiveByteStream, ArchiveError> {
        let replica = self.replica(object)?;
        let metadata = stat(&replica).await.map_err(status_error)?;
        if metadata.size < 0 || metadata.size as u64 != object.byte_len {
            return Err(ArchiveError::Corrupt(object.key.clone()));
        }
        read_bytes(&replica, metadata, byte_range.unwrap_or(0..object.byte_len)).await
    }
}

/// A larger CAS register stored in a GCS object body. Each update replaces the
/// body using the observed generation, never delete/recreate. It remains a
/// bounded control register; archive history belongs in paged catalog objects.
pub struct GcsBodyManifestStore {
    replica: GrpcReplica,
    max_directory_bytes: usize,
}

impl GcsBodyManifestStore {
    /// Bind an ordinary regional object and its advertised directory budget.
    pub fn new(
        factory: GrpcReplicaFactory,
        object: impl Into<String>,
        max_directory_bytes: usize,
    ) -> Result<Self, Error> {
        let object = object.into();
        if object.is_empty() || max_directory_bytes == 0 {
            return Err(Error::InvalidConfig(
                "body manifest requires an object and nonzero capacity",
            ));
        }
        Ok(Self {
            replica: bind(&factory, object),
            max_directory_bytes,
        })
    }

    async fn put(
        &self,
        generation: i64,
        fields: HashMap<String, String>,
    ) -> Result<VersionedManifest, ManifestStoreError> {
        let bytes = Bytes::from(
            serde_json::to_vec(&fields).map_err(|e| ManifestStoreError::Backend(e.to_string()))?,
        );
        if bytes.len() as u64 > self.max_body_bytes() {
            return Err(ManifestStoreError::Backend(
                "manifest body exceeds configured limit".into(),
            ));
        }
        let reference = ArchiveObjectRef::for_bytes("manifest", &bytes);
        let object = write_bytes(
            self.replica.clone(),
            generation,
            reference,
            crate::archive::bytes_stream(bytes),
        )
        .await
        .map_err(|e| match e {
            WriteError::Conflict => {
                if generation == 0 {
                    ManifestStoreError::AlreadyExists
                } else {
                    ManifestStoreError::Conflict
                }
            }
            WriteError::Archive(ArchiveError::Unavailable(message)) => {
                ManifestStoreError::Unavailable(message)
            }
            WriteError::Archive(error) => ManifestStoreError::Backend(error.to_string()),
        })?;
        Ok(VersionedManifest {
            version: ManifestVersion(object.generation as u64),
            fields,
        })
    }

    fn max_body_bytes(&self) -> u64 {
        // JSON escaping can expand the directory up to sixfold.
        (self.max_directory_bytes as u64)
            .saturating_mul(6)
            .saturating_add(16384)
            .min(i64::MAX as u64)
    }
}

#[async_trait]
impl ManifestStore for GcsBodyManifestStore {
    fn max_directory_bytes(&self) -> usize {
        self.max_directory_bytes
    }
    async fn read(&self) -> Result<Option<VersionedManifest>, ManifestStoreError> {
        let object = match stat(&self.replica).await {
            Ok(object) => object,
            Err(status) if status.code() == Code::NotFound => return Ok(None),
            Err(status) => return Err(manifest_read_error(status_error(status))),
        };
        let version = ManifestVersion(object.generation as u64);
        if object.size < 0 || object.size as u64 > self.max_body_bytes() {
            return Err(ManifestStoreError::Backend(
                "manifest body exceeds configured limit".into(),
            ));
        }
        let size = object.size as u64;
        let mut stream = read_bytes(&self.replica, object, 0..size)
            .await
            .map_err(manifest_read_error)?;
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            bytes.extend_from_slice(&chunk.map_err(manifest_read_error)?);
        }
        let fields = serde_json::from_slice(&bytes)
            .map_err(|e| ManifestStoreError::Backend(e.to_string()))?;
        Ok(Some(VersionedManifest { version, fields }))
    }
    async fn create(
        &self,
        fields: HashMap<String, String>,
    ) -> Result<VersionedManifest, ManifestStoreError> {
        self.put(0, fields).await
    }
    async fn update(
        &self,
        version: ManifestVersion,
        fields: HashMap<String, String>,
    ) -> Result<VersionedManifest, ManifestStoreError> {
        let generation = i64::try_from(version.0)
            .map_err(|_| ManifestStoreError::Backend("invalid GCS generation".into()))?;
        if generation == 0 {
            return Err(ManifestStoreError::Conflict);
        }
        self.put(generation, fields).await
    }
}
