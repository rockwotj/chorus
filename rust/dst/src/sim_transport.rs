//! In-memory deterministic-simulation transport.
//!
//! Implements [`Replica`]/[`ReplicaFactory`] over a [`chorus_fake_gcs::FakeGcs`]
//! by driving its synchronous in-process session API directly — no tonic, no
//! HTTP/2, no TCP, and no spawned per-session reader task. Collapsing that
//! task soup makes the deterministic simulation reproducible bit-for-bit while
//! still exercising the client's protocol logic unchanged.
//!
//! Fault injection, latency, takeover, finalization, and hold semantics are
//! reused verbatim from the fake's gRPC handler path (`apply_bidi`,
//! `apply_append_continuation`, `before*`, the hold gates), so this transport
//! and the real gRPC transport agree on behavior. The only difference is the
//! plumbing: `lane_send` is fire-and-forget — it applies the bytes and records
//! a virtual-clock `durable_at`, and `lane_durable_change` sleeps to it.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chorus_fake_gcs::proto::{
    bidi_write_object_request, bidi_write_object_response, storage_server::Storage,
    write_object_request, write_object_response, AppendObjectSpec, BidiWriteHandle,
    BidiWriteObjectRequest, ChecksummedData, DeleteObjectRequest, GetObjectRequest,
    ListObjectsRequest, Object, UpdateObjectRequest, WriteObjectRequest, WriteObjectSpec,
};
use chorus_fake_gcs::{FakeGcs, SimSessionOpen};
use tonic::{Code, Request, Status};

use chorus_client::dst_support::pack_append;
use chorus_client::{
    AppendToken, LaneDurableChange, ListedObject, PackedAppend, Replica, ReplicaFactory,
    ReplicaRangeRead, ReplicaSnapshot, TransportCode, TransportError,
};

fn map_code(status: &Status) -> TransportCode {
    match status.code() {
        Code::NotFound => TransportCode::NotFound,
        Code::AlreadyExists => TransportCode::AlreadyExists,
        Code::InvalidArgument => TransportCode::InvalidArgument,
        Code::FailedPrecondition => TransportCode::FailedPrecondition,
        Code::Aborted => TransportCode::Aborted,
        Code::OutOfRange => TransportCode::OutOfRange,
        Code::ResourceExhausted => TransportCode::ResourceExhausted,
        Code::Unimplemented => TransportCode::Unimplemented,
        Code::DataLoss => TransportCode::DataLoss,
        Code::Unauthenticated => TransportCode::Unauthenticated,
        Code::PermissionDenied => TransportCode::PermissionDenied,
        Code::Unavailable => TransportCode::Unavailable,
        Code::DeadlineExceeded => TransportCode::DeadlineExceeded,
        _ => TransportCode::Internal,
    }
}

fn err(zone: usize, code: TransportCode, message: impl Into<String>) -> TransportError {
    TransportError {
        zone,
        code,
        message: message.into(),
    }
}

fn status_err(zone: usize, status: &Status) -> TransportError {
    err(zone, map_code(status), status.message())
}

fn snapshot_from_object(zone: usize, object: Object, bytes: Vec<u8>) -> ReplicaSnapshot {
    let crc32c = object
        .checksums
        .as_ref()
        .and_then(|checksums| checksums.crc32c);
    ReplicaSnapshot {
        zone,
        generation: object.generation,
        metageneration: object.metageneration,
        persisted_size: bytes.len() as i64,
        finalized: object.finalize_time.is_some(),
        crc32c,
        metadata: object.metadata,
        bytes,
    }
}

fn stat_from_object(zone: usize, object: Object) -> ReplicaSnapshot {
    let size = object.size;
    let mut snapshot = snapshot_from_object(zone, object, Vec::new());
    if snapshot.finalized {
        snapshot.persisted_size = size;
    }
    snapshot
}

fn persisted_size_of(response: &bidi_write_object_response::WriteStatus) -> Option<i64> {
    match response {
        bidi_write_object_response::WriteStatus::PersistedSize(size) => Some(*size),
        bidi_write_object_response::WriteStatus::Resource(object) => Some(object.size),
    }
}

/// Live append session state for one in-memory lane.
///
/// `lane_send` only enqueues a flush group's wire messages; `lane_durable_change`
/// applies them to the fake (charging the per-op latency on the virtual clock)
/// and advances `durable`. Deferring the apply keeps `lane_send` fire-and-forget
/// even when the fake parks the flush (`inject_flush_hold`): the park then occurs
/// inside `lane_durable_change` without holding the session lock, so the engine's
/// stall timer fires on schedule and the lane is shed exactly as on the gRPC
/// transport.
struct Session {
    spec: AppendObjectSpec,
    stream_id: u64,
    /// Distinguishes this session from earlier ones on the same replica. A
    /// resumed session keeps its stream id, so the id alone cannot tell an
    /// apply from a replaced session apart from one for the current session.
    epoch: u64,
    durable: i64,
    pending: VecDeque<Vec<BidiWriteObjectRequest>>,
}

/// The application of one lane group: the fence it met, if any, and the
/// session's durable offset after committing it.
struct LaneApplied {
    durable: i64,
    fence: Option<TransportError>,
}

type LaneApply = Pin<Box<dyn Future<Output = Option<LaneApplied>> + Send>>;

/// In-memory replica factory backed by one [`FakeGcs`] zone.
pub struct InMemoryReplicaFactory {
    fake: FakeGcs,
    bucket: String,
    zone: usize,
}

impl InMemoryReplicaFactory {
    /// Bind a factory to one fake zonal bucket. `bucket` is the full v2
    /// resource name (`projects/_/buckets/<name>`), matching the gRPC factory.
    pub fn new(fake: FakeGcs, bucket: impl Into<String>, zone: usize) -> Self {
        Self {
            fake,
            bucket: bucket.into(),
            zone,
        }
    }
}

#[async_trait]
impl ReplicaFactory for InMemoryReplicaFactory {
    fn bucket_name(&self) -> &str {
        &self.bucket
    }

    fn replica(&self, object: &str) -> Arc<dyn Replica> {
        let (shutdown, _) = tokio::sync::watch::channel(false);
        Arc::new(InMemoryReplica {
            fake: self.fake.clone(),
            bucket: self.bucket.clone(),
            object: object.to_string(),
            zone: self.zone,
            session: Arc::new(tokio::sync::Mutex::new(None)),
            next_epoch: std::sync::atomic::AtomicU64::new(0),
            apply: tokio::sync::Mutex::new(None),
            shutdown,
        })
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ListedObject>, TransportError> {
        let request = ListObjectsRequest {
            parent: self.bucket.clone(),
            prefix: prefix.to_string(),
            ..Default::default()
        };
        let response = Storage::list_objects(&self.fake, Request::new(request))
            .await
            .map_err(|status| status_err(self.zone, &status))?
            .into_inner();
        Ok(response
            .objects
            .into_iter()
            .map(|object| ListedObject {
                zone: self.zone,
                name: object
                    .name
                    .strip_prefix(&format!("{}/", self.bucket))
                    .unwrap_or(&object.name)
                    .to_string(),
                generation: object.generation,
                size: object.size,
                finalized: object.finalize_time.is_some(),
                last_modified: object.update_time.and_then(|time| {
                    if !(-62_135_596_800..=253_402_300_799).contains(&time.seconds)
                        || !(0..1_000_000_000).contains(&time.nanos)
                    {
                        return None;
                    }
                    let seconds = std::time::Duration::from_secs(time.seconds.unsigned_abs());
                    let base = if time.seconds < 0 {
                        std::time::UNIX_EPOCH.checked_sub(seconds)
                    } else {
                        std::time::UNIX_EPOCH.checked_add(seconds)
                    }?;
                    base.checked_add(std::time::Duration::from_nanos(time.nanos as u64))
                }),
                crc32c: object
                    .checksums
                    .as_ref()
                    .and_then(|checksums| checksums.crc32c),
                metadata: object.metadata,
            })
            .collect())
    }
}

struct InMemoryReplica {
    fake: FakeGcs,
    bucket: String,
    object: String,
    zone: usize,
    session: Arc<tokio::sync::Mutex<Option<Session>>>,
    next_epoch: std::sync::atomic::AtomicU64,
    /// The group application `lane_durable_change` is driving. The lane
    /// select cancels that call whenever new work arrives, so the application
    /// lives here and resumes on the next call instead of restarting with a
    /// fresh latency charge.
    apply: tokio::sync::Mutex<Option<(u64, LaneApply)>>,
    /// Signals an in-flight open to abort, mirroring how the gRPC transport's
    /// `shutdown` closes the stream and unblocks a held open. `cancel_provision_attempt`
    /// calls `shutdown()` while joining the provision future; without this race an
    /// open parked on an injected open-hold would never return and graceful
    /// shutdown would hang past its deadline.
    shutdown: tokio::sync::watch::Sender<bool>,
}

impl InMemoryReplica {
    fn open_request_append(
        &self,
        generation: i64,
        metageneration: Option<i64>,
        write_handle: Option<Bytes>,
        write_offset: i64,
        data: Option<Vec<u8>>,
        finish_write: bool,
    ) -> BidiWriteObjectRequest {
        BidiWriteObjectRequest {
            first_message: Some(bidi_write_object_request::FirstMessage::AppendObjectSpec(
                AppendObjectSpec {
                    bucket: self.bucket.clone(),
                    object: self.object.clone(),
                    generation,
                    if_metageneration_match: metageneration,
                    write_handle: write_handle.map(|handle| BidiWriteHandle {
                        handle: handle.to_vec(),
                    }),
                    ..Default::default()
                },
            )),
            write_offset,
            data: data.map(|data| {
                bidi_write_object_request::Data::ChecksummedData(ChecksummedData {
                    crc32c: Some(crc32c::crc32c(&data)),
                    content: data,
                })
            }),
            flush: true,
            state_lookup: true,
            finish_write,
        }
    }

    /// Send `template` with `data` split exactly as the gRPC transport's
    /// one-shot writes split it: `template`'s first message and the first data
    /// piece open the stream, later pieces continue it, and only the last
    /// message keeps `template`'s flush, state_lookup and finish_write. The
    /// returned open carries the last response. Empty data sends `template`
    /// unchanged.
    async fn open_split(
        &self,
        template: BidiWriteObjectRequest,
        data: Bytes,
    ) -> Result<SimSessionOpen, TransportError> {
        let packed = pack_append(vec![data]);
        let messages = packed.messages();
        let Some(last_index) = messages.len().checked_sub(1) else {
            return self.open(template).await;
        };
        let request_for = |index: usize| {
            let message = &messages[index];
            let last = index == last_index;
            BidiWriteObjectRequest {
                first_message: if index == 0 {
                    template.first_message.clone()
                } else {
                    None
                },
                write_offset: template.write_offset + message.relative_offset(),
                data: Some(bidi_write_object_request::Data::ChecksummedData(
                    ChecksummedData {
                        crc32c: Some(message.crc32c()),
                        content: message.content().to_vec(),
                    },
                )),
                flush: template.flush && last,
                state_lookup: template.state_lookup && last,
                finish_write: template.finish_write && last,
            }
        };
        let mut open = self.open(request_for(0)).await?;
        if last_index == 0 {
            return Ok(open);
        }
        let Some((spec, stream_id)) = open.append.clone() else {
            return Err(err(
                self.zone,
                TransportCode::Internal,
                "split write opened no stream to continue",
            ));
        };
        for index in 1..=last_index {
            open.response = self
                .fake
                .sim_continue(&spec, stream_id, request_for(index), None)
                .await
                .map_err(|status| status_err(self.zone, &status))?;
        }
        Ok(open)
    }

    async fn open(&self, first: BidiWriteObjectRequest) -> Result<SimSessionOpen, TransportError> {
        // Race the open against shutdown so a `shutdown()` during graceful
        // provisioner teardown cancels an open parked on an injected open-hold
        // (dropping the `sim_open` future cancels its hold wait), exactly as the
        // gRPC transport's stream close aborts a held open. `biased` keeps the
        // poll order fixed for deterministic replay.
        let mut rx = self.shutdown.subscribe();
        if *rx.borrow() {
            return Err(err(
                self.zone,
                TransportCode::Aborted,
                "append session closed during shutdown",
            ));
        }
        tokio::select! {
            biased;
            _ = rx.changed() => Err(err(
                self.zone,
                TransportCode::Aborted,
                "append session closed during shutdown",
            )),
            result = self.fake.sim_open(first, None) => {
                result.map_err(|status| status_err(self.zone, &status))
            }
        }
    }

    fn token_from_open(
        &self,
        open: &SimSessionOpen,
        generation: Option<i64>,
    ) -> (i64, Option<Bytes>) {
        let persisted = open
            .response
            .write_status
            .as_ref()
            .and_then(persisted_size_of)
            .unwrap_or(0);
        let _ = generation;
        let write_handle = open
            .response
            .write_handle
            .as_ref()
            .map(|handle| Bytes::from(handle.handle.clone()));
        (persisted, write_handle)
    }

    async fn store_session(&self, open: &SimSessionOpen) {
        if let Some((spec, stream_id)) = open.append.clone() {
            let persisted = open
                .response
                .write_status
                .as_ref()
                .and_then(persisted_size_of)
                .unwrap_or(0);
            let epoch = self
                .next_epoch
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            *self.session.lock().await = Some(Session {
                spec,
                stream_id,
                epoch,
                durable: persisted,
                pending: VecDeque::new(),
            });
        }
    }
}

/// Apply one lane group to the fake, then commit its progress to the session
/// it was taken from. Each message charges the op's fault and latency on the
/// virtual clock, and a flush hold parks here. Only flushed messages report
/// durable progress, matching the gRPC transport, which receives a persisted
/// size only for requests that carry flush and state_lookup. Returns `None`
/// when the session was replaced or shut down before the commit.
async fn apply_lane_group(
    fake: FakeGcs,
    session: Arc<tokio::sync::Mutex<Option<Session>>>,
    spec: AppendObjectSpec,
    stream_id: u64,
    epoch: u64,
    group: Vec<BidiWriteObjectRequest>,
    zone: usize,
) -> Option<LaneApplied> {
    let mut persisted = None;
    let mut fence = None;
    for request in group {
        let flushed = request.flush;
        match fake.sim_lane_apply(&spec, stream_id, request, None).await {
            Ok((response, close)) => {
                // sim_lane_apply already slept the charged latency before
                // applying the write. Fold the applied progress first so a
                // post-response stream close still reports durable progress
                // alongside the fence (matching the gRPC lane).
                if flushed {
                    if let Some(size) = response.write_status.as_ref().and_then(persisted_size_of) {
                        persisted = Some(size);
                    }
                }
                if let Some(status) = close {
                    fence = Some(status_err(zone, &status));
                    break;
                }
            }
            Err(status) => {
                fence = Some(status_err(zone, &status));
                break;
            }
        }
    }
    let mut guard = session.lock().await;
    let session = guard.as_mut().filter(|session| session.epoch == epoch)?;
    session.pending.pop_front();
    if let Some(size) = persisted {
        session.durable = session.durable.max(size);
    }
    Some(LaneApplied {
        durable: session.durable,
        fence,
    })
}

impl InMemoryReplica {
    /// Enqueue one lane group. When `flush` is set, the final message carries
    /// flush and state_lookup, matching the gRPC lane's write group.
    async fn enqueue_lane_group(
        &self,
        write_offset: i64,
        packed: &PackedAppend,
        flush: bool,
    ) -> Result<(), TransportError> {
        if packed.messages().is_empty() {
            return Err(err(
                self.zone,
                TransportCode::Internal,
                "append lane cannot send an empty flush group",
            ));
        }
        let mut guard = self.session.lock().await;
        let session = guard.as_mut().ok_or_else(|| {
            err(
                self.zone,
                TransportCode::FailedPrecondition,
                "append session disconnected while sending",
            )
        })?;
        // Build the flush group's wire messages from the same packing the gRPC
        // lane sends; the final message carries flush + state_lookup. Only
        // enqueue here — the apply (which may park on an injected flush hold)
        // happens in `lane_durable_change`, so this stays non-blocking.
        let messages = packed.messages();
        let last_index = messages.len() - 1;
        let group = messages
            .iter()
            .enumerate()
            .map(|(index, message)| {
                let last = index == last_index;
                BidiWriteObjectRequest {
                    first_message: None,
                    write_offset: write_offset + message.relative_offset(),
                    data: Some(bidi_write_object_request::Data::ChecksummedData(
                        ChecksummedData {
                            crc32c: Some(message.crc32c()),
                            content: message.content().to_vec(),
                        },
                    )),
                    flush: flush && last,
                    state_lookup: flush && last,
                    ..Default::default()
                }
            })
            .collect();
        session.pending.push_back(group);
        Ok(())
    }
}

#[async_trait]
impl Replica for InMemoryReplica {
    async fn stat(&self) -> Result<ReplicaSnapshot, TransportError> {
        let request = GetObjectRequest {
            bucket: self.bucket.clone(),
            object: self.object.clone(),
            ..Default::default()
        };
        let object = Storage::get_object(&self.fake, Request::new(request))
            .await
            .map_err(|status| status_err(self.zone, &status))?
            .into_inner();
        Ok(stat_from_object(self.zone, object))
    }

    async fn snapshot(&self) -> Result<ReplicaSnapshot, TransportError> {
        let request = GetObjectRequest {
            bucket: self.bucket.clone(),
            object: self.object.clone(),
            ..Default::default()
        };
        let object = Storage::get_object(&self.fake, Request::new(request))
            .await
            .map_err(|status| status_err(self.zone, &status))?
            .into_inner();
        // Bind the byte read to the generation we just observed, so a
        // concurrent replacement cannot return mixed metadata + bytes.
        let generation = object.generation;
        let bytes = self
            .fake
            .sim_read_bytes(&self.bucket, &self.object, generation)
            .await
            .map_err(|status| status_err(self.zone, &status))?;
        Ok(snapshot_from_object(self.zone, object, bytes))
    }

    async fn read_range(&self, offset: i64) -> Result<ReplicaRangeRead, TransportError> {
        let (generation, bytes) = self
            .fake
            .sim_bidi_read_bytes(&self.bucket, &self.object, offset)
            .await
            .map_err(|status| status_err(self.zone, &status))?;
        Ok(ReplicaRangeRead {
            zone: self.zone,
            generation,
            bytes,
        })
    }

    async fn create_appendable(
        &self,
        metadata: HashMap<String, String>,
    ) -> Result<ReplicaSnapshot, TransportError> {
        let object = Object {
            bucket: self.bucket.clone(),
            name: self.object.clone(),
            metadata,
            content_type: "application/vnd.chorus.records".into(),
            ..Default::default()
        };
        let request = BidiWriteObjectRequest {
            first_message: Some(bidi_write_object_request::FirstMessage::WriteObjectSpec(
                WriteObjectSpec {
                    resource: Some(object),
                    if_generation_match: Some(0),
                    appendable: Some(true),
                    ..Default::default()
                },
            )),
            write_offset: 0,
            flush: true,
            state_lookup: true,
            ..Default::default()
        };
        if let Err(error) = self.open(request).await {
            if error.code == TransportCode::FailedPrecondition {
                return Err(TransportError {
                    code: TransportCode::AlreadyExists,
                    ..error
                });
            }
            return Err(error);
        }
        self.stat().await
    }

    async fn create_append_session(
        &self,
        metadata: HashMap<String, String>,
    ) -> Result<AppendToken, TransportError> {
        let object = Object {
            bucket: self.bucket.clone(),
            name: self.object.clone(),
            metadata,
            content_type: "application/vnd.chorus.records".into(),
            ..Default::default()
        };
        let request = BidiWriteObjectRequest {
            first_message: Some(bidi_write_object_request::FirstMessage::WriteObjectSpec(
                WriteObjectSpec {
                    resource: Some(object),
                    if_generation_match: Some(0),
                    appendable: Some(true),
                    ..Default::default()
                },
            )),
            write_offset: 0,
            flush: true,
            state_lookup: true,
            ..Default::default()
        };
        let open = self.open(request).await.map_err(|error| {
            if error.code == TransportCode::FailedPrecondition {
                TransportError {
                    code: TransportCode::AlreadyExists,
                    ..error
                }
            } else {
                error
            }
        })?;
        let (persisted_size, write_handle) = self.token_from_open(&open, None);
        self.store_session(&open).await;
        Ok(AppendToken {
            zone: self.zone,
            generation: None,
            metageneration: None,
            persisted_size,
            write_handle,
        })
    }

    async fn create_register(
        &self,
        metadata: HashMap<String, String>,
    ) -> Result<ReplicaSnapshot, TransportError> {
        let object = Object {
            bucket: self.bucket.clone(),
            name: self.object.clone(),
            metadata,
            content_type: "application/vnd.chorus.manifest".into(),
            ..Default::default()
        };
        let request = WriteObjectRequest {
            first_message: Some(write_object_request::FirstMessage::WriteObjectSpec(
                WriteObjectSpec {
                    resource: Some(object),
                    if_generation_match: Some(0),
                    ..Default::default()
                },
            )),
            write_offset: 0,
            finish_write: true,
            ..Default::default()
        };
        let response = self
            .fake
            .sim_write_object(request)
            .await
            .map_err(|status| {
                let error = status_err(self.zone, &status);
                if error.code == TransportCode::FailedPrecondition {
                    TransportError {
                        code: TransportCode::AlreadyExists,
                        ..error
                    }
                } else {
                    error
                }
            })?;
        let Some(write_object_response::WriteStatus::Resource(object)) = response.write_status
        else {
            return Err(err(
                self.zone,
                TransportCode::Internal,
                "manifest create response omitted the object resource",
            ));
        };
        Ok(stat_from_object(self.zone, object))
    }

    async fn update_register(
        &self,
        metageneration: i64,
        metadata: HashMap<String, String>,
    ) -> Result<ReplicaSnapshot, TransportError> {
        let request = UpdateObjectRequest {
            object: Some(Object {
                bucket: self.bucket.clone(),
                name: self.object.clone(),
                metadata,
                ..Default::default()
            }),
            if_metageneration_match: Some(metageneration),
            ..Default::default()
        };
        let object = Storage::update_object(&self.fake, Request::new(request))
            .await
            .map_err(|status| status_err(self.zone, &status))?
            .into_inner();
        Ok(stat_from_object(self.zone, object))
    }

    async fn resume_tail(&self, token: &mut AppendToken) -> Result<i64, TransportError> {
        let observed = self.stat().await?;
        let request = self.open_request_append(
            observed.generation,
            Some(observed.metageneration),
            token.write_handle.clone(),
            token.persisted_size,
            None,
            false,
        );
        let open = self.open(request).await?;
        let (persisted, write_handle) = self.token_from_open(&open, Some(observed.generation));
        token.generation = Some(observed.generation);
        token.metageneration = Some(observed.metageneration);
        if write_handle.is_some() {
            token.write_handle = write_handle;
        }
        token.persisted_size = persisted;
        self.store_session(&open).await;
        Ok(persisted)
    }

    async fn takeover(&self, observed: &ReplicaSnapshot) -> Result<AppendToken, TransportError> {
        let request = self.open_request_append(
            observed.generation,
            Some(observed.metageneration),
            None,
            observed.persisted_size,
            None,
            false,
        );
        let open = self.open(request).await?;
        let (persisted_size, write_handle) = self.token_from_open(&open, Some(observed.generation));
        self.store_session(&open).await;
        Ok(AppendToken {
            zone: self.zone,
            generation: Some(observed.generation),
            metageneration: Some(observed.metageneration),
            persisted_size,
            write_handle,
        })
    }

    async fn replace_appendable(
        &self,
        observed: &ReplicaSnapshot,
        data: Bytes,
        metadata: HashMap<String, String>,
    ) -> Result<AppendToken, TransportError> {
        let object = Object {
            bucket: self.bucket.clone(),
            name: self.object.clone(),
            metadata,
            content_type: "application/vnd.chorus.records".into(),
            ..Default::default()
        };
        let request = BidiWriteObjectRequest {
            first_message: Some(bidi_write_object_request::FirstMessage::WriteObjectSpec(
                WriteObjectSpec {
                    resource: Some(object),
                    if_generation_match: Some(observed.generation),
                    if_metageneration_match: Some(observed.metageneration),
                    appendable: Some(true),
                    ..Default::default()
                },
            )),
            write_offset: 0,
            flush: true,
            state_lookup: true,
            ..Default::default()
        };
        let open = self.open_split(request, data).await?;
        let (persisted_size, write_handle) = self.token_from_open(&open, None);
        self.store_session(&open).await;
        Ok(AppendToken {
            zone: self.zone,
            generation: None,
            metageneration: None,
            persisted_size,
            write_handle,
        })
    }

    async fn append(
        &self,
        token: &AppendToken,
        write_offset: i64,
        data: Vec<u8>,
    ) -> Result<i64, TransportError> {
        let Some(generation) = token.generation else {
            return Err(err(
                self.zone,
                TransportCode::Internal,
                "one-shot append requires a generation-bound token",
            ));
        };
        let expected = write_offset + data.len() as i64;
        // An empty append keeps its empty checksummed payload, which the
        // service answers with a persisted size.
        let data = Bytes::from(data);
        let request = self.open_request_append(
            generation,
            token.metageneration,
            token.write_handle.clone(),
            write_offset,
            data.is_empty().then(Vec::new),
            false,
        );
        let open = self.open_split(request, data).await?;
        let persisted = open
            .response
            .write_status
            .as_ref()
            .and_then(persisted_size_of)
            .unwrap_or(0);
        if persisted >= expected {
            Ok(persisted)
        } else {
            Err(err(
                self.zone,
                TransportCode::DataLoss,
                format!("flush persisted {persisted}, expected at least {expected}"),
            ))
        }
    }

    async fn lane_send(&self, write_offset: i64, chunks: &[Bytes]) -> Result<(), TransportError> {
        self.enqueue_lane_group(write_offset, &pack_append(chunks.to_vec()), true)
            .await
    }

    async fn lane_send_packed(
        &self,
        write_offset: i64,
        packed: &PackedAppend,
    ) -> Result<(), TransportError> {
        self.enqueue_lane_group(write_offset, packed, true).await
    }

    async fn lane_send_unflushed(
        &self,
        write_offset: i64,
        chunks: &[Bytes],
    ) -> Result<(), TransportError> {
        self.enqueue_lane_group(write_offset, &pack_append(chunks.to_vec()), false)
            .await
    }

    async fn lane_send_packed_unflushed(
        &self,
        write_offset: i64,
        packed: &PackedAppend,
    ) -> Result<(), TransportError> {
        self.enqueue_lane_group(write_offset, packed, false).await
    }

    async fn lane_flush(&self, write_offset: i64) -> Result<(), TransportError> {
        let mut guard = self.session.lock().await;
        let session = guard.as_mut().ok_or_else(|| {
            err(
                self.zone,
                TransportCode::FailedPrecondition,
                "append session disconnected while flushing",
            )
        })?;
        session.pending.push_back(vec![BidiWriteObjectRequest {
            first_message: None,
            write_offset,
            data: None,
            flush: true,
            state_lookup: true,
            ..Default::default()
        }]);
        Ok(())
    }

    async fn lane_durable_change(&self, seen: i64) -> Result<LaneDurableChange, TransportError> {
        let mut apply = self.apply.lock().await;
        loop {
            {
                let guard = self.session.lock().await;
                let session = guard.as_ref().ok_or_else(|| {
                    err(
                        self.zone,
                        TransportCode::FailedPrecondition,
                        "append session reader ended",
                    )
                })?;
                if session.durable > seen {
                    return Ok(LaneDurableChange {
                        persisted_size: session.durable,
                        error: None,
                    });
                }
                if apply
                    .as_ref()
                    .is_some_and(|(epoch, _)| *epoch != session.epoch)
                {
                    *apply = None;
                }
                if apply.is_none() {
                    let Some(group) = session.pending.front() else {
                        return Err(err(
                            self.zone,
                            TransportCode::Internal,
                            "append session made no progress",
                        ));
                    };
                    *apply = Some((
                        session.epoch,
                        Box::pin(apply_lane_group(
                            self.fake.clone(),
                            Arc::clone(&self.session),
                            session.spec.clone(),
                            session.stream_id,
                            session.epoch,
                            group.clone(),
                            self.zone,
                        )),
                    ));
                }
            }
            // Drive the application with the session lock released so a flush
            // hold can park here without blocking a concurrent shutdown or
            // shed. Cancellation leaves it in the slot for the next call.
            let applied = apply
                .as_mut()
                .expect("an application was just staged")
                .1
                .as_mut()
                .await;
            *apply = None;
            let Some(applied) = applied else {
                // The session was replaced or shut down during the apply.
                continue;
            };
            if let Some(error) = applied.fence {
                return Ok(LaneDurableChange {
                    persisted_size: applied.durable,
                    error: Some(error),
                });
            }
            if applied.durable > seen {
                return Ok(LaneDurableChange {
                    persisted_size: applied.durable,
                    error: None,
                });
            }
        }
    }

    async fn delete(&self, generation: i64) -> Result<(), TransportError> {
        let request = DeleteObjectRequest {
            bucket: self.bucket.clone(),
            object: self.object.clone(),
            generation,
            if_generation_match: Some(generation),
            ..Default::default()
        };
        Storage::delete_object(&self.fake, Request::new(request))
            .await
            .map_err(|status| status_err(self.zone, &status))?;
        Ok(())
    }

    async fn finalize(
        &self,
        token: &mut AppendToken,
        write_offset: i64,
    ) -> Result<ReplicaSnapshot, TransportError> {
        // Finalize through the live session when one is held; otherwise resume
        // the exact server-side session by write handle and finish it.
        let mut guard = self.session.lock().await;
        if let Some(session) = guard.as_mut() {
            let spec = session.spec.clone();
            let stream_id = session.stream_id;
            let request = BidiWriteObjectRequest {
                first_message: None,
                write_offset,
                finish_write: true,
                ..Default::default()
            };
            let response = self
                .fake
                .sim_continue(&spec, stream_id, request, None)
                .await
                .map_err(|status| status_err(self.zone, &status))?;
            *guard = None;
            let object = match response.write_status {
                Some(bidi_write_object_response::WriteStatus::Resource(object)) => object,
                _ => {
                    return Err(err(
                        self.zone,
                        TransportCode::Internal,
                        "missing finalized resource response",
                    ))
                }
            };
            let finalized = stat_from_object(self.zone, object);
            // Verify the finalized resource as the non-live branch and the gRPC
            // finish_live_session do: it must be finalized at exactly the
            // requested offset, never accepting an object longer than requested.
            if !finalized.finalized || finalized.persisted_size != write_offset {
                return Err(err(
                    self.zone,
                    TransportCode::DataLoss,
                    "live finalize response does not match the requested offset",
                ));
            }
            token.generation = Some(finalized.generation);
            token.metageneration = Some(finalized.metageneration);
            return Ok(finalized);
        }
        drop(guard);
        let observed = self.stat().await?;
        let generation = observed.generation;
        let request = self.open_request_append(
            generation,
            None,
            token.write_handle.clone(),
            write_offset,
            None,
            true,
        );
        let open = self.open(request).await?;
        let object = match open.response.write_status {
            Some(bidi_write_object_response::WriteStatus::Resource(object)) => object,
            _ => {
                return Err(err(
                    self.zone,
                    TransportCode::Internal,
                    "missing finalized resource response",
                ))
            }
        };
        let finalized = stat_from_object(self.zone, object);
        if !finalized.finalized
            || finalized.generation != generation
            || finalized.persisted_size != write_offset
        {
            return Err(err(
                self.zone,
                TransportCode::DataLoss,
                "finalized segment does not match the recovered prefix",
            ));
        }
        Ok(finalized)
    }

    async fn shutdown(&self) {
        let _ = self.shutdown.send(true);
        *self.session.lock().await = None;
    }
}
