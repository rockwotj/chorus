use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arc_swap::{ArcSwap, ArcSwapOption};
use async_trait::async_trait;
use bytes::Bytes;
use googleapis_tonic_google_storage_v2::google::storage::v2::{
    bidi_write_object_request, bidi_write_object_response, storage_client::StorageClient,
    write_object_request, write_object_response, AppendObjectSpec, BidiWriteHandle,
    BidiWriteObjectRedirectedError, BidiWriteObjectRequest, BidiWriteObjectResponse,
    ChecksummedData, DeleteObjectRequest, GetObjectRequest, ListObjectsRequest, Object,
    ReadObjectRequest, UpdateObjectRequest, WriteObjectRequest, WriteObjectSpec,
};
use prost::Message as _;
use tokio::sync::{mpsc, watch, Mutex as SessionMutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Code, Request, Status, Streaming};

use crate::auth::BearerAuth;
use crate::error::Error;
use crate::transport::{
    AppendToken, LaneDurableChange, ListedObject, PackedAppend, PackedAppendMessage, Replica,
    ReplicaFactory, ReplicaSnapshot, TransportCode, TransportError,
};

/// Cached zonal write routing token, learned from a `BidiWriteObjectRedirectedError`
/// and replayed in `x-goog-request-params` to land on the bucket's location.
/// Shared across every replica produced by one factory.
type RoutingToken = Arc<ArcSwap<Option<Arc<String>>>>;

#[derive(Clone)]
struct GrpcReplica {
    zone: usize,
    bucket: String,
    object: String,
    auth: Option<BearerAuth>,
    client: StorageClient<Channel>,
    routing_token: RoutingToken,
    /// The lane's persistent append session. The live service rate limits
    /// per-object *mutations*, and every fresh append open is one — so a
    /// lane opens its session once (at takeover) and streams every window
    /// through it as continuations. Flushes are not rate limited.
    /// Clones share the session; the hot send and
    /// progress paths load its immutable handle without taking the replacement
    /// mutex.
    session: Arc<SessionSlot>,
}

#[derive(Clone)]
/// Reusable Google Storage v2 channel, authentication handle, and zonal routing
/// token for one Rapid bucket.
pub struct GrpcReplicaFactory {
    zone: usize,
    bucket: String,
    auth: Option<BearerAuth>,
    client: StorageClient<Channel>,
    routing_token: RoutingToken,
}

/// A live appendable write stream: an ordered request sender plus a
/// background reader translating flush acknowledgments into a watchable
/// durable tail. Send-ahead lanes write through `tx` while waiting on
/// `state` — sends never block on acknowledgments.
struct AppendSession {
    handle: Arc<AppendSessionHandle>,
    reader: tokio::task::JoinHandle<()>,
}

struct AppendSessionHandle {
    tx: mpsc::Sender<BidiWriteObjectRequest>,
    state: tokio::sync::watch::Receiver<LaneProgress>,
}

struct RedirectAwareStream {
    tx: Option<mpsc::Sender<BidiWriteObjectRequest>>,
    responses: Streaming<BidiWriteObjectResponse>,
    first: BidiWriteObjectResponse,
    attempt: u32,
}

struct SessionWait {
    timeout: Option<std::time::Duration>,
    clear_on_ready: bool,
    inspect_before_error: bool,
    reader_ended: &'static str,
    stalled: &'static str,
}

struct SessionSlot {
    current: ArcSwapOption<AppendSessionHandle>,
    owned: SessionMutex<Option<AppendSession>>,
    shutdown: watch::Sender<bool>,
}

impl SessionSlot {
    fn new() -> Self {
        let (shutdown, _) = watch::channel(false);
        Self {
            current: ArcSwapOption::empty(),
            owned: SessionMutex::new(None),
            shutdown,
        }
    }
}

impl Drop for AppendSession {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

impl AppendSession {
    async fn shutdown(mut self) {
        self.reader.abort();
        let _ = (&mut self.reader).await;
    }
}

#[derive(Clone, Debug, Default)]
/// Durable tail plus any coalesced stream-termination error published by the
/// session reader.
struct LaneProgress {
    durable: i64,
    finalized: Option<ReplicaSnapshot>,
    error: Option<TransportError>,
}

/// Target wire-message size for packed appends. Large enough to amortize
/// per-message overhead (protobuf, CRC field, HTTP/2 framing, server
/// per-message processing). The hard ceiling, measured live, is
/// the service's 4 MiB gRPC inbound message cap — a 2 MiB + 1 byte chunk is
/// accepted, 4 MiB is rejected with ResourceExhausted; the proto's only
/// documented chunk constant (MaxReadChunkBytes = 2 MiB) is read-side.
const WIRE_MESSAGE_TARGET_BYTES: usize = 262_144;

pub(crate) fn pack_append(chunks: Vec<Bytes>) -> PackedAppend {
    let total_len = chunks.iter().map(Bytes::len).sum::<usize>();
    let mut packed =
        bytes::BytesMut::with_capacity(total_len.min(WIRE_MESSAGE_TARGET_BYTES.saturating_mul(2)));
    let mut messages = Vec::new();
    let mut relative_offset = 0i64;
    for data in &chunks {
        if !packed.is_empty() && packed.len() + data.len() > WIRE_MESSAGE_TARGET_BYTES {
            let content = packed.split().freeze();
            let len = content.len() as i64;
            messages.push(PackedAppendMessage {
                relative_offset,
                crc32c: crc32c::crc32c(&content),
                content,
            });
            relative_offset += len;
        }
        packed.extend_from_slice(data);
    }
    if !packed.is_empty() {
        let content = packed.freeze();
        messages.push(PackedAppendMessage {
            relative_offset,
            crc32c: crc32c::crc32c(&content),
            content,
        });
    }
    PackedAppend::new(chunks, messages, total_len)
}

/// Per-response progress timeout for the persistent session. The session
/// itself has no overall deadline (it lives for the segment), but a server
/// that stops acknowledging flushed appends must fail the lane.
const SESSION_PROGRESS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const MAX_LIST_PAGES: usize = 10_000;

impl AppendSessionHandle {
    async fn send(
        self: &Arc<Self>,
        replica: &GrpcReplica,
        request: BidiWriteObjectRequest,
        disconnected: &'static str,
    ) -> Result<(), TransportError> {
        match tokio::time::timeout(SESSION_PROGRESS_TIMEOUT, self.tx.send(request)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => {
                replica.clear_session_if(self).await;
                Err(replica.error(TransportCode::Unavailable, disconnected))
            }
            Err(_) => {
                replica.clear_session_if(self).await;
                Err(replica.error(
                    TransportCode::DeadlineExceeded,
                    "append session request channel made no progress",
                ))
            }
        }
    }

    async fn wait_for<T>(
        self: &Arc<Self>,
        replica: &GrpcReplica,
        wait: SessionWait,
        mut inspect: impl FnMut(&LaneProgress) -> Option<Result<T, TransportError>>,
    ) -> Result<T, TransportError> {
        let mut state = self.state.clone();
        loop {
            let progress = state.borrow_and_update().clone();
            if wait.inspect_before_error {
                if let Some(result) = inspect(&progress) {
                    if wait.clear_on_ready || result.is_err() {
                        replica.clear_session_if(self).await;
                    }
                    return result;
                }
            }
            if let Some(error) = progress.error {
                replica.clear_session_if(self).await;
                return Err(error);
            }
            if !wait.inspect_before_error {
                if let Some(result) = inspect(&progress) {
                    if wait.clear_on_ready || result.is_err() {
                        replica.clear_session_if(self).await;
                    }
                    return result;
                }
            }
            let changed = match wait.timeout {
                Some(timeout) => match tokio::time::timeout(timeout, state.changed()).await {
                    Ok(changed) => changed,
                    Err(_) => {
                        replica.clear_session_if(self).await;
                        return Err(replica.error(TransportCode::DeadlineExceeded, wait.stalled));
                    }
                },
                None => state.changed().await,
            };
            if changed.is_err() {
                replica.clear_session_if(self).await;
                return Err(replica.error(TransportCode::Unavailable, wait.reader_ended));
            }
        }
    }
}

impl GrpcReplica {
    fn live_session(&self) -> Result<Arc<AppendSessionHandle>, TransportError> {
        self.session.current.load_full().ok_or_else(|| {
            self.error(
                TransportCode::Unavailable,
                "no live append session (resume required)",
            )
        })
    }

    fn replace_session_locked(
        &self,
        owned: &mut Option<AppendSession>,
        session: Option<AppendSession>,
    ) -> Option<AppendSession> {
        let previous = owned.take();
        self.session
            .current
            .store(session.as_ref().map(|session| Arc::clone(&session.handle)));
        *owned = session;
        previous
    }

    async fn replace_session(&self, session: Option<AppendSession>) {
        let previous = {
            let mut owned = self.session.owned.lock().await;
            self.replace_session_locked(&mut owned, session)
        };
        if let Some(previous) = previous {
            previous.shutdown().await;
        }
    }

    async fn clear_session_if(&self, expected: &Arc<AppendSessionHandle>) {
        let previous = {
            let mut owned = self.session.owned.lock().await;
            if owned
                .as_ref()
                .is_some_and(|session| Arc::ptr_eq(&session.handle, expected))
            {
                self.replace_session_locked(&mut owned, None)
            } else {
                None
            }
        };
        if let Some(previous) = previous {
            previous.shutdown().await;
        }
    }

    /// Like [`Self::request`], but without the per-RPC deadline: used for the
    /// persistent append session, whose stream must outlive any fixed
    /// deadline. Progress is bounded per response instead.
    fn request_no_deadline<T>(&self, value: T) -> Result<Request<T>, TransportError> {
        let mut request = self.request(value)?;
        request.metadata_mut().remove("grpc-timeout");
        Ok(request)
    }

    fn request<T>(&self, value: T) -> Result<Request<T>, TransportError> {
        let mut request = Request::new(value);
        request.set_timeout(RPC_TIMEOUT);
        // GCS v2 gRPC routes every object RPC by the bucket resource path
        // carried in x-goog-request-params; without it the service rejects the
        // call with INVALID_ARGUMENT. Once a zonal write redirect has supplied a
        // routing token, replay it here so the stream lands on the right location.
        let guard = self.routing_token.load();
        let value = match (**guard).as_ref() {
            Some(token) => format!("bucket={}&routing_token={}", self.bucket, token),
            None => format!("bucket={}", self.bucket),
        };
        let params = MetadataValue::try_from(value).map_err(|_| {
            self.error(
                TransportCode::Internal,
                "request params are not valid gRPC metadata",
            )
        })?;
        request
            .metadata_mut()
            .insert("x-goog-request-params", params);
        if let Some(auth) = &self.auth {
            let value = auth
                .authorization_header()
                .map_err(|error| self.error(TransportCode::Unauthenticated, error.to_string()))?;
            request.metadata_mut().insert("authorization", value);
        }
        Ok(request)
    }

    fn error(&self, code: TransportCode, message: impl Into<String>) -> TransportError {
        TransportError {
            zone: self.zone,
            code,
            message: message.into(),
        }
    }

    fn status(&self, status: Status) -> TransportError {
        // Retry classification is a protocol contract, not a direct mirror of
        // tonic's codes. This mapper is shared by append and non-append RPCs:
        // RESOURCE_EXHAUSTED is GCS throttling, not a permanent rejection:
        // per-object mutation-rate and per-project quotas reset over time, so
        // retrying the valid request with backoff can succeed.
        let code = match status.code() {
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
        };
        self.error(code, status.message())
    }

    fn snapshot_from_object(&self, object: Object, bytes: Vec<u8>) -> ReplicaSnapshot {
        let crc32c = object
            .checksums
            .as_ref()
            .and_then(|checksums| checksums.crc32c);
        ReplicaSnapshot {
            zone: self.zone,
            generation: object.generation,
            metageneration: object.metageneration,
            persisted_size: bytes.len() as i64,
            finalized: object.finalize_time.is_some(),
            crc32c,
            metadata: object.metadata,
            bytes,
        }
    }

    fn stat_from_object(&self, object: Object) -> ReplicaSnapshot {
        let size = object.size;
        let mut snapshot = self.snapshot_from_object(object, Vec::new());
        if snapshot.finalized {
            snapshot.persisted_size = size;
        }
        snapshot
    }

    /// If `status` is a zonal write redirect, cache its routing token for replay
    /// and report `true` so the caller retries the same guarded open. The
    /// redirect fields are intentionally ignored: conditional creates replay
    /// their create precondition, takeovers replay their metageneration guard,
    /// and resumes replay their existing handle.
    fn try_capture_redirect(&self, status: &Status) -> bool {
        match redirect_routing_token(status) {
            Some(token) => {
                self.routing_token.store(Arc::new(Some(Arc::new(token))));
                true
            }
            None => false,
        }
    }

    /// Open one write stream and obtain its first response, transparently
    /// replaying redirects that occur before any response is observed.
    ///
    /// Both one-shot writes and persistent append sessions use this driver, so
    /// routing-token capture, redirect limits, opening deadlines, and the
    /// no-midstream-replay rule have one implementation.
    async fn open_redirect_aware_stream(
        &self,
        requests: &[BidiWriteObjectRequest],
        persistent: bool,
    ) -> Result<Option<RedirectAwareStream>, TransportError> {
        let mut attempt = 0u32;
        let mut shutdown = self.session.shutdown.subscribe();
        loop {
            if *shutdown.borrow_and_update() {
                return Err(self.error(
                    TransportCode::Unavailable,
                    "append session open cancelled by shutdown",
                ));
            }
            attempt += 1;
            let (tx, rx) = mpsc::channel(requests.len().max(64));
            for request in requests {
                if tx.send(request.clone()).await.is_err() {
                    return Err(self.error(TransportCode::Internal, "write stream channel closed"));
                }
            }
            let request = if persistent {
                self.request_no_deadline(ReceiverStream::new(rx))?
            } else {
                self.request(ReceiverStream::new(rx))?
            };
            let mut tx = if persistent {
                Some(tx)
            } else {
                drop(tx);
                None
            };
            let mut client = self.client.clone();
            let mut opening = Box::pin(client.bidi_write_object(request));
            let opened = if persistent {
                // The established session has no RPC deadline, so bound its
                // opening handshake separately before returning the live stream.
                tokio::select! {
                    biased;
                    changed = shutdown.changed() => {
                        tx.take();
                        if changed.is_ok() {
                            if let Ok(Ok(response)) =
                                tokio::time::timeout(SESSION_PROGRESS_TIMEOUT, &mut opening).await
                            {
                                let mut responses = response.into_inner();
                                let _ = tokio::time::timeout(
                                    SESSION_PROGRESS_TIMEOUT,
                                    responses.message(),
                                )
                                .await;
                            }
                        }
                        return Err(self.error(
                            TransportCode::Unavailable,
                            "append session open cancelled by shutdown",
                        ));
                    }
                    opened = tokio::time::timeout(SESSION_PROGRESS_TIMEOUT, &mut opening) => {
                        match opened {
                            Ok(opened) => opened,
                            Err(_) => {
                                return Err(self.error(
                                    TransportCode::DeadlineExceeded,
                                    "append session open timed out",
                                ));
                            }
                        }
                    }
                }
            } else {
                opening.await
            };
            let mut responses = match opened {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    if attempt <= MAX_WRITE_REDIRECTS && self.try_capture_redirect(&status) {
                        continue;
                    }
                    return Err(self.status(status));
                }
            };
            let first = if persistent {
                tokio::select! {
                    biased;
                    changed = shutdown.changed() => {
                        tx.take();
                        if changed.is_ok() {
                            let _ = tokio::time::timeout(
                                SESSION_PROGRESS_TIMEOUT,
                                responses.message(),
                            )
                            .await;
                        }
                        return Err(self.error(
                            TransportCode::Unavailable,
                            "append session open cancelled by shutdown",
                        ));
                    }
                    response = tokio::time::timeout(
                        SESSION_PROGRESS_TIMEOUT,
                        responses.message(),
                    ) => {
                        match response {
                            Ok(response) => response,
                            Err(_) => {
                                return Err(self.error(
                                    TransportCode::DeadlineExceeded,
                                    "append open made no progress",
                                ));
                            }
                        }
                    }
                }
            } else {
                responses.message().await
            };
            match first {
                Ok(Some(first)) => {
                    return Ok(Some(RedirectAwareStream {
                        tx,
                        responses,
                        first,
                        attempt,
                    }));
                }
                Ok(None) => return Ok(None),
                Err(status)
                    if attempt <= MAX_WRITE_REDIRECTS && self.try_capture_redirect(&status) =>
                {
                    continue;
                }
                Err(status) => return Err(self.status(status)),
            }
        }
    }

    /// Open (or resume) the persistent append session: send the first
    /// message, wait for the opening acknowledgment, and return the live
    /// session with the server-certified durable tail and session handle.
    /// Redirects are captured and retried with the routing token.
    async fn open_session(
        &self,
        first: BidiWriteObjectRequest,
    ) -> Result<(AppendSession, i64, Option<Bytes>), TransportError> {
        let (session, persisted_size, write_handle, _) = self.open_session_observed(first).await?;
        Ok((session, persisted_size, write_handle))
    }

    async fn open_session_observed(
        &self,
        first: BidiWriteObjectRequest,
    ) -> Result<(AppendSession, i64, Option<Bytes>, Option<i64>), TransportError> {
        let Some(opened) = self
            .open_redirect_aware_stream(std::slice::from_ref(&first), true)
            .await?
        else {
            return Err(self.error(
                TransportCode::Unavailable,
                "append open closed without a response",
            ));
        };
        let opening_has_status = opened.first.write_status.is_some();
        let opening_resource_size = match opened.first.write_status.as_ref() {
            Some(bidi_write_object_response::WriteStatus::Resource(resource)) => {
                Some(resource.size)
            }
            _ => None,
        };
        let handle = opened
            .first
            .write_handle
            .clone()
            .map(|handle| handle.handle);
        let mut progress = LaneProgress {
            durable: if opening_has_status {
                0
            } else {
                first.write_offset
            },
            finalized: None,
            error: None,
        };
        self.fold_session_progress(&mut progress, opened.first);
        let persisted = progress.durable;
        let (state_tx, state_rx) = tokio::sync::watch::channel(progress);
        let this = self.clone();
        let reader = tokio::spawn(async move {
            let mut responses = opened.responses;
            loop {
                match responses.message().await {
                    Ok(Some(response)) => {
                        state_tx.send_modify(|progress| {
                            this.fold_session_progress(progress, response);
                        });
                    }
                    Ok(None) => {
                        state_tx.send_modify(|progress| {
                            progress.error = Some(this.error(
                                TransportCode::Unavailable,
                                "append session closed by the service",
                            ));
                        });
                        return;
                    }
                    Err(status) => {
                        this.try_capture_redirect(&status);
                        let error = this.status(status);
                        state_tx.send_modify(|progress| {
                            progress.error = Some(error.clone());
                        });
                        return;
                    }
                }
            }
        });
        tracing::debug!(
            zone = self.zone,
            persisted_size = persisted,
            has_write_handle = handle.is_some(),
            attempt = opened.attempt,
            "append session opened"
        );
        let tx = opened.tx.ok_or_else(|| {
            self.error(
                TransportCode::Internal,
                "persistent write stream omitted its request sender",
            )
        })?;
        Ok((
            AppendSession {
                handle: Arc::new(AppendSessionHandle {
                    tx,
                    state: state_rx,
                }),
                reader,
            },
            persisted,
            handle,
            opening_resource_size,
        ))
    }

    fn fold_session_progress(
        &self,
        progress: &mut LaneProgress,
        response: BidiWriteObjectResponse,
    ) {
        match response.write_status {
            Some(bidi_write_object_response::WriteStatus::PersistedSize(size)) => {
                progress.durable = progress.durable.max(size);
            }
            Some(bidi_write_object_response::WriteStatus::Resource(resource)) => {
                let persisted_size = resource.size;
                let snapshot = self.stat_from_object(resource);
                progress.durable = progress.durable.max(persisted_size);
                if snapshot.finalized {
                    progress.finalized = Some(snapshot);
                }
            }
            None => {}
        }
    }

    /// The opening message for this object's append session.
    fn session_open_request(
        &self,
        generation: i64,
        metageneration: i64,
        write_handle: Option<Bytes>,
        write_offset: i64,
    ) -> BidiWriteObjectRequest {
        BidiWriteObjectRequest {
            first_message: Some(bidi_write_object_request::FirstMessage::AppendObjectSpec(
                AppendObjectSpec {
                    bucket: self.bucket.clone(),
                    object: self.object.clone(),
                    generation,
                    if_metageneration_match: Some(metageneration),
                    write_handle: write_handle.map(|handle| BidiWriteHandle { handle }),
                    ..Default::default()
                },
            )),
            write_offset,
            flush: true,
            state_lookup: true,
            ..Default::default()
        }
    }

    /// Construct the candidate handle-free current-generation open. Generation
    /// zero is wire-identical to an unset proto3 scalar; the live probe decides
    /// whether GCS accepts it as a selector or rejects the missing generation.
    #[cfg(feature = "probe-support")]
    fn current_session_open_request(&self) -> BidiWriteObjectRequest {
        BidiWriteObjectRequest {
            first_message: Some(bidi_write_object_request::FirstMessage::AppendObjectSpec(
                AppendObjectSpec {
                    bucket: self.bucket.clone(),
                    object: self.object.clone(),
                    generation: 0,
                    ..Default::default()
                },
            )),
            write_offset: 0,
            flush: true,
            state_lookup: true,
            ..Default::default()
        }
    }

    #[cfg(feature = "probe-support")]
    async fn takeover_current_generation(
        &self,
    ) -> Result<(AppendToken, Option<i64>), TransportError> {
        let (session, persisted_size, write_handle, opening_resource_size) = self
            .open_session_observed(self.current_session_open_request())
            .await?;
        self.replace_session(Some(session)).await;
        tracing::debug!(
            zone = self.zone,
            persisted_size,
            has_write_handle = write_handle.is_some(),
            "current append session takeover completed"
        );
        Ok((
            AppendToken {
                zone: self.zone,
                // The append-open response does not provide identity fields
                // needed by later resume/finalize paths. Resolve them lazily.
                generation: None,
                metageneration: None,
                persisted_size,
                write_handle,
            },
            opening_resource_size,
        ))
    }

    async fn resolve_token_identity(
        &self,
        token: &mut AppendToken,
    ) -> Result<(i64, i64), TransportError> {
        match (token.generation, token.metageneration) {
            (Some(generation), Some(metageneration)) => Ok((generation, metageneration)),
            (None, None) => {
                let observed = self.stat().await?;
                if observed.finalized {
                    return Err(self.error(
                        TransportCode::FailedPrecondition,
                        "append session object is already finalized",
                    ));
                }
                token.generation = Some(observed.generation);
                token.metageneration = Some(observed.metageneration);
                Ok((observed.generation, observed.metageneration))
            }
            _ => Err(self.error(
                TransportCode::Internal,
                "append token has incomplete generation identity",
            )),
        }
    }

    async fn finish_live_session(
        &self,
        write_offset: i64,
        expected_generation: Option<i64>,
    ) -> Result<ReplicaSnapshot, TransportError> {
        let handle = self.live_session().map_err(|_| {
            self.error(
                TransportCode::Unavailable,
                "no live append session to finalize",
            )
        })?;
        let request = BidiWriteObjectRequest {
            first_message: None,
            write_offset,
            finish_write: true,
            ..Default::default()
        };
        handle
            .send(
                self,
                request,
                "append session disconnected while finalizing",
            )
            .await?;
        handle
            .wait_for(
                self,
                SessionWait {
                    timeout: Some(SESSION_PROGRESS_TIMEOUT),
                    clear_on_ready: true,
                    inspect_before_error: true,
                    reader_ended: "append session reader ended while finalizing",
                    stalled: "append session finalization made no progress",
                },
                |progress| {
                    let finalized = progress.finalized.clone()?;
                    Some(
                        if !finalized.finalized
                            || expected_generation
                                .is_some_and(|generation| finalized.generation != generation)
                            || finalized.persisted_size != write_offset
                        {
                            tracing::warn!(
                                expected_generation,
                                actual_generation = finalized.generation,
                                expected_size = write_offset,
                                actual_size = finalized.persisted_size,
                                finalized = finalized.finalized,
                                "finalized append response did not match the requested prefix"
                            );
                            Err(self.error(
                                TransportCode::DataLoss,
                                "finalized segment does not match the committed prefix",
                            ))
                        } else {
                            Ok(finalized)
                        },
                    )
                },
            )
            .await
    }

    async fn drive_redirect_aware_stream<S>(
        &self,
        requests: Vec<BidiWriteObjectRequest>,
        make_state: impl Fn() -> S,
        mut step: impl FnMut(&mut S, BidiWriteObjectResponse),
        complete: impl Fn(&S) -> bool,
    ) -> (S, Option<TransportError>) {
        let mut state = make_state();
        let opened = match self.open_redirect_aware_stream(&requests, false).await {
            Ok(Some(opened)) => opened,
            Ok(None) => return (state, None),
            Err(error) => return (state, Some(error)),
        };
        step(&mut state, opened.first);
        if complete(&state) {
            return (state, None);
        }
        let mut stream = opened.responses;
        loop {
            match stream.message().await {
                Ok(Some(response)) => {
                    step(&mut state, response);
                    if complete(&state) {
                        // Appendable sessions stay open server-side; once the
                        // caller has what it needs, abandon the stream instead
                        // of waiting for the live service's idle expiry.
                        return (state, None);
                    }
                }
                Ok(None) => return (state, None),
                Err(status) => return (state, Some(self.status(status))),
            }
        }
    }
}

impl GrpcReplicaFactory {
    #[cfg(feature = "probe-support")]
    fn probe_replica(&self, object: &str) -> GrpcReplica {
        GrpcReplica {
            zone: self.zone,
            bucket: self.bucket.clone(),
            object: object.to_string(),
            auth: self.auth.clone(),
            client: self.client.clone(),
            routing_token: self.routing_token.clone(),
            session: Arc::new(SessionSlot::new()),
        }
    }

    /// Connect a zonal bucket with an optional static bearer token.
    ///
    /// The bucket must be a full v2 resource name such as
    /// `projects/_/buckets/example-zone-a`. All replicas created by the factory
    /// share the routing token learned from zonal write redirects. The normal
    /// production endpoint is `https://storage.googleapis.com`; Cloud Storage
    /// regional JSON/XML endpoints do not support this gRPC client.
    pub async fn connect(
        zone: usize,
        endpoint: &str,
        bucket: impl Into<String>,
        bearer_token: Option<String>,
    ) -> Result<Self, Error> {
        let mut builder = Endpoint::from_shared(endpoint.to_string())
            .map_err(|error| Error::Connection(error.to_string()))?;
        builder = builder.connect_timeout(RPC_TIMEOUT);
        if endpoint.starts_with("https://") {
            builder = builder
                .tls_config(ClientTlsConfig::new().with_webpki_roots())
                .map_err(|error| Error::Connection(error.to_string()))?;
        }
        let channel = builder
            .connect()
            .await
            .map_err(|error| Error::Connection(error.to_string()))?;
        Ok(Self {
            zone,
            bucket: bucket.into(),
            auth: bearer_token.map(BearerAuth::static_token),
            client: StorageClient::new(channel),
            routing_token: Arc::new(ArcSwap::from_pointee(None)),
        })
    }

    /// Build a factory over an already-established channel. The deterministic
    /// simulator uses this to route the production client over a simulated
    /// transport (`Endpoint::connect_with_connector`); production callers use
    /// `connect`/`connect_with_auth`.
    pub fn from_channel(
        zone: usize,
        channel: Channel,
        bucket: impl Into<String>,
        bearer_token: Option<String>,
    ) -> Self {
        Self {
            zone,
            bucket: bucket.into(),
            auth: bearer_token.map(BearerAuth::static_token),
            client: StorageClient::new(channel),
            routing_token: Arc::new(ArcSwap::from_pointee(None)),
        }
    }

    /// Connect a zonal bucket using a shared static or refreshing auth handle.
    /// Cloned refreshing handles update existing clients through `ArcSwap`.
    /// Use full v2 bucket resource names and a gRPC-compatible endpoint such as
    /// `https://storage.googleapis.com`.
    pub async fn connect_with_auth(
        zone: usize,
        endpoint: &str,
        bucket: impl Into<String>,
        auth: BearerAuth,
    ) -> Result<Self, Error> {
        let mut builder = Endpoint::from_shared(endpoint.to_string())
            .map_err(|error| Error::Connection(error.to_string()))?;
        builder = builder.connect_timeout(RPC_TIMEOUT);
        if endpoint.starts_with("https://") {
            builder = builder
                .tls_config(ClientTlsConfig::new().with_webpki_roots())
                .map_err(|error| Error::Connection(error.to_string()))?;
        }
        let channel = builder
            .connect()
            .await
            .map_err(|error| Error::Connection(error.to_string()))?;
        Ok(Self {
            zone,
            bucket: bucket.into(),
            auth: Some(auth),
            client: StorageClient::new(channel),
            routing_token: Arc::new(ArcSwap::from_pointee(None)),
        })
    }
}

#[cfg(feature = "probe-support")]
#[derive(Clone, Debug)]
/// One generation-zero append-open response observed by the live-GCS probe.
pub struct GenerationZeroOpenObservation {
    /// Authoritative tail derived from the opening response.
    pub persisted_size: i64,
    /// Object-resource size in the opening response, if GCS supplied a resource.
    pub resource_size: Option<i64>,
}

#[cfg(feature = "probe-support")]
#[derive(Clone, Debug)]
/// Independent observations from the focused live-GCS generation-zero probe.
pub struct GenerationZeroTakeoverProbeResult {
    /// Number of bytes the probe requested GCS to persist.
    pub expected_size: i64,
    /// Result of creating an appendable object and flushing the payload.
    pub append: Result<i64, TransportError>,
    /// Present-object takeover result, absent only when creation or append failed.
    pub takeover: Option<Result<GenerationZeroOpenObservation, TransportError>>,
    /// Generation-zero open result for a never-created object.
    pub absent: Result<GenerationZeroOpenObservation, TransportError>,
    /// Cleanup result for the object that may have been created.
    pub cleanup: Result<(), TransportError>,
}

#[cfg(feature = "probe-support")]
/// Exercise the production gRPC append path against one present and one absent
/// object, then delete the present object before returning.
///
/// Both takeovers send `AppendObjectSpec.generation = 0` through the same
/// redirect-aware `open_session` implementation used by recovery. Operations
/// are reported independently so a provider rejection on the present object
/// does not suppress the absent-name observation.
pub async fn probe_generation_zero_takeover(
    factory: &GrpcReplicaFactory,
    present_object: &str,
    absent_object: &str,
    payload: Bytes,
) -> Result<GenerationZeroTakeoverProbeResult, TransportError> {
    let expected_size = i64::try_from(payload.len()).map_err(|_| TransportError {
        zone: factory.zone,
        code: TransportCode::InvalidArgument,
        message: "probe payload length does not fit in i64".into(),
    })?;
    if expected_size == 0 {
        return Err(TransportError {
            zone: factory.zone,
            code: TransportCode::InvalidArgument,
            message: "probe payload must be non-empty".into(),
        });
    }

    let present = factory.probe_replica(present_object);
    let append = async {
        let token = present.create_append_session(HashMap::new()).await?;
        if token.persisted_size != 0 {
            return Err(present.error(
                TransportCode::DataLoss,
                format!(
                    "new appendable object opened at persisted_size={}, expected 0",
                    token.persisted_size
                ),
            ));
        }
        present
            .lane_send(token.persisted_size, std::slice::from_ref(&payload))
            .await?;
        let change = present.lane_durable_change(token.persisted_size).await?;
        if let Some(error) = change.error {
            return Err(error);
        }
        Ok(change.persisted_size)
    }
    .await;
    let takeover = if append.is_ok() {
        Some(
            present
                .takeover_current_generation()
                .await
                .map(|(token, resource_size)| GenerationZeroOpenObservation {
                    persisted_size: token.persisted_size,
                    resource_size,
                }),
        )
    } else {
        None
    };

    present.shutdown().await;
    let cleanup = match present.stat().await {
        Ok(snapshot) => present.delete(snapshot.generation).await,
        Err(error) if error.code == TransportCode::NotFound => Ok(()),
        Err(error) => Err(error),
    };

    let absent_replica = factory.probe_replica(absent_object);
    let absent =
        absent_replica
            .takeover_current_generation()
            .await
            .map(|(token, resource_size)| GenerationZeroOpenObservation {
                persisted_size: token.persisted_size,
                resource_size,
            });
    absent_replica.shutdown().await;

    Ok(GenerationZeroTakeoverProbeResult {
        expected_size,
        append,
        takeover,
        absent,
        cleanup,
    })
}

#[async_trait]
impl ReplicaFactory for GrpcReplicaFactory {
    fn bucket_name(&self) -> &str {
        self.bucket.rsplit('/').next().unwrap_or_default()
    }

    fn replica(&self, object: &str) -> Arc<dyn Replica> {
        Arc::new(GrpcReplica {
            zone: self.zone,
            bucket: self.bucket.clone(),
            object: object.to_string(),
            auth: self.auth.clone(),
            client: self.client.clone(),
            routing_token: self.routing_token.clone(),
            session: Arc::new(SessionSlot::new()),
        })
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ListedObject>, TransportError> {
        let replica = GrpcReplica {
            zone: self.zone,
            bucket: self.bucket.clone(),
            object: String::new(),
            auth: self.auth.clone(),
            client: self.client.clone(),
            routing_token: self.routing_token.clone(),
            session: Arc::new(SessionSlot::new()),
        };
        let mut page_token = String::new();
        let mut seen_page_tokens = HashSet::new();
        let mut listed = Vec::new();
        for _ in 0..MAX_LIST_PAGES {
            let request = replica.request(ListObjectsRequest {
                parent: self.bucket.clone(),
                page_size: 1000,
                page_token,
                prefix: prefix.to_string(),
                ..Default::default()
            })?;
            let response = self
                .client
                .clone()
                .list_objects(request)
                .await
                .map_err(|status| replica.status(status))?
                .into_inner();
            listed.extend(response.objects.into_iter().map(|object| ListedObject {
                zone: self.zone,
                name: object.name,
                generation: object.generation,
                finalized: object.finalize_time.is_some(),
                metadata: object.metadata,
            }));
            if response.next_page_token.is_empty() {
                return Ok(listed);
            }
            if !seen_page_tokens.insert(response.next_page_token.clone()) {
                return Err(
                    replica.error(TransportCode::Internal, "ListObjects repeated a page token")
                );
            }
            page_token = response.next_page_token;
        }
        Err(replica.error(
            TransportCode::Internal,
            "ListObjects exceeded the pagination bound",
        ))
    }
}

#[async_trait]
impl Replica for GrpcReplica {
    async fn stat(&self) -> Result<ReplicaSnapshot, TransportError> {
        let get = GetObjectRequest {
            bucket: self.bucket.clone(),
            object: self.object.clone(),
            ..Default::default()
        };
        let request = self.request(get)?;
        let object = self
            .client
            .clone()
            .get_object(request)
            .await
            .map_err(|status| self.status(status))?
            .into_inner();
        // A finalized object's size is frozen and authoritative; open
        // objects keep persisted_size = 0 here (stats are tail-blind).
        Ok(self.stat_from_object(object))
    }

    async fn snapshot(&self) -> Result<ReplicaSnapshot, TransportError> {
        let get = GetObjectRequest {
            bucket: self.bucket.clone(),
            object: self.object.clone(),
            ..Default::default()
        };
        let request = self.request(get)?;
        let object = self
            .client
            .clone()
            .get_object(request)
            .await
            .map_err(|status| self.status(status))?
            .into_inner();

        let read = ReadObjectRequest {
            bucket: self.bucket.clone(),
            object: self.object.clone(),
            generation: object.generation,
            if_generation_match: Some(object.generation),
            ..Default::default()
        };
        let request = self.request(read)?;
        let mut stream = self
            .client
            .clone()
            .read_object(request)
            .await
            .map_err(|status| self.status(status))?
            .into_inner();
        let mut bytes = Vec::new();
        while let Some(response) = stream
            .message()
            .await
            .map_err(|status| self.status(status))?
        {
            if let Some(data) = response.checksummed_data {
                if data
                    .crc32c
                    .is_some_and(|expected| expected != crc32c::crc32c(&data.content))
                {
                    return Err(self.error(
                        TransportCode::DataLoss,
                        "ReadObject response CRC32C mismatch",
                    ));
                }
                bytes.extend_from_slice(&data.content);
            }
        }
        Ok(self.snapshot_from_object(object, bytes))
    }

    async fn create_appendable(
        &self,
        metadata: HashMap<String, String>,
    ) -> Result<ReplicaSnapshot, TransportError> {
        let object = Object {
            bucket: self.bucket.clone(),
            name: self.object.clone(),
            metadata: metadata.clone(),
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
        let (_, error) = self
            .drive_redirect_aware_stream(
                vec![request],
                || false,
                |seen, _| *seen = true,
                |seen| *seen,
            )
            .await;
        if let Some(error) = error {
            // The only precondition this request carries is
            // `if_generation_match=0`; the live service reports the conflict
            // as FAILED_PRECONDITION where the protocol expects AlreadyExists.
            if error.code == TransportCode::FailedPrecondition {
                return Err(TransportError {
                    code: TransportCode::AlreadyExists,
                    ..error
                });
            }
            return Err(error);
        }
        // Appendable create responses report only persisted size, not the
        // generation and metageneration required to guard the takeover open.
        // A metadata-only read supplies those fields without reading the
        // empty object body.
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
        let (session, persisted_size, write_handle) =
            self.open_session(request).await.map_err(|error| {
                if error.code == TransportCode::FailedPrecondition {
                    TransportError {
                        code: TransportCode::AlreadyExists,
                        ..error
                    }
                } else {
                    error
                }
            })?;
        self.replace_session(Some(session)).await;
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
        // A plain one-shot WriteObject: regional buckets reject appendable
        // creates, and the register is never appended to anyway.
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
        let request = self.request(tokio_stream::iter([request]))?;
        let response = self
            .client
            .clone()
            .write_object(request)
            .await
            .map_err(|status| {
                let error = self.status(status);
                // As with segment creates: the only precondition here is
                // `if_generation_match=0`, reported as FAILED_PRECONDITION.
                if error.code == TransportCode::FailedPrecondition {
                    TransportError {
                        code: TransportCode::AlreadyExists,
                        ..error
                    }
                } else {
                    error
                }
            })?
            .into_inner();
        let Some(write_object_response::WriteStatus::Resource(object)) = response.write_status
        else {
            return Err(self.error(
                TransportCode::Internal,
                "manifest create response omitted the object resource",
            ));
        };
        Ok(self.stat_from_object(object))
    }

    async fn resume_tail(&self, token: &mut AppendToken) -> Result<i64, TransportError> {
        let (generation, metageneration) = self.resolve_token_identity(token).await?;
        let mut owned = self.session.owned.lock().await;
        let first = self.session_open_request(
            generation,
            metageneration,
            token.write_handle.clone(),
            token.persisted_size,
        );
        let (result, previous) = match self.open_session(first).await {
            Ok((session, persisted, _)) => {
                let previous = self.replace_session_locked(&mut owned, Some(session));
                tracing::debug!(
                    zone = self.zone,
                    persisted_size = persisted,
                    "append session resumed"
                );
                (Ok(persisted), previous)
            }
            Err(error) => {
                let previous = self.replace_session_locked(&mut owned, None);
                (Err(error), previous)
            }
        };
        drop(owned);
        if let Some(previous) = previous {
            previous.shutdown().await;
        }
        result
    }

    async fn takeover(&self, observed: &ReplicaSnapshot) -> Result<AppendToken, TransportError> {
        let first = self.session_open_request(
            observed.generation,
            observed.metageneration,
            None, // handle-free: this open IS the fence
            observed.persisted_size,
        );
        let (session, persisted_size, write_handle) = self.open_session(first).await?;
        self.replace_session(Some(session)).await;
        tracing::debug!(
            zone = self.zone,
            persisted_size,
            has_write_handle = write_handle.is_some(),
            "append session takeover completed"
        );
        Ok(AppendToken {
            zone: self.zone,
            generation: Some(observed.generation),
            metageneration: Some(observed.metageneration),
            persisted_size,
            write_handle,
        })
    }

    async fn update_register(
        &self,
        metageneration: i64,
        metadata: HashMap<String, String>,
    ) -> Result<ReplicaSnapshot, TransportError> {
        // Generation 0 addresses the live object; the register is never
        // deleted or recreated, so the metageneration precondition alone is
        // the CAS guard.
        let request = UpdateObjectRequest {
            object: Some(Object {
                bucket: self.bucket.clone(),
                name: self.object.clone(),
                metadata,
                ..Default::default()
            }),
            if_metageneration_match: Some(metageneration),
            update_mask: Some(prost_types::FieldMask {
                paths: vec!["metadata".into()],
            }),
            ..Default::default()
        };
        let object = self
            .client
            .clone()
            .update_object(self.request(request)?)
            .await
            .map_err(|status| self.status(status))?
            .into_inner();
        Ok(self.stat_from_object(object))
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
            metadata: metadata.clone(),
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
            data: Some(bidi_write_object_request::Data::ChecksummedData(
                ChecksummedData {
                    crc32c: Some(crc32c::crc32c(&data)),
                    content: data.clone(),
                },
            )),
            flush: true,
            state_lookup: true,
            ..Default::default()
        };
        let expected = data.len() as i64;
        let (persisted_size, error) = self
            .drive_redirect_aware_stream(
                vec![request],
                || None,
                |persisted_size, response| {
                    if let Some(bidi_write_object_response::WriteStatus::PersistedSize(size)) =
                        response.write_status
                    {
                        *persisted_size = Some(size);
                    }
                },
                |persisted_size| persisted_size.is_some_and(|size| size >= expected),
            )
            .await;
        if let Some(error) = error {
            return Err(error);
        }
        if persisted_size != Some(expected) {
            return Err(self.error(
                TransportCode::DataLoss,
                format!("replacement persisted {persisted_size:?}, expected {expected}"),
            ));
        }
        let snapshot = self.snapshot().await?;
        if snapshot.bytes != data[..] || snapshot.metadata != metadata {
            return Err(self.error(
                TransportCode::DataLoss,
                "replacement generation failed verification",
            ));
        }
        Ok(AppendToken {
            zone: self.zone,
            generation: Some(snapshot.generation),
            metageneration: Some(snapshot.metageneration),
            persisted_size: expected,
            write_handle: None,
        })
    }

    async fn append(
        &self,
        token: &AppendToken,
        write_offset: i64,
        data: Vec<u8>,
    ) -> Result<i64, TransportError> {
        let Some(generation) = token.generation else {
            return Err(self.error(
                TransportCode::Internal,
                "one-shot append requires a generation-bound token",
            ));
        };
        let request = BidiWriteObjectRequest {
            first_message: Some(bidi_write_object_request::FirstMessage::AppendObjectSpec(
                AppendObjectSpec {
                    bucket: self.bucket.clone(),
                    object: self.object.clone(),
                    generation,
                    if_metageneration_match: token.metageneration,
                    write_handle: token
                        .write_handle
                        .clone()
                        .map(|handle| BidiWriteHandle { handle }),
                    ..Default::default()
                },
            )),
            write_offset,
            data: Some(bidi_write_object_request::Data::ChecksummedData(
                ChecksummedData {
                    content: Bytes::from(data.clone()),
                    crc32c: Some(crc32c::crc32c(&data)),
                },
            )),
            flush: true,
            state_lookup: true,
            ..Default::default()
        };
        let expected = write_offset + data.len() as i64;
        let (persisted_size, error) = self
            .drive_redirect_aware_stream(
                vec![request],
                || None,
                |persisted_size, response| {
                    if let Some(bidi_write_object_response::WriteStatus::PersistedSize(size)) =
                        response.write_status
                    {
                        *persisted_size = Some(size);
                    }
                },
                |persisted_size| persisted_size.is_some_and(|size| size >= expected),
            )
            .await;
        if let Some(error) = error {
            return Err(error);
        }
        match persisted_size {
            Some(size) if size >= expected => Ok(size),
            Some(size) => Err(self.error(
                TransportCode::DataLoss,
                format!("flush persisted {size}, expected at least {expected}"),
            )),
            None => Err(self.error(TransportCode::Internal, "missing persisted-size response")),
        }
    }

    async fn lane_send(&self, write_offset: i64, chunks: &[Bytes]) -> Result<(), TransportError> {
        let packed = pack_append(chunks.to_vec());
        self.lane_send_packed(write_offset, &packed).await
    }

    async fn lane_send_packed(
        &self,
        write_offset: i64,
        packed: &PackedAppend,
    ) -> Result<(), TransportError> {
        if packed.is_empty() {
            return Err(self.error(
                TransportCode::Internal,
                "append lane cannot send an empty flush group",
            ));
        }
        let handle = self.live_session()?;
        // The coalesced group was packed once before replica dispatch. Each
        // lane builds only its protobuf envelopes and shallow-clones the
        // refcounted message bytes; CRC32C and byte concatenation are shared.
        let messages = packed.messages();
        let last_index = messages.len() - 1;
        for (index, message) in messages.iter().enumerate() {
            let last = index == last_index;
            let request = BidiWriteObjectRequest {
                first_message: None,
                write_offset: write_offset + message.relative_offset,
                data: Some(bidi_write_object_request::Data::ChecksummedData(
                    ChecksummedData {
                        crc32c: Some(message.crc32c),
                        content: message.content.clone(),
                    },
                )),
                flush: last,
                state_lookup: last,
                ..Default::default()
            };
            handle
                .send(self, request, "append session disconnected while sending")
                .await?;
        }
        Ok(())
    }

    async fn lane_durable_change(&self, seen: i64) -> Result<LaneDurableChange, TransportError> {
        let handle = self.live_session()?;
        let change = handle
            .wait_for(
                self,
                SessionWait {
                    timeout: None,
                    clear_on_ready: false,
                    // Preserve a coalesced response and stream error in one
                    // observation. Protocol code publishes the durable offset
                    // before deciding whether to recover or fence the writer.
                    inspect_before_error: true,
                    reader_ended: "append session reader ended",
                    stalled: "append session made no progress",
                },
                |progress| {
                    (progress.durable > seen).then(|| {
                        Ok(LaneDurableChange {
                            persisted_size: progress.durable,
                            error: progress.error.clone(),
                        })
                    })
                },
            )
            .await?;
        if change.error.is_some() {
            self.clear_session_if(&handle).await;
        }
        Ok(change)
    }
    async fn delete(&self, generation: i64) -> Result<(), TransportError> {
        let request = DeleteObjectRequest {
            bucket: self.bucket.clone(),
            object: self.object.clone(),
            generation,
            if_generation_match: Some(generation),
            ..Default::default()
        };
        let request = self.request(request)?;
        self.client
            .clone()
            .delete_object(request)
            .await
            .map_err(|status| self.status(status))?;
        Ok(())
    }

    async fn finalize(
        &self,
        token: &mut AppendToken,
        write_offset: i64,
    ) -> Result<ReplicaSnapshot, TransportError> {
        // A healthy lane finishes on the same stream that conditionally
        // created the object. This needs neither another RPC nor a generation
        // lookup; the returned resource supplies the identity used by the
        // seal-time metadata CAS.
        if self.session.current.load().is_some() {
            let finalized = self
                .finish_live_session(write_offset, token.generation)
                .await?;
            token.generation = Some(finalized.generation);
            token.metageneration = Some(finalized.metageneration);
            return Ok(finalized);
        }

        // If an idle or failed stream disappeared before finalization, stat
        // only to obtain a candidate identity, then require the original
        // write handle to resume that exact server-side session. A replacement
        // generation cannot be adopted through this path.
        let (generation, _) = self.resolve_token_identity(token).await?;
        let request = BidiWriteObjectRequest {
            first_message: Some(bidi_write_object_request::FirstMessage::AppendObjectSpec(
                AppendObjectSpec {
                    bucket: self.bucket.clone(),
                    object: self.object.clone(),
                    generation,
                    if_metageneration_match: None,
                    write_handle: token
                        .write_handle
                        .clone()
                        .map(|handle| BidiWriteHandle { handle }),
                    ..Default::default()
                },
            )),
            write_offset,
            finish_write: true,
            ..Default::default()
        };
        let (resource, error) = self
            .drive_redirect_aware_stream(
                vec![request],
                || None,
                |resource, response| {
                    if let Some(bidi_write_object_response::WriteStatus::Resource(object)) =
                        response.write_status
                    {
                        *resource = Some(object);
                    }
                },
                Option::is_some,
            )
            .await;
        if let Some(error) = error {
            return Err(error);
        }
        let Some(resource) = resource else {
            return Err(self.error(
                TransportCode::Internal,
                "missing finalized resource response",
            ));
        };
        let finalized = self.stat_from_object(resource);
        if !finalized.finalized
            || finalized.generation != generation
            || finalized.persisted_size != write_offset
        {
            return Err(self.error(
                TransportCode::DataLoss,
                "finalized segment does not match the recovered prefix",
            ));
        }
        Ok(finalized)
    }

    async fn shutdown(&self) {
        self.session.shutdown.send_replace(true);
        self.replace_session(None).await;
    }
}

/// Bounded number of consecutive zonal redirects honored per write open.
const MAX_WRITE_REDIRECTS: u32 = 5;

/// Minimal view of `google.rpc.Status` for decoding the rich-error payload that
/// tonic exposes via `Status::details()` (the `grpc-status-details-bin` trailer).
#[derive(Clone, PartialEq, ::prost::Message)]
struct RichStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    details: ::prost::alloc::vec::Vec<::prost_types::Any>,
}

/// Extract the routing token from a `BidiWriteObjectRedirectedError` attached to
/// an ABORTED status, if present. The feature-gated `probe_support` module
/// exports this for repository probes that must replay redirects exactly like
/// the production transport.
///
/// Normal users do not need this helper; the production transport consumes and
/// caches redirects automatically.
pub fn redirect_routing_token(status: &Status) -> Option<String> {
    let details = status.details();
    if details.is_empty() {
        return None;
    }
    let rich = RichStatus::decode(details).ok()?;
    for any in rich.details {
        if any
            .type_url
            .ends_with("google.storage.v2.BidiWriteObjectRedirectedError")
        {
            if let Ok(redirect) = BidiWriteObjectRedirectedError::decode(any.value.as_slice()) {
                if let Some(token) = redirect.routing_token {
                    return Some(token);
                }
            }
        }
    }
    None
}
