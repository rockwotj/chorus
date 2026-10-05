//! Transport over the Chorus storage node TCP protocol (`chorus-wire`).
//!
//! A [`TcpReplicaFactory`] owns one multiplexed connection to one storage
//! node; every replica it creates shares that connection. Append sessions are
//! scoped to the connection (see the wire crate's `PROTOCOL.md`): when it
//! drops, every live session fails with `Unavailable`, the next request
//! reconnects, and lanes reattach through [`Replica::resume_tail`] with the
//! session's write handle.

mod conn;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use bytes::Bytes;
use chorus_wire::{ObjectInfo, ObjectVersion, Request, Response, SessionId, SessionOpened};
use tokio::sync::watch;

use crate::error::Error;
use crate::transport::{
    AppendSessionId, AppendToken, LaneDurableChange, LaneSessionDiagnostics, ListedObject,
    PackedAppend, Replica, ReplicaFactory, ReplicaRangeRead, ReplicaSnapshot, TransportCode,
    TransportError,
};
use conn::{Connection, Reply, SessionProgress};

/// Largest replacement payload sent inline in `ReplaceAppendable`; the rest
/// follows as appends on the new session, so no frame nears the wire limit.
const REPLACE_INLINE_BYTES: usize = 8 * 1024 * 1024;
/// Payload of each follow-up append of a large replacement.
const REPLACE_APPEND_BYTES: usize = 4 * 1024 * 1024;

static NEXT_APPEND_SESSION_ID: AtomicU64 = AtomicU64::new(1);

/// Replica factory for one bucket on one Chorus storage node, reached over a
/// single multiplexed TCP connection.
///
/// The connection is established (with its protocol handshake) by
/// [`TcpReplicaFactory::connect`]. If it later drops, pending requests and
/// live append sessions fail with `Unavailable` and the next request
/// reconnects transparently.
#[derive(Clone)]
pub struct TcpReplicaFactory {
    inner: Arc<FactoryInner>,
}

struct FactoryInner {
    addr: String,
    bucket: String,
    zone: usize,
    /// Fast path to the current connection.
    current: ArcSwapOption<Connection>,
    /// Serializes reconnects.
    reconnect: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for TcpReplicaFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpReplicaFactory")
            .field("addr", &self.inner.addr)
            .field("bucket", &self.inner.bucket)
            .field("zone", &self.inner.zone)
            .finish()
    }
}

impl TcpReplicaFactory {
    /// Connect to the storage node at `addr` (`host:port`) and perform the
    /// protocol handshake.
    ///
    /// `bucket` names the node-local namespace this factory reads and writes;
    /// keep it stable across restarts, since the volume binds replica identity
    /// to it. `zone` is the replica's position in the volume's factory list
    /// and is attached to every transport error.
    pub async fn connect(
        addr: impl Into<String>,
        bucket: impl Into<String>,
        zone: usize,
    ) -> Result<Self, Error> {
        let factory = Self {
            inner: Arc::new(FactoryInner {
                addr: addr.into(),
                bucket: bucket.into(),
                zone,
                current: ArcSwapOption::empty(),
                reconnect: tokio::sync::Mutex::new(()),
            }),
        };
        factory
            .inner
            .connection()
            .await
            .map_err(|error| Error::Connection(error.to_string()))?;
        Ok(factory)
    }

    /// Stable identity reported by the node in the latest handshake, or
    /// `None` while disconnected.
    pub fn node_id(&self) -> Option<String> {
        self.inner
            .current
            .load_full()
            .filter(|connection| !connection.is_closed())
            .map(|connection| connection.node_id().to_string())
    }

    fn object_replica(&self, object: &str) -> TcpReplica {
        TcpReplica {
            factory: Arc::clone(&self.inner),
            object: object.to_string(),
            session: Mutex::new(None),
            shutdown: AtomicBool::new(false),
        }
    }
}

impl FactoryInner {
    fn error(&self, code: TransportCode, message: impl Into<String>) -> TransportError {
        TransportError {
            zone: self.zone,
            code,
            message: message.into(),
        }
    }

    /// The live connection, reconnecting if the previous one was lost.
    async fn connection(&self) -> Result<Arc<Connection>, TransportError> {
        if let Some(connection) = self.current.load_full().filter(|c| !c.is_closed()) {
            return Ok(connection);
        }
        let _reconnecting = self.reconnect.lock().await;
        if let Some(connection) = self.current.load_full().filter(|c| !c.is_closed()) {
            return Ok(connection);
        }
        self.current.store(None);
        let connection = Connection::open(&self.addr, self.zone).await?;
        self.current.store(Some(Arc::clone(&connection)));
        Ok(connection)
    }

    async fn call(&self, request: Request) -> Result<Response, TransportError> {
        Ok(self.connection().await?.call(request).await?.response)
    }
}

fn unexpected(zone: usize, operation: &str, response: &Response) -> TransportError {
    TransportError {
        zone,
        code: TransportCode::Internal,
        message: format!("unexpected {operation} response: {response:?}"),
    }
}

fn last_modified(info: &ObjectInfo) -> Option<SystemTime> {
    let nanos = u64::try_from(info.last_modified_unix_nanos?).ok()?;
    UNIX_EPOCH.checked_add(Duration::from_nanos(nanos))
}

#[async_trait]
impl ReplicaFactory for TcpReplicaFactory {
    fn bucket_name(&self) -> &str {
        &self.inner.bucket
    }

    fn replica(&self, object: &str) -> Arc<dyn Replica> {
        Arc::new(self.object_replica(object))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ListedObject>, TransportError> {
        let response = self
            .inner
            .call(Request::List {
                bucket: self.inner.bucket.clone(),
                prefix: prefix.to_string(),
            })
            .await?;
        let Response::Listed(objects) = response else {
            return Err(unexpected(self.inner.zone, "List", &response));
        };
        Ok(objects
            .into_iter()
            .map(|info| ListedObject {
                zone: self.inner.zone,
                last_modified: last_modified(&info),
                name: info.name,
                generation: info.generation,
                size: info.size,
                finalized: info.finalized,
                crc32c: info.crc32c,
                metadata: info.metadata,
            })
            .collect())
    }
}

/// The live append session of one replica.
struct LiveSession {
    /// Process-wide id for diagnostics and logs.
    id: AppendSessionId,
    /// Server-assigned id, valid on `connection` only.
    session_id: SessionId,
    generation: i64,
    write_handle: Vec<u8>,
    connection: Arc<Connection>,
    progress: watch::Receiver<SessionProgress>,
}

impl LiveSession {
    /// Detach locally and ask the server to close the session.
    fn retire(&self, reason: &str) {
        self.connection.close_session(self.session_id, reason);
    }
}

/// One object on a storage node, bound through a [`TcpReplicaFactory`].
pub(crate) struct TcpReplica {
    factory: Arc<FactoryInner>,
    object: String,
    session: Mutex<Option<Arc<LiveSession>>>,
    shutdown: AtomicBool,
}

impl TcpReplica {
    fn zone(&self) -> usize {
        self.factory.zone
    }

    fn error(&self, code: TransportCode, message: impl Into<String>) -> TransportError {
        self.factory.error(code, message)
    }

    fn bucket(&self) -> String {
        self.factory.bucket.clone()
    }

    fn slot(&self) -> std::sync::MutexGuard<'_, Option<Arc<LiveSession>>> {
        self.session
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn live_session(&self) -> Result<Arc<LiveSession>, TransportError> {
        self.slot().clone().ok_or_else(|| {
            self.error(
                TransportCode::Unavailable,
                "no live append session (resume required)",
            )
        })
    }

    /// Install `session` as the live session, retiring the previous one.
    fn install(&self, session: Option<Arc<LiveSession>>) {
        let previous = std::mem::replace(&mut *self.slot(), session);
        if let Some(previous) = previous {
            previous.retire("append session replaced");
        }
    }

    /// Retire `expected` if it is still the live session.
    fn clear_session_if(&self, expected: &Arc<LiveSession>) {
        let previous = {
            let mut slot = self.slot();
            if slot
                .as_ref()
                .is_some_and(|session| Arc::ptr_eq(session, expected))
            {
                slot.take()
            } else {
                None
            }
        };
        if let Some(previous) = previous {
            previous.retire("append session cleared");
        }
    }

    fn check_shutdown(&self) -> Result<(), TransportError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(self.error(
                TransportCode::Unavailable,
                "append session open cancelled by shutdown",
            ));
        }
        Ok(())
    }

    /// Send a session-opening request and wrap the opened session. The caller
    /// decides whether to install it.
    async fn open_session(
        &self,
        request: Request,
    ) -> Result<(Arc<LiveSession>, SessionOpened), TransportError> {
        self.check_shutdown()?;
        let connection = self.factory.connection().await?;
        let Reply { response, session } = connection.call(request).await?;
        let (Response::SessionOpened(opened), Some(progress)) = (response, session) else {
            return Err(self.error(
                TransportCode::Internal,
                "session open answered without a session",
            ));
        };
        let session = Arc::new(LiveSession {
            id: AppendSessionId(NEXT_APPEND_SESSION_ID.fetch_add(1, Ordering::Relaxed)),
            session_id: opened.session_id,
            generation: opened.generation,
            write_handle: opened.write_handle.clone(),
            connection,
            progress,
        });
        if let Err(error) = self.check_shutdown() {
            session.retire("append session opened during shutdown");
            return Err(error);
        }
        tracing::debug!(
            zone = self.zone(),
            session_id = %session.id,
            server_session_id = opened.session_id,
            generation = opened.generation,
            persisted_size = opened.persisted_size,
            "append session opened"
        );
        Ok((session, opened))
    }

    fn token(&self, opened: &SessionOpened) -> AppendToken {
        AppendToken {
            zone: self.zone(),
            generation: Some(opened.generation),
            metageneration: Some(opened.metageneration),
            persisted_size: opened.persisted_size,
            write_handle: Some(Bytes::from(opened.write_handle.clone())),
        }
    }

    fn snapshot_from_info(&self, info: ObjectInfo, bytes: Vec<u8>) -> ReplicaSnapshot {
        ReplicaSnapshot {
            zone: self.zone(),
            generation: info.generation,
            metageneration: info.metageneration,
            persisted_size: bytes.len() as i64,
            finalized: info.finalized,
            crc32c: info.crc32c,
            metadata: info.metadata,
            bytes,
        }
    }

    /// Metadata-only view: tail-blind for open objects, exactly as the gRPC
    /// transport reports a `GetObject`.
    fn stat_from_info(&self, info: ObjectInfo) -> ReplicaSnapshot {
        let size = info.size;
        let mut snapshot = self.snapshot_from_info(info, Vec::new());
        if snapshot.finalized {
            snapshot.persisted_size = size;
        }
        snapshot
    }

    /// Read the durable bytes `[offset, persisted_size)` of one generation.
    /// A node may answer with a prefix of the range (to bound frame size);
    /// the remainder is read from the same generation.
    async fn read_from(&self, offset: i64) -> Result<(ObjectInfo, Vec<u8>), TransportError> {
        if offset < 0 {
            return Err(self.error(
                TransportCode::InvalidArgument,
                "read offset must be nonnegative",
            ));
        }
        let mut info: Option<ObjectInfo> = None;
        let mut bytes = Vec::new();
        loop {
            let at = offset + bytes.len() as i64;
            let response = self
                .factory
                .call(Request::Read {
                    bucket: self.bucket(),
                    object: self.object.clone(),
                    offset: at,
                })
                .await?;
            let Response::ReadData {
                info: read,
                bytes: chunk,
            } = response
            else {
                return Err(unexpected(self.zone(), "Read", &response));
            };
            if let Some(first) = &info {
                if first.generation != read.generation {
                    return Err(self.error(
                        TransportCode::Unavailable,
                        "object generation changed during a multi-part read",
                    ));
                }
            }
            let empty = chunk.is_empty();
            bytes.extend_from_slice(&chunk);
            let target = info.as_ref().unwrap_or(&read).persisted_size;
            let first = info.get_or_insert(read);
            if offset + bytes.len() as i64 >= target {
                return Ok((first.clone(), bytes));
            }
            if empty {
                return Err(self.error(
                    TransportCode::Internal,
                    "read returned no bytes before the durable tail",
                ));
            }
        }
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

    /// Queue a packed group as one `Append` frame per wire message.
    async fn lane_send_messages(
        &self,
        write_offset: i64,
        packed: &PackedAppend,
        flush: bool,
    ) -> Result<(), TransportError> {
        if packed.is_empty() {
            return Err(self.error(
                TransportCode::Internal,
                "append lane cannot send an empty flush group",
            ));
        }
        let session = self.live_session()?;
        let messages = packed.messages();
        let last_index = messages.len() - 1;
        for (index, message) in messages.iter().enumerate() {
            let request = Request::Append {
                session_id: session.session_id,
                offset: write_offset + message.relative_offset,
                data: message.content.to_vec(),
                crc32c: message.crc32c,
                flush: flush && index == last_index,
            };
            if let Err(error) = session.connection.send(request).await {
                self.clear_session_if(&session);
                return Err(error);
            }
        }
        Ok(())
    }

    /// Wait until `session`'s durable tail reaches `target` or it fails.
    async fn wait_durable(
        &self,
        session: &Arc<LiveSession>,
        target: i64,
    ) -> Result<(), TransportError> {
        let mut progress = session.progress.clone();
        let wait = async {
            loop {
                let state = progress.borrow_and_update().clone();
                if state.durable >= target {
                    return Ok(());
                }
                if let Some(error) = state.error {
                    return Err(error);
                }
                if progress.changed().await.is_err() && progress.borrow().durable < target {
                    return Err(self.error(TransportCode::Unavailable, "append session ended"));
                }
            }
        };
        match tokio::time::timeout(conn::RPC_TIMEOUT, wait).await {
            Ok(result) => result,
            Err(_) => Err(self.error(
                TransportCode::DeadlineExceeded,
                "append session made no durable progress",
            )),
        }
    }

    fn verify_finalized(
        &self,
        response: Response,
        generation: i64,
        write_offset: i64,
    ) -> Result<ReplicaSnapshot, TransportError> {
        let Response::Object(info) = response else {
            return Err(unexpected(self.zone(), "Finalize", &response));
        };
        let finalized = self.stat_from_info(info);
        if !finalized.finalized
            || finalized.generation != generation
            || finalized.persisted_size != write_offset
        {
            tracing::warn!(
                zone = self.zone(),
                expected_generation = generation,
                actual_generation = finalized.generation,
                expected_size = write_offset,
                actual_size = finalized.persisted_size,
                finalized = finalized.finalized,
                "finalize response did not match the requested prefix"
            );
            return Err(self.error(
                TransportCode::DataLoss,
                "finalized segment does not match the committed prefix",
            ));
        }
        Ok(finalized)
    }
}

#[async_trait]
impl Replica for TcpReplica {
    async fn snapshot(&self) -> Result<ReplicaSnapshot, TransportError> {
        let (info, bytes) = self.read_from(0).await?;
        Ok(self.snapshot_from_info(info, bytes))
    }

    async fn read_range(&self, offset: i64) -> Result<ReplicaRangeRead, TransportError> {
        let (info, bytes) = self.read_from(offset).await?;
        Ok(ReplicaRangeRead {
            zone: self.zone(),
            generation: info.generation,
            bytes,
        })
    }

    async fn stat(&self) -> Result<ReplicaSnapshot, TransportError> {
        let response = self
            .factory
            .call(Request::Stat {
                bucket: self.bucket(),
                object: self.object.clone(),
            })
            .await?;
        let Response::Object(info) = response else {
            return Err(unexpected(self.zone(), "Stat", &response));
        };
        Ok(self.stat_from_info(info))
    }

    async fn create_append_session(
        &self,
        metadata: HashMap<String, String>,
    ) -> Result<AppendToken, TransportError> {
        let request = Request::CreateAppendable {
            bucket: self.bucket(),
            object: self.object.clone(),
            metadata,
        };
        let (session, opened) = self.open_session(request).await.map_err(|error| {
            // The gRPC transport reports a lost conditional create the same way.
            if error.code == TransportCode::FailedPrecondition {
                TransportError {
                    code: TransportCode::AlreadyExists,
                    ..error
                }
            } else {
                error
            }
        })?;
        self.install(Some(session));
        Ok(self.token(&opened))
    }

    async fn resume_tail(&self, token: &mut AppendToken) -> Result<i64, TransportError> {
        let (generation, metageneration) = self.resolve_token_identity(token).await?;
        // Detach the old session first so the server closes it quietly
        // instead of fencing it when the resume displaces it.
        self.install(None);
        let request = match &token.write_handle {
            Some(handle) => Request::Resume {
                bucket: self.bucket(),
                object: self.object.clone(),
                write_handle: handle.to_vec(),
            },
            // Without a handle, fall back to a guarded handle-free open.
            None => Request::Takeover {
                bucket: self.bucket(),
                object: self.object.clone(),
                if_match: ObjectVersion {
                    generation,
                    metageneration,
                },
            },
        };
        let (session, opened) = self.open_session(request).await?;
        self.install(Some(session));
        token.persisted_size = opened.persisted_size;
        token.write_handle = Some(Bytes::from(opened.write_handle));
        tracing::debug!(
            zone = self.zone(),
            persisted_size = opened.persisted_size,
            "append session resumed"
        );
        Ok(opened.persisted_size)
    }

    async fn takeover(&self, observed: &ReplicaSnapshot) -> Result<AppendToken, TransportError> {
        let request = Request::Takeover {
            bucket: self.bucket(),
            object: self.object.clone(),
            if_match: ObjectVersion {
                generation: observed.generation,
                metageneration: observed.metageneration,
            },
        };
        let (session, opened) = self.open_session(request).await?;
        self.install(Some(session));
        Ok(self.token(&opened))
    }

    async fn replace_appendable(
        &self,
        observed: Option<&ReplicaSnapshot>,
        data: Bytes,
        metadata: HashMap<String, String>,
    ) -> Result<AppendToken, TransportError> {
        let inline = data.len().min(REPLACE_INLINE_BYTES);
        let request = Request::ReplaceAppendable {
            bucket: self.bucket(),
            object: self.object.clone(),
            if_match: observed.map(|observed| ObjectVersion {
                generation: observed.generation,
                metageneration: observed.metageneration,
            }),
            data: data[..inline].to_vec(),
            metadata,
        };
        let (session, opened) = self.open_session(request).await.map_err(|error| {
            // Both modes report a lost race as a failed precondition.
            if error.code == TransportCode::AlreadyExists {
                TransportError {
                    code: TransportCode::FailedPrecondition,
                    ..error
                }
            } else {
                error
            }
        })?;
        if opened.persisted_size != inline as i64 {
            session.retire("replacement size mismatch");
            return Err(self.error(
                TransportCode::DataLoss,
                format!(
                    "replacement persisted {}, expected {inline}",
                    opened.persisted_size
                ),
            ));
        }
        // Stream any remainder through the new session and wait until all of
        // it is durable.
        if inline < data.len() {
            let mut offset = inline;
            while offset < data.len() {
                let end = (offset + REPLACE_APPEND_BYTES).min(data.len());
                let chunk = &data[offset..end];
                let request = Request::Append {
                    session_id: session.session_id,
                    offset: offset as i64,
                    data: chunk.to_vec(),
                    crc32c: crc32c::crc32c(chunk),
                    flush: end == data.len(),
                };
                if let Err(error) = session.connection.send(request).await {
                    session.retire("replacement append failed");
                    return Err(error);
                }
                offset = end;
            }
            if let Err(error) = self.wait_durable(&session, data.len() as i64).await {
                session.retire("replacement append failed");
                return Err(error);
            }
        }
        self.install(Some(session));
        let mut token = self.token(&opened);
        token.persisted_size = data.len() as i64;
        Ok(token)
    }

    async fn lane_send(
        &self,
        write_offset: i64,
        packed: &PackedAppend,
    ) -> Result<(), TransportError> {
        self.lane_send_messages(write_offset, packed, true).await
    }

    async fn lane_send_unflushed(
        &self,
        write_offset: i64,
        packed: &PackedAppend,
    ) -> Result<(), TransportError> {
        self.lane_send_messages(write_offset, packed, false).await
    }

    async fn lane_flush(&self, write_offset: i64) -> Result<(), TransportError> {
        let session = self.live_session()?;
        let request = Request::Flush {
            session_id: session.session_id,
            offset: write_offset,
        };
        if let Err(error) = session.connection.send(request).await {
            self.clear_session_if(&session);
            return Err(error);
        }
        Ok(())
    }

    async fn lane_durable_change(&self, seen: i64) -> Result<LaneDurableChange, TransportError> {
        let session = self.live_session()?;
        let mut progress = session.progress.clone();
        let mut ended = false;
        loop {
            let state = progress.borrow_and_update().clone();
            // Report durable progress together with a coalesced error so the
            // protocol publishes the physical progress before classifying it.
            if state.durable > seen {
                if state.error.is_some() {
                    self.clear_session_if(&session);
                }
                return Ok(LaneDurableChange {
                    persisted_size: state.durable,
                    error: state.error,
                });
            }
            if let Some(error) = state.error {
                self.clear_session_if(&session);
                return Err(error);
            }
            if ended {
                self.clear_session_if(&session);
                return Err(self.error(TransportCode::Unavailable, "append session ended"));
            }
            ended = progress.changed().await.is_err();
        }
    }

    fn lane_session_diagnostics(&self) -> LaneSessionDiagnostics {
        let Some(session) = self.slot().clone() else {
            return LaneSessionDiagnostics::default();
        };
        let open = !session.connection.is_closed()
            && session.progress.has_changed().is_ok()
            && session.progress.borrow().error.is_none();
        LaneSessionDiagnostics {
            session_id: Some(session.id),
            response_stream_open: Some(open),
        }
    }

    async fn delete(&self, generation: i64) -> Result<(), TransportError> {
        let response = self
            .factory
            .call(Request::Delete {
                bucket: self.bucket(),
                object: self.object.clone(),
                generation,
            })
            .await?;
        match response {
            Response::Deleted => Ok(()),
            other => Err(unexpected(self.zone(), "Delete", &other)),
        }
    }

    async fn finalize(
        &self,
        token: &mut AppendToken,
        write_offset: i64,
    ) -> Result<ReplicaSnapshot, TransportError> {
        // A healthy lane finalizes with its live session's handle, on the
        // connection that carried its appends, so the finalize is ordered
        // after them.
        let live = self.slot().clone();
        if let Some(session) = live.filter(|session| !session.connection.is_closed()) {
            let request = Request::Finalize {
                bucket: self.bucket(),
                object: self.object.clone(),
                generation: session.generation,
                write_offset,
                write_handle: Some(session.write_handle.clone()),
            };
            let result = session.connection.call(request).await;
            // A successful finalize closed the session server-side; a failed
            // one leaves it unusable for this lane either way.
            let previous = {
                let mut slot = self.slot();
                match slot.as_ref() {
                    Some(current) if Arc::ptr_eq(current, &session) => slot.take(),
                    _ => None,
                }
            };
            if previous.is_some() {
                if result.is_ok() {
                    session
                        .connection
                        .forget_session(session.session_id, "append session finalized");
                } else {
                    session.retire("append session finalize failed");
                }
            }
            let finalized =
                self.verify_finalized(result?.response, session.generation, write_offset)?;
            token.generation = Some(finalized.generation);
            token.metageneration = Some(finalized.metageneration);
            return Ok(finalized);
        }
        self.install(None);

        // No live session: finalize by write handle (or, without one, as an
        // implicit takeover). The node answers an already-finalized object at
        // this generation and length idempotently.
        let (generation, _) = self.resolve_token_identity(token).await?;
        let response = self
            .factory
            .call(Request::Finalize {
                bucket: self.bucket(),
                object: self.object.clone(),
                generation,
                write_offset,
                write_handle: token.write_handle.as_ref().map(|handle| handle.to_vec()),
            })
            .await?;
        let finalized = self.verify_finalized(response, generation, write_offset)?;
        token.metageneration = Some(finalized.metageneration);
        Ok(finalized)
    }

    async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.install(None);
    }
}
