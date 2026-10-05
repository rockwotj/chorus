//! The in-memory coordination layer: the object table, generations, writer
//! epochs, append sessions and durable tails. See `rust/wire/PROTOCOL.md` for
//! the normative semantics of every request.
//!
//! # Concurrency model
//!
//! The store lives on one single-threaded runtime and is shared as
//! `Rc<Store<B>>`. Its tables sit in a `RefCell` that is never borrowed
//! across an `.await`. Every request that reads or changes an object's bytes
//! or metadata first takes that object's async lock ([`Store::lock`]) and
//! holds it across its backend calls, so operations on one object are
//! linearized in the order they acquire the lock, and a backend never sees two
//! concurrent calls for one object. `List` and `Stat` read the table without
//! the lock: they observe every acknowledged mutation.
//!
//! The connection layer awaits each request before decoding the next one, so
//! requests from one connection are applied in arrival order.
//!
//! # Group commit
//!
//! By default a flush (`Flush`, or `Append` with `flush`) syncs inline while
//! holding the object lock, so the connection's next request waits for the
//! fdatasync. After [`Store::enable_group_commit`], a flush only records the
//! session as a waiter on its object and returns. One background task per
//! object (started on demand, exiting when idle) repeatedly snapshots the
//! accepted size, calls [`Backend::sync`] *without* the object lock, raises
//! the durable size to the snapshot, and sends `Durable` to every waiter that
//! is still live. Flushes that arrive during a sync are coalesced into the
//! next one. Appends keep being written (and ordered) under the lock while a
//! sync runs; a sync covers every write that completed before it started,
//! which is exactly the snapshot. The durable size only ever grows (to the
//! larger of concurrent sync results), and every `Durable` carries the
//! object's durable size at emission time, so events stay monotonic per
//! session. `Takeover`, `Resume` and `Finalize` still sync inline before they
//! answer. A session that died meanwhile gets no `Durable` (its
//! `SessionFailed` stays its last event), and a waiter only exists after its
//! session's `SessionOpened` was queued.
//!
//! A newly opened session is registered after the last `.await` of the
//! opening request, so no event for it can be queued before the caller
//! queues the `SessionOpened` response.

#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use chorus_wire::{
    Event, ObjectInfo, ObjectVersion, Request, Response, ServerFrame, SessionId, SessionOpened,
    WireCode, WireError, WriteHandle,
};
use futures::channel::mpsc;
use futures::lock::{Mutex, OwnedMutexGuard};

use crate::backend::{Backend, ObjectKey, ObjectMeta};

/// Largest payload of one `ReadData` response; a client reads larger ranges
/// in several requests.
pub const READ_CHUNK_BYTES: usize = 16 * 1024 * 1024;
/// Largest total size of custom metadata keys plus values.
pub const MAX_METADATA_BYTES: usize = 8 * 1024;

/// Identifier of one client connection, unique for the process lifetime.
pub type ConnId = u64;

/// Queue of frames to one connection's writer.
pub type Outbox = mpsc::UnboundedSender<ServerFrame>;

/// The connection a request arrived on: sessions it opens are scoped to it,
/// and their events go to its outbox.
#[derive(Clone, Debug)]
pub struct ConnCtx {
    /// Connection id from [`Store::new_connection_id`].
    pub id: ConnId,
    /// The connection's outbound frame queue.
    pub outbox: Outbox,
}

/// Result of one request.
pub type Reply = Result<Response, WireError>;

fn error(code: WireCode, message: impl Into<String>) -> WireError {
    WireError::new(code, message)
}

fn precondition(message: impl Into<String>) -> WireError {
    error(WireCode::FailedPrecondition, message)
}

fn internal(context: &str, io: std::io::Error) -> WireError {
    error(WireCode::Internal, format!("{context}: {io}"))
}

fn not_found(key: &ObjectKey) -> WireError {
    error(
        WireCode::NotFound,
        format!("object {}/{} not found", key.bucket, key.name),
    )
}

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn check_metadata(metadata: &HashMap<String, String>) -> Result<(), WireError> {
    let bytes: usize = metadata.iter().map(|(k, v)| k.len() + v.len()).sum();
    if bytes > MAX_METADATA_BYTES {
        return Err(error(
            WireCode::InvalidArgument,
            format!("metadata is {bytes} bytes, limit {MAX_METADATA_BYTES}"),
        ));
    }
    Ok(())
}

fn decode_handle(bytes: &[u8]) -> Result<WriteHandle, WireError> {
    WriteHandle::decode(bytes)
        .ok_or_else(|| error(WireCode::InvalidArgument, "malformed write handle"))
}

/// Pending group-commit flushes of one object generation.
#[derive(Debug, Default)]
struct FlushQueue {
    /// Sessions owed a `Durable` once the next sync completes.
    waiters: Vec<SessionId>,
    /// Whether a group-commit task is running for this generation.
    running: bool,
}

/// One live object generation.
#[derive(Debug)]
struct Object {
    meta: ObjectMeta,
    /// Bytes accepted (written to the backend, maybe not durable).
    accepted: u64,
    accepted_crc: u32,
    /// Bytes durable.
    durable: u64,
    durable_crc: u32,
    flush: FlushQueue,
}

impl Object {
    /// An object whose `size` bytes (with checksum `crc`) are all durable.
    fn new(meta: ObjectMeta, size: u64, crc: u32) -> Self {
        Self {
            meta,
            accepted: size,
            accepted_crc: crc,
            durable: size,
            durable_crc: crc,
            flush: FlushQueue::default(),
        }
    }

    fn info(&self, name: &str) -> ObjectInfo {
        ObjectInfo {
            name: name.to_string(),
            generation: self.meta.generation,
            metageneration: self.meta.metageneration,
            size: if self.meta.finalized {
                self.meta.finalized_size as i64
            } else {
                0
            },
            persisted_size: self.durable as i64,
            finalized: self.meta.finalized,
            crc32c: Some(self.durable_crc),
            last_modified_unix_nanos: Some(self.meta.last_modified_unix_nanos),
            metadata: self.meta.metadata.clone(),
        }
    }

    fn matches(&self, version: &ObjectVersion) -> bool {
        self.meta.generation == version.generation
            && self.meta.metageneration == version.metageneration
    }
}

#[derive(Debug)]
struct Session {
    key: ObjectKey,
    generation: i64,
    epoch: u64,
    conn: ConnId,
    outbox: Outbox,
    /// Set once the session is dead; later requests fail with this error.
    dead: Option<WireError>,
}

impl Session {
    fn emit(&self, event: Event) {
        // A closed outbox means the connection is gone; the event is dropped.
        let _ = self.outbox.unbounded_send(ServerFrame::Event(event));
    }
}

#[derive(Debug, Default)]
struct State {
    objects: BTreeMap<ObjectKey, Object>,
    sessions: HashMap<SessionId, Session>,
    next_session_id: SessionId,
    next_conn_id: ConnId,
    last_generation: i64,
}

impl State {
    fn object(&self, key: &ObjectKey) -> Result<&Object, WireError> {
        self.objects.get(key).ok_or_else(|| not_found(key))
    }

    fn object_mut(&mut self, key: &ObjectKey) -> Result<&mut Object, WireError> {
        self.objects.get_mut(key).ok_or_else(|| not_found(key))
    }

    /// A generation greater than every earlier one: wall-clock microseconds,
    /// or the previous value plus one.
    fn next_generation(&mut self) -> i64 {
        let micros = now_nanos() / 1_000;
        self.last_generation = micros.max(self.last_generation + 1);
        self.last_generation
    }

    fn open_session(
        &mut self,
        conn: &ConnCtx,
        key: &ObjectKey,
        generation: i64,
        epoch: u64,
    ) -> SessionId {
        self.next_session_id += 1;
        let session_id = self.next_session_id;
        self.sessions.insert(
            session_id,
            Session {
                key: key.clone(),
                generation,
                epoch,
                conn: conn.id,
                outbox: conn.outbox.clone(),
                dead: None,
            },
        );
        session_id
    }

    fn opened(&self, session_id: SessionId, key: &ObjectKey) -> SessionOpened {
        let object = &self.objects[key];
        SessionOpened {
            session_id,
            generation: object.meta.generation,
            metageneration: object.meta.metageneration,
            persisted_size: object.durable as i64,
            write_handle: WriteHandle {
                generation: object.meta.generation,
                writer_epoch: object.meta.writer_epoch,
            }
            .encode(),
        }
    }

    /// Kill every live session on `key` with `SessionFailed(error)`, except
    /// `quiet`, which is removed without an event.
    fn fence_sessions(&mut self, key: &ObjectKey, quiet: Option<SessionId>, error: &WireError) {
        if let Some(quiet) = quiet {
            self.sessions.remove(&quiet);
        }
        for (&session_id, session) in self.sessions.iter_mut() {
            if &session.key == key && session.dead.is_none() {
                session.dead = Some(error.clone());
                session.emit(Event::SessionFailed {
                    session_id,
                    error: error.clone(),
                });
            }
        }
    }

    /// Kill one session with `SessionFailed(error)`.
    fn fail_session(&mut self, session_id: SessionId, error: &WireError) {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            if session.dead.is_none() {
                session.dead = Some(error.clone());
                session.emit(Event::SessionFailed {
                    session_id,
                    error: error.clone(),
                });
            }
        }
    }

    /// The live session `session_id` of `conn`, or the error a request naming
    /// it fails with (no event: a dead session already got its one event).
    fn live_session(&self, conn: &ConnCtx, session_id: SessionId) -> Result<&Session, WireError> {
        match self.sessions.get(&session_id) {
            Some(session) if session.conn == conn.id => match &session.dead {
                None => Ok(session),
                Some(error) => Err(error.clone()),
            },
            _ => Err(precondition(format!("unknown append session {session_id}"))),
        }
    }
}

/// Exclusive access to one object while held.
struct ObjectLock<'a, B> {
    store: &'a Store<B>,
    key: ObjectKey,
    guard: Option<OwnedMutexGuard<()>>,
}

impl<B> Drop for ObjectLock<'_, B> {
    fn drop(&mut self) {
        drop(self.guard.take());
        let mut locks = self.store.locks.borrow_mut();
        if locks
            .get(&self.key)
            .is_some_and(|lock| Arc::strong_count(lock) == 1)
        {
            locks.remove(&self.key);
        }
    }
}

/// The coordination layer of one storage node over a persistence
/// [`Backend`].
pub struct Store<B> {
    node_id: String,
    backend: B,
    state: RefCell<State>,
    /// Per-object operation locks; an entry exists while someone holds or
    /// waits for it.
    locks: RefCell<HashMap<ObjectKey, Arc<Mutex<()>>>>,
    /// Set by [`Store::enable_group_commit`]: the store itself, for spawning
    /// background sync tasks.
    group_commit: RefCell<Option<Weak<Self>>>,
}

impl<B: Backend + 'static> Store<B> {
    /// Open the store, loading whatever the backend recovers.
    pub async fn open(node_id: impl Into<String>, backend: B) -> std::io::Result<Self> {
        let recovered = backend.recover().await?;
        let mut state = State {
            last_generation: recovered.last_generation,
            ..State::default()
        };
        for object in recovered.objects {
            state.last_generation = state.last_generation.max(object.meta.generation);
            state.objects.insert(
                object.key,
                Object::new(object.meta, object.durable_size, object.durable_crc32c),
            );
        }
        Ok(Self {
            node_id: node_id.into(),
            backend,
            state: RefCell::new(state),
            locks: RefCell::new(HashMap::new()),
            group_commit: RefCell::new(None),
        })
    }

    /// Run flushes in background per-object tasks that coalesce concurrent
    /// flushes (see the module docs), so a flush does not hold up the
    /// connection's next request. Call within the compio runtime that serves
    /// the store; the tasks are spawned on it.
    pub fn enable_group_commit(self: &Rc<Self>) {
        *self.group_commit.borrow_mut() = Some(Rc::downgrade(self));
    }

    /// Number of objects in the table (for diagnostics).
    pub fn object_count(&self) -> usize {
        self.state.borrow().objects.len()
    }

    /// Stable identity reported in `HelloOk`.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// The persistence backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Allocate an id for a new connection.
    pub fn new_connection_id(&self) -> ConnId {
        let mut state = self.state.borrow_mut();
        state.next_conn_id += 1;
        state.next_conn_id
    }

    /// Forget every session of a closed connection. Objects and epochs are
    /// unaffected; the client resumes with its write handles.
    pub fn connection_closed(&self, conn: ConnId) {
        self.state
            .borrow_mut()
            .sessions
            .retain(|_, session| session.conn != conn);
    }

    /// Number of sessions (live or dead) currently tracked; for tests and
    /// diagnostics.
    pub fn session_count(&self) -> usize {
        self.state.borrow().sessions.len()
    }

    async fn lock(&self, key: &ObjectKey) -> ObjectLock<'_, B> {
        let lock = Arc::clone(self.locks.borrow_mut().entry(key.clone()).or_default());
        let guard = lock.lock_owned().await;
        ObjectLock {
            store: self,
            key: key.clone(),
            guard: Some(guard),
        }
    }

    /// Apply one request from `conn`. `Hello` is handled by the connection
    /// layer; here it is a protocol violation.
    pub async fn handle(&self, conn: &ConnCtx, request: Request) -> Reply {
        match request {
            Request::Hello { .. } => Err(error(
                WireCode::InvalidArgument,
                "Hello must be the first and only handshake",
            )),
            Request::List { bucket, prefix } => Ok(Response::Listed(self.list(&bucket, &prefix))),
            Request::Stat { bucket, object } => self.stat(&ObjectKey::new(bucket, object)),
            Request::Read {
                bucket,
                object,
                offset,
            } => self.read(&ObjectKey::new(bucket, object), offset).await,
            Request::CreateAppendable {
                bucket,
                object,
                metadata,
            } => {
                self.create_appendable(conn, &ObjectKey::new(bucket, object), metadata)
                    .await
            }
            Request::Takeover {
                bucket,
                object,
                if_match,
            } => {
                self.takeover(conn, &ObjectKey::new(bucket, object), if_match)
                    .await
            }
            Request::Resume {
                bucket,
                object,
                write_handle,
            } => {
                self.resume(conn, &ObjectKey::new(bucket, object), &write_handle)
                    .await
            }
            Request::ReplaceAppendable {
                bucket,
                object,
                if_match,
                data,
                metadata,
            } => {
                self.replace_appendable(
                    conn,
                    &ObjectKey::new(bucket, object),
                    if_match,
                    data,
                    metadata,
                )
                .await
            }
            Request::Append {
                session_id,
                offset,
                data,
                crc32c,
                flush,
            } => {
                self.append(conn, session_id, offset, data, crc32c, flush)
                    .await
            }
            Request::Flush { session_id, offset } => self.flush(conn, session_id, offset).await,
            Request::Finalize {
                bucket,
                object,
                generation,
                write_offset,
                write_handle,
            } => {
                self.finalize(
                    conn,
                    &ObjectKey::new(bucket, object),
                    generation,
                    write_offset,
                    write_handle,
                )
                .await
            }
            Request::Delete {
                bucket,
                object,
                generation,
            } => {
                self.delete(&ObjectKey::new(bucket, object), generation)
                    .await
            }
            Request::CloseSession { session_id } => {
                self.close_session(conn, session_id);
                Ok(Response::SessionClosed)
            }
        }
    }

    /// Every live object of `bucket` whose name starts with `prefix`, sorted
    /// by name.
    pub fn list(&self, bucket: &str, prefix: &str) -> Vec<ObjectInfo> {
        let state = self.state.borrow();
        state
            .objects
            .range(ObjectKey::new(bucket, prefix)..)
            .take_while(|(key, _)| key.bucket == bucket && key.name.starts_with(prefix))
            .map(|(key, object)| object.info(&key.name))
            .collect()
    }

    fn stat(&self, key: &ObjectKey) -> Reply {
        let state = self.state.borrow();
        Ok(Response::Object(state.object(key)?.info(&key.name)))
    }

    async fn read(&self, key: &ObjectKey, offset: i64) -> Reply {
        if offset < 0 {
            return Err(error(
                WireCode::InvalidArgument,
                "read offset must be nonnegative",
            ));
        }
        let _lock = self.lock(key).await;
        let (info, generation, durable, crc) = {
            let state = self.state.borrow();
            let object = state.object(key)?;
            (
                object.info(&key.name),
                object.meta.generation,
                object.durable,
                object.durable_crc,
            )
        };
        let offset = offset as u64;
        if offset > durable {
            return Err(error(
                WireCode::OutOfRange,
                format!("read offset {offset} beyond the durable tail {durable}"),
            ));
        }
        let len = (durable - offset).min(READ_CHUNK_BYTES as u64) as usize;
        let bytes = self
            .backend
            .read(key, generation, offset, len)
            .await
            .map_err(|io| internal("read", io))?;
        if offset == 0 && len as u64 == durable && crc32c::crc32c(&bytes) != crc {
            return Err(error(
                WireCode::DataLoss,
                "stored bytes do not match their checksum",
            ));
        }
        Ok(Response::ReadData { info, bytes })
    }

    async fn create_appendable(
        &self,
        conn: &ConnCtx,
        key: &ObjectKey,
        metadata: HashMap<String, String>,
    ) -> Reply {
        check_metadata(&metadata)?;
        let _lock = self.lock(key).await;
        let meta = {
            let mut state = self.state.borrow_mut();
            if state.objects.contains_key(key) {
                return Err(error(
                    WireCode::AlreadyExists,
                    format!("object {}/{} already exists", key.bucket, key.name),
                ));
            }
            ObjectMeta {
                generation: state.next_generation(),
                metageneration: 1,
                metadata,
                finalized: false,
                finalized_size: 0,
                finalized_crc32c: 0,
                writer_epoch: 1,
                last_modified_unix_nanos: now_nanos(),
            }
        };
        self.backend
            .create(key, &meta, Vec::new())
            .await
            .map_err(|io| internal("create", io))?;
        let mut state = self.state.borrow_mut();
        let (generation, epoch) = (meta.generation, meta.writer_epoch);
        state.objects.insert(key.clone(), Object::new(meta, 0, 0));
        let session_id = state.open_session(conn, key, generation, epoch);
        Ok(Response::SessionOpened(state.opened(session_id, key)))
    }

    /// Make every accepted byte of `key` durable (the caller holds the
    /// object lock). Returns the durable size.
    async fn sync_object(&self, key: &ObjectKey) -> Result<u64, WireError> {
        let (generation, accepted, accepted_crc, durable) = {
            let state = self.state.borrow();
            let object = state.object(key)?;
            (
                object.meta.generation,
                object.accepted,
                object.accepted_crc,
                object.durable,
            )
        };
        if accepted == durable {
            return Ok(durable);
        }
        self.backend
            .sync(key, generation)
            .await
            .map_err(|io| internal("sync", io))?;
        self.advance_durable(key, generation, accepted, accepted_crc);
        Ok(self.state.borrow().object(key)?.durable)
    }

    /// Record that the first `size` bytes (checksum `crc`) of `generation`
    /// are durable. A concurrent group-commit sync may already have recorded
    /// more; the durable size never goes back.
    fn advance_durable(&self, key: &ObjectKey, generation: i64, size: u64, crc: u32) {
        let mut state = self.state.borrow_mut();
        if let Some(object) = state.objects.get_mut(key) {
            if object.meta.generation == generation && size > object.durable {
                object.durable = size;
                object.durable_crc = crc;
                object.meta.last_modified_unix_nanos = now_nanos();
            }
        }
    }

    /// Make the session's object durable through its accepted size and send
    /// the session `Durable`: inline, or (with group commit) by queueing the
    /// session for the object's background sync task. The caller holds the
    /// object lock.
    async fn request_flush(&self, key: &ObjectKey, session_id: SessionId) -> Result<(), WireError> {
        let store = self.group_commit.borrow().as_ref().and_then(Weak::upgrade);
        let Some(store) = store else {
            return self.flush_session(key, session_id).await;
        };
        let generation = {
            let mut state = self.state.borrow_mut();
            let object = state.object_mut(key)?;
            object.flush.waiters.push(session_id);
            if object.flush.running {
                return Ok(());
            }
            object.flush.running = true;
            object.meta.generation
        };
        let key = key.clone();
        compio::runtime::spawn(async move { store.group_commit_loop(key, generation).await })
            .detach();
        Ok(())
    }

    /// The background sync task of one object generation: sync, report to
    /// the waiters, repeat while there are waiters.
    async fn group_commit_loop(&self, key: ObjectKey, generation: i64) {
        loop {
            let (target, target_crc, durable, waiters) = {
                let mut state = self.state.borrow_mut();
                let Some(object) = state
                    .objects
                    .get_mut(&key)
                    .filter(|object| object.meta.generation == generation)
                else {
                    // Deleted or replaced; its sessions were fenced.
                    return;
                };
                if object.flush.waiters.is_empty() {
                    object.flush.running = false;
                    return;
                }
                (
                    object.accepted,
                    object.accepted_crc,
                    object.durable,
                    std::mem::take(&mut object.flush.waiters),
                )
            };
            if target > durable {
                if let Err(io) = self.backend.sync(&key, generation).await {
                    let error = internal("sync", io);
                    let mut state = self.state.borrow_mut();
                    for session_id in waiters {
                        state.fail_session(session_id, &error);
                    }
                    continue;
                }
                self.advance_durable(&key, generation, target, target_crc);
            }
            let state = self.state.borrow();
            let Some(object) = state
                .objects
                .get(&key)
                .filter(|object| object.meta.generation == generation)
            else {
                return;
            };
            let mut notified = Vec::with_capacity(waiters.len());
            for session_id in waiters {
                if notified.contains(&session_id) {
                    continue;
                }
                notified.push(session_id);
                if let Some(session) = state.sessions.get(&session_id) {
                    if session.dead.is_none() {
                        session.emit(Event::Durable {
                            session_id,
                            persisted_size: object.durable as i64,
                        });
                    }
                }
            }
        }
    }

    /// Bump the writer epoch of an unfinalized object durably, after making
    /// its accepted bytes durable. The caller holds the object lock.
    async fn bump_epoch(&self, key: &ObjectKey) -> Result<u64, WireError> {
        self.sync_object(key).await?;
        let meta = {
            let state = self.state.borrow();
            let mut meta = state.object(key)?.meta.clone();
            meta.writer_epoch += 1;
            meta
        };
        self.backend
            .commit_meta(key, &meta)
            .await
            .map_err(|io| internal("commit metadata", io))?;
        let epoch = meta.writer_epoch;
        self.state.borrow_mut().object_mut(key)?.meta = meta;
        Ok(epoch)
    }

    async fn takeover(&self, conn: &ConnCtx, key: &ObjectKey, if_match: ObjectVersion) -> Reply {
        let _lock = self.lock(key).await;
        {
            let state = self.state.borrow();
            let object = state.object(key)?;
            if !object.matches(&if_match) {
                return Err(precondition("object version does not match"));
            }
            if object.meta.finalized {
                return Err(precondition("object is finalized"));
            }
        }
        let epoch = self.bump_epoch(key).await?;
        let mut state = self.state.borrow_mut();
        state.fence_sessions(key, None, &precondition("append session taken over"));
        let session_id = state.open_session(conn, key, if_match.generation, epoch);
        Ok(Response::SessionOpened(state.opened(session_id, key)))
    }

    async fn resume(&self, conn: &ConnCtx, key: &ObjectKey, write_handle: &[u8]) -> Reply {
        let handle = decode_handle(write_handle)?;
        let _lock = self.lock(key).await;
        {
            let state = self.state.borrow();
            let object = state.object(key)?;
            if object.meta.generation != handle.generation {
                return Err(precondition("object generation changed"));
            }
            if object.meta.finalized {
                return Err(precondition("object is finalized"));
            }
            if object.meta.writer_epoch != handle.writer_epoch {
                return Err(precondition("append session was taken over"));
            }
        }
        // The answer's persisted_size must equal the accepted size: a client
        // resends from it, possibly with different message boundaries.
        self.sync_object(key).await?;
        let mut state = self.state.borrow_mut();
        state.fence_sessions(key, None, &precondition("append session resumed elsewhere"));
        let session_id = state.open_session(conn, key, handle.generation, handle.writer_epoch);
        Ok(Response::SessionOpened(state.opened(session_id, key)))
    }

    async fn replace_appendable(
        &self,
        conn: &ConnCtx,
        key: &ObjectKey,
        if_match: Option<ObjectVersion>,
        data: Vec<u8>,
        metadata: HashMap<String, String>,
    ) -> Reply {
        check_metadata(&metadata)?;
        let _lock = self.lock(key).await;
        let meta = {
            let mut state = self.state.borrow_mut();
            let current = state.objects.get(key);
            let epoch = match (if_match, current) {
                (Some(version), Some(object)) if object.matches(&version) => {
                    object.meta.writer_epoch + 1
                }
                (None, None) => 1,
                (Some(_), _) => return Err(precondition("object version does not match")),
                (None, Some(_)) => return Err(precondition("object already exists")),
            };
            ObjectMeta {
                generation: state.next_generation(),
                metageneration: 1,
                metadata,
                finalized: false,
                finalized_size: 0,
                finalized_crc32c: 0,
                writer_epoch: epoch,
                last_modified_unix_nanos: now_nanos(),
            }
        };
        let len = data.len() as u64;
        let crc = crc32c::crc32c(&data);
        self.backend
            .create(key, &meta, data)
            .await
            .map_err(|io| internal("replace", io))?;
        let mut state = self.state.borrow_mut();
        state.fence_sessions(key, None, &precondition("object replaced"));
        let (generation, epoch) = (meta.generation, meta.writer_epoch);
        state
            .objects
            .insert(key.clone(), Object::new(meta, len, crc));
        let session_id = state.open_session(conn, key, generation, epoch);
        Ok(Response::SessionOpened(state.opened(session_id, key)))
    }

    /// Look up a live session and take its object's lock, revalidating the
    /// session once the lock is held.
    async fn lock_session(
        &self,
        conn: &ConnCtx,
        session_id: SessionId,
    ) -> Result<(ObjectKey, i64, ObjectLock<'_, B>), WireError> {
        let key = self
            .state
            .borrow()
            .live_session(conn, session_id)?
            .key
            .clone();
        let lock = self.lock(&key).await;
        let generation = self
            .state
            .borrow()
            .live_session(conn, session_id)?
            .generation;
        Ok((key, generation, lock))
    }

    /// Kill the session with `error` (sending `SessionFailed`) and return it.
    fn fail(&self, session_id: SessionId, error: WireError) -> WireError {
        self.state.borrow_mut().fail_session(session_id, &error);
        error
    }

    /// Sync the session's object and report the durable tail to it.
    async fn flush_session(&self, key: &ObjectKey, session_id: SessionId) -> Result<(), WireError> {
        let durable = self.sync_object(key).await?;
        let state = self.state.borrow();
        if let Some(session) = state.sessions.get(&session_id) {
            if session.dead.is_none() {
                session.emit(Event::Durable {
                    session_id,
                    persisted_size: durable as i64,
                });
            }
        }
        Ok(())
    }

    async fn append(
        &self,
        conn: &ConnCtx,
        session_id: SessionId,
        offset: i64,
        data: Vec<u8>,
        crc: u32,
        flush: bool,
    ) -> Reply {
        let (key, generation, _lock) = self.lock_session(conn, session_id).await?;
        let (accepted, accepted_crc) = {
            let state = self.state.borrow();
            let object = state.object(&key)?;
            if object.meta.generation != generation || object.meta.finalized {
                drop(state);
                return Err(self.fail(session_id, precondition("object is finalized")));
            }
            (object.accepted, object.accepted_crc)
        };
        if crc32c::crc32c(&data) != crc {
            return Err(self.fail(
                session_id,
                error(WireCode::DataLoss, "append payload checksum mismatch"),
            ));
        }
        if offset < 0 || offset as u64 > accepted {
            return Err(self.fail(
                session_id,
                error(
                    WireCode::OutOfRange,
                    format!("append offset {offset} beyond the object size {accepted}"),
                ),
            ));
        }
        let offset = offset as u64;
        let len = data.len() as u64;
        if offset < accepted {
            // A resend overlapping accepted bytes is an idempotent no-op only
            // if it lies entirely inside them and matches them exactly.
            if offset + len > accepted {
                return Err(self.fail(
                    session_id,
                    precondition("append overlaps the object tail without matching it"),
                ));
            }
            let stored = self
                .backend
                .read(&key, generation, offset, data.len())
                .await
                .map_err(|io| self.fail(session_id, internal("read", io)))?;
            if stored != data {
                return Err(self.fail(
                    session_id,
                    precondition("append differs from the bytes already accepted"),
                ));
            }
        } else if !data.is_empty() {
            let new_crc = crc32c::crc32c_append(accepted_crc, &data);
            self.backend
                .write(&key, generation, offset, data)
                .await
                .map_err(|io| self.fail(session_id, internal("write", io)))?;
            let mut state = self.state.borrow_mut();
            let object = state.object_mut(&key)?;
            object.accepted = offset + len;
            object.accepted_crc = new_crc;
        }
        if flush {
            self.request_flush(&key, session_id)
                .await
                .map_err(|error| self.fail(session_id, error))?;
        }
        let size = self.state.borrow().object(&key)?.accepted as i64;
        Ok(Response::Accepted { size })
    }

    async fn flush(&self, conn: &ConnCtx, session_id: SessionId, offset: i64) -> Reply {
        let (key, _generation, _lock) = self.lock_session(conn, session_id).await?;
        let accepted = self.state.borrow().object(&key)?.accepted;
        if offset < 0 || offset as u64 > accepted {
            return Err(self.fail(
                session_id,
                error(
                    WireCode::OutOfRange,
                    format!("flush offset {offset} beyond the object size {accepted}"),
                ),
            ));
        }
        self.request_flush(&key, session_id)
            .await
            .map_err(|error| self.fail(session_id, error))?;
        Ok(Response::Accepted {
            size: accepted as i64,
        })
    }

    async fn finalize(
        &self,
        conn: &ConnCtx,
        key: &ObjectKey,
        generation: i64,
        write_offset: i64,
        write_handle: Option<Vec<u8>>,
    ) -> Reply {
        let _lock = self.lock(key).await;
        let handle = {
            let state = self.state.borrow();
            let object = state.object(key)?;
            if object.meta.generation != generation {
                return Err(precondition("object generation does not match"));
            }
            if object.meta.finalized {
                // Idempotent retry: no handle or epoch check.
                if object.meta.finalized_size as i64 == write_offset {
                    return Ok(Response::Object(object.info(&key.name)));
                }
                return Err(precondition("object is finalized at a different length"));
            }
            match write_handle {
                Some(bytes) => {
                    let handle = decode_handle(&bytes)?;
                    if handle.generation != generation
                        || handle.writer_epoch != object.meta.writer_epoch
                    {
                        return Err(precondition("append session was taken over"));
                    }
                    Some(handle)
                }
                None => None,
            }
        };
        let durable = self.sync_object(key).await?;
        if write_offset < 0 || write_offset as u64 != durable {
            let code = if write_offset > durable as i64 {
                WireCode::OutOfRange
            } else {
                WireCode::FailedPrecondition
            };
            return Err(error(
                code,
                format!("finalize at {write_offset} but the durable tail is {durable}"),
            ));
        }
        let meta = {
            let state = self.state.borrow();
            let object = state.object(key)?;
            let mut meta = object.meta.clone();
            meta.metageneration += 1;
            meta.finalized = true;
            meta.finalized_size = durable;
            meta.finalized_crc32c = object.durable_crc;
            meta.last_modified_unix_nanos = now_nanos();
            if handle.is_none() {
                // A handle-free finalize is an implicit takeover.
                meta.writer_epoch += 1;
            }
            meta
        };
        self.backend
            .commit_meta(key, &meta)
            .await
            .map_err(|io| internal("commit metadata", io))?;
        let mut state = self.state.borrow_mut();
        state.object_mut(key)?.meta = meta;
        // The session that finalized (same writer, this connection) closes
        // silently; every other session on the object is told.
        let finisher = handle.and_then(|handle| {
            state
                .sessions
                .iter()
                .find(|(_, s)| {
                    &s.key == key
                        && s.conn == conn.id
                        && s.epoch == handle.writer_epoch
                        && s.dead.is_none()
                })
                .map(|(&id, _)| id)
        });
        state.fence_sessions(key, finisher, &precondition("object finalized"));
        Ok(Response::Object(state.object(key)?.info(&key.name)))
    }

    async fn delete(&self, key: &ObjectKey, generation: i64) -> Reply {
        let _lock = self.lock(key).await;
        {
            let state = self.state.borrow();
            if state.object(key)?.meta.generation != generation {
                return Err(precondition("object generation does not match"));
            }
        }
        self.backend
            .delete(key, generation)
            .await
            .map_err(|io| internal("delete", io))?;
        let mut state = self.state.borrow_mut();
        state.objects.remove(key);
        state.fence_sessions(key, None, &precondition("object deleted"));
        Ok(Response::Deleted)
    }

    fn close_session(&self, conn: &ConnCtx, session_id: SessionId) {
        let mut state = self.state.borrow_mut();
        if state
            .sessions
            .get(&session_id)
            .is_some_and(|session| session.conn == conn.id)
        {
            state.sessions.remove(&session_id);
        }
    }
}
