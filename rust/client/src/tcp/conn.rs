//! One multiplexed connection to a storage node: a writer task draining an
//! ordered frame queue, and a reader task routing responses to their callers
//! and events to per-session watch channels.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::BytesMut;
use chorus_wire::{
    decode_server_frame, encode_client_frame, ClientFrame, Event, FrameError, Request, Response,
    ServerFrame, SessionId, WireCode, WireError, PROTOCOL_VERSION,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::AbortHandle;

use crate::transport::{TransportCode, TransportError};

/// Bound on connecting plus the handshake.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Per-request deadline for request/response calls (matches the gRPC
/// transport's per-RPC deadline).
pub(super) const RPC_TIMEOUT: Duration = Duration::from_secs(30);
/// Bound on waiting for room in the outbound queue (matches the gRPC
/// transport's per-message session progress timeout).
const SEND_TIMEOUT: Duration = Duration::from_secs(30);
/// Outbound frames queued ahead of the socket before senders wait.
const WRITE_QUEUE_FRAMES: usize = 256;

/// Durable tail and terminal error of one server-side append session, as
/// published by the connection reader.
#[derive(Clone, Debug)]
pub(super) struct SessionProgress {
    pub(super) durable: i64,
    pub(super) error: Option<TransportError>,
}

/// A successful response, plus the progress channel of the session it
/// opened when the response is `SessionOpened`.
pub(super) struct Reply {
    pub(super) response: Response,
    pub(super) session: Option<watch::Receiver<SessionProgress>>,
}

type PendingReply = oneshot::Sender<Result<Reply, TransportError>>;

struct RouterState {
    /// Set once the connection is unusable; the reason for diagnostics.
    closed: Option<String>,
    next_request_id: u64,
    pending: HashMap<u64, PendingReply>,
    sessions: HashMap<SessionId, watch::Sender<SessionProgress>>,
}

/// Request and session bookkeeping shared by the connection's callers and
/// its reader and writer tasks.
struct Router {
    zone: usize,
    state: Mutex<RouterState>,
}

pub(super) fn transport_code(code: WireCode) -> TransportCode {
    match code {
        WireCode::NotFound => TransportCode::NotFound,
        WireCode::AlreadyExists => TransportCode::AlreadyExists,
        WireCode::InvalidArgument => TransportCode::InvalidArgument,
        WireCode::FailedPrecondition => TransportCode::FailedPrecondition,
        WireCode::Aborted => TransportCode::Aborted,
        WireCode::OutOfRange => TransportCode::OutOfRange,
        WireCode::ResourceExhausted => TransportCode::ResourceExhausted,
        WireCode::Unimplemented => TransportCode::Unimplemented,
        WireCode::DataLoss => TransportCode::DataLoss,
        WireCode::Ambiguous => TransportCode::Ambiguous,
        WireCode::Unauthenticated => TransportCode::Unauthenticated,
        WireCode::PermissionDenied => TransportCode::PermissionDenied,
        WireCode::Unavailable => TransportCode::Unavailable,
        WireCode::DeadlineExceeded => TransportCode::DeadlineExceeded,
        WireCode::Internal => TransportCode::Internal,
    }
}

pub(super) fn wire_error(zone: usize, error: WireError) -> TransportError {
    TransportError {
        zone,
        code: transport_code(error.code),
        message: error.message,
    }
}

fn error(zone: usize, code: TransportCode, message: impl Into<String>) -> TransportError {
    TransportError {
        zone,
        code,
        message: message.into(),
    }
}

impl Router {
    fn unavailable(&self, message: impl Into<String>) -> TransportError {
        error(self.zone, TransportCode::Unavailable, message)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RouterState> {
        // No code path panics while holding the lock; recover anyway.
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Allocate a request id, registering `reply` for its response when given.
    fn register(&self, reply: Option<PendingReply>) -> Result<u64, TransportError> {
        let mut state = self.lock();
        if let Some(reason) = &state.closed {
            return Err(self.unavailable(format!("storage node connection lost: {reason}")));
        }
        let request_id = state.next_request_id;
        state.next_request_id += 1;
        if let Some(reply) = reply {
            state.pending.insert(request_id, reply);
        }
        Ok(request_id)
    }

    fn unregister(&self, request_id: u64) {
        self.lock().pending.remove(&request_id);
    }

    fn closed(&self) -> bool {
        self.lock().closed.is_some()
    }

    /// Mark the connection dead: fail every pending call and every live
    /// session with `Unavailable`. Sessions are scoped to their connection, so
    /// none survive it. Idempotent.
    fn fail(&self, reason: &str) {
        let (pending, sessions) = {
            let mut state = self.lock();
            if state.closed.is_some() {
                return;
            }
            state.closed = Some(reason.to_string());
            (
                std::mem::take(&mut state.pending),
                std::mem::take(&mut state.sessions),
            )
        };
        tracing::debug!(
            zone = self.zone,
            reason,
            pending = pending.len(),
            sessions = sessions.len(),
            "storage node connection closed"
        );
        let message = format!("storage node connection lost: {reason}");
        for (_, reply) in pending {
            let _ = reply.send(Err(self.unavailable(message.clone())));
        }
        for (_, session) in sessions {
            session.send_modify(|progress| {
                progress
                    .error
                    .get_or_insert_with(|| self.unavailable(message.clone()));
            });
        }
    }

    /// Forget a session locally. Its watch sender is dropped, so waiters see
    /// the session end; later events for it are ignored.
    fn forget_session(&self, session_id: SessionId, reason: &str) {
        let removed = self.lock().sessions.remove(&session_id);
        if let Some(session) = removed {
            session.send_modify(|progress| {
                progress
                    .error
                    .get_or_insert_with(|| self.unavailable(reason.to_string()));
            });
        }
    }

    /// Route one decoded frame. Returns a session id that nobody is waiting
    /// for (the opening caller went away), which the reader then closes.
    fn dispatch(&self, frame: ServerFrame) -> Option<SessionId> {
        match frame {
            ServerFrame::Response { request_id, body } => {
                let mut state = self.lock();
                let reply = state.pending.remove(&request_id);
                let result = match body {
                    Ok(Response::SessionOpened(opened)) => {
                        // Register the session before any later frame is read:
                        // events for it may follow immediately.
                        let session_id = opened.session_id;
                        let (tx, rx) = watch::channel(SessionProgress {
                            durable: opened.persisted_size,
                            error: None,
                        });
                        state.sessions.insert(session_id, tx);
                        let reply_result = Ok(Reply {
                            response: Response::SessionOpened(opened),
                            session: Some(rx),
                        });
                        drop(state);
                        let delivered = reply.is_some_and(|reply| reply.send(reply_result).is_ok());
                        if !delivered {
                            self.lock().sessions.remove(&session_id);
                            return Some(session_id);
                        }
                        return None;
                    }
                    Ok(response) => Ok(Reply {
                        response,
                        session: None,
                    }),
                    Err(error) => Err(wire_error(self.zone, error)),
                };
                drop(state);
                // Responses nobody waits for (appends and flushes, whose
                // outcome arrives as events, or abandoned calls) are dropped.
                if let Some(reply) = reply {
                    let _ = reply.send(result);
                }
                None
            }
            ServerFrame::Event(Event::Durable {
                session_id,
                persisted_size,
            }) => {
                let state = self.lock();
                if let Some(session) = state.sessions.get(&session_id) {
                    session.send_if_modified(|progress| {
                        if persisted_size > progress.durable {
                            progress.durable = persisted_size;
                            true
                        } else {
                            false
                        }
                    });
                }
                None
            }
            ServerFrame::Event(Event::SessionFailed { session_id, error }) => {
                let removed = self.lock().sessions.remove(&session_id);
                if let Some(session) = removed {
                    let error = wire_error(self.zone, error);
                    session.send_modify(|progress| {
                        progress.error.get_or_insert(error);
                    });
                }
                None
            }
        }
    }
}

/// A handshaken connection to one storage node.
pub(super) struct Connection {
    router: Arc<Router>,
    writer: mpsc::Sender<Vec<u8>>,
    tasks: [AbortHandle; 2],
    node_id: String,
}

impl Drop for Connection {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Connection {
    /// Connect to `addr` and perform the `Hello` handshake.
    pub(super) async fn open(addr: &str, zone: usize) -> Result<Arc<Self>, TransportError> {
        let unavailable = |message: String| error(zone, TransportCode::Unavailable, message);
        let stream = match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(io)) => return Err(unavailable(format!("connect to {addr}: {io}"))),
            Err(_) => return Err(unavailable(format!("connect to {addr} timed out"))),
        };
        let _ = stream.set_nodelay(true);
        let (read, write) = stream.into_split();
        let router = Arc::new(Router {
            zone,
            state: Mutex::new(RouterState {
                closed: None,
                next_request_id: 1,
                pending: HashMap::new(),
                sessions: HashMap::new(),
            }),
        });
        let (writer, queue) = mpsc::channel(WRITE_QUEUE_FRAMES);
        let write_task = tokio::spawn(write_loop(write, queue, Arc::clone(&router)));
        let read_task = tokio::spawn(read_loop(read, Arc::clone(&router), writer.clone()));
        let mut connection = Connection {
            router,
            writer,
            tasks: [write_task.abort_handle(), read_task.abort_handle()],
            node_id: String::new(),
        };
        let hello = Request::Hello {
            protocol_version: PROTOCOL_VERSION,
            client_name: format!("chorus-client/{}", env!("CARGO_PKG_VERSION")),
        };
        let reply = match tokio::time::timeout(CONNECT_TIMEOUT, connection.call(hello)).await {
            Ok(reply) => reply?,
            Err(_) => return Err(unavailable(format!("handshake with {addr} timed out"))),
        };
        match reply.response {
            Response::HelloOk {
                node_id,
                protocol_version,
            } if protocol_version == PROTOCOL_VERSION => {
                tracing::debug!(zone, addr, node_id, "storage node connected");
                connection.node_id = node_id;
                Ok(Arc::new(connection))
            }
            other => Err(error(
                zone,
                TransportCode::Unimplemented,
                format!("unexpected handshake response from {addr}: {other:?}"),
            )),
        }
    }

    pub(super) fn node_id(&self) -> &str {
        &self.node_id
    }

    pub(super) fn is_closed(&self) -> bool {
        self.router.closed()
    }

    fn encode(&self, request_id: u64, body: Request) -> Result<Vec<u8>, TransportError> {
        encode_client_frame(&ClientFrame { request_id, body }).map_err(|frame| {
            let code = match frame {
                // An oversized message fails identically on every retry.
                FrameError::TooLarge { .. } => TransportCode::InvalidArgument,
                _ => TransportCode::Internal,
            };
            error(self.router.zone, code, frame.to_string())
        })
    }

    async fn enqueue(&self, frame: Vec<u8>) -> Result<(), TransportError> {
        match tokio::time::timeout(SEND_TIMEOUT, self.writer.send(frame)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(self
                .router
                .unavailable("storage node connection writer stopped")),
            Err(_) => Err(error(
                self.router.zone,
                TransportCode::DeadlineExceeded,
                "storage node connection made no send progress",
            )),
        }
    }

    /// Send `body` and wait for its response.
    pub(super) async fn call(&self, body: Request) -> Result<Reply, TransportError> {
        let (reply, response) = oneshot::channel();
        let request_id = self.router.register(Some(reply))?;
        let frame = match self.encode(request_id, body) {
            Ok(frame) => frame,
            Err(error) => {
                self.router.unregister(request_id);
                return Err(error);
            }
        };
        if let Err(error) = self.enqueue(frame).await {
            self.router.unregister(request_id);
            return Err(error);
        }
        match tokio::time::timeout(RPC_TIMEOUT, response).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(self.router.unavailable("storage node connection closed")),
            Err(_) => {
                self.router.unregister(request_id);
                Err(error(
                    self.router.zone,
                    TransportCode::DeadlineExceeded,
                    "storage node request timed out",
                ))
            }
        }
    }

    /// Queue `body` without waiting for (or keeping) its response.
    pub(super) async fn send(&self, body: Request) -> Result<(), TransportError> {
        let request_id = self.router.register(None)?;
        let frame = self.encode(request_id, body)?;
        self.enqueue(frame).await
    }

    /// Forget a session the server already closed (for example by finalizing
    /// it), without sending anything.
    pub(super) fn forget_session(&self, session_id: SessionId, reason: &str) {
        self.router.forget_session(session_id, reason);
    }

    /// Detach a session: forget it locally and ask the server to close it,
    /// without waiting. A no-op once the connection is gone.
    pub(super) fn close_session(&self, session_id: SessionId, reason: &str) {
        self.router.forget_session(session_id, reason);
        if let Ok(request_id) = self.router.register(None) {
            if let Ok(frame) = self.encode(request_id, Request::CloseSession { session_id }) {
                // Best effort: a full queue only delays the server-side
                // cleanup until the connection or the next resume closes it.
                let _ = self.writer.try_send(frame);
            }
        }
    }
}

async fn write_loop(
    write: OwnedWriteHalf,
    mut queue: mpsc::Receiver<Vec<u8>>,
    router: Arc<Router>,
) {
    let mut write = BufWriter::new(write);
    while let Some(frame) = queue.recv().await {
        let mut result = write.write_all(&frame).await;
        // Coalesce whatever else is already queued into the same flush.
        while result.is_ok() {
            match queue.try_recv() {
                Ok(frame) => result = write.write_all(&frame).await,
                Err(_) => break,
            }
        }
        if let Err(io) = result.and(write.flush().await) {
            router.fail(&format!("write failed: {io}"));
            return;
        }
    }
    // Every sender is gone: the connection was dropped.
    let _ = write.shutdown().await;
}

async fn read_loop(mut read: OwnedReadHalf, router: Arc<Router>, writer: mpsc::Sender<Vec<u8>>) {
    let mut buf = BytesMut::with_capacity(64 * 1024);
    loop {
        loop {
            match decode_server_frame(&mut buf) {
                Ok(Some(frame)) => {
                    if let Some(orphan) = router.dispatch(frame) {
                        close_orphan(&router, &writer, orphan);
                    }
                }
                Ok(None) => break,
                Err(frame) => {
                    router.fail(&format!("bad frame from server: {frame}"));
                    return;
                }
            }
        }
        match read.read_buf(&mut buf).await {
            Ok(0) => {
                router.fail("closed by the server");
                return;
            }
            Ok(_) => {}
            Err(io) => {
                router.fail(&format!("read failed: {io}"));
                return;
            }
        }
    }
}

/// Close a session whose opening caller gave up before the response arrived.
fn close_orphan(router: &Router, writer: &mpsc::Sender<Vec<u8>>, session_id: SessionId) {
    let Ok(request_id) = router.register(None) else {
        return;
    };
    let frame = ClientFrame {
        request_id,
        body: Request::CloseSession { session_id },
    };
    if let Ok(frame) = encode_client_frame(&frame) {
        let _ = writer.try_send(frame);
    }
}
