//! Protocol messages. Every type here is an rkyv archive root or part of one;
//! see `PROTOCOL.md` for the semantics of each request.

use std::collections::HashMap;

use rkyv::{Archive, Deserialize, Serialize};

/// Version carried by [`Request::Hello`] and [`Response::HelloOk`]. A server
/// rejects a `Hello` naming any other version with [`WireCode::Unimplemented`].
pub const PROTOCOL_VERSION: u32 = 1;

/// Server-assigned identifier of one append session. Unique for the lifetime
/// of a server process and valid only on the connection that opened it.
pub type SessionId = u64;

/// One client-to-server frame body.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ClientFrame {
    /// Client-chosen id echoed by the matching [`ServerFrame::Response`].
    /// Must be unique among the connection's in-flight requests.
    pub request_id: u64,
    /// The request.
    pub body: Request,
}

/// One server-to-client frame body.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ServerFrame {
    /// The single answer to the client frame with the same `request_id`.
    Response {
        /// Id copied from the [`ClientFrame`].
        request_id: u64,
        /// Success payload or failure.
        body: Result<Response, WireError>,
    },
    /// Unsolicited push about a session opened on this connection.
    Event(Event),
}

/// Exact `(generation, metageneration)` an operation is conditioned on.
#[derive(Archive, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectVersion {
    /// Required current generation.
    pub generation: i64,
    /// Required current metageneration.
    pub metageneration: i64,
}

/// Client requests. Each maps onto one `Replica`/`ReplicaFactory` operation.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Request {
    /// Handshake; must be the first request on a connection.
    Hello {
        /// Must equal [`PROTOCOL_VERSION`].
        protocol_version: u32,
        /// Free-form client description for server logs.
        client_name: String,
    },
    /// Strongly consistent listing of `bucket` objects whose name starts with
    /// `prefix`. Answered with [`Response::Listed`].
    List {
        /// Bucket namespace.
        bucket: String,
        /// Name prefix; empty lists the whole bucket.
        prefix: String,
    },
    /// Metadata only, content-blind. Answered with [`Response::Object`].
    Stat {
        /// Bucket namespace.
        bucket: String,
        /// Object name.
        object: String,
    },
    /// Read durable bytes `[offset, persisted_size)` plus the metadata of the
    /// same generation, atomically. Answered with [`Response::ReadData`].
    Read {
        /// Bucket namespace.
        bucket: String,
        /// Object name.
        object: String,
        /// First byte to return; `0` reads a full snapshot.
        offset: i64,
    },
    /// Conditionally create a new, empty, unfinalized object (fails with
    /// [`WireCode::AlreadyExists`] if any generation exists) and open an
    /// append session on it. Answered with [`Response::SessionOpened`].
    CreateAppendable {
        /// Bucket namespace.
        bucket: String,
        /// Object name.
        object: String,
        /// Custom metadata of the new generation.
        metadata: HashMap<String, String>,
    },
    /// Handle-free open of an append session on the current generation,
    /// guarded by the observed version. Bumps the writer epoch, revoking every
    /// earlier session. Answered with [`Response::SessionOpened`].
    Takeover {
        /// Bucket namespace.
        bucket: String,
        /// Object name.
        object: String,
        /// Required current generation and metageneration.
        if_match: ObjectVersion,
    },
    /// Reattach to the writer identified by `write_handle` without bumping
    /// the epoch. Answered with [`Response::SessionOpened`].
    Resume {
        /// Bucket namespace.
        bucket: String,
        /// Object name.
        object: String,
        /// Handle from an earlier [`SessionOpened`].
        write_handle: Vec<u8>,
    },
    /// Write a new unfinalized generation holding exactly `data` and open an
    /// append session on it. With `if_match`, replaces only that exact
    /// version; without, creates only when no generation exists. A lost race
    /// fails with [`WireCode::FailedPrecondition`]. Answered with
    /// [`Response::SessionOpened`] once `data` is durable.
    ReplaceAppendable {
        /// Bucket namespace.
        bucket: String,
        /// Object name.
        object: String,
        /// Version to replace, or `None` for create-if-absent.
        if_match: Option<ObjectVersion>,
        /// Complete content of the new generation.
        data: Vec<u8>,
        /// Custom metadata of the new generation.
        metadata: HashMap<String, String>,
    },
    /// Append `data` at exactly `offset` on a live session. Answered with
    /// [`Response::Accepted`] once ordered into the object (not durable);
    /// durability is reported through [`Event::Durable`].
    ///
    /// The payload is the only large field in the protocol; a server that
    /// wants to avoid the copy can validate the frame with `rkyv::access` and
    /// write `ArchivedRequest::Append.data` straight from the receive buffer.
    Append {
        /// Session opened on this connection.
        session_id: SessionId,
        /// Must equal the session's current object size (see `PROTOCOL.md`
        /// for the idempotent-retry rule).
        offset: i64,
        /// Bytes to append.
        data: Vec<u8>,
        /// CRC32C of `data`; a mismatch fails with [`WireCode::DataLoss`].
        crc32c: u32,
        /// Make everything through `offset + data.len()` durable.
        flush: bool,
    },
    /// Make every byte through `offset` durable. Answered with
    /// [`Response::Accepted`]; durability follows as [`Event::Durable`].
    Flush {
        /// Session opened on this connection.
        session_id: SessionId,
        /// Expected current size of the object.
        offset: i64,
    },
    /// Finalize `generation` at exactly `write_offset` bytes. Idempotent when
    /// that generation is already finalized at that length. Answered with
    /// [`Response::Object`] once durable.
    Finalize {
        /// Bucket namespace.
        bucket: String,
        /// Object name.
        object: String,
        /// Generation to finalize.
        generation: i64,
        /// Exact final length.
        write_offset: i64,
        /// Writer handle; `None` finalizes as an implicit takeover.
        write_handle: Option<Vec<u8>>,
    },
    /// Delete exactly `generation`. Answered with [`Response::Deleted`].
    Delete {
        /// Bucket namespace.
        bucket: String,
        /// Object name.
        object: String,
        /// Generation that must be current.
        generation: i64,
    },
    /// Detach a session from this connection without changing the object or
    /// its epoch. Answered with [`Response::SessionClosed`]; idempotent.
    CloseSession {
        /// Session to close.
        session_id: SessionId,
    },
}

/// Successful responses.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Response {
    /// Answer to [`Request::Hello`].
    HelloOk {
        /// Stable identity of the storage node.
        node_id: String,
        /// Version the server speaks (equals the client's on success).
        protocol_version: u32,
    },
    /// Answer to [`Request::List`], sorted by name.
    Listed(Vec<ObjectInfo>),
    /// Answer to [`Request::Stat`] and [`Request::Finalize`].
    Object(ObjectInfo),
    /// Answer to [`Request::Read`].
    ReadData {
        /// Metadata of the generation the bytes were read from.
        info: ObjectInfo,
        /// Durable bytes from the requested offset to `info.persisted_size`.
        bytes: Vec<u8>,
    },
    /// Answer to [`Request::CreateAppendable`], [`Request::Takeover`],
    /// [`Request::Resume`] and [`Request::ReplaceAppendable`].
    SessionOpened(SessionOpened),
    /// Answer to [`Request::Append`] and [`Request::Flush`]: the request was
    /// applied in order. Not a durability acknowledgment.
    Accepted {
        /// Object size (accepted, not necessarily durable) after the request.
        size: i64,
    },
    /// Answer to [`Request::Delete`].
    Deleted,
    /// Answer to [`Request::CloseSession`].
    SessionClosed,
}

/// A newly opened (or reattached) append session.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SessionOpened {
    /// Id to use in [`Request::Append`] / [`Request::Flush`].
    pub session_id: SessionId,
    /// Generation the session is bound to.
    pub generation: i64,
    /// Metageneration at open time.
    pub metageneration: i64,
    /// Authoritative durable tail; the next append offset.
    pub persisted_size: i64,
    /// Opaque handle for [`Request::Resume`] and [`Request::Finalize`]; see
    /// [`crate::WriteHandle`].
    pub write_handle: Vec<u8>,
}

/// Object metadata, as listed or stat'ed.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ObjectInfo {
    /// Object name within the bucket.
    pub name: String,
    /// Generation; unique and increasing per node.
    pub generation: i64,
    /// Metageneration; starts at 1, bumped by finalize.
    pub metageneration: i64,
    /// Provider-visible length: the full length when finalized, `0` while
    /// unfinalized (appends are hidden from metadata, as in GCS).
    pub size: i64,
    /// Durable length (the visible tail of an unfinalized object).
    pub persisted_size: i64,
    /// Whether the object is finalized.
    pub finalized: bool,
    /// CRC32C of the first `persisted_size` bytes.
    pub crc32c: Option<u32>,
    /// Last data or metadata change, in nanoseconds since the Unix epoch.
    pub last_modified_unix_nanos: Option<i64>,
    /// Custom metadata.
    pub metadata: HashMap<String, String>,
}

/// Unsolicited server pushes about sessions on this connection.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The session's durable tail advanced to `persisted_size` (monotonic).
    Durable {
        /// Session whose tail advanced.
        session_id: SessionId,
        /// New durable byte length of the object.
        persisted_size: i64,
    },
    /// The session is dead (fenced, finalized, or failed); later requests on
    /// it fail with the same error. Sent at most once per session.
    SessionFailed {
        /// Session that failed.
        session_id: SessionId,
        /// Why.
        error: WireError,
    },
}

/// A failed request or session.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{code:?}: {message}")]
pub struct WireError {
    /// Stable classification.
    pub code: WireCode,
    /// Diagnostic text; correctness logic must not match on it.
    pub message: String,
}

impl WireError {
    /// Build an error.
    pub fn new(code: WireCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Error classification. Mirrors `chorus_client::TransportCode` one to one;
/// the client maps between the two (this crate does not depend on it).
#[derive(Archive, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WireCode {
    /// Object (or generation) not found.
    NotFound,
    /// Conditional create found an existing object.
    AlreadyExists,
    /// Malformed or out-of-protocol request.
    InvalidArgument,
    /// Precondition failed; on a session this is a terminal writer fence.
    FailedPrecondition,
    /// Operation aborted; also fences a writer.
    Aborted,
    /// Offset outside the accepted range.
    OutOfRange,
    /// Throttled; retry with backoff.
    ResourceExhausted,
    /// Unsupported operation or protocol version.
    Unimplemented,
    /// Stored bytes or a supplied checksum are corrupt.
    DataLoss,
    /// The server cannot distinguish absence from a routing failure.
    Ambiguous,
    /// No valid credential.
    Unauthenticated,
    /// Credential lacks permission.
    PermissionDenied,
    /// Temporary outage (e.g. shutting down).
    Unavailable,
    /// Deadline exceeded.
    DeadlineExceeded,
    /// Unclassified server failure (e.g. disk I/O error).
    Internal,
}

impl WireCode {
    /// Every code, in declaration order.
    pub const ALL: [WireCode; 15] = [
        WireCode::NotFound,
        WireCode::AlreadyExists,
        WireCode::InvalidArgument,
        WireCode::FailedPrecondition,
        WireCode::Aborted,
        WireCode::OutOfRange,
        WireCode::ResourceExhausted,
        WireCode::Unimplemented,
        WireCode::DataLoss,
        WireCode::Ambiguous,
        WireCode::Unauthenticated,
        WireCode::PermissionDenied,
        WireCode::Unavailable,
        WireCode::DeadlineExceeded,
        WireCode::Internal,
    ];
}
