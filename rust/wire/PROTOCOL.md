# Chorus storage node wire protocol (version 1)

This document is normative. The Rust types in `src/messages.rs` are the
schema; this file defines what they mean. A storage node gives one node's
worth of GCS Rapid (zonal bucket) semantics, matching what the
`chorus_client::Replica` / `ReplicaFactory` traits require. The reference
behavior is `rust/fake-gcs` as seen through `rust/client/src/grpc.rs`.

## 1. Transport and framing

One TCP connection carries a stream of frames in each direction:

```
u32 LE  body_len      length of the body, at most MAX_FRAME_LEN (64 MiB)
u32 LE  crc32c(body)  CRC32C (Castagnoli) of the body bytes
body    rkyv 0.8 archive of ClientFrame (client->server) or ServerFrame (server->client)
```

- Decoders copy the body into an aligned buffer and validate it with
  bytecheck before use. Bytes from the network are never trusted unchecked.
- Any framing error is fatal: the receiver closes the connection. That covers
  a `body_len` over the limit, a CRC mismatch, or a body that fails archive
  validation. A server may first send a best-effort error response, but it is
  not required to.
- rkyv archives are not self-delimiting. The CRC is the integrity check;
  validation only guarantees memory safety.

`chorus-wire` provides `encode_client_frame` / `encode_server_frame`, which
return a whole frame with its header, and `decode_client_frame` /
`decode_server_frame`. A decode call returns `Ok(None)` and consumes nothing
until a whole frame is buffered.

## 2. Frames and multiplexing

```
ClientFrame { request_id: u64, body: Request }
ServerFrame::Response { request_id: u64, body: Result<Response, WireError> }
ServerFrame::Event(Event)
```

- The connection is multiplexed. A client may have any number of requests in
  flight, and each gets exactly one `Response` with the same `request_id`.
  The client chooses `request_id`, which must be unique among its in-flight
  requests (a counter works).
- Requests that name the same session are applied in the order they arrive.
  Requests on different sessions or objects may complete in any order.
  Responses may come back out of order.
- `Event`s are unsolicited pushes about sessions opened on this connection.
  They can arrive between any two responses.

## 3. Handshake

The first request on a connection must be
`Hello { protocol_version, client_name }`.

- If `protocol_version == PROTOCOL_VERSION`, the server answers
  `HelloOk { node_id, protocol_version }`. `node_id` is stable across
  restarts of the node.
- Otherwise the server answers `Unimplemented` and closes the connection.
- Any other request sent before a successful `Hello` fails with
  `InvalidArgument`.
- A repeated `Hello` fails with `InvalidArgument`.

## 4. Object model

The namespace is `bucket` (a string, one directory per bucket on disk) plus an
`object` name. For each name the node stores at most one live generation:

| field | meaning |
|---|---|
| `generation: i64` | Unique and strictly increasing across the whole node, including across restarts. A new value comes from create and from replace. |
| `metageneration: i64` | Starts at 1 and increases by 1 on finalize. |
| `metadata` | Custom `string -> string` map. The total size of keys plus values is at most 8 KiB (`InvalidArgument` otherwise). |
| `finalized: bool` | Whether the object is finalized. A finalized object is immutable. |
| size / `persisted_size` | Bytes accepted / bytes durable (fdatasync'd). |
| `crc32c` | CRC32C of the durable bytes, maintained incrementally. |
| `last_modified` | Unix nanoseconds of the last data or metadata change. |
| `writer_epoch: u64` | Writer-exclusivity counter. It is stored durably with the object and never decreases. |

`ObjectInfo` reports the following. `size` is the full length once finalized
and `0` while unfinalized: as in GCS, metadata hides appended bytes from an
open object. `persisted_size` is the durable length. `crc32c` covers the first
`persisted_size` bytes. A client must take an open object's tail from
`persisted_size` (or from `SessionOpened` / `Durable`), never from `size`.

## 5. Sessions, epochs and write handles

An **append session** is the server-side state of one writer incarnation on
one object generation. Opening a session returns:

```
SessionOpened { session_id, generation, metageneration, persisted_size, write_handle }
```

- `session_id` is unique for the lifetime of the server process. It is
  **scoped to its connection**: only requests on the connection that opened
  it may use it. On any other connection it is unknown.
- A dropped connection closes all of its sessions. The object, its bytes, and
  its `writer_epoch` survive. Bytes accepted but not yet durable may be lost;
  `persisted_size` is the only promise. The client reconnects and sends
  `Resume` with the write handle.
- `write_handle` is an opaque byte string. Clients must not interpret it. The
  server encodes `{generation, writer_epoch}`. Version 1 uses `WriteHandle`:
  `u8 1 | i64 LE generation | u64 LE writer_epoch`, 17 bytes. Handle bytes
  that do not decode are rejected with `InvalidArgument`.
- **Handle-free opens bump the epoch.** These are `CreateAppendable`,
  `Takeover`, `ReplaceAppendable` and `Finalize` without a handle. Each one
  increments `writer_epoch`, and every live session holding an older epoch
  for that object, on any connection, is **fenced**:
  - Its connection receives `Event::SessionFailed { session_id, error: FailedPrecondition }`.
  - Later `Append` and `Flush` requests on it fail with `FailedPrecondition`.
- **Resume reattaches.** `Resume` with a handle whose generation and epoch
  match the object's current ones opens a new `session_id` for the same epoch
  without bumping it. Any other session already attached at that epoch is
  closed and receives `SessionFailed(FailedPrecondition)`, so at most one
  session per object can append at a time. This is the single-writer rule.
- Once a session is dead it stays dead. "Dead" means fenced, finalized,
  failed by an append error, or closed. The server sends at most one
  `SessionFailed` per session. Every later request that names the session
  fails with the same code (`FailedPrecondition` if it was closed or is
  unknown).

## 6. Requests

The error codes below are the ones a conforming server must use in each
situation. Any request may also fail with `Internal`, for example on a disk
I/O error, or with `Unavailable`, for example while the node shuts down.

### `List { bucket, prefix }` -> `Listed(Vec<ObjectInfo>)`
Lists every live object whose name starts with `prefix`, sorted by name. The
listing is strongly consistent: it is served from the authoritative in-memory
table and reflects every mutation already acknowledged. A missing bucket is
an empty list.

### `Stat { bucket, object }` -> `Object(ObjectInfo)`
Returns metadata only. Stat is content-blind: it succeeds even when the
stored bytes are corrupt. `NotFound` if the object is absent.

### `Read { bucket, object, offset }` -> `ReadData { info, bytes }`
Atomically reads `info` and the durable bytes `[offset, info.persisted_size)`
from the same generation. Offset `0` is a full snapshot.
- `NotFound` if the object is absent.
- `InvalidArgument` if `offset < 0`.
- `OutOfRange` if `offset > persisted_size`. An offset equal to
  `persisted_size` returns empty bytes.
- `DataLoss` if the stored bytes do not match the stored CRC32C.

### `CreateAppendable { bucket, object, metadata }` -> `SessionOpened`
Conditionally creates a new, empty, unfinalized generation and opens a
session on it, with the epoch bumped. The answer has `persisted_size = 0` and
is sent only after the creation is durable.
- `AlreadyExists` if any generation of the name exists, finalized or not.
- `InvalidArgument` if the metadata is too large.

### `Takeover { bucket, object, if_match: {generation, metageneration} }` -> `SessionOpened`
Handle-free open on the current generation. It is guarded by the observed
version, bumps the epoch, and fences all earlier sessions. The answer's
`persisted_size` is the authoritative durable tail.
- `NotFound` if the object is absent.
- `FailedPrecondition` if the generation or metageneration differs.
- `FailedPrecondition` if the object is finalized.

### `Resume { bucket, object, write_handle }` -> `SessionOpened`
Reattaches to the writer named by the handle without bumping the epoch (see
§5). The answer's `persisted_size` is the durable tail.
- `InvalidArgument` if the handle is malformed.
- `NotFound` if the object is absent.
- `FailedPrecondition` if the generation changed, the epoch moved on (a
  takeover happened), or the object is finalized.

### `ReplaceAppendable { bucket, object, if_match: Option<{generation, metageneration}>, data, metadata }` -> `SessionOpened`
Writes a new **unfinalized** generation that holds exactly `data` and
`metadata`, and opens a session on it with the epoch bumped.
- With `Some(v)`, the write applies only if the current object is exactly `v`.
- With `None`, the write applies only if no generation exists.
- The answer is sent once the data and metadata are durable,
  `persisted_size == data.len()`.
- The old generation's bytes are discarded, and its sessions are fenced with
  `FailedPrecondition`.
- A lost race fails with `FailedPrecondition` in **both** modes. This is not
  `AlreadyExists`; it matches the `Replica::replace_appendable` contract.

### `Append { session_id, offset, data, crc32c, flush }` -> `Accepted { size }`
Appends to a live session. The checks run in this order:
1. Unknown, closed or dead session: `FailedPrecondition`. A dead session
   instead fails with its recorded code.
2. Object finalized: `FailedPrecondition`.
3. `crc32c(data) != crc32c`: `DataLoss`.
4. `offset < 0`, or `offset > size`, where `size` is the accepted size:
   `OutOfRange`.
5. `offset < size` (a retry overlap): accepted as an idempotent no-op only if
   `offset + data.len() <= size` and the stored bytes in that range equal
   `data`. Otherwise `FailedPrecondition`.
6. `offset == size`: the bytes are appended.

The `Accepted` answer means the request has been ordered into the object. It
says nothing about durability. If `flush` is set, the server makes everything
through `offset + data.len()` durable and then sends `Event::Durable`.

An `Append` that fails (steps 2 to 5) also kills the session: the server sends
`SessionFailed` with the same error. A pipelining client therefore sees the
failure on the event channel it already waits on. Clients do not wait for
`Accepted` on the hot path. They wait only for `Durable` / `SessionFailed`.

The `data` payload is the only large field. A server may skip the
deserialize copy by validating with `rkyv::access` and writing the archived
slice directly.

### `Flush { session_id, offset }` -> `Accepted { size }`
Makes every accepted byte through `offset` durable, then sends
`Event::Durable` (also when nothing new had to be synced, so a waiter always
wakes). Session errors are the same as for `Append`. `offset > size` is
`OutOfRange`.

### `Finalize { bucket, object, generation, write_offset, write_handle }` -> `Object(ObjectInfo)`
Finalizes `generation` at exactly `write_offset` bytes. The answer is sent
once durable: data fdatasync'd first, then the metadata commit. Finalize
increments `metageneration`, sets `finalized`, sets `size = persisted_size =
write_offset`, and closes every session on the object. Each of those sessions
receives `SessionFailed(FailedPrecondition)`, except the one whose handle
performed the finalize, which is closed silently.
- `NotFound` if the object is absent.
- `FailedPrecondition` if the generation does not match.
- **Idempotent retry**: if the object is already finalized at this generation
  with size `write_offset`, the answer is `Ok` with the current info. No
  handle or epoch check is made in this case.
- `FailedPrecondition` if the object is already finalized at a different
  length.
- With `Some(handle)`: the handle must decode (`InvalidArgument`) and match
  the current generation and epoch (`FailedPrecondition`).
- With `None`: the finalize acts as an implicit takeover and bumps the epoch.
- Before finalizing, the server makes accepted bytes durable. It then requires
  `write_offset == persisted_size`; otherwise `OutOfRange` if
  `write_offset > persisted_size`, else `FailedPrecondition`.

### `Delete { bucket, object, generation }` -> `Deleted`
Deletes exactly that generation, finalized or not. All of the object's
sessions are fenced with `FailedPrecondition`.
- `NotFound` if the object is absent. Callers treat this as idempotent
  success.
- `FailedPrecondition` if the generation differs.
- The answer is sent after the delete is durable.

### `CloseSession { session_id }` -> `SessionClosed`
Detaches the session. It does not change the object or the epoch. The call is
idempotent, and an unknown session also returns `SessionClosed`. The write
handle still allows a later `Resume`.

## 7. Events

| event | meaning |
|---|---|
| `Durable { session_id, persisted_size }` | The durable tail of the session's object advanced to `persisted_size`. The value is monotonic per session. It is sent after the fdatasync that covers the bytes. Concurrent flushes may be coalesced into one event that carries the greatest value. |
| `SessionFailed { session_id, error }` | The session is dead (§5). This is the last event for that session. |

Events are delivered only to the connection that owns the session. If that
connection is gone, the event is dropped.

## 8. Error codes

`WireError { code: WireCode, message }`. `message` is diagnostic text only;
logic must not match on it. `WireCode` maps one to one, by variant name, onto
`chorus_client::TransportCode`. The client attaches the replica's zone.

| code | used for |
|---|---|
| `NotFound` | The object or generation is absent. |
| `AlreadyExists` | `CreateAppendable` found an existing object. |
| `InvalidArgument` | A malformed request: a bad handle, a negative read offset, oversized metadata, a request before `Hello`. |
| `FailedPrecondition` | A generation or metageneration mismatch, a fenced or dead or unknown session, a finalized object, a non-idempotent overlap, a lost replace race. This is a terminal writer fence on the append path. |
| `Aborted` | Reserved. Not produced by version 1. It also fences a writer. |
| `OutOfRange` | An append or flush offset beyond the size, a read offset beyond `persisted_size`, or a finalize beyond the durable tail. |
| `ResourceExhausted` | Throttling. Transient. |
| `Unimplemented` | A protocol version mismatch. |
| `DataLoss` | A payload CRC mismatch, or stored bytes that fail their CRC. |
| `Ambiguous` | Reserved for parity with `TransportCode`. |
| `Unauthenticated`, `PermissionDenied` | Reserved. Version 1 has no auth. |
| `Unavailable` | A node shutting down or overloaded. Transient. |
| `DeadlineExceeded` | Transient. |
| `Internal` | A disk I/O error or unclassified failure. Transient. |

## 9. Durability rules (summary)

- A metadata mutation (create, takeover epoch bump, replace, finalize, delete)
  is acknowledged only after it is durable. The per-object `.meta` file is the
  commit point.
- Append bytes count as durable only after an fdatasync. `persisted_size` in
  every response and event never exceeds what is durable.
- `writer_epoch` and the node's generation counter are durable. A restart
  never reuses a generation and never lowers an epoch. After a restart, every
  session is gone. A pre-crash handle resumes if and only if no takeover
  happened since. The unfinalized durable tail is recovered from the data
  file length.
