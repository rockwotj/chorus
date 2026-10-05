# chorus-storage-server

A self-hosted Chorus storage node. It serves GCS-Rapid-style appendable
objects (generations, writer epochs, append sessions, durable tails,
finalize) over the `chorus-wire` TCP protocol, so a Chorus volume can
replicate across three machines with local SSDs instead of three GCS zonal
buckets. The client side is `chorus_client::TcpReplicaFactory` (feature
`tcp`). The normative protocol is [`../wire/PROTOCOL.md`](../wire/PROTOCOL.md).

Everything runs on one single-threaded [compio](https://docs.rs/compio)
runtime (io_uring on Linux), for both the network and the disk:

- `net`: listener, per-connection reader/writer tasks, framing, handshake.
- `store`: the in-memory coordination layer. It holds the object table,
  generations, epochs, sessions, durable tails and group commit.
- `backend`: the persistence boundary. `DiskBackend` is the real one and
  `MemoryBackend` is for tests and development.

## Running three nodes

```sh
cargo build --release -p chorus-storage-server
# one per machine (or per port, for a local try-out)
chorus-storage-server --listen 0.0.0.0:7070 --data-dir /mnt/ssd/chorus --node-id node-a
chorus-storage-server --listen 0.0.0.0:7070 --data-dir /mnt/ssd/chorus --node-id node-b
chorus-storage-server --listen 0.0.0.0:7070 --data-dir /mnt/ssd/chorus --node-id node-c
```

| flag | env | meaning |
|---|---|---|
| `--listen` | `CHORUS_LISTEN` | address to bind (default `0.0.0.0:7070`) |
| `--data-dir` | `CHORUS_DATA_DIR` | data directory; required unless `--in-memory` |
| `--in-memory` | | keep everything in memory (nothing survives a restart) |
| `--node-id` | `CHORUS_NODE_ID` | identity for a new data dir (default: generated); must match an existing one |
| `--no-group-commit` | | sync each flush inline instead of in a background per-object task |

Logging uses `RUST_LOG` (default `info`). Startup logs a recovery summary
(buckets, objects, unfinalized objects, files removed, generation
high-water mark).

## Using it from a client

```rust
use std::sync::Arc;
use chorus_client::{ClientConfig, SegmentedVolume, TcpReplicaFactory};

let mut factories = Vec::new();
for (zone, addr) in ["node-a:7070", "node-b:7070", "node-c:7070"].iter().enumerate() {
    // `bucket` is a namespace on that node; keep it stable across restarts.
    factories.push(TcpReplicaFactory::connect(*addr, "my-db", zone).await?);
}
let volume = SegmentedVolume::new_tcp(
    factories,
    manifest_store,        // Arc<dyn ManifestStore>: see Limitations
    "db/wal",              // object-name prefix of this volume
    ClientConfig::default(),
)?;
let mut recovery = volume.recover(chorus_client::WalSeqNo::record(0)).await?;
```

`tests/volume.rs` is a complete example: a WAL over three nodes, a writer
crash, and a restart of all three nodes.

## Disk layout

```text
<data-dir>/node.meta                                 node id + generation high-water mark
<data-dir>/<bucket-stem>/<object-stem>.meta          committed metadata of the live generation
<data-dir>/<bucket-stem>/<object-stem>.<gen>.data    that generation's bytes
```

- **Names.** A stem is the percent-encoded name. The bytes `A-Za-z0-9_-`
  are kept and everything else, including `.`, `/` and `%`, becomes `%XX`.
  So `wal/seg-1` becomes `wal%2Fseg-1`, and a stem can never be `.` or `..`.
  An empty name, or one whose encoding is longer than 200 bytes, uses
  `~<sha256 hex>` instead, which keeps every file name under the 255-byte
  limit. Nothing is rejected. Each `.meta` stores the real bucket and object
  name, and recovery reads the names from there (checking that they map back
  to the file's stem). Directories that are not valid stems, such as
  `lost+found`, are ignored.
- **Records.** `node.meta` and `*.meta` hold
  `magic | u32 len | u32 crc32c | rkyv body`. The body is validated with
  bytecheck when loaded. An object meta holds generation, metageneration,
  custom metadata, finalized flag, final size and CRC, writer epoch and
  last-modified time. The byte count of an unfinalized object is the data
  file's length. It is not stored in the meta.
- **Generations** are wall-clock microseconds, or the previous generation
  plus one when that is larger. Before any file of a new generation exists,
  `node.meta`'s high-water mark is raised to cover it, 10 s of microseconds
  at a time, so most creates do not rewrite `node.meta`. Recovery restarts
  the counter above the high-water mark, so a generation is never reused,
  even one whose object was deleted.

## Durability model

Every metadata commit uses the rename trick: write `X.tmp`, fsync it, rename
it over `X`, fsync the directory. An object's `.meta` is the commit point.

| operation | steps | after a crash |
|---|---|---|
| create / replace | raise the high-water mark if needed; write `<stem>.<gen>.data`, fsync it and the directory; commit `.meta`; unlink the old generation's data | the old object (or none) plus an orphan data file that recovery deletes, or the new generation with all its bytes |
| append | `pwrite` at the accepted tail through a cached handle | bytes may or may not survive (see below) |
| flush | `fdatasync`, then `Durable` | every byte up to the acknowledged size survives |
| takeover / handle-free finalize | sync the data, commit `.meta` with the epoch bumped | the epoch never goes back |
| finalize | `fdatasync` the data, commit `.meta` with `finalized = true` (this is the finalize marker) | finalized at exactly that length, or still open with all its bytes |
| delete | unlink `.meta`, fsync the directory, unlink the data, fsync the directory | the object is gone (possibly leaving an orphan data file), never a `.meta` naming a missing file |

On startup, recovery:

- scans every bucket directory and deletes `*.tmp` files;
- loads and verifies every `.meta`;
- for an unfinalized object, takes the durable size from the data file's
  length and recomputes its CRC32C by reading the file;
- for a finalized object, checks that the data file's length matches;
- deletes data files that no `.meta` references.

A corrupt `.meta`, a missing data file, or a finalized file of the wrong
length cannot come from a crash, so each of them stops startup with an
error instead of being guessed at. All sessions are gone after a restart.
A write handle from before the restart resumes if and only if no takeover
(epoch bump) happened since.

**Group commit** is on by default. A flush (`Flush`, or `Append` with
`flush`) only queues its session on the object and returns, so the
connection's next request is not held up by the `fdatasync`. One background
task per object does the work. It snapshots the accepted size, runs
`fdatasync` without the object lock, raises the durable size, and sends
`Durable` to every queued session that is still live. Flushes that arrive
during a sync are folded into the next one. Appends keep their order under
the object lock. `Takeover`, `Resume` and `Finalize` still sync inline
before they answer. `Durable` values stay monotonic, and a session that has
been fenced gets no `Durable` after its `SessionFailed`. See the module
docs of `store` for the full argument.

## Limitations

- **One thread.** One compio runtime serves every connection and every disk
  operation. Large reads, the copy of request payloads, and CRC computation
  all run on that thread.
- **No auth, no TLS.** Anyone who can reach the port can read and write
  everything. Run it only on a trusted network.
- **No manifest store.** Chorus needs a compare-and-set register for its
  manifest (`ManifestStore`). This server does not provide one over TCP.
  Use `GcsManifestStore` or your own implementation.
- **No backpressure or quotas.** Outbound queues are unbounded, and there
  are no per-connection or per-node limits on memory, in-flight requests or
  disk space. A full disk shows up as `Internal` errors.
- **Bytes past the last fsync.** After a crash, an unfinalized object's tail
  is the data file's length. That length can include appended bytes that
  were never acknowledged as durable, and in theory a torn page, since the
  kernel may write pages back in any order. Chorus recovery keeps only the
  prefix of whole records that a quorum of replicas agree on, which in
  practice discards such a suffix. Other readers must not trust bytes
  beyond the durable size they were acknowledged. Fixing this properly
  needs a durable size watermark, which costs a metadata write per sync.
- **Startup reads every open object** in full to recompute its CRC.
- **Strict recovery.** One corrupt `.meta` keeps the node from starting
  until an operator moves it aside.
- **One generation per name** is kept, as in GCS zonal buckets. There is no
  version history.
- **No compaction or background scrubbing.** Bytes are verified only when a
  full snapshot is read.
- **Linux-oriented.** The code uses compio's io_uring driver and
  directory fsync, which other platforms may not honor.
