# chorus-client

`chorus-client` is the Rust client for [Chorus](https://github.com/rockwotj/chorus),
a single-writer write-ahead log built on Google Cloud Storage. It replicates
opaque application records across GCS Rapid zonal buckets and commits them once
a strict majority reports durability.

For the motivation, architecture, and an approachable protocol overview, start
with [Chorus: A fast WAL for object storage](https://rockwotj.com/blog/chorus/).

Use this crate when a database or storage engine needs a durable ordered log
without operating a separate replicated WAL service. The application defines
the transaction encoding, owns the durable checkpoint, and applies replayed
records; Chorus handles replication, recovery, rotation, repair, and truncation.

The `0.1` release is an initial public API and may evolve before `1.0`.

## Installation

```sh
cargo add chorus-client bytes futures
```

The client is asynchronous and runs on Tokio. Application records are supplied
as `bytes::Bytes`.

## Basic lifecycle

Every process start follows the same sequence:

1. Load the database's durable checkpoint.
2. Recover the Chorus volume from that checkpoint.
3. Apply the complete recovery stream.
4. Start the live WAL.
5. Enqueue records and await their durability completions.
6. Advance the database checkpoint before truncating old WAL history.

```rust,ignore
use bytes::Bytes;
use chorus_client::{ClientConfig, SegmentedVolume, WalEngineConfig, WalSeqNo};
use futures::TryStreamExt;

let volume = SegmentedVolume::new(
    zonal_factories,
    manifest_factory,
    "databases/orders/wal",
    ClientConfig::default(),
)?;

let checkpoint = WalSeqNo::record(load_checkpoint()?);
let mut recovery = volume.recover(checkpoint).await?;
let next_seqno = recovery.end;

while let Some(record) = recovery.try_next().await? {
    apply_replayed_transaction(record.payload.as_ref())?;
    persist_checkpoint(record.next_seqno())?;
}

let mut wal = recovery.start(WalEngineConfig::default()).await?;

let transaction = Bytes::from(encoded_transaction);
let completion = wal
    .enqueue_append(next_seqno, transaction.clone())
    .await?;
let receipt = completion.await?;

apply_committed_transaction(transaction.as_ref())?;
persist_checkpoint(receipt.next_seqno())?;
wal.truncate_before(receipt.next_seqno()).await?;
wal.shutdown().await?;
```

See
[database_wal.rs](https://github.com/rockwotj/chorus/blob/main/rust/client/examples/database_wal.rs)
for a complete example using Application Default Credentials, GCS client
construction, pipelined appends, replay, truncation, and shutdown. In a Chorus
checkout, build it with:

```sh
cargo check -p chorus-client --example database_wal
```

## Commit and recovery semantics

`WalHandle::enqueue_append` validates and reserves the caller-provided sequence
number, then returns an `AppendCompletion` without waiting for GCS. Awaiting that
completion preserves prefix order:

- Success means this record and every preceding record are durable on a strict
  majority of the configured zones.
- A definitive failure means the sequence number was not committed and may be
  retried as documented by the error.
- If `Error::may_have_committed()` is true, restart recovery before reusing the
  sequence number or accepting more work. Recovery may replay the record.

This is an at-least-once boundary. The database should make transaction
application idempotent or deduplicate it using the WAL sequence number.

Recovery returns a fixed stream ending at `Recovery::end`. Consume the stream
completely before calling `Recovery::start`; starting early is rejected. A live
`WalHandle` is write-only, so replay occurs only during startup recovery.

The database remains responsible for its checkpoint. Call
`WalHandle::truncate_before` only after the database has durably incorporated
all records below that boundary.

## Readonly followers

`SegmentedVolume::open_readonly` opens a non-coordinating follower for a read
replica process. The returned `ReadOnlyFollower` is an ordered stream that
remains pending when caught up and automatically polls the active tail and
later seals:

```rust,ignore
use chorus_client::{ReadOnlyConfig, WalSeqNo};
use futures::TryStreamExt;
use std::time::Duration;

let mut follower = volume
    .open_readonly_with_config(
        WalSeqNo::record(load_replica_checkpoint()?),
        ReadOnlyConfig {
            poll_interval: Duration::from_millis(100),
            manifest_poll_interval: Duration::from_secs(1),
        },
    )
    .await?;

while let Some(record) = follower.try_next().await? {
    apply_to_read_replica(record.payload.as_ref())?;
    persist_replica_checkpoint(record.next_seqno())?;
}
```

Readonly open never creates the manifest, claims a writer epoch, opens append
streams, repairs data, or changes the truncation floor. It reads only immutable
segments already published in the manifest directory plus the manifest-selected
active appendable object through GCS `BidiReadObject`. A complete active frame
is emitted only when identical bytes are visible on a strict majority, so a
minority-only or partial suffix is never exposed. The follower keeps one
`BidiReadObject` RPC open per zone for the current active object, sends a new
range message on each poll, and returns as soon as the first matching strict
majority responds; a slow remaining request stays in flight for a later poll.
Manifest refresh runs independently on `manifest_poll_interval` and does not
delay active-tail delivery. The manifest changes only when the writer rotates,
seals, or truncates, so that interval is normally much larger than
`poll_interval`; polling it at the active-tail rate multiplies regional reads
without observing anything new.

A read that delivered records is followed immediately by the next read, so
`poll_interval` bounds only how often an idle follower re-reads the tail. While
the writer is producing faster than one record per round trip, freshness is the
fastest matching majority's bidirectional-read and decoding latency. Once the
follower catches up, the next record still waits up to `poll_interval` to be
observed, which is the latency against request-rate tradeoff that interval
controls. Segment rotation is never required for delivery.

Followers do not register with the writer. Retain WAL history long enough for
every replica to advance its own durable checkpoint. If truncation overtakes a
follower, the stream returns `Error::ReadOnlyLagged` rather than skipping
records; resnapshot that replica before reopening it at a newer checkpoint.

## Experimental SlateDB adapter

The opt-in `slatedb` feature provides
`chorus_client::slatedb::ChorusWal`, implementing SlateDB's pluggable WAL writer,
startup replay, and garbage-collection interfaces. It is disabled by default; the default build
does not compile SlateDB or enable its object-store providers/caches.

**Not ready to merge or publish:** this currently pins SlateDB main at
[`31656fe3`](https://github.com/slatedb/slatedb/commit/31656fe30064ce0d7991a578085341e21438af5e).
Replace the git dependency with the first suitable stable release and rerun
the adapter tests before merging. Cargo still resolves optional dependencies
when generating a lockfile, so even feature-off resolution may need GitHub.
Applications must use the same SlateDB revision to share the trait types:

```toml
[dependencies]
chorus-client = { path = "path/to/chorus/rust/client", features = ["slatedb"] }
slatedb = { git = "https://github.com/slatedb/slatedb", rev = "31656fe30064ce0d7991a578085341e21438af5e", default-features = false }
```

Create a dedicated `SegmentedVolume` using the storage setup below, then pass
it directly to SlateDB. Do **not** recover/start it separately:

```rust,ignore
use chorus_client::slatedb::ChorusWal;
use slatedb::{Db, GarbageCollectorBuilder};
use std::sync::Arc;

let wal = ChorusWal::new(volume);
let gc = GarbageCollectorBuilder::new("databases/orders", sst_object_store.clone())
    .with_wal_gc(Arc::new(wal.clone()));
let db = Db::builder("databases/orders", sst_object_store)
    .with_wal_writer(Box::new(wal))
    .with_gc_builder(gc)
    .build()
    .await?;

let write = db.put(b"customer/7", b"alice").await?;
write.await_durable().await?; // Waits for a Chorus quorum, not an SST flush.
db.close().await?;
```

Use `ChorusWal::with_config(volume, config)` to customize `WalEngineConfig`.
The complete **encoded batch** must fit `max_record_bytes` (default 1 MiB).
Oversized batches fail, rather than being split and losing atomicity. Admission
and completion failures close the adapter; ambiguous outcomes require reopening
the database to resolve the recovered prefix, not retrying within that writer.

Each SlateDB write batch is one Chorus record, preserving values, tombstones,
merge operands, sequence numbers, and optional creation/expiry timestamps. A
versioned binary envelope is independent of SlateDB's SST encoding. Record index
`n` maps to WAL file ID `n + 1`; SlateDB's `replay_after_wal_id` consequently maps
directly to Chorus's inclusive recovery checkpoint. Replay streams one batch at
a time, and the engine starts after replay finishes, making GC available even
before the first append. Append admission is
pipelined and ordered quorum completions drive durability notifications. Flush
is a barrier over preceding admissions; it does not force segment rotation.

Fencing follows SlateDB's manifest-fence / Chorus-recovery / manifest-recheck
protocol. Starting another database writer takes over the volume and invalidates
the previous writer. Use the same dedicated volume on every open; do not share
one volume between databases, mix other application records into it, or switch
an existing database from its native WAL merely by changing this option. The
upstream manifest does not yet persist/validate the custom backend identity.

GC must be wired explicitly with `with_wal_gc` **and** `with_gc_builder`, using
clones of the same `ChorusWal` instance and the same SlateDB database path/store.
SlateDB supplies ranges referenced by the current manifest and retained
checkpoints. Collection removes only whole sealed segments from the prefix
before **every** referenced range, leaving holes between retained ranges alone.
It preserves the current WAL boundary, the active tail, and pending seals.
Replay position and retention authority are separate: an older checkpoint can
keep history below the current writer's startup replay position.

Collection uses the attached writer's existing maintenance queue, serializing
deletion with repair without fencing or pausing the append engine. Committed
truncation floors and generation-guarded deletion tombstones provide the usual
Chorus retry/recovery safety, including when a zone is unavailable. Successful
collection wakes the rotation capacity check. This collector requires an open Db
using the same initializer or a clone; separate-process/offline collection is
not supported and returns an error rather than recovering/fencing the volume.

Configure the GC builder's `GarbageCollectorOptions::wal_options` for interval,
`min_age`, and `dry_run`. Nonzero `min_age` uses GCS object modification time
(`update_time`): every existing replica must be strictly older than the cutoff
before GC authorizes a segment's deletion. A repaired/replaced copy gets its own
age; unavailable listings, missing/invalid timestamps, or future timestamps defer
new truncation. This requires all zones to answer age checks; `min_age = 0`
disables the age gate and allows degraded-zone truncation. Object age survives
restarts and is independent of when a segment becomes unreferenced. No on-disk
schema change is needed. Dry runs do not advance the floor or delete objects;
they log the proposed floor using the same age checks. Background maintenance
may still retry deletions authorized by an earlier real collection. Long-lived
checkpoints, a long minimum age, or unavailable zones can still exhaust the
bounded manifest directory; size rotation/GC settings for the retention window.

`WalReader` and `WalAdmin` remain unimplemented: separate live WAL readers and
WAL-based clones are unsupported. Ordinary `Db::get`/`Db::scan` reads work.
Do not use SlateDB's default WAL reader/clone/delete tools for this backend.
Chorus CLI recovery/repair commands fence the active writer and must run offline.

From the repository's `rust` directory:

```sh
cargo test -p chorus-client --features slatedb slatedb::
cargo test -p chorus-client --features slatedb --test slatedb_integration
cargo test -p chorus-client --no-default-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

The adapter tests run actual SlateDB databases over the loopback fake-GCS
transport, including atomic batch replay, L0 checkpoint resume, takeover,
quorum-only durability, failure propagation, corrupt-record rejection, retained
checkpoints, scheduled GC with live writes, age/dry-run gates, and deletion retries.
External integration tests use only the exported API to check mixed writes,
deletes, reads, and scans against a reference model across GC and fresh-client
reopens, plus writer takeover while a zone is unavailable. These tests use fake
GCS servers, not live-cloud resources.

## Storage setup

A typical volume uses:

- Three Rapid buckets in distinct zones of one region for segment data.
- One regional bucket in the same region for the default manifest register.
- `https://storage.googleapis.com` as the gRPC endpoint.
- Full v2 bucket resource names such as
  `projects/_/buckets/orders-wal-zone-a`.

One, three, and five zonal replicas are supported. The ordered bucket list is
part of the durable volume identity, so keep its membership and order stable
across restarts.

`BearerAuth::google_adc` is the normal authentication path and refreshes
Application Default Credential tokens for existing clients. Static bearer
tokens and anonymous local transports are also supported.

The default manifest register is object metadata in regional GCS. Applications
can implement `ManifestStore` and use
`SegmentedVolume::new_with_manifest_store` to place the register in Firestore,
Spanner, SQL, or another strongly consistent compare-and-swap store. Segment
data remains in the zonal buckets.

## Important behavior

- Segment rotation and immutable sealed-segment repair are automatic.
- Truncation and replacement deletion are permanent. Archive sealed history
  first if the application needs point-in-time recovery.
- `Error::ActiveSegmentFull` is non-poisoning backpressure and does not consume
  the attempted sequence number.
- `SegmentedVolume::new_with_metrics_recorder` connects Chorus to an
  application metrics backend; `SegmentedVolume::new` disables metrics.
- A running `WalHandle` is the exclusive writer. Starting another writer
  requires recovery, which fences the previous writer incarnation.

## Metrics

The default build registers 12 metric names, all prefixed with `chorus.wal.`:

| Kind | Names |
| --- | --- |
| Counter | `append.committed_records`, `append.committed_bytes`, `append.failures`, `transport.rpc_failures{code}` |
| Gauge | `pipeline.queue_bytes`, `maintenance.queue_depth`, `replica.durable_lag_bytes{zone}`, `manifest.directory_bytes` |
| Histogram | `append.commit_latency_seconds`, `transport.rpc_seconds{op}`, `manifest.cas_latency_seconds`, `seal.duration_seconds` |

Lifecycle events, repair outcomes, retries, and maintenance failures are logged.
RPC timing covers quorum-path replica operations, not all storage calls; manifest
CAS timing remains separate. An operation such as `snapshot` may contain multiple
provider RPCs. Labelled metrics register one series per label value.

The internal `dst-support` feature additionally registers `lane.timeouts`,
`lane.capacity_drops`, `repair.passes`, and `seal.segments`. Simulations use these
for fault scheduling and protocol coverage; they are not production metrics.
Unit-test builds also enable these counters.

CLI benchmarks retain append latency percentiles, throughput, and recovery phase
timings. They no longer report batching, write amplification, in-flight high-water
marks, lane event counts, or recovery CAS/seal operation counts. Configured limits
and observed/replayed record and segment counts remain in benchmark output.

## More information

- [Design overview](https://rockwotj.com/blog/chorus/)
- [Repository and verification guide](https://github.com/rockwotj/chorus#readme)
- [API documentation](https://docs.rs/chorus-client)
- [Complete database example](https://github.com/rockwotj/chorus/blob/main/rust/client/examples/database_wal.rs)
- [`chorus-cli`](https://github.com/rockwotj/chorus/tree/main/rust/bin/chorus-cli)
  for inspecting and operating a live volume
