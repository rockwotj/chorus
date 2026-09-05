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
streams, repairs data, or changes the truncation floor. It reads immutable
segments published in the hot directory or archive catalog, plus the
manifest-selected active appendable object through GCS `BidiReadObject`. A complete active frame
is emitted only when identical bytes are visible on a strict majority, so a
minority-only or partial suffix is never exposed. The follower keeps one
`BidiReadObject` RPC open per zone for the current active object, sends a new
range message on each poll, and returns as soon as the first matching strict
majority responds; a slow remaining request stays in flight for a later poll.
Manifest refresh runs independently on `manifest_poll_interval` and does not
delay active-tail delivery. The manifest changes on rotation, sealing,
truncation, archival, and maintenance rather than on each append, so that interval is normally much larger than
`poll_interval`; polling it at the active-tail rate multiplies regional reads
without observing anything new.

A read that delivered records is followed immediately by the next read, so
`poll_interval` bounds only how often an idle follower re-reads the tail. While
the writer is producing faster than one record per round trip, freshness is the
fastest matching majority's bidirectional-read and decoding latency. Once the
follower catches up, the next record still waits up to `poll_interval` to be
observed, which is the latency against request-rate tradeoff that interval
controls. Segment rotation is never required for delivery.

Followers do not register with the writer. Drop the stream to stop at an
application-selected sequence number. With archival enabled, followers can
read history back to the floor recorded at archive activation, independently
of subsequent checkpoint advances. Without archival, retain WAL history long enough for
every replica to advance its own durable checkpoint. If truncation overtakes a
follower, the stream returns `Error::ReadOnlyLagged` rather than skipping
records; resnapshot that replica before reopening it at a newer checkpoint.

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

## Archival and long retention

Attach an immutable `ArchiveStore` and choose the hot sealed-segment target:

```rust,ignore
use chorus_client::{ArchivePolicy, GcsArchiveStore};
use std::sync::Arc;

let archive = Arc::new(GcsArchiveStore::new(regional_factory, "orders/archive")?);
let volume = volume.with_archive(archive, ArchivePolicy {
    keep_sealed_segments: 8,
})?;
```

Maintenance uploads finalized segments and immutable catalog pages, then
atomically publishes the catalog root and removes the old hot directory entry.
Only afterward does it delete the redundant zonal copies. The count is a
placement target, not a deletion policy: archived history is retained
indefinitely. The minimum is one because the latest seal remains a recovery
finalization witness. Upload failures retain hot data; continued archive
unavailability can still cause non-poisoning directory backpressure.
After archival or truncation frees directory capacity, the engine retries a
failed manifest refresh until it can reconsider blocked rotation. Periodic
archive work and queued seal/truncate commands also alternate priority when
maintenance is overdue, so either class of work continues to make progress.

The control manifest stores one bounded root reference. The copy-on-write
catalog has bounded pages and logarithmic seek/append cost; readers do not load
the complete archive directory. `ArchiveStore` provides immutable conditional
uploads and streaming reads (optionally by byte range), and is shared by WAL
objects and catalog pages. It does not expose mutable whole-history manifests.

`GcsBodyManifestStore` separately implements the existing `ManifestStore` with
a generation-CAS GCS object body and a configurable directory budget. Pass it
to `SegmentedVolume::new_with_manifest_store` when the hot register needs more
capacity. Each update replaces that bounded body; it is not the archive index.
Do not switch a running volume between metadata and body storage in place.

Enabling archival persists format 2 and the backing namespace during recovery.
Old clients reject this format. All subsequent opens must supply the same
archive namespace. Already-truncated history is not restored. Writer recovery
still requires the current database checkpoint; use a readonly stream for
historical replay into a separate restored database. PITR also requires an
application snapshot and transaction/time mapping; Chorus stores opaque records.

Archive objects, including superseded and uncommitted catalog pages, are not
garbage-collected in this version. Do not configure bucket lifecycle deletion
for the archive prefix. Reads validate the complete segment's length, SHA-256,
CRC32C, and framing before yielding records, so memory is bounded by a segment
and the catalog traversal, not by retained history.

See [the archival safety argument](../../p/ARCHIVE_SAFETY.md) for the publication
protocol, proof assumptions, and verification commands.

## Important behavior

- Segment rotation and immutable sealed-segment repair are automatic.
- Without archival, truncation and replacement deletion are permanent. Enable
  archival before advancing checkpoints if historical WAL replay is required.
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
