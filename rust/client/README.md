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

## More information

- [Design overview](https://rockwotj.com/blog/chorus/)
- [Repository and verification guide](https://github.com/rockwotj/chorus#readme)
- [API documentation](https://docs.rs/chorus-client)
- [Complete database example](https://github.com/rockwotj/chorus/blob/main/rust/client/examples/database_wal.rs)
- [`chorus-cli`](https://github.com/rockwotj/chorus/tree/main/rust/bin/chorus-cli)
  for inspecting and operating a live volume
