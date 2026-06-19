//! End-to-end database integration using three GCS Rapid zonal buckets for
//! segment data plus one regional bucket hosting the manifest register.

use std::env;

use anyhow::{Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use chorus_client::{
    BearerAuth, ClientConfig, GrpcReplicaFactory, RefreshingAuthConfig, SegmentedVolume,
    WalEngineConfig, WalSeqNo,
};
use futures::TryStreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    // Use the Cloud Storage gRPC endpoint (`https://storage.googleapis.com`) for
    // every factory unless the deployment has validated another gRPC-compatible
    // endpoint. Regional JSON/XML endpoints do not support gRPC. Each zonal
    // factory must use a distinct bucket, passed as a full v2 resource name such
    // as `projects/_/buckets/my-rapid-zone-a`.
    let endpoints = required_csv("CHORUS_GCS_ENDPOINTS")?;
    let buckets = required_csv("CHORUS_GCS_BUCKETS")?;
    let manifest_endpoint = required_var("CHORUS_GCS_MANIFEST_ENDPOINT")?;
    let manifest_bucket = required_var("CHORUS_GCS_MANIFEST_BUCKET")?;
    anyhow::ensure!(endpoints.len() == 3 && buckets.len() == 3);

    // ADC is the normal production path. All factories share this ArcSwap-backed
    // auth handle, so refreshed tokens reach existing gRPC clients without
    // rebuilding channels. Use GrpcReplicaFactory::connect(..., Some(token)) for
    // a controlled static-token deployment, or None for a local anonymous fake.
    let auth = BearerAuth::google_adc(RefreshingAuthConfig::default()).await?;
    let mut factories = Vec::with_capacity(3);
    for zone in 0..3 {
        factories.push(
            GrpcReplicaFactory::connect_with_auth(
                zone,
                &endpoints[zone],
                buckets[zone].clone(),
                auth.clone(),
            )
            .await?,
        );
    }

    // the regional bucket carries only the manifest control register
    let manifest_factory =
        GrpcReplicaFactory::connect_with_auth(3, &manifest_endpoint, manifest_bucket, auth.clone())
            .await?;

    let volume = SegmentedVolume::new(
        factories,
        manifest_factory,
        "databases/orders/wal",
        ClientConfig::default(),
    )?;

    // Load this boundary from the database's own durable checkpoint. Recovery
    // starts replay there. The manifest independently prevents objects below a
    // committed truncation floor from resurrecting history.
    let checkpoint_resume = WalSeqNo::record(0);

    // Recovery fences and seals the manifest's active tail, then directly
    // streams the fixed replay range. It does not create the next appendable
    // segment until this stream has been consumed successfully.
    let mut recovery = volume.recover(checkpoint_resume).await?;
    let replay_end = recovery.end;
    while let Some(record) = recovery.try_next().await? {
        apply_to_database(record.payload.as_ref())?;
        let _next_durable_resume = record.next_seqno();
    }
    // The default queue is 256 records; size max_segment_bytes separately from
    // encoded throughput and the manifest CAS-rate budget.
    let mut wal = recovery.start(WalEngineConfig::default()).await?;

    // The database owns sequence allocation. `replay_end` is the first sequence
    // available after startup. Admission verifies each contiguous number and
    // returns before GCS durability, allowing the completions to flow through a
    // separate transactional apply pipeline.
    let first_seqno = replay_end;
    let completion_a = wal
        .enqueue_append(
            first_seqno,
            encode_transaction(&[b"put customer/7 alice", b"update customer-index/7"]),
        )
        .await?;
    let completion_b = wal
        .enqueue_append(
            WalSeqNo::record(first_seqno.record_index + 1),
            encode_transaction(&[b"debit account/9 50"]),
        )
        .await?;

    // The appends are now owned by the WAL and may be concurrently in flight.
    // Await durability before applying each transaction to database state. On
    // failure, Error::may_have_committed() distinguishes ambiguous outcomes
    // that require recovery before reusing or advancing the sequence number.
    let (receipt_a, receipt_b) = match tokio::try_join!(completion_a, completion_b) {
        Ok(receipts) => receipts,
        Err(error) => {
            if error.may_have_committed() {
                anyhow::bail!(
                    "append outcome may have committed; restart recovery before reusing the \
                     sequence number: {error}"
                );
            }
            return Err(error.into());
        }
    };
    println!(
        "committed appends {:?} and {:?}; durable resumes {:?} and {:?}",
        receipt_a.seqno,
        receipt_b.seqno,
        receipt_a.next_seqno(),
        receipt_b.next_seqno()
    );

    println!(
        "replayed through exclusive record {}",
        replay_end.record_index
    );

    // Sealed-segment repair is automatic at engine startup, after each
    // rotation, and periodically. It never gap-fills or mutates the active
    // appendable segment, and the engine serializes it with truncation.

    // After the database has durably checkpointed all effects before
    // `replay_end`, request whole-segment deletion. Replay exists only during
    // startup recovery, so a running WAL has no concurrent reader pins. Persist
    // `replay_end` as the database checkpoint regardless of deletion counts;
    // Chorus does not manufacture a replacement checkpoint.
    let report = wal.truncate_before(replay_end).await?;
    println!(
        "truncation deleted_segments={} deleted_objects={}",
        report.deleted_segments, report.deleted_objects
    );

    // Graceful shutdown rejects new appends and allows the accepted-work drain
    // and owned-task joins up to `shutdown_timeout`. On expiry it aborts the
    // remaining tasks, bounds the cleanup join by the same interval, and returns
    // `Error::ShutdownTimeout`; `abort` remains for exceptional termination.
    wal.shutdown().await?;
    Ok(())
}

fn required_var(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("set {name}"))
}

fn required_csv(name: &str) -> Result<Vec<String>> {
    env::var(name)
        .with_context(|| format!("{name} is required"))
        .map(|value| value.split(',').map(str::to_owned).collect())
}

fn apply_to_database(record: &[u8]) -> Result<()> {
    println!("apply {}", String::from_utf8_lossy(record));
    Ok(())
}

// The database, not the WAL, defines transaction encoding. This example uses a
// tiny length-prefixed envelope; production code would normally call its
// existing transaction codec and submit the resulting bytes once.
fn encode_transaction(operations: &[&[u8]]) -> Bytes {
    let mut encoded = BytesMut::new();
    encoded.put_u32(operations.len() as u32);
    for operation in operations {
        encoded.put_u32(operation.len() as u32);
        encoded.extend_from_slice(operation);
    }
    encoded.freeze()
}
