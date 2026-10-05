//! The real TCP client (`TcpReplicaFactory`) against a real storage node.

#[macro_use]
mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chorus_client::dst_support::pack_append;
use chorus_client::{
    AppendToken, ListedObject, Replica, ReplicaFactory, TcpReplicaFactory, TransportCode,
};
use common::{Backing, TestNode};

const BUCKET: &str = "zone-a";
const STEP: Duration = Duration::from_secs(20);

async fn connect(node: &TestNode) -> TcpReplicaFactory {
    TcpReplicaFactory::connect(node.addr(), BUCKET, 0)
        .await
        .expect("connect")
}

fn metadata() -> HashMap<String, String> {
    HashMap::from([("chorus.format".to_string(), "1".to_string())])
}

/// Wait until the lane's durable tail reaches `target`.
async fn wait_durable(replica: &Arc<dyn Replica>, target: i64) {
    let mut seen = -1;
    while seen < target {
        let change = tokio::time::timeout(STEP, replica.lane_durable_change(seen))
            .await
            .expect("durable progress")
            .expect("lane healthy");
        assert!(change.error.is_none(), "{:?}", change.error);
        seen = change.persisted_size;
    }
    assert_eq!(seen, target);
}

/// The error the lane reports next (after any durable progress).
async fn lane_error(replica: &Arc<dyn Replica>, seen: i64) -> TransportCode {
    loop {
        match tokio::time::timeout(STEP, replica.lane_durable_change(seen))
            .await
            .expect("lane outcome")
        {
            Ok(change) => {
                if let Some(error) = change.error {
                    return error.code;
                }
            }
            Err(error) => return error.code,
        }
    }
}

async fn send(replica: &Arc<dyn Replica>, offset: i64, chunks: &[&[u8]]) {
    let packed = pack_append(chunks.iter().map(|c| Bytes::copy_from_slice(c)).collect());
    replica.lane_send(offset, &packed).await.expect("lane send");
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

async fn append_lifecycle(backing: Backing) {
    let node = TestNode::start_with("lifecycle", backing);
    let factory = connect(&node).await;
    assert_eq!(factory.node_id().as_deref(), Some("lifecycle"));
    let replica = factory.replica("wal/seg-1");

    let mut token = replica.create_append_session(metadata()).await.unwrap();
    assert_eq!(token.persisted_size, 0);
    send(&replica, 0, &[b"hello ", b"world"]).await;
    wait_durable(&replica, 11).await;
    send(&replica, 11, &[b"!"]).await;
    wait_durable(&replica, 12).await;

    let snapshot = replica.snapshot().await.unwrap();
    assert_eq!(snapshot.bytes, b"hello world!");
    assert_eq!(snapshot.persisted_size, 12);
    assert!(!snapshot.finalized);
    assert_eq!(snapshot.generation, token.generation.unwrap());
    assert_eq!(snapshot.metadata["chorus.format"], "1");
    let range = replica.read_range(6).await.unwrap();
    assert_eq!(range.bytes, b"world!");
    assert_eq!(range.generation, snapshot.generation);

    // Stat is tail-blind while open.
    let stat = replica.stat().await.unwrap();
    assert!(!stat.finalized);
    assert_eq!(stat.persisted_size, 0);

    let finalized = replica.finalize(&mut token, 12).await.unwrap();
    assert!(finalized.finalized);
    assert_eq!(finalized.persisted_size, 12);
    assert_eq!(finalized.crc32c, Some(crc32c::crc32c(b"hello world!")));

    let stat = replica.stat().await.unwrap();
    assert!(stat.finalized);
    assert_eq!(stat.persisted_size, 12);
    assert_eq!(stat.metageneration, 2);

    let listed = factory.list("wal/").await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "wal/seg-1");
    assert_eq!(listed[0].size, 12);
    assert!(listed[0].finalized);
    assert_eq!(listed[0].crc32c, finalized.crc32c);
    assert!(listed[0].last_modified.is_some());
    assert!(factory.list("other/").await.unwrap().is_empty());

    let wrong = replica.delete(stat.generation + 1).await.unwrap_err();
    assert_eq!(wrong.code, TransportCode::FailedPrecondition);
    replica.delete(stat.generation).await.unwrap();
    assert_eq!(
        replica.stat().await.unwrap_err().code,
        TransportCode::NotFound
    );
    assert_eq!(
        replica.snapshot().await.unwrap_err().code,
        TransportCode::NotFound
    );
    assert_eq!(
        replica.delete(stat.generation).await.unwrap_err().code,
        TransportCode::NotFound
    );
}

async fn create_conflict(backing: Backing) {
    let node = TestNode::start_with("conflict", backing);
    let factory = connect(&node).await;
    factory
        .replica("seg")
        .create_append_session(metadata())
        .await
        .unwrap();
    let error = factory
        .replica("seg")
        .create_append_session(metadata())
        .await
        .unwrap_err();
    assert_eq!(error.code, TransportCode::AlreadyExists);
    assert_eq!(error.zone, 0);
}

async fn takeover_fences_the_old_writer(backing: Backing) {
    let node = TestNode::start_with("takeover", backing);
    let old_factory = connect(&node).await;
    let new_factory = connect(&node).await;
    let old = old_factory.replica("seg");
    old.create_append_session(metadata()).await.unwrap();
    send(&old, 0, &[b"abc"]).await;
    wait_durable(&old, 3).await;

    let new = new_factory.replica("seg");
    let observed = new.snapshot().await.unwrap();
    let mut token = new.takeover(&observed).await.unwrap();
    assert_eq!(token.persisted_size, 3);

    // The old lane learns it was fenced.
    assert_eq!(lane_error(&old, 3).await, TransportCode::FailedPrecondition);

    // The new writer continues and finalizes.
    send(&new, 3, &[b"def"]).await;
    wait_durable(&new, 6).await;
    let finalized = new.finalize(&mut token, 6).await.unwrap();
    assert!(finalized.finalized);
    assert_eq!(new.snapshot().await.unwrap().bytes, b"abcdef");

    // A takeover against a stale observation fails.
    let error = new.takeover(&observed).await.unwrap_err();
    assert_eq!(error.code, TransportCode::FailedPrecondition);
}

async fn resume_after_a_dropped_connection(backing: Backing) {
    let node = TestNode::start_with("resume", backing);
    let factory = connect(&node).await;
    let replica = factory.replica("seg");
    let mut token = replica.create_append_session(metadata()).await.unwrap();
    send(&replica, 0, &[b"durable"]).await;
    wait_durable(&replica, 7).await;
    // Accepted but never flushed by the client.
    let packed = pack_append(vec![Bytes::from_static(b"+unflushed")]);
    replica.lane_send_unflushed(7, &packed).await.unwrap();
    // The node applies one connection's requests in order, so once this
    // answer arrives the append has been accepted.
    replica.stat().await.unwrap();

    // The node drops every connection; the lane sees its session end.
    node.disconnect_all();
    assert_eq!(lane_error(&replica, 7).await, TransportCode::Unavailable);

    // Resume reconnects and reattaches; the answer covers every accepted
    // byte, so the lane resends nothing twice.
    let tail = replica.resume_tail(&mut token).await.unwrap();
    assert_eq!(tail, 17);
    send(&replica, 17, &[b"!"]).await;
    wait_durable(&replica, 18).await;

    // A second client resumes with the same token on its own connection;
    // the first lane is displaced.
    let other_factory = connect(&node).await;
    let other = other_factory.replica("seg");
    let mut other_token = token.clone();
    assert_eq!(other.resume_tail(&mut other_token).await.unwrap(), 18);
    assert_eq!(
        lane_error(&replica, 18).await,
        TransportCode::FailedPrecondition
    );
    send(&other, 18, &[b"?"]).await;
    wait_durable(&other, 19).await;
    other.finalize(&mut other_token, 19).await.unwrap();
    assert_eq!(
        other.snapshot().await.unwrap().bytes,
        b"durable+unflushed!?"
    );
}

async fn replace_appendable(backing: Backing) {
    let node = TestNode::start_with("replace", backing);
    let factory = connect(&node).await;
    let replica = factory.replica("seg");

    let small = Bytes::from_static(b"first version");
    let token = replica
        .replace_appendable(None, small.clone(), metadata())
        .await
        .unwrap();
    assert_eq!(token.persisted_size, small.len() as i64);
    let observed = replica.snapshot().await.unwrap();
    assert_eq!(observed.bytes, small);

    // Create-if-absent loses once the object exists.
    let error = replica
        .replace_appendable(None, small.clone(), metadata())
        .await
        .unwrap_err();
    assert_eq!(error.code, TransportCode::FailedPrecondition);

    // Larger than the inline limit: the rest streams through the session.
    let large = Bytes::from(pattern(10 * 1024 * 1024 + 123, 7));
    let mut token = replica
        .replace_appendable(Some(&observed), large.clone(), metadata())
        .await
        .unwrap();
    assert_eq!(token.persisted_size, large.len() as i64);
    let snapshot = replica.snapshot().await.unwrap();
    assert!(snapshot.generation > observed.generation);
    assert_eq!(snapshot.bytes.len(), large.len());
    assert!(snapshot.bytes == large);

    // The stale observation no longer matches.
    let error = replica
        .replace_appendable(Some(&observed), small, metadata())
        .await
        .unwrap_err();
    assert_eq!(error.code, TransportCode::FailedPrecondition);

    // The replacement session stays live and finalizes.
    let finalized = replica
        .finalize(&mut token, large.len() as i64)
        .await
        .unwrap();
    assert_eq!(finalized.crc32c, Some(crc32c::crc32c(&large)));
}

async fn large_object_reads_in_parts(backing: Backing) {
    let node = TestNode::start_with("large", backing);
    let factory = connect(&node).await;
    let replica = factory.replica("seg");
    let mut token = replica.create_append_session(metadata()).await.unwrap();
    let data = pattern(20 * 1024 * 1024 + 17, 3);
    for (index, chunk) in data.chunks(4 * 1024 * 1024).enumerate() {
        send(&replica, (index * 4 * 1024 * 1024) as i64, &[chunk]).await;
    }
    wait_durable(&replica, data.len() as i64).await;

    let snapshot = replica.snapshot().await.unwrap();
    assert_eq!(snapshot.persisted_size, data.len() as i64);
    assert!(snapshot.bytes == data);
    let range = replica.read_range(1000).await.unwrap();
    assert!(range.bytes == data[1000..]);

    replica
        .finalize(&mut token, data.len() as i64)
        .await
        .unwrap();
    assert!(replica.snapshot().await.unwrap().bytes == data);
}

async fn finalize_retries_are_idempotent(backing: Backing) {
    let node = TestNode::start_with("finalize", backing);
    let factory = connect(&node).await;
    let replica = factory.replica("seg");
    let mut token = replica.create_append_session(metadata()).await.unwrap();
    send(&replica, 0, &[b"payload"]).await;
    wait_durable(&replica, 7).await;
    let first = replica.finalize(&mut token, 7).await.unwrap();

    // Retry on the same replica (no live session any more: by handle).
    let again = replica.finalize(&mut token, 7).await.unwrap();
    assert_eq!(again, first);

    // Retry from a fresh client with the saved token.
    let other_factory = connect(&node).await;
    let mut saved: AppendToken = token.clone();
    let other = other_factory.replica("seg");
    assert_eq!(other.finalize(&mut saved, 7).await.unwrap(), first);

    // A different length is not a retry.
    let error = other.finalize(&mut saved, 6).await.unwrap_err();
    assert_eq!(error.code, TransportCode::FailedPrecondition);
}

async fn handle_free_finalize_after_takeover(backing: Backing) {
    let node = TestNode::start_with("handle-free", backing);
    let factory = connect(&node).await;
    let replica = factory.replica("seg");
    replica.create_append_session(metadata()).await.unwrap();
    send(&replica, 0, &[b"xyz"]).await;
    wait_durable(&replica, 3).await;
    // A recovering writer without a handle finalizes as a takeover.
    let observed = replica.snapshot().await.unwrap();
    let mut token = AppendToken {
        zone: 0,
        generation: Some(observed.generation),
        metageneration: Some(observed.metageneration),
        persisted_size: 3,
        write_handle: None,
    };
    let other_factory = connect(&node).await;
    let other = other_factory.replica("seg");
    let finalized = other.finalize(&mut token, 3).await.unwrap();
    assert!(finalized.finalized);
    assert_eq!(
        lane_error(&replica, 3).await,
        TransportCode::FailedPrecondition
    );
}

for_each_backing!(
    append_lifecycle,
    create_conflict,
    takeover_fences_the_old_writer,
    resume_after_a_dropped_connection,
    replace_appendable,
    large_object_reads_in_parts,
    finalize_retries_are_idempotent,
    handle_free_finalize_after_takeover,
);

/// The comparable part of a listing (`last_modified` of an open object moves
/// with every sync and is only persisted with metadata changes).
fn listing(objects: &[ListedObject]) -> Vec<(String, i64, bool, i64, Option<u32>)> {
    objects
        .iter()
        .map(|o| (o.name.clone(), o.generation, o.finalized, o.size, o.crc32c))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn disk_node_survives_a_restart() {
    let node = TestNode::start_with("restart", Backing::Disk);
    let factory = connect(&node).await;

    // An open object with a durable tail.
    let open = factory.replica("wal/open");
    let mut open_token = open.create_append_session(metadata()).await.unwrap();
    send(&open, 0, &[b"abc", b"def"]).await;
    wait_durable(&open, 6).await;

    // A finalized object.
    let done = factory.replica("wal/done");
    let mut done_token = done.create_append_session(metadata()).await.unwrap();
    send(&done, 0, &[b"xyz"]).await;
    wait_durable(&done, 3).await;
    let done_info = done.finalize(&mut done_token, 3).await.unwrap();

    // An object taken over by a second writer.
    let taken = factory.replica("wal/taken");
    let mut taken_token = taken.create_append_session(metadata()).await.unwrap();
    send(&taken, 0, &[b"12"]).await;
    wait_durable(&taken, 2).await;
    let other_factory = connect(&node).await;
    let observed = other_factory.replica("wal/taken").snapshot().await.unwrap();
    let mut takeover_token = other_factory
        .replica("wal/taken")
        .takeover(&observed)
        .await
        .unwrap();

    // A deleted object.
    let gone = factory.replica("wal/gone");
    gone.create_append_session(metadata()).await.unwrap();
    let gone_generation = gone.stat().await.unwrap().generation;
    gone.delete(gone_generation).await.unwrap();

    let before = factory.list("wal/").await.unwrap();
    assert_eq!(before.len(), 3);
    drop((factory, other_factory, open, done, taken, gone));

    let node = node.restart();
    let factory = connect(&node).await;
    assert_eq!(factory.node_id().as_deref(), Some("restart"));
    let after = factory.list("wal/").await.unwrap();
    assert_eq!(listing(&after), listing(&before));

    // The open object: same bytes, and the pre-restart handle resumes.
    let open = factory.replica("wal/open");
    let snapshot = open.snapshot().await.unwrap();
    assert_eq!(snapshot.bytes, b"abcdef");
    assert_eq!(snapshot.persisted_size, 6);
    assert!(!snapshot.finalized);
    assert_eq!(open.resume_tail(&mut open_token).await.unwrap(), 6);
    send(&open, 6, &[b"ghi"]).await;
    wait_durable(&open, 9).await;
    let finalized = open.finalize(&mut open_token, 9).await.unwrap();
    assert_eq!(finalized.crc32c, Some(crc32c::crc32c(b"abcdefghi")));

    // The finalized object: same bytes, finalize retries still succeed.
    let done = factory.replica("wal/done");
    assert_eq!(done.snapshot().await.unwrap().bytes, b"xyz");
    assert_eq!(done.finalize(&mut done_token, 3).await.unwrap(), done_info);

    // The taken-over object: only the takeover's handle resumes.
    let taken = factory.replica("wal/taken");
    let error = taken.resume_tail(&mut taken_token).await.unwrap_err();
    assert_eq!(error.code, TransportCode::FailedPrecondition);
    let taken = factory.replica("wal/taken");
    assert_eq!(taken.resume_tail(&mut takeover_token).await.unwrap(), 2);
    send(&taken, 2, &[b"3"]).await;
    wait_durable(&taken, 3).await;
    assert_eq!(taken.snapshot().await.unwrap().bytes, b"123");

    // The deleted object stays deleted; a new one gets a newer generation.
    let gone = factory.replica("wal/gone");
    assert_eq!(gone.stat().await.unwrap_err().code, TransportCode::NotFound);
    let token = gone.create_append_session(metadata()).await.unwrap();
    assert!(token.generation.unwrap() > gone_generation);
    assert!(token.generation.unwrap() > takeover_token.generation.unwrap());
}
