//! Destructive only within the explicitly supplied fresh experiment prefix.
//! Run on a GCE VM using its service-account ADC; no static credentials needed.
mod startup_gc;

use std::{
    collections::{BTreeMap, BTreeSet},
    process::Stdio,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use chorus_client::{
    slatedb::ChorusWal, BearerAuth, ClientConfig, GrpcReplicaFactory, ReadOnlyConfig,
    RefreshingAuthConfig, SegmentedVolume, WalEngineConfig,
};
use futures::TryStreamExt;
use google_cloud_auth::credentials::{AccessTokenCredentials, Builder};
use serde_json::{json, Value};
use slatedb::{
    admin::{Admin, AdminBuilder},
    config::{
        CheckpointOptions, CloseOptions, DbReaderOptions, FlushOptions, FlushType,
        GarbageCollectorDirectoryOptions, GarbageCollectorOptions, Settings,
    },
    object_store::{gcp::GoogleCloudStorageBuilder, path::Path, ObjectStore},
    wal::{WalError, WalFileRange, WalGc, WalReader, WalRows},
    Db, DbReader, DbReaderMode, GarbageCollectorBuilder, ValueDeletable, WriteBatch,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{mpsc, watch},
};

const BUCKETS: [&str; 4] = [
    "subspace-dev-rapid-zonal-1",
    "subspace-dev-rapid-zonal-2",
    "subspace-dev-rapid-zonal-3",
    "subspace-dev-regional",
];
const TIMEOUT: Duration = Duration::from_secs(120);
const KEYS: u64 = 63;
type Model = BTreeMap<Bytes, Bytes>;
static GC_FAILURES: AtomicU64 = AtomicU64::new(0);

struct CheckedGc {
    wal: ChorusWal,
    results: Option<mpsc::UnboundedSender<Result<(), WalError>>>,
}

#[async_trait::async_trait]
impl WalGc for CheckedGc {
    async fn collect(
        &self,
        ranges: Vec<WalFileRange>,
        min_age: Duration,
        dry_run: bool,
    ) -> Result<(), WalError> {
        emit(
            json!({"event":"gc_collect_begin", "ranges":format!("{ranges:?}"),
            "min_age_secs":min_age.as_secs_f64(), "dry_run":dry_run}),
        );
        let result = self.wal.collect(ranges, min_age, dry_run).await;
        if result.is_err() {
            GC_FAILURES.fetch_add(1, Ordering::SeqCst);
        }
        emit(json!({"event":"gc_collect_end", "ok":result.is_ok(),
            "error":result.as_ref().err().map(ToString::to_string)}));
        if let Some(results) = &self.results {
            let _ = results.send(result.clone());
        }
        result
    }
}

struct Fixture {
    root: String,
    db_path: String,
    stores: Vec<Arc<dyn ObjectStore>>,
    credentials: AccessTokenCredentials,
    http: reqwest::Client,
}

impl Fixture {
    async fn new(root: String) -> Result<Self> {
        ensure!(
            root.starts_with("chorus-experiments/slatedb-readers-20260904-")
                && root.ends_with('/')
                && !root.contains(".."),
            "invalid scratch prefix"
        );
        let stores = BUCKETS
            .iter()
            .map(|bucket| {
                Ok(Arc::new(
                    GoogleCloudStorageBuilder::new()
                        .with_bucket_name(*bucket)
                        .build()?,
                ) as Arc<dyn ObjectStore>)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            db_path: format!("{root}slatedb"),
            root,
            stores,
            credentials: Builder::default()
                .with_scopes(["https://www.googleapis.com/auth/devstorage.read_write"])
                .build_access_token_credentials()?,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()?,
        })
    }

    async fn wal(&self) -> Result<ChorusWal> {
        let auth = BearerAuth::google_adc(RefreshingAuthConfig::default()).await?;
        let mut factories = Vec::new();
        for (zone, bucket) in BUCKETS.iter().enumerate() {
            factories.push(
                GrpcReplicaFactory::connect_with_auth(
                    zone,
                    "https://storage.googleapis.com",
                    format!("projects/_/buckets/{bucket}"),
                    auth.clone(),
                )
                .await?,
            );
        }
        let regional = factories.pop().context("regional factory")?;
        let volume = SegmentedVolume::new(
            factories,
            regional,
            format!("{}wal", self.root),
            ClientConfig::default(),
        )?;
        Ok(ChorusWal::with_config(
            volume,
            WalEngineConfig {
                max_segment_bytes: 512 * 1024,
                repair_interval: None,
                shutdown_timeout: Duration::from_secs(30),
                ..WalEngineConfig::default()
            },
        )
        .with_reader_config(ReadOnlyConfig {
            poll_interval: Duration::from_millis(100),
            manifest_poll_interval: Duration::from_millis(250),
        }))
    }

    fn gc(&self, wal: ChorusWal) -> GarbageCollectorBuilder<String> {
        self.observed_gc(wal, None)
    }

    fn observed_gc(
        &self,
        wal: ChorusWal,
        results: Option<mpsc::UnboundedSender<Result<(), WalError>>>,
    ) -> GarbageCollectorBuilder<String> {
        GarbageCollectorBuilder::new(self.db_path.clone(), self.stores[3].clone())
            .with_wal_gc(Arc::new(CheckedGc { wal, results }))
            .with_options(GarbageCollectorOptions {
                manifest_options: None,
                wal_options: Some(GarbageCollectorDirectoryOptions {
                    interval: None,
                    min_age: Duration::ZERO,
                    dry_run: false,
                }),
                wal_fence_options: None,
                compacted_options: None,
                compactions_options: None,
                detach_options: None,
                ..GarbageCollectorOptions::default()
            })
    }

    async fn open(&self) -> Result<(Db, ChorusWal)> {
        let wal = self.wal().await?;
        let (results, mut receiver) = mpsc::unbounded_channel();
        let gc = self.observed_gc(wal.clone(), Some(results));
        let db = Db::builder(self.db_path.clone(), self.stores[3].clone())
            .with_settings(Settings {
                flush_interval: None,
                compactor_options: None,
                garbage_collector_options: None,
                l0_max_ssts: 64,
                l0_max_ssts_per_key: 64,
                ..Settings::default()
            })
            .with_wal_writer(Box::new(wal.clone()))
            .with_gc_builder(gc)
            .build()
            .await?;
        // Verify the first scheduled callback, rather than mistaking a logged
        // and swallowed GC failure for a successful database open. Also finish
        // that callback before a test intentionally closes/fences this writer.
        tokio::time::timeout(TIMEOUT, receiver.recv())
            .await
            .context("first automatic GC timed out")?
            .context("automatic GC result channel closed")??;
        emit(json!({"event":"automatic_gc_pass", "root":self.root}));
        Ok((db, wal))
    }

    async fn reader(&self, mode: DbReaderMode) -> Result<DbReader> {
        Ok(
            DbReader::builder(self.db_path.clone(), self.stores[3].clone())
                .with_reader_mode(mode)
                .with_wal_reader(Arc::new(self.wal().await?))
                .with_options(DbReaderOptions {
                    manifest_poll_interval: Duration::from_millis(250),
                    ..DbReaderOptions::default()
                })
                .build()
                .await?,
        )
    }

    fn admin(&self) -> Admin {
        AdminBuilder::new(self.db_path.clone(), self.stores[3].clone()).build()
    }

    async fn metadata(&self) -> Result<Value> {
        let mut url = reqwest::Url::parse("https://storage.googleapis.com/storage/v1/b/")?;
        url.path_segments_mut()
            .unwrap()
            .pop_if_empty()
            .push(BUCKETS[3])
            .push("o")
            .push(&format!("{}wal/manifest", self.root));
        let token = self.credentials.access_token().await?;
        let body: Value = self
            .http
            .get(url)
            .bearer_auth(token.token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(body["metadata"].clone())
    }

    async fn objects(&self, zone: usize, suffix: &str) -> Result<BTreeSet<String>> {
        let prefix = Path::from(format!("{}{suffix}", self.root));
        Ok(self.stores[zone]
            .list(Some(&prefix))
            .map_ok(|o| o.location.to_string())
            .try_collect()
            .await?)
    }
}

fn key(n: u64) -> Bytes {
    Bytes::from(format!("key/{n:03}"))
}
fn changes(n: u64) -> Vec<(Bytes, ValueDeletable)> {
    let mut value = vec![n as u8; 16 * 1024];
    value[..8].copy_from_slice(&n.to_be_bytes());
    vec![
        (key(n % KEYS), ValueDeletable::Value(value.into())),
        (key((n + 17) % KEYS), ValueDeletable::Tombstone),
        (
            Bytes::from_static(b"last-batch"),
            ValueDeletable::Value(Bytes::copy_from_slice(&n.to_be_bytes())),
        ),
    ]
}
fn model(n: u64) -> Model {
    let mut out = Model::new();
    for n in 1..=n {
        for (key, value) in changes(n) {
            match value {
                ValueDeletable::Value(value) => {
                    out.insert(key, value);
                }
                ValueDeletable::Tombstone => {
                    out.remove(&key);
                }
                _ => unreachable!(),
            }
        }
    }
    out
}
async fn write(db: &Db, start: u64, end: u64) -> Result<()> {
    for n in start..=end {
        let mut batch = WriteBatch::new();
        for (key, value) in changes(n) {
            match value {
                ValueDeletable::Value(value) => batch.put(key, value),
                ValueDeletable::Tombstone => batch.delete(key),
                _ => unreachable!(),
            }
        }
        db.write(batch).await?.await_durable().await?;
    }
    Ok(())
}
fn check_rows(batch: WalRows, n: u64, previous_seq: u64) -> Result<u64> {
    ensure!(
        batch.last_consumed_wal_file_id == n,
        "WAL gap: expected {n}, got {}",
        batch.last_consumed_wal_file_id
    );
    ensure!(batch.rows.len() == 3, "partial atomic batch {n}");
    let seq = batch.rows[0].seq;
    ensure!(
        seq > previous_seq && batch.rows.iter().all(|row| row.seq == seq),
        "invalid batch sequence {n}"
    );
    let actual: BTreeMap<_, _> = batch.rows.into_iter().map(|r| (r.key, r.value)).collect();
    let expected: BTreeMap<_, _> = changes(n).into_iter().collect();
    ensure!(actual == expected, "WAL contents differ in batch {n}");
    Ok(seq)
}

async fn verify_reader(reader: &DbReader, n: u64) -> Result<()> {
    let expected = model(n);
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let mut scan = reader.scan(..).await?;
            let mut actual = Model::new();
            while let Some(row) = scan.next().await? {
                actual.insert(row.key, row.value);
            }
            if actual == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        for index in 0..KEYS {
            ensure!(
                reader.get(key(index)).await? == expected.get(&key(index)).cloned(),
                "point read {index} at batch {n}"
            );
        }
        let mut scan = reader.scan(key(20)..key(50)).await?;
        let mut actual = Model::new();
        while let Some(row) = scan.next().await? {
            actual.insert(row.key, row.value);
        }
        ensure!(
            actual
                == expected
                    .range(key(20)..key(50))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            "range scan mismatch"
        );
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("reader catch-up timeout")??;
    Ok(())
}
async fn verify_db(db: &Db, n: u64) -> Result<()> {
    let mut scan = db.scan(..).await?;
    let mut actual = Model::new();
    while let Some(row) = scan.next().await? {
        actual.insert(row.key, row.value);
    }
    ensure!(actual == model(n), "writer recovery differs at {n}");
    Ok(())
}
async fn close(db: &Db) -> Result<()> {
    db.close_with_options(CloseOptions::default().with_flush_type(Some(FlushType::Wal)))
        .await?;
    Ok(())
}
async fn flush(db: &Db) -> Result<()> {
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await?;
    Ok(())
}
fn emit(value: Value) {
    println!("{value}");
}

async fn child(f: Fixture) -> Result<()> {
    let wal = f.wal().await?;
    let mut raw = wal.iterator((1..).into()).await?;
    ensure!(
        tokio::time::timeout(Duration::from_millis(150), raw.next())
            .await
            .is_err(),
        "empty tail did not wait"
    );
    let latest = f.reader(DbReaderMode::FollowLatest).await?;
    let managed = f.reader(DbReaderMode::ManagedCheckpoint).await?;
    verify_reader(&latest, 0).await?;
    verify_reader(&managed, 0).await?;
    let (tx, mut rx) = watch::channel(Ok::<u64, String>(0));
    let raw_task = tokio::spawn(async move {
        let result: Result<()> = async {
            let mut n = 0;
            let mut seq = 0;
            loop {
                let rows = raw.next().await?.context("unbounded reader returned EOF")?;
                n += 1;
                seq = check_rows(rows, n, seq)?;
                let _ = tx.send_replace(Ok(n));
            }
        }
        .await;
        if let Err(error) = result {
            let _ = tx.send_replace(Err(format!("{error:#}")));
        }
    });
    emit(json!({"event":"ready", "pid": std::process::id()}));
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        let cmd: Value = serde_json::from_str(&line)?;
        if cmd["command"] == "stop" {
            break;
        }
        let n = cmd["batch"].as_u64().context("batch")?;
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let count = rx.borrow_and_update().clone().map_err(anyhow::Error::msg)?;
                ensure!(count <= n, "raw stream read unexpected batch {count}");
                if count == n {
                    return Ok::<_, anyhow::Error>(());
                }
                rx.changed().await?;
            }
        })
        .await
        .context("raw reader catch-up timeout")??;
        verify_reader(&latest, n).await?;
        verify_reader(&managed, n).await?;
        emit(json!({"event":"verified", "batch":n, "raw_rows":3*n, "readers":2}));
    }
    raw_task.abort();
    let _ = raw_task.await;
    latest.close().await?;
    managed.close().await?;
    emit(json!({"event":"stopped"}));
    Ok(())
}

struct ReaderProcess {
    child: Child,
    stdin: ChildStdin,
    lines: Lines<BufReader<ChildStdout>>,
}
impl ReaderProcess {
    async fn start(root: &str) -> Result<Self> {
        let mut child = Command::new(std::env::current_exe()?)
            .args(["reader", root])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let stdin = child.stdin.take().context("child stdin")?;
        let lines = BufReader::new(child.stdout.take().context("child stdout")?).lines();
        let mut out = Self {
            child,
            stdin,
            lines,
        };
        let ready = out.receive().await?;
        ensure!(ready["event"] == "ready", "child not ready: {ready}");
        Ok(out)
    }
    async fn receive(&mut self) -> Result<Value> {
        let line = tokio::time::timeout(TIMEOUT, self.lines.next_line())
            .await??
            .context("reader process exited")?;
        let value: Value = serde_json::from_str(&line)?;
        emit(json!({"event":"reader_process", "result":value}));
        Ok(value)
    }
    async fn send(&mut self, cmd: Value) -> Result<()> {
        self.stdin.write_all(format!("{cmd}\n").as_bytes()).await?;
        self.stdin.flush().await?;
        Ok(())
    }
    async fn verify(&mut self, n: u64) -> Result<()> {
        self.send(json!({"command":"verify", "batch":n})).await?;
        let result = self.receive().await?;
        ensure!(
            result["event"] == "verified" && result["batch"] == n,
            "reader result {result}"
        );
        Ok(())
    }
    async fn stop(mut self) -> Result<()> {
        self.send(json!({"command":"stop"})).await?;
        ensure!(
            self.receive().await?["event"] == "stopped",
            "reader shutdown"
        );
        ensure!(
            tokio::time::timeout(TIMEOUT, self.child.wait())
                .await??
                .success(),
            "reader process failed"
        );
        Ok(())
    }
}

async fn run(f: Fixture) -> Result<()> {
    let started = Instant::now();
    for zone in 0..4 {
        ensure!(
            f.objects(zone, "").await?.is_empty(),
            "scratch namespace already exists in zone {zone}"
        );
    }
    emit(
        json!({"event":"start", "root":f.root, "pid":std::process::id(), "segment_bytes":512*1024}),
    );
    let (first, _) = f.open().await?;
    let epoch = f.metadata().await?["chorus.epoch"].clone();
    let mut readers = ReaderProcess::start(&f.root).await?;
    ensure!(
        f.metadata().await?["chorus.epoch"] == epoch,
        "reader fenced writer"
    );
    let stale_wal = f.wal().await?;
    let mut stale = stale_wal.iterator((1..).into()).await?;
    write(&first, 1, 1).await?;
    readers.verify(1).await?;
    ensure!(
        f.metadata().await?["chorus.tail_base"] == "0",
        "first write unexpectedly sealed"
    );
    ensure!(
        f.admin()
            .read_manifest(None)
            .await?
            .context("manifest")?
            .l0()
            .is_empty(),
        "first write was flushed to L0"
    );
    emit(json!({"event":"active_tail_and_nonfencing_pass"}));
    write(&first, 2, 32).await?;
    readers.verify(32).await?;
    close(&first).await?;
    let admin = f.admin();
    let cp = admin
        .create_detached_checkpoint(&CheckpointOptions::default())
        .await?;
    let manifest = admin
        .read_manifest(Some(cp.manifest_id))
        .await?
        .context("checkpoint manifest")?;
    ensure!(
        manifest.replay_after_wal_id() == 0
            && manifest.next_wal_sst_id() == 33
            && manifest.l0().is_empty(),
        "checkpoint must depend on exactly 32 WAL-only batches: replay={} next={} l0={}",
        manifest.replay_after_wal_id(),
        manifest.next_wal_sst_id(),
        manifest.l0().len()
    );
    emit(
        json!({"event":"wal_only_checkpoint", "checkpoint":cp.id.to_string(), "manifest":cp.manifest_id,
        "replay_after":manifest.replay_after_wal_id(), "next_wal":manifest.next_wal_sst_id()}),
    );
    let pinned = f.reader(DbReaderMode::Checkpoint(cp.id)).await?;
    verify_reader(&pinned, 32).await?;
    let (old, old_wal) = f.open().await?;
    verify_db(&old, 32).await?;
    write(&old, 33, 96).await?;
    readers.verify(96).await?;
    // Bounded iteration terminates at the exact requested endpoint, with no extra append.
    let mut bounded = f.wal().await?.iterator((1..97).into()).await?;
    let mut seq = 0;
    for n in 1..=96 {
        seq = check_rows(bounded.next().await?.context("early EOF")?, n, seq)?;
    }
    ensure!(
        bounded.next().await?.is_none(),
        "bounded iterator overran endpoint"
    );
    ensure!(
        matches!(
            f.wal().await?.iterator((1..98).into()).await,
            Err(WalError::Unavailable(_))
        ),
        "future finite endpoint accepted"
    );
    flush(&old).await?;
    f.gc(old_wal).build().run_gc_once().await;
    ensure!(
        GC_FAILURES.load(Ordering::SeqCst) == 0,
        "checkpoint GC callback failed"
    );
    ensure!(
        f.metadata().await?["chorus.trunc"] == "0",
        "GC crossed checkpoint's WAL retention"
    );
    verify_reader(&pinned, 32).await?;
    let reopened_checkpoint = f.reader(DbReaderMode::Checkpoint(cp.id)).await?;
    verify_reader(&reopened_checkpoint, 32).await?;
    reopened_checkpoint.close().await?;
    emit(json!({"event":"bounded_and_checkpoint_gc_pass", "bounded_batches":96}));
    // Take over while the old writer remains open; all readers remain running.
    let (new, new_wal) = f.open().await?;
    verify_db(&new, 96).await?;
    let stale_write = match old.put(b"stale-writer", b"must-not-commit").await {
        Ok(handle) => handle.await_durable().await,
        Err(error) => Err(error),
    };
    ensure!(stale_write.is_err(), "fenced writer acknowledged a write");
    let _ = old
        .close_with_options(CloseOptions::default().with_flush_type(None))
        .await;
    write(&new, 97, 160).await?;
    readers.verify(160).await?;
    emit(
        json!({"event":"takeover_pass", "stale_write_error":format!("{}", stale_write.unwrap_err())}),
    );
    let mut before = Vec::new();
    for zone in 0..3 {
        before.push(f.objects(zone, "wal/segments/").await?);
    }
    pinned.close().await?;
    admin.delete_checkpoint(cp.id).await?;
    flush(&new).await?;
    let gc = f.gc(new_wal).build();
    let floor = tokio::time::timeout(TIMEOUT, async {
        loop {
            gc.run_gc_once().await;
            ensure!(
                GC_FAILURES.load(Ordering::SeqCst) == 0,
                "reclamation GC callback failed"
            );
            let metadata = f.metadata().await?;
            let floor: u64 = metadata["chorus.trunc"]
                .as_str()
                .context("GC floor")?
                .parse()?;
            if floor > 32 {
                return Ok::<_, anyhow::Error>(floor);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .context("GC did not advance after checkpoint release")??;
    let mut deleted = Vec::new();
    for (zone, objects) in before.iter().enumerate() {
        let after = f.objects(zone, "wal/segments/").await?;
        let removed: Vec<_> = objects.difference(&after).cloned().collect();
        ensure!(
            !removed.is_empty(),
            "GC removed no physical segments from zone {zone}"
        );
        deleted.push(removed);
    }
    ensure!(
        matches!(stale.next().await, Err(WalError::WalTruncated(1))),
        "lagged reader did not report truncation"
    );
    ensure!(
        matches!(stale.next().await, Err(WalError::WalTruncated(1))),
        "truncation error was not latched"
    );
    ensure!(
        matches!(
            f.wal().await?.iterator((1..).into()).await,
            Err(WalError::WalTruncated(1))
        ),
        "fresh stale reader accepted reclaimed prefix"
    );
    emit(
        json!({"event":"physical_gc_and_lag_detection_pass", "floor":floor, "deleted_objects":deleted}),
    );
    write(&new, 161, 224).await?;
    readers.verify(224).await?;
    let fresh = f.reader(DbReaderMode::FollowLatest).await?;
    verify_reader(&fresh, 224).await?;
    fresh.close().await?;
    readers.stop().await?;
    close(&new).await?;
    let (recovered, _) = f.open().await?;
    verify_db(&recovered, 224).await?;
    ensure!(
        recovered.get(b"stale-writer").await?.is_none(),
        "fenced write appeared after recovery"
    );
    close(&recovered).await?;
    ensure!(
        GC_FAILURES.load(Ordering::SeqCst) == 0,
        "GC callback failed"
    );
    emit(
        json!({"event":"pass", "root":f.root, "batches":224, "atomic_rows":672,
        "bounded_batches":96, "seconds":started.elapsed().as_secs_f64(), "final_metadata":f.metadata().await?}),
    );
    Ok(())
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args: Vec<_> = std::env::args().collect();
    ensure!(
        args.len() == 3 && ["run", "reader", "startup-gc"].contains(&args[1].as_str()),
        "usage: chorus-slatedb-live run|reader|startup-gc PREFIX/"
    );
    let f = Fixture::new(args[2].clone()).await?;
    if args[1] == "reader" {
        child(f).await
    } else if args[1] == "startup-gc" {
        tokio::time::timeout(Duration::from_secs(900), startup_gc::run(f))
            .await
            .context("startup GC suite timeout")?
    } else {
        tokio::time::timeout(Duration::from_secs(900), run(f))
            .await
            .context("live suite timeout")?
    }
}
