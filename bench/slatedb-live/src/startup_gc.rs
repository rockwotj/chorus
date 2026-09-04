use super::*;
use slatedb::wal::{WalIterator, WriterInit, WriterInitResult, WriterManifest};
use tokio::task::JoinHandle;

struct PausedInit {
    wal: ChorusWal,
    release: watch::Receiver<bool>,
}

#[async_trait::async_trait]
impl WriterInit for PausedInit {
    async fn fence_and_init(
        &self,
        manifest: &mut WriterManifest,
    ) -> Result<WriterInitResult, WalError> {
        let mut result = self.wal.fence_and_init(manifest).await?;
        result.replay_iterator = Box::new(PausedReplay {
            inner: result.replay_iterator,
            release: self.release.clone(),
        });
        Ok(result)
    }
}

struct PausedReplay {
    inner: Box<dyn WalIterator>,
    release: watch::Receiver<bool>,
}

#[async_trait::async_trait]
impl WalIterator for PausedReplay {
    async fn next(&mut self) -> Result<Option<WalRows>, WalError> {
        self.release
            .wait_for(|released| *released)
            .await
            .map_err(|_| WalError::Closed)?;
        self.inner.next().await
    }
}

#[derive(Debug)]
enum Event {
    Started,
    Finished(Result<(), WalError>),
}

struct ObservedGc {
    wal: ChorusWal,
    label: &'static str,
    events: mpsc::UnboundedSender<Event>,
}

#[async_trait::async_trait]
impl WalGc for ObservedGc {
    async fn collect(
        &self,
        ranges: Vec<WalFileRange>,
        min_age: Duration,
        dry_run: bool,
    ) -> Result<(), WalError> {
        emit(
            json!({"event":"startup_gc_collect_begin", "case":self.label,
            "ranges":format!("{ranges:?}")}),
        );
        let _ = self.events.send(Event::Started);
        let result = self.wal.collect(ranges, min_age, dry_run).await;
        emit(json!({"event":"startup_gc_collect_end", "case":self.label,
            "ok":result.is_ok(), "error":result.as_ref().err().map(ToString::to_string)}));
        let _ = self.events.send(Event::Finished(result.clone()));
        result
    }
}

async fn start_paused(
    fixture: &Fixture,
    label: &'static str,
) -> Result<(
    JoinHandle<Result<Db, slatedb::Error>>,
    watch::Sender<bool>,
    mpsc::UnboundedReceiver<Event>,
)> {
    let wal = fixture.wal().await?;
    let store = fixture.stores[3].clone();
    let path = fixture.db_path.clone();
    let (release, gate) = watch::channel(false);
    let (events, receiver) = mpsc::unbounded_channel();
    let gc = fixture.gc(wal.clone()).with_wal_gc(Arc::new(ObservedGc {
        wal: wal.clone(),
        label,
        events,
    }));
    let build = tokio::spawn(async move {
        Db::builder(path, store)
            .with_settings(Settings {
                flush_interval: None,
                compactor_options: None,
                garbage_collector_options: None,
                ..Settings::default()
            })
            .with_wal_writer(Box::new(PausedInit { wal, release: gate }))
            .with_gc_builder(gc)
            .build()
            .await
    });
    Ok((build, release, receiver))
}

async fn event(events: &mut mpsc::UnboundedReceiver<Event>) -> Result<Event> {
    tokio::time::timeout(TIMEOUT, events.recv())
        .await
        .context("GC callback did not make progress")?
        .context("GC event stream closed")
}

async fn confirm_waiting(events: &mut mpsc::UnboundedReceiver<Event>) -> Result<()> {
    ensure!(
        matches!(event(events).await?, Event::Started),
        "GC did not start"
    );
    ensure!(
        tokio::time::timeout(Duration::from_millis(250), events.recv())
            .await
            .is_err(),
        "GC completed while replay was paused"
    );
    Ok(())
}

pub(super) async fn run(f: Fixture) -> Result<()> {
    for zone in 0..4 {
        ensure!(
            f.objects(zone, "").await?.is_empty(),
            "scratch prefix exists"
        );
    }
    let started = Instant::now();
    emit(json!({"event":"startup_suite_start", "root":f.root}));
    let (seed, _) = f.open().await?;
    write(&seed, 1, 64).await?;
    flush(&seed).await?;
    write(&seed, 65, 96).await?;
    close(&seed).await?;
    ensure!(
        f.metadata().await?["chorus.trunc"] == "0",
        "seed was collected"
    );
    let mut before = Vec::new();
    for zone in 0..3 {
        before.push(f.objects(zone, "wal/segments/").await?);
    }

    let (build, release, mut events) = start_paused(&f, "replay-success").await?;
    confirm_waiting(&mut events).await?;
    ensure!(!build.is_finished(), "build did not wait for replay");
    ensure!(
        f.metadata().await?["chorus.trunc"] == "0",
        "GC deleted during replay"
    );
    for (zone, objects) in before.iter().enumerate() {
        ensure!(
            objects.is_subset(&f.objects(zone, "wal/segments/").await?),
            "objects disappeared in zone {zone} while replay was paused"
        );
    }
    emit(json!({"event":"gc_waiting_for_replay_pass"}));
    release.send_replace(true);
    let db = tokio::time::timeout(TIMEOUT, build).await???;
    match event(&mut events).await? {
        Event::Finished(result) => result?,
        other => anyhow::bail!("expected first GC completion: {other:?}"),
    }
    let metadata = f.metadata().await?;
    let floor: u64 = metadata["chorus.trunc"]
        .as_str()
        .context("floor")?
        .parse()?;
    ensure!(floor > 0 && floor <= 64, "unexpected floor {floor}");
    let mut deleted = Vec::new();
    for (zone, objects) in before.iter().enumerate() {
        let after = f.objects(zone, "wal/segments/").await?;
        let removed: Vec<_> = objects.difference(&after).cloned().collect();
        ensure!(!removed.is_empty(), "no physical GC in zone {zone}");
        deleted.push(removed);
    }
    verify_db(&db, 96).await?;
    write(&db, 97, 128).await?;
    verify_db(&db, 128).await?;
    close(&db).await?;
    ensure!(events.try_recv().is_err(), "unexpected extra GC invocation");
    emit(json!({"event":"first_tick_reclamation_pass", "floor":floor,
        "deleted_objects":deleted, "verified_batches":128}));

    let cancelled = Fixture::new(format!("{}cancel/", f.root)).await?;
    let (build, _release, mut events) = start_paused(&cancelled, "cancelled-build").await?;
    confirm_waiting(&mut events).await?;
    build.abort();
    let error = tokio::time::timeout(TIMEOUT, build)
        .await?
        .err()
        .context("build not cancelled")?;
    ensure!(error.is_cancelled(), "unexpected build failure {error}");
    ensure!(
        matches!(
            event(&mut events).await?,
            Event::Finished(Err(WalError::Closed))
        ),
        "GC did not return Closed after cancellation"
    );
    ensure!(
        cancelled.metadata().await?["chorus.trunc"] == "0",
        "cancelled GC deleted"
    );
    let (db, _) = cancelled.open().await?;
    verify_db(&db, 0).await?;
    close(&db).await?;
    emit(json!({"event":"cancelled_build_gc_release_pass"}));
    ensure!(
        GC_FAILURES.load(Ordering::SeqCst) == 0,
        "unexpected GC failure"
    );
    emit(json!({"event":"startup_suite_pass", "root":f.root,
        "seconds":started.elapsed().as_secs_f64(), "expected_cancelled_gc_errors":1}));
    Ok(())
}
