use super::*;
use async_trait::async_trait;
use slatedb::wal::{
    WalError, WalFileRange, WalGc, WalIterator, WalRows, WriterInit, WriterInitResult,
    WriterManifest,
};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

// These wrappers use only public WAL traits. Hold replay until SlateDB's real
// scheduler invokes GC, and record the callback result: run_gc_once() and the
// background scheduler otherwise log and swallow collector errors.
struct PausedInit {
    wal: ChorusWal,
    release: watch::Receiver<bool>,
}

#[async_trait]
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

#[async_trait]
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
enum GcEvent {
    Started,
    Finished(Result<(), WalError>),
}

struct CheckedGc {
    wal: ChorusWal,
    events: mpsc::UnboundedSender<GcEvent>,
}

#[async_trait]
impl WalGc for CheckedGc {
    async fn collect(
        &self,
        ranges: Vec<WalFileRange>,
        min_age: Duration,
        dry_run: bool,
    ) -> Result<(), WalError> {
        let _ = self.events.send(GcEvent::Started);
        let result = self.wal.collect(ranges, min_age, dry_run).await;
        let _ = self.events.send(GcEvent::Finished(result.clone()));
        result
    }
}

async fn start_paused(
    fixture: &Fixture,
) -> (
    JoinHandle<Result<Db, slatedb::Error>>,
    watch::Sender<bool>,
    mpsc::UnboundedReceiver<GcEvent>,
) {
    let wal = fixture.wal().await;
    let store = fixture.store.clone();
    let (release, gate) = watch::channel(false);
    let (events, receiver) = mpsc::unbounded_channel();
    let build = tokio::spawn(async move {
        let gc = GarbageCollectorBuilder::new("db", store.clone())
            .with_wal_gc(Arc::new(CheckedGc {
                wal: wal.clone(),
                events,
            }))
            .with_options(GarbageCollectorOptions {
                manifest_options: None,
                wal_options: Some(GarbageCollectorDirectoryOptions {
                    // The first tick is immediate; the next is ten minutes
                    // later. The test must succeed on that first callback.
                    interval: None,
                    min_age: Duration::ZERO,
                    dry_run: false,
                }),
                wal_fence_options: None,
                compacted_options: None,
                compactions_options: None,
                detach_options: None,
                ..GarbageCollectorOptions::default()
            });
        Db::builder("db", store)
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
    (build, release, receiver)
}

async fn event(events: &mut mpsc::UnboundedReceiver<GcEvent>) -> GcEvent {
    tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("GC callback did not make progress")
        .expect("GC callback event stream closed")
}

#[tokio::test]
async fn scheduled_gc_waits_for_startup_then_reclaims_on_its_first_tick() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let fixture = Fixture::new().await;
        let (seed, _) = fixture.open().await;
        let mut model = Model::new();
        let mut random = 7;
        write_batches(&seed, &mut model, &mut random, 12).await;
        flush_l0(&seed).await;
        write_batches(&seed, &mut model, &mut random, 12).await;
        close(&seed).await;
        let before = fixture.manifest().await;
        assert_eq!(before["chorus.trunc"], "0");

        let (build, release, mut events) = start_paused(&fixture).await;
        assert!(matches!(event(&mut events).await, GcEvent::Started));
        assert!(!build.is_finished());
        assert!(
            tokio::time::timeout(Duration::from_millis(30), events.recv())
                .await
                .is_err()
        );
        assert_eq!(fixture.manifest().await["chorus.trunc"], "0");

        release.send_replace(true);
        let db = build.await.unwrap().unwrap();
        match event(&mut events).await {
            GcEvent::Finished(result) => result.unwrap(),
            event => panic!("expected completion of the first GC call, got {event:?}"),
        }
        let floor = fixture.manifest().await["chorus.trunc"]
            .parse::<u64>()
            .unwrap();
        assert!(floor > 0 && floor <= 12, "unexpected GC floor: {floor}");
        let deleted = collected_objects(&before, floor);
        assert!(!deleted.is_empty());
        for zone in 0..3 {
            assert!(deleted.is_disjoint(&fixture.segments(zone).await));
        }
        // Both the SST prefix and replayed WAL suffix remain readable, and the
        // writer is still usable after its first automatic collection.
        verify(&db, &model).await;
        write_batches(&db, &mut model, &mut random, 4).await;
        verify(&db, &model).await;
        close(&db).await;
        assert!(events.try_recv().is_err());
    })
    .await
    .expect("startup GC test or database shutdown hung");
}

#[tokio::test]
async fn cancelled_db_build_releases_the_waiting_scheduled_gc_callback() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let fixture = Fixture::new().await;
        let (build, _release, mut events) = start_paused(&fixture).await;
        assert!(matches!(event(&mut events).await, GcEvent::Started));
        assert!(
            tokio::time::timeout(Duration::from_millis(30), events.recv())
                .await
                .is_err()
        );
        build.abort();
        assert!(build.await.err().unwrap().is_cancelled());
        assert!(matches!(
            event(&mut events).await,
            GcEvent::Finished(Err(WalError::Closed))
        ));
        assert_eq!(fixture.manifest().await["chorus.trunc"], "0");
        // A fresh startup remains independent of the cancelled attempt.
        let (db, _) = fixture.open().await;
        close(&db).await;
    })
    .await
    .expect("cancelled startup left GC or database shutdown hanging");
}
