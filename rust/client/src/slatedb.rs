//! SlateDB WAL integration, enabled by the opt-in `slatedb` feature.
//!
//! Pass [`ChorusWal`] to `Db::builder(...).with_wal_writer(Box::new(wal))`.
//! Use one dedicated Chorus volume per SlateDB database, and supply the same
//! volume on every open. Existing native SlateDB WALs cannot be migrated by
//! simply changing the builder option.
//!
//! Each SlateDB batch is one versioned Chorus record. Chorus record index `n`
//! maps to SlateDB WAL file ID `n + 1` (ID zero means no WAL). Thus SlateDB's
//! exclusive `replay_after_wal_id` is exactly Chorus's inclusive replay index.
//! Admission is pipelined; only ordered quorum completions publish durability.
//! Chorus owns WAL admission backpressure. `WalStatus::estimated_bytes` is zero
//! to opt out of SlateDB's WAL-buffer accounting. Durable progress is coalesced
//! by WAL ID; there is no separately bounded adapter completion queue.
//!
//! This implements writer initialization, bounded streaming recovery, writes,
//! flush barriers, observation, close, and `WalGc`. Wire a clone of the same
//! initializer into SlateDB's GC builder as shown below: collection runs through
//! the live writer's maintenance task, preserving all supplied checkpoint ranges.
//! Only whole sealed segments from the unreferenced prefix are reclaimed.
//! Nonzero `min_age` requires every existing replica's storage modification time
//! to be strictly older than the cutoff. Unknown ages defer new truncation;
//! zero disables the age gate. Dry runs do not change storage. An open writer
//! is required; offline or separate-process GC is not supported.
//! `WalReader` uses non-coordinating Chorus reads, including the quorum-visible
//! active tail. Pass a separately constructed initializer for the same volume
//! to `DbReader::builder(...).with_wal_reader(Arc::new(wal))`. Reader opens never
//! fence the writer and do not require the writer's process-local GC connection.
//! Direct WAL iterators do not register retention; a follower overtaken by GC
//! returns `WalError::WalTruncated`. `WalAdmin` and WAL-based clones remain
//! unsupported.
//! Do not run Chorus recovery/maintenance tools concurrently with the database:
//! those tools claim a new writer epoch and fence the database.
//!
//! The adapter targets the SlateDB 0.16 pluggable-WAL traits.
//!
//! # Usage
//!
//! ```no_run
//! use chorus_client::{SegmentedVolume, slatedb::ChorusWal};
//! use slatedb::{Db, GarbageCollectorBuilder, object_store::ObjectStore};
//! use std::sync::Arc;
//!
//! async fn open_database(
//!     volume: SegmentedVolume,
//!     sst_store: Arc<dyn ObjectStore>,
//! ) -> Result<Db, slatedb::Error> {
//!     let wal = ChorusWal::new(volume);
//!     let gc = GarbageCollectorBuilder::new("orders", sst_store.clone())
//!         .with_wal_gc(Arc::new(wal.clone()));
//!     Db::builder("orders", sst_store)
//!         .with_wal_writer(Box::new(wal))
//!         .with_gc_builder(gc)
//!         .build()
//!         .await
//! }
//! ```
//!
//! # Readers
//!
//! ```no_run
//! use chorus_client::{SegmentedVolume, slatedb::ChorusWal};
//! use slatedb::{DbReader, DbReaderMode, object_store::ObjectStore};
//! use std::sync::Arc;
//!
//! async fn open_reader(
//!     volume: SegmentedVolume,
//!     sst_store: Arc<dyn ObjectStore>,
//! ) -> Result<DbReader, slatedb::Error> {
//!     DbReader::builder("orders", sst_store)
//!         .with_reader_mode(DbReaderMode::ManagedCheckpoint)
//!         .with_wal_reader(Arc::new(ChorusWal::new(volume)))
//!         .build()
//!         .await
//! }
//! ```

#[path = "slatedb/codec.rs"]
mod codec;
#[path = "slatedb/gc.rs"]
mod gc;
#[path = "slatedb/progress.rs"]
mod progress;
#[path = "slatedb/reader.rs"]
mod reader;
#[cfg(test)]
#[path = "slatedb/tests.rs"]
mod tests;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::TryStreamExt;
use slatedb::wal::{
    FlushResultFuture, WalError, WalEvent, WalIterator, WalObserver, WalRows, WalStatus,
    WalStatusListener, WalWriter, WriterInit, WriterInitResult, WriterManifest,
};
use slatedb::RowEntry;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

use crate::{
    Error, ReadOnlyConfig, Recovery, SegmentedVolume, WalEngineConfig, WalHandle, WalSeqNo,
};

/// A SlateDB WAL initializer, reader, and collector for a dedicated Chorus volume.
///
/// Construction does no I/O. SlateDB first fences its manifest, then calls this
/// initializer to fence Chorus and resolve replay, then checks its manifest
/// ownership again. Do not recover/start the volume separately before opening
/// SlateDB: that would put the WAL fence outside this protocol.
///
/// Also implements `slatedb::wal::WalGc`. The writer and collector must receive
/// clones of the same initializer so they share the process-local maintenance
/// connection. During startup, collection waits for replay and engine
/// initialization to finish, or returns their failure. Cancelled/dropped replay,
/// collection before initialization, and a closed writer return `WalError::Closed`.
/// Collection never opens/fences the volume itself.
///
/// `slatedb::wal::WalReader` is independent of that connection and works in
/// separate reader processes. It preserves complete batch and WAL ID boundaries.
/// Unbounded iterators wait for new quorum-visible records. Bounded iterators
/// require a currently readable upper bound and return `Unavailable` if that
/// bound is not yet quorum-visible; they never wait for a future endpoint.
/// Locating the WAL end reads and decodes the active segment, since GCS object
/// metadata does not expose its durable length. Size reader refresh intervals
/// and segment rotation with that read cost in mind.
#[derive(Clone)]
pub struct ChorusWal {
    volume: SegmentedVolume,
    config: WalEngineConfig,
    reader_config: ReadOnlyConfig,
    gc: gc::Registry,
}

impl ChorusWal {
    /// Use a dedicated volume and the default Chorus engine configuration.
    pub fn new(volume: SegmentedVolume) -> Self {
        Self::with_config(volume, WalEngineConfig::default())
    }

    /// Use explicit Chorus capacity, rotation, repair, and shutdown settings.
    ///
    /// `max_record_bytes` bounds an entire encoded SlateDB write batch, including
    /// this adapter's framing. Exceeding it fails the write (and SlateDB may close
    /// the database); batches are never split into non-atomic records.
    pub fn with_config(volume: SegmentedVolume, config: WalEngineConfig) -> Self {
        Self {
            volume,
            config,
            reader_config: ReadOnlyConfig::default(),
            gc: Arc::new(Mutex::new(None)),
        }
    }

    /// Configure active-tail and manifest polling for trailing WAL iterators.
    /// SlateDB `DbReader` refresh frequency is configured separately through
    /// `DbReaderOptions::manifest_poll_interval`. Both intervals must be nonzero.
    pub fn with_reader_config(mut self, config: ReadOnlyConfig) -> Self {
        self.reader_config = config;
        self
    }
}

#[async_trait]
impl WriterInit for ChorusWal {
    async fn fence_and_init(
        &self,
        manifest: &mut WriterManifest,
    ) -> Result<WriterInitResult, WalError> {
        self.config.validate().map_err(wal_error)?;
        // Reject a stale initializer before it disrupts the current Chorus
        // writer, and again after fencing. SlateDB also does the final check.
        manifest.refresh().await?;
        let recovery = self
            .volume
            .recover(WalSeqNo::record(manifest.replay_after_wal_id()))
            .await
            .map_err(wal_error)?;
        manifest.refresh().await?;
        writer_and_replay_with_gc(recovery, self.config.clone(), self.gc.clone())
    }
}

#[cfg(test)]
fn writer_and_replay(
    recovery: Recovery,
    config: WalEngineConfig,
) -> Result<WriterInitResult, WalError> {
    writer_and_replay_with_gc(recovery, config, Arc::new(Mutex::new(None)))
}

fn writer_and_replay_with_gc(
    recovery: Recovery,
    config: WalEngineConfig,
    gc: gc::Registry,
) -> Result<WriterInitResult, WalError> {
    config.validate().map_err(wal_error)?;
    let next_index = recovery.end.record_index;
    next_index
        .checked_add(1)
        .ok_or_else(|| internal_error("WAL ID space exhausted"))?;
    let observer = Observer::new(next_index);
    let startup = gc::Startup::new(observer.clone(), &gc);
    let (ready_tx, ready_rx) = oneshot::channel();
    Ok(WriterInitResult {
        replay_iterator: Box::new(Replay {
            recovery: Some(recovery),
            ready: Some(ready_tx),
            observer: observer.clone(),
            last_seq: None,
            failure: None,
            config: config.clone(),
            startup: Some(startup),
        }),
        wal_writer: Box::new(Writer {
            ready: Some(ready_rx),
            handle: None,
            progress: None,
            notifications: None,
            runtime: None,
            observer,
            next_index,
            last_admitted_seq: None,
            total_rows: 0,
            config,
        }),
    })
}

struct Replay {
    recovery: Option<Recovery>,
    ready: Option<oneshot::Sender<StartedHandle>>,
    observer: Observer,
    last_seq: Option<u64>,
    failure: Option<WalError>,
    config: WalEngineConfig,
    startup: Option<gc::Startup>,
}

// A successfully started engine must also be cancelled if replay's receiver is
// dropped before the writer takes it (e.g. a failed Db build).
struct StartedHandle {
    handle: Option<WalHandle>,
    runtime: tokio::runtime::Handle,
}

impl Drop for StartedHandle {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            self.runtime.spawn(handle.abort());
        }
    }
}

#[async_trait]
impl WalIterator for Replay {
    async fn next(&mut self) -> Result<Option<WalRows>, WalError> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if let Err(status) = self.observer.status() {
            let error = WalError::from(status);
            if let Some(startup) = self.startup.take() {
                startup.complete(Err(error.clone()));
            }
            return Err(error);
        }
        let Some(recovery) = self.recovery.as_mut() else {
            return Ok(None);
        };
        // Keep the completion guard in this future across every suspension. If
        // `next` is cancelled, fail closed and wake GC even if Replay is retained.
        let mut startup = self.startup.take();
        let result = match recovery.try_next().await {
            Ok(Some(record)) => codec::decode(record.payload).and_then(|rows| {
                let seq = rows[0].seq;
                if self.last_seq.is_some_and(|last| seq <= last) {
                    return Err(data_error("SlateDB batch sequence regressed during replay"));
                }
                self.last_seq = Some(seq);
                self.observer.state.lock().unwrap().status.last_flushed_seq = Some(seq);
                Ok(Some(WalRows {
                    rows,
                    last_consumed_wal_file_id: record.seqno.record_index + 1,
                }))
            }),
            Ok(None) => {
                // The writer may start only after the complete fixed replay
                // range has been consumed. Keep no second copy of replay data.
                let recovery = self.recovery.take().unwrap();
                match recovery.start(self.config.clone()).await.map_err(wal_error) {
                    Ok(handle) => {
                        let started = StartedHandle {
                            handle: Some(handle),
                            runtime: tokio::runtime::Handle::current(),
                        };
                        let connection = gc::Connection {
                            handle: started.handle.as_ref().unwrap().gc_handle(),
                            observer: self.observer.clone(),
                        };
                        if let Some(ready) = self.ready.take() {
                            let _ = ready.send(started);
                        }
                        startup.take().unwrap().complete(Ok(connection));
                        Ok(None)
                    }
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(wal_error(error)),
        };
        if let Err(error) = &result {
            self.failure = Some(error.clone());
            startup.take().unwrap().complete(Err(error.clone()));
        }
        self.startup = startup;
        result
    }
}

struct State {
    status: WalStatus,
    listeners: Vec<WalStatusListener>,
    admitted_rows: u128,
    committed_rows: u128,
}

impl State {
    fn update_buffered_rows(&mut self) {
        // A fast commit can be published before append() records admission.
        // Cumulative counters make either ordering safe, including coalescing.
        self.status.buffered_wal_entries_count = self
            .admitted_rows
            .saturating_sub(self.committed_rows)
            .try_into()
            .unwrap_or(usize::MAX);
    }
}

#[derive(Default)]
struct Notifications {
    publishing: bool,
    pending: VecDeque<(WalEvent, Vec<WalStatusListener>)>,
}

#[derive(Clone)]
struct Observer {
    state: Arc<Mutex<State>>,
    events: Arc<Mutex<Notifications>>,
    changed: watch::Sender<()>,
}

impl Observer {
    fn new(last_flushed_wal_id: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                status: WalStatus {
                    closed_reason: None,
                    estimated_bytes: 0,
                    last_flushed_wal_id,
                    last_flushed_seq: None,
                    buffered_wal_entries_count: 0,
                },
                listeners: Vec::new(),
                admitted_rows: 0,
                committed_rows: 0,
            })),
            events: Arc::new(Mutex::new(Notifications::default())),
            changed: watch::channel(()).0,
        }
    }

    fn close(&self, error: WalError) {
        self.notify(|state| {
            if state.status.closed_reason.is_some() {
                return None;
            }
            state.status.closed_reason = Some(error);
            Some((
                WalEvent::WalClosed(state.status.clone()),
                std::mem::take(&mut state.listeners),
            ))
        });
    }

    fn admitted(&self, total_rows: u128) {
        let mut state = self.state.lock().unwrap();
        state.admitted_rows = total_rows;
        state.update_buffered_rows();
    }

    fn committed(&self, position: &progress::Position) {
        self.notify(|state| {
            if state.status.closed_reason.is_some()
                || position.wal_id <= state.status.last_flushed_wal_id
            {
                return None;
            }
            state.status.last_flushed_wal_id = position.wal_id;
            state.status.last_flushed_seq = Some(position.seq);
            state.committed_rows = position.total_rows;
            state.update_buffered_rows();
            Some((
                WalEvent::WalFlushed(state.status.clone()),
                state.listeners.clone(),
            ))
        });
    }

    fn notify(
        &self,
        update: impl FnOnce(&mut State) -> Option<(WalEvent, Vec<WalStatusListener>)>,
    ) {
        {
            // Order status changes and their events together. Exactly one
            // publisher drains the queue, without holding either lock during
            // callbacks. Concurrent/reentrant close queues behind older flushes
            // instead of deadlocking (a callback may even drop the writer).
            let mut events = self.events.lock().unwrap();
            let Some(notification) = update(&mut self.state.lock().unwrap()) else {
                return;
            };
            events.pending.push_back(notification);
            if events.publishing {
                return;
            }
            events.publishing = true;
        }
        loop {
            let notification = {
                let mut events = self.events.lock().unwrap();
                match events.pending.pop_front() {
                    Some(notification) => notification,
                    None => {
                        events.publishing = false;
                        return;
                    }
                }
            };
            for listener in notification.1 {
                listener(notification.0.clone());
            }
            self.changed.send_replace(());
        }
    }

    fn barrier(&self, wal_id: u64) -> FlushResultFuture {
        let observer = self.clone();
        let mut changed = self.changed.subscribe();
        Box::pin(async move {
            loop {
                let status = observer.state.lock().unwrap().status.clone();
                // A barrier completed before a subsequent close stays successful.
                if status.last_flushed_wal_id >= wal_id {
                    return Ok(());
                }
                if let Some(error) = status.closed_reason {
                    return Err(error);
                }
                changed.changed().await.map_err(|_| WalError::Closed)?;
            }
        })
    }
}

impl WalObserver for Observer {
    fn status(&self) -> Result<WalStatus, WalStatus> {
        let status = self.state.lock().unwrap().status.clone();
        if status.closed_reason.is_some() {
            Err(status)
        } else {
            Ok(status)
        }
    }

    fn subscribe(&self, listener: WalStatusListener) -> Result<(), WalError> {
        let mut state = self.state.lock().unwrap();
        if let Some(error) = &state.status.closed_reason {
            return Err(error.clone());
        }
        state.listeners.push(listener);
        Ok(())
    }
}

struct Writer {
    ready: Option<oneshot::Receiver<StartedHandle>>,
    handle: Option<WalHandle>,
    progress: Option<watch::Sender<progress::Progress>>,
    notifications: Option<JoinHandle<()>>,
    runtime: Option<tokio::runtime::Handle>,
    observer: Observer,
    next_index: u64,
    last_admitted_seq: Option<u64>,
    total_rows: u128,
    config: WalEngineConfig,
}

impl Writer {
    async fn start(&mut self) -> Result<(), WalError> {
        self.observer.status().map_err(WalError::from)?;
        if self.handle.is_some() {
            return Ok(());
        }
        // Do not wait forever if a caller attempts to write before replay ends.
        let mut started = self
            .ready
            .as_mut()
            .ok_or(WalError::Closed)?
            .try_recv()
            .map_err(|_| internal_error("consume the complete SlateDB replay iterator first"))?;
        self.ready = None;
        let handle = started.handle.take().unwrap();
        let (sender, receiver) = watch::channel(progress::Progress::default());
        self.runtime = Some(tokio::runtime::Handle::current());
        self.notifications = Some(tokio::spawn(progress::forward(
            receiver,
            self.observer.clone(),
        )));
        self.progress = Some(sender);
        self.handle = Some(handle);
        Ok(())
    }

    fn fail(&self, error: WalError) {
        if let Some(updates) = &self.progress {
            updates.send_modify(|progress| progress.fail(error));
            let progress = updates.borrow().clone();
            self.observer.progress(&progress);
        } else {
            self.observer.close(error);
        }
    }

    async fn append_inner(&mut self, rows: &[RowEntry]) -> Result<(), WalError> {
        self.observer.status().map_err(WalError::from)?;
        let payload = codec::encode(rows, self.config.max_record_bytes)?;
        let seq = rows[0].seq;
        let last_seq = self.last_admitted_seq.or(self
            .observer
            .status()
            .map_err(WalError::from)?
            .last_flushed_seq);
        if last_seq.is_some_and(|last| seq <= last) {
            return Err(internal_error("SlateDB batch sequence must increase"));
        }
        // SlateDB internally computes last_wal_id + 1 as well.
        self.next_index
            .checked_add(2)
            .ok_or_else(|| internal_error("WAL ID space exhausted"))?;
        self.start().await?;
        if self.progress.as_ref().unwrap().is_closed() {
            return Err(internal_error("WAL notification task stopped"));
        }
        let total_rows = self
            .total_rows
            .checked_add(rows.len() as u128)
            .ok_or_else(|| internal_error("WAL row count overflowed"))?;
        let position = progress::Position {
            wal_id: self.next_index + 1,
            seq,
            total_rows,
        };
        // Only the engine's byte budgets gate admission. The callback stays
        // with that admitted record and updates one coalescing progress slot.
        self.handle
            .as_mut()
            .unwrap()
            .enqueue_append_notifying(
                WalSeqNo::record(self.next_index),
                payload,
                progress::completion(self.progress.as_ref().unwrap().clone(), position),
            )
            .await
            .map_err(wal_error)?;
        self.next_index += 1;
        self.last_admitted_seq = Some(seq);
        self.total_rows = total_rows;
        self.observer.admitted(total_rows);
        Ok(())
    }
}

#[async_trait]
impl WalWriter for Writer {
    async fn append(&mut self, write_batch: &[RowEntry]) -> Result<(), WalError> {
        let result = self.append_inner(write_batch).await;
        if let Err(error) = &result {
            self.fail(error.clone());
        }
        result
    }

    async fn flush(&mut self) -> Result<FlushResultFuture, WalError> {
        self.observer.status().map_err(WalError::from)?;
        // Chorus dispatches on admission; flush is a prefix durability barrier,
        // not an instruction to seal a physical segment.
        Ok(self.observer.barrier(self.next_index))
    }

    fn observer(&self) -> Box<dyn WalObserver> {
        Box::new(self.observer.clone())
    }

    fn status(&self) -> Result<WalStatus, WalStatus> {
        self.observer.status()
    }

    async fn close(&mut self) -> Result<(), WalError> {
        if self.handle.is_none() {
            if let Some(ready) = self.ready.as_mut() {
                if let Ok(mut started) = ready.try_recv() {
                    self.handle = started.handle.take();
                }
            }
        }
        // Shutdown drains the engine independently of the notification task.
        let mut result = match self.handle.take() {
            Some(handle) => handle.shutdown().await.map_err(wal_error),
            None => Ok(()),
        };
        self.ready = None;
        self.progress = None;
        if let Some(task) = self.notifications.take() {
            if let Err(error) = task.await {
                result = Err(internal_error(&format!(
                    "notification task failed: {error}"
                )));
            }
        }
        if let Err(status) = self.observer.status() {
            if !matches!(status.closed_reason, Some(WalError::Closed)) {
                result = Err(status.into());
            }
        }
        self.observer
            .close(result.clone().err().unwrap_or(WalError::Closed));
        result
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        if let Some(task) = self.notifications.take() {
            task.abort();
        }
        self.fail(WalError::Closed);
        if let Some(handle) = self.handle.take() {
            if let Some(runtime) = &self.runtime {
                // A dropped Db/build failure must not leave a detached writer.
                // Retain its runtime so dropping outside an entered runtime is
                // also safe; a stopped runtime already cancels all owned tasks.
                runtime.spawn(handle.abort());
            }
        }
    }
}

fn internal_error(message: &str) -> WalError {
    WalError::InternalError(Arc::new(std::io::Error::other(message.to_owned())))
}

fn data_error(message: &str) -> WalError {
    WalError::DataError(Arc::new(std::io::Error::other(message.to_owned())))
}

fn wal_error(error: Error) -> WalError {
    match error {
        Error::Fenced(_) => WalError::Fenced,
        Error::Closed => WalError::Closed,
        Error::ReadOnlyLagged { next, .. } => {
            WalError::WalTruncated(next.record_index.saturating_add(1))
        }
        Error::InvalidCatalog(_)
        | Error::InvalidSegmentData(_)
        | Error::InvalidManifest(_)
        | Error::ConflictingPrefix { .. }
        | Error::RecoveryPrefixTooShort { .. }
        | Error::SealDigestMismatch { .. }
        | Error::SealCrc32cMismatch { .. } => WalError::DataError(Arc::new(error)),
        Error::InvalidConfig(_)
        | Error::OutOfOrder { .. }
        | Error::RecordTooLarge { .. }
        | Error::SequenceExhausted
        | Error::RecoveryIncomplete
        | Error::Internal(_) => WalError::InternalError(Arc::new(error)),
        _ => WalError::Unavailable(Arc::new(error)),
    }
}
