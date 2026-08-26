//! Experimental SlateDB writer integration, enabled by the `slatedb` feature.
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
//!
//! This implements writer initialization, bounded streaming recovery, writes,
//! flush barriers, observation, close, and `WalGc`. Wire a clone of the same
//! initializer into SlateDB's GC builder as shown below: collection runs through
//! the live writer's maintenance task, preserving all supplied checkpoint ranges.
//! Only whole sealed segments from the unreferenced prefix are reclaimed.
//! `min_age` conservatively starts when GC first observes a segment as eligible;
//! restart or a referenced observation resets that grace period. Dry runs do not
//! change storage or start timers. An attached, open writer is required; offline
//! or separate-process GC is not supported. `WalReader` and `WalAdmin` remain
//! unimplemented, so live WAL readers and WAL-based clones are unsupported.
//! Do not run Chorus recovery/maintenance tools concurrently with the database:
//! those tools claim a new writer epoch and fence the database.
//!
//! The dependency is pinned to an unreleased SlateDB commit. Replace it with a
//! stable release and revalidate the contract before merging or publishing.
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

mod codec;
mod gc;
#[cfg(test)]
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
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::{
    AppendCompletion, Error, Recovery, SegmentedVolume, WalEngineConfig, WalHandle, WalSeqNo,
};

/// A SlateDB WAL initializer backed by a dedicated Chorus volume.
///
/// Construction does no I/O. SlateDB first fences its manifest, then calls this
/// initializer to fence Chorus and resolve replay, then checks its manifest
/// ownership again. Do not recover/start the volume separately before opening
/// SlateDB: that would put the WAL fence outside this protocol.
///
/// Also implements `slatedb::wal::WalGc`. The writer and collector must receive
/// clones of the same initializer so they share the process-local maintenance
/// connection. Collection returns `WalError::Closed` before replay finishes or
/// after the attached writer closes; it never opens/fences the volume itself.
#[derive(Clone)]
pub struct ChorusWal {
    volume: SegmentedVolume,
    config: WalEngineConfig,
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
            gc: Arc::new(Mutex::new(None)),
        }
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
    // Validate before constructing bounded channels (whose zero capacity panics).
    config.validate().map_err(wal_error)?;
    let next_index = recovery.end.record_index;
    next_index
        .checked_add(1)
        .ok_or_else(|| internal_error("WAL ID space exhausted"))?;
    let observer = Observer::new(next_index);
    let (ready_tx, ready_rx) = oneshot::channel();
    Ok(WriterInitResult {
        replay_iterator: Box::new(Replay {
            recovery: Some(recovery),
            ready: Some(ready_tx),
            observer: observer.clone(),
            last_seq: None,
            failure: None,
            config: config.clone(),
            gc,
        }),
        wal_writer: Box::new(Writer {
            ready: Some(ready_rx),
            handle: None,
            pending: None,
            completions: None,
            runtime: None,
            observer,
            next_index,
            last_admitted_seq: None,
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
    gc: gc::Registry,
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
        let Some(recovery) = self.recovery.as_mut() else {
            return Ok(None);
        };
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
                        *self.gc.lock().unwrap() = Some(gc::Connection {
                            handle: started.handle.as_ref().unwrap().gc_handle(),
                            observer: self.observer.clone(),
                        });
                        if let Some(ready) = self.ready.take() {
                            let _ = ready.send(started);
                        }
                        Ok(None)
                    }
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(wal_error(error)),
        };
        if let Err(error) = &result {
            self.failure = Some(error.clone());
            self.observer.close(error.clone());
        }
        result
    }
}

struct State {
    status: WalStatus,
    listeners: Vec<WalStatusListener>,
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

    fn committed(&self, pending: &Pending) {
        self.notify(|state| {
            if state.status.closed_reason.is_some() {
                return None;
            }
            state.status.last_flushed_wal_id = pending.wal_id;
            state.status.last_flushed_seq = Some(pending.seq);
            state.status.estimated_bytes -= pending.bytes;
            state.status.buffered_wal_entries_count -= pending.rows;
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

struct Pending {
    completion: AppendCompletion,
    wal_id: u64,
    seq: u64,
    bytes: usize,
    rows: usize,
}

struct Writer {
    ready: Option<oneshot::Receiver<StartedHandle>>,
    handle: Option<WalHandle>,
    pending: Option<mpsc::Sender<Pending>>,
    completions: Option<JoinHandle<()>>,
    runtime: Option<tokio::runtime::Handle>,
    observer: Observer,
    next_index: u64,
    last_admitted_seq: Option<u64>,
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
        let (sender, mut receiver) = mpsc::channel::<Pending>(self.config.queue_capacity);
        self.runtime = Some(tokio::runtime::Handle::current());
        let observer = self.observer.clone();
        self.completions = Some(tokio::spawn(async move {
            while let Some(mut pending) = receiver.recv().await {
                match (&mut pending.completion).await {
                    Ok(_) => observer.committed(&pending),
                    Err(error) => {
                        // An ambiguous write is never retried with the same ID.
                        // Stop the database and let takeover resolve its prefix.
                        observer.close(wal_error(error));
                        break;
                    }
                }
            }
        }));
        self.pending = Some(sender);
        self.handle = Some(handle);
        Ok(())
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
        let bytes = payload.len();
        // Reserve notification capacity BEFORE admission. Once Chorus admits
        // the record there is no cancellation point until its completion is
        // transferred to the independently running ordered completion task.
        let slot = self
            .pending
            .as_ref()
            .unwrap()
            .reserve()
            .await
            .map_err(|_| WalError::Closed)?;
        self.observer.status().map_err(WalError::from)?;
        let completion = self
            .handle
            .as_mut()
            .unwrap()
            .enqueue_append(WalSeqNo::record(self.next_index), payload)
            .await
            .map_err(wal_error)?;
        self.next_index += 1;
        self.last_admitted_seq = Some(seq);
        {
            let mut state = self.observer.state.lock().unwrap();
            state.status.estimated_bytes += bytes;
            state.status.buffered_wal_entries_count += rows.len();
        }
        slot.send(Pending {
            completion,
            wal_id: self.next_index,
            seq,
            bytes,
            rows: rows.len(),
        });
        Ok(())
    }
}

#[async_trait]
impl WalWriter for Writer {
    async fn append(&mut self, write_batch: &[RowEntry]) -> Result<(), WalError> {
        let result = self.append_inner(write_batch).await;
        if let Err(error) = &result {
            self.observer.close(error.clone());
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
        // Shutdown drains the engine independently of the completion task.
        let mut result = match self.handle.take() {
            Some(handle) => handle.shutdown().await.map_err(wal_error),
            None => Ok(()),
        };
        self.ready = None;
        self.pending = None;
        if let Some(task) = self.completions.take() {
            if let Err(error) = task.await {
                result = Err(internal_error(&format!("completion task failed: {error}")));
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
        if let Some(task) = self.completions.take() {
            task.abort();
        }
        self.observer.close(WalError::Closed);
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
