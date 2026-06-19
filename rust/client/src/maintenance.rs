//! The WAL's background maintenance task.
//!
//! Sealed-segment repair, floor-committed truncation, tombstone cleanup, and the
//! dead-incarnation sweep deferred by recovery run here, serialized with each
//! other (repair must never recreate history a deletion pass is removing) and
//! concurrent with the append engine — none blocks recovery or a single append.
//! The task owns an epoch-free manifest handle: only an application command
//! raises `chorus.trunc`, while startup and periodic ticks merely retry
//! generation-matched deletes already authorized by a committed floor or
//! recovery epoch. Sealed-catalog snapshots arrive from the engine through a
//! watch channel after every rotation.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, Interval, MissedTickBehavior};

use crate::error::Error;
use crate::manifest::Manifest;
use crate::manifest_store::ManifestStore;
use crate::metrics::Metrics;
use crate::protocol::{retry_sleep, ClientConfig};
use crate::segment::{
    cleanup_tombstones_pass, enforce_committed_seal, repair_sealed_pass, sweep_dead_segments,
    truncate_pass, DeadSegmentSweep, SegmentDescriptor, SwappedSegment, TruncationReport, WalSeqNo,
};
use crate::transport::ReplicaFactory;

const MAINTENANCE_COMMAND_CAPACITY: usize = 64;

pub(crate) enum MaintenanceCmd {
    /// Repair only one newly sealed segment whose finalization did not
    /// succeed on every replica.
    #[cfg_attr(not(test), allow(dead_code))]
    RepairSegment(SegmentDescriptor),
    /// Finalize a swapped-out segment off the hot path: await lane
    /// acknowledgments, finalize at the committed boundary (signaling
    /// `enforced`), and repair only this segment when needed.
    SealSegment {
        segment: Box<SwappedSegment>,
        /// Signaled once a finalized quorum exists. The engine gates its
        /// next swap on this, so at most one sealed segment — always the
        /// manifest's current seal record — is ever unenforced, which is
        /// exactly the one segment recovery enforces.
        enforced: oneshot::Sender<()>,
    },
    /// Raise the committed floor and delete covered sealed segments.
    Truncate {
        floor: WalSeqNo,
        response: oneshot::Sender<Result<TruncationReport, Error>>,
    },
}

/// Control handle owned by the [`crate::WalHandle`]; dropping it ends the
/// maintenance task after the command in progress.
#[derive(Clone)]
pub(crate) struct MaintenanceHandle {
    tx: mpsc::Sender<MaintenanceCmd>,
    shutdown: watch::Sender<bool>,
    metrics: Arc<Metrics>,
}

struct QueueDepthReservation<'a> {
    metrics: &'a Metrics,
    armed: bool,
}

impl QueueDepthReservation<'_> {
    fn commit(mut self) {
        self.armed = false;
    }
}

impl Drop for QueueDepthReservation<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.metrics.adjust_maintenance_queue_depth(-1);
        }
    }
}

impl MaintenanceHandle {
    async fn enqueue(&self, command: MaintenanceCmd) -> Result<(), Error> {
        self.metrics.adjust_maintenance_queue_depth(1);
        let reservation = QueueDepthReservation {
            metrics: &self.metrics,
            armed: true,
        };
        self.tx.send(command).await.map_err(|_| Error::Closed)?;
        reservation.commit();
        Ok(())
    }

    /// Hand a swapped-out segment to the maintenance task for seal
    /// enforcement. The returned receiver resolves once a finalized quorum
    /// exists; the engine gates its next swap on it. If the sender is dropped
    /// instead (live sealing and bounded idempotent enforcement both failed,
    /// or the task died), the engine disables rotation until restart.
    pub(crate) async fn seal_segment(
        &self,
        segment: SwappedSegment,
    ) -> Result<oneshot::Receiver<()>, Error> {
        let (enforced, receiver) = oneshot::channel();
        self.enqueue(MaintenanceCmd::SealSegment {
            segment: Box::new(segment),
            enforced,
        })
        .await?;
        Ok(receiver)
    }

    pub(crate) async fn truncate(&self, floor: WalSeqNo) -> Result<TruncationReport, Error> {
        let (response, receiver) = oneshot::channel();
        self.enqueue(MaintenanceCmd::Truncate { floor, response })
            .await?;
        receiver.await.map_err(|_| Error::Closed)?
    }

    pub(crate) fn shutdown(&self) {
        let _ = self.shutdown.send(true);
    }
}

pub(crate) struct MaintenanceConfig {
    pub factories: Vec<Arc<dyn ReplicaFactory>>,
    pub manifest_store: Arc<dyn ManifestStore>,
    pub bucket_names: Vec<String>,
    pub prefix: String,
    pub client_config: ClientConfig,
    pub checkpoint_floor: u64,
    pub dead_segment_sweep: DeadSegmentSweep,
    pub repair_interval: Option<Duration>,
}

pub(crate) fn start(
    config: MaintenanceConfig,
    catalog: watch::Receiver<Vec<SegmentDescriptor>>,
    metrics: Arc<Metrics>,
) -> (MaintenanceHandle, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(MAINTENANCE_COMMAND_CAPACITY);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let task_metrics = Arc::clone(&metrics);
    let task = tokio::spawn(run(config, catalog, rx, shutdown_rx, task_metrics));
    (
        MaintenanceHandle {
            tx,
            shutdown,
            metrics,
        },
        task,
    )
}

struct MaintenanceState {
    factories: Vec<Arc<dyn ReplicaFactory>>,
    manifest_store: Arc<dyn ManifestStore>,
    bucket_names: Vec<String>,
    prefix: String,
    client_config: ClientConfig,
    /// Canonical sealed catalog: engine snapshots merged in, minus what this
    /// task has deleted.
    catalog: Vec<SegmentDescriptor>,
    deleted: HashSet<u64>,
    /// Segments this task has finalized since the swap published them with
    /// `seal_pending`: the engine's snapshots keep carrying the flag, so the
    /// merge clears it from here. A seal that exhausts both live finalization
    /// and idempotent reconstruction stays pending — repair keeps skipping it
    /// and restart recovery enforces it.
    sealed: HashSet<u64>,
    checkpoint_floor: u64,
    /// Retained until every zone was listed and every eligible object was
    /// deleted or already absent. The keep set may be stale; its strict
    /// below-claimed-epoch guard is the safety fence.
    dead_segment_sweep: Option<DeadSegmentSweep>,
    /// Lazily opened epoch-free manifest handle; reopened after errors.
    manifest: Option<Manifest>,
    metrics: Arc<Metrics>,
}

async fn run(
    config: MaintenanceConfig,
    mut catalog_rx: watch::Receiver<Vec<SegmentDescriptor>>,
    mut commands: mpsc::Receiver<MaintenanceCmd>,
    mut shutdown: watch::Receiver<bool>,
    metrics: Arc<Metrics>,
) {
    let mut task = MaintenanceState {
        factories: config.factories,
        manifest_store: config.manifest_store,
        bucket_names: config.bucket_names,
        prefix: config.prefix,
        client_config: config.client_config,
        catalog: catalog_rx.borrow_and_update().clone(),
        deleted: HashSet::new(),
        sealed: HashSet::new(),
        checkpoint_floor: config.checkpoint_floor,
        dead_segment_sweep: Some(config.dead_segment_sweep),
        manifest: None,
        metrics,
    };
    let mut interval = config.repair_interval.map(|period| {
        let mut interval = tokio::time::interval_at(Instant::now() + period, period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        interval
    });
    // Engine start (and every restart) is a rejoin opportunity for both
    // deletion stragglers and immutable-copy repair. Cleanup runs first so
    // repair never spends work on a tombstone the committed floor already
    // made unreachable.
    let mut pending = PendingCommands::default();
    if maintenance_pass_or_shutdown(&mut task, &mut catalog_rx, &mut shutdown).await {
        commands.close();
        while let Some(command) = next_command(&mut commands, &mut pending).await {
            execute_command(&mut task, &mut catalog_rx, command).await;
        }
        return;
    }
    loop {
        // `biased` keeps the poll order deterministic (an unbiased `select!`
        // randomizes its starting branch per call). Shutdown wins even when
        // every maintenance pass overruns a hot interval. An overdue tick then
        // wins before each logical command, so a sustained command flood cannot
        // starve autonomous cleanup and repair.
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                // Graceful shutdown is signaled only after the engine has
                // stopped producing seals. Close ingress, then drain every
                // buffered command without periodic ticks interleaving; a
                // committed seal is never silently dropped.
                commands.close();
                break;
            }
            () = tick(&mut interval) => {
                if maintenance_pass_or_shutdown(&mut task, &mut catalog_rx, &mut shutdown).await {
                    commands.close();
                    break;
                }
            }
            command = next_command(&mut commands, &mut pending) => match command {
                Some(command) => execute_command(&mut task, &mut catalog_rx, command).await,
                None => return,
            },
        }
    }

    while let Some(command) = next_command(&mut commands, &mut pending).await {
        execute_command(&mut task, &mut catalog_rx, command).await;
    }
}

async fn maintenance_pass_or_shutdown(
    task: &mut MaintenanceState,
    catalog_rx: &mut watch::Receiver<Vec<SegmentDescriptor>>,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        biased;
        // Shutdown stays first inside the pass so a slow repair cannot hide it.
        _ = shutdown.changed() => true,
        () = async {
            task.adopt_catalog(catalog_rx);
            task.sweep_dead_segments_once().await;
            task.cleanup_tombstones_once().await;
            task.repair_once().await;
        } => false,
    }
}

async fn execute_command(
    task: &mut MaintenanceState,
    catalog_rx: &mut watch::Receiver<Vec<SegmentDescriptor>>,
    command: ReadyCommand,
) {
    task.metrics.adjust_maintenance_queue_depth(
        -i64::try_from(command.queued_requests).unwrap_or(i64::MAX),
    );
    match command.kind {
        ReadyCommandKind::RepairSegment(segment) => {
            task.adopt_catalog(catalog_rx);
            task.repair_segment_once(segment).await;
        }
        ReadyCommandKind::SealSegment { segment, enforced } => {
            task.seal_segment(*segment, enforced, catalog_rx).await;
            task.adopt_catalog(catalog_rx);
        }
        ReadyCommandKind::Truncate { floor, responses } => {
            task.adopt_catalog(catalog_rx);
            let result = task.truncate_once(floor).await;
            match &result {
                Ok(report) => {
                    tracing::info!(
                        floor = floor.record_index,
                        deleted_objects = report.deleted_objects,
                        deleted_segments = report.deleted_segments,
                        "truncation completed"
                    );
                    task.metrics.truncation_cycles.increment();
                }
                Err(error) => {
                    tracing::warn!(
                        floor = floor.record_index,
                        %error,
                        "truncation failed"
                    );
                    task.manifest = None;
                    task.metrics.operation_failures.increment();
                }
            }
            for response in responses {
                let _ = response.send(result.clone());
            }
        }
    }
}

#[derive(Default)]
struct PendingCommands {
    groups: VecDeque<PendingGroup>,
}

enum PendingGroup {
    Work(PendingWork),
    Seal {
        segment: Box<SwappedSegment>,
        enforced: oneshot::Sender<()>,
    },
}

#[derive(Default)]
struct PendingWork {
    repairs: BTreeMap<(u64, String), (SegmentDescriptor, usize)>,
    truncation: Option<PendingTruncation>,
}

struct PendingTruncation {
    floor: WalSeqNo,
    responses: Vec<oneshot::Sender<Result<TruncationReport, Error>>>,
}

struct ReadyCommand {
    kind: ReadyCommandKind,
    queued_requests: usize,
}

enum ReadyCommandKind {
    RepairSegment(SegmentDescriptor),
    SealSegment {
        segment: Box<SwappedSegment>,
        enforced: oneshot::Sender<()>,
    },
    Truncate {
        floor: WalSeqNo,
        responses: Vec<oneshot::Sender<Result<TruncationReport, Error>>>,
    },
}

impl PendingCommands {
    fn push(&mut self, command: MaintenanceCmd) {
        match command {
            MaintenanceCmd::SealSegment { segment, enforced } => {
                // A seal owns a swapped writer and is a strict FIFO barrier:
                // work received before it remains before it, and work received
                // after it cannot overtake it.
                self.groups
                    .push_back(PendingGroup::Seal { segment, enforced });
            }
            MaintenanceCmd::RepairSegment(segment) => self.push_repair(segment),
            MaintenanceCmd::Truncate { floor, response } => {
                self.push_truncation(floor, response);
            }
        }
    }

    fn push_repair(&mut self, segment: SegmentDescriptor) {
        let key = (segment.base_record_index, segment.id.clone());
        if let Some(PendingGroup::Work(work)) = self.groups.back_mut() {
            match work.repairs.entry(key) {
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let value = entry.get_mut();
                    value.0 = segment;
                    value.1 += 1;
                }
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert((segment, 1));
                }
            }
            return;
        }
        let mut work = PendingWork::default();
        work.repairs.insert(key, (segment, 1));
        self.groups.push_back(PendingGroup::Work(work));
    }

    fn push_truncation(
        &mut self,
        floor: WalSeqNo,
        response: oneshot::Sender<Result<TruncationReport, Error>>,
    ) {
        if let Some(PendingGroup::Work(work)) = self.groups.back_mut() {
            merge_truncation(work, floor, response);
            return;
        }
        let mut work = PendingWork::default();
        merge_truncation(&mut work, floor, response);
        self.groups.push_back(PendingGroup::Work(work));
    }

    fn pop_front(&mut self) -> Option<ReadyCommand> {
        loop {
            match self.groups.front_mut()? {
                PendingGroup::Work(work) => {
                    if let Some(truncation) = work.truncation.take() {
                        let queued_requests = truncation.responses.len();
                        return Some(ReadyCommand {
                            kind: ReadyCommandKind::Truncate {
                                floor: truncation.floor,
                                responses: truncation.responses,
                            },
                            queued_requests,
                        });
                    }
                    if let Some((_, (segment, queued_requests))) = work.repairs.pop_first() {
                        return Some(ReadyCommand {
                            kind: ReadyCommandKind::RepairSegment(segment),
                            queued_requests,
                        });
                    }
                    self.groups.pop_front();
                }
                PendingGroup::Seal { .. } => match self.groups.pop_front() {
                    Some(PendingGroup::Seal { segment, enforced }) => {
                        return Some(ReadyCommand {
                            kind: ReadyCommandKind::SealSegment { segment, enforced },
                            queued_requests: 1,
                        });
                    }
                    Some(PendingGroup::Work(_)) | None => continue,
                },
            }
        }
    }
}

fn merge_truncation(
    work: &mut PendingWork,
    floor: WalSeqNo,
    response: oneshot::Sender<Result<TruncationReport, Error>>,
) {
    match &mut work.truncation {
        Some(truncation) => {
            if floor.record_index > truncation.floor.record_index {
                truncation.floor = floor;
            }
            truncation.responses.push(response);
        }
        None => {
            work.truncation = Some(PendingTruncation {
                floor,
                responses: vec![response],
            });
        }
    }
}

async fn next_command(
    commands: &mut mpsc::Receiver<MaintenanceCmd>,
    pending: &mut PendingCommands,
) -> Option<ReadyCommand> {
    loop {
        if let Some(command) = pending.pop_front() {
            return Some(command);
        }
        pending.push(commands.recv().await?);
        while let Ok(command) = commands.try_recv() {
            pending.push(command);
        }
    }
}

impl MaintenanceState {
    /// Enforce one already-committed rotation seal and release the engine.
    ///
    /// `Writer::seal` is deliberately destructive: it closes and takes every
    /// live lane before finalization, so an error cannot be retried on that
    /// writer. The manifest decision remains valid, however. On any fast-path
    /// failure we therefore switch to the storage-only path, which reconstructs
    /// the exact committed record count from a read quorum, verifies the digest
    /// committed by the fold CAS, and idempotently installs the bytes. Only
    /// exhaustion of that separate retry budget drops `enforced` and preserves
    /// the old gate-until-restart fallback.
    async fn seal_segment(
        &mut self,
        segment: SwappedSegment,
        enforced: oneshot::Sender<()>,
        catalog_rx: &mut watch::Receiver<Vec<SegmentDescriptor>>,
    ) {
        let SwappedSegment {
            id,
            base_record_index: base,
            end_record_index: end,
            digest,
            crc32c,
            mut writer,
        } = segment;
        let seal_started = tokio::time::Instant::now();
        let quorum = crate::protocol::majority(self.factories.len());
        let enforcement = match writer.seal().await {
            Ok(report) => Ok((
                SegmentDescriptor {
                    id: id.clone(),
                    base_record_index: base,
                    end_record_index: end,
                    crc32c,
                    copies: quorum,
                    finalized_copies: quorum,
                    seal_pending: false,
                },
                report.all_replicas_finalized(),
            )),
            Err(error) => {
                tracing::warn!(
                    segment_base = base,
                    %error,
                    "live swapped-segment seal failed; reconstructing committed seal from storage"
                );
                self.retry_committed_seal(&id, base, end, &digest, crc32c)
                    .await
                    .map(|segment| (segment, false))
            }
        };

        match enforcement {
            Ok((segment, all_replicas_finalized)) => {
                self.sealed.insert(base);
                self.metrics.segments_sealed.increment();
                self.metrics
                    .seal_duration
                    .record_duration(seal_started.elapsed());
                tracing::info!(segment_base = base, "swapped segment seal enforced");
                self.adopt_catalog(catalog_rx);
                let _ = enforced.send(());
                if !all_replicas_finalized {
                    self.repair_segment_once(segment).await;
                }
            }
            Err(error) => {
                // The record range and digest remain in the manifest. Keeping
                // the oneshot unresolved until this point lets transient
                // outages recover without an availability cliff; dropping it
                // only after bounded retries leaves the same safe restart
                // recovery path as before.
                tracing::warn!(
                    segment_base = base,
                    %error,
                    "committed seal enforcement exhausted retries; rotation disabled until restart"
                );
                self.metrics.operation_failures.increment();
                drop(enforced);
            }
        }
    }

    async fn retry_committed_seal(
        &self,
        id: &str,
        base_record_index: u64,
        end_record_index: u64,
        digest: &str,
        crc32c: u32,
    ) -> Result<SegmentDescriptor, Error> {
        let mut attempt = 0usize;
        loop {
            if attempt > 0 {
                self.metrics.seal_enforcement_retries.increment();
            }
            match enforce_committed_seal(
                &self.factories,
                &self.prefix,
                &self.client_config,
                Arc::clone(&self.metrics),
                id,
                base_record_index,
                end_record_index,
                Some(digest),
                crc32c,
            )
            .await
            {
                Ok(segment) => return Ok(segment),
                Err(error) if attempt < self.client_config.max_retries => {
                    tracing::warn!(
                        segment_base = base_record_index,
                        retry = attempt + 1,
                        %error,
                        "committed seal enforcement failed; retrying"
                    );
                    retry_sleep(&self.client_config, attempt).await;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Merge the engine's latest sealed-catalog snapshot, keeping our
    /// deletions (the engine's writer never prunes).
    fn adopt_catalog(&mut self, rx: &mut watch::Receiver<Vec<SegmentDescriptor>>) {
        let snapshot = rx.borrow_and_update().clone();
        let mut merged = snapshot;
        merged.retain(|segment| !self.deleted.contains(&segment.base_record_index));
        for segment in &mut merged {
            if segment.seal_pending && self.sealed.contains(&segment.base_record_index) {
                segment.seal_pending = false;
            }
        }
        self.catalog = merged;
    }

    async fn ensure_manifest(&mut self) -> Result<&mut Manifest, Error> {
        if self.manifest.is_none() {
            let manifest = Manifest::open(
                Arc::clone(&self.manifest_store),
                self.client_config.clone(),
                Arc::clone(&self.metrics),
                self.factories.len(),
                self.bucket_names.clone(),
            )
            .await
            .map_err(Error::from)?;
            self.manifest = Some(manifest);
        }
        self.manifest
            .as_mut()
            .ok_or_else(|| Error::Internal("maintenance manifest initialization was lost".into()))
    }

    async fn sweep_dead_segments_once(&mut self) {
        let Some(sweep) = self.dead_segment_sweep.clone() else {
            return;
        };
        let report = sweep_dead_segments(&self.factories, &self.prefix, &sweep).await;
        self.metrics
            .orphan_objects_deleted
            .add(report.deleted_objects as u64);
        if report.deferred_operations == 0 {
            self.dead_segment_sweep = None;
            if report.orphan_segments > 0 || report.deleted_objects > 0 {
                tracing::info!(
                    claimed_epoch = sweep.claimed_epoch,
                    orphan_segments = report.orphan_segments,
                    deleted_objects = report.deleted_objects,
                    "dead-segment maintenance sweep completed"
                );
            }
            return;
        }

        self.metrics.orphan_sweeps_deferred.increment();
        match report.failure {
            Some(error) => {
                self.metrics.operation_failures.increment();
                tracing::warn!(
                    claimed_epoch = sweep.claimed_epoch,
                    orphan_segments = report.orphan_segments,
                    deleted_objects = report.deleted_objects,
                    deferred_operations = report.deferred_operations,
                    %error,
                    "dead-segment maintenance sweep failed; will retry"
                );
            }
            None => {
                tracing::warn!(
                    claimed_epoch = sweep.claimed_epoch,
                    orphan_segments = report.orphan_segments,
                    deleted_objects = report.deleted_objects,
                    deferred_operations = report.deferred_operations,
                    "dead-segment maintenance sweep deferred incomplete work"
                );
            }
        }
    }

    async fn cleanup_tombstones_once(&mut self) {
        let factories = self.factories.clone();
        let prefix = self.prefix.clone();
        if let Err(error) = self.ensure_manifest().await {
            tracing::warn!(%error, "tombstone cleanup manifest unavailable");
            self.metrics.operation_failures.increment();
            return;
        }
        let Some(mut manifest) = self.manifest.take() else {
            tracing::warn!("tombstone cleanup lost its ensured manifest handle");
            self.metrics.operation_failures.increment();
            return;
        };
        let before: HashSet<u64> = self
            .catalog
            .iter()
            .map(|segment| segment.base_record_index)
            .collect();
        let result =
            cleanup_tombstones_pass(&factories, &prefix, &mut self.catalog, &mut manifest).await;
        self.manifest = Some(manifest);
        match result {
            Ok(report) => {
                self.remember_catalog_deletions(before);
                if report.deleted_objects > 0 || report.deleted_segments > 0 {
                    tracing::info!(
                        deleted_objects = report.deleted_objects,
                        deleted_segments = report.deleted_segments,
                        "committed-floor tombstone cleanup completed"
                    );
                }
            }
            Err(error) => {
                // Reopen on the next pass: a failed refresh or remove CAS may
                // have left this handle's cache behind a concurrent truncator.
                self.manifest = None;
                tracing::warn!(%error, "committed-floor tombstone cleanup failed");
                self.metrics.operation_failures.increment();
            }
        }
    }

    async fn repair_once(&mut self) {
        let floor = match self.ensure_manifest().await {
            Ok(manifest) => match manifest.refreshed_record().await {
                Ok(record) => record.trunc,
                Err(error) => {
                    tracing::warn!(%error, "repair manifest refresh unavailable");
                    self.manifest = None;
                    self.metrics.repair_failures.increment();
                    self.metrics.repair_passes.increment();
                    return;
                }
            },
            Err(error) => {
                tracing::warn!(%error, "repair manifest unavailable");
                self.metrics.repair_failures.increment();
                self.metrics.repair_passes.increment();
                return;
            }
        };
        match repair_sealed_pass(&self.factories, &self.prefix, &self.catalog, floor).await {
            Ok(report) => {
                self.metrics
                    .repair_objects_repaired
                    .add(report.objects_repaired as u64);
                self.metrics
                    .repair_transient_skips
                    .add(report.transient_failures as u64);
                if report.objects_repaired > 0 {
                    tracing::info!(
                        segments_examined = report.segments_examined,
                        objects_repaired = report.objects_repaired,
                        objects_already_healthy = report.objects_already_healthy,
                        transient_failures = report.transient_failures,
                        floor,
                        "repair pass completed"
                    );
                }
                if report.transient_failures > 0 {
                    tracing::warn!(
                        segments_examined = report.segments_examined,
                        objects_repaired = report.objects_repaired,
                        transient_failures = report.transient_failures,
                        floor,
                        "repair pass left transient failures"
                    );
                }
            }
            Err(error) => {
                tracing::warn!(%error, floor, "repair pass failed");
                self.metrics.repair_failures.increment();
                self.metrics.operation_failures.increment();
            }
        }
        // counted on completion: deterministic tests and the DST poll this
        // as "a full pass has run"
        self.metrics.repair_passes.increment();
    }

    async fn repair_segment_once(&mut self, segment: SegmentDescriptor) {
        let floor = match self.ensure_manifest().await {
            Ok(manifest) => match manifest.refreshed_record().await {
                Ok(record) => record.trunc,
                Err(error) => {
                    tracing::warn!(
                        %error,
                        segment_base = segment.base_record_index,
                        "targeted repair manifest refresh unavailable"
                    );
                    self.manifest = None;
                    self.metrics.repair_failures.increment();
                    self.metrics.repair_passes.increment();
                    return;
                }
            },
            Err(error) => {
                tracing::warn!(
                    %error,
                    segment_base = segment.base_record_index,
                    "targeted repair manifest unavailable"
                );
                self.metrics.repair_failures.increment();
                self.metrics.repair_passes.increment();
                return;
            }
        };
        match repair_sealed_pass(
            &self.factories,
            &self.prefix,
            std::slice::from_ref(&segment),
            floor,
        )
        .await
        {
            Ok(report) => {
                self.metrics
                    .repair_objects_repaired
                    .add(report.objects_repaired as u64);
                self.metrics
                    .repair_transient_skips
                    .add(report.transient_failures as u64);
                if report.objects_repaired > 0 {
                    tracing::info!(
                        segment_base = segment.base_record_index,
                        objects_repaired = report.objects_repaired,
                        transient_failures = report.transient_failures,
                        "targeted post-rotation repair completed"
                    );
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    segment_base = segment.base_record_index,
                    "targeted post-rotation repair failed"
                );
                self.metrics.repair_failures.increment();
                self.metrics.operation_failures.increment();
            }
        }
        self.metrics.repair_passes.increment();
    }

    async fn truncate_once(&mut self, floor: WalSeqNo) -> Result<TruncationReport, Error> {
        let factories = self.factories.clone();
        let prefix = self.prefix.clone();
        self.ensure_manifest().await?;
        let mut manifest = self.manifest.take().ok_or_else(|| {
            Error::Internal("maintenance manifest initialization was lost".into())
        })?;
        let before: HashSet<u64> = self
            .catalog
            .iter()
            .map(|segment| segment.base_record_index)
            .collect();
        let result = truncate_pass(
            &factories,
            &prefix,
            &mut self.catalog,
            &mut self.checkpoint_floor,
            &mut manifest,
            floor,
        )
        .await;
        self.manifest = Some(manifest);
        let report = result?;
        self.remember_catalog_deletions(before);
        Ok(report)
    }

    fn remember_catalog_deletions(&mut self, before: HashSet<u64>) {
        for gone in before {
            if !self
                .catalog
                .iter()
                .any(|segment| segment.base_record_index == gone)
            {
                self.deleted.insert(gone);
            }
        }
    }
}

async fn tick(interval: &mut Option<Interval>) {
    match interval {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segment(base: u64) -> SegmentDescriptor {
        SegmentDescriptor {
            id: format!("segment-{base}"),
            base_record_index: base,
            end_record_index: base,
            crc32c: base as u32,
            copies: 3,
            finalized_copies: 2,
            seal_pending: false,
        }
    }

    #[tokio::test]
    async fn maintenance_command_flood_is_bounded_and_coalesced() {
        let (tx, mut rx) = mpsc::channel(MAINTENANCE_COMMAND_CAPACITY);
        let mut truncation_receivers = Vec::new();
        for index in 0..MAINTENANCE_COMMAND_CAPACITY {
            let command = if index % 2 == 0 {
                MaintenanceCmd::RepairSegment(segment((index % 8) as u64))
            } else {
                let (response, receiver) = oneshot::channel();
                truncation_receivers.push(receiver);
                MaintenanceCmd::Truncate {
                    floor: WalSeqNo::record(index as u64),
                    response,
                }
            };
            tx.try_send(command).expect("fixed-capacity flood fits");
        }
        assert!(matches!(
            tx.try_send(MaintenanceCmd::RepairSegment(segment(99))),
            Err(mpsc::error::TrySendError::Full(_))
        ));

        let mut pending = PendingCommands::default();
        let Some(ReadyCommand {
            kind: ReadyCommandKind::Truncate { floor, responses },
            queued_requests,
        }) = next_command(&mut rx, &mut pending).await
        else {
            panic!("coalesced truncation must run before deduplicated repairs");
        };
        assert_eq!(
            floor,
            WalSeqNo::record((MAINTENANCE_COMMAND_CAPACITY - 1) as u64)
        );
        assert_eq!(responses.len(), truncation_receivers.len());
        assert_eq!(queued_requests, truncation_receivers.len());

        let report = TruncationReport {
            deleted_objects: 3,
            deleted_segments: 1,
        };
        for response in responses {
            let _ = response.send(Ok(report.clone()));
        }
        for receiver in truncation_receivers {
            assert_eq!(receiver.await.unwrap().unwrap(), report);
        }

        let mut repaired = Vec::new();
        let mut repaired_requests = 0;
        while let Some(command) = pending.pop_front() {
            repaired_requests += command.queued_requests;
            match command.kind {
                ReadyCommandKind::RepairSegment(segment) => {
                    repaired.push(segment.base_record_index);
                }
                ReadyCommandKind::SealSegment { .. } | ReadyCommandKind::Truncate { .. } => {
                    panic!("flood coalescer emitted an unexpected command");
                }
            }
        }
        assert_eq!(repaired, (0..8).step_by(2).collect::<Vec<_>>());
        assert_eq!(
            repaired_requests + queued_requests,
            MAINTENANCE_COMMAND_CAPACITY
        );
    }
}
