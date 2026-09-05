use tokio::sync::watch;

use super::{internal_error, wal_error, Observer};
use crate::{AppendReceipt, Error};
use slatedb::wal::WalError;

/// One durable prefix, not a queue of individual completion futures. Sequence
/// numbers may have gaps; retain the SlateDB sequence belonging to this WAL ID.
#[derive(Clone, Copy, Debug)]
pub(super) struct Position {
    pub wal_id: u64,
    pub seq: u64,
    pub total_rows: u128,
}

#[derive(Clone, Debug, Default)]
pub(super) struct Progress {
    pub committed: Option<Position>,
    pub failure: Option<WalError>,
}

impl Progress {
    pub(super) fn fail(&mut self, error: WalError) {
        if self.failure.is_none() {
            self.failure = Some(error);
        }
    }
}

/// Runs on the engine task. Only replace a fixed-size snapshot here; SlateDB
/// listeners run separately and cannot hold up engine admission/completion.
pub(super) fn completion(
    updates: watch::Sender<Progress>,
    position: Position,
) -> impl FnOnce(Result<AppendReceipt, Error>) + Send + 'static {
    move |result| {
        updates.send_modify(|progress| {
            if progress.failure.is_some() {
                return;
            }
            match result {
                Ok(receipt) if receipt.next_seqno().record_index == position.wal_id => {
                    progress.committed = Some(position);
                }
                Ok(_) => progress.fail(internal_error("WAL completion position mismatch")),
                Err(error) => progress.fail(wal_error(error)),
            }
        });
    }
}

pub(super) async fn forward(mut updates: watch::Receiver<Progress>, observer: Observer) {
    loop {
        // Never hold a watch borrow while invoking listeners or awaiting.
        let progress = updates.borrow_and_update().clone();
        observer.progress(&progress);
        if progress.failure.is_some() || updates.changed().await.is_err() {
            return;
        }
    }
}

impl Observer {
    pub(super) fn progress(&self, progress: &Progress) {
        // A consumer may see several commits followed by failure in one
        // snapshot. Publish the successful prefix before closing its suffix.
        if let Some(position) = &progress.committed {
            self.committed(position);
        }
        if let Some(error) = &progress.failure {
            self.close(error.clone());
        }
    }
}
