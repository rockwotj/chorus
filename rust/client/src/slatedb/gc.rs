use std::ops::Bound;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::{BoxFuture, Shared};
use futures::FutureExt;
use slatedb::wal::{WalError, WalFileRange, WalGc, WalObserver};
use tokio::sync::oneshot;

use super::{wal_error, ChorusWal, Observer};

type Ready = Shared<BoxFuture<'static, Result<Connection, WalError>>>;
pub(super) type Registry = Arc<Mutex<Option<Ready>>>;

/// Each replay owns its completion signal. Dropping an unfinished replay (or
/// its in-flight `next` future) must release GC without waiting for writer close:
/// SlateDB shuts down GC before the writer and awaits in-flight GC callbacks.
pub(super) struct Startup {
    ready: Option<oneshot::Sender<Result<Connection, WalError>>>,
    observer: Observer,
}

impl Startup {
    pub(super) fn new(observer: Observer, registry: &Registry) -> Self {
        let (sender, receiver) = oneshot::channel();
        let ready = async move { receiver.await.unwrap_or(Err(WalError::Closed)) }
            .boxed()
            .shared();
        // Replace the previous attempt, not its completion signal: collectors
        // already waiting on an older replay must keep that attempt's result.
        *registry.lock().unwrap() = Some(ready);
        Self {
            ready: Some(sender),
            observer,
        }
    }

    pub(super) fn complete(mut self, result: Result<Connection, WalError>) {
        if let Err(error) = &result {
            self.observer.close(error.clone());
        }
        let _ = self.ready.take().unwrap().send(result);
    }
}

impl Drop for Startup {
    fn drop(&mut self) {
        if self.ready.is_some() {
            self.observer.close(WalError::Closed);
            // Dropping the sole sender resolves every GC waiter as Closed.
        }
    }
}

#[derive(Clone)]
pub(super) struct Connection {
    pub handle: crate::engine::GcHandle,
    pub observer: Observer,
}

/// The first Chorus record index that any supplied range protects. SlateDB's
/// WAL IDs are one-based, whereas Chorus record indices are zero-based.
fn retain_from(ranges: &[WalFileRange]) -> u64 {
    ranges
        .iter()
        .filter_map(|WalFileRange(start, end)| {
            let first = match start {
                Bound::Included(id) => *id,
                Bound::Excluded(id) => id.checked_add(1)?,
                Bound::Unbounded => 0,
            };
            let nonempty = match end {
                Bound::Included(last) => first <= *last,
                Bound::Excluded(end) => first < *end,
                Bound::Unbounded => true,
            };
            nonempty.then_some(first.saturating_sub(1))
        })
        .min()
        .unwrap_or(u64::MAX)
}

#[async_trait]
impl WalGc for ChorusWal {
    /// Collect an unreferenced prefix through the attached writer's maintenance
    /// task. Pass a clone of this initializer to `with_wal_gc` on SlateDB's
    /// `GarbageCollectorBuilder`, and attach that builder with `with_gc_builder`.
    ///
    /// The collector is process-local: it requires successful replay by a Db
    /// using this initializer (or a clone). During startup it waits for replay
    /// and engine initialization, propagating failure or cancellation without
    /// deleting anything. Before initialization or after writer close it returns
    /// `Closed`. It never recovers the volume or takes ownership of its writer.
    ///
    /// Only whole sealed segments preceding every retained range are reclaimed;
    /// gaps between retained ranges are intentionally kept. Nonzero `min_age`
    /// checks storage modification time on every existing replica, including
    /// newly repaired copies. Missing timestamps or unavailable listings defer
    /// new truncation. Zero disables the age gate. Restarts and referenced
    /// observations do not reset object age. Dry runs do not change storage.
    /// Already committed deletion tombstones may be retried by normal
    /// background maintenance regardless of a later dry run or retention change.
    async fn collect(
        &self,
        referenced_ranges: Vec<WalFileRange>,
        min_age: Duration,
        dry_run: bool,
    ) -> Result<(), WalError> {
        let ready = self.gc.lock().unwrap().clone().ok_or(WalError::Closed)?;
        let connection = ready.await?;
        connection.observer.status().map_err(WalError::from)?;
        connection
            .handle
            .collect(retain_from(&referenced_ranges), min_age, dry_run)
            .await
            .map_err(wal_error)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_ranges_translate_inclusive_exclusive_empty_and_extreme_bounds() {
        assert_eq!(retain_from(&[]), u64::MAX);
        assert_eq!(
            retain_from(&[WalFileRange(Bound::Included(0), Bound::Unbounded)]),
            0
        );
        assert_eq!(
            retain_from(&[WalFileRange(Bound::Included(5), Bound::Unbounded)]),
            4
        );
        assert_eq!(
            retain_from(&[
                WalFileRange(Bound::Excluded(2), Bound::Excluded(6)),
                WalFileRange(Bound::Included(5), Bound::Unbounded)
            ]),
            2
        );
        assert_eq!(
            retain_from(&[WalFileRange(Bound::Included(7), Bound::Excluded(7))]),
            u64::MAX
        );
        assert_eq!(
            retain_from(&[WalFileRange(Bound::Excluded(u64::MAX), Bound::Unbounded)]),
            u64::MAX
        );
        assert_eq!(
            retain_from(&[WalFileRange(
                Bound::Included(u64::MAX),
                Bound::Included(u64::MAX)
            )]),
            u64::MAX - 1
        );
        assert_eq!(
            retain_from(&[WalFileRange(Bound::Unbounded, Bound::Included(3))]),
            0
        );
    }
}
