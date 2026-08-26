use std::ops::Bound;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use slatedb::wal::{WalError, WalFileRange, WalGc, WalObserver};

use super::{wal_error, ChorusWal, Observer};

pub(super) type Registry = Arc<Mutex<Option<Connection>>>;

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
    /// using this initializer (or a clone), and returns `Closed` once that writer
    /// closes. It never recovers the volume or takes ownership of its writer.
    ///
    /// Only whole sealed segments preceding every retained range are reclaimed;
    /// gaps between retained ranges are intentionally kept. `min_age` is a
    /// conservative grace period from the first collection pass that observes a
    /// segment as sealed and unreferenced. Reopening or observing the segment as
    /// referenced resets that period. Dry runs do not start timers or change
    /// storage. Already committed deletion tombstones may be retried by normal
    /// background maintenance regardless of a later dry run or retention change.
    async fn collect(
        &self,
        referenced_ranges: Vec<WalFileRange>,
        min_age: Duration,
        dry_run: bool,
    ) -> Result<(), WalError> {
        let connection = self.gc.lock().unwrap().clone().ok_or(WalError::Closed)?;
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
