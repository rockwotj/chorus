use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use slatedb::wal::{WalError, WalFileRange, WalIterator, WalReader, WalRows};

use super::{codec, data_error, internal_error, wal_error, ChorusWal};
use crate::{ReadOnlyFollower, WalSeqNo};

#[async_trait]
impl WalReader for ChorusWal {
    /// Read complete batches without claiming a writer epoch. Unbounded ranges
    /// follow new writes; bounded ranges must already be quorum-visible.
    async fn iterator(&self, range: WalFileRange) -> Result<Box<dyn WalIterator>, WalError> {
        let first = match range.0 {
            Bound::Included(id) => id.max(1),
            Bound::Excluded(id) => id
                .checked_add(1)
                .ok_or_else(|| internal_error("WAL iterator start bound overflowed"))?,
            Bound::Unbounded => return Err(internal_error("WAL iterator start must be bounded")),
        };
        // An inclusive upper WAL ID is also the exclusive Chorus record index.
        // Keeping it inclusive avoids overflowing at u64::MAX.
        let last = match range.1 {
            Bound::Included(id) => Some(id),
            Bound::Excluded(id) => Some(id.saturating_sub(1)),
            Bound::Unbounded => None,
        };
        let next_index = first - 1;
        let follower = if last.is_some_and(|last| first > last) {
            None
        } else {
            if let Some(last) = last {
                let end = self
                    .volume
                    .readonly_end(WalSeqNo::record(next_index), Some(last))
                    .await
                    .map_err(wal_error)?;
                if last > end.record_index {
                    return Err(WalError::Unavailable(Arc::new(std::io::Error::other(
                        "bounded WAL range extends beyond the quorum-visible end",
                    ))));
                }
            }
            Some(
                self.volume
                    .open_readonly_with_config(WalSeqNo::record(next_index), self.reader_config)
                    .await
                    .map_err(wal_error)?,
            )
        };
        Ok(Box::new(Reader {
            follower,
            next_index,
            last,
            last_seq: None,
            failure: None,
        }))
    }

    async fn last_wal_file_id(&self, replay_after_wal_id: u64) -> Result<u64, WalError> {
        self.volume
            .readonly_end(WalSeqNo::record(replay_after_wal_id), None)
            .await
            .map(|end| end.record_index)
            .map_err(wal_error)
    }
}

struct Reader {
    follower: Option<ReadOnlyFollower>,
    next_index: u64,
    last: Option<u64>,
    last_seq: Option<u64>,
    failure: Option<WalError>,
}

#[async_trait]
impl WalIterator for Reader {
    async fn next(&mut self) -> Result<Option<WalRows>, WalError> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if self.last.is_some_and(|last| self.next_index >= last) {
            self.follower = None;
        }
        let Some(follower) = self.follower.as_mut() else {
            return Ok(None);
        };
        let result = match follower.try_next().await {
            Ok(Some(record)) => (|| {
                if record.seqno.record_index != self.next_index {
                    return Err(data_error("WAL reader skipped or repeated a record"));
                }
                let rows = codec::decode(record.payload)?;
                let seq = rows[0].seq;
                if self.last_seq.is_some_and(|last| seq <= last) {
                    return Err(data_error("SlateDB batch sequence regressed while reading"));
                }
                self.next_index = self
                    .next_index
                    .checked_add(1)
                    .ok_or_else(|| internal_error("WAL ID space exhausted"))?;
                self.last_seq = Some(seq);
                Ok(Some(WalRows {
                    rows,
                    last_consumed_wal_file_id: self.next_index,
                }))
            })(),
            Ok(None) => Err(internal_error("readonly WAL follower ended unexpectedly")),
            Err(error) => Err(wal_error(error)),
        };
        if let Err(error) = &result {
            self.failure = Some(error.clone());
            self.follower = None;
        }
        result
    }
}
