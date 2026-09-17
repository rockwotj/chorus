//! Readonly SlateDB operations are intentionally unsupported for now.
use super::ChorusWal;
use async_trait::async_trait;
use slatedb::wal::{WalError, WalFileRange, WalIterator, WalReader};

fn unsupported() -> WalError {
    WalError::InternalError(std::sync::Arc::new(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "Chorus SlateDB readonly operations are not implemented",
    )))
}

#[async_trait]
impl WalReader for ChorusWal {
    async fn iterator(&self, _range: WalFileRange) -> Result<Box<dyn WalIterator>, WalError> {
        Err(unsupported())
    }
    async fn last_wal_file_id(&self, _replay_after_wal_id: u64) -> Result<u64, WalError> {
        Err(unsupported())
    }
}
