//! Write-handle encoding.

/// Decoded contents of an opaque write handle: the writer incarnation a
/// [`crate::SessionOpened`] belongs to.
///
/// Encoding (17 bytes): `u8 version (=1) | i64 LE generation | u64 LE writer_epoch`.
/// Clients treat the bytes as opaque; only the server decodes them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WriteHandle {
    /// Object generation the writer is bound to.
    pub generation: i64,
    /// Writer epoch of the object at open time.
    pub writer_epoch: u64,
}

const HANDLE_VERSION: u8 = 1;
const HANDLE_LEN: usize = 17;

impl WriteHandle {
    /// Encode into opaque handle bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HANDLE_LEN);
        out.push(HANDLE_VERSION);
        out.extend_from_slice(&self.generation.to_le_bytes());
        out.extend_from_slice(&self.writer_epoch.to_le_bytes());
        out
    }

    /// Decode handle bytes; `None` if they are not a valid handle (the server
    /// answers such a request with `InvalidArgument`).
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != HANDLE_LEN || bytes[0] != HANDLE_VERSION {
            return None;
        }
        let generation = i64::from_le_bytes(bytes[1..9].try_into().ok()?);
        let writer_epoch = u64::from_le_bytes(bytes[9..17].try_into().ok()?);
        Some(Self {
            generation,
            writer_epoch,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::WriteHandle;

    #[test]
    fn round_trip() {
        let handle = WriteHandle {
            generation: -7,
            writer_epoch: u64::MAX,
        };
        assert_eq!(WriteHandle::decode(&handle.encode()), Some(handle));
    }

    #[test]
    fn rejects_malformed() {
        let mut bytes = WriteHandle {
            generation: 1,
            writer_epoch: 2,
        }
        .encode();
        assert_eq!(WriteHandle::decode(&bytes[..16]), None);
        bytes[0] = 9;
        assert_eq!(WriteHandle::decode(&bytes), None);
        assert_eq!(WriteHandle::decode(&[]), None);
    }
}
