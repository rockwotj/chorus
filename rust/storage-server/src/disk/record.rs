//! On-disk records: `node.meta` and the per-object `.meta` files.
//!
//! Each file holds one checksummed rkyv archive:
//!
//! ```text
//! [u8; 4]  magic (b"CHND" for node.meta, b"CHOM" for an object meta)
//! u32 LE   body length
//! u32 LE   crc32c(body)
//! body     rkyv archive (validated with bytecheck on load)
//! ```

use std::collections::HashMap;
use std::io;

use rkyv::rancor;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

use crate::backend::{ObjectKey, ObjectMeta};

const NODE_MAGIC: [u8; 4] = *b"CHND";
const OBJECT_MAGIC: [u8; 4] = *b"CHOM";
const HEADER_BYTES: usize = 12;

/// Contents of `node.meta`.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct NodeRecord {
    /// Stable node identity reported in `HelloOk`.
    pub node_id: String,
    /// Every generation this node ever handed out is at most this value.
    pub generation_high_water: i64,
}

/// Contents of an object's `.meta` file: the real names (the file name may be
/// a hash) plus [`ObjectMeta`].
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ObjectRecord {
    bucket: String,
    name: String,
    generation: i64,
    metageneration: i64,
    metadata: HashMap<String, String>,
    finalized: bool,
    finalized_size: u64,
    finalized_crc32c: u32,
    writer_epoch: u64,
    last_modified_unix_nanos: i64,
}

impl ObjectRecord {
    /// The record for `meta` of `key`.
    pub fn new(key: &ObjectKey, meta: &ObjectMeta) -> Self {
        Self {
            bucket: key.bucket.clone(),
            name: key.name.clone(),
            generation: meta.generation,
            metageneration: meta.metageneration,
            metadata: meta.metadata.clone(),
            finalized: meta.finalized,
            finalized_size: meta.finalized_size,
            finalized_crc32c: meta.finalized_crc32c,
            writer_epoch: meta.writer_epoch,
            last_modified_unix_nanos: meta.last_modified_unix_nanos,
        }
    }

    /// Split back into key and metadata.
    pub fn into_parts(self) -> (ObjectKey, ObjectMeta) {
        (
            ObjectKey::new(self.bucket, self.name),
            ObjectMeta {
                generation: self.generation,
                metageneration: self.metageneration,
                metadata: self.metadata,
                finalized: self.finalized,
                finalized_size: self.finalized_size,
                finalized_crc32c: self.finalized_crc32c,
                writer_epoch: self.writer_epoch,
                last_modified_unix_nanos: self.last_modified_unix_nanos,
            },
        )
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn frame(magic: [u8; 4], body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_BYTES + body.len());
    out.extend_from_slice(&magic);
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(body).to_le_bytes());
    out.extend_from_slice(body);
    out
}

/// Check the header and checksum; return the body in an aligned buffer.
fn unframe(magic: [u8; 4], bytes: &[u8]) -> io::Result<AlignedVec> {
    if bytes.len() < HEADER_BYTES || bytes[..4] != magic {
        return Err(invalid("bad magic or truncated header"));
    }
    let len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let body = &bytes[HEADER_BYTES..];
    if body.len() != len {
        return Err(invalid(format!(
            "body is {} bytes, header says {len}",
            body.len()
        )));
    }
    if crc32c::crc32c(body) != crc {
        return Err(invalid("checksum mismatch"));
    }
    let mut aligned = AlignedVec::with_capacity(len);
    aligned.extend_from_slice(body);
    Ok(aligned)
}

/// Serialize `node.meta`.
pub fn encode_node(record: &NodeRecord) -> io::Result<Vec<u8>> {
    let body = rkyv::to_bytes::<rancor::Error>(record).map_err(|e| invalid(e.to_string()))?;
    Ok(frame(NODE_MAGIC, &body))
}

/// Parse and verify `node.meta`.
pub fn decode_node(bytes: &[u8]) -> io::Result<NodeRecord> {
    let body = unframe(NODE_MAGIC, bytes)?;
    rkyv::from_bytes::<NodeRecord, rancor::Error>(&body).map_err(|e| invalid(e.to_string()))
}

/// Serialize an object's `.meta`.
pub fn encode_object(record: &ObjectRecord) -> io::Result<Vec<u8>> {
    let body = rkyv::to_bytes::<rancor::Error>(record).map_err(|e| invalid(e.to_string()))?;
    Ok(frame(OBJECT_MAGIC, &body))
}

/// Parse and verify an object's `.meta`.
pub fn decode_object(bytes: &[u8]) -> io::Result<ObjectRecord> {
    let body = unframe(OBJECT_MAGIC, bytes)?;
    rkyv::from_bytes::<ObjectRecord, rancor::Error>(&body).map_err(|e| invalid(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip_and_detect_corruption() {
        let key = ObjectKey::new("bucket", "a/b");
        let meta = ObjectMeta {
            generation: 42,
            metageneration: 2,
            metadata: HashMap::from([("k".into(), "v".into())]),
            finalized: true,
            finalized_size: 7,
            finalized_crc32c: 9,
            writer_epoch: 3,
            last_modified_unix_nanos: 11,
        };
        let bytes = encode_object(&ObjectRecord::new(&key, &meta)).unwrap();
        let (k, m) = decode_object(&bytes).unwrap().into_parts();
        assert_eq!((k, m), (key, meta));

        let mut corrupt = bytes.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        assert!(decode_object(&corrupt).is_err());
        assert!(decode_object(&bytes[..bytes.len() - 1]).is_err());
        assert!(decode_node(&bytes).is_err(), "wrong magic");

        let node = NodeRecord {
            node_id: "n".into(),
            generation_high_water: 5,
        };
        assert_eq!(decode_node(&encode_node(&node).unwrap()).unwrap(), node);
    }
}
