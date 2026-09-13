//! Private, versioned batch encoding. This is not SlateDB's native SST format.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use slatedb::wal::WalError;
use slatedb::{RowEntry, ValueDeletable};

use super::{data_error, internal_error};

// Magic + format version. Changing layout requires a new version/decoder.
const MAGIC: &[u8; 8] = b"CHSLWAL\x01";

pub(super) fn encode(rows: &[RowEntry], max_bytes: usize) -> Result<Bytes, WalError> {
    let first = rows
        .first()
        .ok_or_else(|| internal_error("empty SlateDB write batch"))?;
    let count = u32::try_from(rows.len()).map_err(|_| internal_error("too many batch rows"))?;
    let mut size = MAGIC.len() + 4 + 8;
    for row in rows {
        if row.seq != first.seq {
            return Err(internal_error(
                "all rows in a SlateDB batch must share a sequence",
            ));
        }
        u32::try_from(row.key.len()).map_err(|_| internal_error("key too large"))?;
        u32::try_from(row.value.len()).map_err(|_| internal_error("value too large"))?;
        let metadata = 6
            + usize::from(row.create_ts.is_some()) * 8
            + usize::from(row.expire_ts.is_some()) * 8
            + if row.value.is_tombstone() { 0 } else { 4 };
        size = size
            .checked_add(metadata)
            .and_then(|n| n.checked_add(row.key.len()))
            .and_then(|n| n.checked_add(row.value.len()))
            .ok_or_else(|| internal_error("batch size overflow"))?;
    }
    if size > max_bytes {
        return Err(internal_error(&format!(
            "encoded SlateDB batch is {size} bytes, exceeding the {max_bytes}-byte record limit"
        )));
    }
    let mut bytes = BytesMut::with_capacity(size);
    bytes.extend_from_slice(MAGIC);
    bytes.put_u32(count);
    bytes.put_u64(first.seq);
    for row in rows {
        bytes.put_u32(row.key.len() as u32);
        bytes.extend_from_slice(&row.key);
        bytes.put_u8(match row.value {
            ValueDeletable::Value(_) => 0,
            ValueDeletable::Merge(_) => 1,
            ValueDeletable::Tombstone => 2,
        });
        bytes.put_u8(u8::from(row.create_ts.is_some()) | (u8::from(row.expire_ts.is_some()) << 1));
        if let Some(ts) = row.create_ts {
            bytes.put_i64(ts);
        }
        if let Some(ts) = row.expire_ts {
            bytes.put_i64(ts);
        }
        if let Some(value) = row.value.as_bytes() {
            bytes.put_u32(value.len() as u32);
            bytes.extend_from_slice(&value);
        }
    }
    debug_assert_eq!(size, bytes.len());
    Ok(bytes.freeze())
}

fn take(bytes: &mut Bytes, count: usize) -> Result<Bytes, WalError> {
    if bytes.len() < count {
        return Err(data_error("truncated SlateDB WAL batch"));
    }
    Ok(bytes.split_to(count))
}

fn field(bytes: &mut Bytes) -> Result<Bytes, WalError> {
    let size = take(bytes, 4)?.get_u32() as usize;
    take(bytes, size)
}

pub(super) fn decode(mut bytes: Bytes) -> Result<Vec<RowEntry>, WalError> {
    if take(&mut bytes, MAGIC.len())?.as_ref() != MAGIC {
        return Err(data_error("unknown Chorus SlateDB WAL format/version"));
    }
    let count = take(&mut bytes, 4)?.get_u32() as usize;
    let seq = take(&mut bytes, 8)?.get_u64();
    if count == 0 || count > bytes.len() / 6 {
        return Err(data_error("invalid SlateDB WAL row count"));
    }
    // Do not allocate from an untrusted count; each row must decode first.
    let mut rows = Vec::new();
    for _ in 0..count {
        let key = field(&mut bytes)?;
        let tag = take(&mut bytes, 1)?.get_u8();
        let flags = take(&mut bytes, 1)?.get_u8();
        if flags & !3 != 0 {
            return Err(data_error("unknown SlateDB WAL row flags"));
        }
        let create_ts = if flags & 1 != 0 {
            Some(take(&mut bytes, 8)?.get_i64())
        } else {
            None
        };
        let expire_ts = if flags & 2 != 0 {
            Some(take(&mut bytes, 8)?.get_i64())
        } else {
            None
        };
        let value = match tag {
            0 => ValueDeletable::Value(field(&mut bytes)?),
            1 => ValueDeletable::Merge(field(&mut bytes)?),
            2 => ValueDeletable::Tombstone,
            _ => return Err(data_error("unknown SlateDB WAL value tag")),
        };
        rows.push(RowEntry {
            key,
            value,
            seq,
            create_ts,
            expire_ts,
        });
    }
    if !bytes.is_empty() {
        return Err(data_error("trailing bytes in SlateDB WAL batch"));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows() -> Vec<RowEntry> {
        [
            ValueDeletable::Value(Bytes::from_static(b"\x00value")),
            ValueDeletable::Merge(Bytes::new()),
            ValueDeletable::Tombstone,
        ]
        .into_iter()
        .enumerate()
        .map(|(i, value)| RowEntry {
            key: Bytes::from(vec![0, i as u8, 255]),
            value,
            seq: u64::MAX,
            create_ts: Some(i64::MIN),
            expire_ts: if i == 0 { None } else { Some(i64::MAX) },
        })
        .collect()
    }

    #[test]
    fn round_trip_all_row_fields_and_exact_size_limit() {
        let rows = rows();
        let encoded = encode(&rows, usize::MAX).unwrap();
        assert_eq!(decode(encoded.clone()).unwrap(), rows);
        assert_eq!(encode(&rows, encoded.len()).unwrap(), encoded);
        assert!(encode(&rows, encoded.len() - 1).is_err());
    }

    #[test]
    fn rejects_truncation_versions_counts_tags_flags_and_trailing_data() {
        let encoded = encode(&rows(), usize::MAX).unwrap();
        for end in 0..encoded.len() {
            assert!(decode(encoded.slice(..end)).is_err(), "end={end}");
        }
        for (offset, value) in [(7, 2), (8, 255), (27, 255), (28, 255)] {
            let mut corrupted = encoded.to_vec();
            corrupted[offset] = value;
            assert!(matches!(
                decode(corrupted.into()),
                Err(WalError::DataError(_))
            ));
        }
        let mut trailing = encoded.to_vec();
        trailing.push(0);
        assert!(decode(trailing.into()).is_err());
    }

    #[test]
    fn rejects_empty_and_mixed_sequence_batches() {
        assert!(encode(&[], usize::MAX).is_err());
        let mut rows = rows();
        rows[1].seq = 1;
        assert!(encode(&rows, usize::MAX).is_err());
    }
}
