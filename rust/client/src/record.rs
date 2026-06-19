use bytes::{BufMut, Bytes, BytesMut};

const HEADER_LEN: usize = 4;

#[derive(Clone, Debug, Eq, PartialEq)]
/// Internal durable envelope for one opaque application record.
pub struct RecordFrame {
    /// Application-owned bytes. The WAL does not interpret this payload.
    pub payload: Bytes,
}

impl RecordFrame {
    pub(crate) const MAX_PAYLOAD_BYTES: usize = u32::MAX as usize - HEADER_LEN;

    pub(crate) fn encoded_len(&self) -> Result<usize, RecordError> {
        let total_len = HEADER_LEN
            .checked_add(self.payload.len())
            .ok_or(RecordError::TooLarge)?;
        u32::try_from(total_len).map_err(|_| RecordError::TooLarge)?;
        Ok(total_len)
    }

    /// Encode one self-delimiting durable record.
    ///
    /// The layout is `total_len: u32 | payload`. The enclosing segment's
    /// `chorus.format` metadata selects this encoding, and GCS validates object
    /// checksums, so the record does not duplicate a version or checksum.
    pub fn encode(&self) -> Result<Bytes, RecordError> {
        let total_len = self.encoded_len()?;
        let mut output = BytesMut::with_capacity(total_len);
        output.put_u32(total_len as u32);
        output.extend_from_slice(&self.payload);
        Ok(output.freeze())
    }

    /// Decode every record in a complete segment byte slice.
    pub fn decode_all(mut input: &[u8]) -> Result<Vec<Self>, RecordError> {
        let mut records = Vec::new();
        while !input.is_empty() {
            let (record, consumed) = Self::decode_one(input)?;
            records.push(record);
            input = &input[consumed..];
        }
        Ok(records)
    }

    /// Decode the contiguous well-formed prefix of an appendable object.
    ///
    /// A partial or malformed tail terminates the prefix. Recovery never scans
    /// beyond that point looking for a later frame because doing so would turn
    /// a gap into silently reordered WAL history.
    pub fn decode_complete_prefix(mut input: &[u8]) -> (Vec<Self>, usize) {
        let mut records = Vec::new();
        let mut consumed = 0usize;
        while !input.is_empty() {
            let Ok((record, record_len)) = Self::decode_one(input) else {
                break;
            };
            records.push(record);
            consumed += record_len;
            input = &input[record_len..];
        }
        (records, consumed)
    }

    fn decode_one(input: &[u8]) -> Result<(Self, usize), RecordError> {
        if input.len() < HEADER_LEN {
            return Err(RecordError::Truncated);
        }
        let total_len = u32::from_be_bytes(input[..HEADER_LEN].try_into().unwrap()) as usize;
        if total_len < HEADER_LEN {
            return Err(RecordError::InvalidLength(total_len));
        }
        if input.len() < total_len {
            return Err(RecordError::Truncated);
        }
        Ok((
            Self {
                payload: Bytes::copy_from_slice(&input[HEADER_LEN..total_len]),
            },
            total_len,
        ))
    }
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
/// Structural failure while encoding or decoding a durable record.
pub enum RecordError {
    /// Input ended before the declared record boundary.
    #[error("truncated record")]
    Truncated,
    /// The total length is smaller than the fixed header.
    #[error("invalid record length {0}")]
    InvalidLength(usize),
    /// The payload and framing cannot fit in the wire format.
    #[error("record exceeds u32 bytes")]
    TooLarge,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_multiple_records() {
        let expected = vec![
            RecordFrame {
                payload: Bytes::from_static(b"alpha"),
            },
            RecordFrame {
                payload: Bytes::from_static(b"beta"),
            },
        ];
        let bytes: Vec<u8> = expected
            .iter()
            .flat_map(|record| record.encode().unwrap())
            .collect();
        assert_eq!(RecordFrame::decode_all(&bytes).unwrap(), expected);
    }

    #[test]
    fn prefix_decode_stops_at_a_partial_or_malformed_tail() {
        let first = RecordFrame {
            payload: Bytes::from_static(b"first"),
        }
        .encode()
        .unwrap();
        let mut partial = RecordFrame {
            payload: Bytes::from_static(b"second"),
        }
        .encode()
        .unwrap()
        .to_vec();
        partial.truncate(partial.len() - 2);
        let bytes = [first.as_ref(), partial.as_slice()].concat();
        let (records, consumed) = RecordFrame::decode_complete_prefix(&bytes);
        assert_eq!(records.len(), 1);
        assert_eq!(consumed, first.len());
    }

    #[test]
    fn empty_payload_is_a_valid_record() {
        let encoded = RecordFrame {
            payload: Bytes::new(),
        }
        .encode()
        .unwrap();
        assert_eq!(
            RecordFrame::decode_all(&encoded).unwrap(),
            vec![RecordFrame {
                payload: Bytes::new()
            }]
        );
    }
}
