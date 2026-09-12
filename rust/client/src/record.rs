use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Clone, Debug, Eq, PartialEq)]
/// Internal durable envelope for one opaque application record.
pub struct RecordFrame {
    /// Application-owned bytes. The WAL does not interpret this payload.
    pub payload: Bytes,
}

impl RecordFrame {
    /// Bytes the `total_len` prefix adds to every encoded record.
    pub(crate) const HEADER_LEN: usize = 4;
    pub(crate) const MAX_PAYLOAD_BYTES: usize = u32::MAX as usize - Self::HEADER_LEN;

    pub(crate) fn encoded_len(&self) -> Result<usize, RecordError> {
        let total_len = Self::HEADER_LEN
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
    ///
    /// The borrowed input is copied once into shared storage rather than once
    /// per payload. Callers that already own [`Bytes`] should use the internal
    /// owned form to avoid that segment-level copy as well.
    pub fn decode_all(input: &[u8]) -> Result<Vec<Self>, RecordError> {
        Self::decode_all_bytes(Bytes::copy_from_slice(input))
    }

    /// Decode every record from an owned segment without copying its payloads.
    ///
    /// Each returned payload is a slice of `input`, so retaining one record
    /// retains the complete backing allocation. This is intended for complete
    /// segment reads whose records have the same lifetime as the read buffer.
    pub(crate) fn decode_all_bytes(mut input: Bytes) -> Result<Vec<Self>, RecordError> {
        let mut records = Vec::new();
        while !input.is_empty() {
            let (record, consumed) = Self::decode_one_bytes(&input)?;
            records.push(record);
            input.advance(consumed);
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
        let total_len = Self::decoded_len(input)?;
        Ok((
            Self {
                payload: Bytes::copy_from_slice(&input[Self::HEADER_LEN..total_len]),
            },
            total_len,
        ))
    }

    fn decode_one_bytes(input: &Bytes) -> Result<(Self, usize), RecordError> {
        let total_len = Self::decoded_len(input)?;
        Ok((
            Self {
                payload: input.slice(Self::HEADER_LEN..total_len),
            },
            total_len,
        ))
    }

    fn decoded_len(input: &[u8]) -> Result<usize, RecordError> {
        if input.len() < Self::HEADER_LEN {
            return Err(RecordError::Truncated);
        }
        let total_len = u32::from_be_bytes(input[..Self::HEADER_LEN].try_into().unwrap()) as usize;
        if total_len < Self::HEADER_LEN {
            return Err(RecordError::InvalidLength(total_len));
        }
        if input.len() < total_len {
            return Err(RecordError::Truncated);
        }
        Ok(total_len)
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

    #[test]
    fn owned_decode_shares_the_input_allocation() {
        let first = RecordFrame {
            payload: Bytes::from_static(b"alpha"),
        }
        .encode()
        .unwrap();
        let second = RecordFrame {
            payload: Bytes::from_static(b"beta"),
        }
        .encode()
        .unwrap();
        let mut input = BytesMut::with_capacity(first.len() + second.len());
        input.extend_from_slice(&first);
        input.extend_from_slice(&second);
        let input = input.freeze();
        let first_payload = input[RecordFrame::HEADER_LEN..].as_ptr();
        let second_payload = input[first.len() + RecordFrame::HEADER_LEN..].as_ptr();

        let records = RecordFrame::decode_all_bytes(input.clone()).unwrap();

        assert_eq!(records[0].payload.as_ptr(), first_payload);
        assert_eq!(records[1].payload.as_ptr(), second_payload);
        drop(input);
        assert_eq!(records[0].payload.as_ref(), b"alpha");
        assert_eq!(records[1].payload.as_ref(), b"beta");
    }

    #[test]
    fn owned_decode_preserves_validation_errors() {
        for malformed in [
            Bytes::from_static(&[0, 0, 0]),
            Bytes::from_static(&[0, 0, 0, 3]),
            Bytes::from_static(&[0, 0, 0, 8, 1, 2]),
        ] {
            assert_eq!(
                RecordFrame::decode_all_bytes(malformed.clone()).unwrap_err(),
                RecordFrame::decode_all(&malformed).unwrap_err()
            );
        }
    }
}
