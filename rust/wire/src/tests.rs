use std::collections::HashMap;

use bytes::BytesMut;

use crate::*;

fn metadata() -> HashMap<String, String> {
    HashMap::from([
        ("chorus.format".to_string(), "1".to_string()),
        ("k".to_string(), String::new()),
    ])
}

fn info(finalized: bool) -> ObjectInfo {
    ObjectInfo {
        name: "wal/segment-0001".into(),
        generation: 1_700_000_000_000_001,
        metageneration: if finalized { 2 } else { 1 },
        size: if finalized { 5 } else { 0 },
        persisted_size: 5,
        finalized,
        crc32c: Some(crc32c::crc32c(b"hello")),
        last_modified_unix_nanos: Some(1_700_000_000_000_000_123),
        metadata: metadata(),
    }
}

fn handle() -> Vec<u8> {
    WriteHandle {
        generation: 42,
        writer_epoch: 3,
    }
    .encode()
}

fn all_requests() -> Vec<Request> {
    vec![
        Request::Hello {
            protocol_version: PROTOCOL_VERSION,
            client_name: "test".into(),
        },
        Request::List {
            bucket: "b".into(),
            prefix: "wal/".into(),
        },
        Request::Stat {
            bucket: "b".into(),
            object: "o".into(),
        },
        Request::Read {
            bucket: "b".into(),
            object: "o".into(),
            offset: 17,
        },
        Request::CreateAppendable {
            bucket: "b".into(),
            object: "o".into(),
            metadata: metadata(),
        },
        Request::Takeover {
            bucket: "b".into(),
            object: "o".into(),
            if_match: ObjectVersion {
                generation: 42,
                metageneration: 1,
            },
        },
        Request::Resume {
            bucket: "b".into(),
            object: "o".into(),
            write_handle: handle(),
        },
        Request::ReplaceAppendable {
            bucket: "b".into(),
            object: "o".into(),
            if_match: Some(ObjectVersion {
                generation: 42,
                metageneration: 2,
            }),
            data: b"canonical".to_vec(),
            metadata: metadata(),
        },
        Request::ReplaceAppendable {
            bucket: "b".into(),
            object: "o".into(),
            if_match: None,
            data: Vec::new(),
            metadata: HashMap::new(),
        },
        Request::Append {
            session_id: 9,
            offset: 1024,
            data: (0..=255u8).collect(),
            crc32c: 0xdead_beef,
            flush: true,
        },
        Request::Flush {
            session_id: 9,
            offset: 1280,
        },
        Request::Finalize {
            bucket: "b".into(),
            object: "o".into(),
            generation: 42,
            write_offset: 1280,
            write_handle: Some(handle()),
        },
        Request::Finalize {
            bucket: "b".into(),
            object: "o".into(),
            generation: 42,
            write_offset: 0,
            write_handle: None,
        },
        Request::Delete {
            bucket: "b".into(),
            object: "o".into(),
            generation: 42,
        },
        Request::CloseSession { session_id: 9 },
    ]
}

fn all_server_frames() -> Vec<ServerFrame> {
    let responses = vec![
        Response::HelloOk {
            node_id: "node-a".into(),
            protocol_version: PROTOCOL_VERSION,
        },
        Response::Listed(vec![info(false), info(true)]),
        Response::Listed(Vec::new()),
        Response::Object(info(true)),
        Response::ReadData {
            info: info(false),
            bytes: b"hello".to_vec(),
        },
        Response::SessionOpened(SessionOpened {
            session_id: 9,
            generation: 42,
            metageneration: 1,
            persisted_size: 1024,
            write_handle: handle(),
        }),
        Response::Accepted { size: 1280 },
        Response::Deleted,
        Response::SessionClosed,
    ];
    let mut frames: Vec<ServerFrame> = responses
        .into_iter()
        .enumerate()
        .map(|(i, response)| ServerFrame::Response {
            request_id: i as u64,
            body: Ok(response),
        })
        .collect();
    for (i, code) in WireCode::ALL.into_iter().enumerate() {
        frames.push(ServerFrame::Response {
            request_id: 1000 + i as u64,
            body: Err(WireError::new(code, format!("{code:?} happened"))),
        });
    }
    frames.push(ServerFrame::Event(Event::Durable {
        session_id: 9,
        persisted_size: 1280,
    }));
    frames.push(ServerFrame::Event(Event::SessionFailed {
        session_id: 9,
        error: WireError::new(
            WireCode::FailedPrecondition,
            "A different writer has become the exclusive writer of this object.",
        ),
    }));
    frames
}

fn all_client_frames() -> Vec<ClientFrame> {
    all_requests()
        .into_iter()
        .enumerate()
        .map(|(i, body)| ClientFrame {
            request_id: u64::MAX - i as u64,
            body,
        })
        .collect()
}

#[test]
fn every_request_round_trips() {
    for frame in all_client_frames() {
        let bytes = encode_client_frame(&frame).unwrap();
        let mut buf = BytesMut::from(&bytes[..]);
        assert_eq!(decode_client_frame(&mut buf).unwrap(), Some(frame));
        assert!(buf.is_empty());
    }
}

#[test]
fn every_response_and_event_round_trips() {
    for frame in all_server_frames() {
        let bytes = encode_server_frame(&frame).unwrap();
        let mut buf = BytesMut::from(&bytes[..]);
        assert_eq!(decode_server_frame(&mut buf).unwrap(), Some(frame));
        assert!(buf.is_empty());
    }
}

#[test]
fn back_to_back_frames_fed_one_byte_at_a_time() {
    let frames = all_client_frames();
    let stream: Vec<u8> = frames
        .iter()
        .flat_map(|frame| encode_client_frame(frame).unwrap())
        .collect();
    let mut buf = BytesMut::new();
    let mut decoded = Vec::new();
    for byte in stream {
        buf.extend_from_slice(&[byte]);
        while let Some(frame) = decode_client_frame(&mut buf).unwrap() {
            decoded.push(frame);
        }
    }
    assert_eq!(decoded, frames);
    assert!(buf.is_empty());

    let frames = all_server_frames();
    let stream: Vec<u8> = frames
        .iter()
        .flat_map(|frame| encode_server_frame(frame).unwrap())
        .collect();
    let mut decoded = Vec::new();
    for byte in stream {
        buf.extend_from_slice(&[byte]);
        while let Some(frame) = decode_server_frame(&mut buf).unwrap() {
            decoded.push(frame);
        }
    }
    assert_eq!(decoded, frames);
}

#[test]
fn partial_frame_consumes_nothing() {
    let bytes = encode_client_frame(&all_client_frames()[0]).unwrap();
    for cut in 0..bytes.len() {
        let mut buf = BytesMut::from(&bytes[..cut]);
        assert_eq!(decode_client_frame(&mut buf).unwrap(), None);
        assert_eq!(buf.len(), cut);
    }
}

#[test]
fn header_layout_is_len_then_crc() {
    let bytes = encode_client_frame(&all_client_frames()[1]).unwrap();
    let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    assert_eq!(len, bytes.len() - FRAME_HEADER_LEN);
    assert_eq!(crc, crc32c::crc32c(&bytes[FRAME_HEADER_LEN..]));
}

#[test]
fn corrupted_body_fails_checksum() {
    let mut bytes = encode_client_frame(&all_client_frames()[9]).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x01;
    let mut buf = BytesMut::from(&bytes[..]);
    assert!(matches!(
        decode_client_frame(&mut buf),
        Err(FrameError::BadChecksum { .. })
    ));
}

#[test]
fn corrupted_crc_field_fails_checksum() {
    let mut bytes = encode_server_frame(&all_server_frames()[0]).unwrap();
    bytes[5] ^= 0x80;
    let mut buf = BytesMut::from(&bytes[..]);
    assert!(matches!(
        decode_server_frame(&mut buf),
        Err(FrameError::BadChecksum { .. })
    ));
}

#[test]
fn oversized_length_is_rejected_without_waiting_for_the_body() {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&((MAX_FRAME_LEN as u32) + 1).to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes());
    assert!(matches!(
        decode_client_frame(&mut buf),
        Err(FrameError::TooLarge { .. })
    ));
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&u32::MAX.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes());
    assert!(matches!(
        decode_server_frame(&mut buf),
        Err(FrameError::TooLarge { .. })
    ));
}

#[test]
fn oversized_append_is_rejected_on_encode() {
    let frame = ClientFrame {
        request_id: 1,
        body: Request::Append {
            session_id: 1,
            offset: 0,
            data: vec![0; MAX_FRAME_LEN + 1],
            crc32c: 0,
            flush: false,
        },
    };
    assert!(matches!(
        encode_client_frame(&frame),
        Err(FrameError::TooLarge { .. })
    ));
}

fn raw_frame(body: &[u8]) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
    buf.extend_from_slice(&crc32c::crc32c(body).to_le_bytes());
    buf.extend_from_slice(body);
    buf
}

#[test]
fn garbage_body_with_valid_crc_is_invalid_archive() {
    for body in [&[][..], &[0xff; 3][..], &[0xff; 64][..], &[0x5a; 257][..]] {
        let mut buf = raw_frame(body);
        assert!(
            matches!(
                decode_client_frame(&mut buf),
                Err(FrameError::InvalidArchive(_))
            ),
            "client body {body:?}"
        );
        assert!(buf.is_empty(), "invalid frame is consumed");
        let mut buf = raw_frame(body);
        assert!(
            matches!(
                decode_server_frame(&mut buf),
                Err(FrameError::InvalidArchive(_))
            ),
            "server body {body:?}"
        );
    }
}

#[test]
fn server_frame_is_not_a_client_frame() {
    // A well-formed archive of the wrong root type must not validate as a
    // request whose layout it does not match (here: an event whose tag is
    // past the end of `Request`'s tags fails validation, or decodes to some
    // other value — never panics).
    let bytes = encode_server_frame(&ServerFrame::Event(Event::SessionFailed {
        session_id: 1,
        error: WireError::new(WireCode::Internal, "x"),
    }))
    .unwrap();
    let mut buf = BytesMut::from(&bytes[..]);
    let _ = decode_client_frame(&mut buf);
}

#[test]
fn truncated_archive_with_valid_crc_never_decodes_to_the_original() {
    // rkyv archives are not self-delimiting: a truncated body may still be a
    // valid (different) archive. The CRC is the integrity check; validation
    // only guarantees decoding is memory safe and never panics.
    let frame = all_client_frames()[4].clone();
    let bytes = encode_client_frame(&frame).unwrap();
    let body = &bytes[FRAME_HEADER_LEN..];
    for cut in 0..body.len() {
        let mut buf = raw_frame(&body[..cut]);
        match decode_client_frame(&mut buf) {
            Err(FrameError::InvalidArchive(_)) => {}
            Ok(Some(decoded)) => assert_ne!(decoded, frame),
            other => panic!("unexpected {other:?}"),
        }
        assert!(buf.is_empty());
    }
}

#[test]
fn misaligned_input_buffer_still_decodes() {
    let bytes = encode_server_frame(&all_server_frames()[4]).unwrap();
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&[0u8; 3]);
    buf.extend_from_slice(&bytes);
    let _ = buf.split_to(3);
    assert_eq!(
        decode_server_frame(&mut buf).unwrap(),
        Some(all_server_frames()[4].clone())
    );
}
