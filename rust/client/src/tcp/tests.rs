//! Client tests against a scripted in-process server: each test accepts the
//! connection, decodes client frames with `chorus-wire`, and answers with
//! hand-written server frames.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use chorus_wire::{
    decode_client_frame, encode_server_frame, ClientFrame, Event, ObjectInfo, Request, Response,
    ServerFrame, SessionOpened, WireCode, WireError, WriteHandle, PROTOCOL_VERSION,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::conn::transport_code;
use super::TcpReplicaFactory;
use crate::error::Error;
use crate::transport::{
    AppendToken, PackedAppend, PackedAppendMessage, Replica, ReplicaFactory, TransportCode,
};

const ZONE: usize = 2;
const BUCKET: &str = "zone-c";
const STEP: Duration = Duration::from_secs(5);

struct Server {
    listener: TcpListener,
    addr: String,
}

impl Server {
    async fn bind() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        Self { listener, addr }
    }

    async fn accept(&self) -> Peer {
        let (stream, _) = tokio::time::timeout(STEP, self.listener.accept())
            .await
            .expect("client connects")
            .unwrap();
        Peer {
            stream,
            buf: BytesMut::new(),
        }
    }

    /// Accept a connection and complete the handshake.
    async fn accept_hello(&self) -> Peer {
        let mut peer = self.accept().await;
        let hello = peer.recv().await;
        match hello.body {
            Request::Hello {
                protocol_version, ..
            } => assert_eq!(protocol_version, PROTOCOL_VERSION),
            other => panic!("expected Hello, got {other:?}"),
        }
        peer.reply(
            hello.request_id,
            Ok(Response::HelloOk {
                node_id: "node-1".into(),
                protocol_version: PROTOCOL_VERSION,
            }),
        )
        .await;
        peer
    }

    /// A connected factory and the server side of its connection.
    async fn factory(&self) -> (TcpReplicaFactory, Peer) {
        let (factory, peer) = tokio::join!(
            TcpReplicaFactory::connect(self.addr.clone(), BUCKET, ZONE),
            self.accept_hello()
        );
        (factory.unwrap(), peer)
    }
}

struct Peer {
    stream: TcpStream,
    buf: BytesMut,
}

impl Peer {
    async fn recv(&mut self) -> ClientFrame {
        tokio::time::timeout(STEP, async {
            loop {
                if let Some(frame) = decode_client_frame(&mut self.buf).unwrap() {
                    return frame;
                }
                let read = self.stream.read_buf(&mut self.buf).await.unwrap();
                assert_ne!(read, 0, "client closed the connection");
            }
        })
        .await
        .expect("client sends a frame")
    }

    async fn send(&mut self, frame: ServerFrame) {
        let bytes = encode_server_frame(&frame).unwrap();
        self.stream.write_all(&bytes).await.unwrap();
    }

    async fn reply(&mut self, request_id: u64, body: Result<Response, WireError>) {
        self.send(ServerFrame::Response { request_id, body }).await;
    }

    async fn event(&mut self, event: Event) {
        self.send(ServerFrame::Event(event)).await;
    }
}

fn handle(generation: i64) -> Vec<u8> {
    WriteHandle {
        generation,
        writer_epoch: 1,
    }
    .encode()
}

fn opened(session_id: u64, generation: i64, persisted_size: i64) -> Response {
    Response::SessionOpened(SessionOpened {
        session_id,
        generation,
        metageneration: 1,
        persisted_size,
        write_handle: handle(generation),
    })
}

fn info(name: &str, generation: i64, finalized: bool, size: i64) -> ObjectInfo {
    ObjectInfo {
        name: name.into(),
        generation,
        metageneration: if finalized { 2 } else { 1 },
        size: if finalized { size } else { 0 },
        persisted_size: size,
        finalized,
        crc32c: Some(7),
        last_modified_unix_nanos: Some(1_000_000_123),
        metadata: HashMap::from([("chorus.format".to_string(), "1".to_string())]),
    }
}

/// Open a session on `replica` through `CreateAppendable`, answered by the
/// scripted server with `session_id` at `persisted_size`.
async fn create(
    replica: &Arc<dyn Replica>,
    peer: &mut Peer,
    session_id: u64,
    persisted_size: i64,
) -> AppendToken {
    let (token, ()) = tokio::join!(replica.create_append_session(HashMap::new()), async {
        let frame = peer.recv().await;
        assert!(matches!(frame.body, Request::CreateAppendable { .. }));
        peer.reply(frame.request_id, Ok(opened(session_id, 11, persisted_size)))
            .await;
    });
    token.unwrap()
}

fn message(relative_offset: i64, content: &'static [u8], crc32c: u32) -> PackedAppendMessage {
    PackedAppendMessage {
        relative_offset,
        content: Bytes::from_static(content),
        crc32c,
    }
}

fn packed() -> PackedAppend {
    PackedAppend::new(
        vec![
            message(0, b"abc", 0xdead_beef),
            message(3, b"defgh", 0x0102_0304),
            message(8, b"ij", 0xffff_0000),
        ],
        10,
    )
}

#[tokio::test]
async fn tcp_handshake_reports_node_id() {
    let server = Server::bind().await;
    let (factory, _peer) = server.factory().await;
    assert_eq!(factory.node_id().as_deref(), Some("node-1"));
    assert_eq!(factory.bucket_name(), BUCKET);
}

#[tokio::test]
async fn tcp_handshake_rejection_fails_connect() {
    let server = Server::bind().await;
    let (result, ()) = tokio::join!(
        TcpReplicaFactory::connect(server.addr.clone(), BUCKET, ZONE),
        async {
            let mut peer = server.accept().await;
            let hello = peer.recv().await;
            peer.reply(
                hello.request_id,
                Err(WireError::new(WireCode::Unimplemented, "version 9 only")),
            )
            .await;
        }
    );
    match result {
        Err(Error::Connection(message)) => assert!(message.contains("Unimplemented")),
        other => panic!("expected a connection error, got {other:?}"),
    }
}

#[tokio::test]
async fn tcp_responses_are_matched_by_request_id_out_of_order() {
    let server = Server::bind().await;
    let (factory, mut peer) = server.factory().await;
    let a = factory.replica("a");
    let b = factory.replica("b");
    let (stat_a, stat_b, ()) = tokio::join!(a.stat(), b.stat(), async {
        let first = peer.recv().await;
        let second = peer.recv().await;
        // Answer in reverse order, each with the object it asked about.
        for frame in [second, first] {
            let Request::Stat { bucket, object } = frame.body else {
                panic!("expected Stat");
            };
            assert_eq!(bucket, BUCKET);
            let generation = if object == "a" { 1 } else { 2 };
            peer.reply(
                frame.request_id,
                Ok(Response::Object(info(&object, generation, true, 5))),
            )
            .await;
        }
    });
    let (stat_a, stat_b) = (stat_a.unwrap(), stat_b.unwrap());
    assert_eq!((stat_a.generation, stat_b.generation), (1, 2));
    assert_eq!(stat_a.zone, ZONE);
    // A finalized stat reports the frozen size; bytes are never read.
    assert_eq!(stat_a.persisted_size, 5);
    assert!(stat_a.bytes.is_empty());
}

#[test]
fn tcp_wire_codes_map_one_to_one() {
    for code in WireCode::ALL {
        assert_eq!(format!("{code:?}"), format!("{:?}", transport_code(code)));
    }
}

#[tokio::test]
async fn tcp_errors_carry_mapped_code_and_factory_zone() {
    let server = Server::bind().await;
    let (factory, mut peer) = server.factory().await;
    let replica = factory.replica("missing");
    for code in [
        WireCode::NotFound,
        WireCode::FailedPrecondition,
        WireCode::DataLoss,
    ] {
        let (result, ()) = tokio::join!(replica.stat(), async {
            let frame = peer.recv().await;
            peer.reply(frame.request_id, Err(WireError::new(code, "scripted")))
                .await;
        });
        let error = result.unwrap_err();
        assert_eq!(error.code, transport_code(code));
        assert_eq!(error.zone, ZONE);
        assert_eq!(error.message, "scripted");
    }
    // A create that loses its race is AlreadyExists, a replace FailedPrecondition.
    let (result, ()) = tokio::join!(
        replica.replace_appendable(None, Bytes::new(), HashMap::new()),
        async {
            let frame = peer.recv().await;
            assert!(matches!(
                frame.body,
                Request::ReplaceAppendable { if_match: None, .. }
            ));
            peer.reply(
                frame.request_id,
                Err(WireError::new(WireCode::AlreadyExists, "raced")),
            )
            .await;
        }
    );
    assert_eq!(result.unwrap_err().code, TransportCode::FailedPrecondition);
}

#[tokio::test]
async fn tcp_list_and_snapshot_convert_object_info() {
    let server = Server::bind().await;
    let (factory, mut peer) = server.factory().await;
    let (listed, ()) = tokio::join!(factory.list("wal/"), async {
        let frame = peer.recv().await;
        assert_eq!(
            frame.body,
            Request::List {
                bucket: BUCKET.into(),
                prefix: "wal/".into()
            }
        );
        peer.reply(
            frame.request_id,
            Ok(Response::Listed(vec![info("wal/1", 3, false, 9)])),
        )
        .await;
    });
    let listed = listed.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!((listed[0].zone, listed[0].size), (ZONE, 0));
    assert_eq!(
        listed[0].last_modified,
        Some(std::time::UNIX_EPOCH + Duration::from_nanos(1_000_000_123))
    );

    // A node may answer a read with a prefix; the client reads on from the
    // same generation until the durable tail.
    let replica = factory.replica("wal/1");
    let (snapshot, ()) = tokio::join!(replica.snapshot(), async {
        let frame = peer.recv().await;
        assert!(matches!(frame.body, Request::Read { offset: 0, .. }));
        peer.reply(
            frame.request_id,
            Ok(Response::ReadData {
                info: info("wal/1", 3, false, 9),
                bytes: b"0123".to_vec(),
            }),
        )
        .await;
        let frame = peer.recv().await;
        assert!(matches!(frame.body, Request::Read { offset: 4, .. }));
        peer.reply(
            frame.request_id,
            Ok(Response::ReadData {
                info: info("wal/1", 3, false, 9),
                bytes: b"45678".to_vec(),
            }),
        )
        .await;
    });
    let snapshot = snapshot.unwrap();
    assert_eq!(snapshot.bytes, b"012345678");
    assert_eq!(snapshot.persisted_size, 9);
    assert!(!snapshot.finalized);
}

#[tokio::test]
async fn tcp_lane_send_frames_each_message_without_waiting() {
    let server = Server::bind().await;
    let (factory, mut peer) = server.factory().await;
    let replica = factory.replica("seg");
    let token = create(&replica, &mut peer, 42, 10).await;
    assert_eq!(token.generation, Some(11));
    assert_eq!(token.persisted_size, 10);
    assert_eq!(token.write_handle.as_deref(), Some(&handle(11)[..]));

    // Both calls return while the server has answered nothing.
    replica.lane_send(10, &packed()).await.unwrap();
    replica.lane_send_unflushed(20, &packed()).await.unwrap();
    replica.lane_flush(30).await.unwrap();

    let expected = [
        (10, &b"abc"[..], 0xdead_beef, false),
        (13, b"defgh", 0x0102_0304, false),
        (18, b"ij", 0xffff_0000, true),
        (20, b"abc", 0xdead_beef, false),
        (23, b"defgh", 0x0102_0304, false),
        (28, b"ij", 0xffff_0000, false),
    ];
    for (offset, data, crc32c, flush) in expected {
        let frame = peer.recv().await;
        assert_eq!(
            frame.body,
            Request::Append {
                session_id: 42,
                offset,
                data: data.to_vec(),
                crc32c,
                flush,
            }
        );
        // Append answers are consumed and dropped by the client.
        peer.reply(frame.request_id, Ok(Response::Accepted { size: offset }))
            .await;
    }
    let frame = peer.recv().await;
    assert_eq!(
        frame.body,
        Request::Flush {
            session_id: 42,
            offset: 30
        }
    );

    // The connection still multiplexes normally afterwards.
    let (stat, ()) = tokio::join!(replica.stat(), async {
        let frame = peer.recv().await;
        peer.reply(
            frame.request_id,
            Ok(Response::Object(info("seg", 11, false, 30))),
        )
        .await;
    });
    assert_eq!(
        stat.unwrap().persisted_size,
        0,
        "open-object stat is tail-blind"
    );
}

#[tokio::test]
async fn tcp_durable_event_wakes_lane_durable_change() {
    let server = Server::bind().await;
    let (factory, mut peer) = server.factory().await;
    let replica = factory.replica("seg");
    create(&replica, &mut peer, 7, 10).await;
    let diagnostics = replica.lane_session_diagnostics();
    assert!(diagnostics.session_id.is_some());
    assert_eq!(diagnostics.response_stream_open, Some(true));

    let waiter = tokio::spawn({
        let replica = Arc::clone(&replica);
        async move { replica.lane_durable_change(10).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiter.is_finished());
    // Events for other sessions are ignored.
    peer.event(Event::Durable {
        session_id: 99,
        persisted_size: 50,
    })
    .await;
    peer.event(Event::Durable {
        session_id: 7,
        persisted_size: 16,
    })
    .await;
    let change = tokio::time::timeout(STEP, waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(change.persisted_size, 16);
    assert!(change.error.is_none());
    // Already beyond `seen`: answers immediately.
    let change = replica.lane_durable_change(12).await.unwrap();
    assert_eq!(change.persisted_size, 16);
}

#[tokio::test]
async fn tcp_session_failed_surfaces_as_error() {
    let server = Server::bind().await;
    let (factory, mut peer) = server.factory().await;
    let replica = factory.replica("seg");
    create(&replica, &mut peer, 7, 10).await;

    let waiter = tokio::spawn({
        let replica = Arc::clone(&replica);
        async move { replica.lane_durable_change(10).await }
    });
    peer.event(Event::SessionFailed {
        session_id: 7,
        error: WireError::new(WireCode::FailedPrecondition, "fenced by takeover"),
    })
    .await;
    let error = tokio::time::timeout(STEP, waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code, TransportCode::FailedPrecondition);
    assert_eq!(error.zone, ZONE);
    // The dead session is cleared: lanes must resume before sending again.
    assert!(replica.lane_session_diagnostics().session_id.is_none());
    let error = replica.lane_send(10, &packed()).await.unwrap_err();
    assert_eq!(error.code, TransportCode::Unavailable);
}

#[tokio::test]
async fn tcp_durable_progress_and_failure_are_reported_together() {
    let server = Server::bind().await;
    let (factory, mut peer) = server.factory().await;
    let replica = factory.replica("seg");
    create(&replica, &mut peer, 7, 10).await;
    peer.event(Event::Durable {
        session_id: 7,
        persisted_size: 20,
    })
    .await;
    peer.event(Event::SessionFailed {
        session_id: 7,
        error: WireError::new(WireCode::OutOfRange, "bad offset"),
    })
    .await;
    // A round trip after the events guarantees the reader routed them.
    let (_, ()) = tokio::join!(replica.stat(), async {
        let frame = peer.recv().await;
        peer.reply(
            frame.request_id,
            Ok(Response::Object(info("seg", 11, false, 20))),
        )
        .await;
    });
    let change = replica.lane_durable_change(10).await.unwrap();
    assert_eq!(change.persisted_size, 20);
    assert_eq!(change.error.unwrap().code, TransportCode::OutOfRange);
}

#[tokio::test]
async fn tcp_connection_drop_fails_pending_calls_and_reconnects() {
    let server = Server::bind().await;
    let (factory, mut peer) = server.factory().await;
    let replica = factory.replica("seg");
    let mut token = create(&replica, &mut peer, 7, 10).await;

    let waiter = tokio::spawn({
        let replica = Arc::clone(&replica);
        async move { replica.lane_durable_change(10).await }
    });
    let (stat, ()) = tokio::join!(replica.stat(), async move {
        let frame = peer.recv().await;
        assert!(matches!(frame.body, Request::Stat { .. }));
        // The node goes away with the stat outstanding.
        drop(peer);
    });
    assert_eq!(stat.unwrap_err().code, TransportCode::Unavailable);
    let error = tokio::time::timeout(STEP, waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code, TransportCode::Unavailable);
    assert_eq!(factory.node_id(), None);

    // The next call reconnects; the lane resumes with its write handle.
    let (tail, mut peer) = tokio::join!(replica.resume_tail(&mut token), async {
        let mut peer = server.accept_hello().await;
        let frame = peer.recv().await;
        assert_eq!(
            frame.body,
            Request::Resume {
                bucket: BUCKET.into(),
                object: "seg".into(),
                write_handle: handle(11),
            }
        );
        peer.reply(frame.request_id, Ok(opened(8, 11, 30))).await;
        peer
    });
    assert_eq!(tail.unwrap(), 30);
    assert_eq!(token.persisted_size, 30);
    replica.lane_flush(30).await.unwrap();
    assert_eq!(
        peer.recv().await.body,
        Request::Flush {
            session_id: 8,
            offset: 30
        }
    );
}

#[tokio::test]
async fn tcp_finalize_uses_the_live_session_handle() {
    let server = Server::bind().await;
    let (factory, mut peer) = server.factory().await;
    let replica = factory.replica("seg");
    let mut token = create(&replica, &mut peer, 7, 0).await;
    let (finalized, ()) = tokio::join!(replica.finalize(&mut token, 40), async {
        let frame = peer.recv().await;
        assert_eq!(
            frame.body,
            Request::Finalize {
                bucket: BUCKET.into(),
                object: "seg".into(),
                generation: 11,
                write_offset: 40,
                write_handle: Some(handle(11)),
            }
        );
        peer.reply(
            frame.request_id,
            Ok(Response::Object(info("seg", 11, true, 40))),
        )
        .await;
    });
    let finalized = finalized.unwrap();
    assert!(finalized.finalized);
    assert_eq!(finalized.persisted_size, 40);
    assert_eq!(token.metageneration, Some(2));
    assert!(replica.lane_session_diagnostics().session_id.is_none());

    // Without a live session the retry goes by handle; a mismatched answer
    // is never accepted.
    let (result, ()) = tokio::join!(replica.finalize(&mut token, 40), async {
        // The finalize closed the session; nothing else was sent since.
        let frame = peer.recv().await;
        assert!(matches!(
            frame.body,
            Request::Finalize {
                write_handle: Some(_),
                ..
            }
        ));
        peer.reply(
            frame.request_id,
            Ok(Response::Object(info("seg", 11, true, 39))),
        )
        .await;
    });
    assert_eq!(result.unwrap_err().code, TransportCode::DataLoss);
}
