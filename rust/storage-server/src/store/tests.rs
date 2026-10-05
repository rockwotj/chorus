//! Store semantics, driven synchronously (the memory backend never waits).

use std::cell::RefCell;
use std::collections::HashMap;

use chorus_wire::{
    Event, ObjectInfo, ObjectVersion, Request, Response, ServerFrame, SessionOpened, WireCode,
    WriteHandle,
};
use futures::channel::mpsc;
use futures::executor::block_on;

use super::{ConnCtx, Reply, Store, READ_CHUNK_BYTES};
use crate::backend::MemoryBackend;

const BUCKET: &str = "b";

struct Conn {
    ctx: ConnCtx,
    frames: RefCell<mpsc::UnboundedReceiver<ServerFrame>>,
}

impl Conn {
    /// Drain the events queued for this connection.
    fn events(&self) -> Vec<Event> {
        let mut events = Vec::new();
        while let Ok(frame) = self.frames.borrow_mut().try_recv() {
            if let ServerFrame::Event(event) = frame {
                events.push(event);
            }
        }
        events
    }
}

struct Harness {
    store: Store<MemoryBackend>,
}

impl Harness {
    fn new() -> Self {
        Self {
            store: block_on(Store::open("node-test", MemoryBackend::new())).unwrap(),
        }
    }

    fn conn(&self) -> Conn {
        let (outbox, frames) = mpsc::unbounded();
        Conn {
            ctx: ConnCtx {
                id: self.store.new_connection_id(),
                outbox,
            },
            frames: RefCell::new(frames),
        }
    }

    fn call(&self, conn: &Conn, request: Request) -> Reply {
        block_on(self.store.handle(&conn.ctx, request))
    }

    fn opened(&self, conn: &Conn, request: Request) -> SessionOpened {
        match self.call(conn, request) {
            Ok(Response::SessionOpened(opened)) => opened,
            other => panic!("expected SessionOpened, got {other:?}"),
        }
    }

    fn create(&self, conn: &Conn, name: &str) -> SessionOpened {
        self.opened(
            conn,
            Request::CreateAppendable {
                bucket: BUCKET.into(),
                object: name.into(),
                metadata: HashMap::from([("k".to_string(), "v".to_string())]),
            },
        )
    }

    fn append(&self, conn: &Conn, session: u64, offset: i64, data: &[u8], flush: bool) -> Reply {
        self.call(
            conn,
            Request::Append {
                session_id: session,
                offset,
                data: data.to_vec(),
                crc32c: crc32c::crc32c(data),
                flush,
            },
        )
    }

    fn stat(&self, conn: &Conn, name: &str) -> Result<ObjectInfo, WireCode> {
        match self.call(
            conn,
            Request::Stat {
                bucket: BUCKET.into(),
                object: name.into(),
            },
        ) {
            Ok(Response::Object(info)) => Ok(info),
            Ok(other) => panic!("unexpected {other:?}"),
            Err(error) => Err(error.code),
        }
    }

    fn read(
        &self,
        conn: &Conn,
        name: &str,
        offset: i64,
    ) -> Result<(ObjectInfo, Vec<u8>), WireCode> {
        match self.call(
            conn,
            Request::Read {
                bucket: BUCKET.into(),
                object: name.into(),
                offset,
            },
        ) {
            Ok(Response::ReadData { info, bytes }) => Ok((info, bytes)),
            Ok(other) => panic!("unexpected {other:?}"),
            Err(error) => Err(error.code),
        }
    }

    fn finalize(
        &self,
        conn: &Conn,
        name: &str,
        generation: i64,
        write_offset: i64,
        handle: Option<&[u8]>,
    ) -> Result<ObjectInfo, WireCode> {
        match self.call(
            conn,
            Request::Finalize {
                bucket: BUCKET.into(),
                object: name.into(),
                generation,
                write_offset,
                write_handle: handle.map(<[u8]>::to_vec),
            },
        ) {
            Ok(Response::Object(info)) => Ok(info),
            Ok(other) => panic!("unexpected {other:?}"),
            Err(error) => Err(error.code),
        }
    }
}

fn code(reply: Reply) -> WireCode {
    reply.expect_err("expected an error").code
}

fn failed(session_id: u64, code: WireCode) -> Event {
    Event::SessionFailed {
        session_id,
        error: chorus_wire::WireError::new(code, ""),
    }
}

/// Compare events ignoring error messages.
fn assert_events(actual: Vec<Event>, expected: Vec<Event>) {
    let strip = |events: Vec<Event>| -> Vec<Event> {
        events
            .into_iter()
            .map(|event| match event {
                Event::SessionFailed { session_id, error } => failed(session_id, error.code),
                durable => durable,
            })
            .collect()
    };
    assert_eq!(strip(actual), strip(expected));
}

fn durable(session_id: u64, persisted_size: i64) -> Event {
    Event::Durable {
        session_id,
        persisted_size,
    }
}

#[test]
fn object_lifecycle() {
    let h = Harness::new();
    let c = h.conn();
    let opened = h.create(&c, "seg");
    assert_eq!(opened.persisted_size, 0);
    assert_eq!(opened.metageneration, 1);
    let handle = WriteHandle::decode(&opened.write_handle).unwrap();
    assert_eq!(handle.generation, opened.generation);

    let s = opened.session_id;
    assert_eq!(
        h.append(&c, s, 0, b"hello ", false).unwrap(),
        Response::Accepted { size: 6 }
    );
    assert!(c.events().is_empty());
    // Accepted but not durable: hidden from reads.
    assert_eq!(h.read(&c, "seg", 0).unwrap().1, b"");
    h.append(&c, s, 6, b"world", true).unwrap();
    assert_events(c.events(), vec![durable(s, 11)]);

    let (info, bytes) = h.read(&c, "seg", 0).unwrap();
    assert_eq!(bytes, b"hello world");
    assert_eq!(info.persisted_size, 11);
    assert_eq!(info.size, 0, "open objects hide their size");
    assert_eq!(info.crc32c, Some(crc32c::crc32c(b"hello world")));
    assert_eq!(h.read(&c, "seg", 6).unwrap().1, b"world");
    assert_eq!(h.read(&c, "seg", 11).unwrap().1, b"");
    assert_eq!(h.read(&c, "seg", 12).unwrap_err(), WireCode::OutOfRange);
    assert_eq!(
        h.read(&c, "seg", -1).unwrap_err(),
        WireCode::InvalidArgument
    );

    // A flush with nothing new still reports the tail.
    h.call(
        &c,
        Request::Flush {
            session_id: s,
            offset: 11,
        },
    )
    .unwrap();
    assert_events(c.events(), vec![durable(s, 11)]);

    let info = h
        .finalize(&c, "seg", opened.generation, 11, Some(&opened.write_handle))
        .unwrap();
    assert!(info.finalized);
    assert_eq!((info.size, info.persisted_size), (11, 11));
    assert_eq!(info.metageneration, 2);
    // The finishing session closes silently.
    assert!(c.events().is_empty());
    assert_eq!(
        code(h.append(&c, s, 11, b"x", true)),
        WireCode::FailedPrecondition
    );
    assert!(c.events().is_empty());

    let listed = match h.call(
        &c,
        Request::List {
            bucket: BUCKET.into(),
            prefix: "se".into(),
        },
    ) {
        Ok(Response::Listed(listed)) => listed,
        other => panic!("{other:?}"),
    };
    assert_eq!(listed, vec![info.clone()]);
    assert_eq!(listed[0].metadata["k"], "v");

    assert_eq!(
        code(h.call(
            &c,
            Request::Delete {
                bucket: BUCKET.into(),
                object: "seg".into(),
                generation: info.generation + 1,
            }
        )),
        WireCode::FailedPrecondition
    );
    assert_eq!(
        h.call(
            &c,
            Request::Delete {
                bucket: BUCKET.into(),
                object: "seg".into(),
                generation: info.generation,
            }
        )
        .unwrap(),
        Response::Deleted
    );
    assert_eq!(h.stat(&c, "seg").unwrap_err(), WireCode::NotFound);
    assert_eq!(
        code(h.call(
            &c,
            Request::Delete {
                bucket: BUCKET.into(),
                object: "seg".into(),
                generation: info.generation,
            }
        )),
        WireCode::NotFound
    );
}

#[test]
fn list_filters_by_bucket_and_prefix() {
    let h = Harness::new();
    let c = h.conn();
    for name in ["a/1", "a/2", "b/1", "a"] {
        h.create(&c, name);
    }
    h.opened(
        &c,
        Request::CreateAppendable {
            bucket: "other".into(),
            object: "a/3".into(),
            metadata: HashMap::new(),
        },
    );
    let names = |prefix: &str| -> Vec<String> {
        h.store
            .list(BUCKET, prefix)
            .into_iter()
            .map(|info| info.name)
            .collect()
    };
    assert_eq!(names("a/"), vec!["a/1", "a/2"]);
    assert_eq!(names(""), vec!["a", "a/1", "a/2", "b/1"]);
    assert!(h.store.list("missing", "").is_empty());
}

#[test]
fn generations_strictly_increase() {
    let h = Harness::new();
    let c = h.conn();
    let mut last = 0;
    for i in 0..50 {
        let generation = h.create(&c, &format!("o{i}")).generation;
        assert!(generation > last);
        last = generation;
    }
}

#[test]
fn create_conflicts() {
    let h = Harness::new();
    let c = h.conn();
    h.create(&c, "seg");
    let reply = h.call(
        &c,
        Request::CreateAppendable {
            bucket: BUCKET.into(),
            object: "seg".into(),
            metadata: HashMap::new(),
        },
    );
    assert_eq!(code(reply), WireCode::AlreadyExists);
    let big = HashMap::from([("k".to_string(), "x".repeat(9000))]);
    let reply = h.call(
        &c,
        Request::CreateAppendable {
            bucket: BUCKET.into(),
            object: "other".into(),
            metadata: big,
        },
    );
    assert_eq!(code(reply), WireCode::InvalidArgument);
}

#[test]
fn takeover_fences_older_sessions() {
    let h = Harness::new();
    let a = h.conn();
    let b = h.conn();
    let first = h.create(&a, "seg");
    h.append(&a, first.session_id, 0, b"abc", false).unwrap();

    let stale = ObjectVersion {
        generation: first.generation,
        metageneration: 2,
    };
    assert_eq!(
        code(h.call(
            &b,
            Request::Takeover {
                bucket: BUCKET.into(),
                object: "seg".into(),
                if_match: stale,
            }
        )),
        WireCode::FailedPrecondition
    );

    let second = h.opened(
        &b,
        Request::Takeover {
            bucket: BUCKET.into(),
            object: "seg".into(),
            if_match: ObjectVersion {
                generation: first.generation,
                metageneration: 1,
            },
        },
    );
    // Accepted bytes are made durable before the takeover answers.
    assert_eq!(second.persisted_size, 3);
    assert_eq!(second.generation, first.generation);
    let epoch = |opened: &SessionOpened| WriteHandle::decode(&opened.write_handle).unwrap();
    assert_eq!(epoch(&second).writer_epoch, epoch(&first).writer_epoch + 1);
    assert_events(
        a.events(),
        vec![failed(first.session_id, WireCode::FailedPrecondition)],
    );
    assert!(b.events().is_empty());

    // The fenced session keeps failing, without further events.
    assert_eq!(
        code(h.append(&a, first.session_id, 3, b"d", true)),
        WireCode::FailedPrecondition
    );
    assert!(a.events().is_empty());
    // Its handle no longer resumes.
    assert_eq!(
        code(h.call(
            &a,
            Request::Resume {
                bucket: BUCKET.into(),
                object: "seg".into(),
                write_handle: first.write_handle.clone(),
            }
        )),
        WireCode::FailedPrecondition
    );
    // Nor finalizes.
    assert_eq!(
        h.finalize(&a, "seg", first.generation, 3, Some(&first.write_handle))
            .unwrap_err(),
        WireCode::FailedPrecondition
    );
    // The new writer carries on.
    h.append(&b, second.session_id, 3, b"d", true).unwrap();
    assert_events(b.events(), vec![durable(second.session_id, 4)]);
}

#[test]
fn takeover_of_finalized_or_missing_object_fails() {
    let h = Harness::new();
    let c = h.conn();
    let version = ObjectVersion {
        generation: 1,
        metageneration: 1,
    };
    let takeover = |if_match| Request::Takeover {
        bucket: BUCKET.into(),
        object: "seg".into(),
        if_match,
    };
    assert_eq!(code(h.call(&c, takeover(version))), WireCode::NotFound);
    let opened = h.create(&c, "seg");
    h.finalize(&c, "seg", opened.generation, 0, Some(&opened.write_handle))
        .unwrap();
    let finalized = ObjectVersion {
        generation: opened.generation,
        metageneration: 2,
    };
    assert_eq!(
        code(h.call(&c, takeover(finalized))),
        WireCode::FailedPrecondition
    );
}

#[test]
fn resume_reattaches_and_syncs_accepted_bytes() {
    let h = Harness::new();
    let a = h.conn();
    let b = h.conn();
    let first = h.create(&a, "seg");
    h.append(&a, first.session_id, 0, b"durable", true).unwrap();
    h.append(&a, first.session_id, 7, b"+pending", false)
        .unwrap();
    assert_events(a.events(), vec![durable(first.session_id, 7)]);

    let resume = Request::Resume {
        bucket: BUCKET.into(),
        object: "seg".into(),
        write_handle: first.write_handle.clone(),
    };
    let second = h.opened(&b, resume.clone());
    assert_eq!(second.persisted_size, 15);
    assert_eq!(second.write_handle, first.write_handle, "epoch unchanged");
    assert_ne!(second.session_id, first.session_id);
    // The displaced session is told.
    assert_events(
        a.events(),
        vec![failed(first.session_id, WireCode::FailedPrecondition)],
    );

    // A resend inside the durable bytes is an idempotent no-op; new bytes
    // continue from the returned tail.
    h.append(&b, second.session_id, 7, b"+pend", false).unwrap();
    h.append(&b, second.session_id, 15, b"!", true).unwrap();
    assert_events(b.events(), vec![durable(second.session_id, 16)]);

    // Bad handles.
    let bad = Request::Resume {
        bucket: BUCKET.into(),
        object: "seg".into(),
        write_handle: vec![1, 2, 3],
    };
    assert_eq!(code(h.call(&b, bad)), WireCode::InvalidArgument);
    let missing = Request::Resume {
        bucket: BUCKET.into(),
        object: "nope".into(),
        write_handle: first.write_handle.clone(),
    };
    assert_eq!(code(h.call(&b, missing)), WireCode::NotFound);
}

#[test]
fn append_offset_rules() {
    let h = Harness::new();
    let c = h.conn();
    let open = |name: &str| h.create(&c, name).session_id;

    // Exact-overlap resend is an idempotent no-op.
    let s = open("idem");
    h.append(&c, s, 0, b"abcdef", false).unwrap();
    assert_eq!(
        h.append(&c, s, 2, b"cd", true).unwrap(),
        Response::Accepted { size: 6 }
    );
    assert_events(c.events(), vec![durable(s, 6)]);

    // Beyond the tail.
    let s = open("gap");
    assert_eq!(code(h.append(&c, s, 1, b"x", false)), WireCode::OutOfRange);
    assert_events(c.events(), vec![failed(s, WireCode::OutOfRange)]);
    // A dead session fails with its recorded code and no new event.
    assert_eq!(code(h.append(&c, s, 0, b"x", false)), WireCode::OutOfRange);
    assert_eq!(
        code(h.call(
            &c,
            Request::Flush {
                session_id: s,
                offset: 0
            }
        )),
        WireCode::OutOfRange
    );
    assert!(c.events().is_empty());

    // Overlap that runs past the tail.
    let s = open("partial");
    h.append(&c, s, 0, b"abc", false).unwrap();
    assert_eq!(
        code(h.append(&c, s, 2, b"cd", false)),
        WireCode::FailedPrecondition
    );
    assert_events(c.events(), vec![failed(s, WireCode::FailedPrecondition)]);

    // Overlap with different bytes.
    let s = open("diff");
    h.append(&c, s, 0, b"abc", false).unwrap();
    assert_eq!(
        code(h.append(&c, s, 0, b"abd", false)),
        WireCode::FailedPrecondition
    );
    assert_events(c.events(), vec![failed(s, WireCode::FailedPrecondition)]);

    // Payload checksum mismatch.
    let s = open("crc");
    let reply = h.call(
        &c,
        Request::Append {
            session_id: s,
            offset: 0,
            data: b"abc".to_vec(),
            crc32c: 7,
            flush: true,
        },
    );
    assert_eq!(code(reply), WireCode::DataLoss);
    assert_events(c.events(), vec![failed(s, WireCode::DataLoss)]);

    // Flush beyond the tail.
    let s = open("flush");
    let reply = h.call(
        &c,
        Request::Flush {
            session_id: s,
            offset: 1,
        },
    );
    assert_eq!(code(reply), WireCode::OutOfRange);
    assert_events(c.events(), vec![failed(s, WireCode::OutOfRange)]);

    // Unknown session.
    assert_eq!(
        code(h.append(&c, 9999, 0, b"x", false)),
        WireCode::FailedPrecondition
    );
}

#[test]
fn sessions_are_scoped_to_their_connection() {
    let h = Harness::new();
    let a = h.conn();
    let b = h.conn();
    let opened = h.create(&a, "seg");
    assert_eq!(
        code(h.append(&b, opened.session_id, 0, b"x", false)),
        WireCode::FailedPrecondition
    );
    // Closing from another connection does nothing.
    h.call(
        &b,
        Request::CloseSession {
            session_id: opened.session_id,
        },
    )
    .unwrap();
    h.append(&a, opened.session_id, 0, b"x", true).unwrap();
    assert_events(a.events(), vec![durable(opened.session_id, 1)]);

    assert_eq!(h.store.session_count(), 1);
    h.store.connection_closed(a.ctx.id);
    assert_eq!(h.store.session_count(), 0);
    // The handle still resumes on another connection.
    let resumed = h.opened(
        &b,
        Request::Resume {
            bucket: BUCKET.into(),
            object: "seg".into(),
            write_handle: opened.write_handle,
        },
    );
    assert_eq!(resumed.persisted_size, 1);
}

#[test]
fn close_session_is_quiet_and_idempotent() {
    let h = Harness::new();
    let c = h.conn();
    let opened = h.create(&c, "seg");
    for _ in 0..2 {
        assert_eq!(
            h.call(
                &c,
                Request::CloseSession {
                    session_id: opened.session_id
                }
            )
            .unwrap(),
            Response::SessionClosed
        );
    }
    assert!(c.events().is_empty());
    assert_eq!(
        code(h.append(&c, opened.session_id, 0, b"x", false)),
        WireCode::FailedPrecondition
    );
    assert!(c.events().is_empty());
}

#[test]
fn replace_appendable() {
    let h = Harness::new();
    let c = h.conn();
    let first = h.opened(
        &c,
        Request::ReplaceAppendable {
            bucket: BUCKET.into(),
            object: "seg".into(),
            if_match: None,
            data: b"one".to_vec(),
            metadata: HashMap::new(),
        },
    );
    assert_eq!(first.persisted_size, 3);
    assert_eq!(h.read(&c, "seg", 0).unwrap().1, b"one");

    // Create-if-absent loses against an existing object.
    let reply = h.call(
        &c,
        Request::ReplaceAppendable {
            bucket: BUCKET.into(),
            object: "seg".into(),
            if_match: None,
            data: b"x".to_vec(),
            metadata: HashMap::new(),
        },
    );
    assert_eq!(code(reply), WireCode::FailedPrecondition);
    // A stale version loses too.
    let stale = ObjectVersion {
        generation: first.generation - 1,
        metageneration: 1,
    };
    let reply = h.call(
        &c,
        Request::ReplaceAppendable {
            bucket: BUCKET.into(),
            object: "seg".into(),
            if_match: Some(stale),
            data: b"x".to_vec(),
            metadata: HashMap::new(),
        },
    );
    assert_eq!(code(reply), WireCode::FailedPrecondition);
    assert!(c.events().is_empty());

    let second = h.opened(
        &c,
        Request::ReplaceAppendable {
            bucket: BUCKET.into(),
            object: "seg".into(),
            if_match: Some(ObjectVersion {
                generation: first.generation,
                metageneration: first.metageneration,
            }),
            data: b"two!".to_vec(),
            metadata: HashMap::from([("m".to_string(), "2".to_string())]),
        },
    );
    assert!(second.generation > first.generation);
    assert_eq!(second.persisted_size, 4);
    assert_events(
        c.events(),
        vec![failed(first.session_id, WireCode::FailedPrecondition)],
    );
    let (info, bytes) = h.read(&c, "seg", 0).unwrap();
    assert_eq!(bytes, b"two!");
    assert!(!info.finalized);
    assert_eq!(info.metadata["m"], "2");
    // The new session appends and finalizes.
    h.append(&c, second.session_id, 4, b"+", true).unwrap();
    let info = h
        .finalize(&c, "seg", second.generation, 5, Some(&second.write_handle))
        .unwrap();
    assert_eq!(info.crc32c, Some(crc32c::crc32c(b"two!+")));

    // Replacing a finalized object yields a fresh open generation.
    let third = h.opened(
        &c,
        Request::ReplaceAppendable {
            bucket: BUCKET.into(),
            object: "seg".into(),
            if_match: Some(ObjectVersion {
                generation: info.generation,
                metageneration: info.metageneration,
            }),
            data: Vec::new(),
            metadata: HashMap::new(),
        },
    );
    assert_eq!(third.persisted_size, 0);
    assert!(!h.stat(&c, "seg").unwrap().finalized);
}

#[test]
fn finalize_rules() {
    let h = Harness::new();
    let a = h.conn();
    let b = h.conn();
    let opened = h.create(&a, "seg");
    let s = opened.session_id;
    let generation = opened.generation;
    let handle = opened.write_handle.clone();
    h.append(&a, s, 0, b"abcd", false).unwrap();

    assert_eq!(
        h.finalize(&a, "nope", generation, 4, None).unwrap_err(),
        WireCode::NotFound
    );
    assert_eq!(
        h.finalize(&a, "seg", generation + 1, 4, Some(&handle))
            .unwrap_err(),
        WireCode::FailedPrecondition
    );
    assert_eq!(
        h.finalize(&a, "seg", generation, 4, Some(b"junk"))
            .unwrap_err(),
        WireCode::InvalidArgument
    );
    // Accepted bytes are synced first, so 4 is the durable tail.
    assert_eq!(
        h.finalize(&a, "seg", generation, 5, Some(&handle))
            .unwrap_err(),
        WireCode::OutOfRange
    );
    assert_eq!(
        h.finalize(&a, "seg", generation, 3, Some(&handle))
            .unwrap_err(),
        WireCode::FailedPrecondition
    );
    // Failed finalizes leave the session alive.
    assert!(a.events().is_empty());

    // Move the writer to connection `b`; that displaces `a`'s session.
    let resumed = h.opened(
        &b,
        Request::Resume {
            bucket: BUCKET.into(),
            object: "seg".into(),
            write_handle: handle.clone(),
        },
    );
    assert_events(a.events(), vec![failed(s, WireCode::FailedPrecondition)]);
    let info = h.finalize(&b, "seg", generation, 4, Some(&handle)).unwrap();
    assert!(
        b.events().is_empty(),
        "the finishing session closes quietly"
    );
    assert_eq!(
        code(h.append(&b, resumed.session_id, 4, b"x", false)),
        WireCode::FailedPrecondition
    );

    // Idempotent retries succeed with or without the handle, from anywhere.
    assert_eq!(
        h.finalize(&a, "seg", generation, 4, Some(&handle)),
        Ok(info.clone())
    );
    assert_eq!(h.finalize(&a, "seg", generation, 4, None), Ok(info.clone()));
    assert_eq!(
        h.finalize(&a, "seg", generation, 3, None).unwrap_err(),
        WireCode::FailedPrecondition
    );
}

#[test]
fn handle_free_finalize_is_an_implicit_takeover() {
    let h = Harness::new();
    let a = h.conn();
    let b = h.conn();
    let opened = h.create(&a, "seg");
    h.append(&a, opened.session_id, 0, b"xy", true).unwrap();
    assert_events(a.events(), vec![durable(opened.session_id, 2)]);
    let info = h.finalize(&b, "seg", opened.generation, 2, None).unwrap();
    assert!(info.finalized);
    assert_events(
        a.events(),
        vec![failed(opened.session_id, WireCode::FailedPrecondition)],
    );
}

#[test]
fn delete_fences_sessions() {
    let h = Harness::new();
    let a = h.conn();
    let opened = h.create(&a, "seg");
    h.call(
        &a,
        Request::Delete {
            bucket: BUCKET.into(),
            object: "seg".into(),
            generation: opened.generation,
        },
    )
    .unwrap();
    assert_events(
        a.events(),
        vec![failed(opened.session_id, WireCode::FailedPrecondition)],
    );
    // The name can be created again, with a newer generation.
    assert!(h.create(&a, "seg").generation > opened.generation);
}

#[test]
fn reads_are_capped_per_response() {
    let h = Harness::new();
    let c = h.conn();
    let opened = h.create(&c, "big");
    let data: Vec<u8> = (0..READ_CHUNK_BYTES + 10).map(|i| i as u8).collect();
    h.append(&c, opened.session_id, 0, &data, true).unwrap();
    c.events();
    let (info, first) = h.read(&c, "big", 0).unwrap();
    assert_eq!(info.persisted_size as usize, data.len());
    assert_eq!(first.len(), READ_CHUNK_BYTES);
    let (_, rest) = h.read(&c, "big", first.len() as i64).unwrap();
    assert_eq!([first, rest].concat(), data);
}

#[test]
fn hello_is_not_a_store_request() {
    let h = Harness::new();
    let c = h.conn();
    let reply = h.call(
        &c,
        Request::Hello {
            protocol_version: 1,
            client_name: "x".into(),
        },
    );
    assert_eq!(code(reply), WireCode::InvalidArgument);
}
