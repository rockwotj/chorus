//! `DiskBackend` on a temporary directory, directly and under a `Store`.

use std::collections::HashMap;
use std::future::Future;
use std::path::Path;

use chorus_wire::{ObjectVersion, Request, Response, SessionOpened, WireCode};
use futures::channel::mpsc;

use super::*;
use crate::store::{ConnCtx, Store};

fn run<F: Future>(future: F) -> F::Output {
    compio::runtime::Runtime::new()
        .expect("compio runtime")
        .block_on(future)
}

fn meta(generation: i64) -> ObjectMeta {
    ObjectMeta {
        generation,
        metageneration: 1,
        metadata: HashMap::from([("k".to_string(), "v".to_string())]),
        finalized: false,
        finalized_size: 0,
        finalized_crc32c: 0,
        writer_epoch: 1,
        last_modified_unix_nanos: 1234,
    }
}

async fn reopen(dir: &Path) -> (DiskBackend, Recovered) {
    let backend = DiskBackend::open(dir, None).await.expect("open");
    let mut recovered = backend.recover().await.expect("recover");
    recovered.objects.sort_by(|a, b| a.key.cmp(&b.key));
    (backend, recovered)
}

fn files_in(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

#[test]
fn create_write_sync_read_finalize_delete() {
    let tmp = tempfile::tempdir().unwrap();
    run(async {
        let (backend, recovered) = reopen(tmp.path()).await;
        assert!(recovered.objects.is_empty());
        let key = ObjectKey::new("bucket", "wal/seg-1");
        backend
            .create(&key, &meta(100), b"abc".to_vec())
            .await
            .unwrap();
        assert!(backend.generation_high_water() >= 100);
        backend.write(&key, 100, 3, b"def".to_vec()).await.unwrap();
        backend.sync(&key, 100).await.unwrap();
        assert_eq!(backend.read(&key, 100, 0, 6).await.unwrap(), b"abcdef");
        assert_eq!(backend.read(&key, 100, 2, 2).await.unwrap(), b"cd");
        assert_eq!(backend.read(&key, 100, 6, 0).await.unwrap(), b"");
        assert!(backend.read(&key, 100, 4, 10).await.is_err());
        assert_eq!(backend.open_handles(), 1);

        let mut finalized = meta(100);
        finalized.finalized = true;
        finalized.metageneration = 2;
        finalized.finalized_size = 6;
        finalized.finalized_crc32c = crc32c::crc32c(b"abcdef");
        backend.commit_meta(&key, &finalized).await.unwrap();
        assert_eq!(backend.open_handles(), 0, "finalize closes the handle");
        assert_eq!(backend.read(&key, 100, 0, 6).await.unwrap(), b"abcdef");

        let dir = tmp.path().join("bucket");
        assert_eq!(files_in(&dir), ["wal%2Fseg-1.100.data", "wal%2Fseg-1.meta"]);
        backend.delete(&key, 100).await.unwrap();
        assert!(files_in(&dir).is_empty());
        assert!(backend.read(&key, 100, 0, 1).await.is_err());
    });
}

#[test]
fn recovery_restores_tails_finalized_state_and_epochs() {
    let tmp = tempfile::tempdir().unwrap();
    let open_key = ObjectKey::new("b", "open");
    let final_key = ObjectKey::new("b", "final");
    run(async {
        let (backend, _) = reopen(tmp.path()).await;
        backend
            .create(&open_key, &meta(10), b"hello".to_vec())
            .await
            .unwrap();
        backend
            .write(&open_key, 10, 5, b" world".to_vec())
            .await
            .unwrap();
        backend.sync(&open_key, 10).await.unwrap();
        let mut bumped = meta(10);
        bumped.writer_epoch = 7;
        backend.commit_meta(&open_key, &bumped).await.unwrap();

        backend
            .create(&final_key, &meta(11), Vec::new())
            .await
            .unwrap();
        backend
            .write(&final_key, 11, 0, b"done".to_vec())
            .await
            .unwrap();
        backend.sync(&final_key, 11).await.unwrap();
        let mut finalized = meta(11);
        finalized.finalized = true;
        finalized.metageneration = 2;
        finalized.finalized_size = 4;
        finalized.finalized_crc32c = crc32c::crc32c(b"done");
        backend.commit_meta(&final_key, &finalized).await.unwrap();
        // "Crash": drop without any cleanup.
    });
    run(async {
        let (_backend, recovered) = reopen(tmp.path()).await;
        assert!(recovered.last_generation >= 11);
        assert_eq!(recovered.objects.len(), 2);
        let final_obj = &recovered.objects[0];
        assert_eq!(final_obj.key, final_key);
        assert!(final_obj.meta.finalized);
        assert_eq!(final_obj.meta.metageneration, 2);
        assert_eq!(final_obj.durable_size, 4);
        assert_eq!(final_obj.durable_crc32c, crc32c::crc32c(b"done"));
        let open_obj = &recovered.objects[1];
        assert_eq!(open_obj.key, open_key);
        assert!(!open_obj.meta.finalized);
        assert_eq!(open_obj.meta.writer_epoch, 7);
        assert_eq!(open_obj.meta.metadata["k"], "v");
        assert_eq!(open_obj.durable_size, 11);
        assert_eq!(open_obj.durable_crc32c, crc32c::crc32c(b"hello world"));
    });
}

#[test]
fn generation_high_water_survives_deletes_and_restarts() {
    let tmp = tempfile::tempdir().unwrap();
    run(async {
        let (backend, recovered) = reopen(tmp.path()).await;
        assert_eq!(recovered.last_generation, 0);
        let key = ObjectKey::new("b", "x");
        backend.create(&key, &meta(500), Vec::new()).await.unwrap();
        assert_eq!(backend.generation_high_water(), 500 + GENERATION_RESERVE);
        // Within the reservation: no node.meta rewrite needed.
        backend.delete(&key, 500).await.unwrap();
        backend.create(&key, &meta(501), Vec::new()).await.unwrap();
        assert_eq!(backend.generation_high_water(), 500 + GENERATION_RESERVE);
        backend.delete(&key, 501).await.unwrap();
    });
    run(async {
        let (_backend, recovered) = reopen(tmp.path()).await;
        assert!(recovered.objects.is_empty());
        assert_eq!(recovered.last_generation, 500 + GENERATION_RESERVE);
    });
}

#[test]
fn replace_discards_the_old_generation_and_recovery_removes_orphans() {
    let tmp = tempfile::tempdir().unwrap();
    let key = ObjectKey::new("b", "seg");
    let dir = tmp.path().join("b");
    run(async {
        let (backend, _) = reopen(tmp.path()).await;
        backend
            .create(&key, &meta(10), b"old".to_vec())
            .await
            .unwrap();
        let mut replacement = meta(20);
        replacement.writer_epoch = 2;
        backend
            .create(&key, &replacement, b"new!".to_vec())
            .await
            .unwrap();
        assert_eq!(files_in(&dir), ["seg.20.data", "seg.meta"]);
        assert_eq!(backend.read(&key, 20, 0, 4).await.unwrap(), b"new!");
    });
    // Leftovers of interrupted operations, plus foreign entries.
    std::fs::write(dir.join("seg.10.data"), b"orphan of a replace").unwrap();
    std::fs::write(dir.join("seg.meta.tmp"), b"uncommitted meta").unwrap();
    std::fs::write(dir.join("lonely.30.data"), b"create that never committed").unwrap();
    std::fs::write(dir.join("README"), b"not ours").unwrap();
    std::fs::write(tmp.path().join("node.meta.tmp"), b"torn").unwrap();
    std::fs::create_dir(tmp.path().join("lost+found")).unwrap();
    std::fs::write(tmp.path().join("lost+found").join("x.1.data"), b"").unwrap();
    run(async {
        let (backend, recovered) = reopen(tmp.path()).await;
        assert_eq!(recovered.objects.len(), 1);
        assert_eq!(recovered.objects[0].meta.generation, 20);
        assert_eq!(recovered.objects[0].durable_size, 4);
        assert!(recovered.last_generation >= 30, "orphans count too");
        assert_eq!(files_in(&dir), ["README", "seg.20.data", "seg.meta"]);
        assert!(!tmp.path().join("node.meta.tmp").exists());
        assert!(tmp.path().join("lost+found").join("x.1.data").exists());
        assert_eq!(backend.read(&key, 20, 0, 4).await.unwrap(), b"new!");
    });
}

#[test]
fn corrupt_meta_fails_recovery() {
    let tmp = tempfile::tempdir().unwrap();
    let key = ObjectKey::new("b", "seg");
    run(async {
        let (backend, _) = reopen(tmp.path()).await;
        backend
            .create(&key, &meta(10), b"x".to_vec())
            .await
            .unwrap();
    });
    let path = tmp.path().join("b").join("seg.meta");
    let mut bytes = std::fs::read(&path).unwrap();
    *bytes.last_mut().unwrap() ^= 0xff;
    std::fs::write(&path, bytes).unwrap();
    run(async {
        let backend = DiskBackend::open(tmp.path(), None).await.unwrap();
        let error = backend.recover().await.unwrap_err();
        assert!(error.to_string().contains("corrupt"), "{error}");
        // The data file is left alone for an operator to inspect.
        assert!(tmp.path().join("b").join("seg.10.data").exists());
    });
}

#[test]
fn node_id_is_stored_and_checked() {
    let tmp = tempfile::tempdir().unwrap();
    run(async {
        let backend = DiskBackend::open(tmp.path(), Some("node-a".into()))
            .await
            .unwrap();
        assert_eq!(backend.node_id(), "node-a");
        drop(backend);
        let backend = DiskBackend::open(tmp.path(), None).await.unwrap();
        assert_eq!(backend.node_id(), "node-a");
        let backend2 = DiskBackend::open(tmp.path(), Some("node-a".into())).await;
        assert!(backend2.is_ok());
        let error = DiskBackend::open(tmp.path(), Some("node-b".into()))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    });
}

#[test]
fn awkward_names_round_trip_through_recovery() {
    let tmp = tempfile::tempdir().unwrap();
    let keys = [
        ObjectKey::new("zone/ü", "a/b/c"),
        ObjectKey::new("zone/ü", ".."),
        ObjectKey::new("zone/ü", "."),
        ObjectKey::new("zone/ü", ""),
        ObjectKey::new("..", "../../escape"),
        ObjectKey::new("b", "文件/🦀"),
        ObjectKey::new("b", "x".repeat(300)),
        ObjectKey::new("b", "/".repeat(120)),
        ObjectKey::new("y".repeat(250), "long bucket"),
    ];
    run(async {
        let (backend, _) = reopen(tmp.path()).await;
        for (i, key) in keys.iter().enumerate() {
            let data = format!("payload {i}").into_bytes();
            backend
                .create(key, &meta(i as i64 + 1), data)
                .await
                .unwrap();
        }
    });
    // Every entry is a bucket directory (or node.meta) inside the data
    // directory; nothing escaped it.
    for entry in files_in(tmp.path()) {
        assert!(entry == NODE_META || names::is_stem(&entry), "{entry}");
    }
    run(async {
        let (backend, recovered) = reopen(tmp.path()).await;
        let mut expected = keys.to_vec();
        expected.sort();
        let found: Vec<_> = recovered.objects.iter().map(|o| o.key.clone()).collect();
        assert_eq!(found, expected);
        for (i, key) in keys.iter().enumerate() {
            let data = format!("payload {i}").into_bytes();
            let object = recovered.objects.iter().find(|o| &o.key == key).unwrap();
            assert_eq!(object.durable_size, data.len() as u64);
            let read = backend
                .read(key, i as i64 + 1, 0, data.len())
                .await
                .unwrap();
            assert_eq!(read, data);
        }
    });
}

// ---- Store over DiskBackend: restarts as a client sees them ----

struct Client {
    ctx: ConnCtx,
    _frames: mpsc::UnboundedReceiver<chorus_wire::ServerFrame>,
}

fn client(store: &Store<DiskBackend>) -> Client {
    let (outbox, frames) = mpsc::unbounded();
    Client {
        ctx: ConnCtx {
            id: store.new_connection_id(),
            outbox,
        },
        _frames: frames,
    }
}

async fn open_store(dir: &Path) -> Store<DiskBackend> {
    let backend = DiskBackend::open(dir, Some("node".into())).await.unwrap();
    Store::open("node", backend).await.unwrap()
}

fn opened(reply: Result<Response, chorus_wire::WireError>) -> SessionOpened {
    match reply {
        Ok(Response::SessionOpened(opened)) => opened,
        other => panic!("expected SessionOpened, got {other:?}"),
    }
}

fn append(session_id: u64, offset: i64, data: &[u8]) -> Request {
    Request::Append {
        session_id,
        offset,
        data: data.to_vec(),
        crc32c: crc32c::crc32c(data),
        flush: true,
    }
}

fn create(name: &str) -> Request {
    Request::CreateAppendable {
        bucket: "b".into(),
        object: name.into(),
        metadata: HashMap::new(),
    }
}

fn resume(name: &str, handle: &[u8]) -> Request {
    Request::Resume {
        bucket: "b".into(),
        object: name.into(),
        write_handle: handle.to_vec(),
    }
}

#[test]
fn store_restart_keeps_tails_epochs_and_generations() {
    let tmp = tempfile::tempdir().unwrap();
    let (kept, taken, done) = run(async {
        let store = open_store(tmp.path()).await;
        let c = client(&store);
        // "kept": written, never taken over.
        let kept = opened(store.handle(&c.ctx, create("kept")).await);
        store
            .handle(&c.ctx, append(kept.session_id, 0, b"abc"))
            .await
            .unwrap();
        // "taken": taken over after the first writer wrote.
        let taken = opened(store.handle(&c.ctx, create("taken")).await);
        store
            .handle(&c.ctx, append(taken.session_id, 0, b"12"))
            .await
            .unwrap();
        let other = client(&store);
        let takeover = opened(
            store
                .handle(
                    &other.ctx,
                    Request::Takeover {
                        bucket: "b".into(),
                        object: "taken".into(),
                        if_match: ObjectVersion {
                            generation: taken.generation,
                            metageneration: taken.metageneration,
                        },
                    },
                )
                .await,
        );
        // "done": finalized.
        let done = opened(store.handle(&c.ctx, create("done")).await);
        store
            .handle(&c.ctx, append(done.session_id, 0, b"xyz"))
            .await
            .unwrap();
        store
            .handle(
                &c.ctx,
                Request::Finalize {
                    bucket: "b".into(),
                    object: "done".into(),
                    generation: done.generation,
                    write_offset: 3,
                    write_handle: Some(done.write_handle.clone()),
                },
            )
            .await
            .unwrap();
        (kept, (taken, takeover), done)
    });
    let (taken, takeover) = taken;
    run(async {
        let store = open_store(tmp.path()).await;
        let c = client(&store);
        // A pre-restart handle resumes when no takeover happened.
        let resumed = opened(
            store
                .handle(&c.ctx, resume("kept", &kept.write_handle))
                .await,
        );
        assert_eq!(resumed.generation, kept.generation);
        assert_eq!(resumed.persisted_size, 3);
        assert_eq!(resumed.write_handle, kept.write_handle);
        store
            .handle(&c.ctx, append(resumed.session_id, 3, b"def"))
            .await
            .unwrap();
        // ... and is fenced when one did.
        let error = store
            .handle(&c.ctx, resume("taken", &taken.write_handle))
            .await
            .unwrap_err();
        assert_eq!(error.code, WireCode::FailedPrecondition);
        let resumed = opened(
            store
                .handle(&c.ctx, resume("taken", &takeover.write_handle))
                .await,
        );
        assert_eq!(resumed.persisted_size, 2);
        // Finalized state survives.
        let Ok(Response::ReadData { info, bytes }) = store
            .handle(
                &c.ctx,
                Request::Read {
                    bucket: "b".into(),
                    object: "done".into(),
                    offset: 0,
                },
            )
            .await
        else {
            panic!("read failed");
        };
        assert!(info.finalized);
        assert_eq!(info.metageneration, 2);
        assert_eq!(info.size, 3);
        assert_eq!(info.crc32c, Some(crc32c::crc32c(b"xyz")));
        assert_eq!(bytes, b"xyz");
        let error = store
            .handle(&c.ctx, resume("done", &done.write_handle))
            .await
            .unwrap_err();
        assert_eq!(error.code, WireCode::FailedPrecondition);
        // Generations keep increasing across the restart.
        let fresh = opened(store.handle(&c.ctx, create("fresh")).await);
        assert!(fresh.generation > done.generation);
        assert!(fresh.generation > takeover.generation);
    });
    run(async {
        let store = open_store(tmp.path()).await;
        let c = client(&store);
        let Ok(Response::ReadData { bytes, info }) = store
            .handle(
                &c.ctx,
                Request::Read {
                    bucket: "b".into(),
                    object: "kept".into(),
                    offset: 0,
                },
            )
            .await
        else {
            panic!("read failed");
        };
        assert_eq!(bytes, b"abcdef");
        assert_eq!(info.persisted_size, 6);
        assert_eq!(info.crc32c, Some(crc32c::crc32c(b"abcdef")));
    });
}
