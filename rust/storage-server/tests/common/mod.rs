//! Shared harness: storage nodes on background compio runtimes, backed by
//! memory or by a temporary data directory.
// Each test crate uses a different subset of the harness.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::mpsc;
use std::thread::JoinHandle;

use chorus_storage_server::{Backend, DiskBackend, MemoryBackend, Server, ServerControl, Store};
use tempfile::TempDir;

/// Which persistence backend a test node uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backing {
    Memory,
    Disk,
}

/// Generate one `#[tokio::test]` per backend for each listed
/// `async fn name(backing: Backing)`, in modules `memory` and `disk`.
macro_rules! for_each_backing {
    ($($name:ident),* $(,)?) => {
        mod memory {
            $(
                #[tokio::test(flavor = "multi_thread")]
                async fn $name() {
                    super::$name(crate::common::Backing::Memory).await
                }
            )*
        }
        mod disk {
            $(
                #[tokio::test(flavor = "multi_thread")]
                async fn $name() {
                    super::$name(crate::common::Backing::Disk).await
                }
            )*
        }
    };
}

/// A storage node serving on 127.0.0.1 from its own thread, with group
/// commit enabled as in production.
pub struct TestNode {
    pub addr: SocketAddr,
    node_id: String,
    control: Option<ServerControl>,
    thread: Option<JoinHandle<()>>,
    /// The data directory of a disk node (removed when the node is dropped).
    dir: Option<TempDir>,
}

impl Drop for TestNode {
    fn drop(&mut self) {
        // Stopping the node wakes a task on its compio runtime from this
        // thread. compio aborts the process when such a cross-thread wake
        // happens on a panicking thread, which would hide the failing
        // assertion; leak the node instead.
        if std::thread::panicking() {
            std::mem::forget(self.control.take());
            std::mem::forget(self.thread.take());
            std::mem::forget(self.dir.take());
        } else {
            self.stop_server();
        }
    }
}

async fn serve<B: Backend + 'static>(
    node_id: String,
    backend: B,
    ready: mpsc::Sender<(SocketAddr, ServerControl)>,
) {
    let store = Rc::new(Store::open(node_id, backend).await.expect("open store"));
    store.enable_group_commit();
    let server = Server::bind("127.0.0.1:0", store).await.expect("bind");
    ready
        .send((server.local_addr().unwrap(), server.control()))
        .unwrap();
    server.run().await.expect("serve");
}

impl TestNode {
    /// An in-memory node.
    pub fn start(node_id: &str) -> Self {
        Self::start_with(node_id, Backing::Memory)
    }

    /// A node on `backing`; a disk node gets a fresh temporary data dir.
    pub fn start_with(node_id: &str, backing: Backing) -> Self {
        match backing {
            Backing::Memory => Self::spawn(node_id, None),
            Backing::Disk => {
                let dir = tempfile::tempdir().expect("tempdir");
                let mut node = Self::spawn(node_id, Some(dir.path().to_path_buf()));
                node.dir = Some(dir);
                node
            }
        }
    }

    fn spawn(node_id: &str, data_dir: Option<PathBuf>) -> Self {
        let id = node_id.to_string();
        let (ready, started) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name(format!("storage-{node_id}"))
            .spawn(move || {
                let runtime = compio::runtime::Runtime::new().expect("compio runtime");
                runtime.block_on(async move {
                    match data_dir {
                        None => serve(id, MemoryBackend::new(), ready).await,
                        Some(dir) => {
                            let backend = DiskBackend::open(&dir, Some(id.clone()))
                                .await
                                .expect("open data dir");
                            serve(id, backend, ready).await
                        }
                    }
                });
                drop(runtime);
            })
            .expect("spawn server thread");
        let (addr, control) = started.recv().expect("server starts");
        Self {
            addr,
            node_id: node_id.to_string(),
            control: Some(control),
            thread: Some(thread),
            dir: None,
        }
    }

    pub fn addr(&self) -> String {
        self.addr.to_string()
    }

    /// The data directory of a disk node.
    pub fn data_dir(&self) -> Option<&Path> {
        self.dir.as_ref().map(TempDir::path)
    }

    /// Abruptly close every client connection of the node.
    pub fn disconnect_all(&self) {
        if let Some(control) = &self.control {
            control.disconnect_all();
        }
    }

    fn stop_server(&mut self) {
        if let Some(control) = self.control.take() {
            control.shutdown();
        }
        if let Some(thread) = self.thread.take() {
            thread.join().expect("server thread");
        }
    }

    /// Stop the server (its runtime, store and open files go away, as in a
    /// process exit) and start a new one on the same data directory. The new
    /// node listens on a new port.
    pub fn restart(mut self) -> Self {
        let dir = self.dir.take().expect("only a disk node can restart");
        self.stop_server();
        let mut node = Self::spawn(&self.node_id, Some(dir.path().to_path_buf()));
        node.dir = Some(dir);
        node
    }
}
