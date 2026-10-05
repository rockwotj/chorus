//! Shared harness: storage nodes on background compio runtimes.
// Each test crate uses a different subset of the harness.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::mpsc;

use chorus_storage_server::{MemoryBackend, Server, ServerControl, Store};

/// A storage node serving on 127.0.0.1 from its own thread. The thread lives
/// until the test process exits.
pub struct TestNode {
    pub addr: SocketAddr,
    control: Option<ServerControl>,
}

impl Drop for TestNode {
    fn drop(&mut self) {
        // Dropping the control handle wakes a task on the node's compio
        // runtime from this thread. compio aborts the process when such a
        // cross-thread wake happens on a panicking thread, which would hide
        // the failing assertion; leak the handle instead.
        if std::thread::panicking() {
            std::mem::forget(self.control.take());
        }
    }
}

impl TestNode {
    pub fn start(node_id: &str) -> Self {
        let node_id = node_id.to_string();
        let (ready, started) = mpsc::channel();
        std::thread::Builder::new()
            .name(format!("storage-{node_id}"))
            .spawn(move || {
                let runtime = compio::runtime::Runtime::new().expect("compio runtime");
                runtime.block_on(async move {
                    let store = Store::open(node_id, MemoryBackend::new())
                        .await
                        .expect("open store");
                    let server = Server::bind("127.0.0.1:0", Rc::new(store))
                        .await
                        .expect("bind");
                    ready
                        .send((server.local_addr().unwrap(), server.control()))
                        .unwrap();
                    server.run().await.expect("serve");
                });
            })
            .expect("spawn server thread");
        let (addr, control) = started.recv().expect("server starts");
        Self {
            addr,
            control: Some(control),
        }
    }

    pub fn addr(&self) -> String {
        self.addr.to_string()
    }

    /// Abruptly close every client connection of the node.
    pub fn disconnect_all(&self) {
        if let Some(control) = &self.control {
            control.disconnect_all();
        }
    }
}
