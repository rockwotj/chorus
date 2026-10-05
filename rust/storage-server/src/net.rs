//! Networking: the accept loop and one reader plus one writer task per
//! connection.
//!
//! The reader decodes client frames and applies each request to the store
//! before decoding the next, so one connection's requests take effect in
//! arrival order. Responses and session events go through the connection's
//! outbox, an unbounded local queue drained by the single writer task, which
//! keeps frames whole and lets events interleave with responses.

use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::{Shutdown, SocketAddr};
use std::rc::Rc;

use bytes::BytesMut;
use chorus_wire::{
    decode_client_frame, encode_server_frame, ClientFrame, Request, Response, ServerFrame,
    WireCode, WireError, PROTOCOL_VERSION,
};
use compio::buf::BufResult;
use compio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use compio::net::{TcpListener, TcpStream, ToSocketAddrsAsync};
use futures::channel::mpsc;
use futures::StreamExt;

use crate::backend::Backend;
use crate::store::{ConnCtx, ConnId, Store};

/// Initial receive buffer size and the minimum free space before a read.
const READ_BUF_BYTES: usize = 64 * 1024;
/// Stop coalescing queued frames into one socket write past this size.
const WRITE_BATCH_BYTES: usize = 256 * 1024;

/// Commands from a [`ServerControl`].
enum Command {
    DisconnectAll,
}

/// Thread-safe handle for poking a running [`Server`] from outside its
/// runtime.
#[derive(Clone, Debug)]
pub struct ServerControl {
    commands: mpsc::UnboundedSender<Command>,
}

impl ServerControl {
    /// Abruptly close every open client connection (both directions), as a
    /// network failure would. Their sessions close; objects are unaffected.
    pub fn disconnect_all(&self) {
        let _ = self.commands.unbounded_send(Command::DisconnectAll);
    }
}

type Registry = Rc<RefCell<HashMap<ConnId, TcpStream>>>;

/// A bound storage-node listener.
pub struct Server<B> {
    listener: TcpListener,
    store: Rc<Store<B>>,
    connections: Registry,
    commands: mpsc::UnboundedReceiver<Command>,
    control: ServerControl,
}

impl<B: Backend + 'static> Server<B> {
    /// Bind the listener. Must be called inside a compio runtime.
    pub async fn bind(addr: impl ToSocketAddrsAsync, store: Rc<Store<B>>) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let (tx, commands) = mpsc::unbounded();
        Ok(Self {
            listener,
            store,
            connections: Rc::default(),
            commands,
            control: ServerControl { commands: tx },
        })
    }

    /// The bound address (useful after binding port 0).
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// A handle for controlling the server from any thread.
    pub fn control(&self) -> ServerControl {
        self.control.clone()
    }

    /// Accept and serve connections forever.
    pub async fn run(self) -> io::Result<()> {
        let Server {
            listener,
            store,
            connections,
            mut commands,
            control,
        } = self;
        drop(control);
        let registry = Rc::clone(&connections);
        compio::runtime::spawn(async move {
            while let Some(command) = commands.next().await {
                match command {
                    Command::DisconnectAll => {
                        for stream in registry.borrow().values() {
                            shutdown(stream);
                        }
                    }
                }
            }
        })
        .detach();
        tracing::info!(addr = %listener.local_addr()?, node_id = store.node_id(), "listening");
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(error) => {
                    tracing::warn!(%error, "accept failed");
                    continue;
                }
            };
            let _ = stream.set_nodelay(true);
            let store = Rc::clone(&store);
            let connections = Rc::clone(&connections);
            compio::runtime::spawn(serve_connection(store, connections, stream, peer)).detach();
        }
    }
}

/// Close both directions of a socket, waking any pending read or write.
fn shutdown(stream: &TcpStream) {
    let _ = socket2::SockRef::from(stream).shutdown(Shutdown::Both);
}

async fn serve_connection<B: Backend>(
    store: Rc<Store<B>>,
    connections: Registry,
    stream: TcpStream,
    peer: SocketAddr,
) {
    let (outbox, queue) = mpsc::unbounded();
    let conn = ConnCtx {
        id: store.new_connection_id(),
        outbox,
    };
    tracing::debug!(conn = conn.id, %peer, "connection opened");
    connections.borrow_mut().insert(conn.id, stream.clone());
    let writer = compio::runtime::spawn(write_loop(stream.clone(), queue));
    read_loop(&store, &conn, stream).await;
    // Closing the sessions drops their outbox clones; the writer then drains
    // what is queued and closes the socket.
    store.connection_closed(conn.id);
    connections.borrow_mut().remove(&conn.id);
    drop(conn);
    let _ = writer.await;
    tracing::debug!(%peer, "connection closed");
}

async fn read_loop<B: Backend>(store: &Store<B>, conn: &ConnCtx, mut stream: TcpStream) {
    let mut buf = BytesMut::with_capacity(READ_BUF_BYTES);
    let mut greeted = false;
    loop {
        loop {
            match decode_client_frame(&mut buf) {
                Ok(Some(frame)) => {
                    if !handle_frame(store, conn, &mut greeted, frame).await {
                        return;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    tracing::warn!(conn = conn.id, %error, "bad frame; closing the connection");
                    return;
                }
            }
        }
        // `append` reads into the spare capacity only; with none it would
        // return 0 and look like end of stream.
        if buf.capacity() - buf.len() < READ_BUF_BYTES / 4 {
            buf.reserve(READ_BUF_BYTES);
        }
        let BufResult(result, returned) = stream.append(buf).await;
        buf = returned;
        match result {
            Ok(0) => return,
            Ok(_) => {}
            Err(error) => {
                tracing::debug!(conn = conn.id, %error, "read failed");
                return;
            }
        }
    }
}

/// Apply one frame. Returns `false` when the connection must close.
async fn handle_frame<B: Backend>(
    store: &Store<B>,
    conn: &ConnCtx,
    greeted: &mut bool,
    frame: ClientFrame,
) -> bool {
    let ClientFrame { request_id, body } = frame;
    let mut keep_open = true;
    let reply = if *greeted {
        store.handle(conn, body).await
    } else {
        match body {
            Request::Hello {
                protocol_version,
                client_name,
            } if protocol_version == PROTOCOL_VERSION => {
                tracing::debug!(conn = conn.id, client_name, "handshake");
                *greeted = true;
                Ok(Response::HelloOk {
                    node_id: store.node_id().to_string(),
                    protocol_version: PROTOCOL_VERSION,
                })
            }
            Request::Hello {
                protocol_version, ..
            } => {
                keep_open = false;
                Err(WireError::new(
                    WireCode::Unimplemented,
                    format!(
                        "protocol version {protocol_version} unsupported (server speaks {PROTOCOL_VERSION})"
                    ),
                ))
            }
            _ => Err(WireError::new(
                WireCode::InvalidArgument,
                "the first request must be Hello",
            )),
        }
    };
    let _ = conn.outbox.unbounded_send(ServerFrame::Response {
        request_id,
        body: reply,
    });
    keep_open
}

fn encode(frame: &ServerFrame) -> Vec<u8> {
    match encode_server_frame(frame) {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::error!(%error, "cannot encode a server frame");
            // Answer the request with an error rather than leaving it hanging.
            let fallback = match frame {
                ServerFrame::Response { request_id, .. } => ServerFrame::Response {
                    request_id: *request_id,
                    body: Err(WireError::new(WireCode::Internal, error.to_string())),
                },
                ServerFrame::Event(_) => return Vec::new(),
            };
            encode_server_frame(&fallback).unwrap_or_default()
        }
    }
}

async fn write_loop(mut stream: TcpStream, mut queue: mpsc::UnboundedReceiver<ServerFrame>) {
    while let Some(frame) = queue.next().await {
        let mut batch = encode(&frame);
        while batch.len() < WRITE_BATCH_BYTES {
            match queue.try_recv() {
                Ok(frame) => batch.extend_from_slice(&encode(&frame)),
                Err(_) => break,
            }
        }
        let BufResult(result, _) = stream.write_all(batch).await;
        if let Err(error) = result {
            tracing::debug!(%error, "write failed; closing the connection");
            // Wake the reader so the connection tears down.
            shutdown(&stream);
            return;
        }
    }
    let _ = stream.shutdown().await;
}
