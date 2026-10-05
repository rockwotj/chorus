//! `chorus-storage-server`: run one Chorus storage node.

use std::path::PathBuf;
use std::rc::Rc;
use std::time::Instant;

use anyhow::Context;
use chorus_storage_server::{Backend, DiskBackend, MemoryBackend, Server, Store};
use clap::Parser;

/// Serve GCS-Rapid-style appendable objects over the Chorus TCP protocol.
#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    /// Address to listen on.
    #[arg(long, env = "CHORUS_LISTEN", default_value = "0.0.0.0:7070")]
    listen: String,
    /// Data directory holding `node.meta` and one directory per bucket.
    /// Required unless `--in-memory`.
    #[arg(
        long,
        env = "CHORUS_DATA_DIR",
        required_unless_present = "in_memory",
        conflicts_with = "in_memory"
    )]
    data_dir: Option<PathBuf>,
    /// Keep everything in process memory (nothing survives a restart); for
    /// tests and development.
    #[arg(long)]
    in_memory: bool,
    /// Stable node identity reported to clients. A new data directory stores
    /// it (default: generated); an existing one must match it.
    #[arg(long, env = "CHORUS_NODE_ID")]
    node_id: Option<String>,
    /// Sync every flush inline instead of coalescing flushes in background
    /// tasks (group commit).
    #[arg(long)]
    no_group_commit: bool,
}

fn generated_node_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("node-{:x}-{:x}", std::process::id(), nanos)
}

async fn serve<B: Backend + 'static>(
    args: &Args,
    node_id: String,
    backend: B,
) -> anyhow::Result<()> {
    let started = Instant::now();
    let store = Rc::new(
        Store::open(node_id, backend)
            .await
            .context("recover the store")?,
    );
    tracing::info!(
        node_id = store.node_id(),
        objects = store.object_count(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "recovered"
    );
    if !args.no_group_commit {
        store.enable_group_commit();
    }
    let server = Server::bind(args.listen.as_str(), store)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    server.run().await?;
    Ok(())
}

#[compio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();
    match &args.data_dir {
        Some(dir) => {
            let backend = DiskBackend::open(dir, args.node_id.clone())
                .await
                .with_context(|| format!("open data directory {}", dir.display()))?;
            tracing::info!(
                data_dir = %dir.display(),
                generation_high_water = backend.generation_high_water(),
                "opened data directory"
            );
            let node_id = backend.node_id().to_string();
            serve(&args, node_id, backend).await
        }
        None => {
            let node_id = args.node_id.clone().unwrap_or_else(generated_node_id);
            tracing::warn!("in-memory mode: nothing survives a restart");
            serve(&args, node_id, MemoryBackend::new()).await
        }
    }
}
