//! `chorus-storage-server`: run one Chorus storage node.

use std::path::{Path, PathBuf};
use std::rc::Rc;

use anyhow::Context;
use chorus_storage_server::{MemoryBackend, Server, Store};
use clap::Parser;

/// Serve GCS-Rapid-style appendable objects over the Chorus TCP protocol.
#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    /// Address to listen on.
    #[arg(long, env = "CHORUS_LISTEN", default_value = "0.0.0.0:7070")]
    listen: String,
    /// Data directory. Holds the node id; object data is kept in memory for
    /// now.
    #[arg(long, env = "CHORUS_DATA_DIR")]
    data_dir: Option<PathBuf>,
    /// Stable node identity reported to clients. Defaults to the id stored in
    /// the data directory (created on first start), else a random one.
    #[arg(long, env = "CHORUS_NODE_ID")]
    node_id: Option<String>,
}

fn generated_node_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("node-{:x}-{:x}", std::process::id(), nanos)
}

/// The node id stored in `data_dir`, creating it on first start.
fn stored_node_id(data_dir: &Path) -> anyhow::Result<String> {
    let path = data_dir.join("node-id");
    match std::fs::read_to_string(&path) {
        Ok(id) if !id.trim().is_empty() => return Ok(id.trim().to_string()),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).with_context(|| format!("read {}", path.display())),
    }
    std::fs::create_dir_all(data_dir).with_context(|| format!("create {}", data_dir.display()))?;
    let id = generated_node_id();
    std::fs::write(&path, format!("{id}\n"))
        .with_context(|| format!("write {}", path.display()))?;
    Ok(id)
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
    let node_id = match (&args.node_id, &args.data_dir) {
        (Some(id), _) => id.clone(),
        (None, Some(dir)) => stored_node_id(dir)?,
        (None, None) => generated_node_id(),
    };
    let store = Rc::new(Store::open(node_id, MemoryBackend::new()).await?);
    let server = Server::bind(args.listen.as_str(), store)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    server.run().await?;
    Ok(())
}
