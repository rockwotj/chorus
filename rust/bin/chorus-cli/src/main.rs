use anyhow::{bail, Context, Result};
use chorus_client::{
    BearerAuth, ClientConfig, GrpcReplicaFactory, RefreshingAuthConfig, SegmentedVolume,
    WalEngineConfig, WalSeqNo,
};
use clap::{Parser, Subcommand};
use futures::TryStreamExt;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(about = "Zonal quorum append client for GCS Rapid appendable objects (1, 3, or 5 zones)")]
struct Args {
    #[arg(long, value_delimiter = ',', num_args = 1..=5)]
    endpoints: Vec<String>,
    #[arg(long, value_delimiter = ',', num_args = 1..=5)]
    buckets: Vec<String>,
    /// Endpoint of the regional bucket hosting the manifest register.
    #[arg(long)]
    manifest_endpoint: String,
    /// Full v2 resource name of the regional manifest bucket.
    #[arg(long)]
    manifest_bucket: String,
    #[arg(long, alias = "object")]
    prefix: String,
    #[arg(long, env = "GCS_BEARER_TOKEN")]
    bearer_token: Option<String>,
    #[arg(long, conflicts_with = "bearer_token")]
    anonymous: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Append {
        #[arg(long, conflicts_with = "file")]
        data: Option<String>,
        #[arg(long, conflicts_with = "data")]
        file: Option<std::path::PathBuf>,
    },
    RepairSealed,
    TruncateBefore {
        #[arg(long)]
        record_index: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let zones = args.endpoints.len();
    if args.buckets.len() != zones {
        bail!("--endpoints and --buckets must list the same number of zones");
    }
    if !matches!(zones, 1 | 3 | 5) {
        bail!("1, 3, or 5 comma-separated endpoints and buckets are required");
    }
    let auth = if args.anonymous {
        None
    } else if let Some(token) = args.bearer_token {
        Some(BearerAuth::static_token(token))
    } else {
        Some(
            BearerAuth::google_adc(RefreshingAuthConfig::default())
                .await
                .context("load Google Application Default Credentials")?,
        )
    };
    let mut factories = Vec::with_capacity(zones);
    for zone in 0..zones {
        let factory = match &auth {
            Some(auth) => {
                GrpcReplicaFactory::connect_with_auth(
                    zone,
                    &args.endpoints[zone],
                    args.buckets[zone].clone(),
                    auth.clone(),
                )
                .await
            }
            None => {
                GrpcReplicaFactory::connect(
                    zone,
                    &args.endpoints[zone],
                    args.buckets[zone].clone(),
                    None,
                )
                .await
            }
        }
        .with_context(|| format!("connect zone {zone}"))?;
        factories.push(factory);
    }
    let manifest_factory = match &auth {
        Some(auth) => {
            GrpcReplicaFactory::connect_with_auth(
                zones,
                &args.manifest_endpoint,
                args.manifest_bucket.clone(),
                auth.clone(),
            )
            .await
        }
        None => {
            GrpcReplicaFactory::connect(
                zones,
                &args.manifest_endpoint,
                args.manifest_bucket.clone(),
                None,
            )
            .await
        }
    }
    .context("connect regional manifest bucket")?;
    let volume = SegmentedVolume::new(
        factories,
        manifest_factory,
        &args.prefix,
        ClientConfig::default(),
    )?;
    match args.command {
        Command::Append { data, file } => {
            let mut recovery = volume.recover_from_committed_floor().await?;
            let next_seqno = recovery.end;
            while recovery.try_next().await?.is_some() {}
            let mut wal = recovery.start(WalEngineConfig::default()).await?;
            let payload = match (data, file) {
                (Some(data), None) => data.into_bytes(),
                (None, Some(path)) => std::fs::read(path)?,
                (None, None) => bail!("append requires --data or --file"),
                (Some(_), Some(_)) => unreachable!("clap enforces conflicts"),
            };
            let receipt = wal
                .enqueue_append(next_seqno, payload.into())
                .await?
                .await?;
            println!(
                "committed global record index {}",
                receipt.seqno.record_index
            );
            wal.shutdown().await?;
        }
        Command::RepairSealed => {
            let report = volume.repair_sealed_segments().await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "segments_examined": report.segments_examined,
                    "objects_repaired": report.objects_repaired,
                    "objects_already_healthy": report.objects_already_healthy,
                    "transient_failures": report.transient_failures,
                }))?
            );
        }
        Command::TruncateBefore { record_index } => {
            let mut recovery = volume.recover_from_committed_floor().await?;
            while recovery.try_next().await?.is_some() {}
            let wal = recovery.start(WalEngineConfig::default()).await?;
            let report = wal.truncate_before(WalSeqNo::record(record_index)).await?;
            println!(
                "deleted {} segments and {} zonal objects",
                report.deleted_segments, report.deleted_objects
            );
            wal.shutdown().await?;
        }
    }
    Ok(())
}
