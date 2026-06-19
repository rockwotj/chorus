use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use chorus_dst::{production, SimulationReport};
use clap::Parser;
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Parser, Debug)]
#[command(about = "Deterministic failure-injection simulator for quorum replication")]
struct Args {
    #[arg(long, default_value_t = 0)]
    start_seed: u64,
    #[arg(long, default_value_t = 0)]
    wall_seconds: u64,
    #[arg(long, default_value_t = 100)]
    seeds: u64,
    #[arg(long, default_value_t = 1_000)]
    steps: u64,
    #[arg(long, default_value = "../artifacts/dst-trace.jsonl")]
    trace: PathBuf,
    #[arg(long, default_value = "../artifacts/cert-batch")]
    batch_dir: PathBuf,
    #[arg(long, default_value_t = NonZeroUsize::new(50).unwrap())]
    batch_size: NonZeroUsize,
    #[arg(long)]
    pobserve_jar: Option<PathBuf>,
    #[arg(long)]
    receipt: Option<PathBuf>,
    #[arg(long, default_value = "unspecified")]
    source_digest: String,
    #[arg(long)]
    source_root: Option<PathBuf>,
    #[arg(long)]
    cargo_lock: Option<PathBuf>,
    /// Model normal GCS service latency in addition to injected faults.
    #[arg(long)]
    inject_latency: bool,
}

#[derive(Serialize)]
struct Receipt {
    schema: u32,
    passed: bool,
    requested_wall_seconds: u64,
    elapsed_wall_millis: u128,
    started_unix_ms: u128,
    completed_unix_ms: u128,
    seeds_completed: u64,
    steps_per_seed: u64,
    last_seed: u64,
    last_trace_digest: String,
    source_digest: String,
    rustc_version: String,
    cargo_lock_sha256: String,
    source_commit: Option<String>,
    dirty_tree: Option<bool>,
    mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<String>,
}

struct ExecutionEnvironment {
    rustc_version: String,
    cargo_lock_sha256: String,
    source_commit: Option<String>,
    dirty_tree: Option<bool>,
}

struct CertificationRun {
    last: SimulationReport,
    seeds_completed: u64,
    monitor_failure: Option<anyhow::Error>,
}

// No outer tokio runtime: each seed runs inside a turmoil simulation, which
// owns its runtimes and virtual clocks. The wall-clock budget below stays real
// `std::time::Instant` — virtual time runs far faster than wall time, and the
// certification receipt's fields are deliberately host-execution semantics.
fn main() -> Result<()> {
    let args = Args::parse();
    prepare_batch_directory(&args.batch_dir)?;
    if let Some(jar) = &args.pobserve_jar {
        if !jar.is_file() {
            bail!("PObserve jar does not exist: {}", jar.display());
        }
    } else {
        eprintln!(
            "warning: --pobserve-jar was not provided; per-seed traces will not be monitor-checked"
        );
    }
    let environment = args
        .receipt
        .as_ref()
        .map(|_| capture_execution_environment(&args))
        .transpose()?;
    let start_wall = Instant::now();
    let started_unix_ms = unix_ms();
    let minimum = Duration::from_secs(args.wall_seconds);
    let run = certify(&args, start_wall, minimum)?;
    write_trace(&args.trace, &run.last)?;
    let elapsed = start_wall.elapsed();
    if let Some(path) = &args.receipt {
        let environment = environment.expect("receipt provenance was captured before execution");
        let receipt = Receipt {
            schema: 1,
            passed: run.monitor_failure.is_none() && elapsed >= minimum,
            requested_wall_seconds: args.wall_seconds,
            elapsed_wall_millis: elapsed.as_millis(),
            started_unix_ms,
            completed_unix_ms: unix_ms(),
            seeds_completed: run.seeds_completed,
            steps_per_seed: args.steps,
            last_seed: run.last.seed,
            last_trace_digest: run.last.digest.clone(),
            source_digest: args.source_digest.clone(),
            rustc_version: environment.rustc_version,
            cargo_lock_sha256: environment.cargo_lock_sha256,
            source_commit: environment.source_commit,
            dirty_tree: environment.dirty_tree,
            mode: if args.inject_latency {
                "production-latency".into()
            } else {
                "production".into()
            },
            failure: run.monitor_failure.as_ref().map(ToString::to_string),
        };
        write_receipt(path, &receipt)?;
    }
    if let Some(failure) = run.monitor_failure {
        return Err(failure);
    }
    println!(
        "DST passed: seeds={} elapsed={:.3}s last_digest={}",
        run.seeds_completed,
        elapsed.as_secs_f64(),
        run.last.digest
    );
    Ok(())
}

fn certify(args: &Args, start_wall: Instant, minimum: Duration) -> Result<CertificationRun> {
    let mut seed = args.start_seed;
    let mut pending = Vec::new();
    let mut last = execute_seed(args, seed, &mut pending)?;
    let mut seeds_completed = 1;

    loop {
        if pending.len() == args.batch_size.get() {
            if let Err(failure) = observe_batch(args, &mut pending) {
                return Ok(CertificationRun {
                    last,
                    seeds_completed,
                    monitor_failure: Some(failure),
                });
            }
        }

        let complete = if args.wall_seconds == 0 {
            seeds_completed >= args.seeds
        } else {
            start_wall.elapsed() >= minimum
        };
        if complete {
            break;
        }

        seed = seed.checked_add(1).context("DST seed range overflow")?;
        last = execute_seed(args, seed, &mut pending)?;
        seeds_completed += 1;
    }

    if let Err(failure) = observe_batch(args, &mut pending) {
        return Ok(CertificationRun {
            last,
            seeds_completed,
            monitor_failure: Some(failure),
        });
    }

    Ok(CertificationRun {
        last,
        seeds_completed,
        monitor_failure: None,
    })
}

fn execute_seed(args: &Args, seed: u64, pending: &mut Vec<PathBuf>) -> Result<SimulationReport> {
    let report =
        production::assert_deterministic_with_latency(seed, args.steps, args.inject_latency)
            .with_context(|| format!("DST seed {seed} failed"))?;
    let batch_trace = args.batch_dir.join(format!("seed-{seed}.jsonl"));
    write_trace(&batch_trace, &report)?;
    pending.push(batch_trace);
    Ok(report)
}

fn prepare_batch_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path).with_context(|| {
        format!(
            "create DST certification batch directory {}",
            path.display()
        )
    })?;

    // The directory is scratch space owned by this certification run. Accepted
    // batches remove themselves; stale seed traces therefore mean a prior
    // rejection or an intentionally unmonitored run. Clearing only our
    // deterministic names prevents those traces from contaminating a later
    // directory-mode PObserve invocation without deleting unrelated artifacts.
    for entry in fs::read_dir(path)
        .with_context(|| format!("read DST certification batch directory {}", path.display()))?
    {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if entry.file_type()?.is_file() && name.starts_with("seed-") && name.ends_with(".jsonl") {
            fs::remove_file(entry.path())
                .with_context(|| format!("remove stale batch trace {}", entry.path().display()))?;
        }
    }
    Ok(())
}

fn observe_batch(args: &Args, pending: &mut Vec<PathBuf>) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let Some(jar) = &args.pobserve_jar else {
        pending.clear();
        return Ok(());
    };

    let output = Command::new("java")
        .arg("-jar")
        .arg(jar)
        .arg(&args.batch_dir)
        .output()
        .with_context(|| format!("run PObserve jar {}", jar.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let detail = if stderr.trim().is_empty() {
            stdout.trim()
        } else {
            stderr.trim()
        };
        bail!("PObserve batch failed with {}: {detail}", output.status);
    }

    io::stdout().write_all(&output.stdout)?;
    io::stderr().write_all(&output.stderr)?;
    for trace in pending.iter() {
        fs::remove_file(trace)
            .with_context(|| format!("remove accepted batch trace {}", trace.display()))?;
    }
    pending.clear();
    Ok(())
}

fn write_trace(path: &Path, report: &SimulationReport) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut writer = BufWriter::new(File::create(path)?);
    for event in &report.events {
        serde_json::to_writer(&mut writer, event)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    Ok(())
}

fn write_receipt(path: &Path, receipt: &Receipt) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(receipt)?)?;
    fs::rename(temporary, path)?;
    Ok(())
}

fn capture_execution_environment(args: &Args) -> Result<ExecutionEnvironment> {
    let cargo_lock = args
        .cargo_lock
        .clone()
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../Cargo.lock"));
    let source_root = args.source_root.clone().unwrap_or_else(|| {
        cargo_lock
            .parent()
            .and_then(|rust| rust.parent())
            .expect("the workspace Cargo.lock has a repository parent")
            .to_path_buf()
    });

    let rustc = Command::new("rustc")
        .arg("-V")
        .output()
        .context("run rustc -V for certification provenance")?;
    if !rustc.status.success() {
        bail!("rustc -V failed with {}", rustc.status);
    }
    let rustc_version = String::from_utf8(rustc.stdout)
        .context("rustc -V returned non-UTF-8 output")?
        .trim()
        .to_string();

    let cargo_lock_bytes = fs::read(&cargo_lock)
        .with_context(|| format!("read Cargo.lock at {}", cargo_lock.display()))?;
    let cargo_lock_sha256 = hex::encode(Sha256::digest(cargo_lock_bytes));
    let (source_commit, dirty_tree) = capture_git_provenance(&source_root);

    Ok(ExecutionEnvironment {
        rustc_version,
        cargo_lock_sha256,
        source_commit,
        dirty_tree,
    })
}

fn capture_git_provenance(source_root: &PathBuf) -> (Option<String>, Option<bool>) {
    let Ok(commit) = Command::new("git")
        .args(["rev-parse", "--verify", "HEAD"])
        .current_dir(source_root)
        .output()
    else {
        return (None, None);
    };
    if !commit.status.success() {
        return (None, None);
    }
    let Ok(source_commit) = String::from_utf8(commit.stdout) else {
        return (None, None);
    };

    let Ok(status) = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=normal"])
        .current_dir(source_root)
        .output()
    else {
        return (None, None);
    };
    if !status.status.success() {
        return (None, None);
    }

    (
        Some(source_commit.trim().to_string()),
        Some(!status.stdout.is_empty()),
    )
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis()
}
