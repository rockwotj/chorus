use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use chorus_dst::{validate_trace_structure, TraceEvent, TRACE_EVENTS};
use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    trace: PathBuf,
    #[arg(long, default_value = "../p/TRACE_EVENTS.txt")]
    event_manifest: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let manifest = std::fs::read_to_string(&args.event_manifest)
        .with_context(|| format!("read {}", args.event_manifest.display()))?;
    let declared: Vec<_> = manifest.lines().filter(|line| !line.is_empty()).collect();
    if declared != TRACE_EVENTS {
        bail!("Rust/P transition event manifest mismatch");
    }
    let file = File::open(&args.trace).with_context(|| format!("open {}", args.trace.display()))?;
    let mut events = Vec::new();
    for (line, value) in BufReader::new(file).lines().enumerate() {
        let value = value?;
        events.push(
            serde_json::from_str::<TraceEvent>(&value)
                .with_context(|| format!("invalid JSONL at line {}", line + 1))?,
        );
    }
    validate_trace_structure(&events)?;
    println!("trace structure accepted: {} events", events.len());
    Ok(())
}
