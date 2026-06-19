#![cfg(unix)]

use std::env;
use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test clock is after the Unix epoch")
            .as_nanos();
        let path = env::temp_dir().join(format!("chorus-dst-cert-{}-{unique}", std::process::id()));
        fs::create_dir(&path).expect("create test directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).ok();
    }
}

#[test]
fn pobserve_rejection_fails_certification_and_writes_failed_receipt() {
    let temporary = TestDirectory::new();
    let fake_bin = temporary.path().join("bin");
    fs::create_dir(&fake_bin).expect("create fake binary directory");
    let java = fake_bin.join("java");
    fs::write(
        &java,
        r#"#!/bin/sh
set -eu
batch="$3"
trace="$(find "$batch" -type f -name 'seed-*.jsonl' | sort | head -n 1)"
cp "$CHORUS_BAD_TRACE" "$trace"
echo "$trace: PObserve monitor GetSizeExcludesOpenTail rejected line 1: eGetSizeObserved" >&2
exit 1
"#,
    )
    .expect("write fake java");
    fs::set_permissions(&java, fs::Permissions::from_mode(0o755))
        .expect("make fake java executable");

    let fake_jar = temporary.path().join("chorus-pobserve.jar");
    fs::write(&fake_jar, []).expect("write fake PObserve jar");
    let batch_dir = temporary.path().join("batch");
    let receipt = temporary.path().join("receipt.json");
    let last_trace = temporary.path().join("last.jsonl");
    let bad_trace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/pobserve-rejects-open-tail.jsonl");
    let path = prepend_path(&fake_bin);

    let output = Command::new(env!("CARGO_BIN_EXE_chorus-dst"))
        .args([
            "--start-seed",
            "7",
            "--seeds",
            "1",
            "--steps",
            "1",
            "--batch-size",
            "1",
            "--source-digest",
            "test",
        ])
        .arg("--trace")
        .arg(&last_trace)
        .arg("--batch-dir")
        .arg(&batch_dir)
        .arg("--pobserve-jar")
        .arg(&fake_jar)
        .arg("--receipt")
        .arg(&receipt)
        .env("PATH", path)
        .env("CHORUS_BAD_TRACE", &bad_trace)
        .output()
        .expect("run chorus-dst");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("seed-7.jsonl"), "{stderr}");
    assert!(stderr.contains("GetSizeExcludesOpenTail"), "{stderr}");

    let receipt: serde_json::Value =
        serde_json::from_slice(&fs::read(&receipt).expect("read receipt")).expect("parse receipt");
    assert_eq!(receipt["passed"], false);
    assert_eq!(receipt["seeds_completed"], 1);
    assert_eq!(receipt["last_seed"], 7);
    let failure = receipt["failure"]
        .as_str()
        .expect("failed receipt names its cause");
    assert!(failure.contains("seed-7.jsonl"));
    assert!(failure.contains("GetSizeExcludesOpenTail"));
    assert!(last_trace.is_file());

    let rejected = fs::read_to_string(batch_dir.join("seed-7.jsonl"))
        .expect("rejected batch trace is retained");
    let fixture = fs::read_to_string(bad_trace).expect("read bad trace fixture");
    assert_eq!(rejected, fixture);
}

fn prepend_path(directory: &Path) -> OsString {
    let mut paths = vec![directory.to_path_buf()];
    paths.extend(env::split_paths(&env::var_os("PATH").unwrap_or_default()));
    env::join_paths(paths).expect("join test PATH")
}
