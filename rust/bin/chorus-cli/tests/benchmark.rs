use std::process::{Command, Stdio};
use std::time::Duration;

use chorus_fake_gcs::FakeGcs;
use serde_json::Value;

async fn benchmark(args: &[&str]) -> Value {
    let mut servers = Vec::new();
    for _ in 0..4 {
        servers.push(FakeGcs::default().start().await.unwrap());
    }
    let endpoints = servers[..3]
        .iter()
        .map(|server| server.endpoint.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let mut child = Command::new(env!("CARGO_BIN_EXE_chorus"))
        .args([
            "--anonymous",
            "--endpoints",
            &endpoints,
            "--buckets",
            "projects/_/buckets/z0,projects/_/buckets/z1,projects/_/buckets/z2",
            "--manifest-endpoint",
            &servers[3].endpoint,
            "--manifest-bucket",
            "projects/_/buckets/regional",
            "--prefix",
            "benchmark-smoke",
            "benchmark",
        ])
        .args(args)
        .env("RUST_LOG", "error")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let finished = tokio::time::timeout(Duration::from_secs(30), async {
        while child.try_wait().unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if finished.is_err() {
        child.kill().unwrap();
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        finished.is_ok() && output.status.success(),
        "benchmark failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn assert_latency(latency: &Value) {
    for field in ["p50", "p99", "p99_9", "max"] {
        assert!(
            latency[field].as_u64().unwrap() > 0,
            "missing latency sample: {field}"
        );
    }
    assert!(latency["p50"].as_u64() <= latency["p99"].as_u64());
    assert!(latency["p99"].as_u64() <= latency["p99_9"].as_u64());
    assert!(latency["p99_9"].as_u64() <= latency["max"].as_u64());
}

#[tokio::test]
async fn append_benchmarks_keep_latency_and_throughput_without_internal_counters() {
    for arrival_rate in ["0", "100"] {
        let report = benchmark(&[
            "append",
            "--duration-seconds",
            "1",
            "--arrival-rate",
            arrival_rate,
            "--payload-bytes",
            "32",
            "--outstanding-appends",
            "8",
        ])
        .await;
        assert_latency(&report["latency_us"]);
        assert!(report["payload_mib_per_second"].as_f64().unwrap() > 0.0);
        assert!(report["record_iops"].as_f64().unwrap() > 0.0);
        assert!(report["committed_records"].as_u64().unwrap() > 0);
        assert_eq!(report["drain_timed_out"], false);
        assert!(
            report["configuration"]["max_inflight_bytes"]
                .as_u64()
                .unwrap()
                > 0
        );
        for removed in [
            "batches_sent",
            "records_per_persist",
            "wal_record_bytes",
            "replica_bytes_attempted",
            "write_amplification",
            "max_inflight_records",
            "max_inflight_bytes",
            "lane_capacity_drops",
            "lane_timeouts",
            "pipeline_refills",
        ] {
            assert!(
                report.get(removed).is_none(),
                "obsolete diagnostic: {removed}"
            );
        }
    }
}

#[tokio::test]
async fn recovery_benchmark_keeps_phase_latencies_without_operation_counts() {
    let report = benchmark(&[
        "recovery",
        "--populate-records",
        "100",
        "--target-sealed-segments",
        "2",
        "--iterations",
        "2",
        "--payload-bytes",
        "32",
        "--populate-window",
        "8",
    ])
    .await;
    for phase in ["epoch_claim", "prepare", "replay", "start", "total"] {
        assert_latency(&report["phase_latency_us"][phase]);
    }
    assert_eq!(report["replayed_records_avg"], 100.0);
    assert!(report["observed_sealed_segments_avg"].as_f64().unwrap() > 0.0);
    assert!(report.get("manifest_cas_attempts_avg").is_none());
    assert!(report.get("segments_sealed_avg").is_none());
}
