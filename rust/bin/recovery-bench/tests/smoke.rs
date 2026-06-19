//! Offline smoke test: drive the `recovery-bench` binary against loopback
//! fake-GCS servers and assert it populates, recovers, and emits the phase
//! breakdown. This validates the binary logic without a cloud VM or real
//! buckets; representative latency numbers come from an in-region run.

use chorus_fake_gcs::FakeGcs;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_bench_populates_recovers_and_reports_phases() {
    // Three zonal data fakes plus one regional manifest fake, each on its own
    // loopback port, mirroring the production topology.
    let mut zonal = Vec::new();
    for _ in 0..3 {
        zonal.push(FakeGcs::default().start().await.expect("start zonal fake"));
    }
    let regional = FakeGcs::default()
        .start()
        .await
        .expect("start regional fake");

    let endpoints = zonal
        .iter()
        .map(|f| f.endpoint.clone())
        .collect::<Vec<_>>()
        .join(",");
    let buckets = (0..3)
        .map(|z| format!("projects/_/buckets/zone-{z}"))
        .collect::<Vec<_>>()
        .join(",");

    let binary = env!("CARGO_BIN_EXE_recovery-bench");
    let output = tokio::process::Command::new(binary)
        .args([
            "--anonymous",
            "--endpoints",
            &endpoints,
            "--buckets",
            &buckets,
            "--manifest-endpoint",
            &regional.endpoint,
            "--manifest-bucket",
            "projects/_/buckets/regional",
            "--prefix",
            "smoke/recovery",
            "--populate-records",
            "400",
            "--target-sealed-segments",
            "2",
            "--replay-records",
            "400",
            "--iterations",
            "2",
            "--populate-window",
            "32",
        ])
        .output()
        .await
        .expect("run recovery-bench");

    assert!(
        output.status.success(),
        "recovery-bench failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse recovery-bench JSON");

    // Phase breakdown is present and the total is positive.
    let total_p50 = report["phase_latency_us"]["total"]["p50"]
        .as_u64()
        .expect("total p50 present");
    assert!(total_p50 > 0, "total recovery p50 should be positive");

    // Population actually created sealed segments and recovery replayed records.
    let sealed = report["observed_sealed_segments_avg"]
        .as_f64()
        .expect("sealed avg present");
    assert!(
        sealed >= 1.0,
        "expected at least one sealed segment, got {sealed}"
    );
    let replayed = report["replayed_records_avg"]
        .as_f64()
        .expect("replayed avg present");
    assert!(
        replayed > 0.0,
        "expected replay to cover records, got {replayed}"
    );

    for phase in ["epoch_claim", "prepare", "replay", "start"] {
        assert!(
            report["phase_latency_us"][phase]["p50"].is_u64(),
            "phase {phase} missing p50"
        );
    }

    drop(zonal);
    drop(regional);
}
