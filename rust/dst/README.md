# chorus-dst

`chorus-dst` is the deterministic failure-injection harness for the Chorus
client. It is verification infrastructure, not a storage service or a
production deployment mode.

## Simulation model

Each seed runs the production WAL protocol against three zonal
`chorus-fake-gcs` instances and one regional manifest instance. The primary
harness uses `InMemoryReplicaFactory`, which drives the fake service directly
without HTTP/2, TCP, or spawned stream-reader tasks. The client protocol,
recovery, segment rotation, repair, truncation, and admission logic remain the
same implementation used with the production transport; only the transport
adapter and service are replaced.

The harness runs in turmoil's single-threaded virtual-time runtime. Seeded
schedules cover:

- transient, delayed, redirected, expired, and throttled storage operations;
- partial writes, ambiguous outcomes, stream fences, and competing writers;
- zone crashes and restarts, including recovery with one zone unavailable;
- automatic rotation, sealed-copy repair, replay, and truncation;
- corruption and divergence during reduced-redundancy windows;
- admission pressure and lane-stall behavior.

Adversarial scenarios run as a 16-phase deck. Seed `0` keeps the fixed phase
order for debugging; other seeds shuffle each complete deck. Every seed is
executed twice, and the two runs must produce the same trace digest.

Targeted transport tests also run tonic over turmoil TCP. Those tests cover
transport-specific behavior, but they are not the main certification path.
Kernel networking, separate processes, and live GCS behavior require separate
integration or chaos tests.

## Run a smoke test

From `rust/`:

```sh
cargo run --release -p chorus-dst -- \
  --seeds 10 \
  --steps 128 \
  --trace ../artifacts/dst-trace.jsonl \
  --batch-dir ../artifacts/dst-smoke-batch
```

Use `--inject-latency` to add deterministic normal-service latency in addition
to explicit faults. Without `--pobserve-jar`, this command emits a warning,
performs only structural trace validation, and leaves its per-seed traces in
the batch directory. Run `cargo run -p chorus-dst -- --help` for the full CLI.

## Trace validation

Trace checking has two intentionally separate layers:

1. `chorus-trace-checker` parses JSONL, verifies contiguous sequence numbers,
   rejects undeclared events, and requires the Rust event list to exactly match
   `p/TRACE_EVENTS.txt`.
2. The generated PObserve adapter evaluates protocol semantics using the
   monitors in `p/model/Monitors.p`.

The structural checker is a second binary in this package:

```sh
cargo run --release -p chorus-dst --bin chorus-trace-checker -- \
  ../artifacts/dst-trace.jsonl \
  --event-manifest ../p/TRACE_EVENTS.txt
```

Structural acceptance does not imply semantic conformance. Run PObserve over
the same trace for that gate:

```sh
java -jar ../p/pobserve/target/chorus-pobserve.jar \
  ../artifacts/dst-trace.jsonl
```

## Certification

The repository orchestrator requires a previously built PObserve JAR. Build the
generated adapter, then start certification from the repository root:

```sh
cd ../p
p compile -pp QuorumModel.pproj -md pobserve
mvn -q -f pobserve/pom.xml package
cd ..
python3 docker/scripts/orchestrate.py certify --wall-seconds 3600
```

The orchestrator passes the JAR to `chorus-dst`, which checks every seed batch
with fresh PObserve monitor instances for each trace. During certification,
traces are written by default as
`artifacts/cert-batch/seed-{seed}.jsonl`. Accepted batches are deleted. A
rejected batch is retained and causes a non-zero exit with the failing trace
and monitor in the diagnostic. The final partial batch is always checked.

The receipt at `artifacts/dst-certification.json` records the elapsed wall
time, seed count, last trace digest, source digest, Rust compiler version,
`Cargo.lock` digest, source commit, and dirty-tree state. The orchestrator does
not permit an unmonitored certification run. Omitting `--pobserve-jar` is
available only when invoking `chorus-dst` directly for development; that mode
emits a warning and produces only structurally validated traces.
