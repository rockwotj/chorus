# chorus-dst

`chorus-dst` provides deterministic failure-injection testing for Chorus.

`production` mode starts three zonal `chorus-fake-gcs` gRPC servers plus one
regional manifest server and exercises only the normal public
`GrpcReplicaFactory`, `SegmentedVolume`, `Recovery`, and `WalHandle` APIs. It
imports no client protocol types or simulator hooks.

Seeded schedules inject transient and delayed RPCs, zone crashes/restarts,
partial writes, competing writer incarnations, takeover, admission bursts,
automatic segment rotation, immutable sealed-copy repair, startup replay,
truncation, and corruption of the two committing copies during the
reduced-redundancy window. Recovery is exercised with one zone held down. Each
seed runs twice and must emit an identical refinement trace. Segment and
manifest observations come directly from explicit fake-service state
snapshots. Fault phases run as a seeded shuffled deck, with every phase exactly
once per 16-phase epoch; seed zero retains the fixed phase order for debugging.
Production-mode repair uses only deterministic engine-start and post-rotation
triggers; the periodic repair interval is disabled and no wall-clock timer
drives convergence.
Persistence, creation, finalization, and deletion observations retain every
actual supporting zone in sorted order rather than normalizing witness identity.
Manifest snapshots produce `EpochClaimed`, `ViewCommitted`, and
`FloorCommitted` observations.

`chorus-trace-checker` validates the JSON envelope and event manifest. The semantic
gate compiles `p/model/Monitors.p` with PObserve and feeds the production trace
into the generated monitor classes. A checked-in JSONL fixture separately keeps
the structural validator pinned to every declared event class.

Long certification runs retain every seed long enough to replay it through
PObserve. Each seed is written deterministically as
`--batch-dir/seed-{seed}.jsonl`; every `--batch-size` traces (50 by default) the
Java adapter checks the directory with fresh monitor instances per trace.
Accepted files are deleted, while a rejected batch is retained and makes the
receipt fail with the offending seed path and monitor name before the Rust
binary exits non-zero. The final partial batch is always checked. `--trace`
still receives the last seed for compatibility with the single-trace
development gate. Omitting `--pobserve-jar` is allowed for structural-only
development runs, but emits a warning and leaves the per-seed traces
unverified.

The real tonic transport runs over turmoil's simulated TCP network with a
single-threaded scheduler, fixed seed-derived latency, and virtual time. Kernel
network behavior and separate operating-system processes belong to a different
chaos-test layer.

```sh
cargo run -p chorus-dst -- \
  --seeds 10 --steps 128 \
  --trace ../artifacts/dst-trace.jsonl \
  --batch-dir ../artifacts/cert-batch \
  --pobserve-jar ../p/pobserve/target/chorus-pobserve.jar

cd ..
python3 docker/scripts/orchestrate.py certify --wall-seconds 3600
```

Certification should run through the pinned Docker verification image. Its
receipt records measured runtime, achieved seed count, source digest, compiler
version, `rust/Cargo.lock` SHA-256, and whether the source tree was dirty in
`artifacts/dst-certification.json`.
