# Live SlateDB reader validation

This standalone test crate exercises the working-tree `chorus-client` with
SlateDB 0.16.0 against real GCS. It is a correctness/lifecycle test, not a
throughput or latency benchmark. Its separate workspace and lockfile keep the
GCS object-store dependency out of the library's default feature set.

The current fixture is deliberately restricted to the `subspace-dev-2020`
experiment: three `subspace-dev-rapid-zonal-{1,2,3}` buckets and
`subspace-dev-regional`, under a fresh
`chorus-experiments/slatedb-readers-20260904-.../` prefix. Both processes use ADC.
Never point it at an existing database. The harness refuses a nonempty prefix.

Each run writes 224 deterministic atomic batches (16 KiB value, deletion, and
batch marker), uses 512 KiB physical segments, and checks:

- A separate reader process starts against an empty active writer without fencing
  it. Canceled empty-tail polling resumes correctly.
- One raw unbounded iterator, `FollowLatest`, and `ManagedCheckpoint` follow
  active-tail writes, segment rotation, writer restart, takeover, and GC.
- Every raw batch has exactly the expected three rows, contiguous WAL IDs, and
  strictly increasing, batch-atomic SlateDB sequence numbers. Database full
  scans, point reads, and range scans match a reference model at stage barriers.
- Bounded WAL replay ends without requiring another write. An unavailable future
  endpoint returns an error.
- A detached checkpoint created after WAL-only close retains 32 batches with
  **zero SSTs**. It remains readable through a fresh checkpoint reader after L0
  flush and GC. The persisted manifest bounds are asserted explicitly.
- Taking over an open writer fences subsequent writes from the old writer.
- Releasing the checkpoint allows the truncation floor to advance and deletes
  concrete physical segment objects in each zone. Lagging raw readers return
  latched `WalTruncated`; a fresh stale reader also fails.
- A fresh latest reader and a fresh database writer recover the final model,
  including the suffix never flushed into an SST. The rejected stale write is
  absent.

Reader polling is set to 100 ms for the raw tail and 250 ms for manifests and
SlateDB reader refresh. These are test settings, not recommendations. GC uses
zero minimum age and shares the active writer's initializer. Every writer open
attaches automatic GC and waits for its first callback result before the test
can deliberately close or fence the writer. Explicit checkpoint/reclamation
passes also run during the reader suite.
Every actual `WalGc::collect` call records its ranges and result, and the run
fails if any callback unexpectedly fails. SlateDB 0.16 treats a configured GC
directory's `interval: None` as the default interval, not as disabled scheduling.

The `startup-gc` mode additionally pauses replay until the first scheduled GC
callback arrives. It checks that GC stays pending and deletes nothing, then
releases replay and checks that the same callback reclaims the sealed prefix.
A separate cancelled-build case requires its waiting GC callback to return
`Closed`, without hanging or deleting data, and verifies a fresh reopen.
Those intentional cancellation errors are recorded separately from successful
startup and reader-suite callbacks. Object-set differences can include cleanup
of speculative objects, so not every disappeared object is attributable to GC.
The test does not modify bucket settings, IAM, networks, or other databases.

## Running

On an authorized GCE VM with bucket read/write IAM access and the
`https://www.googleapis.com/auth/devstorage.full_control` VM OAuth scope (the
recorded run's narrower `devstorage.read_write` VM scope was rejected by the
GCS gRPC manifest path):

```sh
cargo build --release --locked --manifest-path bench/slatedb-live/Cargo.toml
bash bench/slatedb-live/run.sh chorus-experiments/slatedb-readers-20260904-UNIQUE/
```

`run.sh` runs three repetitions of both suites in independent namespaces and
stops on the first failure.
It preserves JSONL results, stderr, and hashes under `results/`. Each run has
both an internal deadline and an external process timeout. `bootstrap.sh`
installs Rust 1.95.0 on Ubuntu and builds a supplied `source.tar.gz` containing
`rust/` and this directory, including uncommitted reader changes.
Copy `test-ubuntu.sources` beside the bootstrap script; it selects the public
Ubuntu mirror only for those package-installation commands.

`cloud_audit.py` has an intentionally fixed root for the recorded experiment.
Without arguments it only inventories objects/versions and Rapid folders.
After stopping all writers/readers, `--delete` deletes that exact inventory
using generation preconditions, then removes empty folders with metageneration
preconditions and verifies absence. Change the fixed root deliberately for a
new experiment; never broaden it to a bucket or shared parent prefix.
