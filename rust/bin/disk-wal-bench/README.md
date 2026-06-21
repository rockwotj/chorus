# disk-wal-bench

`disk-wal-bench` is the local durable-write baseline for Chorus benchmark runs.
It drives `walrus-rust` with `ReadConsistency::StrictlyAtOnce` and
`FsyncSchedule::SyncEach`, so every completed append represents one synchronous
durability operation. `walrus-rust` implements this with synchronous file I/O
such as `O_SYNC` on Linux; the benchmark does not count `fsync(2)` syscalls.

Use a dedicated, preferably empty directory on the filesystem being measured.
The benchmark creates the directory if needed and leaves the WAL files in
place.

## Run

From `rust/`:

```sh
cargo run --release -p disk-wal-bench -- \
  --data-dir /mnt/benchmark/chorus-disk-wal \
  --duration-seconds 300 \
  --pipeline-window 32 \
  --payload-bytes 4096 \
  --worker-threads 4
```

`--pipeline-window` is the maximum number of concurrent durable appends;
`--outstanding-appends` is an alias.

With `--arrival-rate 0`, load is closed-loop. A positive `--arrival-rate`
schedules an open-loop records-per-second target and measures latency from the
intended arrival time. Open-loop runs wait up to 60 seconds to drain after the
measurement interval.

## Output

The JSON report includes append rate, the equivalent logical `SyncEach` rate,
payload throughput, p50/p99/p99.9/max latency, target and achieved arrival
rate, cap waits, drain state, and the full workload configuration.

This is a workload-shape baseline, not a protocol-equivalent replacement for
Chorus. It measures one local synchronous durability operation per record and
has no quorum replication, GCS service behavior, regional manifest, recovery,
or network path.
