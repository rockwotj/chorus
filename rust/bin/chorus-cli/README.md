# chorus-cli

`chorus-cli` builds the `chorus` diagnostic executable around the public
`chorus-client` API. It supports manual inspection, maintenance, and live
benchmarking of a Chorus WAL. It is not a database integration or a long-running
service.

Every command requires matching lists of 1, 3, or 5 zonal endpoints and bucket
resource names, plus the regional manifest endpoint and bucket. Maintenance
commands must use the prefix and topology that created the WAL; benchmarks
should use a dedicated scratch prefix.

Run the Cargo examples below from `rust/`.

## Authentication

The CLI uses Google Application Default Credentials by default:

```sh
gcloud auth application-default login
```

For automation, pass `--bearer-token` or set `GCS_BEARER_TOKEN`. Use
`--anonymous` only with local test services.

## Commands

`append` recovers from the committed truncation floor, replays the WAL, starts
the engine, commits one record, and shuts down cleanly:

```sh
cargo run -p chorus-cli --bin chorus -- \
  --endpoints https://storage.googleapis.com,https://storage.googleapis.com,https://storage.googleapis.com \
  --buckets projects/_/buckets/zone-a,projects/_/buckets/zone-b,projects/_/buckets/zone-c \
  --manifest-endpoint https://storage.googleapis.com \
  --manifest-bucket projects/_/buckets/regional-control \
  --prefix production/wal \
  append --file payload.bin
```

Use `append --data 'text payload'` for an inline payload.

`repair-sealed` scans manifest-owned sealed segments and repairs missing or
damaged immutable copies. It prints a JSON report:

```sh
cargo run -p chorus-cli --bin chorus -- \
  --endpoints https://storage.googleapis.com,https://storage.googleapis.com,https://storage.googleapis.com \
  --buckets projects/_/buckets/zone-a,projects/_/buckets/zone-b,projects/_/buckets/zone-c \
  --manifest-endpoint https://storage.googleapis.com \
  --manifest-bucket projects/_/buckets/regional-control \
  --prefix production/wal \
  repair-sealed
```

`truncate-before` commits a new truncation floor and deletes older sealed
segments:

```sh
cargo run -p chorus-cli --bin chorus -- \
  --endpoints https://storage.googleapis.com,https://storage.googleapis.com,https://storage.googleapis.com \
  --buckets projects/_/buckets/zone-a,projects/_/buckets/zone-b,projects/_/buckets/zone-c \
  --manifest-endpoint https://storage.googleapis.com \
  --manifest-bucket projects/_/buckets/regional-control \
  --prefix production/wal \
  truncate-before --record-index 100000
```

`truncate-before` is destructive. Confirm the prefix, bucket set, and record
index before running it. Normal WAL engines already perform background
sealed-copy repair; the explicit repair command is mainly for diagnosis and
operator-controlled remediation.

## Benchmarks

The benchmark commands use the same production `chorus-client` transport and
live GCS connection options as the maintenance commands. They write persistent,
billable objects and do not clean them up. Use a unique scratch prefix.

`benchmark append` runs either closed-loop load or an open-loop target rate and
prints the append benchmark JSON schema used by `bench/run_suite.py`:

```sh
cargo run --release -p chorus-cli --bin chorus -- \
  --endpoints https://storage.googleapis.com,https://storage.googleapis.com,https://storage.googleapis.com \
  --buckets projects/_/buckets/zone-a,projects/_/buckets/zone-b,projects/_/buckets/zone-c \
  --manifest-endpoint https://storage.googleapis.com \
  --manifest-bucket projects/_/buckets/regional-control \
  --prefix benchmarks/append-001 \
  benchmark append \
  --duration-seconds 300 \
  --outstanding-appends 256 \
  --payload-bytes 4096 \
  --pipeline-window 32 \
  --worker-threads 8
```

Set a positive `--arrival-rate` for open-loop records per second. The report
includes throughput, latency percentiles, batching, write amplification,
pipeline metrics, and drain accounting.

`benchmark recovery` populates a separate WAL below `<prefix>/iNNN` for each
iteration, cleanly shuts down its writer, then measures epoch claim, prepare,
replay, start, and total recovery time:

```sh
cargo run --release -p chorus-cli --bin chorus -- \
  --endpoints https://storage.googleapis.com,https://storage.googleapis.com,https://storage.googleapis.com \
  --buckets projects/_/buckets/zone-a,projects/_/buckets/zone-b,projects/_/buckets/zone-c \
  --manifest-endpoint https://storage.googleapis.com \
  --manifest-bucket projects/_/buckets/regional-control \
  --prefix benchmarks/recovery-001 \
  benchmark recovery \
  --populate-records 50000 \
  --target-sealed-segments 8 \
  --replay-records 10000 \
  --iterations 5 \
  --payload-bytes 4096
```

The base prefix must be unused because each iteration starts record numbering at
zero. This measures a new Chorus recovery pass in the same process with reused
gRPC factories; it does not restart the process or clear operating-system,
runtime, DNS, TLS, or provider caches.

Set `--target-sealed-segments 0` to keep one large active segment.
`--replay-records 0` measures no replay; a value at or above
`--populate-records` replays the full log.
