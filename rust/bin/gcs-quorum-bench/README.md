# gcs-quorum-bench

`gcs-quorum-bench` is the production load generator for the Chorus WAL. It drives
one ordered admission loop with many outstanding durability completions against
1, 3, or 5 live GCS Rapid zonal buckets plus one regional manifest bucket
through the same `chorus-client` implementation used by the database API and
production DST. The example below uses the typical three-zone layout.

```sh
gcloud auth application-default login

cargo run --release -p gcs-quorum-bench -- \
  --endpoints https://zone-a-storage.googleapis.com,https://zone-b-storage.googleapis.com,https://zone-c-storage.googleapis.com \
  --buckets projects/_/buckets/a,projects/_/buckets/b,projects/_/buckets/c \
  --manifest-endpoint https://storage.googleapis.com \
  --manifest-bucket projects/_/buckets/regional-control \
  --prefix benchmarks/run-001 \
  --duration-seconds 300 \
  --outstanding-appends 256 \
  --payload-bytes 4096 \
  --max-record-bytes 1048576 \
  --pipeline-window 32 \
  --worker-threads 8
```

The default is closed-loop load: each completion admits one replacement append,
up to `--outstanding-appends`. Set a positive `--arrival-rate` to schedule an
open-loop records-per-second target; latency then starts at the intended arrival
time so admission backpressure and commit stalls remain visible.

The JSON report includes append and record IOPS, payload throughput,
p50/p99/p99.9/max latency, WAL and attempted replica bytes, write amplification,
pipeline occupancy/refill metrics, workload mode and target/achieved arrival
rates, `records_per_persist`, and open-loop drain/cap accounting.

Application Default Credentials are used unless `--bearer-token` or
`GCS_BEARER_TOKEN` supplies a static token. `--anonymous` is intended only for
local test services.
