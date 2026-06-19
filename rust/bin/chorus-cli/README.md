# chorus-cli

`chorus-cli` provides the `chorus` diagnostic executable without adding command-line
dependencies to the `chorus-client` library. It exercises the same public startup
recovery, append, truncation, and shutdown APIs used by a database integration.
Its diagnostic `repair-sealed` subcommand invokes the public volume-level repair
primitive directly and prints a detailed repair report; normal WAL engines
repair sealed copies automatically in the background.

Every invocation requires matching lists of 1, 3, or 5 zonal endpoints and
buckets plus a regional `--manifest-endpoint` and `--manifest-bucket`. The CLI
recovers from the manifest's committed truncation floor. Segment rotation
remains automatic.

```sh
cargo run -p chorus-cli --bin chorus -- \
  --endpoints https://storage.googleapis.com,https://storage.googleapis.com,https://storage.googleapis.com \
  --buckets projects/_/buckets/a,projects/_/buckets/b,projects/_/buckets/c \
  --manifest-endpoint https://storage.googleapis.com \
  --manifest-bucket projects/_/buckets/regional-control \
  --prefix production/wal append --file payload.bin
```
