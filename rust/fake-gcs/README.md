# chorus-fake-gcs

`chorus-fake-gcs` is an in-memory tonic implementation of only the
`google.storage.v2.Storage` behavior required by Chorus. It is a protocol test
service, not a general GCS emulator or production backend.

The fake models the live-verified provider behaviors that Chorus relies on or
characterizes:

- fresh handle-free append opens revoke the prior stream;
- metadata compare-and-swap does not revoke an open stream;
- a deposed stream and an already-finalized object produce distinct
  `FAILED_PRECONDITION` diagnostics;
- `GetObject.size` is zero for flushed but unfinalized append bytes, while write
  responses expose the durable `persisted_size`;
- guarded metadata updates model the regional manifest CAS register and other
  metageneration preconditions;
- listing is strongly consistent, lexically sorted, and paginated;
- offsets, CRC32C, generations, metagenerations, conditional deletion, reads,
  injected failures/delays, crashes, partial writes, and corruption are modeled.

```rust,no_run
use chorus_fake_gcs::FakeGcs;

# async fn start() -> Result<(), Box<dyn std::error::Error>> {
let server = FakeGcs::default().start().await?;
println!("fake GCS endpoint: {}", server.endpoint);
# Ok(())
# }
```

Production DST connects the unmodified `chorus-client` gRPC transport to three
zonal fake servers and one regional manifest fake over turmoil's simulated
network.
