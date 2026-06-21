# chorus-fake-gcs

`chorus-fake-gcs` is the shared in-memory implementation of the
`google.storage.v2.Storage` subset used by Chorus. It supports both a loopback
tonic server for integration tests and a direct in-process API for deterministic
simulation.

It remains a separate crate because it is used by `chorus-client` integration
tests and `chorus-dst`. Folding it into `chorus-dst` would force the client
tests to depend on `chorus-dst`, while `chorus-dst` already depends on
`chorus-client`.

## Modeled behavior

The fake models the storage rules that affect the Chorus protocol:

- appendable create, takeover open, handle resume, append flush, and finalize;
- generation and metageneration preconditions;
- fresh handle-free opens fencing the previous append stream;
- metadata compare-and-swap without revoking an existing stream;
- durable `persisted_size` while `GetObject.size` hides an unfinalized tail;
- generations, CRC32C, conditional delete, reads, listing, sorting, and paging;
- regional manifest updates through guarded `UpdateObject`;
- object corruption, byte divergence, partial writes, and crashed zones.

Faults can be targeted at decoded operations such as `BidiTakeoverOpen`,
`BidiAppendFlush`, and `BidiFinalize`, rather than only at the generic bidi RPC.
The API also supports one-shot status failures, delays, response loss,
redirects, session expiry, post-response stream closes, mutation throttling,
and deterministic per-operation latency profiles.

## Loopback server

```rust,no_run
use chorus_fake_gcs::FakeGcs;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let running = FakeGcs::default().start().await?;
println!("endpoint: {}", running.endpoint);

// `running.service` is a cloneable control handle for fault injection and
// direct state observation. Dropping `running` stops the server.
# Ok(())
# }
```

`serve_with_incoming` accepts a caller-provided connection stream when a test
needs to host tonic on a simulated network.

## In-process simulation

`sim_open`, `sim_continue`, `sim_lane_apply`, and `sim_read_bytes` reuse the same
storage mutation and fault logic as the tonic handlers without creating TCP or
background stream tasks. `chorus-dst` uses this path to keep seeded schedules
reproducible while still exercising the client protocol.

`observe_prefix`, operation counters, and the ordered operation log expose
test-only observations. These APIs deliberately bypass normal RPC visibility
rules and must not be used as a production client.

## Scope

This crate is not a general GCS emulator. It does not implement authentication,
all Storage v2 methods, real service placement, quota clocks, or provider
availability behavior. Redirects, expiry, throttling, and latency are
deterministic test controls, not attempts to reproduce every live-service
policy.
