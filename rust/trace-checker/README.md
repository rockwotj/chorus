# chorus-trace-checker

`chorus-trace-checker` validates JSONL traces emitted by `chorus-dst`, requires the
Rust transition list to exactly match `p/TRACE_EVENTS.txt`, rejects unknown
events, and requires contiguous sequence numbers, including manifest events.

```sh
cargo run -p chorus-trace-checker -- \
  ../artifacts/dst-trace.jsonl \
  --event-manifest ../p/TRACE_EVENTS.txt
```

Protocol semantics intentionally do not live in this crate. The repository gate
translates the validated JSONL into generated P event classes and runs the
PObserve monitors generated from `p/model/Monitors.p`. This prevents a
handwritten Rust oracle from drifting away from the specification.
