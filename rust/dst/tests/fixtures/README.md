# DST trace fixtures

Each fixture is newline-delimited JSON with the same `TraceEvent` envelope
emitted by `chorus-dst`. Structurally valid fixtures use contiguous `seq`
values and event names from `p/TRACE_EVENTS.txt`; they may still describe a
protocol execution that a generated PObserve monitor must reject.

`pobserve-rejects-open-tail.jsonl` is deliberately bad for
`GetSizeExcludesOpenTail`: line 1 reports one visible byte while
`finalized=false`. GCS must report size zero until the object is finalized, so
the trace is structurally valid but monitor-invalid.
