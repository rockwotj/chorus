# DST trace fixtures

Each fixture is newline-delimited JSON with the same `TraceEvent` envelope the
production DST writes. Structural fixtures must use contiguous `seq` values and
event names from `p/TRACE_EVENTS.txt`; semantic fixtures may still describe a
protocol execution that a generated PObserve monitor must reject.

`pobserve-rejects-open-tail.jsonl` is deliberately bad for
`GetSizeExcludesOpenTail`: line 1 reports one visible byte while
`finalized=false`. GCS must report size zero until the object is finalized, so
the trace is structurally valid but monitor-invalid.
