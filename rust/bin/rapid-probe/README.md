# rapid-probe

`rapid-probe` is a destructive live-service characterization tool for GCS Rapid
zonal buckets. It verifies the append-stream, fencing, visibility, metadata,
and finalization behavior that Chorus depends on but cannot infer from the
protobuf schema alone.

This binary is not part of the hermetic verification gate. It talks to real
GCS, writes billable objects, and should be run against a scratch bucket before
relying on a new bucket class, endpoint, region, or service revision.

## Run

Application Default Credentials are used when no token is supplied:

```sh
gcloud auth application-default login
```

From `rust/`:

```sh
cargo run --release -p rapid-probe -- \
  --endpoint https://storage.googleapis.com \
  --bucket YOUR_RAPID_ZONAL_BUCKET
```

A bare bucket name is normalized to
`projects/_/buckets/YOUR_RAPID_ZONAL_BUCKET`. A full v2 bucket resource name is
also accepted.

For automation, pass `--bearer-token`. The wrapper exposes the common settings
through environment variables. Run it from the repository root:

```sh
BUCKET=YOUR_RAPID_ZONAL_BUCKET ./rust/bin/rapid-probe/run.sh
```

Optional wrapper variables are `ENDPOINT`, `OBJECT_PREFIX`, `BEARER_TOKEN`, and
`KEEP`.

Use a gRPC-capable Storage v2 endpoint for the bucket. The default is
`https://storage.googleapis.com`; a non-gRPC HTML endpoint can surface as
`invalid compression flag: 60`, which indicates the wrong protocol endpoint
rather than a failed storage invariant.

## Probe suite

Each probe uses unique scratch object names; T6 creates two objects:

| Probe | Behavior under test |
| --- | --- |
| T1 | `if_metageneration_match` is enforced on a fresh handle-free append open |
| T2 | metadata CAS advances metageneration and rejects later stale guarded opens |
| T3 | a second fresh open revokes the previous writer's stream |
| T4 | metadata CAS does not revoke an already-open stream |
| T5 | finalization rejects a subsequent append open |
| T6 | newly created segment-like objects are immediately visible to listing |
| T7 | requires `BidiReadObject` to return flushed open-object bytes, reports ordinary read/size visibility, then verifies finalized reads |
| T8 | reports whether 100 per-record flushed appends on one session encounter throttling |
| T9 | measures sequential flush latency, per-message-flush burst pacing, and one-final-flush group durability |
| T10 | reports outcomes for 256 KiB, 1 MiB, 2 MiB, 2 MiB + 1 byte, and 4 MiB write messages |
| T11 | two metadata CAS operations preserve an open session across later writes |
| T12 | the appendable create RPC can remain the lifetime stream through finalize |

T3 is the load-bearing fence for the Chorus single-writer protocol. If the
deposed stream can continue persisting bytes after takeover, the protocol
assumption is invalid.

The probe also exercises two service-specific transport requirements:

- zonal bidi writes may return `ABORTED` with a
  `BidiWriteObjectRedirectedError`; its routing token must be replayed in
  `x-goog-request-params`;
- writer-lane durability still comes from write-response `persisted_size`;
  readonly clients observe open bytes independently through `BidiReadObject`.

Expected rejections and failures include the observed gRPC status code and
message. Successful characterization results report their relevant sizes or
timings. The process exits non-zero if an enforced expectation fails.

## Generation-zero recovery probe

The production client can separately test whether
`AppendObjectSpec.generation = 0` selects the current object:

```sh
cargo run --release -p rapid-probe -- \
  --endpoint https://storage.googleapis.com \
  --bucket YOUR_RAPID_ZONAL_BUCKET \
  --generation-zero-only
```

This mode creates and flushes an appendable object through `GrpcReplica`, then
attempts a handle-free generation-zero takeover through the production
`open_session` path. It also probes a never-created name. Cleanup is mandatory:
the mode fails if deletion fails, and `--keep` does not apply.

## Recorded observations

The repository records these historical live-service results for
`subspace-dev-rapid-zonal-1`:

- On June 11, 2026, T1 through T12 passed. In particular, takeover revoked the
  previous stream, metadata CAS did not revoke it, finalization fenced the
  tested later open, and the conditional-create stream remained usable through
  append and finalization.
- On June 14, 2026, both generation-zero opens were rejected with
  `INVALID_ARGUMENT` stating that `append_object_spec.generation` must be
  specified. Recovery therefore retains an explicit current-generation lookup
  followed by exact-generation takeover.
- On June 23, 2026, T7 returned all 16 flushed bytes from the unfinalized
  appendable object through `BidiReadObject`. That run also exposed the bytes
  through ordinary `ReadObject` and `GetObject.size`; Chorus readonly following
  nevertheless uses the explicit bidirectional-read API.

These observations are evidence for those runs, not a permanent provider
contract. Preserve new output with validation records.

CRC32C population in the finalize response and immediate finalized
`GetObject` remains a planned T13 and is not claimed by the current suite.

## Cleanup and safety

The normal suite names objects
`<object-prefix>-<run-id>-tN` and makes a best-effort conditional delete after
each probe. Pass `--keep` only when the retained objects are needed for
debugging.

Run against a bucket you own and can clean up. A crash, process kill, permission
failure, or provider error can leave probe objects behind even when `--keep`
was not requested.
