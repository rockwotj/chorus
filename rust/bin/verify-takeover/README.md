# verify-takeover

Empirically verifies, against a **live GCS Rapid zonal bucket**, the append-stream
fencing semantics used by Chorus. The proto exposes the precondition primitive;
takeover and revocation are server behavior that must be checked against the
real service.

## What it checks

| Probe | Claim | Expected |
| --- | --- | --- |
| **T1** | `if_metageneration_match` is enforced on a fresh open (no `write_handle`) | wrong → `FAILED_PRECONDITION`, correct → ok |
| **T2** | a metadata CAS bumps metageneration and fences later guarded opens | stale → `FAILED_PRECONDITION`, current → ok |
| **T3** | **takeover**: a second fresh open revokes a held-open prior stream | S2 opens; S1's continued append **fails** |
| **T4** | a metadata CAS does **not** fence an already-open stream | after bump, S1's continued append **succeeds** |
| **T5** | finalization is a hard append fence | later open → `FAILED_PRECONDITION` |
| **T6** | object listing is strongly consistent | two fresh segment-like objects are immediately visible |
| **T7** | open appendable bytes and size remain hidden from reads | visible only after finalization |
| **T8** | flushes on one held stream are not per-object mutation opens | 100 flushed appends succeed |
| **T9** | one trailing flush durably covers a pipelined group | final persisted size covers the group |
| **T10** | accepted write-message sizes | reports the live service ceiling |
| **T11** | metadata CAS preserves an already-open stream | later continuation succeeds |
| **T12** | the appendable create RPC remains the lifetime stream | continuation append and finalization succeed |

T3 is load-bearing: if S1 keeps writing after S2 takes over, the zonal data-plane
fence is invalid and the protocol must change. T4 and T11 are supplementary
provider characterization; Chorus stamps segment metadata after finalization.

**Planned T13 (not yet implemented or claimed by the recorded run):** verify
that the finalize response and an immediate `GetObject`/stat of a finalized
Rapid APPENDABLE object populate `Object.checksums.crc32c`. The field is
documented for objects and the deterministic fake exercises it, but this object
class still needs a live-bucket probe.

### Result (2026-06, bucket `subspace-dev-rapid-zonal-1`)

The recorded live run matched the fencing and finalization expectations:

* **T1/T2** — `if_metageneration_match` is enforced on a fresh (handle-free)
  open; a metadata CAS bumps metageneration and fences subsequent guarded opens
  (stale precondition → `FAILED_PRECONDITION`).
* **T3** — after the second writer opens and appends, the original stream's next
  append (at its own believed tail) is rejected with
  `FAILED_PRECONDITION: "A different writer has become the exclusive writer of this object."`
  GCS append-stream takeover is a real, server-enforced fence.
* **T4** — a metadata CAS under an open stream does **not** revoke it; the prior
  stream still persisted. Hence the fence must be the takeover open, not the CAS.
* **T5** — append after finalization is rejected with a distinct
  `FAILED_PRECONDITION: "The object has already been finalized."` diagnostic.
T6 is included so each future live run also verifies the listing assumption
used by segment discovery.

A June 11, 2026 run against the same bucket passed all twelve probes. **T12**
confirmed that one conditional-create `BidiWriteObject` RPC accepted an
append continuation (no first message) and a finalization continuation, and
the finalize response carried the authoritative object resource. The live
service supports the lifetime-create-stream optimization. Re-run the suite
before deploying against a new bucket class or service revision.

The production client (`rust/client/src/grpc.rs`) and this probe both implement
two service requirements:

* Zonal writes are redirected: the first `BidiWriteObject` returns
  `ABORTED` with a `BidiWriteObjectRedirectedError`; the `routing_token` must be
  replayed in `x-goog-request-params`.
* `GetObject.size` does **not** reflect flushed-but-unfinalized appends; the
  authoritative tail offset is the `persisted_size` from the write responses.

## Generation-zero recovery gate

Recovery can be checked independently with the production Chorus transport:

```sh
cargo run -p verify-takeover -- \
  --endpoint https://storage.googleapis.com \
  --bucket YOUR_RAPID_ZONAL_BUCKET \
  --generation-zero-only
```

This focused mode creates an appendable object through `GrpcReplica`, flushes a
known payload on its retained session, and performs a handle-free
`AppendObjectSpec.generation = 0` open through the same redirect-aware
`open_session` path used by recovery. It requires the opening object resource
and takeover tail to equal the flushed byte count. It then repeats the open for
a never-created name and requires `NOT_FOUND`. The created object is always
deleted; `--keep` does not apply to this gate.

### Result (2026-06-14, bucket `subspace-dev-rapid-zonal-1`)

Real GCS rejected both generation-zero opens before selecting an object:

```text
STEP 1(a) append: N=37 bytes; flush persisted_size=37
STEP 1(a) generation=0 takeover: error_code=INVALID_ARGUMENT (TransportCode::InvalidArgument) message="The 'append_object_spec.generation' must be specified."; object_resource_size=None
STEP 1(b) never-created generation=0 takeover: error_code=INVALID_ARGUMENT (TransportCode::InvalidArgument) message="The 'append_object_spec.generation' must be specified."
STEP 1(c) cleanup: deleted object reclaim-recovery-rpc-final-1781481774220964000-generation-zero-present
Error: GENERATION-ZERO TAKEOVER PROBE FAILED
```

Therefore generation zero is not a usable current-object selector for the live
append API on this bucket/service revision. Recovery must retain its explicit
current-generation lookup and exact-generation takeover unless a different
single-RPC selector is verified against the real service.

Each probe runs on its own fresh object (`<prefix>-<run-id>-tN`) and deletes it
on completion (`--keep` to retain). The process exits non-zero if any probe
does not match its expectation, and always prints the raw gRPC status codes.

## Run

```sh
# Auth: Application Default Credentials by default (gcloud auth application-default login),
# or pass --bearer-token "$(gcloud auth print-access-token)".

cargo run -p verify-takeover -- \
  --endpoint https://storage.googleapis.com \
  --bucket   YOUR_RAPID_ZONAL_BUCKET
```

**Endpoint:** use the gRPC endpoint `https://storage.googleapis.com`. Pointing
at a non-gRPC host returns an HTML `400 Bad Request` that surfaces as
`invalid compression flag: 60` — that means wrong endpoint, not a probe failure.

**Bucket:** a bare name is normalized to `projects/_/buckets/<name>` (the
resource-path form the v2 API requires). Passing a bare name to the raw API
otherwise yields `Bucket '' not found`.

Or use the wrapper:

```sh
BUCKET=YOUR_RAPID_ZONAL_BUCKET ./rust/bin/verify-takeover/run.sh
```

## Caveats

* **Writes and deletes** real objects in the target bucket — point it at a
  scratch bucket you own; it incurs billing.
* It is a **manual cloud probe**, not part of the hermetic verification gate
  (same status as the live benchmark). Preserve its output with validation
  records.
* The twelve characterization probes send raw `google.storage.v2` requests and
  reuse `chorus-client`'s rich-error redirect extractor. The focused
  generation-zero gate instead uses the production `GrpcReplica` append-open
  implementation end to end.
