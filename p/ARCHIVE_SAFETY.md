# WAL archival safety

## Storage and publication

The live `ManifestStore` remains a linearizable, bounded CAS register. It holds
the writer epoch, current/pending segment names, newest seal, checkpoint floor,
hot directory, and one archive root. `ArchiveStore` holds both canonical sealed
WAL bytes and immutable catalog pages. Its contract requires complete atomic
publication, durability, immediate readability, and idempotence for identical
content. A lost response can leave an object durable without publishing it.

The catalog is a sequence-ordered copy-on-write B+ tree with fanout 32, pages
bounded at 64 KiB, and bounded-height traversal. References include range,
height, exact byte length, and SHA-256. Append copies only the rightmost path;
seek reads one path. Old roots remain valid while readers hold them. There is
no whole-history read/rewrite, bucket listing, or mutable linked-list traversal
on the archive lookup path. Superseded and orphaned pages are currently retained.

One archival step does the following:

1. Read the oldest hot descriptor and its exclusive end from the next entry.
   It must precede the current seal, whose finalization witness stays hot.
2. Read and verify canonical sealed bytes and conditionally upload them.
3. Prepare and durably upload the new immutable catalog path.
4. In one manifest CAS, publish that root and remove that exact descriptor.
   Retry against the latest manifest, preserving concurrent checkpoint/epoch
   updates and rechecking the expected parent, segment ID, bounds, and checksum.
5. Delete redundant zonal copies with generation guards. Advance the bounded
   cleanup cursor only after every zone confirms absence. An unavailable zone
   holds this cursor, not a hot directory slot.

Steps 2–3 alone never authorize deletion. Crashes before step 4 can leak archive
objects but cannot lose hot history. Crashes after step 4 leave the published
catalog as the durable cleanup work list. Stale readers that fail to read a hot
segment refresh the manifest and resolve its exact archived identity. Missing
or corrupt referenced data returns an error, never a skipped range.

Startup recovery captures the archive root together with its adopted hot
directory. Later recovery CAS retries may refresh the live manifest but must
not replace that captured root: the newer root can contain segments still in
the adopted hot directory, causing duplicate replay. The retained root and hot
descriptors remain a disjoint replay catalog; descriptors whose zonal copies
are subsequently deleted use the exact-identity archive fallback.

`keep_sealed_segments` is a target count, with a minimum of one. Checkpointed
predecessors may also move to the archive. A long archive outage can still fill
the bounded directory and backpressure new writes; it never authorizes data
loss. Archive availability and eventual successful CAS are liveness assumptions,
not safety assumptions. Slow or stale repairers may leave redundant zonal
copies for a later dead-segment sweep; those copies do not become authoritative.
Once archival or truncation frees hot-directory capacity, refresh failures are
retried with backoff so a lost notification cannot leave rotation permanently
blocked. When archive passes overrun their period, maintenance alternates an
overdue pass with a ready seal/truncate command so neither source of work can
starve the other.

## Compatibility and recovery

Archive activation is writer-claimed and persists format 2, a stable backing
namespace, and the then-current truncation floor. Old clients reject format 2;
opens without that namespace fail. Format 1 volumes retain existing behavior.
Records below the activation floor are not promised. Truncation after activation
does not delete promised history. Writer startup still requires a current
database checkpoint; historical PITR replay uses a readonly stream and a
separate application restore. WAL retention does not itself provide database
snapshots or a timestamp-to-sequence mapping.

Readers lazily seek catalog pages but verify one complete segment before
exposing its records. Dropping a follower cancels its owned poller/read work;
no registration or explicit upper-bound API is required. Startup recovery must
still be fully consumed before starting a writer.

## Machine-checked argument

`model/Archive.p` composes with the actual P `ManifestRegister`. It bounds the
history to eight one-record seals and explores two independently cached worker
proposals, immutable pages, lost requests/responses, publication CAS races,
worker crashes, per-zone deletion, checkpoint/epoch changes, stale readers, and
early stream drops. Assertions check canonical contiguous history, the hot/archive
join, preservation of the newest seal, durable recovery sources, and reader prefixes.
The existing quorum model supplies the canonical-byte/finalization premise.

`model/ArchiveRecovery.p` separately exercises a recovery snapshot, archival
publication and deletion, and a conflicting recovery CAS followed by refresh.
It traverses the captured archive pages and hot descriptors independently and
checks every emitted sequence number and the complete replay range, including
archive fallback for deleted hot copies. It covers empty/existing roots and
nonzero checkpoints. Substituting the refreshed root reproduces duplicate
replay and fails the ordering assertion.

`proof/ArchiveProof.p` proves an inductive invariant over unbounded integer
prefix boundaries. Writing `D`, `A`, `I`, `U`, `F`, and `S` for deletion,
publication, indexing, upload, finalization, and sealing boundaries:

```
0 <= D <= A <= I <= U <= F < S
hot_start = A
```

These inequalities imply that any evicted sealed record has durable indexed
archive bytes, the current seal remains hot, and checkpoint advancement alone
cannot authorize eviction. Auxiliary invariants establish CAS proposal bounds,
monotone versions, a bounded reader cursor, and no delivery after drop. Recovery
invariants additionally keep the adopted archive/hot boundary fixed while the
live manifest advances, then establish ordered concatenation and completion
of its captured replay range. The
proof is of this reduced transfer protocol under the store/quorum premises,
not a formal refinement proof of Rust, gRPC, GCS, cryptography, or the B+ tree.
Rust regression tests cover the concrete tree and storage implementations.
The existing DST/PObserve gates exercise the quorum protocol; archive-specific
behavior is covered by the P archive driver and Rust archival tests, not by
archive events in the production DST trace.

## Reproducing checks

From the repository root, with P, Java, a compatible UCLID build and its native
Z3 bindings installed:

```sh
python3 docker/scripts/check_archive_proof.py
cd p
p compile -pp QuorumModel.pproj
python3 ../docker/scripts/orchestrate.py check-model --schedules 1000 --max-steps 10000
cd ../rust
cargo test -p chorus-client archive_ --lib
```

The archive proof checker uses fresh temporary outputs and checks both the
correct protocol and mutations permitting premature deletion/publication or
replacing an adopted recovery root after refresh. The correct model must prove
all thirteen named invariants plus P's default obligations. The first two
mutations must fail the prefix-order invariant; the last must fail the
recovery snapshot-join invariant.

Do not rely on the historical UCLID `v0.9.5` release zip: it lacks the `-M`
option and algebraic data types needed by P 3.1.0. P can misinterpret its
zero-exit usage message as successful verification. The checked wrapper
requires nonempty assertion results and the verification workflow discards
old cached results. It also checks P's reported invariant results because P
can exit zero after reporting failed invariants. The source build used for these
checks is UCLID commit
`a4e4e7a22780833c86d6a650af2fcf88a7d00a06` with Z3 4.12.2 Java/native bindings.
See the [PVerifier installation instructions](https://p-org.github.io/P/advanced/PVerifierLanguageExtensions/install-pverifier/)
for the source-build dependency requirements.

Validation for this change passed the full Rust workspace tests and strict
Clippy checks; all 21 P model cases at 1,000 schedules each; the quick DST and
PObserve verification gates; all 41 existing quorum invariants and 14 archive
invariants/obligations; and all three deliberately unsafe archive proof mutations.
The ten archival tests include a multi-level catalog, response-loss retries,
publication races, unavailable archive/zonal storage, stale readers, recovery,
automatic hot-directory pressure relief, and the regional GCS adapters against
the fake gRPC server. Both recovery-race regressions failed with duplicate
sequence numbers before the fix and passed afterward; the bounded P model's
refreshed-root mutation also fails. Separate liveness regressions inject the
first capacity refresh failure after truncation and keep the archive timer
overdue while a command is queued. Live cloud GCS behavior was not exercised.
