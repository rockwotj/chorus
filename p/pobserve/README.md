# Chorus PObserve adapter

This module converts `chorus-dst` JSONL observations into the Java event classes
generated directly from `model/Monitors.p`, then runs those generated monitors.
It deliberately contains no independent protocol rules.

```sh
cd p
p compile -pp QuorumModel.pproj -md pobserve
mvn -q -f pobserve/pom.xml package
java -jar pobserve/target/chorus-pobserve.jar ../artifacts/dst-trace.jsonl
```

The argument may also be a directory or a file whose name ends in
`.manifest`. Directory mode checks regular `*.jsonl` files in sorted path
order. A manifest contains one trace path per non-blank line; relative paths
are resolved from the manifest's directory. Both are batch modes and print one
`batch accepted: N traces` summary on success.

Every trace gets fresh generated monitor instances. Seed executions have no
shared protocol history, so reusing monitor state would make acceptance depend
on batch size and would create false cross-seed violations. A rejection prints
the trace path, generated monitor name, and offending line before exiting
non-zero.

The Rust trace checker runs first to validate the JSON envelope, event manifest,
and contiguous sequence numbers. PObserve is the semantic conformance gate.

Production traces run the generated `DirectoryStructure` monitor over each
recovery-adopted manifest snapshot and `DirectoryEnforcement` over historical
`SealQuorumEnforced` evidence. `RotationGateSafety` remains model-only: the
production oracle can witness finalized storage and later manifest states, but
not the exact instant the engine consumed the gate relative to another CAS.
