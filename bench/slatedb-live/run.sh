#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
prefix="${1:?supply the exact fresh experiment root ending in /}"
mkdir -p results
sha256sum src/main.rs src/startup_gc.rs Cargo.lock target/release/chorus-slatedb-live > results/hashes.txt
for run in 1 2 3; do
    RUST_LOG=warn timeout --kill-after=30s 16m target/release/chorus-slatedb-live startup-gc "${prefix}startup-${run}/" \
        > "results/startup-${run}.jsonl" 2> "results/startup-${run}.stderr"
    RUST_LOG=warn timeout --kill-after=30s 16m target/release/chorus-slatedb-live run "${prefix}run-${run}/" \
        > "results/run-${run}.jsonl" 2> "results/run-${run}.stderr"
done
