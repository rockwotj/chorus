#!/usr/bin/env bash
# Live verification of GCS Rapid append-stream takeover fencing (whitepaper, "Object operations").
#
# WARNING: writes and deletes real objects in $BUCKET and incurs billing.
# Point it at a scratch Rapid zonal bucket you own.
#
# Usage:
#   BUCKET=my-rapid-bucket ./run.sh
#   BUCKET=my-rapid-bucket ENDPOINT=https://us-central1-storage.googleapis.com ./run.sh
#   BUCKET=my-rapid-bucket BEARER_TOKEN="$(gcloud auth print-access-token)" ./run.sh
set -euo pipefail

if [[ -z "${BUCKET:-}" ]]; then
  echo "error: set BUCKET=<your-rapid-zonal-bucket>" >&2
  exit 2
fi

ENDPOINT="${ENDPOINT:-https://storage.googleapis.com}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

args=(--endpoint "$ENDPOINT" --bucket "$BUCKET")
[[ -n "${OBJECT_PREFIX:-}" ]] && args+=(--object-prefix "$OBJECT_PREFIX")
[[ -n "${BEARER_TOKEN:-}" ]] && args+=(--bearer-token "$BEARER_TOKEN")
[[ -n "${KEEP:-}" ]] && args+=(--keep)

cd "$SCRIPT_DIR/../.."
exec cargo run --release -p verify-takeover -- "${args[@]}"
