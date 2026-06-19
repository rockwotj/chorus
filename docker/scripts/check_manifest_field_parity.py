#!/usr/bin/env python3
"""Fail when Rust and P manifest record fields drift without classification."""

from __future__ import annotations

from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[2]
RUST_MANIFEST = ROOT / "rust" / "client" / "src" / "manifest.rs"
P_TYPES = ROOT / "p" / "model" / "Types.p"

# Each group documents how production data corresponds to the bounded model.
# Rust-only topology is fixed by the model configuration rather than stored in
# tManifestRecord. P's tail generation and seal sum are finite surrogates for
# opaque object identity and the production SHA-256 digest.
FIELD_GROUPS = (
    ("epoch", {"epoch"}, {"epoch"}),
    ("owner", {"owner"}, {"owner"}),
    ("active tail identity", {"tail_id"}, {"tailGen"}),
    ("chain boundary", {"tail_base"}, {"tailBase", "sealEnd"}),
    ("seal identity", {"seal_base", "seal_id"}, {"sealBase", "sealId"}),
    ("seal digest", {"seal_digest"}, {"sealSum"}),
    ("truncation floor", {"trunc"}, {"trunc"}),
    ("sealed directory", {"segments"}, {"directory"}),
    ("pending segment", {"pending_id"}, {"pending"}),
    ("deployment topology", {"buckets"}, set()),
)


def extract_rust_fields(source: str) -> set[str]:
    match = re.search(
        r"pub\(crate\)\s+struct\s+ManifestRecord\s*\{(?P<body>.*?)^\}",
        source,
        re.MULTILINE | re.DOTALL,
    )
    if match is None:
        raise ValueError("could not find Rust ManifestRecord")
    return set(re.findall(r"^\s*pub\s+([a-z][a-z0-9_]*)\s*:", match["body"], re.MULTILINE))


def extract_p_fields(source: str) -> set[str]:
    match = re.search(
        r"type\s+tManifestRecord\s*=\s*\((?P<body>.*?)^\);",
        source,
        re.MULTILINE | re.DOTALL,
    )
    if match is None:
        raise ValueError("could not find P tManifestRecord")
    return set(re.findall(r"^\s*([A-Za-z][A-Za-z0-9]*)\s*:", match["body"], re.MULTILINE))


def declared_fields(side: int) -> set[str]:
    result: set[str] = set()
    for name, rust_fields, p_fields in FIELD_GROUPS:
        fields = rust_fields if side == 0 else p_fields
        overlap = result & fields
        if overlap:
            raise ValueError(f"{name} repeats declared fields: {sorted(overlap)}")
        result.update(fields)
    return result


def mismatch(label: str, actual: set[str], declared: set[str]) -> list[str]:
    failures = []
    missing = declared - actual
    unexpected = actual - declared
    if missing:
        failures.append(f"{label} is missing declared fields: {sorted(missing)}")
    if unexpected:
        failures.append(
            f"{label} has unclassified fields: {sorted(unexpected)}; "
            "update the other manifest and FIELD_GROUPS together"
        )
    return failures


def main() -> int:
    try:
        rust_fields = extract_rust_fields(RUST_MANIFEST.read_text())
        p_fields = extract_p_fields(P_TYPES.read_text())
        declared_rust = declared_fields(0)
        declared_p = declared_fields(1)
    except (OSError, UnicodeError, ValueError) as error:
        print(f"manifest field parity check failed: {error}", file=sys.stderr)
        return 1

    failures = mismatch("Rust ManifestRecord", rust_fields, declared_rust)
    failures.extend(mismatch("P tManifestRecord", p_fields, declared_p))
    if failures:
        for failure in failures:
            print(f"manifest field parity check failed: {failure}", file=sys.stderr)
        return 1

    print(
        "manifest field parity ok: "
        f"{len(rust_fields)} Rust fields, {len(p_fields)} P fields, "
        f"{len(FIELD_GROUPS)} correspondence groups"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
