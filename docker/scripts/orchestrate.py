#!/usr/bin/env python3
"""Hermetic Chorus certification and verification orchestration."""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
from typing import Iterable, Sequence


ROOT = Path(__file__).resolve().parents[2]
RUST = ROOT / "rust"
P_ROOT = ROOT / "p"
ARTIFACTS = ROOT / "artifacts"
RECEIPT = ARTIFACTS / "dst-certification.json"
POBSERVE_JAR = P_ROOT / "pobserve" / "target" / "chorus-pobserve.jar"
CERT_BATCH = ARTIFACTS / "cert-batch"
MODEL_DLL = P_ROOT / "PGenerated" / "PChecker" / "net8.0" / "QuorumModel.dll"


class OrchestrationError(RuntimeError):
    """A verification precondition failed before a child command could run."""


def _run(
    command: Sequence[str | Path],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
) -> None:
    subprocess.run([str(part) for part in command], cwd=cwd, env=env, check=True)


def _source_files(root: Path) -> Iterable[tuple[str, Path]]:
    excluded_directories = {
        "target",
        "PGenerated",
        "PVerifierGenerated",
        "PCheckerOutput",
    }
    files: list[tuple[str, Path]] = []
    for tree in ("docker", "p", "rust"):
        tree_root = root / tree
        for directory, directories, names in os.walk(tree_root, followlinks=False):
            directories[:] = [
                name for name in directories if name not in excluded_directories
            ]
            for name in names:
                path = Path(directory) / name
                if not stat.S_ISREG(path.lstat().st_mode):
                    continue
                relative = path.relative_to(root).as_posix()
                if fnmatch.fnmatchcase(relative, "rust/*/README.md"):
                    continue
                if fnmatch.fnmatchcase(relative, "rust/bin/*/README.md"):
                    continue
                files.append((relative, path))
    return sorted(files, key=lambda item: os.fsencode(item[0]))


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_digest(root: Path = ROOT) -> str:
    """Reproduce common.sh's sorted sha256sum-of-sha256sum output exactly."""

    aggregate = hashlib.sha256()
    for relative, path in _source_files(root):
        aggregate.update(f"{_file_sha256(path)}  {relative}\n".encode())
    return aggregate.hexdigest()


def _receipt_matches(
    path: Path,
    digest: str,
    *,
    require_mode: bool,
) -> bool:
    try:
        receipt = json.loads(path.read_text())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False

    requested = receipt.get("requested_wall_seconds")
    elapsed = receipt.get("elapsed_wall_millis")
    return (
        receipt.get("passed") is True
        and isinstance(requested, int)
        and not isinstance(requested, bool)
        and requested >= 3600
        and isinstance(elapsed, int)
        and not isinstance(elapsed, bool)
        and elapsed >= 3_600_000
        and receipt.get("source_digest") == digest
        and (not require_mode or receipt.get("mode") == "production")
    )


def _positive_environment_integer(name: str, default: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as failure:
        raise OrchestrationError(f"{name} must be an integer, got {raw!r}") from failure
    if value <= 0:
        raise OrchestrationError(f"{name} must be greater than zero, got {value}")
    return value


def _environment_path(name: str, default: Path) -> Path:
    value = Path(os.environ.get(name, str(default)))
    return value if value.is_absolute() else (ROOT / value).resolve()


def certify(wall_seconds: int) -> int:
    # The image is the preferred entry point because it pins P, Java, UCLID,
    # Z3, and Rust around the provenance facts recorded by chorus-dst.
    steps = _positive_environment_integer("DST_STEPS_PER_SEED", 2000)
    batch_size = _positive_environment_integer("DST_POBSERVE_BATCH_SIZE", 50)
    batch_dir = _environment_path("DST_POBSERVE_BATCH_DIR", CERT_BATCH)
    pobserve_jar = _environment_path("DST_POBSERVE_JAR", POBSERVE_JAR)
    digest = source_digest()

    # Certification precedes verify in the Docker pipeline. Requiring the jar
    # here prevents a long structural-only run from producing a trusted-looking
    # receipt merely because the later verification phase would build it.
    if not pobserve_jar.is_file():
        raise OrchestrationError(
            f"missing PObserve jar: {pobserve_jar}\n"
            "build it with: cd p && p compile -pp QuorumModel.pproj -md pobserve "
            "&& mvn -q -f pobserve/pom.xml package"
        )

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    print(
        f"Starting deterministic simulation gate for {wall_seconds}s "
        f"(source {digest})",
        flush=True,
    )
    _run(
        (
            "cargo",
            "run",
            "--release",
            "-p",
            "chorus-dst",
            "--",
            "--wall-seconds",
            str(wall_seconds),
            "--steps",
            str(steps),
            "--trace",
            ARTIFACTS / "dst-certified-trace.jsonl",
            "--batch-dir",
            batch_dir,
            "--batch-size",
            str(batch_size),
            "--pobserve-jar",
            pobserve_jar,
            "--receipt",
            RECEIPT,
            "--source-digest",
            digest,
            "--source-root",
            ROOT,
            "--cargo-lock",
            RUST / "Cargo.lock",
        ),
        cwd=RUST,
    )

    if wall_seconds < 3600:
        print(
            "Development run completed, but Phase 2 requires at least 3600 seconds.",
            file=sys.stderr,
        )
        return 2
    return 0


def _pobserve_regressions(jar: Path) -> None:
    formed_record = {
        "seq": 0,
        "time_ms": 0,
        "event": "RecordFormed",
        "writer": 1,
        "epoch": 1,
        "zone": None,
        "logical_offset": 0,
        "value": 7,
        "segment": 0,
        "gen": None,
        "record_end": None,
        "truncation_floor": None,
        "reader": None,
        "reported_size": None,
        "finalized": None,
    }
    encoded = json.dumps(formed_record, separators=(",", ":")) + "\n"

    with tempfile.TemporaryDirectory(prefix="chorus-pobserve.") as temporary_name:
        temporary = Path(temporary_name)
        directory = temporary / "directory"
        directory.mkdir()
        (directory / "seed-2.jsonl").write_text(encoded)
        (directory / "seed-1.jsonl").write_text(encoded)
        accepted = subprocess.run(
            ("java", "-jar", str(jar), str(directory)),
            cwd=P_ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
        if accepted.stdout.strip() != "batch accepted: 2 traces":
            raise OrchestrationError(
                f"unexpected PObserve directory summary: {accepted.stdout.strip()!r}"
            )

        manifest_traces = temporary / "manifest-traces"
        manifest_traces.mkdir()
        (manifest_traces / "one.jsonl").write_text(encoded)
        (manifest_traces / "two.jsonl").write_text(encoded)
        manifest = temporary / "traces.manifest"
        manifest.write_text(
            "manifest-traces/one.jsonl\n\nmanifest-traces/two.jsonl\n"
        )
        accepted = subprocess.run(
            ("java", "-jar", str(jar), str(manifest)),
            cwd=P_ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
        if accepted.stdout.strip() != "batch accepted: 2 traces":
            raise OrchestrationError(
                f"unexpected PObserve manifest summary: {accepted.stdout.strip()!r}"
            )

        rejected_trace = temporary / "seed-9.jsonl"
        shutil.copyfile(
            RUST / "dst" / "tests" / "fixtures" / "pobserve-rejects-open-tail.jsonl",
            rejected_trace,
        )
        rejected = subprocess.run(
            ("java", "-jar", str(jar), str(rejected_trace)),
            cwd=P_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        rejection = rejected.stdout + rejected.stderr
        if rejected.returncode == 0:
            raise OrchestrationError(
                "PObserve accepted the deliberately invalid open-tail fixture"
            )
        for expected in ("seed-9.jsonl", "GetSizeExcludesOpenTail", "line 1"):
            if expected not in rejection:
                raise OrchestrationError(
                    f"PObserve rejection did not contain {expected!r}: {rejection.strip()}"
                )

    print("PObserve batch regressions passed.")


def _verification_environment() -> dict[str, str]:
    environment = os.environ.copy()
    if shutil.which("uclid", path=environment.get("PATH")):
        return environment

    fallback = Path("/tmp/chorus-uclid/uclid-0.9.5/bin/uclid")
    if fallback.is_file() and os.access(fallback, os.X_OK):
        environment["PATH"] = (
            f"{fallback.parent}{os.pathsep}{environment.get('PATH', '')}"
        )
        return environment
    raise OrchestrationError("uclid 0.9.5 is required for PVerifier")


def _parse_model_test_cases(output: str) -> tuple[str, ...]:
    cases: list[str] = []
    in_list = False
    for line in output.splitlines():
        line = line.strip()
        if line.startswith(".. List of test cases"):
            in_list = True
            continue
        if in_list and line == ". Done":
            break
        if in_list and line and line.isidentifier():
            cases.append(line)
    if not cases:
        raise OrchestrationError("P checker reported no model test cases")
    if len(cases) != len(set(cases)):
        raise OrchestrationError("P checker reported duplicate model test cases")
    return tuple(cases)


def _model_test_cases() -> tuple[str, ...]:
    listing = subprocess.run(
        ("p", "check", str(MODEL_DLL), "--list-tests"),
        cwd=P_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    return _parse_model_test_cases(listing.stdout)


def check_model(*, schedules: int, max_steps: int) -> int:
    test_cases = _model_test_cases()
    print(
        f"Model-checking {len(test_cases)} discovered test cases "
        f"({schedules} schedules each)",
        flush=True,
    )
    for test_case in test_cases:
        _run(
            (
                "p",
                "check",
                MODEL_DLL,
                "-tc",
                test_case,
                "-s",
                str(schedules),
                "--max-steps",
                str(max_steps),
            ),
            cwd=P_ROOT,
        )
    return 0


def verify(*, quick: bool) -> int:
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    _run(("cargo", "fmt", "--all", "--", "--check"), cwd=RUST)
    _run(
        (
            "cargo",
            "clippy",
            "--workspace",
            "--all-targets",
            "--all-features",
            "--",
            "-D",
            "warnings",
        ),
        cwd=RUST,
    )
    _run(("cargo", "test", "--workspace", "--all-features"), cwd=RUST)
    smoke_trace = ARTIFACTS / "dst-trace.jsonl"
    _run(
        (
            "cargo",
            "run",
            "--release",
            "-p",
            "chorus-dst",
            "--",
            "--seeds",
            "10",
            "--steps",
            "128",
            "--trace",
            smoke_trace,
        ),
        cwd=RUST,
    )
    _run(
        (
            "cargo",
            "run",
            "--release",
            "-p",
            "chorus-dst",
            "--bin",
            "chorus-trace-checker",
            "--",
            smoke_trace,
            "--event-manifest",
            P_ROOT / "TRACE_EVENTS.txt",
        ),
        cwd=RUST,
    )

    _run(("p", "compile", "-pp", "QuorumModel.pproj"), cwd=P_ROOT)
    _run(
        ("p", "compile", "-pp", "QuorumModel.pproj", "-md", "pobserve"),
        cwd=P_ROOT,
    )
    _run(("mvn", "-q", "-f", "pobserve/pom.xml", "package"), cwd=P_ROOT)
    _run(("java", "-jar", POBSERVE_JAR, smoke_trace), cwd=P_ROOT)
    _pobserve_regressions(POBSERVE_JAR)

    check_model(schedules=1000 if quick else 10000, max_steps=10000)

    if not quick and not _receipt_matches(
        RECEIPT, source_digest(), require_mode=True
    ):
        if not RECEIPT.is_file():
            raise OrchestrationError(f"missing one-hour DST receipt: {RECEIPT}")
        raise OrchestrationError("DST receipt is failed, short, or stale")

    environment = _verification_environment()
    verifier_cache = (
        P_ROOT / "PVerifierGenerated" / "PVerifier" / ".verifier-cache.db"
    )
    if verifier_cache.is_file():
        descriptor, temporary_name = tempfile.mkstemp(
            prefix="chorus-verifier-cache.", suffix=".db"
        )
        os.close(descriptor)
        Path(temporary_name).unlink()
        shutil.move(verifier_cache, temporary_name)
    _run(
        ("p", "compile", "-pp", "QuorumProof.pproj", "-md", "verification"),
        cwd=P_ROOT,
        env=environment,
    )
    print("All Chorus verification gates passed.")
    return 0


def pipeline() -> int:
    digest = source_digest()
    if not _receipt_matches(RECEIPT, digest, require_mode=False):
        status = certify(3600)
        if status != 0:
            return status
    return verify(quick=False)


def _nonnegative_integer(raw: str) -> int:
    value = int(raw)
    if value < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return value


def _positive_integer(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Chorus certification and verification gates"
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    verify_parser = subcommands.add_parser("verify", help="run the verification gate")
    verify_parser.add_argument(
        "--quick",
        action="store_true",
        help="use 1000 model schedules and skip the certification receipt",
    )

    certify_parser = subcommands.add_parser(
        "certify", help="run wall-clock DST certification"
    )
    certify_parser.add_argument(
        "--wall-seconds",
        type=_nonnegative_integer,
        required=True,
        help="minimum wall-clock duration",
    )

    model_parser = subcommands.add_parser(
        "check-model", help="run every test case discovered from the compiled P model"
    )
    model_parser.add_argument(
        "--schedules",
        type=_positive_integer,
        required=True,
        help="schedules to explore per test case",
    )
    model_parser.add_argument(
        "--max-steps",
        type=_positive_integer,
        default=10000,
        help="maximum scheduling steps per execution",
    )

    subcommands.add_parser(
        "pipeline", help="reuse a fresh receipt or certify, then verify"
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "verify":
            return verify(quick=args.quick)
        if args.command == "certify":
            return certify(args.wall_seconds)
        if args.command == "check-model":
            return check_model(schedules=args.schedules, max_steps=args.max_steps)
        return pipeline()
    except OrchestrationError as failure:
        print(failure, file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as failure:
        return failure.returncode or 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
