#!/usr/bin/env python3
"""Run the in-region Chorus and disk WAL benchmark suite."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
import shlex
import subprocess
import sys
from typing import Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
RUST_DIR = REPO_ROOT / "rust"


def _positive_integer(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def parse_zonal_buckets(raw: str | None) -> tuple[str, str, str]:
    if raw is None:
        raise ValueError(
            "ZONAL_BUCKETS or --zonal-buckets is required; "
            "provide three comma-separated v2 bucket resource names"
        )
    buckets = tuple(part.strip() for part in raw.split(",") if part.strip())
    if len(buckets) != 3:
        raise ValueError("exactly three zonal bucket resource names are required")
    first, second, third = buckets
    return first, second, third


def require_regional_bucket(raw: str | None) -> str:
    if raw is None or not raw.strip():
        raise ValueError(
            "REGIONAL_BUCKET or --regional-bucket is required; "
            "provide its v2 bucket resource name"
        )
    return raw.strip()


@dataclass(frozen=True)
class SuiteConfig:
    endpoint: str
    zonal_buckets: tuple[str, str, str]
    regional_endpoint: str
    regional_bucket: str
    duration: int
    payload: int
    outdir: Path
    prefix_base: str
    chorus_loaded_rate: int
    nvme_loaded_rate: int
    hdha_loaded_rate: int
    nvme_dir: Path | None
    hdha_def_dir: Path | None
    hdha_high_dir: Path | None

    @property
    def endpoints(self) -> str:
        return ",".join([self.endpoint] * len(self.zonal_buckets))

    @property
    def bucket_list(self) -> str:
        return ",".join(self.zonal_buckets)


def _optional_path(raw: str | None) -> Path | None:
    if raw is None or not raw.strip():
        return None
    return Path(raw).expanduser().resolve()


def _parser(environ: Mapping[str, str]) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run append-latency and recovery benchmarks against real Chorus "
            "buckets and optional local disk backends. This writes billable "
            "GCS objects."
        )
    )
    parser.add_argument(
        "--endpoint",
        default=environ.get("ENDPOINT", "https://storage.googleapis.com"),
        help="gRPC endpoint for zonal buckets",
    )
    parser.add_argument(
        "--zonal-buckets",
        default=environ.get("ZONAL_BUCKETS"),
        help="three comma-separated v2 Rapid bucket resource names",
    )
    parser.add_argument(
        "--regional-endpoint",
        default=environ.get("REGIONAL_ENDPOINT"),
        help="gRPC endpoint for the manifest bucket; defaults to --endpoint",
    )
    parser.add_argument(
        "--regional-bucket",
        default=environ.get("REGIONAL_BUCKET"),
        help="v2 resource name of the regional manifest bucket",
    )
    parser.add_argument(
        "--duration",
        type=_positive_integer,
        default=_positive_integer(environ.get("DURATION", "30")),
        help="seconds per append-latency run",
    )
    parser.add_argument(
        "--payload",
        type=_positive_integer,
        default=_positive_integer(environ.get("PAYLOAD", "4096")),
        help="payload bytes per record",
    )
    parser.add_argument(
        "--outdir",
        default=environ.get("OUTDIR"),
        help="artifact directory, relative to the repository unless absolute",
    )
    parser.add_argument(
        "--prefix-base",
        default=environ.get("PREFIX_BASE"),
        help="GCS object prefix for this run",
    )
    parser.add_argument(
        "--chorus-loaded-rate",
        type=_positive_integer,
        default=_positive_integer(environ.get("CHORUS_LOADED_RATE", "5000")),
    )
    parser.add_argument(
        "--nvme-loaded-rate",
        type=_positive_integer,
        default=_positive_integer(environ.get("NVME_LOADED_RATE", "1600")),
    )
    parser.add_argument(
        "--hdha-loaded-rate",
        type=_positive_integer,
        default=_positive_integer(environ.get("HDHA_LOADED_RATE", "210")),
    )
    parser.add_argument("--nvme-dir", default=environ.get("NVME_DIR"))
    parser.add_argument("--hdha-def-dir", default=environ.get("HDHA_DEF_DIR"))
    parser.add_argument("--hdha-high-dir", default=environ.get("HDHA_HIGH_DIR"))
    return parser


def parse_config(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] = os.environ,
    now: datetime | None = None,
) -> SuiteConfig:
    parser = _parser(environ)
    args = parser.parse_args(argv)
    try:
        zonal_buckets = parse_zonal_buckets(args.zonal_buckets)
        regional_bucket = require_regional_bucket(args.regional_bucket)
    except ValueError as failure:
        parser.error(str(failure))

    current = now or datetime.now(timezone.utc)
    outdir_raw = args.outdir or f"artifacts/bench/{current.date().isoformat()}-vm"
    outdir = Path(outdir_raw).expanduser()
    if not outdir.is_absolute():
        outdir = REPO_ROOT / outdir
    prefix_base = args.prefix_base or current.strftime("bench/%Y%m%dT%H%M%SZ")
    return SuiteConfig(
        endpoint=args.endpoint,
        zonal_buckets=zonal_buckets,
        regional_endpoint=args.regional_endpoint or args.endpoint,
        regional_bucket=regional_bucket,
        duration=args.duration,
        payload=args.payload,
        outdir=outdir.resolve(),
        prefix_base=prefix_base,
        chorus_loaded_rate=args.chorus_loaded_rate,
        nvme_loaded_rate=args.nvme_loaded_rate,
        hdha_loaded_rate=args.hdha_loaded_rate,
        nvme_dir=_optional_path(args.nvme_dir),
        hdha_def_dir=_optional_path(args.hdha_def_dir),
        hdha_high_dir=_optional_path(args.hdha_high_dir),
    )


def _display(command: Sequence[str | Path]) -> str:
    return shlex.join(str(part) for part in command)


def run(
    command: Sequence[str | Path],
    *,
    cwd: Path,
    stdout: Path | None = None,
) -> None:
    print(f"+ {_display(command)}", flush=True)
    if stdout is None:
        subprocess.run([str(part) for part in command], cwd=cwd, check=True)
        return
    with stdout.open("wb") as destination:
        subprocess.run(
            [str(part) for part in command],
            cwd=cwd,
            stdout=destination,
            check=True,
        )


def chorus_command(
    config: SuiteConfig,
    binary: Path,
    name: str,
    extra: Sequence[str],
) -> list[str | Path]:
    return [
        binary,
        "--endpoints",
        config.endpoints,
        "--buckets",
        config.bucket_list,
        "--manifest-endpoint",
        config.regional_endpoint,
        "--manifest-bucket",
        config.regional_bucket,
        "--prefix",
        f"{config.prefix_base}/chorus-{name}",
        "--duration-seconds",
        str(config.duration),
        "--payload-bytes",
        str(config.payload),
        "--worker-threads",
        "8",
        *extra,
    ]


def disk_command(
    config: SuiteConfig,
    binary: Path,
    data_dir: Path,
    name: str,
    extra: Sequence[str],
) -> list[str | Path]:
    return [
        binary,
        "--data-dir",
        data_dir / f"chorus-bench-{name}",
        "--duration-seconds",
        str(config.duration),
        "--payload-bytes",
        str(config.payload),
        "--worker-threads",
        "8",
        *extra,
    ]


def recovery_command(
    config: SuiteConfig,
    binary: Path,
    name: str,
    extra: Sequence[str],
) -> list[str | Path]:
    return [
        binary,
        "--endpoints",
        config.endpoints,
        "--buckets",
        config.bucket_list,
        "--manifest-endpoint",
        config.regional_endpoint,
        "--manifest-bucket",
        config.regional_bucket,
        "--prefix",
        f"{config.prefix_base}/recovery-{name}",
        "--payload-bytes",
        str(config.payload),
        "--worker-threads",
        "8",
        *extra,
    ]


def _source_revision() -> str:
    return subprocess.check_output(
        ("git", "rev-parse", "HEAD"),
        cwd=REPO_ROOT,
        text=True,
    ).strip()


def _write_run_config(config: SuiteConfig) -> None:
    lines = [
        f"endpoint={config.endpoint}",
        f"zonal_buckets={config.bucket_list}",
        f"regional_endpoint={config.regional_endpoint}",
        f"regional_bucket={config.regional_bucket}",
        f"prefix_base={config.prefix_base}",
        f"duration={config.duration}",
        f"payload={config.payload}",
        f"source_revision={_source_revision()}",
    ]
    (config.outdir / "RUN_CONFIG.txt").write_text("\n".join(lines) + "\n")


def run_suite(config: SuiteConfig) -> None:
    config.outdir.mkdir(parents=True, exist_ok=True)
    _write_run_config(config)

    print("==> building release binaries", flush=True)
    run(
        (
            "cargo",
            "build",
            "--release",
            "-p",
            "gcs-quorum-bench",
            "-p",
            "disk-wal-bench",
            "-p",
            "recovery-bench",
        ),
        cwd=RUST_DIR,
    )
    gqb = RUST_DIR / "target/release/gcs-quorum-bench"
    dwb = RUST_DIR / "target/release/disk-wal-bench"
    rcb = RUST_DIR / "target/release/recovery-bench"

    chorus_runs = (
        ("qd1", ("--outstanding-appends", "1", "--arrival-rate", "0")),
        ("rate10", ("--outstanding-appends", "128", "--arrival-rate", "10")),
        (
            "loaded",
            (
                "--outstanding-appends",
                "128",
                "--arrival-rate",
                str(config.chorus_loaded_rate),
            ),
        ),
        ("tput", ("--outstanding-appends", "256", "--arrival-rate", "0")),
    )
    for name, extra in chorus_runs:
        print(f"==> chorus {name}", flush=True)
        run(
            chorus_command(config, gqb, name, extra),
            cwd=REPO_ROOT,
            stdout=config.outdir / f"chorus-{name}.json",
        )

    disk_backends = (
        ("nvme", config.nvme_dir, config.nvme_loaded_rate),
        ("hdha-def", config.hdha_def_dir, config.hdha_loaded_rate),
        ("hdha-high", config.hdha_high_dir, config.hdha_loaded_rate),
    )
    for backend, data_dir, loaded_rate in disk_backends:
        if data_dir is None:
            print(f"==> skip {backend} (dir unset)", flush=True)
            continue
        data_dir.mkdir(parents=True, exist_ok=True)
        disk_runs = (
            ("qd1", ("--pipeline-window", "1", "--arrival-rate", "0")),
            ("rate10", ("--pipeline-window", "128", "--arrival-rate", "10")),
            (
                "loaded",
                (
                    "--pipeline-window",
                    "128",
                    "--arrival-rate",
                    str(loaded_rate),
                ),
            ),
            ("tput", ("--pipeline-window", "256", "--arrival-rate", "0")),
        )
        for name, extra in disk_runs:
            print(f"==> {backend} {name}", flush=True)
            run(
                disk_command(config, dwb, data_dir, name, extra),
                cwd=REPO_ROOT,
                stdout=config.outdir / f"{backend}-{name}.json",
            )

    for replay_records in (0, 1000, 10000, 100000):
        name = f"replay-{replay_records}"
        print(f"==> recovery {name}", flush=True)
        run(
            recovery_command(
                config,
                rcb,
                name,
                (
                    "--populate-records",
                    "100000",
                    "--target-sealed-segments",
                    "8",
                    "--replay-records",
                    str(replay_records),
                    "--iterations",
                    "5",
                ),
            ),
            cwd=REPO_ROOT,
            stdout=config.outdir / f"recovery-{name}.json",
        )

    for sealed_segments in (0, 1, 4, 16, 64):
        name = f"segs-{sealed_segments}"
        print(f"==> recovery {name}", flush=True)
        run(
            recovery_command(
                config,
                rcb,
                name,
                (
                    "--populate-records",
                    "50000",
                    "--target-sealed-segments",
                    str(sealed_segments),
                    "--replay-records",
                    "1000",
                    "--iterations",
                    "5",
                ),
            ),
            cwd=REPO_ROOT,
            stdout=config.outdir / f"recovery-{name}.json",
        )

    print(f"==> done; artifacts in {config.outdir}", flush=True)
    for artifact in sorted(config.outdir.iterdir()):
        print(artifact.name)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        run_suite(parse_config(argv))
        return 0
    except subprocess.CalledProcessError as failure:
        return failure.returncode or 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
