#!/usr/bin/env python3

from datetime import datetime, timezone
from pathlib import Path
import unittest

import run_suite


class BenchmarkSuiteTests(unittest.TestCase):
    def test_requires_explicit_bucket_configuration(self) -> None:
        with self.assertRaisesRegex(ValueError, "ZONAL_BUCKETS"):
            run_suite.parse_zonal_buckets(None)
        with self.assertRaisesRegex(ValueError, "REGIONAL_BUCKET"):
            run_suite.require_regional_bucket(None)

    def test_requires_exactly_three_zonal_buckets(self) -> None:
        with self.assertRaisesRegex(ValueError, "exactly three"):
            run_suite.parse_zonal_buckets("zone-a,zone-b")

    def test_environment_configuration_has_no_project_default(self) -> None:
        config = run_suite.parse_config(
            [],
            environ={
                "ZONAL_BUCKETS": "projects/_/buckets/a,projects/_/buckets/b,projects/_/buckets/c",
                "REGIONAL_BUCKET": "projects/_/buckets/control",
            },
            now=datetime(2026, 6, 20, tzinfo=timezone.utc),
        )

        self.assertEqual(
            config.zonal_buckets,
            (
                "projects/_/buckets/a",
                "projects/_/buckets/b",
                "projects/_/buckets/c",
            ),
        )
        self.assertEqual(config.regional_bucket, "projects/_/buckets/control")
        self.assertNotIn("subspace", config.bucket_list)

    def test_chorus_command_uses_only_supplied_buckets(self) -> None:
        config = run_suite.SuiteConfig(
            endpoint="https://storage.googleapis.com",
            zonal_buckets=("zone-a", "zone-b", "zone-c"),
            regional_endpoint="https://storage.googleapis.com",
            regional_bucket="regional",
            duration=30,
            payload=4096,
            outdir=Path("/tmp/bench"),
            prefix_base="bench/test",
            chorus_loaded_rate=5000,
            nvme_loaded_rate=1600,
            hdha_loaded_rate=210,
            nvme_dir=None,
            hdha_def_dir=None,
            hdha_high_dir=None,
        )

        command = run_suite.chorus_command(
            config,
            Path("/tmp/gcs-quorum-bench"),
            "qd1",
            ("--outstanding-appends", "1", "--arrival-rate", "0"),
        )
        self.assertEqual(command[command.index("--buckets") + 1], "zone-a,zone-b,zone-c")
        self.assertEqual(command[command.index("--manifest-bucket") + 1], "regional")


if __name__ == "__main__":
    unittest.main()
