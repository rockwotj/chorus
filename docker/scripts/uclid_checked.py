#!/usr/bin/env python3
"""Fail closed when UCLID does not actually execute verification obligations."""

import os
import re
import subprocess
import sys


def has_verification_results(output: str) -> bool:
    counts = [re.findall(rf"(\d+) assertions {kind}\.", output)
              for kind in ("passed", "failed", "indeterminate")]
    return all(len(values) == 1 for values in counts) and sum(
        int(values[0]) for values in counts
    ) > 0


def main() -> int:
    real = os.environ["CHORUS_UCLID_REAL"]
    environment = os.environ.copy()
    if "CHORUS_UCLID_JAVA_HOME" in environment:
        environment["JAVA_HOME"] = environment["CHORUS_UCLID_JAVA_HOME"]
    # P's nested records require UCLID's Z3 Java interface.
    result = subprocess.run(
        [real, *sys.argv[1:]],
        text=True, capture_output=True, env=environment,
    )
    print(result.stdout, end="")
    print(result.stderr, end="", file=sys.stderr)
    if result.returncode or not has_verification_results(result.stdout):
        print("UCLID did not produce a complete verification result", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
