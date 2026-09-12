#!/usr/bin/env python3
"""Check the archive proof and require unsafe mutations to be rejected."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile

import orchestrate


def main() -> None:
    environment = (os.environ.copy() if "CHORUS_UCLID_REAL" in os.environ
                   else orchestrate._verification_environment())
    source = (orchestrate.P_ROOT / "proof/ArchiveProof.p").read_text()
    cases = [
        ("safe", source, True, ""),
        ("delete_before_publish", source.replace(
            "if (deletedEnd < archiveEnd)", "if (deletedEnd < sealedEnd)"), False, "archive_prefix_order"),
        ("publish_before_index", source.replace(
            "proposalParent == archiveEnd && proposalEnd <= indexedEnd",
            "proposalParent == archiveEnd"), False, "archive_prefix_order"),
        ("refresh_recovery_root", source.replace(
            "recoveryPhase = 2;", "recoveryArchiveEnd = archiveEnd; recoveryPhase = 2;"),
         False, "archive_recovery_snapshot_join"),
    ]
    if "--mutations-only" in sys.argv:
        cases = cases[1:]
    for name, model, expected, failed_invariant in cases:
        if not expected and model == source:
            raise RuntimeError(f"{name}: mutation no longer matches source")
        with tempfile.TemporaryDirectory(prefix=f"chorus-proof-{name}.") as temporary:
            directory = Path(temporary)
            (directory / "ArchiveProof.p").write_text(model)
            (directory / "Check.pproj").write_text(
                '<Project><ProjectName>ArchiveCheck</ProjectName><InputFiles>'
                '<PFile>./ArchiveProof.p</PFile></InputFiles>'
                '<OutputDir>./generated/</OutputDir><Target>PVerifier</Target></Project>')
            result = subprocess.run(
                ["p", "compile", "-pp", "Check.pproj", "-md", "verification"],
                cwd=directory, env=environment, text=True, capture_output=True,
            )
            output = result.stdout + result.stderr
            valid = (result.returncode == 0 and "Verified 14 invariants!" in output
                     and "Failed to verify" not in output
                     if expected else
                     f"❌ Failed to verify invariant archive_transfer_inductive_{failed_invariant}" in output)
            if not valid:
                raise RuntimeError(f"{name}: unexpected verifier result\n{output}")
            print(f"{name}: {'proved' if expected else 'rejected as expected'}", flush=True)


if __name__ == "__main__":
    main()
