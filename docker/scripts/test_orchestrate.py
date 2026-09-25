#!/usr/bin/env python3

import unittest
from unittest.mock import patch
from subprocess import CompletedProcess

import orchestrate
import uclid_checked


class ModelTestCaseParsingTests(unittest.TestCase):
    def test_zero_exit_failed_proof_is_rejected(self) -> None:
        with patch("orchestrate.subprocess.run", return_value=CompletedProcess(
                [], 0, "Verified 7 invariants!\nFailed to verify 3 properties!", "")):
            with self.assertRaises(orchestrate.OrchestrationError):
                orchestrate._prove("ArchiveProof.pproj", {}, 14)

    def test_verifier_requires_actual_obligations(self) -> None:
        self.assertFalse(uclid_checked.has_verification_results("Error: Unknown option -M"))
        self.assertFalse(uclid_checked.has_verification_results("Syntax error"))
        self.assertFalse(uclid_checked.has_verification_results("0 assertions passed.\n0 assertions failed.\n0 assertions indeterminate."))
        self.assertTrue(uclid_checked.has_verification_results("9 assertions passed.\n0 assertions failed.\n0 assertions indeterminate."))
        self.assertTrue(uclid_checked.has_verification_results("8 assertions passed.\n1 assertions failed.\n0 assertions indeterminate."))

    def test_extracts_only_the_reported_test_cases(self) -> None:
        output = """\
.. List of test cases:
tcFirst
tcSecond
. Done
~~ [PTool]: Thanks for using P! ~~
"""

        self.assertEqual(
            orchestrate._parse_model_test_cases(output),
            ("tcFirst", "tcSecond"),
        )

    def test_rejects_an_empty_listing(self) -> None:
        with self.assertRaisesRegex(
            orchestrate.OrchestrationError,
            "reported no model test cases",
        ):
            orchestrate._parse_model_test_cases(".. List of test cases:\n. Done\n")

    def test_rejects_duplicate_test_cases(self) -> None:
        output = """\
.. List of test cases:
tcDuplicate
tcDuplicate
. Done
"""

        with self.assertRaisesRegex(
            orchestrate.OrchestrationError,
            "reported duplicate model test cases",
        ):
            orchestrate._parse_model_test_cases(output)


if __name__ == "__main__":
    unittest.main()
