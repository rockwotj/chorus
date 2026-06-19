#!/usr/bin/env python3

import unittest

import orchestrate


class ModelTestCaseParsingTests(unittest.TestCase):
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
