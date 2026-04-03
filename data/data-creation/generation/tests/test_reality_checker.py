"""
Tests for RealityChecker fail-closed behavior and retry logic.

Verifies that:
- Malformed JSON responses trigger retries (up to MAX_PARSE_RETRIES).
- When all retries are exhausted, every record is marked FAILED.
- Records not covered by a partial LLM response are marked FAILED.
- Valid JSON responses are parsed correctly.
- The old _fail_open method no longer exists.
"""

import json
import sys
import os
import unittest
from unittest.mock import MagicMock, patch, call
from dataclasses import dataclass
from typing import List

# Add the parent directory to the path so we can import the shared module
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..")
    ),
)

from shared.reality_checker import (
    RealityChecker,
    MAX_PARSE_RETRIES,
)
from shared.reality_check_models import BatchCheckResult, RecordCheckResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@dataclass
class FakeRecord:
    """Minimal record stand-in for testing."""
    name: str


def _make_checker(
    generate_text_side_effect=None,
    generate_text_return_value=None,
):
    """Build a RealityChecker wired to a mock FunctionClient."""
    mock_client = MagicMock()
    if generate_text_side_effect is not None:
        mock_client.generate_text.side_effect = generate_text_side_effect
    elif generate_text_return_value is not None:
        mock_client.generate_text.return_value = generate_text_return_value

    checker = RealityChecker(
        api_client=mock_client,
        format_records_fn=lambda recs: "\n".join(
            f"[Record {i}] {r.name}" for i, r in enumerate(recs)
        ),
        validation_prompt_fn=lambda text: f"Evaluate:\n{text}",
        system_prompt="You are a test validator.",
        batch_size=10,
    )
    return checker, mock_client


def _make_records(n: int) -> List[FakeRecord]:
    return [FakeRecord(name=f"record_{i}") for i in range(n)]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestFailOpenRemoved(unittest.TestCase):
    """The old _fail_open method must no longer exist."""

    def test_no_fail_open_method(self):
        checker, _ = _make_checker(generate_text_return_value="")
        self.assertFalse(
            hasattr(checker, "_fail_open"),
            "_fail_open should have been removed from RealityChecker",
        )


class TestMalformedJsonRetries(unittest.TestCase):
    """Malformed JSON triggers retries, then fail-closed."""

    @patch("shared.reality_checker.time.sleep", return_value=None)
    def test_retries_on_garbage_text(self, mock_sleep):
        """Pure garbage text should be retried MAX_PARSE_RETRIES times."""
        garbage = "This is not JSON at all, just plain text."
        checker, mock_client = _make_checker(
            generate_text_return_value=garbage,
        )
        records = _make_records(3)
        result = checker.check_batch(records)

        # All records must be FAILED
        self.assertEqual(len(result.passed_records), 0)
        self.assertEqual(len(result.failed_records), 3)
        self.assertEqual(result.pass_rate, 0.0)

        # generate_text should have been called MAX_PARSE_RETRIES times
        self.assertEqual(
            mock_client.generate_text.call_count,
            MAX_PARSE_RETRIES,
        )

    @patch("shared.reality_checker.time.sleep", return_value=None)
    def test_retries_on_truncated_json(self, mock_sleep):
        """Truncated JSON that cannot be parsed should also retry."""
        truncated = '{"results": [{"index": 0, "realistic": true, "reas'
        checker, mock_client = _make_checker(
            generate_text_return_value=truncated,
        )
        records = _make_records(2)
        result = checker.check_batch(records)

        self.assertEqual(len(result.passed_records), 0)
        self.assertEqual(len(result.failed_records), 2)
        self.assertEqual(
            mock_client.generate_text.call_count,
            MAX_PARSE_RETRIES,
        )

    @patch("shared.reality_checker.time.sleep", return_value=None)
    def test_retries_on_empty_response(self, mock_sleep):
        """Empty string response should retry, not pass through."""
        checker, mock_client = _make_checker(
            generate_text_return_value="",
        )
        records = _make_records(2)
        result = checker.check_batch(records)

        self.assertEqual(len(result.passed_records), 0)
        self.assertEqual(len(result.failed_records), 2)
        self.assertEqual(
            mock_client.generate_text.call_count,
            MAX_PARSE_RETRIES,
        )

    @patch("shared.reality_checker.time.sleep", return_value=None)
    def test_retries_on_api_exception(self, mock_sleep):
        """When generate_text raises an exception, retry then fail-closed."""
        checker, mock_client = _make_checker(
            generate_text_side_effect=RuntimeError("API down"),
        )
        records = _make_records(4)
        result = checker.check_batch(records)

        self.assertEqual(len(result.passed_records), 0)
        self.assertEqual(len(result.failed_records), 4)
        self.assertEqual(
            mock_client.generate_text.call_count,
            MAX_PARSE_RETRIES,
        )

    @patch("shared.reality_checker.time.sleep", return_value=None)
    def test_retries_stop_on_first_success(self, mock_sleep):
        """If a retry succeeds, no further retries happen."""
        good_response = json.dumps({
            "results": [
                {"index": 0, "realistic": True, "reason": "ok", "improvement": ""},
                {"index": 1, "realistic": True, "reason": "ok", "improvement": ""},
            ]
        })
        checker, mock_client = _make_checker(
            generate_text_side_effect=[
                "not json",          # attempt 1: fails
                good_response,       # attempt 2: succeeds
            ],
        )
        records = _make_records(2)
        result = checker.check_batch(records)

        self.assertEqual(len(result.passed_records), 2)
        self.assertEqual(len(result.failed_records), 0)
        self.assertEqual(mock_client.generate_text.call_count, 2)

    @patch("shared.reality_checker.time.sleep", return_value=None)
    def test_markdown_wrapped_json_retries(self, mock_sleep):
        """JSON wrapped in markdown fences should still be parseable
        (strategy 2), so no retry needed."""
        inner = json.dumps({
            "results": [
                {"index": 0, "realistic": True, "reason": "ok", "improvement": ""},
            ]
        })
        response = f"```json\n{inner}\n```"
        checker, mock_client = _make_checker(
            generate_text_return_value=response,
        )
        records = _make_records(1)
        result = checker.check_batch(records)

        # Should succeed on first attempt via code-block extraction
        self.assertEqual(len(result.passed_records), 1)
        self.assertEqual(mock_client.generate_text.call_count, 1)


class TestUncoveredRecordsFailClosed(unittest.TestCase):
    """Records not evaluated by the LLM must be marked FAILED."""

    @patch("shared.reality_checker.time.sleep", return_value=None)
    def test_partial_response_fails_uncovered(self, mock_sleep):
        """If LLM only evaluates record 0 out of [0,1,2], records 1 and 2
        should be FAILED."""
        partial_response = json.dumps({
            "results": [
                {"index": 0, "realistic": True, "reason": "fine", "improvement": ""},
            ]
        })
        checker, _ = _make_checker(
            generate_text_return_value=partial_response,
        )
        records = _make_records(3)
        result = checker.check_batch(records)

        self.assertEqual(len(result.passed_records), 1)
        self.assertEqual(len(result.failed_records), 2)

        # Verify the failed records are the uncovered ones
        failed_indices = {r.record_index for r in result.failed_records}
        self.assertEqual(failed_indices, {1, 2})

        # Check justification mentions fail-closed
        for r in result.failed_records:
            self.assertIn("fail-closed", r.justification.lower())

    @patch("shared.reality_checker.time.sleep", return_value=None)
    def test_empty_results_array_fails_all(self, mock_sleep):
        """An empty results array means no records were evaluated."""
        response = json.dumps({"results": []})
        checker, _ = _make_checker(
            generate_text_return_value=response,
        )
        records = _make_records(2)
        result = checker.check_batch(records)

        self.assertEqual(len(result.passed_records), 0)
        self.assertEqual(len(result.failed_records), 2)


class TestValidResponseParsing(unittest.TestCase):
    """Correctly formatted responses should be parsed without retries."""

    def test_all_pass(self):
        response = json.dumps({
            "results": [
                {"index": 0, "realistic": True, "reason": "good", "improvement": ""},
                {"index": 1, "realistic": True, "reason": "fine", "improvement": ""},
            ]
        })
        checker, mock_client = _make_checker(
            generate_text_return_value=response,
        )
        records = _make_records(2)
        result = checker.check_batch(records)

        self.assertEqual(len(result.passed_records), 2)
        self.assertEqual(len(result.failed_records), 0)
        self.assertAlmostEqual(result.pass_rate, 1.0)
        self.assertEqual(mock_client.generate_text.call_count, 1)

    def test_mixed_pass_fail(self):
        response = json.dumps({
            "results": [
                {"index": 0, "realistic": True, "reason": "good", "improvement": ""},
                {"index": 1, "realistic": False, "reason": "bad ratio", "improvement": "fix it"},
                {"index": 2, "realistic": True, "reason": "ok", "improvement": ""},
            ]
        })
        checker, mock_client = _make_checker(
            generate_text_return_value=response,
        )
        records = _make_records(3)
        result = checker.check_batch(records)

        self.assertEqual(len(result.passed_records), 2)
        self.assertEqual(len(result.failed_records), 1)
        self.assertEqual(result.failed_records[0].record_index, 1)

    def test_default_realistic_is_false(self):
        """If the LLM omits the 'realistic' key, it should default to
        False (fail-closed), not True."""
        response = json.dumps({
            "results": [
                {"index": 0, "reason": "missing realistic field", "improvement": ""},
            ]
        })
        checker, _ = _make_checker(
            generate_text_return_value=response,
        )
        records = _make_records(1)
        result = checker.check_batch(records)

        self.assertEqual(len(result.passed_records), 0)
        self.assertEqual(len(result.failed_records), 1)


class TestFailClosedJustifications(unittest.TestCase):
    """Verify that fail-closed results carry informative justifications."""

    @patch("shared.reality_checker.time.sleep", return_value=None)
    def test_fail_closed_justification_text(self, mock_sleep):
        checker, _ = _make_checker(
            generate_text_return_value="not json",
        )
        records = _make_records(1)
        result = checker.check_batch(records)

        self.assertEqual(len(result.failed_records), 1)
        justification = result.failed_records[0].justification
        self.assertIn("Fail-closed", justification)

    @patch("shared.reality_checker.time.sleep", return_value=None)
    def test_fail_closed_improvement_hint(self, mock_sleep):
        checker, _ = _make_checker(
            generate_text_return_value="not json",
        )
        records = _make_records(1)
        result = checker.check_batch(records)

        hint = result.failed_records[0].improvement_hint
        self.assertTrue(len(hint) > 0, "improvement_hint should not be empty")


class TestBatchSplitting(unittest.TestCase):
    """Verify that large batches are split and each sub-batch retries
    independently."""

    @patch("shared.reality_checker.time.sleep", return_value=None)
    def test_two_subbatches_both_fail(self, mock_sleep):
        """With batch_size=2 and 4 records, there are 2 sub-batches.
        Each should be retried independently."""
        mock_client = MagicMock()
        mock_client.generate_text.return_value = "garbage"

        checker = RealityChecker(
            api_client=mock_client,
            format_records_fn=lambda recs: str(recs),
            validation_prompt_fn=lambda text: text,
            system_prompt="test",
            batch_size=2,
        )
        records = _make_records(4)
        result = checker.check_batch(records)

        # 4 records, all failed
        self.assertEqual(len(result.failed_records), 4)
        self.assertEqual(len(result.passed_records), 0)

        # 2 sub-batches * MAX_PARSE_RETRIES retries each
        self.assertEqual(
            mock_client.generate_text.call_count,
            2 * MAX_PARSE_RETRIES,
        )


class TestRetryBackoff(unittest.TestCase):
    """Verify that retries use exponential back-off."""

    @patch("shared.reality_checker.time.sleep", return_value=None)
    def test_sleep_called_between_retries(self, mock_sleep):
        checker, _ = _make_checker(
            generate_text_return_value="not json",
        )
        records = _make_records(1)
        checker.check_batch(records)

        # There should be MAX_PARSE_RETRIES - 1 sleep calls
        # (no sleep after the last attempt)
        self.assertEqual(
            mock_sleep.call_count,
            MAX_PARSE_RETRIES - 1,
        )

        # Verify delays are increasing (exponential back-off)
        delays = [c.args[0] for c in mock_sleep.call_args_list]
        for i in range(1, len(delays)):
            self.assertGreater(
                delays[i], delays[i - 1],
                "Back-off delays should increase",
            )


if __name__ == "__main__":
    unittest.main()
