"""
Base RealityChecker for per-layer LLM reality validation.

Validates batches of generated records using the same FunctionClient
that powers generation. Each layer provides its own prompt module.

Fail-closed: when the LLM returns unparseable JSON, the checker retries
the same request up to MAX_PARSE_RETRIES times. If all retries fail,
every record in the batch is marked FAILED (not passed).
"""

import json
import logging
import re
import time
from typing import Any, Callable, Dict, List, Optional

from .api_client import FunctionClient
from .reality_check_models import BatchCheckResult, RecordCheckResult

logger = logging.getLogger(__name__)

# Default batch size for validation API calls
DEFAULT_VALIDATION_BATCH_SIZE = 10

# Maximum retries when the LLM returns unparseable / malformed JSON
MAX_PARSE_RETRIES = 3

# Seconds to wait between parse-failure retries (doubles each attempt)
PARSE_RETRY_BASE_DELAY = 1.0


class RealityChecker:
    """
    Validates generated records for realism using LLM judgments.

    Each layer subclass provides three callables:
      - format_records_for_prompt: serialize records to readable text
      - get_validation_prompt: build the full prompt with records embedded
      - get_system_prompt: role assignment for the validator LLM

    On parse failure the batch is retried up to MAX_PARSE_RETRIES times.
    If every retry fails, all records in the batch are marked FAILED
    (fail-closed).  Records not explicitly evaluated by the LLM are also
    treated as FAILED rather than auto-passed.
    """

    def __init__(
        self,
        api_client: FunctionClient,
        format_records_fn: Callable[[List[Any]], str],
        validation_prompt_fn: Callable[[str], str],
        system_prompt: str,
        batch_size: int = DEFAULT_VALIDATION_BATCH_SIZE,
    ):
        self._client = api_client
        self._format_records = format_records_fn
        self._validation_prompt = validation_prompt_fn
        self._system_prompt = system_prompt
        self._batch_size = batch_size

    def check_batch(self, records: List[Any]) -> BatchCheckResult:
        """Validate a list of records, splitting into sub-batches as needed."""
        all_passed: List[RecordCheckResult] = []
        all_failed: List[RecordCheckResult] = []

        for start in range(0, len(records), self._batch_size):
            chunk = records[start : start + self._batch_size]
            chunk_offset = start
            result = self._check_single_batch(chunk, chunk_offset)
            all_passed.extend(result.passed_records)
            all_failed.extend(result.failed_records)

        total = len(all_passed) + len(all_failed)
        pass_rate = len(all_passed) / total if total > 0 else 0.0
        return BatchCheckResult(
            passed_records=all_passed,
            failed_records=all_failed,
            total_checked=total,
            pass_rate=pass_rate,
        )

    def _check_single_batch(
        self, records: List[Any], offset: int
    ) -> BatchCheckResult:
        """Call the LLM for one sub-batch, retrying on malformed JSON.

        Retries up to MAX_PARSE_RETRIES times when the response cannot
        be parsed into structured results.  If all attempts fail, every
        record in the batch is marked FAILED (fail-closed).
        """
        records_text = self._format_records(records)
        prompt = self._validation_prompt(records_text)

        # Build a prompt that clearly separates the system context from
        # user instructions.  generate_text() does not accept a separate
        # system_prompt argument, so we concatenate them with an explicit
        # delimiter to preserve role framing.
        full_prompt = (
            f"[SYSTEM INSTRUCTIONS]\n{self._system_prompt}\n"
            f"[END SYSTEM INSTRUCTIONS]\n\n"
            f"[USER REQUEST]\n{prompt}\n[END USER REQUEST]"
        )

        last_error: Optional[str] = None

        for attempt in range(1, MAX_PARSE_RETRIES + 1):
            try:
                content = self._client.generate_text(
                    prompt=full_prompt,
                    temperature=0.2,
                    max_tokens=2000,
                )
                result = self._parse_response(content, records, offset)
                if result is not None:
                    return result

                # _parse_response returned None -- unparseable response
                last_error = (
                    f"Unparseable response on attempt {attempt}: "
                    f"{(content or '')[:200]}"
                )
                logger.warning(
                    "Reality check parse failed (attempt %d/%d) for %d "
                    "records: %s",
                    attempt, MAX_PARSE_RETRIES, len(records), last_error,
                )
            except Exception as e:
                last_error = str(e)
                logger.warning(
                    "Reality check LLM call failed (attempt %d/%d) for "
                    "%d records: %s",
                    attempt, MAX_PARSE_RETRIES, len(records), e,
                )

            # Exponential back-off between retries
            if attempt < MAX_PARSE_RETRIES:
                delay = PARSE_RETRY_BASE_DELAY * (2 ** (attempt - 1))
                time.sleep(delay)

        # All retries exhausted -- fail closed
        logger.error(
            "All %d reality-check retries failed for %d records; "
            "fail-closed. Last error: %s",
            MAX_PARSE_RETRIES, len(records), last_error,
        )
        return self._fail_closed(records, offset, reason=last_error or "")

    def _parse_response(
        self, response_text: str, records: List[Any], offset: int
    ) -> Optional[BatchCheckResult]:
        """Parse LLM response into per-record results.

        Returns None when the response cannot be parsed at all (signals
        the caller to retry).  Valid but partial results are accepted;
        any records not covered by the LLM response are marked FAILED.

        Tries four strategies:
        1. Direct JSON parse
        2. Code-block extraction (```json ... ```)
        3. Bracket-matching for a JSON object with a results key
        4. Regex fallback for individual result objects
        """
        if not response_text or not response_text.strip():
            logger.warning("Empty response from reality check")
            return None

        results_list = self._extract_results_list(response_text)
        if results_list is None:
            logger.warning(
                "Could not parse reality check response for %d records",
                len(records),
            )
            return None

        passed: List[RecordCheckResult] = []
        failed: List[RecordCheckResult] = []

        for item in results_list:
            idx = item.get("index", -1)
            if idx < 0 or idx >= len(records):
                continue
            realistic = item.get("realistic", False)
            reason = item.get("reason", "")
            improvement = item.get("improvement", "")

            result = RecordCheckResult(
                record_index=offset + idx,
                passed=bool(realistic),
                justification=str(reason),
                improvement_hint=str(improvement),
                raw_record=records[idx],
            )
            if result.passed:
                passed.append(result)
            else:
                failed.append(result)

        # Records not covered by the LLM response are treated as FAILED
        # (fail-closed), not auto-passed.
        covered_indices = {
            item.get("index", -1) for item in results_list
        }
        for i, record in enumerate(records):
            if i not in covered_indices:
                failed.append(
                    RecordCheckResult(
                        record_index=offset + i,
                        passed=False,
                        justification="Not evaluated by LLM (fail-closed)",
                        improvement_hint="Record was not included in LLM "
                        "response; needs re-evaluation",
                        raw_record=record,
                    )
                )

        total = len(passed) + len(failed)
        pass_rate = len(passed) / total if total > 0 else 0.0
        return BatchCheckResult(
            passed_records=passed,
            failed_records=failed,
            total_checked=total,
            pass_rate=pass_rate,
        )

    def _extract_results_list(
        self, text: str
    ) -> Optional[List[Dict[str, Any]]]:
        """Extract the results array from various response formats."""
        # Strip thinking tags
        text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
        text = re.sub(r"<thinking>.*?</thinking>", "", text, flags=re.DOTALL)
        text = text.strip()

        # Strategy 1: direct JSON parse
        parsed = self._try_parse_json(text)
        if parsed is not None:
            return parsed

        # Strategy 2: code block extraction
        code_blocks = re.findall(
            r"```(?:json)?\s*([\s\S]*?)\s*```", text
        )
        for block in code_blocks:
            parsed = self._try_parse_json(block.strip())
            if parsed is not None:
                return parsed

        # Strategy 3: find JSON object with "results" key via bracket matching
        start = text.find("{")
        if start != -1:
            depth = 0
            for i in range(start, len(text)):
                if text[i] == "{":
                    depth += 1
                elif text[i] == "}":
                    depth -= 1
                    if depth == 0:
                        parsed = self._try_parse_json(text[start : i + 1])
                        if parsed is not None:
                            return parsed
                        break

        # Strategy 4: regex fallback for individual objects
        pattern = r'\{\s*"index"\s*:\s*\d+.*?\}'
        matches = re.findall(pattern, text, re.DOTALL)
        if matches:
            items = []
            for m in matches:
                try:
                    items.append(json.loads(m))
                except json.JSONDecodeError:
                    continue
            if items:
                return items

        return None

    @staticmethod
    def _try_parse_json(text: str) -> Optional[List[Dict[str, Any]]]:
        """Attempt to parse text as JSON and return a results list."""
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return None

        if isinstance(data, dict):
            for key in ("results", "data", "evaluations", "checks"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            return None
        if isinstance(data, list):
            return data
        return None

    @staticmethod
    def _fail_closed(
        records: List[Any], offset: int, reason: str = ""
    ) -> BatchCheckResult:
        """Mark all records as FAILED when validation cannot complete.

        This is the opposite of fail-open: no unvalidated record is
        allowed through the pipeline.
        """
        justification = (
            f"Fail-closed: validation could not be completed. {reason}"
        ).strip()
        failed = [
            RecordCheckResult(
                record_index=offset + i,
                passed=False,
                justification=justification,
                improvement_hint="Record could not be validated; "
                "requires re-generation or manual review",
                raw_record=record,
            )
            for i, record in enumerate(records)
        ]
        return BatchCheckResult(
            passed_records=[],
            failed_records=failed,
            total_checked=len(records),
            pass_rate=0.0,
        )
