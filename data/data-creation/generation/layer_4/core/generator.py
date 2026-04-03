"""
Packaging Generator for Layer 4 (V2).

Single-record packaging estimation with retry logic and two-pass
correction feedback. Replaces V1 PackagingConfig/PackagingConfigGenerator
and all Nemotron-specific code.
"""

import logging
import time
from typing import Any, Dict, List, Optional

from data.data_generation.layer_4.clients.api_client import Layer4Client
from data.data_generation.layer_4.config.config import Layer4Config
from data.data_generation.layer_4.models.models import Layer4Record, PackagingResult
from data.data_generation.layer_4.prompts.builder import PromptBuilder

logger = logging.getLogger(__name__)


class PackagingGenerator:
    """Generates packaging estimates for Layer 3 records via LLM.

    Uses PromptBuilder for prompt assembly and Layer4Client for API calls.
    Each Layer 3 record produces exactly one Layer4Record containing
    packaging category masses and reasoning.

    The system prompt is loaded once at construction and reused for all
    records. This class does not validate mass ranges and does not write
    output -- those concerns belong to the caller.
    """

    def __init__(
        self,
        config: Layer4Config,
        api_client: Layer4Client,
        prompt_builder: PromptBuilder,
    ):
        self.config = config
        self.api_client = api_client
        self.prompt_builder = prompt_builder
        self._system_prompt = prompt_builder.get_system_prompt()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_for_record(
        self, record: Dict[str, Any]
    ) -> Optional[Layer4Record]:
        """Generate a packaging estimate for a single Layer 3 record.

        Retries up to config.max_retries times with exponential backoff
        on API error or JSON parse failure. Returns None after all
        retries are exhausted.

        Args:
            record: A Layer 3 record dict as produced by the Layer 3
                pipeline (may contain JSON-encoded list/dict fields).

        Returns:
            A Layer4Record on success, or None if all retries fail.
        """
        user_prompt = self.prompt_builder.build_user_prompt(record)
        product_name = record.get("subcategory_name", "unknown")

        for attempt in range(self.config.max_retries):
            try:
                packaging_result = self._attempt_generation(user_prompt)
                layer4_record = Layer4Record.from_layer3(record, packaging_result)

                logger.info(
                    "Generated packaging for '%s': %.4g kg total",
                    product_name,
                    packaging_result.total_mass_kg(),
                )
                return layer4_record

            except Exception as exc:
                delay = self.config.retry_delay * (2 ** attempt)
                logger.warning(
                    "Attempt %d/%d for '%s' failed: %s. Retrying in %.1fs",
                    attempt + 1,
                    self.config.max_retries,
                    product_name,
                    exc,
                    delay,
                )
                if attempt < self.config.max_retries - 1:
                    time.sleep(delay)

        logger.warning(
            "All %d retries exhausted for '%s'. Returning None.",
            self.config.max_retries,
            product_name,
        )
        return None

    def generate_for_batch(
        self, records: List[Dict[str, Any]]
    ) -> List[Optional[Layer4Record]]:
        """Generate packaging estimates for a batch of records in one API call.

        Builds a multi-product prompt, calls the batch API endpoint, and
        maps results back to records by the ``index`` field.  Missing
        indices produce None so callers can fall through to individual
        retry.

        Retries the whole batch up to config.max_retries on API failure.
        """
        batch_prompt = self.prompt_builder.build_batch_user_prompt(records)
        n = len(records)

        for attempt in range(self.config.max_retries):
            try:
                batch_results = self.api_client.generate_packaging_batch(
                    self._system_prompt, batch_prompt
                )
                return self._map_batch_results(records, batch_results, n)

            except Exception as exc:
                delay = self.config.retry_delay * (2 ** attempt)
                logger.warning(
                    "Batch attempt %d/%d failed (%d records): %s. "
                    "Retrying in %.1fs",
                    attempt + 1,
                    self.config.max_retries,
                    n,
                    exc,
                    delay,
                )
                if attempt < self.config.max_retries - 1:
                    time.sleep(delay)

        logger.warning(
            "All %d batch retries exhausted for %d records. "
            "Returning all None.",
            self.config.max_retries,
            n,
        )
        return [None] * n

    def _map_batch_results(
        self,
        records: List[Dict[str, Any]],
        batch_results: List[Dict[str, Any]],
        n: int,
    ) -> List[Optional[Layer4Record]]:
        """Map batch API results back to records by index field."""
        results_by_index = {}
        for item in batch_results:
            idx = int(item["index"])
            results_by_index[idx] = item

        mapped: List[Optional[Layer4Record]] = []
        for i, record in enumerate(records):
            one_based = i + 1
            item = results_by_index.get(one_based)
            if item is None:
                logger.debug(
                    "Batch missing index %d for '%s'",
                    one_based,
                    record.get("subcategory_name", "unknown"),
                )
                mapped.append(None)
                continue
            try:
                pkg = PackagingResult.from_dict(item)
                mapped.append(Layer4Record.from_layer3(record, pkg))
            except Exception as exc:
                logger.debug(
                    "Failed to parse batch index %d: %s", one_based, exc
                )
                mapped.append(None)

        return mapped

    def regenerate_with_feedback(
        self, record: Dict[str, Any], failures: List[str]
    ) -> Optional[Layer4Record]:
        """Regenerate a packaging estimate with correction feedback.

        Used in the two-pass validation flow. Builds a correction prompt
        that includes the list of validation failures so the model can
        address them directly. Only one attempt is made; returns None on
        failure.

        Args:
            record: The Layer 3 record dict that produced a failing result.
            failures: Human-readable validation failure strings.

        Returns:
            A Layer4Record on success, or None if the attempt fails.
        """
        product_name = record.get("subcategory_name", "unknown")
        correction_prompt = self.prompt_builder.build_correction_prompt(
            record, failures
        )

        try:
            packaging_result = self._attempt_generation(correction_prompt)
            layer4_record = Layer4Record.from_layer3(record, packaging_result)

            logger.info(
                "Regenerated packaging for '%s': %.4g kg total",
                product_name,
                packaging_result.total_mass_kg(),
            )
            return layer4_record

        except Exception as exc:
            logger.warning(
                "Correction attempt for '%s' failed: %s. Returning None.",
                product_name,
                exc,
            )
            return None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _attempt_generation(self, user_prompt: str) -> PackagingResult:
        """Execute a single API call and parse the response.

        Raises on API error or if the response cannot be parsed into a
        valid PackagingResult.

        Args:
            user_prompt: The per-record (or correction) user prompt.

        Returns:
            A PackagingResult parsed from the model response.

        Raises:
            ValueError: If the API returns an empty response or the
                response is missing required keys.
            Exception: Re-raises any exception from the API client.
        """
        response = self.api_client.generate_packaging(
            self._system_prompt, user_prompt
        )

        if not response:
            raise ValueError("API returned an empty response")

        return PackagingResult.from_dict(response)
