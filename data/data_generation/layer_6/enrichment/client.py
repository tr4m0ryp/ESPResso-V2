"""LLM client wrapper for Layer 6 transport distance extraction.

Thin wrapper around the shared FunctionClient.  Posts directly to the
chat completions endpoint (bypassing FunctionClient._call_model which
hardcodes its own system message), applies retry logic with exponential
backoff, and returns a parsed list of transport distance dicts.

Primary class:
    EnrichmentClient -- Handles LLM calls for batch transport distance
                        extraction with retry and JSON parsing.
"""

import json
import logging
import random
import re
import time
from typing import Any, Dict, List

from data.data_generation.layer_6.enrichment.config import EnrichmentConfig
from data.data_generation.shared.api_client import FunctionClient

logger = logging.getLogger(__name__)

_REQUIRED_KEYS = {
    "id",
    "road_km",
    "sea_km",
    "rail_km",
    "air_km",
    "inland_waterway_km",
}


class EnrichmentClient:
    """Thin wrapper around FunctionClient for transport distance extraction.

    Handles:
    - Direct HTTP POST to the chat completions endpoint
    - Retry logic with exponential backoff and jitter (up to max_retries)
    - JSON parsing with markdown fence stripping and thinking tag removal
    - Validation that each element in the returned list has the required keys
    """

    def __init__(self, config: EnrichmentConfig):
        """Initialize client with enrichment configuration.

        Args:
            config: EnrichmentConfig instance with API settings.
        """
        self.config = config
        self.client = FunctionClient(
            api_key=config.api_key,
            model_id=config.api_model,
            base_url=config.api_base_url,
            layer_name="layer_6_enrichment",
        )
        logger.info(
            "Initialized EnrichmentClient with model: %s", config.api_model
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract_transport_distances(
        self,
        system_prompt: str,
        user_prompt: str,
    ) -> List[Dict[str, Any]]:
        """Call the LLM and return parsed transport distance records.

        Retries up to config.max_retries times with exponential backoff
        and jitter.  Raises the last exception if all attempts fail.

        Args:
            system_prompt: Static system prompt for the LLM.
            user_prompt: Per-batch user prompt containing the transport
                         leg data to be summarised into distances.

        Returns:
            List of dicts, each containing the keys:
            id, road_km, sea_km, rail_km, air_km, inland_waterway_km.

        Raises:
            Exception: Re-raises the last error after max_retries
                       exhausted (orchestrator handles fail-open).
        """
        url = f"{self.client.base_url}/chat/completions"
        payload: Dict[str, Any] = {
            "model": self.config.api_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens,
        }

        last_exc: Exception = RuntimeError("No attempts made")

        for attempt in range(self.config.max_retries):
            try:
                response = self.client._make_api_call(url, payload)
                raw_text = response.content or response.reasoning or ""
                result = self._parse_json_response(raw_text)
                if result:
                    return result
                # Empty parse result -- treat as retriable failure
                raise ValueError(
                    "JSON parse returned empty list from non-empty response"
                )
            except Exception as exc:
                last_exc = exc
                if attempt < self.config.max_retries - 1:
                    delay = min(2 ** attempt + random.random(), 60.0)
                    logger.warning(
                        "Attempt %d/%d failed: %s. Retrying in %.2fs",
                        attempt + 1,
                        self.config.max_retries,
                        exc,
                        delay,
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        "All %d attempts exhausted. Last error: %s",
                        self.config.max_retries,
                        exc,
                    )

        raise last_exc

    def test_connection(self) -> bool:
        """Verify the API is reachable with a minimal prompt.

        Returns:
            True if the API returns a parseable non-empty response.
        """
        system_prompt = (
            "You are a transport analyst. "
            "Output ONLY valid JSON array with keys: "
            "id, road_km, sea_km, rail_km, air_km, inland_waterway_km."
        )
        user_prompt = (
            '[{"id": "pp-000001", "transport_legs": '
            '[{"transport_modes": ["road"], "distance_km": 100}]}]'
        )
        try:
            result = self.extract_transport_distances(
                system_prompt, user_prompt
            )
            return bool(result)
        except Exception as exc:
            logger.error("Connection test failed: %s", exc)
            return False

    def get_model_info(self) -> Dict[str, Any]:
        """Return metadata about the configured model."""
        return {
            "model": self.config.api_model,
            "base_url": self.config.api_base_url,
            "api_type": "chat_completions",
            "max_retries": self.config.max_retries,
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_json_response(self, raw_text: str) -> List[Dict[str, Any]]:
        """Parse LLM response text into a validated list of distance dicts.

        Applies in order:
        1. Strip thinking tags via FunctionClient helper.
        2. Strip markdown code fences.
        3. Extract outermost JSON array (first [ to last ]).
        4. Fall back to parsing the stripped text as-is.

        Invalid elements (missing required keys or wrong type) are logged
        and dropped rather than causing a total failure.

        Args:
            raw_text: Raw text content from the model response.

        Returns:
            List of valid distance dicts (may be empty on complete failure).
        """
        if not raw_text:
            logger.warning("Empty response text received")
            return []

        strip_fn = getattr(self.client, "_strip_thinking_tags", None)
        text = strip_fn(raw_text) if callable(strip_fn) else raw_text

        # 1. Markdown fence: ```json ... ``` or ``` ... ```
        fence_match = re.search(
            r"```(?:json)?\s*(\[[\s\S]*?\])\s*```", text
        )
        if fence_match:
            parsed = self._try_parse_array(fence_match.group(1))
            if parsed is not None:
                return parsed

        # 2. Outermost array brackets
        start = text.find("[")
        end = text.rfind("]")
        if start != -1 and end != -1 and end > start:
            parsed = self._try_parse_array(text[start: end + 1])
            if parsed is not None:
                return parsed

        # 3. Last resort: parse as-is
        parsed = self._try_parse_array(text.strip())
        if parsed is not None:
            return parsed

        logger.warning(
            "Failed to parse JSON array response. Preview: %.200s", raw_text
        )
        return []

    def _try_parse_array(
        self, text: str
    ) -> List[Dict[str, Any]] | None:
        """Attempt to parse text as a JSON array of distance records.

        Args:
            text: Candidate JSON string.

        Returns:
            List of valid elements (elements with missing keys are dropped),
            or None if the text is not a valid JSON array at all.
        """
        try:
            data = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            return None

        if not isinstance(data, list):
            return None

        valid: List[Dict[str, Any]] = []
        for i, element in enumerate(data):
            if not isinstance(element, dict):
                logger.debug(
                    "Response element %d is not a dict, skipping", i
                )
                continue
            missing = _REQUIRED_KEYS - element.keys()
            if missing:
                logger.debug(
                    "Response element %d missing keys %s, skipping",
                    i,
                    missing,
                )
                continue
            valid.append(element)

        return valid
