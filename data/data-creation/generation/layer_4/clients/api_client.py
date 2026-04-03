"""
Layer 4 API client -- packaging weight estimation.

Thin wrapper around the shared FunctionClient.  Builds the chat completions
payload manually (bypassing FunctionClient._call_model which hardcodes a
supply-chain system prompt) and returns a parsed dict with the four required
keys: paper_cardboard_kg, plastic_kg, other_kg, reasoning.
"""

import json
import logging
import re
from typing import Any, Dict, List

from data.data_generation.layer_4.config.config import Layer4Config
from data.data_generation.shared.api_client import FunctionClient

logger = logging.getLogger(__name__)

_REQUIRED_KEYS = {"paper_cardboard_kg", "plastic_kg", "other_kg", "reasoning"}


class Layer4Client:
    """Thin wrapper around FunctionClient for Layer 4 packaging estimation."""

    def __init__(self, config: Layer4Config):
        self.config = config
        self.client = FunctionClient(
            api_key=config.api_key,
            model_id=config.api_model,
            base_url=config.api_base_url,
            layer_name="layer_4",
        )
        logger.info("Initialized Layer4Client with model: %s", config.api_model)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_packaging(
        self,
        system_prompt: str,
        user_prompt: str,
    ) -> Dict[str, Any]:
        """Estimate packaging weights for a single product record.

        Builds the messages list with the caller-supplied system prompt,
        posts directly to the chat completions endpoint (bypassing
        FunctionClient._call_model which hardcodes its own system message),
        and returns a parsed dict.

        Args:
            system_prompt: Static system prompt forwarded as the ``system``
                message.  Expected to be cached on the API side.
            user_prompt: Per-record user prompt describing the product.

        Returns:
            Dict with keys: paper_cardboard_kg, plastic_kg, other_kg,
            reasoning.  Returns an empty dict on failure.
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

        response = self.client._make_api_call(url, payload)

        raw_text = response.content or response.reasoning or ""
        return self._parse_json_response(raw_text)

    def generate_packaging_batch(
        self,
        system_prompt: str,
        user_prompt: str,
    ) -> List[Dict[str, Any]]:
        """Estimate packaging weights for a batch of products.

        Same HTTP call as generate_packaging but expects and parses a
        JSON array response. Returns a list of dicts, each with the
        required keys plus an ``index`` field.  Invalid elements are
        silently dropped.
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

        response = self.client._make_api_call(url, payload)
        raw_text = response.content or response.reasoning or ""
        return self._parse_json_array_response(raw_text)

    def test_connection(self) -> bool:
        """Send a minimal prompt and verify the response parses correctly.

        Returns:
            True if the API returns a parseable response, False otherwise.
        """
        system_prompt = (
            "You are a packaging expert. "
            "Output ONLY valid JSON with the keys: "
            "paper_cardboard_kg, plastic_kg, other_kg, reasoning."
        )
        user_prompt = (
            "Estimate packaging weights for one cotton t-shirt (0.2 kg)."
        )
        try:
            result = self.generate_packaging(system_prompt, user_prompt)
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
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_json_array_response(self, raw_text: str) -> List[Dict[str, Any]]:
        """Parse a JSON array response from a batch API call.

        Handles markdown fences, bracket extraction, and partial
        failures (drops invalid elements rather than failing entirely).
        """
        if not raw_text:
            logger.warning("Empty batch response text received")
            return []

        if callable(getattr(self.client, "_strip_thinking_tags", None)):
            text = self.client._strip_thinking_tags(raw_text)
        else:
            text = raw_text

        # 1. Markdown fence: ```json ... ``` or ``` ... ```
        fence_match = re.search(
            r"```(?:json)?\s*(\[[\s\S]*?\])\s*```", text
        )
        if fence_match:
            parsed = self._try_parse_array(fence_match.group(1))
            if parsed is not None:
                return parsed

        # 2. Locate outermost array: first [ to last ]
        start = text.find("[")
        end = text.rfind("]")
        if start != -1 and end != -1 and end > start:
            parsed = self._try_parse_array(text[start : end + 1])
            if parsed is not None:
                return parsed

        # 3. Last resort: try parsing as-is
        parsed = self._try_parse_array(text.strip())
        if parsed is not None:
            return parsed

        logger.warning(
            "Failed to parse JSON array response. Preview: %.200s", raw_text
        )
        return []

    def _try_parse_array(self, text: str) -> List[Dict[str, Any]] | None:
        """Attempt to parse text as a JSON array of packaging results.

        Returns a list of valid elements (with required keys + index),
        or None if the text is not a valid JSON array at all.
        """
        try:
            data = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            return None

        if not isinstance(data, list):
            return None

        batch_keys = _REQUIRED_KEYS | {"index"}
        valid: List[Dict[str, Any]] = []
        for i, element in enumerate(data):
            if not isinstance(element, dict):
                logger.debug("Batch element %d is not a dict, skipping", i)
                continue
            missing = batch_keys - element.keys()
            if missing:
                logger.debug(
                    "Batch element %d missing keys %s, skipping", i, missing
                )
                continue
            valid.append(element)

        return valid

    def _parse_json_response(self, raw_text: str) -> Dict[str, Any]:
        """Parse the model response into a packaging weight dict.

        Handles:
        - Clean JSON
        - Markdown-fenced JSON (```json ... ```)
        - JSON with surrounding prose (extracts between first { and last })
        - Thinking tags (delegates to FunctionClient._strip_thinking_tags
          when available)

        Args:
            raw_text: Raw text content from the model response.

        Returns:
            Dict with required keys on success, empty dict on failure.
        """
        if not raw_text:
            logger.warning("Empty response text received")
            return {}

        # Strip thinking tags if the shared helper is available.
        if callable(getattr(self.client, "_strip_thinking_tags", None)):
            text = self.client._strip_thinking_tags(raw_text)
        else:
            text = raw_text

        # 1. Markdown fence: ```json ... ``` or ``` ... ```
        fence_match = re.search(
            r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", text
        )
        if fence_match:
            candidate = fence_match.group(1)
            result = self._try_parse(candidate)
            if result is not None:
                return result

        # 2. Locate outermost object: first { to last }
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            candidate = text[start : end + 1]
            result = self._try_parse(candidate)
            if result is not None:
                return result

        # 3. Last resort: attempt to parse the stripped text as-is
        result = self._try_parse(text.strip())
        if result is not None:
            return result

        logger.warning(
            "Failed to parse JSON response. Preview: %.200s", raw_text
        )
        return {}

    def _try_parse(self, text: str) -> Dict[str, Any] | None:
        """Attempt to parse text as JSON and validate required keys.

        Returns:
            Validated dict on success, None on any failure.
        """
        try:
            data = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            return None

        if not isinstance(data, dict):
            return None

        missing = _REQUIRED_KEYS - data.keys()
        if missing:
            logger.debug("Parsed JSON missing required keys: %s", missing)
            return None

        return data
