"""LLM client for Layer 6 transport distance extraction.

Calls an OpenAI-compatible /chat/completions endpoint (Gemini, NVIDIA,
etc.) with multi-key round-robin, retry logic, and JSON parsing.

Primary class:
    EnrichmentClient -- Handles LLM calls for batch transport distance
                        extraction with retry and JSON parsing.
"""

import json
import logging
import random
import re
import threading
import time
from typing import Any, Dict, List

import requests

from data.data_generation.layer_6.enrichment.config import EnrichmentConfig

logger = logging.getLogger(__name__)

_REQUIRED_KEYS = {
    "id", "road_km", "sea_km", "rail_km", "air_km", "inland_waterway_km",
}


class EnrichmentClient:
    """Client for transport distance extraction via OpenAI-compatible API.

    Supports multiple API keys with round-robin distribution.
    Rate control is handled by the orchestrator's wave dispatcher.
    """

    def __init__(self, config: EnrichmentConfig):
        self.config = config
        self.base_url = config.api_base_url.rstrip("/")
        self._keys = config.api_keys
        if not self._keys:
            raise ValueError("No API keys configured")
        self._models = [
            m.strip() for m in config.api_models.split(',') if m.strip()
        ]
        if not self._models:
            raise ValueError("No API models configured")
        self._counter = 0
        self._counter_lock = threading.Lock()
        logger.info(
            "EnrichmentClient: models=%s keys=%d",
            self._models, len(self._keys),
        )

    def _next_key_and_model(self):
        """Round-robin select next API key and model."""
        with self._counter_lock:
            key = self._keys[self._counter % len(self._keys)]
            model = self._models[self._counter % len(self._models)]
            self._counter += 1
        return key, model

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract_transport_distances(
        self,
        system_prompt: str,
        user_prompt: str,
    ) -> List[Dict[str, Any]]:
        """Call the LLM and return parsed transport distance records.

        Retries up to config.max_retries times with exponential backoff.
        Each attempt round-robins to the next API key.
        """
        url = f"{self.base_url}/chat/completions"
        last_exc: Exception = RuntimeError("No attempts made")

        for attempt in range(self.config.max_retries):
            key, model = self._next_key_and_model()
            payload = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": self.config.temperature,
                "max_tokens": self.config.max_tokens,
                "reasoning_effort": "none",
            }
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {key}",
            }
            try:
                raw_text = self._call_api(url, payload, headers)
                result = self._parse_json_response(raw_text)
                if result:
                    return result
                raise ValueError("JSON parse returned empty list")
            except Exception as exc:
                last_exc = exc
                if attempt < self.config.max_retries - 1:
                    is_rate_limit = "429" in str(exc)
                    if is_rate_limit:
                        delay = 10.0 + random.random() * 10.0
                    else:
                        delay = min(2 ** attempt + random.random(), 60.0)
                    logger.warning(
                        "Attempt %d/%d failed: %s. Retrying in %.1fs",
                        attempt + 1, self.config.max_retries, exc, delay,
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        "All %d attempts exhausted. Last error: %s",
                        self.config.max_retries, exc,
                    )

        raise last_exc

    def _call_api(self, url: str, payload: Dict, headers: Dict) -> str:
        """POST to /chat/completions and extract response text."""
        resp = requests.post(
            url, json=payload, headers=headers, timeout=300
        )
        resp.raise_for_status()
        data = resp.json()
        choices = data.get("choices", [])
        if not choices:
            raise ValueError("API returned no choices")
        return choices[0].get("message", {}).get("content", "")

    def test_connection(self) -> bool:
        """Verify the API is reachable with a minimal prompt."""
        sys_p = (
            "You are a transport analyst. "
            "Output ONLY valid JSON array with keys: "
            "id, road_km, sea_km, rail_km, air_km, inland_waterway_km."
        )
        usr_p = (
            '[{"id": "pp-000001", "transport_legs": '
            '[{"transport_modes": ["road"], "distance_km": 100}]}]'
        )
        try:
            return bool(self.extract_transport_distances(sys_p, usr_p))
        except Exception as exc:
            logger.error("Connection test failed: %s", exc)
            return False

    # ------------------------------------------------------------------
    # JSON parsing
    # ------------------------------------------------------------------

    def _parse_json_response(self, raw_text: str) -> List[Dict[str, Any]]:
        """Parse LLM response into validated list of distance dicts."""
        if not raw_text:
            logger.warning("Empty response text received")
            return []

        text = re.sub(
            r"<think(?:ing)?>[\s\S]*?</think(?:ing)?>", "", raw_text
        ).strip()

        fence_match = re.search(
            r"```(?:json)?\s*(\[[\s\S]*?\])\s*```", text
        )
        if fence_match:
            parsed = self._try_parse_array(fence_match.group(1))
            if parsed is not None:
                return parsed

        start = text.find("[")
        end = text.rfind("]")
        if start != -1 and end != -1 and end > start:
            parsed = self._try_parse_array(text[start: end + 1])
            if parsed is not None:
                return parsed

        parsed = self._try_parse_array(text.strip())
        if parsed is not None:
            return parsed

        logger.warning("Failed to parse JSON array. Preview: %.200s", raw_text)
        return []

    @staticmethod
    def _try_parse_array(text: str) -> List[Dict[str, Any]] | None:
        """Attempt to parse text as a JSON array of distance records."""
        try:
            data = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            return None
        if not isinstance(data, list):
            return None

        valid: List[Dict[str, Any]] = []
        for i, element in enumerate(data):
            if not isinstance(element, dict):
                continue
            if _REQUIRED_KEYS - element.keys():
                continue
            valid.append(element)
        return valid
