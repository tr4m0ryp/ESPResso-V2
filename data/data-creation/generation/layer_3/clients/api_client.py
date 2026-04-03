"""
Layer 3 API client (V2) -- single-call transport leg generation.

Replaces the V1 5-strategy variant logic with one call per record
that returns a flat list of transport leg dictionaries.
"""

import logging
import time
from typing import Any, Dict, List

from data.data_generation.layer_3.config.config import Layer3Config
from data.data_generation.shared.api_client import FunctionClient

logger = logging.getLogger(__name__)


class Layer3Client:
    """Thin wrapper around FunctionClient for Layer 3 transport leg generation.

    Key differences from V1:
    - Single method ``generate_transport_legs`` instead of
      ``generate_transport_scenarios``.
    - No 5-strategy suffix logic or scenario ID generation.
    - System prompt is provided by the caller and forwarded as-is
      to the chat completions API as the ``system`` message.
    - Retry with exponential backoff is handled here so the caller
      only sees success or a final failure.
    """

    def __init__(self, config: Layer3Config):
        self.config = config
        self.client = FunctionClient(
            api_key=config.api_key,
            model_id=config.api_model,
            base_url=config.api_base_url,
            layer_name="layer_3",
        )
        logger.info("Initialized Layer3Client with model: %s", config.api_model)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_transport_legs(
        self,
        system_prompt: str,
        user_prompt: str,
    ) -> List[Dict[str, Any]]:
        """Generate transport legs for a single record.

        Args:
            system_prompt: Static system prompt (cached by caller via
                prompt_caching on the API side).
            user_prompt: Per-record user prompt describing the product,
                materials, processing steps, and geographic constraints.

        Returns:
            List of leg dictionaries matching the transport_legs schema
            defined in LAYER3_DESIGN.md section 4.2.  Returns an empty
            list when the API call fails after all retries.
        """
        max_retries: int = getattr(self.config, "max_retries", 3)
        base_delay: float = getattr(self.config, "retry_delay", 2.0)

        last_error: Exception | None = None

        for attempt in range(1, max_retries + 1):
            try:
                legs = self._call_with_system_prompt(system_prompt, user_prompt)
                if not self._validate_legs(legs):
                    logger.warning(
                        "Attempt %d/%d: response failed basic validation",
                        attempt,
                        max_retries,
                    )
                    last_error = ValueError("Response failed validation")
                    # Fall through to retry logic below
                else:
                    return legs
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "Attempt %d/%d failed: %s", attempt, max_retries, exc
                )

            # Exponential backoff (skip sleep on the final attempt)
            if attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))
                logger.info("Retrying in %.1f seconds ...", delay)
                time.sleep(delay)

        logger.error(
            "All %d attempts exhausted. Last error: %s", max_retries, last_error
        )
        return []

    def test_connection(self) -> bool:
        """Test API connection with a trivial prompt."""
        return self.client.test_connection()

    def get_model_info(self) -> Dict[str, Any]:
        """Return metadata about the configured model."""
        return self.client.get_model_info()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _call_with_system_prompt(
        self,
        system_prompt: str,
        user_prompt: str,
    ) -> List[Dict[str, Any]]:
        """Build a messages list with a custom system prompt and call the API.

        We bypass ``FunctionClient._call_model`` (which hardcodes its own
        system message) and instead construct the payload ourselves, then
        delegate to ``FunctionClient._make_api_call`` for the actual HTTP
        call.
        """
        url = f"{self.client.base_url}/chat/completions"

        messages = [
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": user_prompt,
            },
        ]

        effective_max_tokens = max(self.config.max_tokens, 8000)

        payload: Dict[str, Any] = {
            "model": self.config.api_model,
            "messages": messages,
            "temperature": self.config.temperature,
            "max_tokens": effective_max_tokens,
        }

        response = self.client._make_api_call(url, payload)
        return self.client._extract_json_from_response(response)

    @staticmethod
    def _validate_legs(legs: List[Dict[str, Any]]) -> bool:
        """Basic structural validation: non-empty list of dicts.

        Full schema validation is performed downstream by the validator
        module, so this check is intentionally minimal.
        """
        if not isinstance(legs, list) or len(legs) == 0:
            return False
        return all(isinstance(leg, dict) for leg in legs)
