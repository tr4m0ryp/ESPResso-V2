"""
Layer 2 API client

Supports multiple API keys for parallel processing with round-robin distribution.
"""

import logging
import threading
from typing import Optional, Dict, Any, List

from ..config.config import Layer2Config
from data.data_generation.shared.api_client import FunctionClient, APIError

logger = logging.getLogger(__name__)


class MultiKeyClientPool:
    """
    Pool of API clients with multiple keys for high-throughput parallel processing.

    Uses round-robin distribution to spread requests across API keys,
    maximizing throughput while respecting per-key rate limits.
    """

    def __init__(self, config: Layer2Config):
        self.config = config
        self._clients: List[FunctionClient] = []
        self._lock = threading.Lock()
        self._current_index = 0

        # Initialize clients for all available keys
        api_keys = config.api_keys
        for i, key in enumerate(api_keys):
            client = FunctionClient(api_key=key, model_id=config.api_model, base_url=config.api_base_url, layer_name="layer_2")
            self._clients.append(client)
            logger.info(f"Initialized API client {i+1}/{len(api_keys)} for Layer 2")

        logger.info(f"MultiKeyClientPool ready with {len(self._clients)} clients, "
                   f"total rate limit: {config.total_rate_limit} req/min")

    def get_client(self) -> FunctionClient:
        """Get the next client in round-robin fashion (thread-safe)."""
        with self._lock:
            client = self._clients[self._current_index]
            self._current_index = (self._current_index + 1) % len(self._clients)
            return client

    def __len__(self) -> int:
        return len(self._clients)


class Layer2Client:
    """
    Layer 2 API client.

    Supports both single-key and multi-key modes:
    - Single-key mode: Uses one API key (default, for backward compatibility)
    - Multi-key mode: Uses a client pool for high-throughput parallel processing
    """

    def __init__(self, config: Layer2Config, client_pool: Optional[MultiKeyClientPool] = None):
        self.config = config
        self._client_pool = client_pool
        self._single_client: Optional[FunctionClient] = None

        if client_pool:
            logger.info(f"Initialized Layer 2 with multi-key pool ({len(client_pool)} clients)")
        else:
            # Single-key mode (backward compatible)
            self._single_client = FunctionClient(api_key=config.api_key, model_id=config.api_model, base_url=config.api_base_url, layer_name="layer_2")
            logger.info(f"Initialized Layer 2 with model: {config.api_model}")

    @property
    def client(self) -> FunctionClient:
        """Get the client to use for the next request."""
        if self._client_pool:
            return self._client_pool.get_client()
        return self._single_client

    def generate_json(self, prompt: str, system_prompt: str = None) -> List[Dict[str, Any]]:
        """
        Generate JSON response.

        This method is used by the generator for preprocessing path generation.

        Args:
            prompt: The generation prompt
            system_prompt: Optional system prompt (currently not used by underlying client)

        Returns:
            Parsed JSON response as list of dicts
        """
        try:
            content = self.client.generate_text(
                prompt=prompt,
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens
            )

            if content:
                return self._parse_preprocessing_response(content)
            else:
                logger.warning("No content generated for JSON request")
                return []

        except Exception as e:
            logger.error(f"JSON generation failed: {e}")
            return []

    def generate_preprocessing_paths(self, prompt: str, num_paths: int = 10) -> List[Dict[str, Any]]:
        """Generate preprocessing paths."""
        try:
            # Enhanced prompt to force JSON output for thinking models like Qwen3
            enhanced_prompt = self._enhance_prompt_for_json_output(prompt)
            
            # Get both content and reasoning (Qwen3 puts reasoning in separate field)
            content, reasoning = self.client.generate_text_with_reasoning(
                prompt=enhanced_prompt,
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens
            )
            
            logger.debug(f"Content length: {len(content)}, Reasoning length: {len(reasoning)}")
            
            # Try to extract JSON from both fields
            if content and content.strip():
                # Primary: try content field first
                result = self._parse_preprocessing_response(content)
                if result:
                    return result
            
            if reasoning and reasoning.strip():
                # Secondary: try reasoning field (Qwen3 often puts JSON here)
                result = self._parse_preprocessing_response(reasoning)
                if result:
                    logger.info("Extracted JSON from reasoning field")
                    return result
                
                # Tertiary: try to extract from end of reasoning
                result = self._extract_json_from_reasoning_end(reasoning)
                if result:
                    logger.info("Extracted JSON from end of reasoning")
                    return result
            
            # Final fallback: extract from reasoning text (silent success)
            logger.debug(f"Using fallback extraction from reasoning text for {num_paths} paths")
            return self._extract_paths_from_reasoning_text(reasoning, num_paths)
                
        except Exception as e:
            logger.error(f"Layer 2 generation failed: {e}")
            return []

    def _enhance_prompt_for_json_output(self, original_prompt: str) -> str:
        """Enhance prompt to force JSON output for thinking models."""

        json_instruction = """

CRITICAL JSON OUTPUT REQUIREMENTS:
1. Your ENTIRE response must be a valid JSON array
2. Put the JSON directly in your response content field
3. Do NOT put JSON in the reasoning field
4. Do NOT include explanations or thinking in the content field
5. Start your response with '[' and end with ']'
6. Use double quotes for all strings

Example:
[{"preprocessing_path_id": "pp-001", "preprocessing_steps": ["step1"], "step_material_mapping": {}, "reasoning": ""}]
"""

        return original_prompt + json_instruction

    def _parse_preprocessing_response(self, content: str) -> List[Dict[str, Any]]:
        """Parse preprocessing response from text content."""
        import json
        import re

        try:
            # Try to parse as complete JSON first
            data = json.loads(content.strip())
            if isinstance(data, list):
                return [item for item in data if isinstance(item, dict)]
            elif isinstance(data, dict):
                return [data]
        except json.JSONDecodeError:
            pass

        # Try code blocks
        code_block_pattern = r'```(?:json)?\s*([\[\{].*?[\]\}])\s*```'
        matches = re.findall(code_block_pattern, content, re.DOTALL)
        for match in matches:
            try:
                data = json.loads(match)
                if isinstance(data, list):
                    return [item for item in data if isinstance(item, dict)]
                elif isinstance(data, dict):
                    return [data]
            except json.JSONDecodeError:
                continue

        return []

    def test_connection(self) -> bool:
        """Test API connection."""
        return self.client.test_connection()

    def health_check(self) -> bool:
        """Check if the API is working (alias for test_connection)."""
        return self.test_connection()

    def validate_response(self, content: str) -> bool:
        """Validate API response."""
        return bool(content and len(content.strip()) > 0)
