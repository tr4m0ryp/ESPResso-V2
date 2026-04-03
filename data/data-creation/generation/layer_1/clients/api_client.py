"""
Layer 1 API client
"""

import logging
from typing import Optional, Dict, Any, List

from ..config.config import Layer1Config
from ...shared.api_client import FunctionClient, APIError

logger = logging.getLogger(__name__)


class Layer1Client:
    """Layer 1 API client."""

    def __init__(self, config: Layer1Config):
        self.config = config
        self.client = FunctionClient(api_key=config.api_key, model_id=config.api_model, base_url=config.api_base_url, layer_name="layer_1")
        logger.info(f"Initialized Layer 1 with model: {config.api_model}")

    def generate_compositions(self, prompt: str) -> List[Dict[str, Any]]:
        """Generate product compositions."""
        try:
            content = self.client.generate_text(
                prompt=prompt,
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens
            )

            if content:
                # Parse the generated content as JSON
                return self._parse_composition_response(content)
            else:
                logger.warning("No content generated for Layer 1")
                return []

        except Exception as e:
            logger.error(f"Layer 1 generation failed: {e}")
            return []

    def generate_json(self, prompt: str, system_prompt: str = None) -> Dict[str, Any]:
        """Generate JSON response for single-stage and two-stage generation."""
        try:
            content = self.client.generate_text(
                prompt=prompt,
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens
            )

            if content:
                return self._parse_json_response(content)
            else:
                logger.warning("No content generated for JSON request")
                return {}

        except Exception as e:
            logger.error(f"JSON generation failed: {e}")
            return {}

    def generate_batch_json(self, prompt: str, system_prompt: str = None, max_tokens: int = None) -> List[Dict[str, Any]]:
        """
        Generate batch JSON response for multiple products.

        Returns a list of product dictionaries.
        """
        try:
            # Use higher max_tokens for batch generation
            tokens = max_tokens or self.config.max_tokens * 10

            # Use the new method that returns both content and reasoning
            content, reasoning = self.client.generate_text_with_reasoning(
                prompt=prompt,
                temperature=self.config.temperature,
                max_tokens=tokens
            )

            # Log what we received
            logger.debug(f"Batch response - content length: {len(content)}, reasoning length: {len(reasoning)}")

            # Try to parse from content first
            if content:
                result = self._parse_batch_json_response(content)
                if result:
                    logger.debug(f"Successfully parsed {len(result)} products from content field")
                    return result

            # If content parsing failed, try reasoning field
            if reasoning:
                logger.debug("Content parsing failed, trying reasoning field...")
                result = self._parse_batch_json_response(reasoning)
                if result:
                    logger.debug(f"Successfully parsed {len(result)} products from reasoning field")
                    return result

            # If both failed, try combining them
            if content or reasoning:
                combined = f"{content}\n{reasoning}".strip()
                logger.debug("Trying combined content+reasoning...")
                result = self._parse_batch_json_response(combined)
                if result:
                    logger.debug(f"Successfully parsed {len(result)} products from combined fields")
                    return result

            logger.warning("No content or reasoning generated for batch JSON request")
            return []

        except Exception as e:
            logger.error(f"Batch JSON generation failed: {e}")
            return []

    def _parse_batch_json_response(self, content: str) -> List[Dict[str, Any]]:
        """Parse batch JSON response containing multiple products."""
        import json
        import re

        if not content or not content.strip():
            logger.debug("Empty content received for parsing")
            return []

        # Debug: Log first 1000 chars of content to help diagnose issues
        logger.debug(f"Parsing batch response (first 1000 chars): {content[:1000] if content else 'EMPTY'}")

        try:
            # First, try direct JSON parsing
            try:
                data = json.loads(content.strip())
                if isinstance(data, dict) and "products" in data:
                    logger.debug(f"Direct JSON parse success: found {len(data['products'])} products")
                    return data["products"]
                elif isinstance(data, list):
                    logger.debug(f"Direct JSON parse success: found list of {len(data)} items")
                    return data
            except json.JSONDecodeError as e:
                logger.debug(f"Direct JSON parse failed: {e}")

            # Try to extract JSON from ```json blocks
            json_block_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```'
            matches = re.findall(json_block_pattern, content)
            for i, match in enumerate(matches):
                logger.debug(f"Trying JSON block {i+1}, length: {len(match)}")
                try:
                    data = json.loads(match.strip())
                    if isinstance(data, dict) and "products" in data:
                        logger.debug(f"JSON block {i+1} success: found {len(data['products'])} products")
                        return data["products"]
                    elif isinstance(data, list):
                        logger.debug(f"JSON block {i+1} success: found list of {len(data)} items")
                        return data
                except json.JSONDecodeError as e:
                    logger.debug(f"JSON block {i+1} parse failed: {e}")
                    continue

            # Look for {"products": [...]} pattern - use greedy matching for nested JSON
            products_pattern = r'\{\s*"products"\s*:\s*(\[[\s\S]*\])\s*\}'
            match = re.search(products_pattern, content)
            if match:
                logger.debug(f"Found products pattern, array length: {len(match.group(1))}")
                try:
                    data = json.loads(match.group(1))
                    if isinstance(data, list):
                        logger.debug(f"Products pattern success: found {len(data)} products")
                        return data
                except json.JSONDecodeError as e:
                    logger.debug(f"Products pattern parse failed: {e}")

            # Look for first complete JSON object or array in content
            # Find the first { or [ and try to parse from there
            for start_char, end_char in [('{', '}'), ('[', ']')]:
                start_idx = content.find(start_char)
                if start_idx != -1:
                    # Try to find matching end by counting brackets
                    depth = 0
                    for i, char in enumerate(content[start_idx:]):
                        if char == start_char:
                            depth += 1
                        elif char == end_char:
                            depth -= 1
                            if depth == 0:
                                json_str = content[start_idx:start_idx + i + 1]
                                logger.debug(f"Found balanced {start_char}{end_char} of length {len(json_str)}")
                                try:
                                    data = json.loads(json_str)
                                    if isinstance(data, dict) and "products" in data:
                                        return data["products"]
                                    elif isinstance(data, list) and len(data) > 0:
                                        return data
                                    elif isinstance(data, dict):
                                        # Single product dict
                                        return [data]
                                except json.JSONDecodeError as e:
                                    logger.debug(f"Balanced JSON parse failed: {e}")
                                break

            # Try to extract individual product objects
            # This handles cases where products are on separate lines
            individual_product_pattern = r'\{[^{}]*"category_id"[^{}]*"materials"[^{}]*\}'
            matches = re.findall(individual_product_pattern, content)
            if matches:
                logger.debug(f"Found {len(matches)} individual product patterns")
                products = []
                for match in matches:
                    try:
                        product = json.loads(match)
                        if isinstance(product, dict) and "category_id" in product:
                            products.append(product)
                    except json.JSONDecodeError:
                        continue
                if products:
                    logger.debug(f"Extracted {len(products)} individual products")
                    return products

            logger.warning(f"No valid batch JSON found in response (content length: {len(content)})")
            # Log more of the content for debugging
            if len(content) > 1000:
                logger.debug(f"Full content start: {content[:500]}...")
                logger.debug(f"Full content end: ...{content[-500:]}")
            return []

        except Exception as e:
            logger.error(f"Failed to parse batch JSON response: {e}")
            return []

    def _parse_json_response(self, content: str) -> Dict[str, Any]:
        """Parse JSON from text content, returning a single dict."""
        import json
        import re

        try:
            # First, try direct JSON parsing
            try:
                data = json.loads(content.strip())
                if isinstance(data, dict):
                    return data
                elif isinstance(data, list) and len(data) > 0:
                    return data[0] if isinstance(data[0], dict) else {}
            except json.JSONDecodeError:
                pass

            # Try to extract JSON object from content
            # Look for ```json blocks
            json_block_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```'
            matches = re.findall(json_block_pattern, content)
            for match in matches:
                try:
                    data = json.loads(match.strip())
                    if isinstance(data, dict):
                        return data
                    elif isinstance(data, list) and len(data) > 0:
                        return data[0] if isinstance(data[0], dict) else {}
                except json.JSONDecodeError:
                    continue

            # Look for JSON object pattern
            json_object_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
            matches = re.findall(json_object_pattern, content, re.DOTALL)

            for match in matches:
                try:
                    data = json.loads(match)
                    if isinstance(data, dict):
                        return data
                except json.JSONDecodeError:
                    continue

            # Look for JSON array pattern
            json_array_pattern = r'\[\s*\{.*?\}\s*\]'
            matches = re.findall(json_array_pattern, content, re.DOTALL)

            for match in matches:
                try:
                    data = json.loads(match)
                    if isinstance(data, list) and len(data) > 0:
                        return data[0] if isinstance(data[0], dict) else {}
                except json.JSONDecodeError:
                    continue

            logger.warning("No valid JSON found in response")
            return {}

        except Exception as e:
            logger.error(f"Failed to parse JSON response: {e}")
            return {}

    def _parse_composition_response(self, content: str) -> List[Dict[str, Any]]:
        """Parse composition response from text content."""
        import json
        
        try:
            # Try to extract JSON from the content
            # Look for JSON array pattern
            import re
            json_pattern = r'\[\s*\{.*?\}\s*\]'
            matches = re.findall(json_pattern, content, re.DOTALL)
            
            for match in matches:
                try:
                    data = json.loads(match)
                    if isinstance(data, list):
                        return data
                except json.JSONDecodeError:
                    continue
            
            
        except Exception as e:
            logger.error(f"Failed to parse Layer 1 response: {e}")
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