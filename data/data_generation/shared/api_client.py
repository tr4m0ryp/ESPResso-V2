"""
Unified API client for all layers

Uses the chat completions API compatible with any OpenAI-compatible endpoint.
"""

import json
import re
import time
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
import requests

logger = logging.getLogger(__name__)


@dataclass
class APIResponse:
    """Response from the API."""
    content: str
    reasoning: str
    finish_reason: str
    usage: Dict[str, int]
    raw_response: Dict[str, Any]


class APIError(Exception):
    """Exception raised for API errors."""
    def __init__(self, message: str, status_code: Optional[int] = None, response: Optional[Dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class FunctionClient:
    """Unified API client that works with any OpenAI-compatible endpoint."""

    def __init__(self, api_key: str, model_id: str = None, base_url: str = "http://localhost:3000/v1", layer_name: str = "unknown"):
        self.api_key = api_key
        self.model_id = model_id
        self.base_url = base_url
        self.layer_name = layer_name
        self.session = requests.Session()
        self._setup_session()
        self._function_ids = self._resolve_models()

    def _setup_session(self) -> None:
        """Setup session with authentication headers."""
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=600,
            pool_maxsize=600,
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        headers = {"Content-Type": "application/json"}
        if self.api_key and self.api_key not in ("", "uva-local"):
            headers["Authorization"] = f"Bearer {self.api_key}"
        self.session.headers.update(headers)

    def _resolve_models(self) -> Dict[str, str]:
        """Resolve model mappings for use cases."""
        function_map = {}

        if self.model_id:
            logger.info(f"Using explicitly configured model: {self.model_id}")
            function_map['text_generation'] = self.model_id
            function_map['complex_reasoning'] = self.model_id
            return function_map

        logger.info("No explicit model configured, using default model")
        return {
            'text_generation': 'claude-sonnet-4-6',
            'complex_reasoning': 'claude-sonnet-4-6'
        }

    def generate_text(self, prompt: str, temperature: float = 0.7, max_tokens: int = 500) -> str:
        """Generate text content."""
        try:
            response = self._call_model(
                function_type='text_generation',
                prompt=prompt,
                temperature=temperature,
                max_tokens=max_tokens
            )
            logger.debug(f"Raw response content (first 500): {response.content[:500] if response.content else 'EMPTY'}")
            logger.debug(f"Raw response reasoning (first 500): {response.reasoning[:500] if response.reasoning else 'EMPTY'}")
            return self._extract_text_content(response)
        except Exception as e:
            logger.error(f"Text generation failed: {e}")
            return ""

    def generate_text_with_reasoning(self, prompt: str, temperature: float = 0.7, max_tokens: int = 500) -> Tuple[str, str]:
        """
        Generate text content and return both content and reasoning fields.

        Returns:
            Tuple of (content, reasoning) strings
        """
        try:
            response = self._call_model(
                function_type='text_generation',
                prompt=prompt,
                temperature=temperature,
                max_tokens=max_tokens
            )
            content = response.content.strip() if response.content else ""
            reasoning = response.reasoning.strip() if response.reasoning else ""
            logger.debug(f"Response content length: {len(content)}, reasoning length: {len(reasoning)}")
            return content, reasoning
        except Exception as e:
            logger.error(f"Text generation with reasoning failed: {e}")
            return "", ""

    def generate_complex_scenarios(self, prompt: str, temperature: float = 0.6, max_tokens: int = 800) -> List[Dict[str, Any]]:
        """Generate complex scenarios with reasoning."""
        try:
            response = self._call_model(
                function_type='complex_reasoning',
                prompt=prompt,
                temperature=temperature,
                max_tokens=max_tokens
            )
            return self._extract_json_from_response(response)
        except Exception as e:
            logger.error(f"Complex scenario generation failed: {e}")
            return []

    def _call_model(self, function_type: str, prompt: str, temperature: float, max_tokens: int) -> APIResponse:
        """Call the configured model via chat completions API."""
        model_id = self._function_ids.get(function_type)

        if not model_id:
            raise APIError(f"No model available for type: {function_type}")

        url = f"{self.base_url}/chat/completions"

        effective_max_tokens = max(max_tokens, 3000)

        payload = {
            "model": model_id,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a supply chain expert. Output ONLY a valid JSON array with no explanation or thinking tags. Start with [ and end with ]. The JSON must be complete and parseable."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": temperature,
            "max_tokens": effective_max_tokens
        }

        return self._make_api_call(url, payload)

    def _make_api_call(self, url: str, payload: Dict) -> APIResponse:
        """Make the actual API call with retry logic."""
        max_retries = 3
        retry_delay = 2.0

        for attempt in range(max_retries):
            try:
                api_response = self.session.post(
                    url,
                    json=payload,
                    timeout=300
                )
                api_response.raise_for_status()

                data = api_response.json()

                choice = data["choices"][0] if data.get("choices") else {}
                message = choice.get("message", {})
                content = message.get("content", "")
                reasoning = message.get("reasoning_content", "")

                finish_reason = choice.get("finish_reason", "stop")
                usage = data.get("usage", {})

                logger.debug(f"API call successful. Usage: {usage}")

                if usage:
                    try:
                        from .token_tracker import get_tracker
                        tracker = get_tracker(self.layer_name)
                        if tracker:
                            tracker.record_usage(usage)
                    except Exception:
                        pass

                return APIResponse(
                    content=content,
                    reasoning=reasoning,
                    finish_reason=finish_reason,
                    usage=usage,
                    raw_response=data
                )

            except requests.exceptions.RequestException as e:
                is_rate_limit = False
                if hasattr(e, 'response') and e.response is not None:
                    if e.response.status_code == 429:
                        is_rate_limit = True
                        logger.warning(f"Rate limit hit (429) on attempt {attempt + 1}")

                if attempt < max_retries - 1:
                    base_delay = retry_delay * (2 ** attempt)
                    if is_rate_limit:
                        base_delay *= 2
                    import random
                    jitter = random.uniform(0, 0.1 * base_delay)
                    sleep_time = base_delay + jitter
                    logger.info(f"Retrying in {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                else:
                    error_type = "rate limit" if is_rate_limit else "API"
                    raise APIError(f"{error_type} errors after {max_retries} attempts: {e}")

            except (KeyError, IndexError) as e:
                logger.error(f"Unexpected API response format: {e}")
                raise APIError(f"Invalid response format: {e}")

    def _strip_thinking_tags(self, text: str) -> str:
        """Strip <think>...</think> tags from text (used by thinking models).

        Handles both closed tags and unclosed tags (when response is truncated).
        """
        if not text:
            return ""
        stripped = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
        stripped = re.sub(r'<thinking>.*?</thinking>', '', stripped, flags=re.DOTALL)
        stripped = re.sub(r'\[think\].*?\[/think\]', '', stripped, flags=re.DOTALL)
        stripped = re.sub(r'Thinking:.*?\n', '', stripped, flags=re.DOTALL)

        if '<think>' in stripped and '</think>' not in stripped:
            stripped = re.sub(r'<think>.*$', '', stripped, flags=re.DOTALL)
        if '<thinking>' in stripped and '</thinking>' not in stripped:
            stripped = re.sub(r'<thinking>.*$', '', stripped, flags=re.DOTALL)

        return stripped.strip()

    def _extract_text_content(self, response: APIResponse) -> str:
        """Extract text content from response."""
        if response.content:
            return self._strip_thinking_tags(response.content)
        elif response.reasoning:
            return self._strip_thinking_tags(response.reasoning)
        else:
            logger.warning("No content found in response")
            return ""

    def _extract_json_from_response(self, response: APIResponse) -> List[Dict[str, Any]]:
        """Extract JSON from response."""

        logger.debug(f"Extracting JSON - Content length: {len(response.content) if response.content else 0}")
        logger.debug(f"Extracting JSON - Reasoning length: {len(response.reasoning) if response.reasoning else 0}")

        if response.content:
            content = self._strip_thinking_tags(response.content)
            logger.debug(f"Content after stripping thinking tags: {content[:200]}...")
            json_data = self._parse_json_content(content)
            if json_data:
                logger.debug(f"Successfully extracted JSON from content: {len(json_data)} items")
                return json_data

        if response.reasoning:
            reasoning = self._strip_thinking_tags(response.reasoning)
            logger.debug(f"Reasoning after stripping thinking tags: {reasoning[:200]}...")
            json_data = self._parse_json_content(reasoning)
            if json_data:
                logger.debug(f"Successfully extracted JSON from reasoning: {len(json_data)} items")
                return json_data
            json_data = self._parse_json_from_reasoning(reasoning)
            if json_data:
                logger.debug(f"Successfully extracted JSON from reasoning parser: {len(json_data)} items")
                return json_data

        combined_text = self._strip_thinking_tags(f"{response.content or ''} {response.reasoning or ''}")
        logger.debug(f"Combined text after stripping: {combined_text[:200]}...")
        json_data = self._extract_any_json(combined_text)
        if json_data:
            logger.debug(f"Successfully extracted JSON from combined text: {len(json_data)} items")
            return json_data

        if response.content:
            logger.debug("Attempting fallback JSON extraction from raw content")
            json_data = self._extract_any_json(response.content)
            if json_data:
                logger.debug(f"Successfully extracted JSON from raw content: {len(json_data)} items")
                return json_data

            json_data = self._extract_json_array_from_text(response.content)
            if json_data:
                logger.debug(f"Successfully extracted JSON array from text: {len(json_data)} items")
                return json_data

        logger.warning("Failed to extract JSON from response")
        logger.warning(f"Raw content preview: {response.content[:300] if response.content else 'EMPTY'}")
        logger.warning(f"Raw reasoning preview: {response.reasoning[:300] if response.reasoning else 'EMPTY'}")
        return []

    def _parse_json_content(self, content: str) -> Optional[List[Dict[str, Any]]]:
        """Parse JSON from content field."""
        if not content or content.strip() == "":
            return None

        try:
            data = json.loads(content.strip())
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                for key in ['transport_scenarios', 'scenarios', 'data', 'result', 'results']:
                    if key in data and isinstance(data[key], list):
                        logger.debug(f"Extracted array from key '{key}' in object wrapper")
                        return data[key]
                return [data]
            else:
                return None
        except json.JSONDecodeError as e:
            logger.debug(f"JSON parse error: {e}")
            return None

    def _parse_json_from_reasoning(self, reasoning: str) -> Optional[List[Dict[str, Any]]]:
        """Extract JSON from reasoning content."""
        if not reasoning or reasoning.strip() == "":
            return None

        json_array_pattern = r'\[\s*\{.*?\}\s*\]'
        matches = re.findall(json_array_pattern, reasoning, re.DOTALL)

        for match in matches:
            try:
                data = json.loads(match)
                if isinstance(data, list):
                    return data
            except json.JSONDecodeError:
                continue

        json_object_pattern = r'\{\s*".*?"\s*:\s*.*?\s*\}'
        matches = re.findall(json_object_pattern, reasoning, re.DOTALL)

        if matches:
            objects = []
            for match in matches:
                try:
                    obj = json.loads(match)
                    objects.append(obj)
                except json.JSONDecodeError:
                    continue
            return objects if objects else None

        return None

    def _extract_any_json(self, text: str) -> Optional[List[Dict[str, Any]]]:
        """Extract any JSON-like structures from text."""
        text_clean = text.strip()

        text_clean = re.sub(r'(\w+):', r'"\1":', text_clean)
        text_clean = re.sub(r':\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*([,}])', r': "\1"\2', text_clean)

        json_patterns = [
            r'\[\s*\{.*?\}\s*\]',
            r'\{\s*".*?"\s*:\s*.*?\s*\}',
        ]

        for pattern in json_patterns:
            matches = re.findall(pattern, text_clean, re.DOTALL)
            if matches:
                for match in matches:
                    try:
                        data = json.loads(match)
                        if isinstance(data, list):
                            return data
                        elif isinstance(data, dict):
                            return [data]
                    except json.JSONDecodeError:
                        continue

        return None

    def _extract_json_array_from_text(self, text: str) -> Optional[List[Dict[str, Any]]]:
        """Extract JSON array from text by finding balanced brackets."""
        if not text:
            return None

        start_idx = text.find('[')
        if start_idx == -1:
            return None

        depth = 0
        end_idx = -1

        for i in range(start_idx, len(text)):
            if text[i] == '[':
                depth += 1
            elif text[i] == ']':
                depth -= 1
                if depth == 0:
                    end_idx = i
                    break

        if end_idx == -1:
            json_str = text[start_idx:]
            open_brackets = json_str.count('[') - json_str.count(']')
            open_braces = json_str.count('{') - json_str.count('}')
            json_str += '}' * open_braces + ']' * open_brackets
        else:
            json_str = text[start_idx:end_idx + 1]

        try:
            data = json.loads(json_str)
            if isinstance(data, list):
                return data
            return None
        except json.JSONDecodeError:
            return None

    def test_connection(self) -> bool:
        """Test API connection."""
        try:
            test_prompt = "Generate one transport scenario for a cotton t-shirt."
            response = self._call_model('text_generation', test_prompt, 0.7, 200)
            content = self._extract_text_content(response)
            return len(content.strip()) > 0
        except Exception as e:
            logger.error(f"API connection test failed: {e}")
            return False

    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the current models."""
        model_id = self._function_ids.get('text_generation', self.model_id or 'unknown')
        return {
            "api_type": "function_based",
            "model_type": "api_client",
            "model": model_id,
            "function_id": model_id,
            "available_functions": self._function_ids,
            "extraction_method": "content_and_reasoning_parsing"
        }
