"""
API Client for Layer 5: Multi-Key with Infinite Retry

Handles cross-layer coherence evaluation (50-record batches) and
quality scoring using a single configurable instruct model.

Features:
- Multi-API key rotation with per-key rate limiting
- Sliding window rate limiter
- Infinite retry until successful generation (no data loss)
- Thinking mode disabled via system prompts
"""

import logging
import re
import threading
import time
from collections import defaultdict
from typing import Dict, Any, List, Optional

import requests

from data.data_generation.layer_5.config.config import Layer5Config

logger = logging.getLogger(__name__)


class MultiKeyRateLimiter:
    """Thread-safe rate limiter for multiple API keys.

    Each key has an independent rate limit (configurable requests per minute).
    Automatically selects the key with available capacity via round-robin.
    """

    def __init__(self, api_keys: List[str], requests_per_minute: int = 40):
        """Initialize rate limiter with multiple API keys.

        Args:
            api_keys: List of API keys to rotate between
            requests_per_minute: Rate limit per key (default 40)
        """
        self.api_keys = api_keys
        self.requests_per_minute = requests_per_minute
        self._lock = threading.Lock()
        self._key_timestamps: Dict[str, List[float]] = defaultdict(list)
        self._current_key_index = 0

    def acquire_key(self) -> str:
        """Acquire an API key with available rate limit capacity.

        Returns:
            API key that can be used for the next request

        Note:
            This method blocks until a key is available.
        """
        with self._lock:
            while True:
                now = time.time()
                window_start = now - 60.0

                # Try each key in round-robin fashion
                for _ in range(len(self.api_keys)):
                    key = self.api_keys[self._current_key_index]
                    self._current_key_index = (
                        (self._current_key_index + 1) % len(self.api_keys)
                    )

                    # Clean old timestamps outside the window
                    self._key_timestamps[key] = [
                        ts for ts in self._key_timestamps[key]
                        if ts > window_start
                    ]

                    # Check if this key has capacity
                    if len(self._key_timestamps[key]) < self.requests_per_minute:
                        self._key_timestamps[key].append(now)
                        logger.debug(
                            f"Using key {self.api_keys.index(key) + 1}, "
                            f"requests in window: {len(self._key_timestamps[key])}"
                        )
                        return key

                # All keys exhausted, wait and retry
                oldest_timestamps = []
                for key in self.api_keys:
                    if self._key_timestamps[key]:
                        oldest_timestamps.append(
                            min(self._key_timestamps[key])
                        )

                if oldest_timestamps:
                    oldest = min(oldest_timestamps)
                    wait_time = max(0.1, oldest + 60.0 - now)
                    logger.debug(
                        f"All keys at rate limit, waiting {wait_time:.1f}s"
                    )
                    # Release lock while waiting
                    self._lock.release()
                    try:
                        time.sleep(wait_time)
                    finally:
                        self._lock.acquire()
                else:
                    time.sleep(0.1)

    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        with self._lock:
            now = time.time()
            window_start = now - 60.0
            stats = {
                "total_keys": len(self.api_keys),
                "requests_per_minute_per_key": self.requests_per_minute,
                "key_usage": {}
            }
            for i, key in enumerate(self.api_keys):
                active_requests = len([
                    ts for ts in self._key_timestamps[key]
                    if ts > window_start
                ])
                stats["key_usage"][f"key_{i + 1}"] = active_requests
            return stats


class Layer5Client:
    """Layer 5 API client with multi-key rotation and infinite retry."""

    def __init__(self, config: Layer5Config):
        """Initialize client with configuration.

        Args:
            config: Layer5Config instance with API settings
        """
        self.config = config
        self.api_keys = config.api_keys
        self.base_url = config.api_base_url
        self.model_instruct = config.api_model_instruct
        # Initialize multi-key rate limiter
        if self.api_keys:
            self.rate_limiter = MultiKeyRateLimiter(
                api_keys=self.api_keys,
                requests_per_minute=config.rate_limit_per_key
            )
            logger.info(
                f"Initialized Layer 5 with {len(self.api_keys)} API keys, "
                f"{config.rate_limit_per_key} req/min per key = "
                f"{config.total_rate_limit} req/min total"
            )
        else:
            self.rate_limiter = None
            logger.warning("No API keys configured for Layer 5")

        # Separate sessions for each key for better connection pooling
        self._sessions: Dict[str, requests.Session] = {}
        for key in self.api_keys:
            session = requests.Session()
            session.headers.update({
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json"
            })
            self._sessions[key] = session

        logger.info(
            f"Layer 5 model: instruct={self.model_instruct}"
        )

    def _get_session(self, api_key: str) -> requests.Session:
        """Get session for a specific API key."""
        return self._sessions.get(api_key, requests.Session())

    def generate_batch_coherence_evaluation(
        self,
        prompt: str,
        temperature: float = 0.3,
        max_tokens: int = 4096
    ) -> Optional[str]:
        """Generate batch coherence evaluation with infinite retry until success.

        Evaluates cross-layer coherence for 50-record batches.

        Args:
            prompt: The prompt containing multiple records for coherence evaluation
            temperature: Sampling temperature
            max_tokens: Maximum tokens in response (increased for batch)

        Returns:
            Evaluation response string, or None if no API keys configured
        """
        if not self.api_keys:
            logger.warning("No API key configured - skipping generation")
            return None

        attempt = 0
        while True:
            try:
                attempt += 1
                api_key = self.rate_limiter.acquire_key()
                logger.debug(f"Batch coherence eval attempt {attempt}")

                response = self._call_chat_api(
                    api_key=api_key,
                    prompt=prompt,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    model_id=self.model_instruct,
                    system_prompt=self._get_instruct_system_prompt()
                )

                if response and response.strip():
                    logger.debug(
                        f"Generated batch coherence evaluation "
                        f"in {attempt} attempt(s)"
                    )
                    return response

                logger.warning(
                    f"Attempt {attempt}: Empty response, retrying..."
                )
                time.sleep(min(2 ** min(attempt, 6), 60))

            except Exception as e:
                logger.warning(
                    f"Error on attempt {attempt}: {e}. Retrying..."
                )
                time.sleep(min(2 ** min(attempt, 6), 60))


    def generate_reward_score(
        self,
        context: str,
        temperature: float = 0.1,
        max_tokens: int = 200
    ) -> Optional[float]:
        """Generate reward score with infinite retry until success.

        Args:
            context: Product context for scoring
            temperature: Sampling temperature
            max_tokens: Maximum tokens in response

        Returns:
            Reward score between 0.0 and 1.0, or None if no API keys configured
        """
        if not self.api_keys:
            logger.warning("No API key configured - skipping generation")
            return None

        attempt = 0
        while True:
            try:
                attempt += 1
                api_key = self.rate_limiter.acquire_key()
                logger.debug(f"Reward score attempt {attempt}")

                reward_prompt = self._build_reward_prompt(context)

                response = self._call_chat_api(
                    api_key=api_key,
                    prompt=reward_prompt,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    model_id=self.model_instruct,
                    system_prompt=self._get_reward_system_prompt()
                )

                if response:
                    score = self._extract_reward_score(response)
                    if score is not None:
                        logger.debug(
                            f"Generated reward score in {attempt} attempt(s): "
                            f"{score}"
                        )
                        return score
                    logger.warning(
                        f"Attempt {attempt}: Could not extract score, retrying..."
                    )
                else:
                    logger.warning(
                        f"Attempt {attempt}: No response, retrying..."
                    )

                time.sleep(min(2 ** min(attempt, 6), 60))

            except Exception as e:
                logger.warning(
                    f"Error on attempt {attempt}: {e}. Retrying..."
                )
                time.sleep(min(2 ** min(attempt, 6), 60))

    def _call_chat_api(
        self,
        api_key: str,
        prompt: str,
        temperature: float,
        max_tokens: int,
        model_id: str,
        system_prompt: str
    ) -> Optional[str]:
        """Call chat API with specified key and settings.

        Args:
            api_key: API key to use
            prompt: User prompt
            temperature: Sampling temperature
            max_tokens: Maximum tokens
            model_id: Model identifier
            system_prompt: System prompt for the model

        Returns:
            Response content string or None on error
        """
        # Build messages array with system + user format
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ]
        
        payload = {
            "model": model_id,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens
        }

        session = self._get_session(api_key)

        try:
            response = session.post(
                f"{self.base_url}/chat/completions",
                json=payload,
                timeout=90
            )
            response.raise_for_status()

            result = response.json()

            usage = result.get("usage", {})
            if usage:
                try:
                    from data.data_generation.shared.token_tracker import get_tracker
                    tracker = get_tracker("layer_5")
                    if tracker:
                        tracker.record_usage(usage)
                except Exception:
                    pass

            if 'choices' in result and result['choices']:
                message = result['choices'][0].get('message', {})

                # Try reasoning_content first (for reasoning models)
                content = message.get('reasoning_content', '')
                if content:
                    content = self._strip_thinking_tags(content)
                    if content:
                        return content

                # Then try regular content
                content = message.get('content', '')
                if content:
                    content = self._strip_thinking_tags(content)
                    return content

            return None

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                logger.warning("Rate limit hit (429), will retry with backoff")
            raise
        except Exception as e:
            logger.error(f"Chat API call failed: {e}")
            raise

    def _get_instruct_system_prompt(self) -> str:
        """Get system prompt for instruct model."""
        return """You are a cross-layer coherence evaluator for textile product data.

CRITICAL RULES:
1. DO NOT use thinking tags (<think>...</think>)
2. DO NOT validate individual fields (weights, distances, percentages)
3. Output ONLY the JSON result
4. Focus on cross-layer coherence and lifecycle plausibility
5. Be direct and concise

Your response must be immediately usable without parsing thinking tags."""

    def _get_reward_system_prompt(self) -> str:
        """Get system prompt for reward model."""
        return """You are a quality assessment expert.

CRITICAL RULES:
1. DO NOT use thinking tags
2. Output ONLY the score and brief justification
3. Be direct and concise
4. Score format: "Score: 0.XX\""""

    def _strip_thinking_tags(self, text: str) -> str:
        """Strip thinking tags from response."""
        if not text:
            return ""
        text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
        text = re.sub(r'<thinking>.*?</thinking>', '', text, flags=re.DOTALL)
        return text.strip()

    def _build_reward_prompt(self, context: str) -> str:
        """Build prompt for reward scoring."""
        return f"""Evaluate the quality and realism of this textile product data \
for carbon footprint modeling. Consider material composition, processing steps, \
transport scenarios, and packaging configuration.

PRODUCT DATA:
{context}

PROVIDE A QUALITY SCORE (0.0 to 1.0) AND BRIEF JUSTIFICATION:
- 0.8-1.0: High quality, very realistic and useful for training
- 0.6-0.79: Acceptable quality, minor issues
- 0.4-0.59: Marginal quality, some concerns
- Below 0.4: Low quality, significant problems

OUTPUT FORMAT:
Score: 0.XX
Justification: Brief explanation of score

Evaluate this product data."""

    def _extract_reward_score(self, response: str) -> Optional[float]:
        """Extract reward score from API response."""
        # Look for score pattern
        score_pattern = r'[Ss]core\s*:\s*([0-9.]+)'
        match = re.search(score_pattern, response)

        if match:
            try:
                score = float(match.group(1))
                return max(0.0, min(1.0, score))
            except ValueError:
                pass

        # Try to find any decimal number between 0 and 1
        number_pattern = r'\b(0?\.[0-9]+)\b'
        matches = re.findall(number_pattern, response)

        for m in matches:
            try:
                score = float(m)
                if 0.0 <= score <= 1.0:
                    return score
            except ValueError:
                continue

        return None

    def test_connection(self) -> bool:
        """Test API connection with the first available key."""
        if not self.api_keys:
            logger.error("No API keys available for connection test")
            return False

        try:
            test_prompt = (
                "Evaluate this simple textile product: cotton t-shirt, "
                "0.25kg, basic processing, 5000km transport."
            )
            api_key = self.api_keys[0]
            response = self._call_chat_api(
                api_key=api_key,
                prompt=test_prompt,
                temperature=0.3,
                max_tokens=200,
                model_id=self.model_instruct,
                system_prompt=self._get_instruct_system_prompt()
            )
            return response is not None and len(response.strip()) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def health_check(self) -> bool:
        """Check if the API is working (alias for test_connection)."""
        return self.test_connection()

    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the current models."""
        return {
            "instruct_model": self.model_instruct,
            "api_type": "multi_key_chat",
            "base_url": self.base_url,
            "num_api_keys": len(self.api_keys),
            "rate_limit_per_key": self.config.rate_limit_per_key,
            "total_rate_limit": self.config.total_rate_limit
        }

    def get_rate_limiter_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        if self.rate_limiter:
            return self.rate_limiter.get_stats()
        return {"error": "No rate limiter configured"}
