"""
Parallel Processing Utility for API Calls

Implements rate-limited parallel processing for API calls with:
- Pause/Resume functionality (press 'p' to pause, 'r' to resume)
- Automatic retry mechanism for connection failures
- Rate limit: 42 requests/minute = 1.43 seconds between requests
"""

import time
import logging
import threading
import sys
import select
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Callable, Any, Optional, Dict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import requests.exceptions
import urllib3.exceptions

logger = logging.getLogger(__name__)


class ProcessorState(Enum):
    """State of the parallel processor."""
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"


@dataclass
class RetryConfig:
    """Configuration for retry mechanism."""
    max_retries: int = 5
    initial_delay: float = 2.0  # seconds
    max_delay: float = 60.0  # seconds
    exponential_base: float = 2.0

    # Exceptions that trigger retry
    retryable_exceptions: tuple = (
        ConnectionError,
        TimeoutError,
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.ChunkedEncodingError,
        urllib3.exceptions.ProtocolError,
        urllib3.exceptions.NewConnectionError,
    )


@dataclass
class RateLimiter:
    """Thread-safe rate limiter for API calls."""
    requests_per_minute: int = 42
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _last_request_time: float = field(default=0.0)
    _request_count: int = field(default=0)
    _window_start: float = field(default=0.0)

    def __post_init__(self):
        self._min_interval = 60.0 / self.requests_per_minute
        self._window_start = time.time()

    def acquire(self) -> None:
        """Wait if necessary to respect rate limit."""
        with self._lock:
            now = time.time()

            # Reset window if a minute has passed
            if now - self._window_start >= 60.0:
                self._request_count = 0
                self._window_start = now

            # If we've hit the limit, wait until the window resets
            if self._request_count >= self.requests_per_minute:
                sleep_time = 60.0 - (now - self._window_start)
                if sleep_time > 0:
                    logger.debug(f"Rate limit reached, sleeping {sleep_time:.2f}s")
                    time.sleep(sleep_time)
                self._request_count = 0
                self._window_start = time.time()

            # Ensure minimum interval between requests
            elapsed = now - self._last_request_time
            if elapsed < self._min_interval:
                sleep_time = self._min_interval - elapsed
                time.sleep(sleep_time)

            self._last_request_time = time.time()
            self._request_count += 1


@dataclass
class ProcessingResult:
    """Result of a parallel processing task."""
    index: int
    input_data: Any
    output_data: Any = None
    success: bool = False
    error: Optional[str] = None
    duration_seconds: float = 0.0
    retries: int = 0


class PauseController:
    """
    Controls pause/resume functionality for the processor.

    Press 'p' to pause, 'r' to resume, 'q' to quit.
    """

    def __init__(self):
        self._state = ProcessorState.RUNNING
        self._lock = threading.Lock()
        self._pause_event = threading.Event()
        self._pause_event.set()  # Start in running state
        self._stop_requested = False
        self._input_thread: Optional[threading.Thread] = None

    def start_input_listener(self):
        """Start listening for keyboard input (non-blocking)."""
        if sys.stdin.isatty():
            self._input_thread = threading.Thread(target=self._listen_for_input, daemon=True)
            self._input_thread.start()
            logger.info("Pause controller active: Press 'p' to pause, 'r' to resume, 'q' to quit")

    def _listen_for_input(self):
        """Listen for keyboard input in a separate thread."""
        try:
            while not self._stop_requested:
                # Check if input is available (Unix-like systems)
                if hasattr(select, 'select'):
                    readable, _, _ = select.select([sys.stdin], [], [], 0.5)
                    if readable:
                        char = sys.stdin.read(1).lower()
                        self._handle_input(char)
                else:
                    # Fallback for Windows
                    time.sleep(0.5)
        except Exception:
            pass  # Silently handle input errors

    def _handle_input(self, char: str):
        """Handle a single character input."""
        if char == 'p':
            self.pause()
        elif char == 'r':
            self.resume()
        elif char == 'q':
            self.stop()

    def pause(self):
        """Pause processing."""
        with self._lock:
            if self._state == ProcessorState.RUNNING:
                self._state = ProcessorState.PAUSED
                self._pause_event.clear()
                logger.warning("\nPAUSED - Press 'r' to resume, 'q' to quit")
                print("\nPAUSED - Press 'r' to resume, 'q' to quit", flush=True)

    def resume(self):
        """Resume processing."""
        with self._lock:
            if self._state == ProcessorState.PAUSED:
                self._state = ProcessorState.RUNNING
                self._pause_event.set()
                logger.info("\nRESUMED - Processing continues...")
                print("\nRESUMED - Processing continues...", flush=True)

    def stop(self):
        """Request stop."""
        with self._lock:
            self._state = ProcessorState.STOPPED
            self._stop_requested = True
            self._pause_event.set()  # Unblock any waiting threads
            logger.warning("\nSTOP REQUESTED - Finishing current tasks...")
            print("\nSTOP REQUESTED - Finishing current tasks...", flush=True)

    def wait_if_paused(self) -> bool:
        """
        Wait if paused. Returns False if stop was requested.

        Returns:
            True if should continue, False if should stop
        """
        self._pause_event.wait()
        return not self._stop_requested

    @property
    def is_stopped(self) -> bool:
        """Check if stop was requested."""
        return self._stop_requested

    @property
    def state(self) -> ProcessorState:
        """Get current state."""
        return self._state

    def cleanup(self):
        """Cleanup resources."""
        self._stop_requested = True
        self._pause_event.set()


class ParallelProcessor:
    """
    Parallel processor with rate limiting, pause/resume, and retry for API calls.

    Usage:
        processor = ParallelProcessor(max_workers=4, requests_per_minute=42)
        results = processor.process_batch(
            items=["item1", "item2", ...],
            process_func=my_api_call_function
        )

    Features:
        - Press 'p' to pause processing
        - Press 'r' to resume processing
        - Press 'q' to quit gracefully
        - Automatic retry on connection failures
    """

    def __init__(
        self,
        max_workers: int = 4,
        requests_per_minute: int = 42,
        show_progress: bool = True,
        retry_config: Optional[RetryConfig] = None,
        enable_pause: bool = True
    ):
        self.max_workers = max_workers
        self.rate_limiter = RateLimiter(requests_per_minute=requests_per_minute)
        self.show_progress = show_progress
        self.retry_config = retry_config or RetryConfig()
        self.enable_pause = enable_pause
        self.pause_controller = PauseController() if enable_pause else None

        self._stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "retries": 0,
            "start_time": None,
            "end_time": None
        }

    def _execute_with_retry(
        self,
        func: Callable,
        item: Any,
        index: int
    ) -> tuple[Any, int, Optional[str]]:
        """
        Execute a function with retry logic for connection failures.

        Returns:
            Tuple of (result, retry_count, error_message)
        """
        last_error = None
        retry_count = 0

        for attempt in range(self.retry_config.max_retries + 1):
            try:
                # Check if we should continue
                if self.pause_controller and not self.pause_controller.wait_if_paused():
                    return None, retry_count, "Processing stopped by user"

                result = func(item)
                return result, retry_count, None

            except self.retry_config.retryable_exceptions as e:
                last_error = str(e)
                retry_count = attempt + 1

                if attempt < self.retry_config.max_retries:
                    # Calculate delay with exponential backoff
                    delay = min(
                        self.retry_config.initial_delay * (self.retry_config.exponential_base ** attempt),
                        self.retry_config.max_delay
                    )

                    logger.warning(
                        f"Connection error on item {index} (attempt {attempt + 1}/{self.retry_config.max_retries + 1}): {e}"
                    )
                    logger.info(f"Retrying in {delay:.1f}s...")

                    # Wait with pause check
                    sleep_end = time.time() + delay
                    while time.time() < sleep_end:
                        if self.pause_controller:
                            if not self.pause_controller.wait_if_paused():
                                return None, retry_count, "Processing stopped by user"
                        time.sleep(min(0.5, sleep_end - time.time()))
                else:
                    logger.error(f"Max retries exceeded for item {index}: {e}")

            except Exception as e:
                # Non-retryable exception
                return None, retry_count, str(e)

        return None, retry_count, f"Max retries exceeded: {last_error}"

    def process_batch(
        self,
        items: List[Any],
        process_func: Callable[[Any], Any],
        on_complete: Optional[Callable[[ProcessingResult], None]] = None
    ) -> List[ProcessingResult]:
        """
        Process a batch of items in parallel with rate limiting.

        Args:
            items: List of items to process
            process_func: Function to call for each item (should make the API call)
            on_complete: Optional callback for each completed item

        Returns:
            List of ProcessingResult objects
        """
        self._stats = {
            "total": len(items),
            "success": 0,
            "failed": 0,
            "retries": 0,
            "start_time": datetime.now(),
            "end_time": None
        }

        results: List[ProcessingResult] = [None] * len(items)

        # Start pause controller
        if self.pause_controller:
            self.pause_controller.start_input_listener()

        def process_with_rate_limit(index: int, item: Any) -> ProcessingResult:
            """Process a single item with rate limiting and retry."""
            # Check pause state before acquiring rate limit
            if self.pause_controller and not self.pause_controller.wait_if_paused():
                return ProcessingResult(
                    index=index,
                    input_data=item,
                    success=False,
                    error="Processing stopped by user"
                )

            self.rate_limiter.acquire()

            start_time = time.time()
            output, retries, error = self._execute_with_retry(process_func, item, index)
            duration = time.time() - start_time

            if error is None:
                result = ProcessingResult(
                    index=index,
                    input_data=item,
                    output_data=output,
                    success=True,
                    duration_seconds=duration,
                    retries=retries
                )
                self._stats["success"] += 1
            else:
                result = ProcessingResult(
                    index=index,
                    input_data=item,
                    success=False,
                    error=error,
                    duration_seconds=duration,
                    retries=retries
                )
                self._stats["failed"] += 1

            self._stats["retries"] += retries

            if on_complete:
                on_complete(result)

            return result

        try:
            # Process in parallel with ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(process_with_rate_limit, i, item): i
                    for i, item in enumerate(items)
                }

                completed = 0
                for future in as_completed(futures):
                    # Check if stop was requested
                    if self.pause_controller and self.pause_controller.is_stopped:
                        logger.warning("Stop requested, cancelling remaining tasks...")
                        for f in futures:
                            f.cancel()
                        break

                    result = future.result()
                    results[result.index] = result
                    completed += 1

                    if self.show_progress and completed % 10 == 0:
                        elapsed = (datetime.now() - self._stats["start_time"]).total_seconds()
                        rate = completed / elapsed if elapsed > 0 else 0
                        logger.info(
                            f"Progress: {completed}/{len(items)} "
                            f"({100*completed/len(items):.1f}%) - "
                            f"{rate:.1f} items/sec - "
                            f"Retries: {self._stats['retries']}"
                        )
        finally:
            if self.pause_controller:
                self.pause_controller.cleanup()

        self._stats["end_time"] = datetime.now()

        # Filter out None results (from cancelled tasks)
        return [r for r in results if r is not None]

    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        stats = self._stats.copy()
        if stats["start_time"] and stats["end_time"]:
            duration = (stats["end_time"] - stats["start_time"]).total_seconds()
            stats["duration_seconds"] = duration
            stats["items_per_second"] = stats["total"] / duration if duration > 0 else 0
        return stats


# Convenience function for simple parallel API calls
def parallel_api_calls(
    items: List[Any],
    api_func: Callable[[Any], Any],
    max_workers: int = 4,
    requests_per_minute: int = 42,
    enable_retry: bool = True,
    enable_pause: bool = True
) -> List[ProcessingResult]:
    """
    Simple wrapper for parallel API calls with rate limiting.

    Args:
        items: List of items to process
        api_func: Function to call for each item
        max_workers: Number of parallel workers
        requests_per_minute: API rate limit
        enable_retry: Enable retry on connection failures
        enable_pause: Enable pause/resume functionality

    Returns:
        List of ProcessingResult objects
    """
    processor = ParallelProcessor(
        max_workers=max_workers,
        requests_per_minute=requests_per_minute,
        retry_config=RetryConfig() if enable_retry else None,
        enable_pause=enable_pause
    )
    return processor.process_batch(items, api_func)
