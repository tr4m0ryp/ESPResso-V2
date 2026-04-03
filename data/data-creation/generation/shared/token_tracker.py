"""Persistent token usage tracker for the data generation pipeline.

Accumulates prompt/completion token counts across all API calls,
broken down by layer.  Flushes to disk periodically (every 50 calls
or 30 seconds) using atomic writes and cross-process file locking.

Usage:
    from shared.token_tracker import get_tracker
    tracker = get_tracker("layer_3")
    tracker.record_usage({"prompt_tokens": 100, "completion_tokens": 50,
                          "total_tokens": 150})
"""

import atexit
import fcntl
import json
import logging
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

_FLUSH_INTERVAL = 30.0  # seconds
_FLUSH_CALL_COUNT = 50

_EMPTY_COUNTERS = {
    "prompt_tokens": 0,
    "completion_tokens": 0,
    "total_tokens": 0,
    "api_calls": 0,
}


def _empty_store() -> Dict:
    return {
        "all_time": dict(_EMPTY_COUNTERS),
        "per_layer": {},
        "sessions": [],
    }


class TokenTracker:
    """Thread-safe, disk-persistent token usage tracker."""

    def __init__(self, layer_name: str, storage_path: Path):
        self._layer = layer_name
        self._path = storage_path
        self._lock_path = storage_path.with_suffix(".lock")

        # In-memory delta since last flush
        self._delta = dict(_EMPTY_COUNTERS)
        self._mu = threading.Lock()
        self._calls_since_flush = 0
        self._last_flush = time.monotonic()

        # Session bookkeeping
        self._session_id = (
            datetime.now().strftime("%Y%m%d_%H%M%S") + f"_{layer_name}"
        )
        self._session_start = datetime.now().isoformat(timespec="seconds")
        self._session_counters = dict(_EMPTY_COUNTERS)

        atexit.register(self._atexit_flush)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_usage(self, usage: Dict) -> None:
        """Record token counts from a single API call.

        Silently skips if *usage* is empty or missing expected keys.
        """
        prompt = usage.get("prompt_tokens", 0) or 0
        completion = usage.get("completion_tokens", 0) or 0
        total = usage.get("total_tokens", 0) or 0
        if total == 0 and (prompt or completion):
            total = prompt + completion

        if total == 0:
            return

        with self._mu:
            self._delta["prompt_tokens"] += prompt
            self._delta["completion_tokens"] += completion
            self._delta["total_tokens"] += total
            self._delta["api_calls"] += 1

            self._session_counters["prompt_tokens"] += prompt
            self._session_counters["completion_tokens"] += completion
            self._session_counters["total_tokens"] += total
            self._session_counters["api_calls"] += 1

            self._calls_since_flush += 1

            should_flush = (
                self._calls_since_flush >= _FLUSH_CALL_COUNT
                or (time.monotonic() - self._last_flush) >= _FLUSH_INTERVAL
            )

        if should_flush:
            self.flush()

    def flush(self) -> None:
        """Merge in-memory delta into the on-disk JSON file."""
        with self._mu:
            if self._delta["api_calls"] == 0:
                return
            delta = dict(self._delta)
            session_snap = dict(self._session_counters)
            self._delta = dict(_EMPTY_COUNTERS)
            self._calls_since_flush = 0
            self._last_flush = time.monotonic()

        self._merge_to_disk(delta, session_snap)

    # ------------------------------------------------------------------
    # Disk I/O (with cross-process locking)
    # ------------------------------------------------------------------

    def _merge_to_disk(self, delta: Dict, session_snap: Dict) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        lock_fd = None
        try:
            lock_fd = open(self._lock_path, "w")
            fcntl.flock(lock_fd, fcntl.LOCK_EX)

            store = self._read_store()

            # Merge all_time
            for key in ("prompt_tokens", "completion_tokens",
                        "total_tokens", "api_calls"):
                store["all_time"][key] += delta[key]

            # Merge per_layer
            if self._layer not in store["per_layer"]:
                store["per_layer"][self._layer] = dict(_EMPTY_COUNTERS)
            for key in ("prompt_tokens", "completion_tokens",
                        "total_tokens", "api_calls"):
                store["per_layer"][self._layer][key] += delta[key]

            # Upsert session
            self._upsert_session(store, session_snap)

            # Atomic write
            tmp = self._path.with_suffix(".tmp")
            tmp.write_text(json.dumps(store, indent=2))
            os.replace(str(tmp), str(self._path))

        except Exception:
            logger.exception("Failed to flush token usage to disk")
        finally:
            if lock_fd is not None:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    lock_fd.close()
                except OSError:
                    pass

    def _read_store(self) -> Dict:
        if not self._path.exists():
            return _empty_store()
        try:
            text = self._path.read_text()
            if not text.strip():
                return _empty_store()
            data = json.loads(text)
            # Minimal validation
            if not isinstance(data.get("all_time"), dict):
                raise ValueError("missing all_time")
            return data
        except (json.JSONDecodeError, ValueError) as exc:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            corrupted = self._path.with_suffix(f".corrupted.{ts}")
            logger.warning(
                "Corrupted token_usage.json (%s), renaming to %s",
                exc, corrupted,
            )
            try:
                os.replace(str(self._path), str(corrupted))
            except OSError:
                pass
            return _empty_store()

    def _upsert_session(self, store: Dict, session_snap: Dict) -> None:
        sessions = store.setdefault("sessions", [])
        for entry in sessions:
            if entry.get("session_id") == self._session_id:
                entry.update({
                    "ended_at": datetime.now().isoformat(timespec="seconds"),
                    **{k: session_snap[k] for k in _EMPTY_COUNTERS},
                })
                return
        sessions.append({
            "session_id": self._session_id,
            "layer": self._layer,
            "started_at": self._session_start,
            "ended_at": datetime.now().isoformat(timespec="seconds"),
            **{k: session_snap[k] for k in _EMPTY_COUNTERS},
        })

    # ------------------------------------------------------------------
    # atexit
    # ------------------------------------------------------------------

    def _atexit_flush(self) -> None:
        try:
            self.flush()
        except Exception:
            pass


# ------------------------------------------------------------------
# Module-level singleton access
# ------------------------------------------------------------------

_instances: Dict[str, TokenTracker] = {}
_instance_lock = threading.Lock()


def get_tracker(layer_name: str) -> Optional[TokenTracker]:
    """Return (or create) the TokenTracker singleton for *layer_name*."""
    with _instance_lock:
        if layer_name in _instances:
            return _instances[layer_name]

    # Resolve path outside the lock (I/O)
    try:
        from .paths import PipelinePaths
        path = PipelinePaths().token_usage_path
    except Exception:
        logger.debug("Could not resolve token_usage_path, tracking disabled")
        return None

    with _instance_lock:
        # Double-check after re-acquiring
        if layer_name not in _instances:
            _instances[layer_name] = TokenTracker(layer_name, path)
        return _instances[layer_name]
