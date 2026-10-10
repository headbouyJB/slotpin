"""Append-only JSONL metrics writer with size-based rotation.

Never raises: every public method swallows and logs its own exceptions,
since a metrics-file hiccup (disk full, permissions) must never break the
request path. The original single-file proxy's metrics file was unbounded;
this module adds size-based rotation. The metrics *schema* is produced by
callers (server.py), not this module.
"""

from __future__ import annotations

import json
import logging
import threading
from typing import Any

from .config import Config

logger = logging.getLogger("slotpin.metrics")


class MetricsWriter:
    """Thread-safe, never-raising JSONL metrics writer with rotation."""

    def __init__(self, cfg: Config) -> None:
        """Bind to ``cfg`` for the metrics file path, rotate size, and keep count."""
        self._cfg = cfg
        self._lock = threading.Lock()

    def write(self, event: dict[str, Any]) -> None:
        """Append one JSON line for ``event``. Never raises."""
        path = self._cfg.metrics_file
        if path is None:
            return
        try:
            with self._lock:
                self._rotate_locked()
                with open(path, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps(event, default=str) + "\n")
        except Exception:  # noqa: BLE001
            logger.exception("metrics: failed to write event")

    def maybe_rotate(self) -> None:
        """Public rotation check/action, usable independently of ``write``."""
        try:
            with self._lock:
                self._rotate_locked()
        except Exception:  # noqa: BLE001
            logger.exception("metrics: rotation failed")

    def _rotate_locked(self) -> None:
        path = self._cfg.metrics_file
        if path is None or not path.exists():
            return
        try:
            size = path.stat().st_size
        except OSError:
            return
        if size <= self._cfg.metrics_rotate_mb * 1024 * 1024:
            return

        keep = self._cfg.metrics_keep
        oldest = path.with_name(f"{path.name}.{keep}")
        if oldest.exists():
            oldest.unlink()
        for i in range(keep - 1, 0, -1):
            src = path.with_name(f"{path.name}.{i}")
            dst = path.with_name(f"{path.name}.{i + 1}")
            if src.exists():
                src.rename(dst)
        path.rename(path.with_name(f"{path.name}.1"))
        logger.info("metrics: rotated %s (kept %d)", path, keep)
