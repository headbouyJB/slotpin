"""Inferred cache-ram (L2 KV) pool model.

llama.cpp's ``--cache-ram`` L2 pool is not introspectable, so this class
maintains a best-effort model of "which fingerprints were evicted from the
slot and are presumably still resident in host cache-ram, not yet
restored" -- as a thread-safe object with its own lock.

Note: ``note_eviction(fp, tokens)`` passes tokens in directly, so this pool
keeps its own ``fp -> tokens`` side table rather than looking tokens up in a
shared ``fp_info`` dict during trim.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict

from .config import Config

_MAX_ENTRIES = 24


class CacheRamPool:
    """Thread-safe inferred model of the server's cache-ram L2 pool."""

    def __init__(self, cfg: Config) -> None:
        """Bind to ``cfg`` for the derived ``cram_token_cap`` trim budget."""
        self._cfg = cfg
        self._lock = threading.Lock()
        self._evicted_at: OrderedDict[str, float] = OrderedDict()
        self._tokens: dict[str, int] = {}

    def note_eviction(self, fp: str, tokens: int) -> None:
        """Record that fingerprint ``fp`` (with ``tokens`` prompt tokens) was
        just evicted from its slot, presumably surviving in cache-ram."""
        with self._lock:
            self._tokens[fp] = tokens
            self._evicted_at[fp] = time.time()
            self._evicted_at.move_to_end(fp)
            self._trim_locked()

    def mark_restored(self, fp: str) -> None:
        """Remove ``fp`` from the pool: it was just restored back into a slot."""
        with self._lock:
            self._evicted_at.pop(fp, None)
            self._tokens.pop(fp, None)

    def __contains__(self, fp: str) -> bool:
        with self._lock:
            return fp in self._evicted_at

    def trim(self) -> None:
        """Public entry point to re-apply the cap (also run after every eviction)."""
        with self._lock:
            self._trim_locked()

    def _trim_locked(self) -> None:
        cap = self._cfg.cram_token_cap
        total = sum(self._tokens.get(fp, 0) for fp in self._evicted_at)
        while self._evicted_at and (total > cap or len(self._evicted_at) > _MAX_ENTRIES):
            oldest, _ts = self._evicted_at.popitem(last=False)
            total -= self._tokens.pop(oldest, 0)

    def clear(self) -> None:
        """Drop all pool knowledge -- called on detected model/server reload."""
        with self._lock:
            self._evicted_at.clear()
            self._tokens.clear()

    def snapshot(self) -> list[dict]:
        """Most-recent-first list of ``{"fp", "tokens", "evicted_ts"}`` dicts."""
        with self._lock:
            return [
                {"fp": fp, "tokens": self._tokens.get(fp), "evicted_ts": ts}
                for fp, ts in reversed(self._evicted_at.items())
            ]
