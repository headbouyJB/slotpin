"""Shared mutable proxy state, protected by a single lock.

All access to :class:`ProxyState` fields MUST hold ``state.lock`` -- it is a
plain ``threading.Lock`` deliberately exposed on the instance rather than
hidden, so callers (kvstore, scheduler, server) can take it once around a
compound read-modify-write instead of paying for many small internal locks.

The counter set below (``turns, restores, evictions, restored_tokens,
kv_saves, kv_restores, kv_restore_misses, reloads_detected``) is the full set
exposed through ``/_stats``.
"""

from __future__ import annotations

import math
import threading
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass, field

_COUNTER_KEYS = (
    "turns",
    "restores",
    "evictions",
    "restored_tokens",
    "kv_saves",
    "kv_restores",
    "kv_restore_misses",
    "kv_prefix_restores",
    "kv_prefix_restore_misses",
    "reloads_detected",
)


def pctl(values: Sequence[float], p: float) -> float | None:
    """Linear-interpolated percentile of ``values`` at ``p`` (0-100).

    Returns ``None`` for an empty sequence. Used by stats.py and
    scheduler.py to report queue-wait / decode-tps percentiles.
    """
    if not values:
        return None
    s = sorted(values)
    if len(s) == 1:
        return float(s[0])
    k = (len(s) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(s[int(k)])
    d0 = s[int(f)] * (c - k)
    d1 = s[int(c)] * (k - f)
    return float(d0 + d1)


@dataclass
class ProxyState:
    """All fields here are shared across request-handling threads.

    Access convention: callers acquire ``self.lock`` for the duration of any
    read-modify-write; single atomic dict reads are tolerated unlocked
    elsewhere in this codebase only where explicitly noted.
    """

    lock: threading.Lock = field(default_factory=threading.Lock)
    slot_last: dict[int, str | None] = field(default_factory=dict)
    slot_busy: dict[int, bool] = field(default_factory=dict)
    fp_info: dict[str, dict] = field(default_factory=dict)
    counters: dict[str, int] = field(default_factory=lambda: {k: 0 for k in _COUNTER_KEYS})
    recent: deque[dict] = field(default_factory=lambda: deque(maxlen=200))

    def reset_for_reload(self) -> None:
        """Clear per-slot occupancy knowledge after a detected model reload.

        This zeroes the *value* of every existing ``slot_last`` entry rather
        than clearing the dict outright, so slot keys already known to the
        rest of the system (populated at startup) remain present -- avoiding
        a KeyError window for any code that indexes ``slot_last[slot]``
        instead of using ``.get``. This is the "pool coupling point": the caller (reload_detect.py) is responsible for
        also calling ``pool.clear()`` in the same reload-handling step.
        """
        with self.lock:
            for slot in list(self.slot_last):
                self.slot_last[slot] = None
