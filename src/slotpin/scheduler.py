"""Priority scheduler for admission into the single (or dual) llama.cpp slot(s).

Implements strict tier priority with FIFO ordering within a tier, a global
``max_inflight`` admission cap, anti-starvation aging, and optional
backpressure shedding. Tier 0 is the highest priority (the "interactive"
lane in the reference config); higher tier numbers are lower priority.

Design notes
------------
``Scheduler.acquire`` blocks the calling (request-handling) thread until one
of three things happens:

* it is admitted -> returns a :class:`SchedTicket`;
* the caller-supplied ``client_gone`` callback reports the client disconnected
  while queued -> returns ``None``;
* ``cfg.queue_timeout_s`` elapses while queued, or (if enabled) backpressure
  sheds it -> returns ``None``.

All three count as a *shed* request for that (raw) tier in
:meth:`Scheduler.snapshot`. The scheduler never sheds a request that has
already been admitted, and it never preempts a request that is already
running -- there is no mid-generation preemption (turn-boundary priority
only), matching llama.cpp's inability to preempt an in-flight generation.

Aging and backpressure:

* the best-eligible waiter is selected by an EFFECTIVE tier that ages
  continuously toward the highest priority (``TIER_HIGH = 0``) as a waiter's
  queued time approaches ``max_wait_s`` (default 60s) --
  ``eff_tier = tier - int(tier * min(1.0, waited / max_wait_s))`` -- so NO
  request can be starved forever purely by higher-tier traffic. See
  :meth:`_effective_tier`; the discrete "promoted to HIGH at >= max_wait_s"
  case is just this formula's ``frac == 1.0`` endpoint.
* optionally sheds LOW-tier waiters that have queued past ``bp_drop_s``
  (default 45s) when ``sched_backpressure`` is on (default off). Eligibility
  is ``cfg.backpressure_min_tier`` (default 20, the LOW tier on the
  reference profile's 0/10/20 scale).
* ``queue_timeout_s`` is an additional hard backstop (a request cannot
  queue forever even if aging is misconfigured/disabled).

Admission only -- ``state.slot_busy`` is NOT managed here
--------------------------------------------------------
``acquire(self, tier, client_gone)`` deliberately does not take a slot; it only
caps concurrency (admits while ``inflight < max_inflight``) with anti-starvation
aging. Slot assignment and ``state.slot_busy`` are owned entirely by the server
(v8 pooled routing): the server picks a free slot post-admission, marks exactly
that slot busy under ``state.lock``, and frees it before releasing the ticket.
(An earlier version had the scheduler mark a whole managed-slot set busy on
every inflight transition; that was imprecise under pooling and was removed.)
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .state import pctl

if TYPE_CHECKING:
    from .config import Config
    from .state import ProxyState

logger = logging.getLogger("slotpin.scheduler")

# TIER_HIGH: the aging ceiling every waiter's effective tier
# approaches as it queues. Not configurable (fixed at 0); it is the fixed reference point the whole
# 0/10/20 tier convention (see classify.py/config.example.toml) is built around.
_TIER_HIGH = 0


@dataclass(frozen=True)
class SchedTicket:
    """An admission ticket returned by :meth:`Scheduler.acquire`.

    Opaque to callers besides its ``tier`` and ``queue_wait_ms`` (used for
    metrics); must be passed back to :meth:`Scheduler.release` exactly once.
    """

    tier: int
    ticket_id: int
    queue_wait_ms: float
    sched_enabled: bool


@dataclass
class _Waiter:
    """One queued (or just-admitted) request, tracked for effective-tier aging."""

    tier: int
    seq: int
    client_gone: Callable[[], bool] | None
    enq_monotonic: float = field(default_factory=time.monotonic)
    admitted: bool = False
    cancelled: bool = False


class Scheduler:
    """Aging-priority, FIFO-within-effective-tier admission scheduler.

    When ``cfg.sched_enabled`` is False, :meth:`acquire` is a non-blocking
    no-op that still increments the inflight counter (so ``/_stats`` remains
    meaningful) but never queues, sheds, or blocks.
    """

    def __init__(self, cfg: Config, state: ProxyState) -> None:
        """Store config/state and initialise the (empty) waiter list."""
        self._cfg = cfg
        self._state = state
        self._cond = threading.Condition()
        self._waiters: list[_Waiter] = []
        self._shed_count: dict[int, int] = defaultdict(int)
        self._wait_samples: dict[int, deque[float]] = defaultdict(lambda: deque(maxlen=500))
        self._inflight = 0
        self._next_id = 0

    def acquire(self, tier: int, client_gone: Callable[[], bool]) -> SchedTicket | None:
        """Block until admitted, or return ``None`` on shed (client-gone,
        queue_timeout, or backpressure).

        Admission requires: this waiter has the numerically-lowest EFFECTIVE
        tier (see :meth:`_effective_tier`) among all currently-queued
        waiters, FIFO-tiebroken by arrival order, and
        ``inflight < cfg.max_inflight``. Polls at ~0.1s granularity via
        ``Condition.wait(timeout=...)`` (never a tighter busy-wait).
        """
        if not self._cfg.sched_enabled:
            with self._cond:
                self._inflight += 1
            return SchedTicket(tier=tier, ticket_id=-1, queue_wait_ms=0.0, sched_enabled=False)

        start = time.monotonic()
        with self._cond:
            # seq assigned under the lock so concurrent acquirers can't share one
            # (the FIFO tiebreak depends on it being unique).
            w = _Waiter(tier=tier, seq=self._next_id, client_gone=client_gone, enq_monotonic=start)
            self._next_id += 1
            self._waiters.append(w)
            self._cond.notify_all()  # wake others to re-evaluate ordering/aging
            try:
                while True:
                    now = time.monotonic()
                    self._prune_dead_locked(now)
                    if w.cancelled:
                        return None
                    if self._inflight < self._cfg.max_inflight and self._best_locked(now) is w:
                        w.admitted = True
                        self._inflight += 1
                        self._waiters = [x for x in self._waiters if x is not w]
                        # Let the next-best waiter re-evaluate immediately rather
                        # than wait out a 0.1s poll tick (snappier multi-slot ramp).
                        self._cond.notify_all()
                        wait_ms = (now - start) * 1000.0
                        self._wait_samples[tier].append(wait_ms)
                        return SchedTicket(
                            tier=tier, ticket_id=w.seq, queue_wait_ms=wait_ms, sched_enabled=True
                        )
                    self._cond.wait(timeout=0.1)
            except BaseException:
                self._remove_locked(w)
                raise

    def release(self, ticket: SchedTicket) -> None:
        """Release an admitted ticket, freeing one inflight slot and notifying waiters."""
        with self._cond:
            if self._inflight > 0:
                self._inflight -= 1
            self._cond.notify_all()

    def inflight(self) -> int:
        """Current number of admitted, not-yet-released turns (concurrency level).

        Capped at ``max_inflight`` (1 today -> always sequential). Used per-turn
        to attribute shared whole-box energy under concurrency (divide by the
        number of live turns).
        """
        with self._cond:
            return self._inflight

    def snapshot(self) -> dict:
        """Return per-tier queue-wait p50/p95, inflight, queued, and shed counts.

        Grouped by each waiter's RAW (un-aged) tier -- aging is an admission-
        order detail, not a reclassification of the request.
        """
        with self._cond:
            queued_by_tier: dict[int, int] = defaultdict(int)
            for w in self._waiters:
                if not w.admitted:
                    queued_by_tier[w.tier] += 1
            tiers: dict[str, dict] = {}
            all_tiers = set(queued_by_tier) | set(self._shed_count) | set(self._wait_samples)
            for tier in sorted(all_tiers):
                samples = list(self._wait_samples.get(tier, ()))
                tiers[str(tier)] = {
                    "queued": queued_by_tier.get(tier, 0),
                    "shed": self._shed_count.get(tier, 0),
                    "wait_ms_p50": pctl(samples, 50) if samples else None,
                    "wait_ms_p95": pctl(samples, 95) if samples else None,
                }
            return {
                "sched_enabled": self._cfg.sched_enabled,
                "max_inflight": self._cfg.max_inflight,
                "inflight": self._inflight,
                "max_wait_s": self._cfg.max_wait_s,
                "backpressure": self._cfg.sched_backpressure,
                "tiers": tiers,
            }

    def _effective_tier(self, w: _Waiter, now: float) -> float:
        """Continuous aging formula: a waiter's priority rises
        toward ``_TIER_HIGH`` (0) as it queues, capped once ``waited >=
        cfg.max_wait_s`` (at which point it is exactly ``_TIER_HIGH`` --
        the "promoted to TIER_HIGH" cliff is this formula's
        ``frac == 1.0`` endpoint, not a separate branch)."""
        max_wait_s = self._cfg.max_wait_s
        if max_wait_s <= 0:
            return float(w.tier)
        waited = now - w.enq_monotonic
        frac = min(1.0, waited / max_wait_s)
        return w.tier - int((w.tier - _TIER_HIGH) * frac)

    def _best_locked(self, now: float) -> _Waiter | None:
        """Return the highest-effective-priority eligible waiter, or ``None``.

        FIFO within an effective tier (ties broken by arrival ``seq``). O(n)
        over current queue depth (small); no upstream I/O. Must hold
        ``self._cond``.
        """
        best: _Waiter | None = None
        best_key: tuple[float, int] | None = None
        for w in self._waiters:
            if w.admitted or w.cancelled:
                continue
            key = (self._effective_tier(w, now), w.seq)
            if best is None or key < best_key:  # type: ignore[operator]
                best = w
                best_key = key
        return best

    def _prune_dead_locked(self, now: float) -> None:
        """Remove/cancel waiters that are gone, timed out, or backpressure-shed.

        Must hold ``self._cond``. A waiter already admitted is never touched
        here (only queued, not-yet-admitted waiters
        are ever shed).
        """
        live: list[_Waiter] = []
        for w in self._waiters:
            if w.admitted:
                live.append(w)
                continue
            if w.client_gone is not None and w.client_gone():
                w.cancelled = True
                self._shed_count[w.tier] += 1
                continue
            # queue_timeout_s backstop, PER WAITER (each times out relative to
            # its own enqueue time -- a shared deadline would shed every queued
            # waiter the instant the oldest one expired).
            if (
                self._cfg.queue_timeout_s > 0
                and (now - w.enq_monotonic) >= self._cfg.queue_timeout_s
            ):
                w.cancelled = True
                self._shed_count[w.tier] += 1
                logger.info("scheduler: queue_timeout for tier=%d", w.tier)
                continue
            if (
                self._cfg.sched_backpressure
                and w.tier >= self._cfg.backpressure_min_tier
                and (now - w.enq_monotonic) >= self._cfg.bp_drop_s
            ):
                w.cancelled = True
                self._shed_count[w.tier] += 1
                logger.info("scheduler: backpressure shed for tier=%d", w.tier)
                continue
            live.append(w)
        if len(live) != len(self._waiters):
            self._waiters = live

    def _remove_locked(self, w: _Waiter) -> None:
        """Drop a (possibly already-removed) waiter, e.g. on an exception unwind."""
        self._waiters = [x for x in self._waiters if x is not w]
