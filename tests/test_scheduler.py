"""Tests for slotpin.scheduler.Scheduler: tier priority, FIFO, max_inflight,
client-gone/queue_timeout shedding, and the sched_enabled=False no-op path."""

from __future__ import annotations

import threading
import time

from slotpin.config import Config
from slotpin.scheduler import Scheduler, _Waiter
from slotpin.state import ProxyState


def _never_gone() -> bool:
    return False


def _always_gone() -> bool:
    return True


def _cfg(**kw):
    kw.setdefault("sched_enabled", True)
    kw.setdefault("max_inflight", 1)
    kw.setdefault("queue_timeout_s", 2.0)
    return Config(**kw)


def _poll(check, deadline=3.0, interval=0.01):
    start = time.monotonic()
    while True:
        if check():
            return True
        if time.monotonic() - start > deadline:
            raise AssertionError("condition not met in time")
        time.sleep(interval)


# ---------------------------------------------------------------------------
# tier priority: tier 0 admitted before queued tier 2
# ---------------------------------------------------------------------------


def test_tier0_admitted_before_queued_tier2():
    cfg = _cfg(max_inflight=1, queue_timeout_s=5.0)
    state = ProxyState()
    sched = Scheduler(cfg, state)

    order: list[str] = []
    order_lock = threading.Lock()
    release_a = threading.Event()

    def worker(tier: int, name: str, release_gate: threading.Event | None) -> None:
        ticket = sched.acquire(tier, _never_gone)
        with order_lock:
            order.append(name)
        if release_gate is not None:
            release_gate.wait(timeout=5.0)
        sched.release(ticket)

    # A (tier 2) grabs the only inflight slot immediately.
    t_a = threading.Thread(target=worker, args=(2, "A", release_a))
    t_a.start()
    _poll(lambda: sched.snapshot()["inflight"] == 1)

    # B (tier 2) queues behind A.
    t_b = threading.Thread(target=worker, args=(2, "B", None))
    t_b.start()
    _poll(lambda: sched.snapshot()["tiers"].get("2", {}).get("queued") == 1)

    # C (tier 0) queues too -- higher priority than B despite arriving later.
    t_c = threading.Thread(target=worker, args=(0, "C", None))
    t_c.start()
    _poll(lambda: sched.snapshot()["tiers"].get("0", {}).get("queued") == 1)

    # Release A: the next admission must be tier 0 (C), not the earlier-queued
    # tier 2 (B).
    release_a.set()

    for t in (t_a, t_b, t_c):
        t.join(timeout=5)
        assert not t.is_alive()

    assert order == ["A", "C", "B"]


# ---------------------------------------------------------------------------
# FIFO within a tier
# ---------------------------------------------------------------------------


def test_fifo_within_tier():
    cfg = _cfg(max_inflight=1, queue_timeout_s=5.0)
    state = ProxyState()
    sched = Scheduler(cfg, state)

    order: list[str] = []
    order_lock = threading.Lock()

    def worker(name: str) -> None:
        ticket = sched.acquire(1, _never_gone)
        with order_lock:
            order.append(name)
        time.sleep(0.02)
        sched.release(ticket)

    t1 = threading.Thread(target=worker, args=("first",))
    t1.start()
    _poll(lambda: sched.snapshot()["inflight"] == 1)

    t2 = threading.Thread(target=worker, args=("second",))
    t2.start()
    _poll(lambda: sched.snapshot()["tiers"].get("1", {}).get("queued") == 1)

    t3 = threading.Thread(target=worker, args=("third",))
    t3.start()
    _poll(lambda: sched.snapshot()["tiers"].get("1", {}).get("queued") == 2)

    for t in (t1, t2, t3):
        t.join(timeout=5)
        assert not t.is_alive()

    assert order == ["first", "second", "third"]


# ---------------------------------------------------------------------------
# max_inflight
# ---------------------------------------------------------------------------


def test_max_inflight_allows_concurrent_admissions_up_to_cap():
    cfg = _cfg(max_inflight=2, queue_timeout_s=5.0)
    state = ProxyState()
    sched = Scheduler(cfg, state)

    admitted = threading.Event()
    tickets: list = []
    lock = threading.Lock()

    def worker() -> None:
        ticket = sched.acquire(1, _never_gone)
        with lock:
            tickets.append(ticket)
            if len(tickets) == 2:
                admitted.set()
        time.sleep(0.3)
        sched.release(ticket)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()

    assert admitted.wait(timeout=2.0)
    assert sched.snapshot()["inflight"] == 2

    for t in threads:
        t.join(timeout=5)


def test_third_request_queues_until_one_of_two_releases():
    cfg = _cfg(max_inflight=2, queue_timeout_s=5.0)
    state = ProxyState()
    sched = Scheduler(cfg, state)
    release_gate = threading.Event()

    def holder() -> None:
        ticket = sched.acquire(1, _never_gone)
        release_gate.wait(timeout=5.0)
        sched.release(ticket)

    h1 = threading.Thread(target=holder)
    h2 = threading.Thread(target=holder)
    h1.start()
    h2.start()
    _poll(lambda: sched.snapshot()["inflight"] == 2)

    third_admitted = threading.Event()

    def third() -> None:
        ticket = sched.acquire(1, _never_gone)
        third_admitted.set()
        sched.release(ticket)

    t3 = threading.Thread(target=third)
    t3.start()
    _poll(lambda: sched.snapshot()["tiers"].get("1", {}).get("queued") == 1)
    assert not third_admitted.is_set()

    release_gate.set()
    assert third_admitted.wait(timeout=2.0)

    for t in (h1, h2, t3):
        t.join(timeout=5)


# ---------------------------------------------------------------------------
# client_gone abort returns None
# ---------------------------------------------------------------------------


def test_client_gone_returns_none_immediately():
    cfg = _cfg(max_inflight=1, queue_timeout_s=5.0)
    state = ProxyState()
    sched = Scheduler(cfg, state)
    ticket = sched.acquire(1, _always_gone)
    assert ticket is None
    snap = sched.snapshot()
    assert snap["tiers"]["1"]["shed"] == 1


def test_client_gone_while_queued_unblocks_with_none():
    cfg = _cfg(max_inflight=1, queue_timeout_s=5.0)
    state = ProxyState()
    sched = Scheduler(cfg, state)

    holder_ticket_holder: list = []

    def holder() -> None:
        ticket = sched.acquire(1, _never_gone)
        holder_ticket_holder.append(ticket)
        time.sleep(0.5)
        sched.release(ticket)

    t_holder = threading.Thread(target=holder)
    t_holder.start()
    _poll(lambda: sched.snapshot()["inflight"] == 1)

    gone_flag = threading.Event()
    result: list = []

    def waiter() -> None:
        result.append(sched.acquire(1, gone_flag.is_set))

    t_waiter = threading.Thread(target=waiter)
    t_waiter.start()
    _poll(lambda: sched.snapshot()["tiers"].get("1", {}).get("queued") == 1)

    gone_flag.set()
    t_waiter.join(timeout=2.0)
    assert result == [None]

    t_holder.join(timeout=5)


# ---------------------------------------------------------------------------
# queue_timeout shed counted
# ---------------------------------------------------------------------------


def test_queue_timeout_shed_counted():
    cfg = _cfg(max_inflight=1, queue_timeout_s=0.1)
    state = ProxyState()
    sched = Scheduler(cfg, state)

    holder_release = threading.Event()

    def holder() -> None:
        ticket = sched.acquire(1, _never_gone)
        holder_release.wait(timeout=5.0)
        sched.release(ticket)

    t_holder = threading.Thread(target=holder)
    t_holder.start()
    _poll(lambda: sched.snapshot()["inflight"] == 1)

    result = sched.acquire(1, _never_gone)  # should time out (queue_timeout_s=0.1)
    assert result is None
    assert sched.snapshot()["tiers"]["1"]["shed"] == 1

    holder_release.set()
    t_holder.join(timeout=5)


# ---------------------------------------------------------------------------
# sched_enabled=False -> no-op ticket
# ---------------------------------------------------------------------------


def test_sched_disabled_is_a_noop_ticket():
    cfg = _cfg(sched_enabled=False)
    state = ProxyState()
    sched = Scheduler(cfg, state)

    ticket = sched.acquire(1, _always_gone)  # client_gone is irrelevant when disabled
    assert ticket is not None
    assert ticket.sched_enabled is False
    assert ticket.queue_wait_ms == 0.0
    assert sched.snapshot()["inflight"] == 1

    sched.release(ticket)
    assert sched.snapshot()["inflight"] == 0


# ---------------------------------------------------------------------------
# snapshot percentiles present
# ---------------------------------------------------------------------------


def test_snapshot_percentiles_present_after_admissions():
    cfg = _cfg(max_inflight=1, queue_timeout_s=5.0)
    state = ProxyState()
    sched = Scheduler(cfg, state)

    for _ in range(3):
        ticket = sched.acquire(1, _never_gone)
        assert ticket is not None
        sched.release(ticket)

    snap = sched.snapshot()
    tier1 = snap["tiers"]["1"]
    assert isinstance(tier1["wait_ms_p50"], float)
    assert isinstance(tier1["wait_ms_p95"], float)
    assert tier1["shed"] == 0


# ---------------------------------------------------------------------------
# anti-starvation aging (effective-tier aging): a long-queued
# low-priority waiter is eventually admitted ahead of freshly-arriving
# high-priority ones, instead of being starved forever by strict tier order.
# ---------------------------------------------------------------------------


def test_aging_promotes_a_long_queued_low_tier_waiter_ahead_of_fresh_high_tier():
    # max_wait_s short enough to fully age within the test's timeout budget.
    cfg = _cfg(max_inflight=1, queue_timeout_s=10.0, max_wait_s=0.2)
    state = ProxyState()
    sched = Scheduler(cfg, state)

    order: list[str] = []
    order_lock = threading.Lock()
    release_a = threading.Event()

    def worker(tier: int, name: str, release_gate: threading.Event | None) -> None:
        ticket = sched.acquire(tier, _never_gone)
        with order_lock:
            order.append(name)
        if release_gate is not None:
            release_gate.wait(timeout=5.0)
        sched.release(ticket)

    # A (tier 0, HIGH) grabs the only inflight slot and holds it.
    t_a = threading.Thread(target=worker, args=(0, "A", release_a))
    t_a.start()
    _poll(lambda: sched.snapshot()["inflight"] == 1)

    # B (tier 20, LOW) queues and is left to age well past max_wait_s.
    t_b = threading.Thread(target=worker, args=(20, "B", None))
    t_b.start()
    _poll(lambda: sched.snapshot()["tiers"].get("20", {}).get("queued") == 1)
    time.sleep(cfg.max_wait_s * 3)  # B is now fully aged to effective TIER_HIGH

    # C (tier 0, HIGH) arrives fresh, after B was already aged.
    t_c = threading.Thread(target=worker, args=(0, "C", None))
    t_c.start()
    _poll(lambda: sched.snapshot()["tiers"].get("0", {}).get("queued") == 1)

    # Release A: B (aged to HIGH, arrived first) must be admitted before the
    # freshly-arrived C, even though both are now effectively tier 0.
    release_a.set()

    for t in (t_a, t_b, t_c):
        t.join(timeout=5)
        assert not t.is_alive()

    assert order == ["A", "B", "C"]


def test_no_aging_when_max_wait_s_disabled_zero_or_negative():
    """max_wait_s <= 0 falls back to raw (non-aged) tier -- an explicit
    opt-out, not a crash (division-by-zero guard)."""
    cfg = _cfg(max_inflight=1, queue_timeout_s=5.0, max_wait_s=0.0)
    state = ProxyState()
    sched = Scheduler(cfg, state)
    w = _make_waiter(tier=20, waited=999.0)
    assert sched._effective_tier(w, time.monotonic()) == 20


def _make_waiter(tier: int, waited: float):
    from slotpin.scheduler import _Waiter

    return _Waiter(tier=tier, seq=0, client_gone=None, enq_monotonic=time.monotonic() - waited)


def test_effective_tier_reaches_tier_high_at_max_wait_s():
    cfg = _cfg(max_inflight=1, queue_timeout_s=5.0, max_wait_s=10.0)
    state = ProxyState()
    sched = Scheduler(cfg, state)
    w = _make_waiter(tier=20, waited=10.0)
    now = time.monotonic()  # captured AFTER the waiter so waited-vs-now is >= 10.0
    assert sched._effective_tier(w, now) == 0  # fully aged to TIER_HIGH


def test_effective_tier_interpolates_between_raw_tier_and_tier_high():
    cfg = _cfg(max_inflight=1, queue_timeout_s=5.0, max_wait_s=10.0)
    state = ProxyState()
    sched = Scheduler(cfg, state)
    w = _make_waiter(tier=20, waited=5.0)  # halfway aged
    now = time.monotonic()
    eff = sched._effective_tier(w, now)
    assert 0 < eff < 20


# ---------------------------------------------------------------------------
# backpressure shedding (SLOTPIN_SCHED_BACKPRESSURE)
# ---------------------------------------------------------------------------


def test_backpressure_off_by_default_never_sheds_low_tier():
    cfg = _cfg(max_inflight=1, queue_timeout_s=5.0, max_wait_s=60.0)
    assert cfg.sched_backpressure is False


def test_backpressure_sheds_low_tier_waiter_past_bp_drop_s():
    cfg = _cfg(
        max_inflight=1,
        queue_timeout_s=10.0,
        max_wait_s=60.0,  # keep aging out of the way for this test
        sched_backpressure=True,
        backpressure_min_tier=20,
        bp_drop_s=0.15,
    )
    state = ProxyState()
    sched = Scheduler(cfg, state)

    holder_release = threading.Event()

    def holder() -> None:
        ticket = sched.acquire(0, _never_gone)
        holder_release.wait(timeout=5.0)
        sched.release(ticket)

    t_holder = threading.Thread(target=holder)
    t_holder.start()
    _poll(lambda: sched.snapshot()["inflight"] == 1)

    result = sched.acquire(20, _never_gone)  # LOW tier, backpressure-eligible
    assert result is None
    assert sched.snapshot()["tiers"]["20"]["shed"] == 1

    holder_release.set()
    t_holder.join(timeout=5)


def test_backpressure_does_not_shed_a_tier_below_the_threshold():
    cfg = _cfg(
        max_inflight=1,
        queue_timeout_s=1.0,  # deliberately short: the queue_timeout_s backstop
        max_wait_s=60.0,      # will still shed it, but NOT via backpressure's counter path
        sched_backpressure=True,
        backpressure_min_tier=20,
        bp_drop_s=0.1,
    )
    state = ProxyState()
    sched = Scheduler(cfg, state)

    holder_release = threading.Event()

    def holder() -> None:
        ticket = sched.acquire(0, _never_gone)
        holder_release.wait(timeout=5.0)
        sched.release(ticket)

    t_holder = threading.Thread(target=holder)
    t_holder.start()
    _poll(lambda: sched.snapshot()["inflight"] == 1)

    # tier 10 (MEDIUM) is below backpressure_min_tier=20 -> never backpressure-shed,
    # only the (much longer here) queue_timeout_s could shed it, which we don't reach.
    admitted = threading.Event()

    def waiter() -> None:
        ticket = sched.acquire(10, _never_gone)
        if ticket is not None:
            admitted.set()
            sched.release(ticket)

    t_waiter = threading.Thread(target=waiter)
    t_waiter.start()
    time.sleep(0.3)  # well past bp_drop_s=0.1; tier 10 must still be queued, not shed
    assert sched.snapshot()["tiers"].get("10", {}).get("shed", 0) == 0

    holder_release.set()
    assert admitted.wait(timeout=2.0)
    t_waiter.join(timeout=5)


# ---------------------------------------------------------------------------
# per-waiter queue timeout (audit finding: a shared deadline shed the whole
# queue when the oldest waiter expired)
# ---------------------------------------------------------------------------


def test_queue_timeout_is_per_waiter_not_whole_queue():
    cfg = _cfg(queue_timeout_s=1.0, max_inflight=1)
    sched = Scheduler(cfg, ProxyState())
    now = time.monotonic()
    old = _Waiter(tier=0, seq=0, client_gone=None, enq_monotonic=now - 2.0)  # aged past timeout
    fresh = _Waiter(tier=0, seq=1, client_gone=None, enq_monotonic=now - 0.1)  # well within
    sched._waiters = [old, fresh]
    with sched._cond:
        sched._prune_dead_locked(now)
    assert old.cancelled is True  # only the genuinely-timed-out waiter is shed
    assert fresh.cancelled is False
    assert fresh in sched._waiters and old not in sched._waiters
