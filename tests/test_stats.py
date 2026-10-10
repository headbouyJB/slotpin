"""Tests for slotpin.stats.build_stats: block structure and ring aggregates."""

from __future__ import annotations

from slotpin.kvstore import KVStore
from slotpin.metrics import MetricsWriter
from slotpin.pool import CacheRamPool
from slotpin.scheduler import Scheduler
from slotpin.slot_admin import SlotAdmin
from slotpin.state import ProxyState, pctl
from slotpin.stats import build_stats


def _build(make_config, fake_upstream, **overrides):
    cfg = make_config(
        admin_host=fake_upstream.host,
        admin_port=fake_upstream.port,
        upstream_host=fake_upstream.host,
        upstream_port=fake_upstream.port,
        **overrides,
    )
    state = ProxyState()
    pool = CacheRamPool(cfg)
    admin = SlotAdmin(cfg)
    metrics = MetricsWriter(cfg)
    kv = KVStore(cfg, admin, pool, state, metrics)
    sched = Scheduler(cfg, state)
    return cfg, state, pool, kv, sched


def test_build_stats_block_structure(make_config, fake_upstream):
    cfg, state, pool, kv, sched = _build(make_config, fake_upstream)
    stats = build_stats(cfg, state, pool, kv, sched)

    assert set(stats.keys()) == {
        "config",
        "counters",
        "slots",
        "inferred_cacheram_pool",
        "adaptive",
        "scheduler",
        "recent",
    }
    for key in (
        "mode",
        "sched_enabled",
        "kv_persist",
        "kv_adaptive",
        "cache_ram_mb",
        "cram_token_cap",
        "max_inflight",
        "slot_save_path",
        "upstream",
        "admin",
        "listen",
    ):
        assert key in stats["config"]

    for key in ("count", "decode_tps_p50", "warm", "cold", "warm_ratio", "interactive_wait_ms_p95"):
        assert key in stats["recent"]


def test_slots_block_reflects_slot_last_and_busy(make_config, fake_upstream):
    cfg, state, pool, kv, sched = _build(make_config, fake_upstream)
    with state.lock:
        state.slot_last[0] = "some-prefix"
        state.slot_busy[0] = True
        state.fp_info["some-prefix"] = {"label": "workload-x"}

    stats = build_stats(cfg, state, pool, kv, sched)
    assert stats["slots"]["0"] == {
        "last_prefix": "some-prefix",
        "last_workload": "workload-x",
        "busy": True,
    }


def test_recent_aggregates_math_on_seeded_ring(make_config, fake_upstream):
    cfg, state, pool, kv, sched = _build(make_config, fake_upstream)
    entries = [
        {"served": "warm", "decode_tps": 10.0, "interactive": True, "queue_wait_ms": 5.0},
        {"served": "warm", "decode_tps": 20.0, "interactive": False, "queue_wait_ms": 1.0},
        {"served": "cold", "decode_tps": 5.0, "interactive": True, "queue_wait_ms": 50.0},
        {"served": "cold", "decode_tps": None, "interactive": False, "queue_wait_ms": None},
    ]
    with state.lock:
        for e in entries:
            state.recent.append(dict(e))

    stats = build_stats(cfg, state, pool, kv, sched)
    recent = stats["recent"]

    assert recent["count"] == 4
    assert recent["warm"] == 2
    assert recent["cold"] == 2
    assert recent["warm_ratio"] == 2 / 4

    decode_vals = [10.0, 20.0, 5.0]
    assert recent["decode_tps_p50"] == pctl(decode_vals, 50)

    interactive_wait_vals = [5.0, 50.0]
    assert recent["interactive_wait_ms_p95"] == pctl(interactive_wait_vals, 95)


def test_recent_block_handles_empty_ring():
    # build_stats with no recent entries -- percentiles/ratios must be None,
    # not raise (division by zero / pctl on empty list).
    from slotpin.config import Config

    cfg = Config()
    state = ProxyState()
    pool = CacheRamPool(cfg)
    admin = SlotAdmin(cfg)
    metrics = MetricsWriter(cfg)
    kv = KVStore(cfg, admin, pool, state, metrics)
    sched = Scheduler(cfg, state)

    stats = build_stats(cfg, state, pool, kv, sched)
    recent = stats["recent"]
    assert recent["count"] == 0
    assert recent["decode_tps_p50"] is None
    assert recent["warm_ratio"] is None
    assert recent["interactive_wait_ms_p95"] is None
