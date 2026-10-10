"""Tests for v8 pooled concurrency: slot selection + synchronous owns-slot save."""

from __future__ import annotations

import pytest

from slotpin.config import ConfigError
from slotpin.kvstore import KVStore
from slotpin.metrics import MetricsWriter
from slotpin.pool import CacheRamPool
from slotpin.server import _choose_slot
from slotpin.slot_admin import SlotAdmin
from slotpin.state import ProxyState


def _build_kv(make_config, fake_upstream, **overrides):
    cfg = make_config(
        admin_host=fake_upstream.host,
        admin_port=fake_upstream.port,
        upstream_host=fake_upstream.host,
        upstream_port=fake_upstream.port,
        **overrides,
    )
    state = ProxyState()
    kv = KVStore(cfg, SlotAdmin(cfg), CacheRamPool(cfg), state, MetricsWriter(cfg))
    return cfg, kv, state


# --- _choose_slot: pooled selection (warm-preferred, else free, else fallback) ---


def test_choose_slot_prefers_warm_free_slot() -> None:
    assert _choose_slot({}, {0: "A", 1: "B", 2: "C"}, 3, "B") == 1  # free slot already holding B


def test_choose_slot_first_free_when_no_warm() -> None:
    assert _choose_slot({0: True}, {0: "A"}, 3, "Z") == 1  # 0 busy, 1 is first free


def test_choose_slot_skips_warm_slot_when_busy() -> None:
    assert _choose_slot({0: True}, {0: "A"}, 3, "A") == 1  # warm slot busy -> first free instead


def test_choose_slot_fallback_when_all_busy() -> None:
    assert _choose_slot({0: True, 1: True}, {}, 2, "X") == 0  # misconfig fallback


def test_choose_slot_single_slot_is_always_zero() -> None:
    assert _choose_slot({}, {}, 1, "X") == 0  # slots=1 -> today's single-slot behaviour


# --- save_now: synchronous, owns-slot save (the pooled hold-until-saved path) ---


def test_save_now_saves_synchronously(make_config, fake_upstream) -> None:
    cfg, kv, state = _build_kv(make_config, fake_upstream, prefix_save_min_interval_s=0.0)
    kv.save_now(0, "phZ", None)  # synchronous -> recorded immediately, no poll needed
    calls = fake_upstream.admin_calls_matching(action="save")
    assert [c["filename"] for c in calls] == ["phZ.bin"]
    assert state.counters["kv_saves"] == 1


def test_save_now_ignores_busy_slot(make_config, fake_upstream) -> None:
    # Unlike save_async (which defers while the slot is busy), save_now saves
    # even when slot_busy is set -- the caller IS the owner (generation done),
    # which is the pooled hold-slot-until-saved discipline.
    cfg, kv, state = _build_kv(make_config, fake_upstream, prefix_save_min_interval_s=0.0)
    with state.lock:
        state.slot_busy[0] = True
    kv.save_now(0, "phB", None)
    assert [c["filename"] for c in fake_upstream.admin_calls_matching(action="save")] == ["phB.bin"]


def test_save_now_rate_limits_per_prefix(make_config, fake_upstream) -> None:
    cfg, kv, state = _build_kv(make_config, fake_upstream, prefix_save_min_interval_s=10.0)
    kv.save_now(0, "phR", None)
    kv.save_now(0, "phR", None)  # within the interval -> skipped
    assert len(fake_upstream.admin_calls_matching(action="save")) == 1


# --- v8 concurrency-invariant config validation (audit finding: slots vs max_inflight) ---


def test_config_rejects_max_inflight_over_slots(make_config) -> None:
    with pytest.raises(ConfigError):
        make_config(slots=2, max_inflight=4, sched_enabled=True)  # admission could exceed slots


def test_config_rejects_multislot_without_scheduler(make_config) -> None:
    with pytest.raises(ConfigError):
        make_config(slots=4, max_inflight=4, sched_enabled=False)  # no admission cap


def test_config_accepts_valid_concurrency(make_config) -> None:
    cfg = make_config(slots=4, max_inflight=4, sched_enabled=True)
    assert cfg.slots == 4 and cfg.max_inflight == 4
