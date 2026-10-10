"""Tests for slotpin.reload_detect.ReloadDetector: model reload / restart
detection and the reset-on-reload side effects (invariant 7)."""

from __future__ import annotations

from slotpin.pool import CacheRamPool
from slotpin.reload_detect import ReloadDetector
from slotpin.slot_admin import SlotAdmin
from slotpin.state import ProxyState


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
    rd = ReloadDetector(cfg, admin, pool, state)
    return cfg, rd, state, pool


def test_model_path_change_detected(make_config, fake_upstream):
    fake_upstream.replace_props({"model_path": "model-a", "uptime_s": 100.0})
    cfg, rd, state, pool = _build(make_config, fake_upstream)

    assert rd.check_once() is False  # first poll only establishes the baseline

    fake_upstream.replace_props({"model_path": "model-b", "uptime_s": 5.0})
    assert rd.check_once() is True


def test_uptime_regression_detected(make_config, fake_upstream):
    fake_upstream.replace_props({"model_path": "model-a", "uptime_s": 500.0})
    cfg, rd, state, pool = _build(make_config, fake_upstream)
    assert rd.check_once() is False

    fake_upstream.set_props(uptime_s=10.0)  # smaller than before -> process restarted
    assert rd.check_once() is True


def test_no_change_is_not_a_reload(make_config, fake_upstream):
    fake_upstream.replace_props({"model_path": "model-a", "uptime_s": 500.0})
    cfg, rd, state, pool = _build(make_config, fake_upstream)
    assert rd.check_once() is False

    fake_upstream.set_props(uptime_s=510.0)  # uptime advancing normally
    assert rd.check_once() is False


def test_missing_props_never_counts_as_reload(make_config, fake_upstream):
    fake_upstream.replace_props({"model_path": "model-a", "uptime_s": 500.0})
    cfg, rd, state, pool = _build(make_config, fake_upstream)
    assert rd.check_once() is False

    fake_upstream.set_props_status(500)  # /props becomes unavailable
    assert rd.check_once() is False

    # Coming back with an unchanged signature is still not a reload.
    fake_upstream.set_props_status(None)
    assert rd.check_once() is False


def test_partial_props_missing_fields_degrade_gracefully(make_config, fake_upstream):
    # Baseline has both model_path and uptime_s.
    fake_upstream.replace_props({"model_path": "model-a", "uptime_s": 500.0})
    cfg, rd, state, pool = _build(make_config, fake_upstream)
    assert rd.check_once() is False

    # Next poll is missing model_path entirely (can't compare -> not a reload
    # on that signal), and uptime advanced normally.
    fake_upstream.replace_props({"uptime_s": 510.0})
    assert rd.check_once() is False


def test_reload_detection_clears_pool_and_slot_last_and_increments_counter(make_config, fake_upstream):
    fake_upstream.replace_props({"model_path": "model-a", "uptime_s": 500.0})
    cfg, rd, state, pool = _build(make_config, fake_upstream)
    assert rd.check_once() is False

    pool.note_eviction("some-fp", 100)
    with state.lock:
        state.slot_last[0] = "some-prefix-hash"
        before = state.counters["reloads_detected"]

    fake_upstream.replace_props({"model_path": "model-b", "uptime_s": 1.0})
    assert rd.check_once() is True

    assert pool.snapshot() == []
    with state.lock:
        assert state.slot_last[0] is None
        assert state.counters["reloads_detected"] == before + 1


def test_start_and_stop_do_not_raise(make_config, fake_upstream):
    fake_upstream.replace_props({"model_path": "model-a", "uptime_s": 500.0})
    cfg, rd, state, pool = _build(make_config, fake_upstream, reload_poll_s=0.05)
    rd.start()
    rd.start()  # calling twice must be a no-op, not a crash
    rd.stop()


def test_reload_detect_disabled_start_is_noop(make_config, fake_upstream):
    cfg, rd, state, pool = _build(make_config, fake_upstream, reload_detect=False)
    rd.start()  # must not start a polling thread nor raise
    rd.stop()
