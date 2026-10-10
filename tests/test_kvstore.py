"""Tests for slotpin.kvstore.KVStore: disk warmth, rate limiting, busy-save
hardening, alias fallback, and TTL GC."""

from __future__ import annotations

import os
import time

from slotpin import kvstore as kvstore_mod
from slotpin.kvstore import KVStore
from slotpin.metrics import MetricsWriter
from slotpin.pool import CacheRamPool
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
    pool = CacheRamPool(cfg)
    admin = SlotAdmin(cfg)
    metrics = MetricsWriter(cfg)
    kv = KVStore(cfg, admin, pool, state, metrics)
    return cfg, kv, state, pool, admin, metrics


# ---------------------------------------------------------------------------
# record_prefix threshold
# ---------------------------------------------------------------------------


def test_record_prefix_threshold(make_config, fake_upstream):
    cfg, kv, *_ = _build_kv(make_config, fake_upstream, recur_threshold=3)
    assert kv.record_prefix("phX") is False
    assert kv.record_prefix("phX") is False
    assert kv.record_prefix("phX") is True  # 3rd occurrence hits the threshold
    assert kv.record_prefix("phX") is True  # stays warm afterwards


# ---------------------------------------------------------------------------
# warm_worthy: interactive instant warm vs recurrence-gated
# ---------------------------------------------------------------------------


def test_warm_worthy_interactive_is_instant(make_config, fake_upstream):
    cfg, kv, *_ = _build_kv(make_config, fake_upstream, recur_threshold=100)
    assert kv.warm_worthy("ph-interactive", interactive=True) is True


def test_warm_worthy_non_interactive_needs_recurrence(make_config, fake_upstream):
    cfg, kv, *_ = _build_kv(make_config, fake_upstream, recur_threshold=3)
    # warm_worthy is a PURE query now; recurrence is driven by record_prefix
    # (called once per turn by the pipeline), not by warm_worthy itself.
    assert kv.warm_worthy("ph-other", interactive=False) is False
    kv.record_prefix("ph-other")
    assert kv.warm_worthy("ph-other", interactive=False) is False
    kv.record_prefix("ph-other")
    assert kv.warm_worthy("ph-other", interactive=False) is False
    kv.record_prefix("ph-other")  # 3rd occurrence hits recur_threshold
    assert kv.warm_worthy("ph-other", interactive=False) is True


# ---------------------------------------------------------------------------
# is_cold vs is_cold_interactive (invariant 1: interactive gate ignores pool)
# ---------------------------------------------------------------------------


def test_is_cold_trusts_pool_but_is_cold_interactive_ignores_it(make_config, fake_upstream):
    cfg, kv, state, pool, *_ = _build_kv(make_config, fake_upstream)
    slot = 0
    with state.lock:
        state.slot_last[slot] = "other-ph"
    pool.note_eviction("ph1", 100)  # pool believes ph1 is still warm in cache-ram

    # Generic gate trusts the pool: slot occupant differs, but pool says warm.
    assert kv.is_cold(slot, "ph1") is False

    # Interactive gate ignores the pool entirely: only slot_last matters.
    assert kv.is_cold_interactive(slot, "ph1") is True


def test_is_cold_true_when_occupant_differs_and_not_in_pool(make_config, fake_upstream):
    cfg, kv, state, pool, *_ = _build_kv(make_config, fake_upstream)
    slot = 0
    with state.lock:
        state.slot_last[slot] = "other-ph"
    assert kv.is_cold(slot, "ph1") is True


def test_is_cold_interactive_false_when_occupant_already_matches(make_config, fake_upstream):
    cfg, kv, state, pool, *_ = _build_kv(make_config, fake_upstream)
    slot = 0
    with state.lock:
        state.slot_last[slot] = "ph1"
    assert kv.is_cold_interactive(slot, "ph1") is False


# ---------------------------------------------------------------------------
# restore_blocking: alias fallback on miss (invariant 5)
# ---------------------------------------------------------------------------


def test_restore_blocking_falls_back_to_latest_alias_on_miss(make_config, fake_upstream):
    cfg, kv, state, pool, admin, metrics = _build_kv(make_config, fake_upstream, latest_alias="the-latest")
    fake_upstream.fail_restore_filename("ph1.bin")

    ok = kv.restore_blocking(slot=0, ph="ph1", latest_alias="the-latest")

    assert ok is True
    calls = fake_upstream.admin_calls_matching(action="restore")
    assert [c["filename"] for c in calls] == ["ph1.bin", "the-latest.bin"]
    assert calls[0]["ts"] <= calls[1]["ts"]
    with state.lock:
        assert state.counters["kv_restore_misses"] == 1
        assert state.counters["kv_restores"] == 1
    assert "ph1" not in pool  # a successful restore calls pool.mark_restored(ph)


def test_restore_blocking_no_alias_configured_returns_false_on_miss(make_config, fake_upstream):
    cfg, kv, state, *_ = _build_kv(make_config, fake_upstream)
    fake_upstream.fail_restore_filename("ph1.bin")
    ok = kv.restore_blocking(slot=0, ph="ph1", latest_alias=None)
    assert ok is False
    with state.lock:
        assert state.counters["kv_restore_misses"] == 1
        assert state.counters.get("kv_restores", 0) == 0


def test_restore_blocking_succeeds_on_first_try_without_alias_call(make_config, fake_upstream):
    cfg, kv, *_ = _build_kv(make_config, fake_upstream, latest_alias="the-latest")
    ok = kv.restore_blocking(slot=0, ph="ph1", latest_alias="the-latest")
    assert ok is True
    calls = fake_upstream.admin_calls_matching(action="restore")
    assert [c["filename"] for c in calls] == ["ph1.bin"]  # no alias fallback needed


def test_restore_blocking_admin_timeout_returns_false_gracefully(make_config, fake_upstream):
    cfg, kv, *_ = _build_kv(make_config, fake_upstream, admin_timeout_s=0.2)
    fake_upstream.hang_admin(0, "restore", seconds=2.0)
    ok = kv.restore_blocking(slot=0, ph="ph1", latest_alias=None)
    assert ok is False  # SlotAdmin never raises; a timed-out restore is just a failure


# ---------------------------------------------------------------------------
# save_async: rate limiting per prefix
# ---------------------------------------------------------------------------


def test_save_async_rate_limits_per_prefix(make_config, fake_upstream, poll_until):
    cfg, kv, *_ = _build_kv(make_config, fake_upstream, prefix_save_min_interval_s=0.15)
    kv.save_async(slot=0, ph="ph1", latest_alias=None)
    poll_until(lambda: fake_upstream.admin_calls_matching(action="save") or None, deadline_s=2.0)
    assert len(fake_upstream.admin_calls_matching(action="save")) == 1

    # Immediately trying again for the same prefix is rate-limited (no new call).
    kv.save_async(slot=0, ph="ph1", latest_alias=None)
    time.sleep(0.05)
    assert len(fake_upstream.admin_calls_matching(action="save")) == 1

    # After the interval elapses, a save for the same prefix is allowed again.
    time.sleep(0.15)
    kv.save_async(slot=0, ph="ph1", latest_alias=None)
    poll_until(
        lambda: len(fake_upstream.admin_calls_matching(action="save")) >= 2 or None, deadline_s=2.0
    )
    assert len(fake_upstream.admin_calls_matching(action="save")) == 2


def test_save_async_different_prefixes_are_independent(make_config, fake_upstream, poll_until):
    cfg, kv, *_ = _build_kv(make_config, fake_upstream, prefix_save_min_interval_s=10.0)
    kv.save_async(slot=0, ph="ph1", latest_alias=None)
    kv.save_async(slot=0, ph="ph2", latest_alias=None)
    poll_until(
        lambda: len(fake_upstream.admin_calls_matching(action="save")) >= 2 or None, deadline_s=2.0
    )
    filenames = {c["filename"] for c in fake_upstream.admin_calls_matching(action="save")}
    assert filenames == {"ph1.bin", "ph2.bin"}


# ---------------------------------------------------------------------------
# busy-slot refusal + single retry (invariant 8)
# ---------------------------------------------------------------------------


def test_save_async_refuses_while_slot_busy_and_gives_up_after_one_retry(
    make_config, fake_upstream, monkeypatch, poll_until
):
    monkeypatch.setattr(kvstore_mod, "_BUSY_RETRY_DELAY_S", 0.05)
    cfg, kv, state, *_ = _build_kv(make_config, fake_upstream, prefix_save_min_interval_s=0.01)
    with state.lock:
        state.slot_busy[0] = True

    kv.save_async(slot=0, ph="ph1", latest_alias=None)
    time.sleep(0.02)
    assert fake_upstream.admin_calls_matching(action="save") == []  # first attempt refused

    # Wait past the retry delay: slot is still busy, so the retry also gives up.
    time.sleep(0.2)
    assert fake_upstream.admin_calls_matching(action="save") == []


def test_save_async_retry_succeeds_once_slot_frees_up(
    make_config, fake_upstream, monkeypatch, poll_until
):
    monkeypatch.setattr(kvstore_mod, "_BUSY_RETRY_DELAY_S", 0.05)
    cfg, kv, state, *_ = _build_kv(make_config, fake_upstream, prefix_save_min_interval_s=0.01)
    with state.lock:
        state.slot_busy[0] = True

    kv.save_async(slot=0, ph="ph1", latest_alias=None)
    time.sleep(0.02)
    assert fake_upstream.admin_calls_matching(action="save") == []

    with state.lock:
        state.slot_busy[0] = False  # slot frees up before the retry timer fires

    poll_until(
        lambda: fake_upstream.admin_calls_matching(action="save") or None, deadline_s=2.0
    )
    assert len(fake_upstream.admin_calls_matching(action="save")) == 1


# ---------------------------------------------------------------------------
# ttl_evict removes expired snapshots
# ---------------------------------------------------------------------------


def test_ttl_evict_removes_only_expired_snapshots(make_config, fake_upstream, tmp_path):
    save_path = tmp_path / "kv-cache"
    save_path.mkdir()
    cfg, kv, *_ = _build_kv(
        make_config, fake_upstream, slot_save_path=save_path, snapshot_ttl_h=1.0 / 3600.0
    )
    old_file = save_path / "old-ph.bin"
    old_file.write_bytes(b"x")
    fresh_file = save_path / "fresh-ph.bin"
    fresh_file.write_bytes(b"x")

    old_time = time.time() - 10  # 10s old vs a 1s ttl -> expired
    os.utime(old_file, (old_time, old_time))

    kv.ttl_evict()

    assert not old_file.exists()
    assert fresh_file.exists()


def test_ttl_evict_skips_the_alias_file(make_config, fake_upstream, tmp_path):
    save_path = tmp_path / "kv-cache"
    save_path.mkdir()
    cfg, kv, *_ = _build_kv(
        make_config,
        fake_upstream,
        slot_save_path=save_path,
        snapshot_ttl_h=1.0 / 3600.0,
        latest_alias="the-alias",
    )
    alias_file = save_path / "the-alias.bin"
    alias_file.write_bytes(b"x")
    old_time = time.time() - 10
    os.utime(alias_file, (old_time, old_time))

    kv.ttl_evict()

    assert alias_file.exists()  # never evicted regardless of age


# ---------------------------------------------------------------------------
# load_stats_from_disk rebuilds warm set
# ---------------------------------------------------------------------------


def test_load_stats_from_disk_rebuilds_warm_set(make_config, fake_upstream, tmp_path):
    save_path = tmp_path / "kv-cache"
    save_path.mkdir()
    (save_path / "phA.bin").write_bytes(b"x")
    (save_path / "phB.bin").write_bytes(b"x")
    (save_path / "the-alias.bin").write_bytes(b"x")  # must be skipped, not a real prefix

    cfg, kv, *_ = _build_kv(
        make_config, fake_upstream, slot_save_path=save_path, latest_alias="the-alias", recur_threshold=3
    )

    kv.load_stats_from_disk()

    # Warm on first sight post-bootstrap (no need to re-accumulate recur_threshold).
    assert kv.warm_worthy("phA", interactive=False) is True
    assert kv.warm_worthy("phB", interactive=False) is True

    stats = kv.snapshot_stats()
    assert "the-alias" not in stats["prefix_counts"]
    assert stats["tracked_prefixes"] >= 2


def test_load_stats_from_disk_noop_when_dir_missing(make_config, fake_upstream, tmp_path):
    save_path = tmp_path / "does-not-exist"
    cfg, kv, *_ = _build_kv(make_config, fake_upstream, slot_save_path=save_path)
    kv.load_stats_from_disk()  # must not raise
    assert kv.snapshot_stats()["tracked_prefixes"] == 0


# ---------------------------------------------------------------------------
# _update_alias: a symlink, not a copy
# ---------------------------------------------------------------------------


def test_update_alias_creates_a_symlink_not_a_copy(make_config, fake_upstream, tmp_path, poll_until):
    save_path = tmp_path / "kv-cache"
    save_path.mkdir()
    (save_path / "ph1.bin").write_bytes(b"snapshot-bytes")
    cfg, kv, *_ = _build_kv(
        make_config, fake_upstream, slot_save_path=save_path, prefix_save_min_interval_s=0.0
    )

    kv._update_alias("ph1", "the-latest")

    alias_path = save_path / "the-latest.bin"
    assert alias_path.is_symlink()
    assert os.readlink(alias_path) == "ph1.bin"
    assert alias_path.read_bytes() == b"snapshot-bytes"


def test_update_alias_maintains_ckpt_sidecar_when_present(make_config, fake_upstream, tmp_path):
    save_path = tmp_path / "kv-cache"
    save_path.mkdir()
    (save_path / "ph1.bin").write_bytes(b"snapshot")
    (save_path / "ph1.bin.ckpt").write_bytes(b"ckpt-state")
    cfg, kv, *_ = _build_kv(make_config, fake_upstream, slot_save_path=save_path)

    kv._update_alias("ph1", "the-latest")

    ckpt_alias = save_path / "the-latest.bin.ckpt"
    assert ckpt_alias.is_symlink()
    assert os.readlink(ckpt_alias) == "ph1.bin.ckpt"


def test_update_alias_drops_stale_ckpt_alias_when_new_snapshot_has_none(
    make_config, fake_upstream, tmp_path
):
    save_path = tmp_path / "kv-cache"
    save_path.mkdir()
    (save_path / "ph1.bin").write_bytes(b"snapshot")
    # A stale alias .ckpt from a PREVIOUS snapshot that did have a sidecar.
    (save_path / "the-latest.bin.ckpt").symlink_to("ph0.bin.ckpt")
    cfg, kv, *_ = _build_kv(make_config, fake_upstream, slot_save_path=save_path)

    kv._update_alias("ph1", "the-latest")  # ph1 has no .ckpt sidecar

    assert not (save_path / "the-latest.bin.ckpt").exists()
    assert not (save_path / "the-latest.bin.ckpt").is_symlink()


def test_update_alias_re_points_an_existing_alias_symlink(make_config, fake_upstream, tmp_path):
    save_path = tmp_path / "kv-cache"
    save_path.mkdir()
    (save_path / "ph1.bin").write_bytes(b"first")
    (save_path / "ph2.bin").write_bytes(b"second")
    cfg, kv, *_ = _build_kv(make_config, fake_upstream, slot_save_path=save_path)

    kv._update_alias("ph1", "the-latest")
    assert (save_path / "the-latest.bin").read_bytes() == b"first"

    kv._update_alias("ph2", "the-latest")
    assert os.readlink(save_path / "the-latest.bin") == "ph2.bin"
    assert (save_path / "the-latest.bin").read_bytes() == b"second"


# ---------------------------------------------------------------------------
# save_now integrity guard: warn when the saved file never lands in
# slot_save_path (upstream --slot-save-path mismatch -> KV going to tmpfs/RAM)
# ---------------------------------------------------------------------------


def test_save_now_warns_once_when_saved_file_absent(make_config, fake_upstream, tmp_path, caplog):
    import logging

    save_dir = tmp_path / "expected-nvme"
    save_dir.mkdir()
    # Default fake acks the save but writes nothing to slot_save_path -- mimics
    # llama-server saving under a DIFFERENT --slot-save-path (e.g. tmpfs).
    cfg, kv, *_ = _build_kv(
        make_config, fake_upstream, slot_save_path=str(save_dir), prefix_save_min_interval_s=0.0
    )
    with caplog.at_level(logging.WARNING, logger="slotpin.kvstore"):
        kv.save_now(slot=0, ph="alpha", latest_alias=None)
        kv.save_now(slot=0, ph="beta", latest_alias=None)
    warns = [r.getMessage() for r in caplog.records if "absent" in r.getMessage()]
    assert len(warns) == 1  # once, not per-save
    assert "slot_save_path" in warns[0]


def test_save_now_silent_when_saved_file_lands(make_config, fake_upstream, tmp_path, caplog):
    import logging

    save_dir = tmp_path / "nvme-kv"
    cfg, kv, *_ = _build_kv(
        make_config, fake_upstream, slot_save_path=str(save_dir), prefix_save_min_interval_s=0.0
    )
    # Make the fake write the .bin into slot_save_path (paths aligned, as in a
    # correct deployment) -> no mismatch warning.
    fake_upstream.state.save_creates_file = True
    fake_upstream.state.save_dir = cfg.slot_save_path
    with caplog.at_level(logging.WARNING, logger="slotpin.kvstore"):
        kv.save_now(slot=0, ph="alpha", latest_alias=None)
    assert not [r for r in caplog.records if "absent" in r.getMessage()]


# ---------------------------------------------------------------------------
# snapshot size on kv_adaptive records
# ---------------------------------------------------------------------------


def _kv_records(cfg):
    import json

    with open(cfg.metrics_file) as f:
        return [json.loads(line) for line in f if '"kv_adaptive"' in line]


def test_kv_records_carry_snapshot_bytes(make_config, fake_upstream, tmp_path):
    save_dir = tmp_path / "kv"
    cfg, kv, *_ = _build_kv(
        make_config, fake_upstream, slot_save_path=str(save_dir), prefix_save_min_interval_s=0.0
    )
    fake_upstream.enable_save_files(cfg.slot_save_path)
    kv.save_now(slot=0, ph="alpha", latest_alias=None)  # fake writes 16 bytes
    assert kv.restore_blocking(slot=0, ph="alpha", latest_alias=None) is True
    save, restore = _kv_records(cfg)
    assert save["kv_adaptive"] == "save" and save["bytes"] == len(b"fake-kv-snapshot")
    assert restore["kv_adaptive"] == "restore" and restore["bytes"] == len(b"fake-kv-snapshot")
    assert str(save_dir) not in str(save) + str(restore)  # no full path logged


def test_kv_record_bytes_null_when_file_missing_or_call_failed(make_config, fake_upstream, tmp_path):
    cfg, kv, *_ = _build_kv(
        make_config, fake_upstream, slot_save_path=str(tmp_path / "kv"), prefix_save_min_interval_s=0.0
    )
    kv.save_now(slot=0, ph="alpha", latest_alias=None)  # acked, but nothing on local disk
    fake_upstream.fail_restore_filename("ghost.bin")
    assert kv.restore_blocking(slot=0, ph="ghost", latest_alias=None) is False
    save, restore = _kv_records(cfg)
    assert save["ok"] is True and save["bytes"] is None
    assert restore["ok"] is False and restore["bytes"] is None


def test_snapshot_bytes_never_raises(make_config, fake_upstream, tmp_path, monkeypatch):
    cfg, kv, *_ = _build_kv(make_config, fake_upstream, slot_save_path=str(tmp_path))
    (tmp_path / "a.bin").write_bytes(b"12345")
    assert kv._snapshot_bytes("a.bin", True) == 5
    assert kv._snapshot_bytes("a.bin", False) is None
    assert kv._snapshot_bytes("nope.bin", True) is None
    os.symlink("nowhere.bin", tmp_path / "dangling.bin")
    assert kv._snapshot_bytes("dangling.bin", True) is None
    os.symlink("a.bin", tmp_path / "alias.bin")
    assert kv._snapshot_bytes("alias.bin", True) == 5

    def boom(_p):
        raise PermissionError("denied")

    monkeypatch.setattr(kvstore_mod.os, "stat", boom)
    assert kv._snapshot_bytes("a.bin", True) is None
