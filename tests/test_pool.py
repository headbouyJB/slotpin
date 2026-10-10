"""Tests for slotpin.pool.CacheRamPool: the inferred cache-ram model."""

from __future__ import annotations

import threading

from slotpin.config import Config
from slotpin.pool import CacheRamPool


def _cfg(**kw):
    # Generous default cap so tests not explicitly exercising the trim logic
    # aren't accidentally trimmed.
    kw.setdefault("cache_ram_mb", 1024)
    kw.setdefault("bytes_per_token", 1)
    kw.setdefault("cram_safety", 1.0)
    return Config(**kw)


def test_eviction_ordering_most_recent_first():
    pool = CacheRamPool(_cfg())
    pool.note_eviction("fp1", 10)
    pool.note_eviction("fp2", 10)
    snap = pool.snapshot()
    assert [e["fp"] for e in snap] == ["fp2", "fp1"]


def test_contains_reflects_eviction_and_removal():
    pool = CacheRamPool(_cfg())
    assert "fp1" not in pool
    pool.note_eviction("fp1", 5)
    assert "fp1" in pool


def test_token_cap_trims_oldest_first():
    # cram_token_cap = int(cache_ram_mb * 1024**2 / bytes_per_token * cram_safety)
    # Choose values so cap == 250 tokens exactly.
    cfg = _cfg(cache_ram_mb=250 / 1024**2, bytes_per_token=1, cram_safety=1.0)
    pool = CacheRamPool(cfg)
    assert cfg.cram_token_cap == 250

    pool.note_eviction("fp1", 150)
    pool.note_eviction("fp2", 150)  # total 300 > 250 -> oldest (fp1) trimmed
    assert "fp1" not in pool
    assert "fp2" in pool
    snap = pool.snapshot()
    assert [e["fp"] for e in snap] == ["fp2"]


def test_24_entry_cap_enforced_even_with_zero_tokens():
    pool = CacheRamPool(_cfg())
    for i in range(30):
        pool.note_eviction(f"fp{i}", 0)
    snap = pool.snapshot()
    assert len(snap) == 24
    fps = {e["fp"] for e in snap}
    # the oldest 6 (fp0..fp5) must have been dropped
    for i in range(6):
        assert f"fp{i}" not in fps
    for i in range(6, 30):
        assert f"fp{i}" in fps


def test_mark_restored_removes_entry():
    pool = CacheRamPool(_cfg())
    pool.note_eviction("fp1", 10)
    pool.mark_restored("fp1")
    assert "fp1" not in pool
    assert pool.snapshot() == []


def test_clear_drops_everything():
    pool = CacheRamPool(_cfg())
    pool.note_eviction("fp1", 10)
    pool.note_eviction("fp2", 20)
    pool.clear()
    assert pool.snapshot() == []
    assert "fp1" not in pool
    assert "fp2" not in pool


def test_snapshot_shape():
    pool = CacheRamPool(_cfg())
    pool.note_eviction("fp1", 42)
    snap = pool.snapshot()
    assert len(snap) == 1
    entry = snap[0]
    assert set(entry.keys()) == {"fp", "tokens", "evicted_ts"}
    assert entry["fp"] == "fp1"
    assert entry["tokens"] == 42
    assert isinstance(entry["evicted_ts"], float)


def test_trim_is_idempotent_public_entry_point():
    pool = CacheRamPool(_cfg())
    pool.note_eviction("fp1", 10)
    pool.trim()
    pool.trim()
    assert "fp1" in pool


def test_thread_safety_smoke_concurrent_note_eviction():
    pool = CacheRamPool(_cfg())
    errors: list[Exception] = []

    def worker(i: int) -> None:
        try:
            for j in range(50):
                pool.note_eviction(f"fp-{i}-{j}", 1)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
        assert not t.is_alive()

    assert errors == []
    # No crash/deadlock, and the 24-entry cap still holds under contention.
    assert len(pool.snapshot()) <= 24
