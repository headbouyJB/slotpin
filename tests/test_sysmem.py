"""Tests for the fail-open host/GPU memory sampler (v7 metric layer)."""

from __future__ import annotations

import os

from slotpin import sysmem

_KEYS = {"gtt_used_mb", "vram_used_mb", "mem_available_mb", "soc_power_w"}


def test_filesystem_type_and_is_tmpfs(tmp_path, monkeypatch) -> None:
    ram = tmp_path / "ram"
    disk = tmp_path / "disk"
    ram.mkdir()
    disk.mkdir()
    ram_r = os.path.realpath(str(ram))
    disk_r = os.path.realpath(str(disk))
    mounts = tmp_path / "mounts"
    mounts.write_text(
        f"rootfs / ext4 rw 0 0\n"
        f"tmpfs {ram_r} tmpfs rw 0 0\n"
        f"/dev/nvme0n1p2 {disk_r} ext4 rw 0 0\n"
    )
    monkeypatch.setattr(sysmem, "MOUNTS_PATH", str(mounts))
    # longest-prefix match picks the RAM mount for a path under it
    assert sysmem.filesystem_type(ram / "kv" / "x.bin") == "tmpfs"
    assert sysmem.filesystem_type(disk / "kv-cache") == "ext4"
    assert sysmem.is_tmpfs(ram / "x") is True
    assert sysmem.is_tmpfs(disk / "x") is False


def test_filesystem_type_failopen_when_mounts_missing(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(sysmem, "MOUNTS_PATH", str(tmp_path / "does-not-exist"))
    assert sysmem.filesystem_type(tmp_path / "x") is None
    assert sysmem.is_tmpfs(tmp_path / "x") is False


def _isolate(monkeypatch, tmp_path) -> None:
    """Point every source glob at a nonexistent path so a test host's real
    sysfs (amdgpu present on the host) can't leak into a test."""
    for name in ("GTT_GLOB", "VRAM_GLOB", "SOC_POWER_GLOB"):
        monkeypatch.setattr(sysmem, name, str(tmp_path / "none*"))
    monkeypatch.setattr(sysmem, "MEMINFO_PATH", str(tmp_path / "nomeminfo"))


def test_sample_mem_always_returns_keys_and_never_raises() -> None:
    # On the real host this reads sysfs; on CI/dev it reads nothing. Either
    # way the contract is: all keys are always present, no exception.
    result = sysmem.sample_mem()
    assert set(result) == _KEYS


def test_sample_mem_reads_fake_sysfs(tmp_path, monkeypatch) -> None:
    card = tmp_path / "card0" / "device"
    (card / "hwmon" / "hwmon9").mkdir(parents=True)
    (card / "mem_info_gtt_used").write_text(str(2 * 1024 * 1024))  # 2 MiB
    (card / "mem_info_vram_used").write_text(str(4 * 1024 * 1024))  # 4 MiB
    (card / "hwmon" / "hwmon9" / "power1_average").write_text(str(45_000_000))  # 45 W in uW
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal:      100 kB\nMemAvailable:    1024 kB\n")

    monkeypatch.setattr(sysmem, "GTT_GLOB", str(tmp_path / "card*" / "device" / "mem_info_gtt_used"))
    monkeypatch.setattr(sysmem, "VRAM_GLOB", str(tmp_path / "card*" / "device" / "mem_info_vram_used"))
    monkeypatch.setattr(
        sysmem, "SOC_POWER_GLOB", str(tmp_path / "card*" / "device" / "hwmon" / "hwmon*" / "power1_average")
    )
    monkeypatch.setattr(sysmem, "MEMINFO_PATH", str(meminfo))

    result = sysmem.sample_mem()
    assert result["gtt_used_mb"] == 2.0
    assert result["vram_used_mb"] == 4.0
    assert result["mem_available_mb"] == 1.0  # 1024 kB == 1 MiB
    assert result["soc_power_w"] == 45.0


def test_sample_mem_multi_card_sums(tmp_path, monkeypatch) -> None:
    for i, mib in ((0, 3), (1, 5)):
        d = tmp_path / f"card{i}" / "device"
        d.mkdir(parents=True)
        (d / "mem_info_gtt_used").write_text(str(mib * 1024 * 1024))
    monkeypatch.setattr(sysmem, "GTT_GLOB", str(tmp_path / "card*" / "device" / "mem_info_gtt_used"))
    monkeypatch.setattr(sysmem, "VRAM_GLOB", str(tmp_path / "none*"))
    monkeypatch.setattr(sysmem, "SOC_POWER_GLOB", str(tmp_path / "none*"))
    monkeypatch.setattr(sysmem, "MEMINFO_PATH", str(tmp_path / "nomeminfo"))
    result = sysmem.sample_mem()
    assert result["gtt_used_mb"] == 8.0  # 3 + 5 MiB summed across cards
    assert result["vram_used_mb"] is None
    assert result["soc_power_w"] is None


def test_sample_mem_missing_sources_are_none(tmp_path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    result = sysmem.sample_mem()
    assert result == dict.fromkeys(_KEYS, None)


# -- MemPoller (background poller + per-turn start/avg/max/end aggregation) ----


def _s(gtt, avail, vram=10.0, power=20.0):
    return {"gtt_used_mb": gtt, "vram_used_mb": vram, "mem_available_mb": avail, "soc_power_w": power}


def test_mem_poller_turn_stats_aggregates() -> None:
    p = sysmem.MemPoller(poll=False)
    p._hist.extend(
        [
            (100.0, _s(60000.0, 60000.0)),
            (101.0, _s(66000.0, 54000.0)),
            (102.0, _s(63000.0, 57000.0)),
        ]
    )
    p._latest = _s(63000.0, 57000.0)
    m = p.turn_stats(100.0, 102.0)
    # start = first in-window sample
    assert m["gtt_used_start_mb"] == 60000.0
    assert m["mem_available_start_mb"] == 60000.0
    # avg = mean over the window
    assert m["gtt_used_avg_mb"] == 63000.0
    assert m["mem_available_avg_mb"] == 57000.0
    # peak GPU residency (max gtt) / peak RAM pressure (min available)
    assert m["gtt_used_max_mb"] == 66000.0
    assert m["mem_available_min_mb"] == 54000.0
    # end = latest snapshot (backward-compatible keys)
    assert m["gtt_used_mb"] == 63000.0
    assert m["mem_available_mb"] == 57000.0
    # passthrough end values
    assert m["vram_used_mb"] == 10.0
    assert m["soc_power_w"] == 20.0
    p.stop()


def test_mem_poller_window_excludes_out_of_range() -> None:
    p = sysmem.MemPoller(poll=False)
    p._hist.extend(
        [
            (90.0, _s(1.0, 1.0)),  # before window
            (100.0, _s(60000.0, 60000.0)),  # in window
            (110.0, _s(999.0, 999.0)),  # after window
        ]
    )
    p._latest = _s(60000.0, 60000.0)
    m = p.turn_stats(99.0, 101.0)
    assert m["gtt_used_max_mb"] == 60000.0  # only the single in-window sample counts
    assert m["gtt_used_avg_mb"] == 60000.0
    p.stop()


def test_mem_poller_falls_back_to_latest_when_window_empty() -> None:
    p = sysmem.MemPoller(poll=False)
    p._latest = _s(42000.0, 80000.0)  # history empty; window catches nothing
    m = p.turn_stats(0.0, 1.0)
    assert m["gtt_used_start_mb"] == m["gtt_used_avg_mb"] == m["gtt_used_max_mb"] == 42000.0
    assert m["gtt_used_mb"] == 42000.0
    p.stop()


def test_mem_poller_samples_once_when_no_data(monkeypatch) -> None:
    p = sysmem.MemPoller(poll=False)  # no _latest, no _hist
    calls = []
    monkeypatch.setattr(sysmem, "sample_mem", lambda: (calls.append(1), _s(7.0, 8.0))[1])
    m = p.turn_stats(0.0, 1.0)
    assert len(calls) == 1  # exactly one on-demand read as a last resort
    assert m["gtt_used_mb"] == 7.0
    assert m["mem_available_mb"] == 8.0
    p.stop()


def test_mem_poller_fail_open_none_values() -> None:
    p = sysmem.MemPoller(poll=False)
    p._hist.append((100.0, dict.fromkeys(_KEYS, None)))
    p._latest = dict.fromkeys(_KEYS, None)
    m = p.turn_stats(100.0, 100.0)
    for k in (
        "gtt_used_mb",
        "gtt_used_start_mb",
        "gtt_used_avg_mb",
        "gtt_used_max_mb",
        "mem_available_mb",
        "mem_available_start_mb",
        "mem_available_avg_mb",
        "mem_available_min_mb",
    ):
        assert m[k] is None
    p.stop()


def test_mem_poller_snapshot_and_stop() -> None:
    p = sysmem.MemPoller(poll=False)
    assert p.snapshot() is None  # nothing polled yet
    p._latest = _s(1.0, 2.0)
    assert p.snapshot()["gtt_used_mb"] == 1.0
    p.stop()  # idempotent / safe with no running thread
