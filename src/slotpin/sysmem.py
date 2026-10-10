"""Fail-open host/GPU memory sampler (GTT / VRAM / available RAM).

Part of the v7 metric layer. Reads AMD GPU memory residency from sysfs
(``/sys/class/drm/card*/device/mem_info_*``) and system RAM from
``/proc/meminfo``. Every function swallows its own errors and returns
``None`` values on any failure, so a metrics-time sample can never break the
request path (same fail-open discipline as :mod:`slotpin.metrics`). Linux +
amdgpu specific; on any other platform (or a non-AMD GPU) it simply returns
all-``None``, which the metric consumer treats as "not sampled".

:func:`sample_mem` is point-in-time. For per-turn start/avg/max/end aggregates
(the intra-turn trajectory -- e.g. the memory peak while prefilling a large
context, which an end-of-turn spot sample misses) use :class:`MemPoller`, a
background poller that keeps a short timestamped history and summarises a turn
over its ``[t0, t1]`` window. On the request path only the spot sampler or the
in-memory poller is read, so neither blocks the relay.
"""

from __future__ import annotations

import glob
import logging
import os
import threading
import time
from collections import deque

logger = logging.getLogger("slotpin.sysmem")

_MB = 1024 * 1024

# Overridable for tests (a temp dir mimicking the sysfs/proc layout).
GTT_GLOB = "/sys/class/drm/card*/device/mem_info_gtt_used"
VRAM_GLOB = "/sys/class/drm/card*/device/mem_info_vram_used"
MEMINFO_PATH = "/proc/meminfo"
MOUNTS_PATH = "/proc/mounts"
# SoC/GPU package power in microwatts (amdgpu hwmon, under the DRM card so the
# path is stable across hwmonN renumbering). Instantaneous compute-side power
# -- NOT whole-box (that's the smart plug); a fine-grained on-box signal that
# needs no external dependency.
SOC_POWER_GLOB = "/sys/class/drm/card*/device/hwmon/hwmon*/power1_average"

_KEYS = ("gtt_used_mb", "vram_used_mb", "mem_available_mb", "soc_power_w")


def _read_int(path: str) -> int | None:
    try:
        with open(path, encoding="ascii") as fh:
            return int(fh.read().strip())
    except (OSError, ValueError):
        return None


def _sum_glob(pattern: str) -> int | None:
    """Sum an integer sysfs counter across every matching card; None if none read."""
    total = 0
    found = False
    for path in glob.glob(pattern):
        value = _read_int(path)
        if value is not None:
            total += value
            found = True
    return total if found else None


def _mem_available_bytes(meminfo_path: str) -> int | None:
    try:
        with open(meminfo_path, encoding="ascii") as fh:
            for line in fh:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024  # value is in kB
    except (OSError, ValueError, IndexError):
        return None
    return None


def filesystem_type(path, mounts_path: str | None = None) -> str | None:
    """Best-effort filesystem type backing ``path`` (the longest mount-point
    prefix in ``/proc/mounts``). Fail-open: ``None`` on any error or non-Linux
    host. Used to warn when KV snapshots would land on a RAM filesystem.
    """
    # Resolve the module global at call time so tests can monkeypatch MOUNTS_PATH
    # (a default-arg value would bind at definition time).
    mounts = MOUNTS_PATH if mounts_path is None else mounts_path
    try:
        target = os.path.realpath(str(path))
        best_mp = ""
        best_type = None
        with open(mounts, encoding="utf-8") as fh:
            for line in fh:
                parts = line.split()
                if len(parts) < 3:
                    continue
                mp, fstype = parts[1], parts[2]
                covered = target == mp or target.startswith(mp.rstrip("/") + "/") or mp == "/"
                if covered and len(mp) >= len(best_mp):
                    best_mp = mp
                    best_type = fstype
        return best_type
    except (OSError, ValueError):
        return None


def is_tmpfs(path, mounts_path: str | None = None) -> bool:
    """True if ``path`` is on a RAM-backed filesystem (tmpfs/ramfs) -- where KV
    snapshots eat RAM and steal memory bandwidth from decode instead of paging
    off NVMe as the warm lane intends."""
    return filesystem_type(path, mounts_path) in ("tmpfs", "ramfs")


def sample_mem() -> dict[str, float | None]:
    """Point-in-time GPU/host memory sample in MiB. Never raises.

    Returns a dict with keys ``gtt_used_mb``, ``vram_used_mb``,
    ``mem_available_mb`` -- any of which is ``None`` when that source could
    not be read (non-Linux, non-amdgpu, permissions, missing counter).
    """
    try:
        gtt = _sum_glob(GTT_GLOB)
        vram = _sum_glob(VRAM_GLOB)
        avail = _mem_available_bytes(MEMINFO_PATH)
        # First matching amdgpu power sensor (microwatts -> watts).
        power_uw = None
        for path in glob.glob(SOC_POWER_GLOB):
            power_uw = _read_int(path)
            if power_uw is not None:
                break
        return {
            "gtt_used_mb": round(gtt / _MB, 1) if gtt is not None else None,
            "vram_used_mb": round(vram / _MB, 1) if vram is not None else None,
            "mem_available_mb": round(avail / _MB, 1) if avail is not None else None,
            "soc_power_w": round(power_uw / 1_000_000, 2) if power_uw is not None else None,
        }
    except Exception:  # noqa: BLE001 -- fail-open by contract
        logger.debug("sysmem sample failed", exc_info=True)
        return dict.fromkeys(_KEYS, None)


class MemPoller:
    """Background-samples :func:`sample_mem` every ``interval_s`` and, per turn,
    reports start/avg/max/end aggregates over the turn's ``[t0, t1]`` window.

    Mirrors :class:`slotpin.energy.EnergyPoller` (daemon thread, in-memory, zero
    HTTP -- here zero syscalls -- on the request path) but keeps a short
    wall-clock-timestamped history so a turn can be *summarised*, not just spot
    sampled. Fail-open by contract: any read failure yields ``None`` aggregates
    and never touches the request path.

    ``turn_stats`` keeps the plain ``sample_mem`` keys (``gtt_used_mb`` /
    ``vram_used_mb`` / ``mem_available_mb`` / ``soc_power_w``) as the **end**
    (latest) values for backward compatibility, and adds, for the two
    fit-critical series, start/avg plus the worst-case extreme:
    ``gtt_used_{start,avg,max}_mb`` (peak GPU residency) and
    ``mem_available_{start,avg,min}_mb`` (``min`` available == peak RAM pressure).
    """

    def __init__(self, interval_s: float = 1.0, *, poll: bool = True, history_s: float = 7200.0) -> None:
        self._interval = max(0.05, interval_s)
        self._lock = threading.Lock()
        self._latest: dict[str, float | None] | None = None
        # Bounded history so a whole turn (e.g. a multi-minute long-context
        # prefill) is covered without unbounded growth; ~2 h at 1 s is trivial.
        maxlen = max(16, int(history_s / self._interval) + 4)
        self._hist: deque[tuple[float, dict[str, float | None]]] = deque(maxlen=maxlen)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        if poll:
            self._thread = threading.Thread(target=self._loop, name="mem-poller", daemon=True)
            self._thread.start()

    def _tick(self) -> dict[str, float | None]:
        s = sample_mem()
        with self._lock:
            self._latest = s
            self._hist.append((time.time(), s))
        return s

    def _loop(self) -> None:
        # Prime immediately, then refresh every interval until stopped.
        while True:
            self._tick()
            if self._stop.wait(self._interval):
                return

    def stop(self) -> None:
        self._stop.set()

    def snapshot(self) -> dict[str, float | None] | None:
        """Instant in-memory copy of the most recent poll, or None if none yet."""
        with self._lock:
            return dict(self._latest) if self._latest is not None else None

    def turn_stats(self, t0: float, t1: float) -> dict[str, float | None]:
        """Aggregate memory over the wall-clock window ``[t0, t1]`` (turn start ->
        record build). Never raises.

        Falls back to a single reading when no poll landed inside the window
        (a turn shorter than one interval, or the poller disabled), so the row
        is always populated -- degenerate start==avg==max==end in that case.
        """
        with self._lock:
            window = [s for (ts, s) in self._hist if t0 <= ts <= t1]
            latest = dict(self._latest) if self._latest is not None else None
        if not window:
            one = latest if latest is not None else sample_mem()
            window = [one]
            latest = one
        end = latest if latest is not None else window[-1]

        def _start_avg_ext(key: str, ext):
            vals = [s[key] for s in window if s.get(key) is not None]
            if not vals:
                return None, None, None
            return vals[0], round(sum(vals) / len(vals), 1), ext(vals)

        g_start, g_avg, g_max = _start_avg_ext("gtt_used_mb", max)
        a_start, a_avg, a_min = _start_avg_ext("mem_available_mb", min)
        return {
            "gtt_used_mb": end.get("gtt_used_mb"),
            "gtt_used_start_mb": g_start,
            "gtt_used_avg_mb": g_avg,
            "gtt_used_max_mb": g_max,
            "vram_used_mb": end.get("vram_used_mb"),
            "mem_available_mb": end.get("mem_available_mb"),
            "mem_available_start_mb": a_start,
            "mem_available_avg_mb": a_avg,
            "mem_available_min_mb": a_min,
            "soc_power_w": end.get("soc_power_w"),
        }
