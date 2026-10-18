#!/usr/bin/env python3
"""Run the slotpin dashboard on synthetic data, entirely on 127.0.0.1.

A development tool (not part of the installed package, stdlib only). It starts:

* a fake upstream that serves ``/slots`` (four slots, one generating, one in
  prefill, two idle) and a ``/metrics`` page with the token counters the
  dashboard's restart detection reads;
* a fake energy feed (an ESPHome-style Prometheus page) on the same fake server;
* a synthetic ``metrics.jsonl`` covering the last N days (default 7) up to now,
  built from the schemas in ``docs/metrics.md``, deterministic for a given
  ``--seed``;
* a pre-seeded history database (memory, GPU memory, power and temperature
  samples, one restart and one stall episode), filled through the dashboard's
  own folding and flush functions;
* the dashboard itself, with its system readers (``/proc/meminfo``, hwmon
  temperatures, amdgpu GTT, the proxy config file) replaced by synthetic
  providers so it behaves identically on macOS and Linux.

Nothing is read from the machine it runs on and nothing leaves 127.0.0.1. Every
value shown is invented: box label ``demo``, config id ``gufo-demo-4slot``,
routes ``chat`` and ``background``, model ``demo-model``.

    python tools/demo_dashboard.py                  # prints the URL, Ctrl-C to stop
    python tools/demo_dashboard.py --once-seconds 30
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import shutil
import sys
import tempfile
import threading
import time
import types
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Use the checkout's package when slotpin is not installed.
_SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if os.path.isdir(_SRC) and _SRC not in sys.path:
    sys.path.insert(0, _SRC)

LABEL = "demo"
CONFIG_ID = "gufo-demo-4slot"
BUILD = "demo-build-1"
MODEL = "demo-model"
SLOTS = 4
N_CTX = 131072
MEM_TOTAL_MB = 131072  # 128 GB
SWAP_TOTAL_MB = 8192
KV_BYTES_PER_TOKEN = 38000
SYSTEM_PROMPT_TOKENS = 2900
DAY = 86400.0
FINGERPRINT = hashlib.sha256(f"{CONFIG_ID}:{MODEL}".encode()).hexdigest()[:16]


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat(timespec="seconds")


def _hour_utc(epoch: float) -> float:
    return (epoch % DAY) / 3600.0


def _diurnal(epoch: float) -> float:
    """0..1 daily load rhythm in UTC: quiet at night, busiest mid-afternoon."""
    h = _hour_utc(epoch)
    dh = abs(h - 14.0)
    dh = min(dh, 24.0 - dh)
    return math.exp(-((dh / 4.5) ** 2))


def _weekend(epoch: float) -> bool:
    return datetime.fromtimestamp(epoch, UTC).weekday() >= 5


# --------------------------------------------------------------------------
# Synthetic system model: power, energy counter, memory, GPU memory, temps.
# --------------------------------------------------------------------------
class SystemModel:
    """Invented machine readings as pure functions of time (plus seeded noise)."""

    P0 = 66.0
    E0_KWH = 412.8
    # (amplitude W, angular frequency rad/s, phase rad) for sin(w t + phase)
    _W_DAY = 2 * math.pi / DAY
    _TERMS = (
        (13.0, _W_DAY, -2 * math.pi * 14 / 24 + math.pi / 2),  # peaks 14:00 UTC
        (4.0, 2 * math.pi / 21600.0, 1.1),
        (3.0, 2 * math.pi / 2220.0, 0.4),
        (2.0, 2 * math.pi / 420.0, 2.3),
    )

    def __init__(self, seed: int = 7, anchor: float = 0.0):
        self.rng = random.Random(f"model:{seed}")
        self.anchor = anchor  # the energy counter reads E0_KWH here
        self.down_windows: list[tuple[float, float]] = []
        self.stall_windows: list[tuple[float, float]] = []
        self.ramp_s = 240.0

    # -- power / energy (closed form so the live feed continues the history) --
    def base_power_w(self, t: float) -> float:
        return self.P0 + sum(a * math.sin(w * t + ph) for a, w, ph in self._TERMS)

    def energy_kwh(self, t: float) -> float:
        a0 = self.anchor
        integral = self.P0 * (t - a0)
        for a, w, ph in self._TERMS:
            integral += a / w * (math.cos(w * a0 + ph) - math.cos(w * t + ph))
        return self.E0_KWH + integral / 3.6e6

    # -- state --
    def state(self, t: float) -> tuple[str, float]:
        """("down"|"ramp"|"stall"|"up", fraction of the way back for "ramp")."""
        for a, b in self.down_windows:
            if a <= t < b:
                return "down", 0.0
            if b <= t < b + self.ramp_s:
                return "ramp", (t - b) / self.ramp_s
        for a, b in self.stall_windows:
            if a <= t < b:
                return "stall", 1.0
        return "up", 1.0

    def gtt_mb(self, t: float, bump: float = 0.0) -> float:
        base = 62300.0 + 260.0 * math.sin(t / 5400.0) + 90.0 * math.sin(t / 611.0)
        st, frac = self.state(t)
        if st == "down":
            return 900.0
        if st == "ramp":
            return 900.0 + (base - 900.0) * frac + bump
        return base + bump

    def mem(self, t: float, bump: float = 0.0, noisy: bool = True) -> dict:
        g = self.rng.gauss if noisy else (lambda *_: 0.0)
        gtt = self.gtt_mb(t, bump)
        nominal_gtt = self.gtt_mb(t) if self.state(t)[0] == "up" else 62300.0
        free = (
            30500.0
            + 1700.0 * math.sin(2 * math.pi * t / 32400.0 + 1.0)
            + 300.0 * math.sin(t / 410.0)
            + (nominal_gtt - self.gtt_mb(t))
            - 0.7 * bump
            + g(0, 160)
        )
        cache = 9400.0 + 700.0 * math.sin(2 * math.pi * t / DAY + 0.5) + 200.0 * math.sin(t / 700.0)
        swap_used = 96.0 + 12.0 * math.sin(t / 9000.0)
        avail = free + 0.85 * cache
        return {
            "mem_total_mb": MEM_TOTAL_MB,
            "mem_avail_mb": int(avail),
            "swap_total_mb": SWAP_TOTAL_MB,
            "swap_free_mb": int(SWAP_TOTAL_MB - swap_used),
            "mem_free_mb": int(free),
            "page_cache_mb": int(cache),
            "swap_used_mb": int(swap_used),
            "gtt_used_mb": int(gtt),
        }

    def temps(self, t: float, active: float = 0.5, noisy: bool = True) -> dict:
        g = self.rng.gauss if noisy else (lambda *_: 0.0)
        d = _diurnal(t)
        st = self.state(t)[0]
        idle = st in ("down",)
        extra = 6.0 if st == "stall" else 0.0
        if idle:
            return {
                "cpu_c": round(36.0 + g(0, 0.5), 1),
                "gpu_c": round(34.0 + g(0, 0.5), 1),
                "nvme_c": round(40.0 + g(0, 0.3), 1),
            }
        return {
            "cpu_c": round(
                49.0 + 6.0 * d + 5.0 * active + extra + 1.4 * math.sin(t / 900.0) + g(0, 0.6), 1
            ),
            "gpu_c": round(
                54.0 + 7.0 * d + 9.0 * active + extra + 1.2 * math.sin(t / 1300.0) + g(0, 0.7), 1
            ),
            "nvme_c": round(42.0 + 3.0 * d + 2.0 * active + 0.8 * math.sin(t / 2100.0) + g(0, 0.3), 1),
        }

    def power_w(self, t: float, active: float = 0.5, noisy: bool = True) -> float:
        st = self.state(t)[0]
        if st == "down":
            return round(47.0 + (self.rng.gauss(0, 0.6) if noisy else 0.0), 1)
        extra = 30.0 if st == "stall" else 0.0
        return round(
            self.base_power_w(t) + 22.0 * active + extra + (self.rng.gauss(0, 1.2) if noisy else 0)
        , 1)


# --------------------------------------------------------------------------
# Synthetic metrics.jsonl
# --------------------------------------------------------------------------
class Scenario:
    """Builds the invented metric stream and the event timeline."""

    def __init__(self, seed: int = 7, now: float | None = None, days: float = 7.0):
        self.seed = seed
        self.now = time.time() if now is None else now
        self.days = days
        self.rng = random.Random(f"scenario:{seed}")
        self.model = SystemModel(seed, anchor=self.now - days * DAY)
        start = self.now
        # one restart (down 4 min) and one stall (7.5 min), both inside the last 24 h
        self.down = (start - 15 * 3600.0, start - 15 * 3600.0 + 240.0)
        self.stall = (start - 6 * 3600.0, start - 6 * 3600.0 + 450.0)
        self.model.down_windows = [self.down]
        self.model.stall_windows = [self.stall]
        self.t0 = self.now - days * DAY
        self._rid = 0
        self._sessions: list[dict] = []
        self._next_session = 0
        self._cold_left = 0
        self._last_end: float | None = None
        self._restarted = False
        self.bumps: dict[int, float] = {}  # minute -> GTT bump (MB) from a big prefill
        self.active: dict[int, float] = {}  # minute -> fraction of the minute busy

    # -- timeline --
    def events(self) -> list[tuple[float, str, str | None]]:
        det = json.dumps({"slot": 2, "stalled_s": 312.4})
        out = [
            (self.down[0], "down", None),
            (self.down[1], "up", None),
            (self.stall[0], "stall", det),
            (self.stall[1], "stall_cleared", det),
        ]
        return sorted(out)

    def _blocked(self, t: float) -> bool:
        if self.down[0] - 60 <= t < self.down[1] + 20:
            return True
        return self.stall[0] - 60 <= t < self.stall[1] + 5

    # -- helpers --
    def _lognorm(self, median: float, sigma: float, lo: float, hi: float) -> float:
        return min(hi, max(lo, median * math.exp(self.rng.gauss(0, sigma))))

    def _arrivals(self, last: float) -> list[tuple[float, str]]:
        rng = self.rng
        out: list[tuple[float, str]] = []
        m = int(self.t0 // 60)
        last_min = int(last // 60)
        while m <= last_min:
            t = m * 60.0
            g = _diurnal(t)
            wk = 0.7 if _weekend(t) else 1.0
            chat = (1.2 + 13.0 * g) * wk
            bg = (0.5 + 1.0 * (1.0 - g)) * wk
            if t >= self.now - 25 * 60:  # the last stretch is busier so "Live" has content
                chat = max(chat, 30.0)
            if rng.random() < chat / 60.0:
                out.append((t + rng.uniform(0, 60), "chat"))
            if rng.random() < bg / 60.0:
                out.append((t + rng.uniform(0, 60), "background"))
            m += 1
        out.sort()
        return [(t, r) for t, r in out if t <= last and not self._blocked(t)]

    def _new_id(self) -> str:
        self._rid += 1
        return f"demo-{self._rid:06d}"

    def _hash(self, key: str) -> str:
        return hashlib.sha1(f"{self.seed}:{key}".encode()).hexdigest()[:8]

    # -- one turn --
    def _turn(self, t: float, route: str) -> dict:
        rng = self.rng
        mdl = self.model
        # session and prompt size
        if route == "chat":
            self._sessions = [s for s in self._sessions if s["turns"] < 28]
            if not self._sessions or (len(self._sessions) < 5 and rng.random() < 0.12):
                s = {
                    "id": self._next_session,
                    "hist": 0,
                    "turns": 0,
                    "last": None,
                    "fp": self._hash(f"session{self._next_session}"),
                    # one session in ten opens with a pasted document, so the
                    # deep context bins of the depth charts have data
                    "doc": int(self._lognorm(45000, 0.5, 20000, 110000)) if rng.random() < 0.1 else 0,
                }
                self._next_session += 1
                self._sessions.append(s)
            else:
                s = self._sessions[
                    min(len(self._sessions) - 1, int(abs(rng.gauss(0, 1.3))) % len(self._sessions))
                ]
            new_tokens = int(self._lognorm(600, 0.7, 100, 6000))
            if s["turns"] == 0:
                new_tokens += SYSTEM_PROMPT_TOKENS + int(rng.uniform(0, 2500)) + s["doc"]
            prompt_n = s["hist"] + new_tokens
            gap = None if s["last"] is None else t - s["last"]
            tier, fp = 0, s["fp"]
        else:
            s = None
            new_tokens = int(self._lognorm(1800, 0.4, 1000, 5000))
            hist = int(self._lognorm(9000, 0.5, 3000, 26000))
            prompt_n = hist + new_tokens
            gap = rng.uniform(200, 5000)
            tier, fp = 1, self._hash(f"bg{rng.randrange(10**6)}")

        # cache source
        reason = None
        r = rng.random()
        if self._cold_left > 0:
            cache = "miss"
            reason = "cold_start"
            self._cold_left -= 1
        elif s is not None and s["turns"] == 0:
            cache = "memory"  # the shared system prompt is cached
        elif route == "background":
            cache = "memory" if r < 0.55 else "disk" if r < 0.9 else "miss"
        elif gap is not None and gap > 2400:
            cache = "disk" if r < 0.8 else "miss"
        else:
            cache = "memory" if r < 0.96 else "miss"
        if cache == "miss" and reason is None:
            reason = rng.choice(["prefix_changed", "prefix_changed", "evicted"])
        if cache == "miss":
            cache_n = 0
        elif s is not None and s["turns"] == 0:
            cache_n = SYSTEM_PROMPT_TOKENS
        else:
            cache_n = prompt_n - new_tokens
        new_n = prompt_n - cache_n

        # concurrency, speeds
        conc = rng.choices([1.0, 1.5, 2.0, 2.5, 3.0], [55, 15, 18, 6, 6])[0]
        completion_n = int(
            self._lognorm(430 if route == "chat" else 380, 0.7, 24, 2600)
        )
        # both speeds fall with context depth (attention over a longer KV cache)
        decode_tps = self._lognorm(34.0, 0.16, 14.0, 52.0) / (1.0 + 0.32 * (conc - 1.0))
        decode_tps /= 1.0 + prompt_n / 48000.0
        prefill_tps = self._lognorm(1050.0, 0.14, 500.0, 1600.0) / (1.0 + 0.25 * (conc - 1.0))
        prefill_tps /= 1.0 + prompt_n / 90000.0
        stop = rng.choices(["stop", "tool_calls", "length"], [90, 7, 3])[0]
        if stop == "length":
            completion_n = int(self._lognorm(2000, 0.2, 1500, 2600))
        if route == "chat":
            qw = max(0.0, rng.expovariate(1 / 6.0) - 2.0) if rng.random() < 0.8 else rng.uniform(20, 140)
        else:
            qw = rng.expovariate(1 / 600.0) + 40.0
            if rng.random() < 0.04:
                qw += rng.uniform(3000, 9000)
        # restore and snapshot costs
        restore_bytes = restore_ms = 0.0
        if cache == "memory":
            restore_ms = round(rng.uniform(2.0, 14.0), 1)
        elif cache == "disk":
            restore_bytes = float(cache_n * KV_BYTES_PER_TOKEN)
            restore_ms = round(120.0 + restore_bytes / 1.9e6 * (1 + rng.uniform(-0.15, 0.25)), 1)
        snap_bytes = int((prompt_n + completion_n) * KV_BYTES_PER_TOKEN)
        snap_ms = round(18.0 + snap_bytes / 9.0e6 * (1 + rng.uniform(-0.2, 0.3)), 1)
        overhead = self._lognorm(0.85, 0.35, 0.3, 3.5)
        ttft = qw / 1000.0 + overhead + restore_ms / 1000.0 + new_n / prefill_tps
        dur = ttft + completion_n / decode_tps + rng.uniform(0.02, 0.2)
        t_start = t
        t_end = t + dur

        # draft acceptance
        draft_n = int(completion_n * rng.uniform(1.25, 1.6))
        acc_rate = min(0.8, max(0.45, rng.gauss(0.62, 0.05)))
        draft_acc = int(round(draft_n * acc_rate))

        # system readings for the turn
        bump = min(2600.0, new_n * 0.12)
        mm = int(t_start // 60)
        self.bumps[mm] = max(self.bumps.get(mm, 0.0), bump)
        for k in range(mm, int(t_end // 60) + 1):
            lo, hi = max(t_start, k * 60.0), min(t_end, k * 60.0 + 60.0)
            self.active[k] = min(1.0, self.active.get(k, 0.0) + max(0.0, hi - lo) / 60.0)
        m0, m1 = mdl.mem(t_start, noisy=False), mdl.mem(t_end, noisy=False)
        gtt0, gtt1 = m0["gtt_used_mb"], m1["gtt_used_mb"]
        peak = gtt0 + bump
        p_load = mdl.base_power_w(t_end) + 38.0
        e_turn = p_load * dur / 3600.0 * rng.uniform(0.9, 1.1)
        e_idle = None
        if self._last_end is not None and t_start > self._last_end:
            e_idle = round(mdl.base_power_w((t_start + self._last_end) / 2) * (t_start - self._last_end) / 3600.0, 3)
        self._last_end = t_end

        rec = {
            "ts": _iso(t_end),
            "route": route,
            "slot": (s["id"] if s is not None else rng.randrange(SLOTS)) % SLOTS,
            "workload": route,
            "run_id": None,
            "request_id": self._new_id(),
            "config_fingerprint": FINGERPRINT,
            "fingerprint_version": 2,
            "upstream_build": BUILD,
            "fp": fp,
            "prefix": fp,
            "cache_n": cache_n,
            "prompt_n": prompt_n,
            "completion_n": completion_n,
            "dur_s": round(dur, 3),
            "ttft_s": round(ttft, 3),
            "cacheram_restore": False,
            "restored_tokens": 0,
            "decode_tps": round(decode_tps, 2),
            "prefill_tps": round(prefill_tps, 1),
            "predicted_n": completion_n,
            "draft_n": draft_n,
            "draft_n_accepted": draft_acc,
            "accept_rate": round(draft_acc / draft_n, 4),
            "stop_reason": stop,
            "truncated": stop == "length",
            "ok": True,
            "served": "warm" if cache in ("memory", "disk") else "cold",
            "queue_wait_ms": round(qw, 1),
            "tier": tier,
            "concurrency": conc,
            "gtt_used_mb": float(gtt1),
            "gtt_used_start_mb": float(gtt0),
            "gtt_used_avg_mb": float(round((gtt0 + gtt1) / 2 + bump / 2)),
            "gtt_used_max_mb": float(round(peak)),
            "vram_used_mb": 512.0,
            "mem_available_mb": float(m1["mem_avail_mb"]),
            "mem_available_start_mb": float(m0["mem_avail_mb"]),
            "mem_available_avg_mb": float(round((m0["mem_avail_mb"] + m1["mem_avail_mb"]) / 2 - bump * 0.35)),
            "mem_available_min_mb": float(round(min(m0["mem_avail_mb"], m1["mem_avail_mb"]) - bump * 0.7)),
            "soc_power_w": round(mdl.base_power_w(t_end) * 0.62 + 24.0, 1),
            "plug_power_w": round(p_load, 1),
            "energy_kwh": round(mdl.energy_kwh(t_end), 5),
            "energy_turn_wh": round(e_turn, 3),
            "energy_fair_wh": round(e_turn / conc, 3),
            "energy_idle_wh": e_idle,
            "upstream_cache": cache,
            "upstream_cache_miss_reason": reason,
            "upstream_restore_ms": restore_ms,
            "upstream_restore_bytes": int(restore_bytes),
            "upstream_snapshot_ms": snap_ms,
            "upstream_snapshot_bytes": snap_bytes,
            "upstream_disk_queued_bytes": int(snap_bytes * rng.choice([0, 0, 1]) * rng.uniform(0.2, 1.0)),
        }
        if s is not None:
            s["hist"] = prompt_n + completion_n
            s["turns"] += 1
            s["last"] = t_end
        return rec

    # -- failures --
    def _failures(self) -> list[dict]:
        rng = self.rng
        out: list[dict] = []

        def fail(t: float, reason: str, status: int | None = None, route: str = "chat", dur=None):
            rec = {
                "event": "turn_failed",
                "ts": _iso(t),
                "reason": reason,
                "route": route,
                "slot": None if reason == "shed" else rng.randrange(SLOTS),
                "prefix": self._hash(f"f{rng.randrange(10**6)}"),
                "dur_s": round(dur if dur is not None else rng.uniform(0.2, 40.0), 3),
            }
            if status is not None:
                rec["upstream_status"] = status
            out.append(rec)

        n = self.now
        fail(self.stall[1] - 4, "stalled", dur=120.5)
        fail(self.down[0] + 70, "unreachable", dur=0.02)
        # the rest are spread around; a few land in the last 24 h
        plan = [
            (-11.0, "shed", None, "background"),
            (-3.2, "upstream_5xx", 503, "chat"),
            (-26.0, "aborted_client", None, "chat"),
            (-30.0, "aborted_client", None, "chat"),
            (-34.0, "timeout", None, "background"),
            (-52.0, "aborted_client", None, "chat"),
            (-77.0, "upstream_incomplete", 200, "chat"),
            (-101.0, "upstream_4xx", 400, "chat"),
            (-130.0, "shed", None, "background"),
            (-150.0, "aborted_client", None, "chat"),
        ]
        for hrs, reason, status, route in plan:
            t = n + hrs * 3600.0 + rng.uniform(-900, 900)
            if self.t0 < t < n - 30 and not self._blocked(t):
                fail(t, reason, status, route)
        return out

    # -- public --
    def records(self) -> list[dict]:
        recs: list[dict] = []
        limit = self.now - 12.0
        for t, route in self._arrivals(limit - 60):
            if self.down[1] <= t < self.down[1] + 60:
                self._sessions = []  # in-memory cache is gone after the restart
                self._cold_left = 2
            rec = self._turn(t, route)
            if _iso_epoch(rec["ts"]) <= limit:
                recs.append(rec)
        if not self.down[0] - 60 < limit - 40 < self.down[1]:
            rec = self._turn(limit - 40, "chat")  # a very recent turn, so "last completion" is fresh
            if _iso_epoch(rec["ts"]) <= limit:
                recs.append(rec)
        recs.extend(self._failures())
        recs.sort(key=lambda r: r["ts"])
        return recs

    def live_turn(self, now: float) -> dict:
        """One more chat turn stamped now, continuing the same sessions."""
        rec = self._turn(now - 8.0, "chat")
        rec["ts"] = _iso(now)
        return rec


def _iso_epoch(ts: str) -> float:
    return datetime.fromisoformat(ts).timestamp()


def write_metrics(path: str, records: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


# --------------------------------------------------------------------------
# Fake upstream + energy feed
# --------------------------------------------------------------------------
class FakeBox:
    """State behind the fake HTTP endpoints; counters advance with wall time."""

    def __init__(self, model: SystemModel):
        self.model = model
        self.t0 = time.time()
        self._e_kwh = model.energy_kwh(self.t0)
        self._e_t = self.t0
        self._lock = threading.Lock()

    def slots(self, now: float | None = None) -> list[dict]:
        now = time.time() if now is None else now
        el = now - self.t0
        gen_cycle, gen_off = divmod(el + 12.0, 45.0)
        pre_cycle, pre_off = divmod(el + 17.0, 61.0)
        n_prompt_gen, n_prompt_pre = 21480, 58000

        def idle(i):
            return {
                "id": i,
                "is_processing": False,
                "n_ctx": N_CTX,
                "n_prompt_tokens": 0,
                "n_prompt_tokens_processed": 0,
                "next_token": [{"n_decoded": 0}],
                "id_task": -1,
            }

        return [
            idle(0),
            {
                "id": 1,
                "is_processing": True,
                "n_ctx": N_CTX,
                "n_prompt_tokens": n_prompt_gen,
                "n_prompt_tokens_cache": 20950,
                "n_prompt_tokens_processed": n_prompt_gen,
                "next_token": [{"n_decoded": int(gen_off * 31) + 1}],
                "id_task": 1000 + int(gen_cycle),
            },
            {
                "id": 2,
                "is_processing": True,
                "n_ctx": N_CTX,
                "n_prompt_tokens": n_prompt_pre,
                "n_prompt_tokens_cache": 0,
                "n_prompt_tokens_processed": int(pre_off * 950) + 1,
                "next_token": [{"n_decoded": 0}],
                "id_task": 2000 + int(pre_cycle),
            },
            idle(3),
        ]

    def metrics_text(self, now: float | None = None) -> str:
        el = (time.time() if now is None else now) - self.t0
        prompt = 48_000_000 + int(el * 640)
        pred = 5_200_000 + int(el * 62)
        return (
            "# TYPE llamacpp:prompt_tokens_total counter\n"
            f"llamacpp:prompt_tokens_total {prompt}\n"
            "# TYPE llamacpp:tokens_predicted_total counter\n"
            f"llamacpp:tokens_predicted_total {pred}\n"
        )

    def energy_text(self, now: float | None = None) -> str:
        now = time.time() if now is None else now
        p = self.model.base_power_w(now) + 11.0
        with self._lock:
            if now > self._e_t:
                self._e_kwh += p * (now - self._e_t) / 3.6e6
                self._e_t = now
            total = self._e_kwh
        daily = total - self.model.energy_kwh(now - (now % DAY))
        v = 231.4
        lines = [
            ("power", "Power", "W", p),
            ("total_energy", "Total Energy", "kWh", total),
            ("daily_energy", "Daily Energy", "kWh", max(0.0, daily)),
            ("voltage", "Voltage", "V", v),
            ("current", "Current", "A", p / v),
        ]
        return "".join(
            f'esphome_sensor_value{{id="{i}",name="{n}",unit="{u}"}} {x:.5f}\n'
            for i, n, u, x in lines
        )


def _make_handler(box: FakeBox):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            path = self.path.split("?", 1)[0]
            if path == "/slots":
                body, ctype = json.dumps(box.slots()), "application/json"
            elif path == "/metrics":
                body, ctype = box.metrics_text(), "text/plain; version=0.0.4"
            elif path == "/energy":
                body, ctype = box.energy_text(), "text/plain; version=0.0.4"
            else:
                self.send_error(404)
                return
            data = body.encode()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    return H


# --------------------------------------------------------------------------
# Seeding the dashboard's history through its own functions
# --------------------------------------------------------------------------
def reset_dash_state(dash) -> None:
    for name in ("_buckets", "_samples", "_events", "_req_pending", "_kv_pending"):
        getattr(dash, name).clear()
    dash._sample_dirty.clear()
    dash._energy_hist.clear()
    dash._temp_hist.clear()
    dash._offset = 0
    dash._last_build = None
    dash._last_completion_ts = None
    dash._stall_info = None


def seed_history(dash, scn: Scenario) -> None:
    """Fold per-minute samples, events and requests into the dashboard's tables."""
    mdl = scn.model
    first = int(scn.t0 // 60)
    last = int(scn.now // 60)
    with dash._lock:
        for m in range(first, last + 1):
            bump = scn.bumps.get(m, 0.0)
            act = scn.active.get(m, 0.0)
            for sec in (1.0, 58.0):
                t = m * 60.0 + sec
                if t > scn.now:
                    break
                energy = {
                    "power": mdl.power_w(t, active=act),
                    "total_energy": mdl.energy_kwh(t),
                }
                mem = mdl.mem(t, bump=bump)
                temps = mdl.temps(t, active=act)
                dash._fold_sample(energy, mem, temps, t)
                dash._fold_sample_v2(energy, mem, temps, t)
        for ev in scn.events():
            dash._events.append(ev)
        dash._events.sort()
        # the detector already knows the upstream is up, so nothing spurious is recorded
        dash._restart.state = "up"
        dash._restart.config_id = CONFIG_ID
        dash._restart.build = BUILD


class Demo:
    """A running demo. ``url`` is the dashboard; ``stop()`` shuts the servers."""

    def __init__(self):
        self.url = ""
        self.workdir = ""
        self.scenario: Scenario | None = None
        self._servers: list[ThreadingHTTPServer] = []
        self._owns_dir = False
        self._stop_live = threading.Event()

    def stop(self) -> None:
        self._stop_live.set()
        for s in self._servers:
            s.shutdown()
            s.server_close()
        self._servers.clear()
        if self._owns_dir and self.workdir:
            shutil.rmtree(self.workdir, ignore_errors=True)


def start_demo(
    *,
    seed: int = 7,
    days: float = 7.0,
    port: int = 0,
    workdir: str | None = None,
    live_interval: float = 30.0,
    poll_sec: float = 2.0,
    quiet: bool = False,
) -> Demo:
    """Seed everything and start the fake upstream, the dashboard and the live feed."""
    import slotpin.dash as dash

    demo = Demo()
    if workdir is None:
        workdir = tempfile.mkdtemp(prefix="slotpin-demo-")
        demo._owns_dir = True
    os.makedirs(workdir, exist_ok=True)
    demo.workdir = workdir
    metrics_path = os.path.join(workdir, "metrics.jsonl")
    db_path = os.path.join(workdir, "rollup.db")
    for p in (metrics_path, db_path):
        if os.path.exists(p):
            os.remove(p)

    scn = Scenario(seed=seed, days=days)
    demo.scenario = scn
    write_metrics(metrics_path, scn.records())

    box = FakeBox(scn.model)
    fake = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(box))
    fake_port = fake.server_address[1]
    threading.Thread(target=fake.serve_forever, daemon=True).start()
    demo._servers.append(fake)

    # Replace the dashboard's inputs at the module level, before any thread starts.
    dash.LISTEN_HOST = "127.0.0.1"
    dash.LISTEN_PORT = port
    dash.METRICS_FILE = metrics_path
    dash.DB_PATH = db_path
    dash.UPSTREAM = f"http://127.0.0.1:{fake_port}"
    dash.PROXY_ADDR = f"127.0.0.1:{fake_port}"
    dash.ENERGY_URL = f"http://127.0.0.1:{fake_port}/energy"
    dash.DASH_LABEL = LABEL
    dash.DRAFTER = "MTP"
    dash.BOXES = []
    dash.POLL_SEC = poll_sec
    dash.read_meminfo = lambda: scn.model.mem(time.time(), bump=0.0)
    dash.read_temps = lambda: scn.model.temps(time.time(), active=0.5)
    dash.read_config_id = lambda: CONFIG_ID
    dash.read_config_slots = lambda: SLOTS
    dash.sysmem = types.SimpleNamespace(
        sample_mem=lambda: {"gtt_used_mb": scn.model.gtt_mb(time.time())}
    )

    reset_dash_state(dash)
    seed_history(dash, scn)
    dash.backfill()
    dash.backfill_requests()
    dash._flush_requests()
    dash._flush_db()

    threading.Thread(target=dash.collector, daemon=True).start()
    srv = ThreadingHTTPServer((dash.LISTEN_HOST, dash.LISTEN_PORT), dash.Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    demo._servers.append(srv)
    demo.url = f"http://127.0.0.1:{srv.server_address[1]}/"

    if live_interval > 0:

        def live():
            while not demo._stop_live.wait(live_interval):
                try:
                    rec = scn.live_turn(time.time())
                    with open(metrics_path, "a", encoding="utf-8") as fh:
                        fh.write(json.dumps(rec) + "\n")
                except Exception as e:  # keep the demo running
                    print("live feed:", e, file=sys.stderr)

        threading.Thread(target=live, daemon=True).start()
    if not quiet:
        print(f"slotpin demo dashboard: {demo.url}  (synthetic data, Ctrl-C to stop)", flush=True)
    return demo


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Run the slotpin dashboard on synthetic data.")
    ap.add_argument("--seed", type=int, default=7, help="seed for the synthetic data (default 7)")
    ap.add_argument("--days", type=float, default=7.0, help="days of history (default 7)")
    ap.add_argument("--port", type=int, default=8099, help="dashboard port on 127.0.0.1; 0 = any")
    ap.add_argument("--dir", default=None, help="working directory (default: a temp dir)")
    ap.add_argument(
        "--live-interval",
        type=float,
        default=30.0,
        help="seconds between new synthetic turns while running; 0 = none (default 30)",
    )
    ap.add_argument("--once-seconds", type=float, default=None, help="exit after N seconds")
    args = ap.parse_args(argv)
    demo = start_demo(
        seed=args.seed,
        days=args.days,
        port=args.port,
        workdir=args.dir,
        live_interval=args.live_interval,
    )
    try:
        if args.once_seconds is not None:
            time.sleep(args.once_seconds)
        else:
            while True:
                time.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        demo.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
