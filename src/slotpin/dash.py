"""slotpin-dash - read-only live + historical dashboard for the slotpin proxy.

Live view: reads (never writes) metrics.jsonl tail, llama-server /slots, the
smart-plug energy feed, and /proc/meminfo. Trends view: a background collector
rolls every request + periodic energy/mem samples into 5-min buckets, persisted
to SQLite, so 1h / 24h / 72h / 7d windows are cheap to query. Separate process
from slotpin; safe to restart.
"""

import collections
import glob
import gzip
import hashlib
import json
import math
import os
import socket
import sqlite3
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from slotpin import sysmem

# ---- config (env-overridable) ----
LISTEN_HOST = os.environ.get("DASH_HOST", "127.0.0.1")
LISTEN_PORT = int(os.environ.get("DASH_PORT", "8091"))
METRICS_FILE = os.environ.get("DASH_METRICS", "/var/lib/slotpin/metrics.jsonl")
UPSTREAM = os.environ.get("DASH_UPSTREAM", "http://127.0.0.1:5802")
PROXY_ADDR = os.environ.get("DASH_PROXY", "127.0.0.1:8090")
# Empty/unset disables the energy poll (no request is made; energy stays None).
ENERGY_URL = os.environ.get("DASH_ENERGY", "").strip()
SLOTPIN_CFG = os.environ.get("DASH_SLOTPIN_CFG", "/etc/slotpin/config.toml")
CONFIG_ID = os.environ.get("DASH_CFG", "")
DB_PATH = os.environ.get("DASH_DB", "/var/lib/slotpin-dash/rollup.db")
WINDOW_MIN = float(os.environ.get("DASH_WINDOW_MIN", "15"))  # live default
POLL_SEC = float(os.environ.get("DASH_POLL_SEC", "3"))
# A busy slot whose (prompt tokens processed, tokens decoded) is unchanged for
# this long, with no turn completing in that time, makes the upstream "stalled".
STALL_S = float(os.environ.get("DASH_STALL_S", "300"))
# Label for the draft-accept card. Unset: "MTP" / "DFlash2" by substring of the
# config id, else "Draft".
DRAFTER = os.environ.get("DASH_DRAFTER", "").strip()
LIVE_WINDOW_SEC = 900  # the "Live" window; its cards are served in /api/state
BUCKET_SEC = 300  # 5-min rollup buckets (legacy table)
SAMPLE_SEC = 60  # 1-min sample rows (samples table)
RETENTION_DAYS = 8
RESERVOIR = 24  # per-bucket sample cap for percentiles
N_RECENT = 25
HWMON_ROOT = os.environ.get("DASH_HWMON", "/sys/class/hwmon")
# Rough APU throttle-proximity thresholds for the reference hardware (Ryzen AI
# Max+ 395), used for the dashboard's warn/crit colour coding. Not
# vendor-specced -- adjust to taste.
TEMP_WARN_C = float(os.environ.get("DASH_TEMP_WARN_C", "85"))
TEMP_CRIT_C = float(os.environ.get("DASH_TEMP_CRIT_C", "95"))
# Box switcher in the header: "name=url,name=url,...". This box's own name
# must match DASH_LABEL for the header to mark it as the active tab.
DASH_LABEL = os.environ.get("DASH_LABEL", socket.gethostname())
DASH_BOXES = os.environ.get("DASH_BOXES", "")

_lock = threading.Lock()
_live = {
    "slots": None,
    "slots_ok": False,
    "energy": None,
    "mem": None,
    "temps": None,
    "proxy_up": False,
    "poll_ts": 0,
}
_energy_hist = collections.deque(maxlen=2000)
_temp_hist = collections.deque(maxlen=2000)  # (ts, cpu_c, gpu_c, nvme_c)
_buckets = {}  # bucket_start_epoch -> agg dict
_samples = {}  # minute_start_epoch -> sample row (see _new_sample)
_sample_dirty = set()  # _samples keys changed since the last flush
_events = []  # list of (epoch, kind, detail)  kind in {"down","up","stall","stall_cleared"}
_offset = 0  # byte offset read so far in metrics.jsonl
_last_build = None  # newest upstream_build seen in a metrics record
_last_completion_ts = None  # epoch of the newest ok turn record ingested
_req_pending = []  # request rows parsed but not yet written to SQLite
_kv_pending = []  # kv_events rows parsed but not yet written to SQLite


# ---------------- small helpers ----------------
def _get_json(url, timeout=6):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)


def _tcp_up(addr, timeout=2):
    host, _, port = addr.partition(":")
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except Exception:
        return False


def read_config_id():
    try:
        for line in open(SLOTPIN_CFG):
            if line.strip().startswith("upstream_config_id"):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    except Exception:
        pass
    return CONFIG_ID


def read_config_slots():
    """Configured slot count (`slots` in the proxy config, DASH_SLOTPIN_CFG), or None
    when the file is missing/unreadable or has no usable value."""
    try:
        with open(SLOTPIN_CFG, "rb") as fh:
            raw = fh.read()
    except Exception:
        return None
    n = None
    try:
        import tomllib

        n = tomllib.loads(raw.decode("utf-8", "replace")).get("slots")
    except Exception:
        for line in raw.decode("utf-8", "replace").splitlines():  # as read_config_id does
            k, _, v = line.partition("=")
            if k.strip() == "slots":
                n = v.split("#", 1)[0].strip()
                break
    try:
        n = int(n)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def read_meminfo():
    try:
        m = {}
        for line in open("/proc/meminfo"):
            k, _, v = line.partition(":")
            p = v.strip().split()
            if p:
                m[k.strip()] = int(p[0])
        return {
            "mem_total_mb": m.get("MemTotal", 0) // 1024,
            "mem_avail_mb": m.get("MemAvailable", 0) // 1024,
            "swap_total_mb": m.get("SwapTotal", 0) // 1024,
            "swap_free_mb": m.get("SwapFree", 0) // 1024,
            "mem_free_mb": m.get("MemFree", 0) // 1024,
            "page_cache_mb": m.get("Cached", 0) // 1024,
            "swap_used_mb": max(0, m.get("SwapTotal", 0) - m.get("SwapFree", 0)) // 1024,
        }
    except Exception:
        return None


def _read_hwmon_temp1(hwmon_dir):
    try:
        v = int(open(os.path.join(hwmon_dir, "temp1_input")).read().strip())
        return round(v / 1000.0, 1)
    except Exception:
        return None


def read_temps():
    """CPU (k10temp Tctl) + GPU (amdgpu edge) + NVMe composite, straight from
    hwmon sysfs -- milli-degC files, no daemon/driver dependency. On APU
    hardware there is no fan-control daemon guaranteed to be present or
    to expose these consistently, so this reads the kernel sensors directly
    rather than depending on one.
    """
    out = {"cpu_c": None, "gpu_c": None, "nvme_c": None}
    try:
        entries = os.listdir(HWMON_ROOT)
    except Exception:
        return out
    for d in entries:
        hdir = os.path.join(HWMON_ROOT, d)
        try:
            name = open(os.path.join(hdir, "name")).read().strip()
        except Exception:
            continue
        if name == "k10temp" and out["cpu_c"] is None:
            out["cpu_c"] = _read_hwmon_temp1(hdir)
        elif name == "amdgpu" and out["gpu_c"] is None:
            out["gpu_c"] = _read_hwmon_temp1(hdir)
        elif name == "nvme" and out["nvme_c"] is None:
            out["nvme_c"] = _read_hwmon_temp1(hdir)
    return out


def _parse_energy(txt):
    out = {}
    for line in txt.splitlines():
        if not line.startswith("esphome_sensor_value"):
            continue
        for key in ("power", "total_energy", "daily_energy", "voltage", "current"):
            if f'id="{key}"' in line:
                try:
                    out[key] = float(line.rsplit(" ", 1)[-1])
                except ValueError:
                    pass
    return out


def _iso_epoch(ts):
    if not ts:
        return None
    try:
        import datetime

        dt = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.UTC)
        return dt.timestamp()
    except Exception:
        return None


def _pctile(vals, p):
    if not vals:
        return None
    s = sorted(vals)
    i = min(len(s) - 1, max(0, int(round((p / 100.0) * (len(s) - 1)))))
    return s[i]


# ---------------- slots + stall state machine ----------------
def _slot_fields(s):
    """Whitelist one upstream /slots object, field by field. The upstream object
    includes the prompt text; it is never copied, stored, logged or served."""
    if not isinstance(s, dict):
        return None
    nd = s.get("n_decoded")
    nt = s.get("next_token")
    if isinstance(nt, list) and nt:
        nt = nt[0]
    if isinstance(nt, dict) and nt.get("n_decoded") is not None:
        nd = nt.get("n_decoded")
    return {
        "id": s.get("id"),
        "is_processing": bool(s.get("is_processing")),
        "id_task": s.get("id_task"),
        "n_prompt_tokens": s.get("n_prompt_tokens"),
        "n_prompt_tokens_cache": s.get("n_prompt_tokens_cache"),
        "n_ctx": s.get("n_ctx"),
        "n_prompt_tokens_processed": s.get("n_prompt_tokens_processed"),
        "n_decoded": nd,
    }


class StallTracker:
    """Pure stall state machine: no threads, no HTTP.

    update(now, slots, last_completion_ts) -> {"state", "stalled_s", "slot"}.
    `slots` is a list of _slot_fields dicts, or None when /slots is unreachable
    (state "down", never "stalled"; tracking restarts afterwards). A busy slot
    is "frozen" when its (n_prompt_tokens_processed, n_decoded) tuple has not
    changed for stall_s; the upstream is "stalled" when a slot is frozen and no
    turn completed within stall_s. Idle slots are not tracked.
    """

    def __init__(self, stall_s=None):
        self.stall_s = stall_s
        self._slots = {}  # id -> {"tuple", "since", "busy_since"}

    def update(self, now, slots, last_completion_ts):
        limit = STALL_S if self.stall_s is None else self.stall_s
        if slots is None:
            self._slots = {}
            return {"state": "down", "stalled_s": None, "slot": None}
        seen = {}
        for i, s in enumerate(slots):
            if not s.get("is_processing"):
                continue
            sid = s.get("id", i)
            tup = (s.get("n_prompt_tokens_processed"), s.get("n_decoded"))
            old = self._slots.get(sid)
            if old is None:
                seen[sid] = {"tuple": tup, "since": now, "busy_since": now}
            elif old["tuple"] != tup:
                seen[sid] = {"tuple": tup, "since": now, "busy_since": old["busy_since"]}
            else:
                seen[sid] = old
        self._slots = seen
        completed_recently = last_completion_ts is not None and now - last_completion_ts < limit
        worst = None
        if not completed_recently:
            for sid, t in seen.items():
                age = now - t["since"]
                if age >= limit and (worst is None or age > worst[1]):
                    worst = (sid, age)
        if worst is None:
            return {"state": "up", "stalled_s": None, "slot": None}
        return {"state": "stalled", "stalled_s": round(worst[1], 1), "slot": worst[0]}

    def snapshot(self, now):
        """{slot id: (busy_for_s, stall_s)} for slots currently busy."""
        return {
            sid: (round(now - t["busy_since"], 1), round(now - t["since"], 1))
            for sid, t in self._slots.items()
        }


_stall = StallTracker()
_stall_info = None  # last StallTracker.update() result


def _stall_step(now, slots_ok, slots, last_completion_ts):
    """Advance the stall machine and record stall/stall_cleared events.
    Call with _lock held (memory only). Returns the update() result."""
    global _stall_info
    prev = _stall_info["state"] if _stall_info else "up"
    info = _stall.update(now, slots if slots_ok else None, last_completion_ts)
    if info["state"] == "stalled" and prev != "stalled":
        _events.append(
            (now, "stall", json.dumps({"slot": info["slot"], "stalled_s": info["stalled_s"]}))
        )
    elif prev == "stalled" and info["state"] != "stalled":
        was = _stall_info or {}
        det = {"slot": was.get("slot"), "stalled_s": was.get("stalled_s")}
        if info["state"] == "down":
            det["reason"] = "down"
        _events.append((now, "stall_cleared", json.dumps(det)))
    _stall_info = info
    return info


# ---------------- upstream restart detection ----------------
def _parse_counters(txt):
    """Cumulative token counters from the upstream's Prometheus /metrics text."""
    out = {}
    for line in txt.splitlines():
        for name in ("prompt_tokens_total", "tokens_predicted_total"):
            if line.startswith("llamacpp:" + name + " "):
                try:
                    out[name] = float(line.rsplit(" ", 1)[-1])
                except ValueError:
                    pass
    return out


def _read_upstream_counters():
    """Best effort: {} when /metrics is disabled or unreachable."""
    try:
        with urllib.request.urlopen(UPSTREAM + "/metrics", timeout=3) as r:
            return _parse_counters(r.read(1 << 20).decode("utf-8", "replace"))
    except Exception:
        return {}


class RestartDetector:
    """Decides when to record `down` / `up` events. Pure: no threads, no HTTP.

    observe(slots_ok, config_id, build, counters) -> [(kind, detail_dict or None)].

    * `down` / `up` are recorded on /slots reachable -> unreachable -> reachable.
    * The state survives a dashboard restart (dump/load via the meta table), so
      the first poll of a new dashboard process is compared with what the old one
      last saw. Persisted "down" and now reachable: `up` (the down was recorded).
      Persisted "up" and now unreachable: `down`.
    * Restart with no down period observed (too short for the poll, or while the
      dashboard was off) is recorded as `up` with a reason only on positive
      evidence: a cumulative upstream counter (/metrics prompt or predicted token
      total) going backwards, or -- on the first poll after the dashboard starts
      -- a different config id or upstream build than the persisted ones.
    * No persisted state (fresh DB) or an unchanged signature records nothing: the
      restart time stays unknown rather than invented. A dashboard restart alone
      never creates an `up`.
    """

    def __init__(self):
        self.state = None  # "up" / "down" / None (unknown)
        self.config_id = None
        self.build = None
        self.counters = {}
        self.fresh = True  # no observation yet by this process

    def dump(self):
        return json.dumps(
            {
                "state": self.state,
                "config_id": self.config_id,
                "build": self.build,
                "counters": self.counters,
            }
        )

    def load(self, blob):
        try:
            d = json.loads(blob)
            if d.get("state") in ("up", "down"):
                self.state = d["state"]
            self.config_id = d.get("config_id") or None
            self.build = d.get("build") or None
            c = d.get("counters")
            self.counters = (
                {k: v for k, v in c.items() if isinstance(v, int | float)}
                if isinstance(c, dict)
                else {}
            )
        except Exception:
            pass

    def observe(self, slots_ok, config_id=None, build=None, counters=None):
        counters = counters or {}
        out = []
        fresh, self.fresh = self.fresh, False
        if not slots_ok:
            if self.state == "up":
                out.append(("down", None))
            self.state = "down"
            self.counters = {}  # the next reading is a new baseline
            return out
        why = []
        if self.state == "up":
            for k, v in counters.items():
                if k in self.counters and v < self.counters[k]:
                    why.append("counter_reset")
                    break
            if fresh:  # compare signatures only against a previous process's record
                if config_id and self.config_id and config_id != self.config_id:
                    why.append("config_changed")
                if build and self.build and build != self.build:
                    why.append("build_changed")
        elif self.state == "down":
            out.append(("up", None))
        if why:
            out.append(("up", {"reason": "+".join(why)}))
        self.state = "up"
        if config_id:
            self.config_id = config_id
        if build:
            self.build = build
        self.counters.update(counters)
        return out


_restart = RestartDetector()


def _restart_step(now, slots_ok, counters=None, config_id=None):
    """Feed one poll to the detector and record its events. Caller holds _lock.
    Returns True when events were added."""
    n = len(_events)
    for kind, det in _restart.observe(slots_ok, config_id, _last_build, counters):
        _events.append((now, kind, json.dumps(det) if det else None))
    return len(_events) > n


# ---------------- rollup buckets ----------------
def _new_bucket():
    return {
        "reqs": 0,
        "warm": 0,
        "cold": 0,
        "restores": 0,
        "errors": 0,
        "prompt": 0,
        "completion": 0,
        "cache": 0,
        "restored": 0,
        "draft": 0,
        "draft_acc": 0,
        "dec_sum": 0.0,
        "dec_n": 0,
        "dec_res": [],
        "ttft_res": [],
        "min_mem": None,
        "e_first": None,
        "e_last": None,
        "pow_sum": 0.0,
        "pow_n": 0,
        "cpu_t_sum": 0.0,
        "cpu_t_n": 0,
        "cpu_t_max": None,
        "gpu_t_sum": 0.0,
        "gpu_t_n": 0,
        "gpu_t_max": None,
        "nvme_t_max": None,
    }


def _res_add(lst, v):
    if v is None:
        return
    if len(lst) < RESERVOIR:
        lst.append(v)
    else:  # simple reservoir: overwrite deterministically
        lst[int(v * 1000) % RESERVOIR] = v


def _fold_request(d):
    ep = _iso_epoch(d.get("ts"))
    if ep is None:
        return
    key = int(ep // BUCKET_SEC) * BUCKET_SEC
    b = _buckets.get(key)
    if b is None:
        b = _buckets[key] = _new_bucket()
    b["reqs"] += 1
    srv = d.get("served")
    if srv == "warm":
        b["warm"] += 1
    elif srv == "cold":
        b["cold"] += 1
    if d.get("cacheram_restore"):
        b["restores"] += 1
        b["restored"] += d.get("restored_tokens") or 0
    if d.get("ok") is False:
        b["errors"] += 1
    b["prompt"] += d.get("prompt_n") or 0
    b["completion"] += d.get("completion_n") or 0
    b["cache"] += d.get("cache_n") or 0
    b["draft"] += d.get("draft_n") or 0
    b["draft_acc"] += d.get("draft_n_accepted") or 0
    if d.get("decode_tps"):
        b["dec_sum"] += d["decode_tps"]
        b["dec_n"] += 1
        _res_add(b["dec_res"], round(d["decode_tps"], 2))
    if d.get("ttft_s") is not None:
        _res_add(b["ttft_res"], round(d["ttft_s"], 2))
    mm = d.get("mem_available_mb")
    if mm is not None and (b["min_mem"] is None or mm < b["min_mem"]):
        b["min_mem"] = mm


def _fold_sample(energy, mem, temps, now):
    key = int(now // BUCKET_SEC) * BUCKET_SEC
    b = _buckets.get(key)
    if b is None:
        b = _buckets[key] = _new_bucket()
    if energy and energy.get("total_energy") is not None:
        kwh = energy["total_energy"]
        if b["e_first"] is None:
            b["e_first"] = kwh
        b["e_last"] = kwh
    if energy and energy.get("power") is not None:
        b["pow_sum"] += energy["power"]
        b["pow_n"] += 1
    if mem and mem.get("mem_avail_mb") is not None:
        mm = mem["mem_avail_mb"]
        if b["min_mem"] is None or mm < b["min_mem"]:
            b["min_mem"] = mm
    if temps:
        c = temps.get("cpu_c")
        if c is not None:
            b["cpu_t_sum"] += c
            b["cpu_t_n"] += 1
            b["cpu_t_max"] = c if b["cpu_t_max"] is None else max(b["cpu_t_max"], c)
        g = temps.get("gpu_c")
        if g is not None:
            b["gpu_t_sum"] += g
            b["gpu_t_n"] += 1
            b["gpu_t_max"] = g if b["gpu_t_max"] is None else max(b["gpu_t_max"], g)
        n = temps.get("nvme_c")
        if n is not None:
            b["nvme_t_max"] = n if b["nvme_t_max"] is None else max(b["nvme_t_max"], n)


# ---------------- sample rows v2 (1-minute) ----------------
def _new_sample():
    return {
        "pow_sum": 0.0,
        "pow_n": 0,
        "e_first": None,
        "e_last": None,
        "cpu_t_sum": 0.0,
        "cpu_t_n": 0,
        "cpu_t_max": None,
        "gpu_t_sum": 0.0,
        "gpu_t_n": 0,
        "gpu_t_max": None,
        "nvme_t_max": None,
        "min_free": None,
        "min_avail": None,
        "max_cache": None,
        "max_swap": None,
        "max_gtt": None,
    }


def _lo(cur, v):
    return v if cur is None else min(cur, v)


def _hi(cur, v):
    return v if cur is None else max(cur, v)


def _fold_sample_v2(energy, mem, temps, now):
    """Fold one poll into the 1-minute sample row. Caller holds _lock."""
    key = int(now // SAMPLE_SEC) * SAMPLE_SEC
    b = _samples.get(key)
    if b is None:
        b = _samples[key] = _new_sample()
    _sample_dirty.add(key)
    if energy and energy.get("total_energy") is not None:
        if b["e_first"] is None:
            b["e_first"] = energy["total_energy"]
        b["e_last"] = energy["total_energy"]
    if energy and energy.get("power") is not None:
        b["pow_sum"] += energy["power"]
        b["pow_n"] += 1
    if mem:
        if mem.get("mem_free_mb") is not None:
            b["min_free"] = _lo(b["min_free"], mem["mem_free_mb"])
        if mem.get("mem_avail_mb") is not None:
            b["min_avail"] = _lo(b["min_avail"], mem["mem_avail_mb"])
        if mem.get("page_cache_mb") is not None:
            b["max_cache"] = _hi(b["max_cache"], mem["page_cache_mb"])
        if mem.get("swap_used_mb") is not None:
            b["max_swap"] = _hi(b["max_swap"], mem["swap_used_mb"])
        if mem.get("gtt_used_mb") is not None:
            b["max_gtt"] = _hi(b["max_gtt"], mem["gtt_used_mb"])
    if temps:
        c = temps.get("cpu_c")
        if c is not None:
            b["cpu_t_sum"] += c
            b["cpu_t_n"] += 1
            b["cpu_t_max"] = _hi(b["cpu_t_max"], c)
        g = temps.get("gpu_c")
        if g is not None:
            b["gpu_t_sum"] += g
            b["gpu_t_n"] += 1
            b["gpu_t_max"] = _hi(b["gpu_t_max"], g)
        n = temps.get("nvme_c")
        if n is not None:
            b["nvme_t_max"] = _hi(b["nvme_t_max"], n)


def bucket_width(window_sec):
    """Chart bucket width for a window: shared by sample_series and request_series."""
    if window_sec <= 3600:
        return 60
    if window_sec <= 259200:
        return 300
    return 1800


def sample_bucket_width(window_sec):
    return bucket_width(window_sec)


def _legacy_unit(b):
    """A legacy 300 s bucket as a sample-row-shaped dict (v2-only fields None)."""
    u = _new_sample()
    for k in (
        "pow_sum",
        "pow_n",
        "e_first",
        "e_last",
        "cpu_t_sum",
        "cpu_t_n",
        "cpu_t_max",
        "gpu_t_sum",
        "gpu_t_n",
        "gpu_t_max",
        "nvme_t_max",
    ):
        u[k] = b.get(k, u[k])
    u["min_avail"] = b.get("min_mem")
    return u


def sample_series(window_sec, now=None):
    """Sampled-series points for the window at sample_bucket_width(window_sec).

    Built from the 1-minute samples rows; the part of the window before the
    first samples row is filled from whole legacy 300 s buckets that end at or
    before that row (a legacy bucket straddling it overlaps the samples rows,
    so it is skipped rather than double counted).
    """
    now = time.time() if now is None else now
    width = sample_bucket_width(window_sec)
    cutoff = now - window_sec
    with _lock:
        first = min(_samples) if _samples else None
        rows = [(k, dict(b)) for k, b in _samples.items() if k + SAMPLE_SEC > cutoff]
        legacy = [
            (k, _legacy_unit(b))
            for k, b in _buckets.items()
            if k + BUCKET_SEC > cutoff and (first is None or k + BUCKET_SEC <= first)
        ]
    groups = {}
    for k, u in sorted(legacy + sorted(rows, key=lambda r: r[0]), key=lambda r: r[0]):
        groups.setdefault(int(k // width) * width, []).append(u)

    def mx(us, f):
        v = [u[f] for u in us if u[f] is not None]
        return max(v) if v else None

    def mn(us, f):
        v = [u[f] for u in us if u[f] is not None]
        return min(v) if v else None

    def mean(us, s, n):
        c = sum(u[n] or 0 for u in us)
        return sum(u[s] or 0.0 for u in us) / c if c else None

    def gb(v):
        return None if v is None else round(v / 1024.0, 2)

    def r1(v):
        return None if v is None else round(v, 1)

    pts = []
    for t in sorted(groups):
        us = groups[t]
        ef = next((u["e_first"] for u in us if u["e_first"] is not None), None)
        el = next((u["e_last"] for u in reversed(us) if u["e_last"] is not None), None)
        pts.append(
            {
                "t": t,
                "power_w": r1(mean(us, "pow_sum", "pow_n")),
                "cpu_temp_c": r1(mean(us, "cpu_t_sum", "cpu_t_n")),
                "gpu_temp_c": r1(mean(us, "gpu_t_sum", "gpu_t_n")),
                "cpu_temp_max_c": mx(us, "cpu_t_max"),
                "gpu_temp_max_c": mx(us, "gpu_t_max"),
                "nvme_temp_max_c": mx(us, "nvme_t_max"),
                "mem_free_gb": gb(mn(us, "min_free")),
                "mem_avail_gb": gb(mn(us, "min_avail")),
                "page_cache_gb": gb(mx(us, "max_cache")),
                "swap_used_gb": gb(mx(us, "max_swap")),
                "gtt_gb": gb(mx(us, "max_gtt")),
                "energy_kwh": round(el - ef, 4)
                if ef is not None and el is not None and el >= ef
                else None,
            }
        )
    return pts


# ---------------- SQLite persistence ----------------
def _db():
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.execute("""CREATE TABLE IF NOT EXISTS buckets(
        bucket INTEGER PRIMARY KEY, data TEXT)""")
    # 1-minute sample rows; JSON data column like buckets so new fields need no
    # ALTER (missing keys default via _new_sample on load).
    con.execute("""CREATE TABLE IF NOT EXISTS samples(
        bucket INTEGER PRIMARY KEY, data TEXT)""")
    con.execute("""CREATE TABLE IF NOT EXISTS events(
        ts REAL, kind TEXT, detail TEXT)""")
    if "detail" not in [r[1] for r in con.execute("PRAGMA table_info(events)")]:
        con.execute("ALTER TABLE events ADD COLUMN detail TEXT")
    con.execute("""CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)""")
    con.execute(REQUESTS_DDL)
    have = {r[1] for r in con.execute("PRAGMA table_info(requests)")}
    for c in REQUEST_COLS:  # columns added after a DB was created
        if c not in have:
            typ = "TEXT" if c in _REQ_TEXT else "INTEGER" if c in _REQ_INT else "REAL"
            con.execute(f"ALTER TABLE requests ADD COLUMN {c} {typ}")
    con.execute("CREATE INDEX IF NOT EXISTS requests_ts ON requests(ts)")
    con.execute(KV_DDL)
    if "bytes" not in {r[1] for r in con.execute("PRAGMA table_info(kv_events)")}:
        con.execute("ALTER TABLE kv_events ADD COLUMN bytes INTEGER")
    con.execute("CREATE INDEX IF NOT EXISTS kv_events_ts ON kv_events(ts)")
    return con


def _load_from_db():
    global _offset
    try:
        con = _db()
        for bk, data in con.execute("SELECT bucket, data FROM buckets"):
            try:
                # Merge onto a fresh template so buckets persisted by an older
                # dash version (missing newer fields, e.g. temp tracking) get
                # those fields defaulted instead of KeyError-ing later when
                # folded into -- schema-forward, not just today's fields.
                b = _new_bucket()
                b.update(json.loads(data))
                _buckets[bk] = b
            except Exception:
                pass
        for bk, data in con.execute("SELECT bucket, data FROM samples"):
            try:
                b = _new_sample()
                b.update(json.loads(data))
                _samples[bk] = b
            except Exception:
                pass
        for ts, kind, detail in con.execute("SELECT ts, kind, detail FROM events ORDER BY ts"):
            _events.append((ts, kind, detail))
        row = con.execute("SELECT v FROM meta WHERE k='offset'").fetchone()
        if row:
            _offset = int(row[0])
        row = con.execute("SELECT v FROM meta WHERE k='restart_state'").fetchone()
        if row:
            _restart.load(row[0])
        else:
            # First start of this code on a DB: any up/down events were written
            # by the previous dashboard's different detector. Drop them (memory
            # now, the events table at the next flush) so restart time and
            # uptime start unknown and only the new detector establishes them.
            _events[:] = [e for e in _events if e[1] not in ("up", "down")]
        con.close()
    except Exception as e:
        print("db load skipped:", e)


def _flush_db():
    dirty = ()
    try:
        con = _db()
        with _lock:
            items = list(_buckets.items())
            dirty = sorted(_sample_dirty)
            srows = [(k, json.dumps(_samples[k])) for k in dirty if k in _samples]
            _sample_dirty.difference_update(dirty)
            evs = list(_events)
            off = _offset
            rst = _restart.dump()
        con.executemany(
            "INSERT OR REPLACE INTO buckets(bucket,data) VALUES(?,?)",
            [(bk, json.dumps(b)) for bk, b in items],
        )
        con.executemany("INSERT OR REPLACE INTO samples(bucket,data) VALUES(?,?)", srows)
        con.execute("DELETE FROM events")
        con.executemany("INSERT INTO events(ts,kind,detail) VALUES(?,?,?)", evs)
        con.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('offset',?)", (str(off),))
        con.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('restart_state',?)", (rst,))
        # prune old buckets
        cutoff = time.time() - RETENTION_DAYS * 86400
        con.execute("DELETE FROM buckets WHERE bucket < ?", (cutoff,))
        con.execute("DELETE FROM samples WHERE bucket < ?", (cutoff,))
        con.execute("DELETE FROM requests WHERE ts < ?", (cutoff,))
        con.execute("DELETE FROM kv_events WHERE ts < ?", (cutoff,))
        con.commit()
        con.close()
    except Exception as e:
        with _lock:
            _sample_dirty.update(dirty)  # retry these rows next flush
        print("db flush error:", e)


def _flush_events():
    """Write the events table and the restart detector state now (small), so a
    transition seen just before the dashboard itself restarts is not lost."""
    try:
        con = _db()
        with _lock:
            evs = list(_events)
            rst = _restart.dump()
        with con:
            con.execute("DELETE FROM events")
            con.executemany("INSERT INTO events(ts,kind,detail) VALUES(?,?,?)", evs)
            con.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('restart_state',?)", (rst,))
        con.close()
    except Exception as e:
        print("events flush error:", e)


def _prune_mem():
    cutoff = time.time() - RETENTION_DAYS * 86400
    for k in [k for k in _buckets if k < cutoff]:
        del _buckets[k]
    for k in [k for k in _samples if k < cutoff]:
        del _samples[k]
        _sample_dirty.discard(k)
    while _events and _events[0][0] < cutoff:
        _events.pop(0)


# ---------------- incremental metrics reader ----------------
def _read_new_metrics():
    """Read appended lines since _offset; fold into buckets. Handles truncation."""
    global _offset, _last_completion_ts, _last_build
    try:
        sz = os.path.getsize(METRICS_FILE)
        if sz < _offset:  # rotated/truncated
            _offset = 0
        if sz == _offset:
            return 0
        n = 0
        with open(METRICS_FILE, "rb") as f:
            f.seek(_offset)
            data = f.read()
            _offset = f.tell()
        buf = data.decode("utf-8", "replace")
        # if we didn't end on a newline, back off to last newline
        if not buf.endswith("\n"):
            cut = buf.rfind("\n")
            if cut >= 0:
                _offset -= len(buf.encode("utf-8")) - len(buf[: cut + 1].encode("utf-8"))
                buf = buf[: cut + 1]
        for line in buf.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                _fold_request(rec)
                n += 1
                if isinstance(rec, dict) and isinstance(rec.get("upstream_build"), str):
                    _last_build = rec["upstream_build"] or _last_build
            except Exception:
                continue
            kv = _kv_row(rec, line)
            if kv is not None:
                _kv_pending.append(kv)
                continue
            row = _request_row(rec, line)
            if row is not None:
                _req_pending.append(row)
                if row[2] == "ok" and (_last_completion_ts is None or row[1] > _last_completion_ts):
                    _last_completion_ts = row[1]
        return n
    except Exception:
        return 0


# ---------------- requests table ----------------
# One row per chat turn record (status 'ok') and per turn_failed record
# (status 'failed') in metrics.jsonl. Only numbers and short ids are stored --
# never prompt or reply text (the metrics stream has none).
REQUEST_COLS = (
    "ts",
    "status",
    "fail_reason",
    "upstream_status",
    "route",
    "slot",
    "prefix",
    "dur_s",
    "queue_wait_ms",
    "ttft_s",
    "decode_tps",
    "prefill_tps",
    "prompt_n",
    "cache_n",
    "completion_n",
    "draft_n",
    "draft_n_accepted",
    "served",
    "stop_reason",
    "upstream_cache",
    "upstream_cache_miss_reason",
    "upstream_restore_ms",
    "upstream_restore_bytes",
    "upstream_snapshot_ms",
    "upstream_snapshot_bytes",
    "mem_available_start_mb",
    "mem_available_min_mb",
    "energy_turn_wh",
    "energy_fair_wh",
    "concurrency",
    "cacheram_restore",
    "restored_tokens",
)
_REQ_TEXT = {
    "status",
    "fail_reason",
    "route",
    "prefix",
    "served",
    "stop_reason",
    "upstream_cache",
    "upstream_cache_miss_reason",
}
_REQ_INT = {
    "upstream_status",
    "slot",
    "prompt_n",
    "cache_n",
    "completion_n",
    "draft_n",
    "draft_n_accepted",
    "upstream_restore_bytes",
    "upstream_snapshot_bytes",
    "cacheram_restore",
    "restored_tokens",
}
REQUESTS_DDL = (
    "CREATE TABLE IF NOT EXISTS requests(id INTEGER PRIMARY KEY, key TEXT UNIQUE, "
    + ", ".join(
        f"{c} {'TEXT' if c in _REQ_TEXT else 'INTEGER' if c in _REQ_INT else 'REAL'}"
        for c in REQUEST_COLS
    )
    + ")"
)
_REQ_INSERT = (
    f"INSERT OR IGNORE INTO requests(key, {', '.join(REQUEST_COLS)}) "
    f"VALUES(?{', ?' * len(REQUEST_COLS)})"
)
FAIL_FIELDS = (
    "upstream_status",
    "route",
    "slot",
    "prefix",
    "dur_s",
    "prompt_n",
)  # present on turn_failed
FILTERS = ("all", "miss", "slow", "failed")
_REQ_LATE_COLS = ("concurrency", "cacheram_restore", "restored_tokens")  # added after the first release; backfilled in place


def _req_val(col, v):
    """Coerce one metrics value for the requests table; None if unusable."""
    if v is None or isinstance(v, bool):
        return None
    if col in _REQ_TEXT:
        return v[:64] if isinstance(v, str) else None
    if isinstance(v, int | float):
        if v != v or v in (float("inf"), float("-inf")):
            return None
        return int(v) if col in _REQ_INT else float(v)
    return None


def _request_row(d, line):
    """Metrics record -> (key, *REQUEST_COLS) tuple, or None for non-turn records.

    A turn record is any record without an "event" key and without a kv_* key
    (kv_adaptive save/restore events) that carries `route` or `served`; a
    `turn_failed` event is a failed row. Everything else is skipped.
    """
    if not isinstance(d, dict):
        return None
    ev = d.get("event")
    if ev == "turn_failed":
        status = "failed"
        if not d.get("reason"):
            return None
    elif ev is not None or any(str(k).startswith("kv_") for k in d):
        return None
    elif "route" in d or "served" in d:
        status = "ok"
    else:
        return None
    ep = _iso_epoch(d.get("ts")) if isinstance(d.get("ts"), str) else None
    if ep is None:
        return None
    rid = d.get("request_id")
    if isinstance(rid, str) and rid:
        key = f"{status}:{rid}:{int(ep)}"  # a client may reuse request ids
    else:
        key = "h:" + hashlib.sha1(line.encode("utf-8", "replace")).hexdigest()
    vals = []
    for c in REQUEST_COLS:
        if c == "ts":
            vals.append(ep)
        elif c == "status":
            vals.append(status)
        elif c == "fail_reason":
            vals.append(_req_val(c, d.get("reason")) if status == "failed" else None)
        elif status == "failed" and c not in FAIL_FIELDS:
            vals.append(None)
        elif c == "cacheram_restore":
            v = d.get(c)
            vals.append(int(v) if isinstance(v, bool) else None)
        else:
            vals.append(_req_val(c, d.get(c)))
    return (key, *vals)


# ---------------- kv_events table ----------------
# The proxy's own KV save/restore admin calls (kv_adaptive records), one row each.
# Numbers and a short prefix hash only; the snapshot file name is not stored.
KV_COLS = ("ts", "kind", "ok", "dur_s", "slot", "prefix", "bytes")
KV_DDL = (
    "CREATE TABLE IF NOT EXISTS kv_events(id INTEGER PRIMARY KEY, key TEXT UNIQUE, "
    "ts REAL, kind TEXT, ok INTEGER, dur_s REAL, slot INTEGER, prefix TEXT, bytes INTEGER)"
)
_KV_INSERT = (
    f"INSERT OR IGNORE INTO kv_events(key, {', '.join(KV_COLS)}) "
    f"VALUES(?{', ?' * len(KV_COLS)})"
)


def _kv_row(d, line):
    """kv_adaptive save/restore record -> (key, *KV_COLS) tuple, or None."""
    if not isinstance(d, dict) or d.get("kv_adaptive") not in ("save", "restore"):
        return None
    ep = _iso_epoch(d.get("ts")) if isinstance(d.get("ts"), str) else None
    if ep is None:
        return None
    key = "kv:" + hashlib.sha1(line.encode("utf-8", "replace")).hexdigest()
    pre = d.get("prefix")
    return (
        key,
        ep,
        d["kv_adaptive"],
        1 if d.get("ok") is True else 0,
        _req_val("dur_s", d.get("dur_s")),
        _req_val("slot", d.get("slot")),  # "slot" and "bytes" are integer columns
        pre[:64] if isinstance(pre, str) else None,
        _req_val("slot", d.get("bytes")),
    )


def _flush_requests():
    """Write queued request rows to SQLite. Call without holding _lock."""
    with _lock:
        rows = _req_pending[:]
        del _req_pending[:]
        kvs = _kv_pending[:]
        del _kv_pending[:]
    if not rows and not kvs:
        return
    try:
        con = _db()
        with con:
            con.executemany(_REQ_INSERT, rows)
            con.executemany(_KV_INSERT, kvs)
        con.close()
    except Exception as e:
        print("requests flush error:", e)
        with _lock:  # keep for the next attempt (bounded)
            _req_pending[:0] = rows
            del _req_pending[50000:]
            _kv_pending[:0] = kvs
            del _kv_pending[50000:]


def _rotated_metrics_files():
    """METRICS_FILE plus rotated siblings: .N and .N.gz (any order)."""
    out = [METRICS_FILE]
    for p in sorted(glob.glob(glob.escape(METRICS_FILE) + ".*")):
        suffix = p[len(METRICS_FILE) + 1 :]
        if suffix.endswith(".gz"):
            suffix = suffix[:-3]
        if suffix.isdigit():
            out.append(p)
    return out


def backfill_requests():
    """Idempotent: load the last RETENTION_DAYS of turn records from metrics.jsonl
    and its rotated siblings into the requests table. Safe on every start."""
    cutoff = time.time() - RETENTION_DAYS * 86400
    rows = []
    kvs = []
    for path in _rotated_metrics_files():
        try:
            opener = gzip.open if path.endswith(".gz") else open
            with opener(path, "rt", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                        kv = _kv_row(rec, line)
                        row = None if kv is not None else _request_row(rec, line)
                    except Exception:
                        continue
                    if kv is not None and kv[1] >= cutoff:
                        kvs.append(kv)
                    if row is not None and row[1] >= cutoff:
                        rows.append(row)
        except Exception as e:  # missing/truncated file: keep what was read
            if os.path.exists(path):
                print("requests backfill: partial read of", path, e)
    try:
        con = _db()
        with con:
            con.executemany(_REQ_INSERT, rows)
            con.executemany(_KV_INSERT, kvs)
            # INSERT OR IGNORE skips rows stored before a column existed, so
            # fill those columns on existing rows (only where still NULL).
            for col in _REQ_LATE_COLS:
                i = 1 + REQUEST_COLS.index(col)
                con.executemany(
                    f"UPDATE requests SET {col}=? WHERE key=? AND {col} IS NULL",
                    [(r[i], r[0]) for r in rows if r[i] is not None],
                )
            con.execute("DELETE FROM requests WHERE ts < ?", (cutoff,))
            con.execute("DELETE FROM kv_events WHERE ts < ?", (cutoff,))
        con.close()
        print(f"requests backfill: {len(rows)} rows, {len(kvs)} kv events seen")
    except Exception as e:
        print("requests backfill skipped:", e)


def request_series(window_sec, now=None):
    """Per-bucket request series for the charts, from the requests table.

    Buckets are bucket_width(window_sec) wide (the rule sample_series uses);
    buckets with no rows are omitted. `n` counts ok rows, `failed` failed rows.
    Percentiles follow window_stats (_median, _pctile); prefill only counts ok
    rows with prompt_n - cache_n >= 2000. cache_hit_pct is cached/prompt tokens,
    miss_pct is misses/classified rows (Gufo: upstream_cache == 'miss'; else
    served == 'cold'), miss_reasons maps reason -> count over the misses.
    """
    now = time.time() if now is None else now
    width = bucket_width(window_sec)
    cols = ", ".join(REQUEST_COLS)
    try:
        con = _db()
        try:
            cur = con.execute(
                f"SELECT {cols} FROM requests WHERE ts >= ? AND ts <= ?",
                (now - window_sec, now),
            )
            rows = [dict(zip(REQUEST_COLS, r, strict=True)) for r in cur]
        finally:
            con.close()
    except Exception as e:
        print("request_series error:", e)
        return []
    groups = {}
    for r in rows:
        groups.setdefault(int(r["ts"] // width) * width, []).append(r)

    def pair(vals, d):
        return _r(_median(vals), d), _r(_pctile(vals, 95), d)

    # llama.cpp: restore time comes from the proxy's own kv_adaptive events
    kvg = None
    if _restore_source([r for r in rows if r["status"] == "ok"]) == "proxy":
        kvg = {}
        for k in _kv_rows(now - window_sec, now):
            if k["kind"] == "restore" and k["ok"] and k["dur_s"] is not None:
                kvg.setdefault(int(k["ts"] // width) * width, []).append(k["dur_s"] * 1000.0)

    pts = []
    for t in sorted(groups):
        g = groups[t]
        ok = [r for r in g if r["status"] == "ok"]
        dec = [r["decode_tps"] for r in ok if r["decode_tps"]]
        pre = [
            r["prefill_tps"]
            for r in ok
            if r["prefill_tps"]
            and r["prompt_n"] is not None
            and r["cache_n"] is not None
            and r["prompt_n"] - r["cache_n"] >= 2000
        ]
        ttft = [r["ttft_s"] for r in ok if r["ttft_s"] is not None]
        rst = [r["upstream_restore_ms"] for r in ok if r["upstream_restore_ms"] is not None]
        pr = [r for r in ok if r["prompt_n"] is not None and r["cache_n"] is not None]
        pt = sum(r["prompt_n"] for r in pr)
        ct = sum(r["cache_n"] for r in pr)
        cls = []  # (is_miss, reason)
        for r in ok:
            if r["upstream_cache"] is not None:
                cls.append((r["upstream_cache"] == "miss", r["upstream_cache_miss_reason"]))
            elif r["served"] in ("warm", "cold"):
                cls.append((r["served"] == "cold", r["upstream_cache_miss_reason"]))
        reasons = collections.Counter((why or "unknown") for miss, why in cls if miss)
        dn = sum(r["draft_n"] for r in ok if r["draft_n"] and r["draft_n"] > 0)
        da = sum(r["draft_n_accepted"] or 0 for r in ok if r["draft_n"] and r["draft_n"] > 0)
        d50, d95 = pair(dec, 2)
        p50, p95 = pair(pre, 2)
        t50, t95 = pair(ttft, 3)
        r50, r95 = pair(kvg.get(t, []) if kvg is not None else rst, 1)
        pts.append(
            {
                "t": t,
                "n": len(ok),
                "failed": len(g) - len(ok),
                "decode_p50": d50,
                "decode_p95": d95,
                "prefill_p50": p50,
                "prefill_p95": p95,
                "ttft_p50": t50,
                "ttft_p95": t95,
                "restore_ms_p50": r50,
                "restore_ms_p95": r95,
                "cache_hit_pct": _r(100.0 * ct / pt, 1) if pt else None,
                "miss_pct": _r(100.0 * sum(1 for m, _ in cls if m) / len(cls), 1) if cls else None,
                "draft_accept_pct": _r(100.0 * da / dn, 1) if dn else None,
                "miss_reasons": dict(reasons),
            }
        )
    return pts


def _parse_req_params(q, now=None):
    """Query dict (parse_qs shape) -> (params, None) or (None, error message)."""

    def one(name, default):
        v = q.get(name)
        return default if not v else v[0]

    try:
        window = int(one("window", "3600"))
        offset = int(one("offset", "0"))
        limit = int(one("limit", "50"))
    except ValueError:
        return None, "window, offset and limit must be integers"
    now = time.time() if now is None else now
    rng = {}
    for name in ("from", "to"):
        raw = one(name, None)
        if raw is None:
            continue
        try:
            v = float(raw)
        except ValueError:
            return None, "from and to must be epoch seconds"
        if not math.isfinite(v):
            return None, "from and to must be epoch seconds"
        rng[name] = v
    lo = now - RETENTION_DAYS * 86400
    if rng:
        # clamp to retention: [now - 8 d, now]; a missing end is the window edge / now
        t_from = max(
            lo, min(rng.get("from", now - max(300, min(window, RETENTION_DAYS * 86400))), now)
        )
        t_to = max(lo, min(rng.get("to", now), now))
        if t_from > t_to:
            return None, "from must not be after to"
    else:
        t_from = t_to = None
    flt = one("filter", "all")
    if flt not in FILTERS:
        return None, "filter must be one of " + ", ".join(FILTERS)
    route = one("route", None)
    if route is not None and len(route) > 200:
        return None, "route too long"
    return {
        "window": max(300, min(window, RETENTION_DAYS * 86400)),
        "offset": max(0, min(offset, 10_000_000)),
        "limit": max(1, min(limit, 200)),
        "filter": flt,
        "route": route,
        "from": t_from,
        "to": t_to,
    }, None


def query_requests(q, now=None):
    """GET /api/requests logic -> (http_code, json-able dict)."""
    now = time.time() if now is None else now
    p, err = _parse_req_params(q, now)
    if err:
        return 400, {"error": err}
    where = ["ts >= ?"]
    args = [now - p["window"]]
    if p["from"] is not None:
        # an explicit time range replaces the window for row selection (the
        # window still defines the "slow" threshold below)
        where = ["ts >= ? AND ts <= ?"]
        args = [p["from"], p["to"]]
    if p["route"] is not None:
        where.append("route = ?")
        args.append(p["route"])
    if p["filter"] == "miss":
        where.append("status = 'ok' AND served IS NOT NULL AND served != 'warm'")
    elif p["filter"] == "failed":
        where.append("status = 'failed'")
    cols = ", ".join(REQUEST_COLS)
    try:
        con = _db()
        try:
            # p95 of ttft_s over the whole window (all routes), same method
            # as the rest of the dashboard (_pctile). Also returned as
            # slow_ttft_s so the page can flag slow rows.
            tt = [
                r[0]
                for r in con.execute(
                    "SELECT ttft_s FROM requests WHERE ts >= ? AND status='ok' "
                    "AND ttft_s IS NOT NULL",
                    (now - p["window"],),
                )
            ]
            p95 = _pctile(tt, 95)
            if p["filter"] == "slow":
                if p95 is None:
                    where.append("0")
                else:
                    where.append("status = 'ok' AND ttft_s > ?")
                    args.append(p95)
            w = " AND ".join(f"({x})" for x in where)
            total = con.execute(f"SELECT COUNT(*) FROM requests WHERE {w}", args).fetchone()[0]
            cur = con.execute(
                f"SELECT {cols} FROM requests WHERE {w} ORDER BY ts DESC, id DESC LIMIT ? OFFSET ?",
                [*args, p["limit"], p["offset"]],
            )
            rows = []
            for r in cur:
                d = dict(zip(REQUEST_COLS, r, strict=True))
                pn, cn = d["prompt_n"], d["cache_n"]
                d["new_tokens"] = max(0, pn - cn) if pn is not None and cn is not None else None
                d["cached_pct"] = round(100 * cn / pn, 1) if pn and cn is not None else None
                rows.append(d)
        finally:
            con.close()
    except Exception as e:
        return 500, {"error": str(e)}
    out = {"total": total, "window_sec": p["window"], "slow_ttft_s": p95, "rows": rows}
    if p["from"] is not None:
        out["from"], out["to"] = p["from"], p["to"]
    return 200, out


# ---------------- window cards ----------------
def drafter_label(config_id=None):
    """Draft-accept card label: DASH_DRAFTER, else MTP / DFlash2 / Draft by config id."""
    if DRAFTER:
        return DRAFTER
    cid = (read_config_id() if config_id is None else config_id or "").lower()
    if "mtp" in cid:
        return "MTP"
    if "dflash2" in cid:
        return "DFlash2"
    return "Draft"


def _slot_count():
    """Slots on this box: `slots` from the proxy config (DASH_SLOTPIN_CFG), so the
    answer does not depend on upstream state; only when the config gives none, the
    length of the last /slots list the collector saw."""
    n = read_config_slots()
    if n is not None:
        return n
    with _lock:
        return len(_live.get("slots") or [])


def _median(vals):
    if not vals:
        return None
    s = sorted(vals)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def _top(vals):
    """Most common non-empty value (ties: alphabetical), or None."""
    c = collections.Counter(v for v in vals if v)
    return min(c.items(), key=lambda kv: (-kv[1], kv[0]))[0] if c else None


def _r(v, d=2):
    return None if v is None else round(v, d)


def _restore_source(ok):
    """"upstream" (Gufo restore fields on the turn) or "proxy" (the proxy's own
    kv_adaptive events): the kind of the most recent ok turn that shows either."""
    last = None
    for r in ok:
        if r["upstream_cache"] is not None:
            k = "upstream"
        elif r["served"] in ("warm", "cold"):
            k = "proxy"
        else:
            continue
        if last is None or r["ts"] >= last[0]:
            last = (r["ts"], k)
    return last[1] if last else "upstream"


def _kv_rows(t0, t1=None):
    """kv_events rows with t0 <= ts (<= t1) as dicts; [] on any error."""
    try:
        con = _db()
        try:
            q, a = "SELECT " + ", ".join(KV_COLS) + " FROM kv_events WHERE ts >= ?", [t0]
            if t1 is not None:
                q, a = q + " AND ts <= ?", [t0, t1]
            return [dict(zip(KV_COLS, r, strict=True)) for r in con.execute(q, a)]
        finally:
            con.close()
    except Exception as e:
        print("kv_events read error:", e)
        return []


def window_stats(window_sec, now=None):
    """Every card value for one window, from the requests table.

    Exact medians (mean of the two middle values for even n) and p95 (the
    dashboard's _pctile, the same one the turn list's "slow" filter uses) are
    computed here from the selected rows. Same keys for every window; a measure
    with no data is None. Live (900 s) and longer windows share this function.
    """
    now = time.time() if now is None else now
    cols = ", ".join(REQUEST_COLS)
    try:
        con = _db()
        try:
            cur = con.execute(f"SELECT {cols} FROM requests WHERE ts >= ?", (now - window_sec,))
            rows = [dict(zip(REQUEST_COLS, r, strict=True)) for r in cur]
        finally:
            con.close()
    except Exception as e:
        print("window_stats error:", e)
        rows = []
    ok = [r for r in rows if r["status"] == "ok"]
    bad = [r for r in rows if r["status"] == "failed"]

    def col(rs, c):
        return [r[c] for r in rs if r[c] is not None]

    def pq(vals, d=2):
        return {"p50": _r(_median(vals), d), "p95": _r(_pctile(vals, 95), d), "n": len(vals)}

    dec = [r["decode_tps"] for r in ok if r["decode_tps"]]
    pre = [
        r["prefill_tps"]
        for r in ok
        if r["prefill_tps"]
        and r["prompt_n"] is not None
        and r["cache_n"] is not None
        and r["prompt_n"] - r["cache_n"] >= 2000
    ]
    ttft = col(ok, "ttft_s")
    total = len(rows)
    out = {
        "window_sec": window_sec,
        "decode": pq(dec),
        "prefill": pq(pre),
        "ttft": {
            "p50": _r(_median(ttft), 3),
            "p95": _r(_pctile(ttft, 95), 3),
            "n": len(ttft),
            "queue_wait_ms_p50": _r(_median(col(ok, "queue_wait_ms")), 1),
        },
        "requests": {
            "count": total if total else None,
            "per_hour": _r(total * 3600.0 / window_sec, 2) if total and window_sec else None,
        },
    }

    prop = sum(r["draft_n"] for r in ok if r["draft_n"] is not None and r["draft_n"] > 0)
    acc = sum(r["draft_n_accepted"] or 0 for r in ok if r["draft_n"])
    out["draft"] = {
        "drafter": drafter_label(),
        "accepted": acc if prop else None,
        "proposed": prop if prop else None,
        "pct": _r(100.0 * acc / prop, 1) if prop else None,
    }

    conc = None
    if _slot_count() > 1:
        cv = col(ok, "concurrency")
        agg = [
            r["decode_tps"] * r["concurrency"] for r in ok if r["decode_tps"] and r["concurrency"]
        ]
        conc = {
            "mean": _r(sum(cv) / len(cv), 2) if cv else None,
            "peak": _r(max(cv), 2) if cv else None,
            "agg_decode_tps": _r(sum(agg) / len(agg), 1) if agg else None,
            "n": len(cv),
        }
    out["concurrency"] = conc

    pr = [r for r in ok if r["prompt_n"] is not None and r["cache_n"] is not None]
    pt, ct = sum(r["prompt_n"] for r in pr), sum(r["cache_n"] for r in pr)
    out["cache_hit"] = {
        "pct": _r(100.0 * ct / pt, 1) if pt else None,
        "cached_tokens": ct if pt else None,
        "prompt_tokens": pt if pt else None,
    }

    gufo = [r for r in ok if r["upstream_cache"] is not None]
    cs = {
        "kind": None,
        "memory": None,
        "disk": None,
        "warm": None,
        "cold": None,
        "miss": None,
        "miss_pct": None,
        "top_miss_reason": None,
        "n": 0,
    }
    if gufo:
        mem = sum(1 for r in gufo if r["upstream_cache"] == "memory")
        dsk = sum(1 for r in gufo if r["upstream_cache"] == "disk")
        mis = sum(1 for r in gufo if r["upstream_cache"] == "miss")
        n = mem + dsk + mis
        cs.update(
            kind="gufo",
            memory=mem,
            disk=dsk,
            warm=mem + dsk,
            cold=mis,
            miss=mis,
            n=n,
            miss_pct=_r(100.0 * mis / n, 1) if n else None,
            top_miss_reason=_top(
                r["upstream_cache_miss_reason"] for r in gufo if r["upstream_cache"] == "miss"
            ),
        )
    else:
        sv = [r for r in ok if r["served"] in ("warm", "cold")]
        if sv:
            w = sum(1 for r in sv if r["served"] == "warm")
            c = len(sv) - w
            cs.update(
                kind="llamacpp",
                warm=w,
                cold=c,
                miss=c,
                n=len(sv),
                miss_pct=_r(100.0 * c / len(sv), 1),
                top_miss_reason=_top(
                    r["upstream_cache_miss_reason"] for r in sv if r["served"] == "cold"
                ),
            )
    out["cache_source"] = cs

    if _restore_source(ok) == "proxy":
        kv = _kv_rows(now - window_sec)
        rest = [k for k in kv if k["kind"] == "restore"]
        saves = [k for k in kv if k["kind"] == "save"]
        rms = [k["dur_s"] * 1000.0 for k in rest if k["ok"] and k["dur_s"] is not None]
        sms = [k["dur_s"] * 1000.0 for k in saves if k["ok"] and k["dur_s"] is not None]
        sbytes = [k["bytes"] for k in saves if k["ok"] and k["bytes"] is not None]
        ram = [r for r in ok if r["cacheram_restore"]]
        out["restore"] = {
            "source": "proxy",
            "p50": _r(_median(rms), 1),
            "p95": _r(_pctile(rms, 95), 1),
            "n": len(rms),
            "misses": sum(1 for k in rest if not k["ok"]),
            "save_ms_p50": _r(_median(sms), 1),
            "saves": sum(1 for k in saves if k["ok"]),
            "save_failures": sum(1 for k in saves if not k["ok"]),
            "snapshot_ms_p50": _r(_median(sms), 1),
            "snapshot_bytes_p50": _r(_median(sbytes), 0),
            "cacheram_restores": len(ram),
            "cacheram_tokens": sum(r["restored_tokens"] or 0 for r in ram),
        }
    else:
        rs = col(ok, "upstream_restore_ms")
        out["restore"] = {
            "source": "upstream",
            "p50": _r(_median(rs), 1),
            "p95": _r(_pctile(rs, 95), 1),
            "n": len(rs),
            "snapshot_ms_p50": _r(_median(col(ok, "upstream_snapshot_ms")), 1),
            "snapshot_bytes_p50": _r(_median(col(ok, "upstream_snapshot_bytes")), 0),
        }

    out["failures"] = {
        "count": len(bad) if total else None,
        "pct": _r(100.0 * len(bad) / total, 1) if total else None,
        "top_reason": _top(r["fail_reason"] for r in bad),
    }

    er = [
        (r["energy_fair_wh"] if r["energy_fair_wh"] is not None else r["energy_turn_wh"], r)
        for r in ok
        if r["energy_fair_wh"] is not None or r["energy_turn_wh"] is not None
    ]
    wh = sum(e for e, _ in er)
    gen = sum(r["completion_n"] or 0 for _, r in er)
    out["energy"] = {
        "wh_per_request": _r(wh / len(er), 3) if er else None,
        "wh_per_1k_tokens": _r(wh * 1000.0 / gen, 3) if er and gen else None,
        "n": len(er),
    }
    return out


# ---------------- collector thread ----------------
def collector():
    global _last_build
    last_flush = 0
    if _last_build is None:  # the build the proxy last recorded, from before we started
        for rec in reversed(tail_metrics()):
            if isinstance(rec.get("upstream_build"), str) and rec["upstream_build"]:
                _last_build = rec["upstream_build"]
                break
    while True:
        now = time.time()
        slots_ok = False
        slots_new = None
        try:
            slots_new = _get_json(UPSTREAM + "/slots", timeout=8)
            if isinstance(slots_new, dict):
                slots_new = slots_new.get("slots", [])
            # whitelist fields now so prompt text never lives past this line
            slots_new = [f for f in map(_slot_fields, slots_new) if f is not None]
            slots_ok = True
        except Exception:
            slots_ok = False
        energy_ok = False
        energy = None
        if ENERGY_URL:
            try:
                txt = urllib.request.urlopen(ENERGY_URL, timeout=6).read().decode()
                energy = _parse_energy(txt)
                energy_ok = True
                if "total_energy" in energy:
                    _energy_hist.append((now, energy["total_energy"]))
            except Exception:
                energy = None  # no sample this cycle: never re-fold the last reading as fresh
        mem = read_meminfo() or _live.get("mem")
        # same /sys amdgpu GTT counter the proxy puts in its turn records
        gtt_now = sysmem.sample_mem().get("gtt_used_mb")
        if mem is not None or gtt_now is not None:
            mem = dict(mem or {}, gtt_used_mb=gtt_now)
        temps = read_temps()
        proxy_up = _tcp_up(PROXY_ADDR)
        counters = _read_upstream_counters() if slots_ok else {}
        cfg_id = read_config_id()

        with _lock:
            _read_new_metrics()
            _fold_sample(energy, mem, temps, now)
            _fold_sample_v2(energy, mem, temps, now)
            if energy_ok:
                _live["energy_ts"] = now
            _temp_hist.append((now, temps.get("cpu_c"), temps.get("gpu_c"), temps.get("nvme_c")))
            # upstream down / up (restart) events
            n_ev = len(_events)
            _restart_step(now, slots_ok, counters, cfg_id)
            _stall_step(now, slots_ok, slots_new, _last_completion_ts)
            new_events = len(_events) > n_ev
            _prune_mem()
            if energy_ok:
                _live["energy"] = energy  # on failure keep the last reading, flagged stale
            upd = {
                "slots_ok": bool(slots_ok),
                "energy_ok": energy_ok,
                "mem": mem,
                "temps": temps,
                "proxy_up": proxy_up,
                "poll_ts": now,
            }
            if slots_ok:
                upd["slots"] = slots_new
            _live.update(upd)

        _flush_requests()
        if new_events:
            _flush_events()
        if now - last_flush > 60:
            _flush_db()
            last_flush = now
        time.sleep(POLL_SEC)


def backfill():
    """One-time: fold the whole metrics file into buckets, then set offset."""
    global _offset
    try:
        if _offset > 0:
            _read_new_metrics()  # already loaded from db; just catch up
            return
        with open(METRICS_FILE, "rb") as f:
            for raw in f:
                try:
                    _fold_request(json.loads(raw.decode("utf-8", "replace")))
                except Exception:
                    pass
            _offset = f.tell()
        print(f"backfilled {len(_buckets)} buckets from {METRICS_FILE}")
    except Exception as e:
        print("backfill skipped:", e)


# ---------------- live state (for the now-cards, slots, recent) ----------------
def _db_last_completion():
    """Epoch of the newest ok row in the requests table, or None."""
    try:
        con = _db()
        try:
            return con.execute("SELECT MAX(ts) FROM requests WHERE status='ok'").fetchone()[0]
        finally:
            con.close()
    except Exception:
        return None


def tail_metrics(max_bytes=131072):
    recs = []
    try:
        sz = os.path.getsize(METRICS_FILE)
        with open(METRICS_FILE, "rb") as f:
            if sz > max_bytes:
                f.seek(sz - max_bytes)
                f.readline()
            data = f.read().decode("utf-8", "replace")
        for line in data.splitlines():
            line = line.strip()
            if line:
                try:
                    recs.append(json.loads(line))
                except Exception:
                    pass
    except Exception:
        pass
    return recs


def build_state():
    with _lock:
        live = dict(_live)
        ehist = list(_energy_hist)
        thist = list(_temp_hist)
        stall_info = dict(_stall_info) if _stall_info else None
        evs = list(_events)
        mem_completion = _last_completion_ts
    recs = tail_metrics()
    now = time.time()
    with _lock:
        tracked = _stall.snapshot(now)
    win0 = now - WINDOW_MIN * 60

    w = {
        "reqs": 0,
        "warm": 0,
        "cold": 0,
        "restore": 0,
        "restored_tok": 0,
        "completion": 0,
        "prompt": 0,
        "cache": 0,
        "draft": 0,
        "draft_acc": 0,
    }
    ttft = []
    dtps = []
    ptps = []
    conc = []
    for d in recs:
        ep = _iso_epoch(d.get("ts"))
        if ep is None or ep < win0:
            continue
        w["reqs"] += 1
        srv = d.get("served")
        if srv == "warm":
            w["warm"] += 1
        elif srv == "cold":
            w["cold"] += 1
        if d.get("cacheram_restore"):
            w["restore"] += 1
            w["restored_tok"] += d.get("restored_tokens") or 0
        w["completion"] += d.get("completion_n") or 0
        w["prompt"] += d.get("prompt_n") or 0
        w["cache"] += d.get("cache_n") or 0
        w["draft"] += d.get("draft_n") or 0
        w["draft_acc"] += d.get("draft_n_accepted") or 0
        if d.get("ttft_s") is not None:
            ttft.append(d["ttft_s"])
        if d.get("decode_tps"):
            dtps.append(d["decode_tps"])
            conc.append(d.get("concurrency") or 1.0)
        if d.get("prefill_tps"):
            ptps.append(d["prefill_tps"])

    def avg(xs):
        return round(sum(xs) / len(xs), 2) if xs else None

    agg = [dt * c for dt, c in zip(dtps, conc, strict=False)]
    peak_conc = round(max(conc)) if conc else 1
    avg_conc = round(sum(conc) / len(conc), 1) if conc else None

    wh_per_req = wh_per_1k = None
    if ehist:
        cur = ehist[-1][1]
        base = None
        for ep, kwh in ehist:
            if ep >= win0:
                base = kwh
                break
        if base is None:
            base = ehist[0][1]
        dk = cur - base
        if dk > 0 and w["reqs"] > 0:
            wh_per_req = round(dk * 1000 / w["reqs"], 2)
        if dk > 0 and w["completion"] > 0:
            wh_per_1k = round(dk * 1000 / (w["completion"] / 1000.0), 3)

    recent = []

    def _real(d):
        return not (
            d.get("completion_n") is None and d.get("prompt_n") is None and d.get("ttft_s") is None
        )

    for d in sorted([x for x in recs if _real(x)], key=lambda x: _iso_epoch(x.get("ts")) or 0)[
        -N_RECENT:
    ][::-1]:
        recent.append(
            {
                "ts": d.get("ts"),
                "slot": d.get("slot"),
                "prefix": d.get("prefix"),
                "ttft_s": d.get("ttft_s"),
                "decode_tps": d.get("decode_tps"),
                "prefill_tps": d.get("prefill_tps"),
                "prompt_n": d.get("prompt_n"),
                "completion_n": d.get("completion_n"),
                "cache_n": d.get("cache_n"),
                "served": d.get("served"),
                "restored_tokens": d.get("restored_tokens"),
                "stop_reason": d.get("stop_reason"),
                "ok": d.get("ok"),
            }
        )

    slots = []
    for i, raw in enumerate(live.get("slots") or []):
        s = _slot_fields(raw)
        if s is None:
            continue
        busy = bool(s["is_processing"])
        nd = s["n_decoded"]
        bf, st_s = tracked.get(s.get("id", i), (None, None)) if busy else (None, None)
        slots.append(
            {
                "id": s["id"],
                "busy": busy,
                "id_task": s["id_task"],
                "n_prompt": s["n_prompt_tokens"],
                "n_cache": s["n_prompt_tokens_cache"],
                "n_ctx": s["n_ctx"],
                "phase": ("generating" if nd else "prefill") if busy else "idle",
                "busy_for_s": bf,
                "n_processed": s["n_prompt_tokens_processed"],
                "n_decoded": nd,
                "stall_s": st_s,
            }
        )
    en = live.get("energy") or {}
    mem = live.get("mem") or {}
    tv = live.get("temps") or {}
    cpu_ts = [c for ts, c, _g, _n in thist if ts >= win0 and c is not None]
    gpu_ts = [g for ts, _c, g, _n in thist if ts >= win0 and g is not None]
    nvme_ts = [n for ts, _c, _g, n in thist if ts >= win0 and n is not None]
    build = next((d["upstream_build"] for d in reversed(recs) if d.get("upstream_build")), None)
    # "now" is the live sample (the collector's amdgpu GTT reading); the last request
    # record is only a fallback, it can be hours old when the upstream is down
    gtt = mem.get("gtt_used_mb")
    if gtt is None:
        gtt = next(
            (d.get("gtt_used_mb") for d in reversed(recs) if d.get("gtt_used_mb") is not None),
            None,
        )
    recent_ok = any((_iso_epoch(d.get("ts")) or 0) > now - 180 and d.get("ok") for d in recs)
    model_up = bool(live.get("slots_ok") or recent_ok)

    busy_for = [s["busy_for_s"] for s in slots if s["busy_for_s"] is not None]
    last_comp = _db_last_completion()
    if mem_completion is not None and (last_comp is None or mem_completion > last_comp):
        last_comp = mem_completion
    if stall_info:
        upstream = stall_info["state"]
    else:
        upstream = "up" if live.get("slots_ok") else "down"
    last_up = next((t for t, k, _d in reversed(evs) if k == "up"), None)
    health_v2 = {
        "upstream": upstream,
        "stalled_s": stall_info["stalled_s"] if stall_info and upstream == "stalled" else None,
        "last_completion_ts": last_comp,
        "last_completion_age_s": round(now - last_comp, 1) if last_comp is not None else None,
        "busy_slots": sum(1 for s in slots if s["busy"]),
        "busy_for_s": max(busy_for) if busy_for else None,
        "uptime_s": round(now - last_up, 1) if last_up is not None and upstream != "down" else None,
        "last_restart_ts": last_up,
        "config_id": read_config_id(),
        "stall_threshold_s": STALL_S,
    }

    return {
        "now": now,
        "window_min": WINDOW_MIN,
        "poll_age": round(now - live["poll_ts"], 1) if live["poll_ts"] else None,
        "health": {
            "proxy_up": live["proxy_up"],
            "model_up": model_up,
            "slots_ok": bool(live.get("slots_ok")),
            "config_id": read_config_id(),
            "upstream_build": build,
            "slots_total": len(slots),
            "slots_busy": sum(1 for s in slots if s["busy"]),
        },
        "system": {
            "mem_avail_mb": mem.get("mem_avail_mb"),
            "mem_total_mb": mem.get("mem_total_mb"),
            "swap_free_mb": mem.get("swap_free_mb"),
            "swap_total_mb": mem.get("swap_total_mb"),
            "gtt_used_mb": gtt,
            "mem_free_mb": mem.get("mem_free_mb"),
            "page_cache_mb": mem.get("page_cache_mb"),
            "swap_used_mb": mem.get("swap_used_mb"),
        },
        "energy": {
            "power_w": en.get("power"),
            "daily_kwh": en.get("daily_energy"),
            "total_kwh": en.get("total_energy"),
            "voltage": en.get("voltage"),
            "stale": bool(live.get("energy_ts") is None or live.get("energy_ok") is False),
            "age_s": (
                round(now - live["energy_ts"], 1) if live.get("energy_ts") is not None else None
            ),
        },
        "temps": {
            "cpu_c": tv.get("cpu_c"),
            "gpu_c": tv.get("gpu_c"),
            "nvme_c": tv.get("nvme_c"),
            "cpu_avg_c": round(sum(cpu_ts) / len(cpu_ts), 1) if cpu_ts else None,
            "cpu_max_c": round(max(cpu_ts), 1) if cpu_ts else None,
            "gpu_avg_c": round(sum(gpu_ts) / len(gpu_ts), 1) if gpu_ts else None,
            "gpu_max_c": round(max(gpu_ts), 1) if gpu_ts else None,
            "nvme_avg_c": round(sum(nvme_ts) / len(nvme_ts), 1) if nvme_ts else None,
            "nvme_max_c": round(max(nvme_ts), 1) if nvme_ts else None,
            "warn_c": TEMP_WARN_C,
            "crit_c": TEMP_CRIT_C,
        },
        "window": {
            "reqs": w["reqs"],
            "warm": w["warm"],
            "cold": w["cold"],
            "warm_pct": round(100 * w["warm"] / w["reqs"]) if w["reqs"] else None,
            "restore_events": w["restore"],
            "restored_tokens": w["restored_tok"],
            "completion_tokens": w["completion"],
            "prompt_tokens": w["prompt"],
            "cache_tokens": w["cache"],
            "cache_hit_pct": round(100 * w["cache"] / w["prompt"]) if w["prompt"] else None,
            "reqs_per_min": round(w["reqs"] / WINDOW_MIN, 2),
            "avg_ttft_s": avg(ttft),
            "avg_decode_tps": avg(dtps),
            "avg_prefill_tps": avg(ptps),
            "avg_decode_agg_tps": avg(agg),
            "avg_concurrency": avg_conc,
            "peak_concurrency": peak_conc,
            "multi_slot": peak_conc >= 2,
            "mtp_active": w["draft"] > 0,
            "draft_proposed": w["draft"],
            "draft_accepted": w["draft_acc"],
            "mtp_accept_pct": round(100 * w["draft_acc"] / w["draft"]) if w["draft"] else None,
            "wh_per_req": wh_per_req,
            "wh_per_1k_tok": wh_per_1k,
        },
        "health_v2": health_v2,
        "cards": window_stats(LIVE_WINDOW_SEC, now),
        "slots": slots,
        "recent": recent,
    }


# ---------------- history (selectable window) ----------------
def build_history(window_sec):
    now = time.time()
    cutoff = now - window_sec
    with _lock:
        keys = sorted(k for k in _buckets if k + BUCKET_SEC > cutoff)
        buckets = [(k, dict(_buckets[k])) for k in keys]
        raw_events = [(ts, kind, detail) for ts, kind, detail in _events if ts >= cutoff]
    events = []
    for ts, kind, detail in raw_events:
        e = {"t": ts, "kind": kind}
        if detail is not None:
            e["detail"] = detail
        events.append(e)
    failed = failed_pct = top_fail = None
    try:
        con = _db()
        try:
            total_rows = con.execute(
                "SELECT COUNT(*) FROM requests WHERE ts >= ?", (cutoff,)
            ).fetchone()[0]
            failed = con.execute(
                "SELECT COUNT(*) FROM requests WHERE ts >= ? AND status='failed'", (cutoff,)
            ).fetchone()[0]
            failed_pct = round(100 * failed / total_rows, 1) if total_rows else None
            r = con.execute(
                "SELECT fail_reason, COUNT(*) c FROM requests WHERE ts >= ? AND status='failed' "
                "AND fail_reason IS NOT NULL GROUP BY fail_reason ORDER BY c DESC, fail_reason "
                "LIMIT 1",
                (cutoff,),
            ).fetchone()
            top_fail = r[0] if r else None
        finally:
            con.close()
    except Exception:
        pass

    # window summary
    tot = _new_bucket()
    dec_res = []
    ttft_res = []
    min_mem = None
    e_kwh = 0.0
    pow_sum = 0.0
    pow_n = 0
    cpu_t_max = None
    gpu_t_max = None
    nvme_t_max = None
    for _, b in buckets:
        for k in (
            "reqs",
            "warm",
            "cold",
            "restores",
            "restored",
            "prompt",
            "completion",
            "cache",
            "draft",
            "draft_acc",
            "dec_sum",
            "dec_n",
            "cpu_t_sum",
            "cpu_t_n",
            "gpu_t_sum",
            "gpu_t_n",
        ):
            tot[k] += b.get(k) or 0
        dec_res += b.get("dec_res") or []
        ttft_res += b.get("ttft_res") or []
        if b.get("min_mem") is not None:
            min_mem = b["min_mem"] if min_mem is None else min(min_mem, b["min_mem"])
        if (
            b.get("e_first") is not None
            and b.get("e_last") is not None
            and b["e_last"] >= b["e_first"]
        ):
            e_kwh += b["e_last"] - b["e_first"]
        pow_sum += b.get("pow_sum") or 0.0
        pow_n += b.get("pow_n") or 0
        if b.get("cpu_t_max") is not None:
            cpu_t_max = b["cpu_t_max"] if cpu_t_max is None else max(cpu_t_max, b["cpu_t_max"])
        if b.get("gpu_t_max") is not None:
            gpu_t_max = b["gpu_t_max"] if gpu_t_max is None else max(gpu_t_max, b["gpu_t_max"])
        if b.get("nvme_t_max") is not None:
            nvme_t_max = b["nvme_t_max"] if nvme_t_max is None else max(nvme_t_max, b["nvme_t_max"])

    with _lock:
        v2 = [b for k, b in _samples.items() if k + SAMPLE_SEC > cutoff]

    def v2agg(field, fn):
        v = [b[field] for b in v2 if b.get(field) is not None]
        return fn(v) if v else None

    reqs = tot["reqs"]
    days = window_sec / 86400.0
    summ = {
        "reqs": reqs,
        "reqs_per_hr": round(reqs / (window_sec / 3600.0), 1) if window_sec else None,
        "warm": tot["warm"],
        "cold": tot["cold"],
        "warm_pct": round(100 * tot["warm"] / (tot["warm"] + tot["cold"]))
        if (tot["warm"] + tot["cold"])
        else None,
        "restores": tot["restores"],
        "restored_tokens": tot["restored"],
        "prompt_tokens": tot["prompt"],
        "completion_tokens": tot["completion"],
        "cache_tokens": tot["cache"],
        "cache_hit_pct": round(100 * tot["cache"] / tot["prompt"]) if tot["prompt"] else None,
        "avg_decode_tps": round(tot["dec_sum"] / tot["dec_n"], 2) if tot["dec_n"] else None,
        "decode_p95": _pctile(dec_res, 95),
        "decode_p50": _pctile(dec_res, 50),
        "ttft_p50": _pctile(ttft_res, 50),
        "ttft_p95": _pctile(ttft_res, 95),
        "mtp_active": tot["draft"] > 0,
        "draft_proposed": tot["draft"],
        "draft_accepted": tot["draft_acc"],
        "mtp_accept_pct": round(100 * tot["draft_acc"] / tot["draft"]) if tot["draft"] else None,
        "min_mem_avail_mb": min_mem,
        "avg_power_w": round(pow_sum / pow_n) if pow_n else None,
        "energy_kwh": round(e_kwh, 3),
        "kwh_per_day": round(e_kwh / days, 3) if days else None,
        "wh_per_req": round(e_kwh * 1000 / reqs, 2) if reqs and e_kwh > 0 else None,
        "avg_cpu_temp_c": round(tot["cpu_t_sum"] / tot["cpu_t_n"], 1) if tot["cpu_t_n"] else None,
        "max_cpu_temp_c": cpu_t_max,
        "avg_gpu_temp_c": round(tot["gpu_t_sum"] / tot["gpu_t_n"], 1) if tot["gpu_t_n"] else None,
        "max_gpu_temp_c": gpu_t_max,
        "max_nvme_temp_c": nvme_t_max,
        "min_mem_free_mb": v2agg("min_free", min),
        "max_page_cache_mb": v2agg("max_cache", max),
        "max_swap_used_mb": v2agg("max_swap", max),
        "max_gtt_used_mb": v2agg("max_gtt", max),
        "failed": failed,
        "failed_pct": failed_pct,
        "top_fail_reason": top_fail,
        "restarts": sum(1 for e in events if e["kind"] == "up"),
        "stalls": sum(1 for e in events if e["kind"] == "stall"),
    }

    # downsample buckets into <=90 points for sparklines
    def series():
        if not buckets:
            return []
        target = 90
        step = max(1, len(buckets) // target)
        pts = []
        for i in range(0, len(buckets), step):
            grp = buckets[i : i + step]
            r = sum(b["reqs"] for _, b in grp)
            dn = sum(b["dec_n"] for _, b in grp)
            ds = sum(b["dec_sum"] for _, b in grp)
            tr = []
            for _, b in grp:
                tr += b.get("ttft_res") or []
            mm = [b["min_mem"] for _, b in grp if b.get("min_mem") is not None]
            pn = sum(b["prompt"] for _, b in grp)
            cn = sum(b["cache"] for _, b in grp)
            dr = sum(b["draft"] for _, b in grp)
            da = sum(b["draft_acc"] for _, b in grp)
            wl = sum(b["warm"] for _, b in grp)
            cl = sum(b["cold"] for _, b in grp)
            ps = sum(b["pow_sum"] for _, b in grp)
            pnn = sum(b["pow_n"] for _, b in grp)
            cts = sum(b["cpu_t_sum"] for _, b in grp)
            ctn = sum(b["cpu_t_n"] for _, b in grp)
            gts = sum(b["gpu_t_sum"] for _, b in grp)
            gtn = sum(b["gpu_t_n"] for _, b in grp)
            pts.append(
                {
                    "t": grp[0][0],
                    "reqs": r,
                    "decode": round(ds / dn, 1) if dn else None,
                    "ttft_p95": _pctile(tr, 95),
                    "cache_hit": round(100 * cn / pn) if pn else None,
                    "warm_pct": round(100 * wl / (wl + cl)) if (wl + cl) else None,
                    "mtp": round(100 * da / dr) if dr else None,
                    "mem_gb": round(min(mm) / 1024.0, 1) if mm else None,
                    "power": round(ps / pnn) if pnn else None,
                    "cpu_temp": round(cts / ctn, 1) if ctn else None,
                    "gpu_temp": round(gts / gtn, 1) if gtn else None,
                }
            )
        return pts

    return {
        "now": now,
        "window_sec": window_sec,
        "config_id": read_config_id(),
        "bucket_sec": BUCKET_SEC,
        "sample_bucket_sec": sample_bucket_width(window_sec),
        "request_bucket_sec": bucket_width(window_sec),
        "summary": summ,
        "cards": window_stats(window_sec, now),
        "series": series(),
        "samples": sample_series(window_sec, now),
        "request_series": request_series(window_sec, now),
        "events": events,
    }


# ---------------- HTTP ----------------
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path)
        try:
            if u.path == "/api/state":
                self._send(200, json.dumps(build_state()).encode(), "application/json")
            elif u.path == "/api/history":
                q = parse_qs(u.query)
                try:
                    win = int(q.get("window", ["3600"])[0])
                except ValueError:
                    body = json.dumps({"error": "window must be an integer"})
                    self._send(400, body.encode(), "application/json")
                    return
                win = max(300, min(win, RETENTION_DAYS * 86400))
                self._send(200, json.dumps(build_history(win)).encode(), "application/json")
            elif u.path == "/api/requests":
                code, obj = query_requests(parse_qs(u.query))
                self._send(code, json.dumps(obj).encode(), "application/json")
            elif u.path == "/" or u.path == "":
                self._send(200, render_page().encode(), "text/html; charset=utf-8")
            else:
                self._send(404, b"not found", "text/plain")
        except Exception as e:
            self._send(500, json.dumps({"error": str(e)}).encode(), "application/json")


def _parse_boxes(spec):
    """'name=url,name=url' -> [(name,url), ...], skipping malformed entries."""
    out = []
    for part in (spec or "").split(","):
        part = part.strip()
        if not part or "=" not in part:
            continue
        name, _, url = part.partition("=")
        name, url = name.strip(), url.strip()
        if name and url:
            out.append((name, url))
    return out


BOXES = _parse_boxes(DASH_BOXES)


def render_page():
    nav = "".join(
        f'<a class="boxtab{" on" if n == DASH_LABEL else ""}" href="{u}">{n}</a>' for n, u in BOXES
    )
    return PAGE_TEMPLATE.replace("__LABEL__", DASH_LABEL).replace("__BOXNAV__", nav)


PAGE_TEMPLATE = r"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>slotpin · __LABEL__</title>
<style>
:root{--bg:#0b0f14;--panel:#131a22;--panel2:#0f151c;--line:#1e2a36;--fg:#e6edf3;--dim:#7d8da0;--accent:#39d3bb;--warm:#ffb454;--cold:#4a7bd8;--busy:#39d3bb;--bad:#ff5f6d;--good:#39d353}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.4 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
header{display:flex;align-items:center;gap:14px;flex-wrap:wrap;padding:14px 20px;border-bottom:1px solid var(--line);background:var(--panel2);position:sticky;top:0;z-index:5}
header h1{font-size:16px;margin:0;letter-spacing:.5px}
header .cfg{color:var(--dim);font-size:12px}
.boxnav{display:flex;gap:6px}
.boxtab{padding:4px 11px;border-radius:7px;border:1px solid var(--line);color:var(--dim);text-decoration:none;font-size:12px;background:var(--panel)}
.boxtab:hover{color:var(--fg);border-color:var(--accent)}
.boxtab.on{background:var(--accent);color:#04121a;border-color:var(--accent);font-weight:600;pointer-events:none}
.dot{width:9px;height:9px;border-radius:50%;display:inline-block;margin-right:5px;vertical-align:middle}
.dot.g{background:var(--good);box-shadow:0 0 8px var(--good)}.dot.b{background:var(--bad);box-shadow:0 0 8px var(--bad)}
.spacer{flex:1}.clock{color:var(--dim);font-size:12px}
main{padding:18px 20px;max-width:1200px;margin:0 auto}
.wsel{display:flex;gap:6px;margin:2px 0 16px}
.wsel button{background:var(--panel);color:var(--dim);border:1px solid var(--line);border-radius:7px;padding:5px 12px;font:inherit;font-size:12px;cursor:pointer}
.wsel button.on{background:var(--accent);color:#04121a;border-color:var(--accent);font-weight:600}
.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin-bottom:18px}
@media(max-width:900px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
.card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:14px 16px}
.card .lbl{color:var(--dim);font-size:11px;text-transform:uppercase;letter-spacing:.7px;margin-bottom:6px}
.card .big{font-size:28px;font-weight:600}
.card .big.warn{color:var(--warm)}.card .big.crit{color:var(--bad)}
.card .sub{color:var(--dim);font-size:12px;margin-top:4px}
.grp{margin-bottom:18px}
.ghead{display:flex;align-items:baseline;gap:12px;width:100%;background:none;border:0;padding:0;margin:0 0 8px;font:inherit;color:var(--dim);text-align:left;cursor:pointer}
.ghead .gname{font-size:12px;text-transform:uppercase;letter-spacing:1px}
.ghead .gsum{display:none}
.ghead .gcar{font-size:11px}
.ghead .ghint{font-size:11px;color:var(--dim);opacity:.7}
.ghead:hover .gname,.ghead:hover .ghint{color:var(--fg)}
.grp:not(.open) .chs{display:none}
.grp .chs{margin-top:12px}
.grp .grid{margin-bottom:0}
@media(max-width:640px){
 .ghead{padding:9px 12px;margin:0 0 8px;background:var(--panel);border:1px solid var(--line);border-radius:10px}
 .grp.tog .ghead{cursor:pointer}
 .ghead .gsum{display:inline;color:var(--fg);font-size:12px;flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
 .ghead .ghint{display:none}
 .grp.tog:not(.open) .grid,.grp.tog:not(.open) .chs{display:none}
 .grp.open .ghead .gsum{display:none}
}
.card .unit{font-size:14px;color:var(--dim);font-weight:400}
h2{font-size:12px;text-transform:uppercase;letter-spacing:1px;color:var(--dim);margin:20px 0 10px}
.h2top{margin:4px 0 10px}
.slots{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin-bottom:22px}
.slot{min-width:0;background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:14px 16px;position:relative;overflow:hidden}
@media(max-width:900px){.slots{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:640px){.slots{gap:10px}.slots:not(.multi){grid-template-columns:minmax(0,1fr)}.slots.multi .slot{padding:10px 12px}}
.slot .id{font-size:12px;color:var(--dim)}.slot .st{font-size:20px;font-weight:600;margin:6px 0}
.slot.busy{border-color:var(--busy)}.slot.busy .st{color:var(--busy)}.slot.idle .st{color:var(--dim)}
.slot .meta{font-size:12px;color:var(--dim)}
.slot .ph{font-size:13px;margin:2px 0}
.slot.busy.warn{border-color:var(--warm)}.slot.busy.warn .st{color:var(--warm)}.slot.busy.warn .bar{background:var(--warm)}
.slot.busy.crit{border-color:var(--bad)}.slot.busy.crit .st{color:var(--bad)}.slot.busy.crit .bar{background:var(--bad)}
.slot .bar{position:absolute;left:0;bottom:0;height:3px;background:var(--busy)}
.slot.busy .bar{animation:pulse 1.4s ease-in-out infinite}@keyframes pulse{0%,100%{opacity:.5}50%{opacity:1}}
.chgrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px 14px;margin-top:10px}
@media(max-width:900px){.chgrid{grid-template-columns:minmax(0,1fr)}}
.chart{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:10px 14px 8px;overflow:hidden;min-width:0}
.chart .t{color:var(--dim);font-size:11px;text-transform:uppercase;letter-spacing:.6px;display:flex;justify-content:space-between;gap:8px}
.chart .cur{color:var(--fg);font-weight:600}
.chart .rd{height:34px;overflow:hidden;font-size:11.5px;line-height:17px;color:var(--fg)}
.cplot{display:flex;gap:6px;min-width:0}
.yax{flex:0 0 40px;display:flex;flex-direction:column;justify-content:space-between;height:60px;font-size:10px;line-height:1;color:var(--dim);text-align:right}
.cpm{flex:1;min-width:0}
svg.cp{display:block;width:100%;height:60px;touch-action:pan-y;cursor:crosshair}
.gl{stroke:var(--line);stroke-width:1;fill:none}
.nod{fill:var(--dim);font-size:11px}
.xt{position:relative;height:14px;font-size:10px;color:var(--dim);margin-top:2px}
.xt span{position:absolute;transform:translateX(-50%);white-space:nowrap}
.xh{display:none;stroke:var(--fg);stroke-opacity:.6;stroke-width:1;pointer-events:none}.xh.on{display:block}
.mk{fill:none;stroke-width:1.5;vector-effect:non-scaling-stroke}
.mk-down{stroke:var(--bad)}.mk-up{stroke:var(--good)}
.mk-stall{stroke:var(--warm);stroke-dasharray:5 3}.mk-stall_cleared{stroke:var(--warm);stroke-dasharray:1.5 3}
.mk-fail{stroke:var(--bad);stroke-dasharray:1.5 3;opacity:.85}
.leg{display:flex;flex-wrap:wrap;gap:4px 14px;font-size:11px;color:var(--dim);margin-top:8px}
.lm{display:inline-block;width:0;height:11px;border-left:2px solid;vertical-align:middle;margin-right:5px}
.m-down{border-color:var(--bad)}.m-up{border-color:var(--good)}
.m-stall{border-color:var(--warm);border-left-style:dashed}.m-stall_cleared{border-color:var(--warm);border-left-style:dotted}
.m-fail{border-color:var(--bad);border-left-style:dotted}
.chip{background:var(--panel);color:var(--fg);border:1px solid var(--accent);border-radius:20px;padding:3px 12px;font:inherit;font-size:12px;cursor:pointer;margin:0 0 10px}
.chip[hidden]{display:none}
table{width:100%;border-collapse:collapse;font-size:12.5px}
th,td{text-align:right;padding:6px 8px;border-bottom:1px solid var(--line);white-space:nowrap}
th{color:var(--dim);font-weight:500;text-transform:uppercase;font-size:10.5px;letter-spacing:.5px;background:var(--panel2);border-bottom:1px solid var(--line)}
th:first-child,td:first-child{text-align:left}
.pill{padding:1px 7px;border-radius:20px;font-size:11px}
.pill.warm{background:rgba(255,180,84,.16);color:var(--warm)}.pill.cold{background:rgba(74,123,216,.16);color:var(--cold)}
.mono{color:var(--dim)}.tblwrap{background:var(--panel);border:1px solid var(--line);border-radius:10px;overflow-x:auto}
.tlctl{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:0 0 10px}
.tlctl .seg{display:flex;flex-wrap:wrap;gap:6px}
.tlctl button,.tlctl select{background:var(--panel);color:var(--dim);border:1px solid var(--line);border-radius:7px;padding:5px 11px;font:inherit;font-size:12px;cursor:pointer}
.tlctl button.on{background:var(--accent);color:#04121a;border-color:var(--accent);font-weight:600}
.tlctl button:disabled{opacity:.4;cursor:default}
.tlctl .pg{display:flex;flex-wrap:wrap;gap:6px;align-items:center;color:var(--dim);font-size:12px;margin-left:auto}
th.srt{cursor:pointer;user-select:none}th.srt:hover{color:var(--fg)}
tr.trow{cursor:pointer}
tr.x-fail td{background:rgba(255,95,109,.16)}
tr.x-miss td{background:rgba(74,123,216,.14)}
tr.x-slow td{background:rgba(255,180,84,.12)}
tr.trow:hover td{filter:brightness(1.25)}
tr.det td{text-align:left;white-space:normal;background:var(--panel2)}
.dgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:6px 18px;font-size:12px}
.dgrid span{color:var(--dim)}
.ctag{display:inline-block;margin-left:6px;padding:0 5px;border:1px solid currentColor;border-radius:5px;font-size:10.5px;line-height:15px;vertical-align:middle}
.err{color:var(--bad)}.foot{color:var(--dim);font-size:11px;margin:22px 0 8px;text-align:center}
</style></head><body>
<header>
  <h1>slotpin<span style="color:var(--accent)">.</span>__LABEL__</h1>
  <span class="cfg" id="cfg">…</span>
  <nav class="boxnav">__BOXNAV__</nav>
  <span class="spacer"></span>
  <span id="health"></span><span class="clock" id="clock"></span>
</header>
<main>
  <div class="wsel" id="wsel"></div>
  <h2 class="h2top">Slots (live)</h2>
  <div class="slots" id="slots"></div>
  <div id="cards"></div>
  <h2>Recent requests · <span id="winlbl" class="mono"></span></h2>
  <button type="button" class="chip" id="rngchip" hidden></button>
  <div class="tlctl" id="tlctl">
    <div class="seg" id="tlfilt">
      <button data-f="all" class="on">all</button><button data-f="miss">missed cache</button><button data-f="slow">slow</button><button data-f="failed">failed</button>
    </div>
    <select id="tlroute" aria-label="route"><option value="">all routes</option></select>
    <div class="pg"><span id="tlpos"></span><button id="tlprev">previous</button><button id="tlnext">next</button></div>
  </div>
  <div class="tblwrap"><table><thead><tr id="tlhead">
    <th class="srt" data-s="ts">time</th><th>route</th><th>status</th><th class="srt" data-s="dur_s">duration</th><th>queue wait</th><th class="srt" data-s="ttft_s">TTFT</th>
    <th>new tokens</th><th>cached %</th><th>generated</th><th>decode t/s</th><th>draft accept %</th><th>stop reason</th>
  </tr></thead><tbody id="rows"></tbody></table></div>
  <div class="foot" id="foot"></div>
</main>
<script>
const $=id=>document.getElementById(id);
const WINS=[["Live 15m",900],["1h",3600],["24h",86400],["72h",259200],["7d",604800]];
let curWin=900;
function fmt(x,d=1){return x==null?'—':(typeof x==='number'?x.toFixed(d):x);}
function hhmmss(ep){return new Date(ep*1000).toLocaleTimeString('en-GB');}
function tclock(iso){return iso?new Date(iso).toLocaleTimeString('en-GB'):'—';}
function gb(mb){return mb==null?null:mb/1024;}

$('wsel').innerHTML=WINS.map(([l,s])=>`<button data-w="${s}" class="${s===curWin?'on':''}">${l}</button>`).join('');
$('wsel').addEventListener('click',e=>{const b=e.target.closest('button');if(!b)return;
  curWin=+b.dataset.w;[...$('wsel').children].forEach(x=>x.classList.toggle('on',+x.dataset.w===curWin));
  tlWindow();refresh();});

// ---- escaping: every API string goes through esc() before it reaches innerHTML ----
function esc(s){return String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function fnum(x,d=1){return (typeof x==='number'&&isFinite(x))?x.toFixed(d):'—';}
function fint(x){return (typeof x==='number'&&isFinite(x))?x.toLocaleString():'—';}
function fdur(s){if(s==null)return '—';s=Math.round(s);return s<60?s+'s':(s<3600?((s/60|0)+'m '+(s%60)+'s'):((s/3600|0)+'h '+((s%3600)/60|0)+'m'));}
function fstamp(ep){if(typeof ep!=='number')return '—';const d=new Date(ep*1000),t=d.toLocaleTimeString('en-GB');
  return d.toDateString()===new Date().toDateString()?t:(d.toLocaleDateString('en-GB',{day:'2-digit',month:'short'})+' '+t);}

// ---- turn list ----
const TAGS=['#39d3bb','#5aa2ff','#b18cff','#ff8fc7','#a6e22e','#ff9f43','#4dd0e1','#d8c9a3'];
function tagColor(p){let h=2166136261;for(let i=0;i<p.length;i++){h^=p.charCodeAt(i);h=Math.imul(h,16777619)>>>0;}return TAGS[h%TAGS.length];}
const PAGE=50;
const tl={range:null,off:0,filter:'all',route:'',sort:'ts',dir:-1,exp:null,rows:[],total:0,slow:null,routes:new Set(),sig:'',last:0,timer:null,busy:false,seq:0,err:false};
function rowKey(r){return [r.ts,r.route,r.prefix,r.dur_s,r.slot].join('|');}
function tlMinGap(){return curWin>3600?15000:3000;}
function tlSchedule(force){
  if(tl.timer){clearTimeout(tl.timer);tl.timer=null;}
  const gap=force?3000:tlMinGap();
  const wait=Math.max(0,tl.last+gap-Date.now());
  tl.timer=setTimeout(tlLoad,wait);
}
async function tlLoad(){
  tl.timer=null;
  if(tl.busy){tlSchedule(true);return;}
  tl.busy=true;tl.last=Date.now();const my=++tl.seq;
  const q='/api/requests?window='+curWin+'&offset='+tl.off+'&limit='+PAGE+'&filter='+tl.filter+(tl.route?('&route='+encodeURIComponent(tl.route)):'')+(tl.range?('&from='+tl.range.from+'&to='+tl.range.to):'');
  let j=null;
  try{ const r=await fetch(q,{cache:'no-store'}); if(r.ok) j=await r.json(); }catch(e){}
  tl.busy=false;
  if(my===tl.seq){
    if(j&&Array.isArray(j.rows)){
      tl.err=false;tl.rows=j.rows;tl.total=j.total||0;tl.slow=j.slow_ttft_s;
      j.rows.forEach(r=>{if(r.route)tl.routes.add(r.route);});
      if(tl.route)tl.routes.add(tl.route);
      if(!j.rows.length&&tl.total>0&&tl.off>0){ tl.off=Math.max(0,Math.floor((tl.total-1)/PAGE)*PAGE); tlRender(); tlSchedule(true); return; }
    } else tl.err=true;
    tlRender();
  }
  tlSchedule(false);
}
function setRange(from,to){tl.range={from:from,to:to};tl.off=0;tl.exp=null;tl.sig='';tlRender();tlSchedule(true);
  const el=$('tlctl');if(el&&el.scrollIntoView)el.scrollIntoView({behavior:'smooth',block:'nearest'});}
$('rngchip').addEventListener('click',()=>{tl.range=null;tl.off=0;tl.exp=null;tlRender();tlSchedule(true);});
function tlWindow(){tl.range=null;tl.off=0;tl.exp=null;tl.routes=new Set();tl.sig='';tl.rows=[];tl.total=0;tlRender();tlSchedule(true);}
function tlDetail(r){
  const it=(k,v)=>`<div><span>${esc(k)}</span> ${v}</div>`;
  const pf=(typeof r.new_tokens==='number'&&r.new_tokens>=2000)?(fnum(r.prefill_tps,0)+' t/s'):'n/a (under 2,000 tokens processed)';
  const gbv=mb=>(typeof mb==='number')?(mb/1024).toFixed(1)+' GB':'—';
  const en=(typeof r.energy_fair_wh==='number')?r.energy_fair_wh:r.energy_turn_wh;
  return '<div class="dgrid">'+
    it('prefill',esc(pf))+
    (r.cacheram_restore?it('RAM-cache restore',esc(fint(r.restored_tokens))+' tokens'):'')+
    it('restore',esc(fnum(r.upstream_restore_ms,0))+' ms · '+esc(fint(r.upstream_restore_bytes))+' B')+
    it('cache source',esc(r.upstream_cache||'—'))+
    it('miss reason',esc(r.upstream_cache_miss_reason||'—'))+
    it('snapshot',esc(fnum(r.upstream_snapshot_ms,0))+' ms · '+esc(fint(r.upstream_snapshot_bytes))+' B')+
    it('memory at start',esc(gbv(r.mem_available_start_mb)))+
    it('memory minimum',esc(gbv(r.mem_available_min_mb)))+
    it('energy',esc(fnum(en,2))+' Wh')+
    it('slot',esc(r.slot==null?'—':r.slot))+
    it('served',esc(r.served||'—'))+'</div>';
}
function tlRow(r){
  const failed=r.status==='failed';
  const miss=!failed&&r.served!=null&&r.served!=='warm';
  const slow=!failed&&tl.slow!=null&&typeof r.ttft_s==='number'&&r.ttft_s>tl.slow;
  const cls=failed?'x-fail':(miss?'x-miss':(slow?'x-slow':''));
  const k=rowKey(r);
  const tag=r.prefix?`<span class="ctag" style="color:${tagColor(String(r.prefix))}" title="conversation">${esc(String(r.prefix).slice(0,4))}</span>`:'';
  let status;
  if(failed) status='<span class="err">failed · '+esc(r.fail_reason||'unknown')+(r.upstream_status!=null?(' · '+esc(r.upstream_status)):'')+'</span>';
  else status=esc(r.served||'ok');
  const acc=(r.draft_n>0&&r.draft_n_accepted!=null)?(100*r.draft_n_accepted/r.draft_n).toFixed(0)+'%':'—';
  let h=`<tr class="trow ${cls}" data-k="${esc(k)}"><td>${esc(fstamp(r.ts))}${tag}</td><td>${esc(r.route||'—')}</td><td>${status}</td><td>${esc(fnum(r.dur_s,1))}</td><td>${esc(r.queue_wait_ms==null?'—':fnum(r.queue_wait_ms,0)+' ms')}</td><td>${esc(fnum(r.ttft_s,2))}</td><td>${esc(fint(r.new_tokens))}</td><td>${esc(fnum(r.cached_pct,0))}</td><td>${esc(fint(r.completion_n))}</td><td>${esc(fnum(r.decode_tps,1))}</td><td>${esc(acc)}</td><td class="mono">${esc(r.stop_reason||'—')}</td></tr>`;
  if(tl.exp===k) h+=`<tr class="det"><td colspan="12">${tlDetail(r)}</td></tr>`;
  return h;
}
function tlRender(){
  const w=WINS.find(x=>x[1]===curWin);
  $('winlbl').textContent=(tl.range?'time range':(w?w[0]:''))+' · newest first';
  $('rngchip').hidden=!tl.range;
  if(tl.range)$('rngchip').textContent='time range: '+fstamp(tl.range.from)+' – '+fstamp(tl.range.to)+' ✕';
  const rows=tl.rows.slice();
  const key=tl.sort, dir=tl.dir;
  rows.sort((a,b)=>{const x=a[key],y=b[key];
    if(x==null&&y==null)return 0; if(x==null)return 1; if(y==null)return -1;
    return (x-y)*dir;});
  $('rows').innerHTML=rows.map(tlRow).join('')||`<tr><td colspan="12" class="mono" style="text-align:center;padding:20px">${tl.err?'could not load requests':'no requests'}</td></tr>`;
  const a=tl.rows.length?tl.off+1:0, b=tl.off+tl.rows.length;
  $('tlpos').textContent=tl.rows.length?('showing '+a.toLocaleString()+'–'+b.toLocaleString()+' of '+tl.total.toLocaleString()):'showing 0 of '+tl.total.toLocaleString();
  $('tlprev').disabled=tl.off<=0; $('tlnext').disabled=tl.off+PAGE>=tl.total;
  [...$('tlfilt').children].forEach(x=>x.classList.toggle('on',x.dataset.f===tl.filter));
  [...$('tlhead').querySelectorAll('th.srt')].forEach(th=>{const on=th.dataset.s===tl.sort;
    th.textContent=th.textContent.replace(/\s*[▾▴]$/,'')+(on?(tl.dir<0?' ▾':' ▴'):'');});
  const sel=$('tlroute'), list=['',...[...tl.routes].sort()], sig=list.join('\u0001')+'|'+tl.route;
  if(sig!==tl.sig){ tl.sig=sig;
    sel.innerHTML=list.map(v=>`<option value="${esc(v)}"${v===tl.route?' selected':''}>${v===''?'all routes':esc(v)}</option>`).join(''); }
}
$('tlfilt').addEventListener('click',e=>{const b=e.target.closest('button');if(!b)return;tl.filter=b.dataset.f;tl.off=0;tl.exp=null;tlRender();tlSchedule(true);});
$('tlroute').addEventListener('change',e=>{tl.route=e.target.value;tl.off=0;tl.exp=null;tl.sig='';tlRender();tlSchedule(true);});
$('tlprev').addEventListener('click',()=>{tl.off=Math.max(0,tl.off-PAGE);tl.exp=null;tlSchedule(true);});
$('tlnext').addEventListener('click',()=>{tl.off+=PAGE;tl.exp=null;tlSchedule(true);});
$('tlhead').addEventListener('click',e=>{const th=e.target.closest('th.srt');if(!th)return;
  if(tl.sort===th.dataset.s)tl.dir=-tl.dir;else{tl.sort=th.dataset.s;tl.dir=-1;}tlRender();});
$('rows').addEventListener('click',e=>{const tr=e.target.closest('tr.trow');if(!tr)return;
  tl.exp=(tl.exp===tr.dataset.k)?null:tr.dataset.k;tlRender();});

// ---- charts: inline SVG, one shared time axis and one crosshair across all charts ----
const CH_H=60, PADX=5, PADT=5, PADB=5, MAXEV=40, CH_GAP=14;
const CHW={};   // chart id -> {W,tmin,tmax,rd(t),hit(vx)}
const cs={html:null,at:0,win:0,w:0,force:false,hover:0,last:null};
function hovering(){return Date.now()-cs.hover<2500;}
function finite(x){return typeof x==='number'&&isFinite(x);}
function runsOf(pts,w){const out=[];let cur=[];
  pts.forEach(p=>{if(cur.length&&p.t-cur[cur.length-1].t>w*1.5){out.push(cur);cur=[];}cur.push(p);});
  if(cur.length)out.push(cur);return out;}
function dotsSvg(list,col){
  if(!list.length)return '';
  if(list.length<=40)return list.map(d=>`<circle cx="${d[0].toFixed(1)}" cy="${d[1].toFixed(1)}" r="3" fill="${col}"/>`).join('');
  return `<path d="${list.map(d=>'M'+d[0].toFixed(1)+' '+d[1].toFixed(1)+'h0').join('')}" stroke="${col}" stroke-width="6" stroke-linecap="round" fill="none"/>`;
}
function xticks(tmin,tmax){
  const span=tmax-tmin, steps=[60,300,600,900,1800,3600,7200,10800,21600,43200,86400];
  const step=steps.find(s=>span/s<=7)||86400, tz=-new Date(tmin*1000).getTimezoneOffset()*60, out=[];
  for(let t=Math.ceil((tmin+tz)/step)*step-tz;t<=tmax;t+=step)out.push(t);
  return out.map(t=>{const d=new Date(t*1000);let lab;
    if(step>=86400)lab=d.toLocaleDateString('en-GB',{day:'2-digit',month:'short'});
    else{lab=d.toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit'});if(span>86400)lab=d.toLocaleDateString('en-GB',{weekday:'short'})+' '+lab;}
    return [t,lab];});
}
// o: {id,title,val,ymin,ymax,ymaxMin,yfmt,int,col,rows,lines,bars,stack,nofail}
//  lines: [{col,w,pts:[{t,v,v2}],band,light}]; bars: {col,w,pts}; stack: {w,pts:[{t,a,b,c}],cols}
//  rows (readout): [{lab,col,w,pts,fmt}] ; pts may carry x (already escaped html, e.g. miss reasons)
function mkChart(o,ctx){
  const {tmin,tmax,W}=ctx, pw=W-2*PADX, ph=CH_H-PADT-PADB;
  const X=t=>PADX+Math.max(0,Math.min(1,(t-tmin)/(tmax-tmin)))*pw;
  let ymin=o.ymin==null?0:o.ymin, ymax=o.ymax;
  if(ymax==null){
    let m=o.ymaxMin||0;
    (o.lines||[]).forEach(s=>s.pts.forEach(p=>{m=Math.max(m,p.v,p.v2==null?p.v:p.v2);}));
    if(o.bars)o.bars.pts.forEach(p=>{m=Math.max(m,p.v);});
    if(o.stack)o.stack.pts.forEach(p=>{m=Math.max(m,p.a+p.b+p.c);});
    ymax=o.int?Math.max(1,Math.ceil(m)):m;
  }
  if(!(ymax>ymin))ymax=ymin+1;
  const Y=v=>PADT+(1-(Math.max(ymin,Math.min(ymax,v))-ymin)/(ymax-ymin))*ph;
  const cx=(p,w)=>X(Math.min(tmax,p.t+w/2));
  let g='';
  // grid: baseline, top and the shared time ticks
  const ticks=xticks(tmin,tmax);
  g+=`<path class="gl" d="M0 ${Y(ymin).toFixed(1)}H${W}M0 ${Y(ymax).toFixed(1)}H${W}${ticks.map(([t])=>'M'+X(t).toFixed(1)+' '+PADT+'V'+(CH_H-PADB)).join('')}"/>`;
  let any=false;
  if(o.stack){
    const s=o.stack, layers=[[],[],[]], dots=[[],[],[]];
    runsOf(s.pts,s.w).forEach(run=>{
      any=true;
      const tops=run.map(p=>[p.a,p.a+p.b,p.a+p.b+p.c]);
      if(run.length<2){const p=run[0];tops[0].forEach((v,k)=>dots[k].push([cx(p,s.w),Y(v)]));return;}
      for(let k=0;k<3;k++){
        let d='M'+cx(run[0],s.w).toFixed(1)+' '+Y(tops[0][k]).toFixed(1);
        run.forEach((p,i)=>{d+='L'+cx(p,s.w).toFixed(1)+' '+Y(tops[i][k]).toFixed(1);});
        for(let i=run.length-1;i>=0;i--)d+='L'+cx(run[i],s.w).toFixed(1)+' '+Y(k?tops[i][k-1]:0).toFixed(1);
        layers[k].push(d+'Z');
      }
    });
    for(let k=0;k<3;k++){
      if(layers[k].length)g+=`<path d="${layers[k].join('')}" fill="${s.cols[k]}" fill-opacity=".55" stroke="none"/>`;
      g+=dotsSvg(dots[k],s.cols[k]);
    }
  }
  if(o.bars){
    const b=o.bars, bw=Math.max(2,Math.min(14,b.w/(tmax-tmin)*pw-1)), base=Y(ymin).toFixed(1);
    if(o.rows&&o.rows[0].pts.length)any=true;
    if(b.pts.length){
      g+=`<path d="${b.pts.map(p=>{const x=cx(p,b.w);return 'M'+(x-bw/2).toFixed(1)+' '+base+'V'+Y(p.v).toFixed(1)+'h'+bw.toFixed(1)+'V'+base+'Z';}).join('')}" fill="${b.col}" fill-opacity=".85"/>`;}
    o.barw=bw;
  }
  (o.lines||[]).forEach(s=>{
    const dots=[];let band='',line='',whisk='';
    runsOf(s.pts,s.w).forEach(run=>{
      any=true;
      if(run.length<2){const p=run[0],x=cx(p,s.w);dots.push([x,Y(p.v)]);
        if(s.band&&p.v2!=null&&p.v2>p.v)whisk+='M'+x.toFixed(1)+' '+Y(p.v2).toFixed(1)+'V'+Y(p.v).toFixed(1);return;}
      line+=run.map((p,i)=>(i?'L':'M')+cx(p,s.w).toFixed(1)+' '+Y(p.v).toFixed(1)).join('');
      if(s.band){
        band+=run.map((p,i)=>(i?'L':'M')+cx(p,s.w).toFixed(1)+' '+Y(p.v2==null?p.v:p.v2).toFixed(1)).join('')+
              [...run].reverse().map(p=>'L'+cx(p,s.w).toFixed(1)+' '+Y(p.v).toFixed(1)).join('')+'Z';
      }
    });
    const op=s.light?'.45':'1';
    if(band)g+=`<path d="${band}" fill="${s.col}" fill-opacity=".2" stroke="none"/>`;
    if(whisk)g+=`<path d="${whisk}" stroke="${s.col}" stroke-opacity=".5" stroke-width="2" fill="none"/>`;
    if(line)g+=`<path d="${line}" stroke="${s.col}" stroke-opacity="${op}" stroke-width="1.6" fill="none" stroke-linejoin="round"/>`;
    g+=dotsSvg(dots,s.col);
  });
  if(!any)g+=`<text x="${W/2}" y="${CH_H/2+4}" text-anchor="middle" class="nod">no data</text>`;
  // markers: events as lines (with a title), failure buckets as one dashed path
  const mk=ctx.mk.map(m=>({...m,x:X(m.kind==='fail'?m.t+ctx.rw/2:m.t)}));
  let fm='';
  mk.forEach(m=>{
    if(m.kind==='fail'){if(!o.nofail)fm+='M'+m.x.toFixed(1)+' 0V'+CH_H;return;}
    g+=`<line class="mk mk-${m.kind}" x1="${m.x.toFixed(1)}" x2="${m.x.toFixed(1)}" y1="0" y2="${CH_H}"><title>${esc(m.kind.replace('_',' ')+' · '+fstamp(m.t)+(m.detail?' · '+m.detail:''))}</title></line>`;
  });
  if(fm)g+=`<path class="mk mk-fail" d="${fm}"/>`;
  g+=`<line class="xh" x1="0" x2="0" y1="0" y2="${CH_H}"/>`;
  const f=o.yfmt||(v=>fnum(v,1));
  const xt=ticks.map(([t,lab])=>{const pc=100*(X(t)/W);return pc>96?'':`<span style="left:${pc.toFixed(1)}%">${esc(lab)}</span>`;}).join('');
  const rows=o.rows||(o.lines||[]).map(s=>({lab:s.lab,col:s.col,w:s.w,pts:s.pts,fmt:s.fmt}));
  const tol=Math.max(7,(o.barw||0)/2);
  CHW[o.id]={W,tmin,tmax,
    rd(t){
      const parts=[];let extra='';
      rows.forEach(r=>{const p=r.pts.find(q=>q.t<=t&&t<q.t+r.w);if(!p)return;
        parts.push(`<span style="color:${r.col}">${esc(r.lab)} ${esc(r.fmt?r.fmt(p):f(p.v))}</span>`);
        if(p.x&&!extra)extra=` <span class="mono">· ${p.x}</span>`;});
      return `<b>${esc(fstamp(t))}</b> `+(parts.join(' · ')||'<span class="mono">no data</span>')+extra;},
    hit(vx){let best=null,bd=tol;
      mk.forEach(m=>{const d=Math.abs(m.x-vx);if(d<=bd){bd=d;best=m;}});
      if(!best)return null;
      const a=Math.floor(best.t/ctx.rw)*ctx.rw;return {from:a,to:a+ctx.rw};}};
  return `<div class="chart" data-id="${esc(o.id)}"><div class="t"><span>${esc(o.title)}</span><span class="cur">${esc(o.val)}</span></div>`+
    `<div class="rd"></div><div class="cplot"><div class="yax"><span>${esc(f(ymax))}</span><span>${esc(f(ymin))}</span></div>`+
    `<div class="cpm"><svg class="cp" viewBox="0 0 ${W} ${CH_H}" preserveAspectRatio="none" role="img" aria-label="${esc(o.title)} over time">${g}</svg><div class="xt">${xt}</div></div></div></div>`;
}
const LEG=`<div class="leg"><span><i class="lm m-down"></i>down</span><span><i class="lm m-up"></i>up</span><span><i class="lm m-stall"></i>stall</span><span><i class="lm m-stall_cleared"></i>stall cleared</span><span><i class="lm m-fail"></i>failures</span><span class="mono">click a marker to filter the turn list</span></div>`;
// v: window values from the cards, as display strings
function chartBlocks(hi,v,st){
  if(!hi)return {health:'',perf:'',cache:'',system:''};
  // two columns above 900 px (matches the .chgrid media query), else one. Every chart gets the
  // same W = column width - 76 (40 px y-label gutter + 6 gap + 28 padding + 2 border), so
  // all plot areas are equally wide and start at the same offset inside their column.
  const cw=$('cards').clientWidth||640, two=!(typeof window!=='undefined'&&window.innerWidth)||window.innerWidth>900;
  const W=Math.max(120,Math.round((two?(cw-CH_GAP)/2:cw)-76));
  const gap=curWin>3600?15000:0, now=Date.now();
  if(cs.html&&!cs.force&&cs.win===curWin&&cs.w===W&&now-cs.at<gap)return cs.html;
  const tmax=hi.now, tmin=tmax-hi.window_sec, rw=hi.request_bucket_sec||60, sw=hi.sample_bucket_sec||60;
  const rs=Array.isArray(hi.request_series)?hi.request_series:[], sm=Array.isArray(hi.samples)?hi.samples:[];
  const KINDS=['down','up','stall','stall_cleared'];
  const mk=(hi.events||[]).filter(e=>KINDS.includes(e.kind)&&finite(e.t)).sort((a,b)=>a.t-b.t).slice(-MAXEV)
    .map(e=>({kind:e.kind,t:e.t,detail:e.detail}));
  rs.forEach(p=>{if(p.failed>0)mk.push({kind:'fail',t:p.t});});
  const ctx={tmin,tmax,W,rw,mk};
  const pts=(arr,f)=>arr.filter(p=>finite(f(p))).map(p=>({t:p.t,v:f(p)}));
  const band=(arr,a,b)=>arr.filter(p=>finite(p[a])).map(p=>({t:p.t,v:p[a],v2:finite(p[b])?p[b]:p[a]}));
  const bandFmt=(d,u)=>p=>fnum(p.v,d)+' '+u+(p.v2!==p.v?' (p95 '+fnum(p.v2,d)+')':'');
  const C={a:'#39d3bb',w:'#ffb454',c:'#5aa2ff',b:'#ff5f6d',g:'#39d353',d:'#7d8da0'};
  const bandLine=(id,title,val,key,d,u,col)=>mkChart({id,title,val,
    lines:[{lab:'p50',col,w:rw,pts:band(rs,key+'_p50',key+'_p95'),band:true,fmt:bandFmt(d,u)}]},ctx);
  const pctChart=(id,title,val,key,col,extraKey)=>mkChart({id,title,val,ymin:0,ymax:100,yfmt:x=>fnum(x,0)+'%',
    lines:[{lab:title,col,w:rw,pts:rs.filter(p=>finite(p[key])).map(p=>({t:p.t,v:p[key],
      x:extraKey&&p[extraKey]&&Object.keys(p[extraKey]).length?('miss reasons: '+Object.keys(p[extraKey]).sort().map(k=>esc(k)+' ×'+esc(p[extraKey][k])).join(', ')):''})),
      fmt:p=>fnum(p.v,1)+'%'}]},ctx);
  // memory: stacked free / page cache / other used, swap and GTT as lines
  const total=st&&st.system&&finite(st.system.mem_total_mb)?st.system.mem_total_mb/1024:null;
  const stk=sm.filter(p=>finite(p.mem_free_gb)&&finite(p.page_cache_gb)).map(p=>{
    const o=total==null?0:Math.max(0,total-p.mem_free_gb-p.page_cache_gb);
    return {t:p.t,a:p.mem_free_gb,b:p.page_cache_gb,c:o};});
  const gbf=x=>fnum(x,1)+' GB', sk=['#39d353','#5aa2ff','#7d8da0'];
  const memChart=mkChart({id:'memory',title:'Memory',val:v.mem,ymaxMin:total||0,yfmt:gbf,
    stack:{w:sw,pts:stk,cols:sk},
    lines:[{lab:'swap used',col:C.b,w:sw,pts:pts(sm,p=>p.swap_used_gb)},{lab:'GTT',col:C.w,w:sw,pts:pts(sm,p=>p.gtt_gb)}],
    rows:[{lab:'free',col:sk[0],w:sw,pts:stk.map(p=>({t:p.t,v:p.a})),fmt:p=>gbf(p.v)},
          {lab:'cache',col:sk[1],w:sw,pts:stk.map(p=>({t:p.t,v:p.b})),fmt:p=>gbf(p.v)},
          {lab:'other',col:sk[2],w:sw,pts:stk.map(p=>({t:p.t,v:p.c})),fmt:p=>gbf(p.v)},
          {lab:'swap',col:C.b,w:sw,pts:pts(sm,p=>p.swap_used_gb),fmt:p=>gbf(p.v)},
          {lab:'GTT',col:C.w,w:sw,pts:pts(sm,p=>p.gtt_gb),fmt:p=>gbf(p.v)}]},ctx);
  const barChart=(id,title,val,f,col,nofail)=>mkChart({id,title,val,int:true,nofail,
    bars:{w:rw,col,pts:pts(rs,f).filter(p=>p.v>0)},
    rows:[{lab:title,col,w:rw,pts:pts(rs,f),fmt:p=>fnum(p.v,0)}],yfmt:x=>fnum(x,0)},ctx);
  const tempChart=(id,title,val,mean,max)=>mkChart({id,title,val,ymin:20,ymax:100,yfmt:x=>fnum(x,0)+'°C',
    lines:[{lab:'max',col:C.w,w:sw,pts:pts(sm,p=>p[max]),light:true,fmt:p=>fnum(p.v,1)+'°C'}].concat(mean?
      [{lab:'mean',col:C.w,w:sw,pts:pts(sm,p=>p[mean]),fmt:p=>fnum(p.v,1)+'°C'}]:[])},ctx);
  const hasNvme=sm.some(p=>finite(p.nvme_temp_max_c));
  const cg=(...charts)=>LEG+'<div class="chgrid">'+charts.join('')+'</div>';
  const html={
    health:cg(barChart('failures','Failures per bucket',v.fail,p=>p.failed,C.b,true),memChart),
    perf:cg(bandLine('decode','Decode',v.decode,'decode',1,'t/s',C.a),bandLine('prefill','Prefill',v.prefill,'prefill',0,'t/s',C.c),
      bandLine('ttft','TTFT',v.ttft,'ttft',2,'s',C.w),barChart('requests','Requests per bucket',v.req,p=>p.n+p.failed,C.a),
      pctChart('draft','Draft accept %',v.draft,'draft_accept_pct',C.g)),
    cache:cg(pctChart('cachehit','Cache hit %',v.hit,'cache_hit_pct',C.c),
      pctChart('miss','Miss %',v.miss,'miss_pct',C.b,'miss_reasons'),bandLine('restore','Restore ms',v.restore,'restore_ms',0,'ms',C.w)),
    system:cg(mkChart({id:'gtt',title:'GPU memory',val:v.gtt,yfmt:gbf,lines:[{lab:'GTT',col:C.a,w:sw,pts:pts(sm,p=>p.gtt_gb),fmt:p=>gbf(p.v)}]},ctx),
      mkChart({id:'power',title:'Power',val:v.power,yfmt:x=>fnum(x,0)+' W',lines:[{lab:'power',col:C.w,w:sw,pts:pts(sm,p=>p.power_w),fmt:p=>fnum(p.v,0)+' W'}]},ctx),
      tempChart('cputemp','CPU temp',v.cpu,'cpu_temp_c','cpu_temp_max_c'),tempChart('gputemp','GPU temp',v.gpu,'gpu_temp_c','gpu_temp_max_c'),
      hasNvme?tempChart('nvmetemp','NVMe temp',v.nvme,null,'nvme_temp_max_c'):'')
  };
  cs.html=html;cs.at=now;cs.win=curWin;cs.w=W;cs.force=false;
  return html;
}
// chart pointer handling: one crosshair across every chart, click a marker to filter the turn list
function chartPos(e,svg){
  const box=svg.closest('.chart'), sp=box&&CHW[box.dataset.id]; if(!sp)return null;
  const r=svg.getBoundingClientRect(), vx=(e.clientX-r.left)/(r.width||sp.W)*sp.W;
  return {sp,vx,t:sp.tmin+Math.max(0,Math.min(1,(vx-PADX)/(sp.W-2*PADX)))*(sp.tmax-sp.tmin)};
}
// x of time t inside one chart's viewBox: from time only, never from page x, so the
// same moment lands on every chart whichever column it is in
function xhairX(sp,t){return PADX+(t-sp.tmin)/(sp.tmax-sp.tmin)*(sp.W-2*PADX);}
function xhairs(on,t){
  [...$('cards').querySelectorAll('.chart')].forEach(el=>{
    const sp=CHW[el.dataset.id], ln=el.querySelector('.xh'), rd=el.querySelector('.rd'); if(!sp||!ln)return;
    if(on){const x=xhairX(sp,t);
      ln.setAttribute('x1',x.toFixed(1));ln.setAttribute('x2',x.toFixed(1));ln.classList.add('on');rd.innerHTML=sp.rd(t);}
    else{ln.classList.remove('on');rd.innerHTML='';}});
}
$('cards').addEventListener('pointermove',e=>{const svg=e.target.closest&&e.target.closest('svg.cp');
  if(!svg){if(cs.hover){cs.hover=0;xhairs(false);}return;}
  const p=chartPos(e,svg);if(!p)return;cs.hover=Date.now();xhairs(true,p.t);});
$('cards').addEventListener('pointerleave',()=>{cs.hover=0;xhairs(false);});
$('cards').addEventListener('click',e=>{const svg=e.target.closest&&e.target.closest('svg.cp');
  if(!svg)return;const p=chartPos(e,svg);if(!p)return;const h=p.sp.hit(p.vx);if(h)setRange(h.from,h.to);});
if(typeof window!=='undefined'&&window.addEventListener){let rt=null;
  window.addEventListener('resize',()=>{clearTimeout(rt);rt=setTimeout(()=>{cs.force=true;if(cs.last)renderCards(...cs.last);},200);});}

async function refresh(){
  let st=null; try{ const r=await fetch('/api/state',{cache:'no-store'}); if(r.ok) st=await r.json(); }
  catch(e){ $('health').innerHTML='<span class="dot b"></span><span class="err">fetch failed</span>'; return; }
  if(!st||typeof st!=='object'||!st.health||typeof st.health!=='object'){
    $('health').innerHTML='<span class="dot b"></span><span class="err">state error</span>'; return; }
  const h=st.health,e=st.energy,sy=st.system;
  $('cfg').textContent=h.model_up?((h.config_id||'unknown')+(h.upstream_build?('  ·  '+h.upstream_build):'')):'no model loaded';
  $('clock').textContent=hhmmss(st.now)+(st.poll_age!=null?('  · poll '+st.poll_age+'s ago'):'');
  $('health').innerHTML=`<span class="dot ${h.proxy_up?'g':'b'}"></span>proxy `+
     `<span class="dot ${h.model_up?'g':'b'}" style="margin-left:12px"></span>model `+
     `<span style="margin-left:12px" class="mono">${h.slots_ok?(h.slots_busy+'/'+h.slots_total+' busy'):'slots n/a'}</span>`;

  // live slots + recent stream (always live)
  const slotsOk=h.slots_ok, arr=(st.slots&&st.slots.length)?st.slots:[{id:0}];
  const stT=st.health_v2&&st.health_v2.stall_threshold_s;
  $('slots').className='slots'+(arr.length>1?' multi':'');
  $('slots').innerHTML=arr.map(sl=>{const busy=slotsOk&&sl.busy;
    const pct=sl.n_ctx&&sl.n_prompt?Math.min(100,100*sl.n_prompt/sl.n_ctx):0;
    const stt=!slotsOk?'? unknown':(busy?'● processing':'○ idle');
    let sev='';
    if(busy&&stT&&sl.stall_s!=null){ sev=sl.stall_s>=stT?' crit':(sl.stall_s>=stT/2?' warn':''); }
    let ph='';
    if(busy){
      const gen=sl.phase==='generating', tok=gen?sl.n_decoded:sl.n_processed;
      ph=`<div class="ph">${gen?'generating':'prefill'} · ${esc(fdur(sl.busy_for_s))} · ${tok==null?'—':Number(tok).toLocaleString()} ${gen?'generated':'processed'}${sev?(' · no progress '+esc(fdur(sl.stall_s))):''}</div>`;
    }
    const meta=!slotsOk?'slots query stalled':(busy?((sl.n_prompt||0).toLocaleString()+' prompt · '+(sl.n_cache||0).toLocaleString()+' cached · task '+esc(sl.id_task)):('ctx '+((sl.n_ctx||0)/1000|0)+'k'));
    return `<div class="slot ${busy?'busy':'idle'}${sev}"><div class="id">SLOT ${esc(sl.id)}</div><div class="st">${stt}</div>${ph}<div class="meta">${meta}</div><div class="bar" style="width:${busy?Math.max(8,pct):0}%"></div></div>`;}).join('');

  // cards use the same measures in every window: live = state.cards (window 900),
  // sampled values (memory, GPU memory, power, temperatures) come from the history summary
  let hi=null, hiBad=false;
  try{ const r=await fetch('/api/history?window='+curWin,{cache:'no-store'}); if(r.ok) hi=await r.json(); }catch(e){}
  if(!hi||typeof hi!=='object'||!hi.summary||typeof hi.summary!=='object'){ hi=null; hiBad=true; }
  if(hiBad) return;   // bad history reply: keep the last good render
  if(hovering()) return;   // keep the crosshair readout still while a chart is being read
  const cards=curWin===900?st.cards:(hi&&hi.cards);
  if(cards){ cs.last=[cards, st, hi]; renderCards(cards, st, hi); }
}

// ---- cards: four labelled groups; one measure per card in every window ----
const gopen={};  // group -> open on narrow screens (page session; health is always open)
try{ Object.assign(gopen,JSON.parse(sessionStorage.getItem('gopen')||'{}')); }catch(e){}
function gsave(){ try{ sessionStorage.setItem('gopen',JSON.stringify(gopen)); }catch(e){} }
function grp(id,name,cards,sum,charts){
  const tog=true, open=!!gopen[id];
  return `<section class="grp${tog?' tog':''}${open?' open':''}" data-g="${id}"><button class="ghead" type="button"${tog?` aria-expanded="${open}"`:''}><span class="gname">${name}</span><span class="gsum">${sum||''}</span><span class="gcar">${open?'▾':'▸'}</span><span class="ghint">${open?'hide charts':'show charts'}</span></button><div class="grid">${cards.join('')}</div><div class="chs">${charts||''}</div></section>`;
}
$('cards').addEventListener('click',e=>{const h=e.target.closest('.ghead');if(!h)return;
  const g=h.parentNode.dataset.g;
  gopen[g]=!gopen[g];gsave();h.parentNode.classList.toggle('open',!!gopen[g]);
  h.setAttribute('aria-expanded',!!gopen[g]);h.querySelector('.gcar').textContent=gopen[g]?'▾':'▸';
  const hint=h.querySelector('.ghint');if(hint)hint.textContent=gopen[g]?'hide charts':'show charts';});
function num(x,d,unit){return (typeof x==='number'&&isFinite(x))?x.toFixed(d):'—';}
function bign(v,u){return esc(v)+(v==='—'?'':`<span class="unit">${esc(u||'')}</span>`);}
// card(): lbl, value and sub are built from esc()'d values only
function card(lbl,v,unit,sub,cls){return `<div class="card"><div class="lbl">${lbl}</div><div class="big ${cls||''}">${bign(v,unit)}</div><div class="sub">${sub||''}</div></div>`;}

function renderCards(c, st, hi){
  c=c||{};
  const hv=st.health_v2||{}, sy=st.system||{}, en=st.energy||{}, t=st.temps||{}, hs=(hi&&hi.summary)||{};
  const D=c.decode||{}, P=c.prefill||{}, T=c.ttft||{}, R=c.requests||{}, DR=c.draft||{}, CH=c.cache_hit||{},
        CS=c.cache_source||{}, RS=c.restore||{}, F=c.failures||{}, E=c.energy||{}, CC=c.concurrency;
  const thr=hv.stall_threshold_s;

  // Health
  const up=hv.upstream||'down';
  const upCard=card('Upstream',up,'',esc(hv.config_id||'—')+' · up '+esc(fdur(hv.uptime_s))+' · restart '+esc(fstamp(hv.last_restart_ts)),up==='up'?'':'crit');
  const age=hv.last_completion_age_s, busy=(hv.busy_slots||0)>0;
  let lcCls='';
  if(busy&&age!=null&&thr){ lcCls=age>=thr?'crit':(age>thr/2?'warn':''); }
  const lcCard=card('Last completion',age==null?'—':fdur(age)+' ago','',busy?('busy for '+esc(fdur(hv.busy_for_s))):'no slot busy',lcCls);
  const fCls=F.count==null?'':((F.pct!=null&&F.pct>5)?'crit':(F.count>0?'warn':''));
  const fCard=card('Failures',F.count==null?'—':String(F.count),'',F.count==null?'':esc(num(F.pct,1)+'% of turns'+(F.top_reason?(' · '+F.top_reason):'')),fCls);
  const minFree=hs.min_mem_free_mb;
  // colour from the CURRENT values on every window; the window extreme only counts on windows <= 1 h
  // (a reload or benchmark days ago must not leave Health red on the 24 h / 72 h / 7 d views)
  const short=((hi&&hi.window_sec)||900)<=3600, curFree=sy.mem_free_mb;
  const mFree=[curFree,short?minFree:null].filter(x=>typeof x==='number');
  const mLow=mFree.length?Math.min(...mFree):null;
  const swapMax=Math.max(sy.swap_used_mb||0,short?(hs.max_swap_used_mb||0):0);
  let mCls='';
  if(mLow!=null&&mLow<2048) mCls='crit'; else if(mLow!=null&&mLow<4096) mCls='warn';
  if(swapMax>1024) mCls='crit';
  const memCard=card('Memory',minFree==null?'—':num(gb(minFree),1),' GB min free',
    'now: '+esc(num(gb(sy.mem_free_mb),1))+' free · '+esc(num(gb(sy.mem_avail_mb),1))+' avail · '+esc(num(gb(sy.page_cache_mb),1))+' cache · swap '+esc(num(gb(sy.swap_used_mb),1))+' GB',mCls);

  // Performance
  const decCard=card('Decode',num(D.p50,1),' t/s','p95 '+esc(num(D.p95,1))+' · '+esc(fint(D.n))+' turns');
  const preCard=card('Prefill',num(P.p50,0),' t/s','p95 '+esc(num(P.p95,0))+' · '+esc(fint(P.n))+' turns');
  const ttCard=card('TTFT',num(T.p50,2),' s','p95 '+esc(num(T.p95,2))+' s · queue wait '+esc(num(T.queue_wait_ms_p50,0))+' ms');
  const rqCard=card('Requests',num(R.per_hour,1),' /h',esc(fint(R.count))+' in window');
  const drCard=card('Draft accept ('+esc(DR.drafter||'Draft')+')',num(DR.pct,0),'%',esc(fint(DR.accepted))+' / '+esc(fint(DR.proposed)));
  const ccCard=CC?card('Concurrency',num(CC.mean,1),' slots','peak '+esc(num(CC.peak,1))+' · ≈ agg '+esc(num(CC.agg_decode_tps,1))+' t/s'):'';

  // Cache
  const chCard=card('Cache hit',num(CH.pct,0),'%',esc(fint(CH.cached_tokens))+' / '+esc(fint(CH.prompt_tokens))+' prompt tokens');
  let csSub='';
  if(CS.kind==='gufo') csSub='memory '+esc(fint(CS.memory))+' · disk '+esc(fint(CS.disk))+' · miss '+esc(fint(CS.miss))+(CS.top_miss_reason?(' ('+esc(CS.top_miss_reason)+')'):'');
  else if(CS.kind) csSub='warm '+esc(fint(CS.warm))+' · cold '+esc(fint(CS.cold))+(CS.top_miss_reason?(' ('+esc(CS.top_miss_reason)+')'):'');
  const csCard=card('Cache source',num(CS.miss_pct,0),CS.kind==='llamacpp'?'% cold':'% missed',csSub);
  const rsCard=RS.source==='proxy'
    ?card('Restore',num(RS.p50,0),' ms','p95 '+esc(num(RS.p95,0))+' · '+esc(fint(RS.n))+' restores · '+esc(fint(RS.misses))+' misses · save ~'+
        esc(RS.save_ms_p50==null?'—':num(RS.save_ms_p50,0)+' ms')+(RS.snapshot_bytes_p50==null?'':' · snapshot ~'+esc(fbytes(RS.snapshot_bytes_p50)))+
        '<br>RAM cache: '+esc(fint(RS.cacheram_restores))+' turns, '+esc(fint(RS.cacheram_tokens))+' tokens')
    :card('Restore',num(RS.p50,0),' ms','p95 '+esc(num(RS.p95,0))+' · snapshot '+esc(num(RS.snapshot_ms_p50,0))+' ms, '+esc(RS.snapshot_bytes_p50==null?'—':fbytes(RS.snapshot_bytes_p50)));

  // System
  const total=gb(sy.mem_total_mb);
  const gttCard=card('GPU memory',hs.max_gtt_used_mb==null?'—':num(gb(hs.max_gtt_used_mb),1),' GB peak','now '+esc(num(gb(sy.gtt_used_mb),1))+' · of '+esc(num(total,0))+' GB');
  const eStale=!!en.stale&&!(typeof en.age_s==='number'&&en.age_s<=30);
  const ek=hs.energy_kwh, eTxt=(typeof ek!=='number'||!isFinite(ek))?'—':(ek<1?(ek*1000).toFixed(0)+' Wh':ek.toFixed(2)+' kWh');
  const powCard=card('Power',num(hs.avg_power_w,0),' W','now '+(eStale?'—':esc(num(en.power_w,0))+' W')+' · '+esc(eTxt)+' in window');
  const whCard=card('Energy / request',num(E.wh_per_request,2),' Wh',esc(num(E.wh_per_1k_tokens,2))+' Wh per 1k generated tokens');
  const tCls1=v=>v==null?'':(v>=t.crit_c?'crit':(v>=t.warn_c?'warn':''));
  const tCls=(now,mx)=>{const a=tCls1(now),b=short?tCls1(mx):'';return (a==='crit'||b==='crit')?'crit':(a||b);};
  const tcard=(lbl,mx,av,now)=>card(lbl,num(mx,1),'°C','mean '+esc(num(av,1))+' · now '+esc(num(now,1))+'°C',tCls(now,mx));
  const cpuCard=tcard('CPU temp',hs.max_cpu_temp_c,hs.avg_cpu_temp_c,t.cpu_c);
  const gpuCard=tcard('GPU temp',hs.max_gpu_temp_c,hs.avg_gpu_temp_c,t.gpu_c);
  const nvmeCard=(t.nvme_c!=null||hs.max_nvme_temp_c!=null)?card('NVMe temp',num(hs.max_nvme_temp_c,1),'°C','now '+esc(num(t.nvme_c,1))+'°C',tCls(t.nvme_c,hs.max_nvme_temp_c)):'';

  const sum=(...xs)=>xs.join(' · ');
  // chart titles carry the card's window value (same measure as the card)
  const wv=(x,d,u)=>(typeof x==='number'&&isFinite(x))?x.toFixed(d)+u:'—';
  const ch=chartBlocks(hi,{fail:F.count==null?'—':String(F.count),mem:wv(gb(minFree),1,' GB'),
    decode:wv(D.p50,1,' t/s'),prefill:wv(P.p50,0,' t/s'),ttft:wv(T.p50,2,' s'),req:wv(R.per_hour,1,' /h'),
    draft:wv(DR.pct,0,'%'),hit:wv(CH.pct,0,'%'),miss:wv(CS.miss_pct,0,'%'),restore:wv(RS.p50,0,' ms'),
    gtt:wv(gb(hs.max_gtt_used_mb),1,' GB'),power:wv(hs.avg_power_w,0,' W'),cpu:wv(hs.max_cpu_temp_c,1,'°C'),
    gpu:wv(hs.max_gpu_temp_c,1,'°C'),nvme:wv(hs.max_nvme_temp_c,1,'°C')},st);
  $('cards').innerHTML=
    grp('perf','Performance',[decCard,preCard,ttCard,rqCard,drCard,ccCard].filter(Boolean),
        sum('decode '+esc(num(D.p50,1))+' t/s','TTFT '+esc(num(T.p50,2))+' s',''+esc(num(R.per_hour,1))+' /h'),ch.perf)+
    grp('cache','Cache',[chCard,csCard,rsCard],
        sum('hit '+esc(num(CH.pct,0))+'%',(CS.kind==='llamacpp'?'cold ':'missed ')+esc(num(CS.miss_pct,0))+'%','restore '+esc(num(RS.p50,0))+' ms'),ch.cache)+
    grp('health','Health',[upCard,lcCard,fCard,memCard],
        sum((up==='up'&&!lcCls&&!fCls&&!mCls?'':'⚠ ')+esc(up),(F.count==null?'—':String(F.count))+' failed',esc(num(gb(minFree),1))+' GB free'),ch.health)+
    grp('system','System',[gttCard,powCard,whCard,cpuCard,gpuCard,nvmeCard].filter(Boolean),
        sum('GPU mem '+esc(num(gb(hs.max_gtt_used_mb),1))+' GB',esc(num(hs.avg_power_w,0))+' W','CPU '+esc(num(hs.max_cpu_temp_c,0))+'°C'),ch.system);
}
function fbytes(b){if(typeof b!=='number')return '—';const u=['B','KB','MB','GB'];let i=0;while(b>=1024&&i<3){b/=1024;i++;}return b.toFixed(i?1:0)+' '+u[i];}

refresh(); setInterval(refresh,3000); tlRender(); tlSchedule(true);
</script></body></html>"""


def main():
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    except Exception:
        pass
    _load_from_db()
    backfill()
    backfill_requests()
    _flush_requests()
    threading.Thread(target=collector, daemon=True).start()
    srv = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    print(f"slotpin-dash on http://{LISTEN_HOST}:{LISTEN_PORT} (db={DB_PATH})")
    srv.serve_forever()


if __name__ == "__main__":
    main()
