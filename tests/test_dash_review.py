"""Pre-release review fixes: inherited restart events, state errors, request keys,
window-extreme colouring, cache label, stale energy, power units."""

from __future__ import annotations

import json
import re
import shutil
import sqlite3
import subprocess
import time
from datetime import UTC, datetime

import pytest

import slotpin.dash as dash

NOW = 1_800_000_000.0
needs_node = pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")


def _iso(epoch):
    return datetime.fromtimestamp(epoch, UTC).isoformat(timespec="seconds")


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(dash, "DB_PATH", str(tmp_path / "rollup.db"))
    monkeypatch.setattr(dash, "METRICS_FILE", str(tmp_path / "metrics.jsonl"))
    monkeypatch.setattr(dash, "_offset", 0)
    monkeypatch.setattr(dash, "_restart", dash.RestartDetector())
    monkeypatch.setattr(dash, "_live", dict(dash._live))
    dash._buckets.clear()
    dash._samples.clear()
    dash._events.clear()
    dash._req_pending.clear()
    monkeypatch.setattr(time, "time", lambda: NOW)
    yield tmp_path
    dash._events.clear()
    dash._buckets.clear()
    dash._samples.clear()


# ---------------- 1. inherited up/down events ----------------
def test_old_dashboard_up_down_events_are_dropped_on_first_start(env, monkeypatch):
    con = sqlite3.connect(dash.DB_PATH)
    con.execute("CREATE TABLE events(ts REAL, kind TEXT)")  # old schema: no detail column
    con.executemany(
        "INSERT INTO events VALUES(?,?)",
        [(NOW - 9 * 86400, "down"), (NOW - 8 * 86400, "down"), (NOW - 5 * 86400, "up")],
    )
    con.execute("INSERT INTO events VALUES(?,?)", (NOW - 3600, "stall"))
    con.commit()
    con.close()
    dash._load_from_db()
    assert [k for _t, k, _d in dash._events] == ["stall"]  # other kinds are kept
    dash._live.update(slots=[], slots_ok=True, poll_ts=NOW)
    h = dash.build_state()["health_v2"]
    assert h["last_restart_ts"] is None and h["uptime_s"] is None
    dash._flush_db()
    rows = sqlite3.connect(dash.DB_PATH).execute("SELECT kind FROM events").fetchall()
    assert rows == [("stall",)]
    # second start: the state row is present, the new detector's own events survive
    with dash._lock:
        dash._restart_step(NOW, True, {}, "m1")
        dash._restart_step(NOW + 3, False, {}, "m1")
        dash._restart_step(NOW + 6, True, {}, "m1")
    dash._flush_db()
    dash._events.clear()
    dash._restart = dash.RestartDetector()
    dash._load_from_db()
    assert [k for _t, k, _d in dash._events] == ["stall", "down", "up"]


# ---------------- 3. requests key ----------------
def _turn(ts, rid="same", **kw):
    d = {"ts": _iso(ts), "route": "r", "served": "warm", "request_id": rid, "ok": True}
    d.update(kw)
    return d


def test_request_key_includes_epoch(env):
    a, b = _turn(NOW - 100), _turn(NOW - 50)
    ra, rb = dash._request_row(a, json.dumps(a)), dash._request_row(b, json.dumps(b))
    assert ra[0] == f"ok:same:{int(NOW - 100)}" and ra[0] != rb[0]
    f = {"ts": _iso(NOW - 10), "event": "turn_failed", "reason": "x", "request_id": "same"}
    assert dash._request_row(f, "")[0] == f"failed:same:{int(NOW - 10)}"
    with open(dash.METRICS_FILE, "w") as fh:
        for r in (a, b, a):  # a ingested twice
            fh.write(json.dumps(r) + "\n")
    with dash._lock:
        dash._read_new_metrics()
    dash._flush_requests()
    dash._flush_requests()
    n = dash._db().execute("SELECT COUNT(*) FROM requests").fetchone()[0]
    assert n == 2
    dash.backfill_requests()  # re-ingesting the same file stays at two
    assert dash._db().execute("SELECT COUNT(*) FROM requests").fetchone()[0] == 2


# ---------------- 6. stale energy ----------------
def test_failed_energy_poll_records_no_sample_and_marks_state_stale(env, monkeypatch):
    monkeypatch.setattr(dash, "ENERGY_URL", "http://plug.example.com/metrics")
    good = "power 21\ntotal_energy 5.0\n"
    box = {"txt": good}

    class _R:
        def read(self):
            if box["txt"] is None:
                raise OSError("feed down")
            return box["txt"].encode()

    monkeypatch.setattr(dash.urllib.request, "urlopen", lambda *a, **k: _R())
    monkeypatch.setattr(dash, "_parse_energy", lambda t: {"power": 21.0, "total_energy": 5.0})
    monkeypatch.setattr(dash, "_get_json", lambda *a, **k: [])
    monkeypatch.setattr(dash, "read_meminfo", lambda: None)
    monkeypatch.setattr(dash, "read_temps", lambda: {})
    monkeypatch.setattr(dash, "_tcp_up", lambda *a: True)
    monkeypatch.setattr(dash, "_read_upstream_counters", lambda: {})
    monkeypatch.setattr(dash, "read_config_id", lambda: "x")
    monkeypatch.setattr(dash, "_last_build", "b")

    class Stop(Exception):
        pass

    def stop(_s):
        raise Stop

    monkeypatch.setattr(dash.time, "sleep", stop)

    def cycle(now):
        monkeypatch.setattr(time, "time", lambda: now)
        with pytest.raises(Stop):
            dash.collector()

    cycle(NOW)
    n = sum(b["pow_n"] for b in dash._samples.values())
    assert n == 1 and dash._live["energy_ok"] is True
    box["txt"] = None
    cycle(NOW + 3)
    assert sum(b["pow_n"] for b in dash._samples.values()) == 1  # nothing folded
    assert sum(b["pow_n"] for b in dash._buckets.values()) == 1
    monkeypatch.setattr(time, "time", lambda: NOW + 40)
    st = dash.build_state()["energy"]
    assert st["stale"] is True and st["age_s"] == 40.0 and st["power_w"] == 21.0
    box["txt"] = good
    cycle(NOW + 50)
    monkeypatch.setattr(time, "time", lambda: NOW + 51)
    assert dash.build_state()["energy"]["stale"] is False


# ---------------- page tests under Node ----------------
HARNESS = r"""
const vm=require('vm'),fs=require('fs');
const script=fs.readFileSync(process.argv[2],'utf8'), P=JSON.parse(fs.readFileSync(process.argv[3],'utf8'));
const els={};
function el(id){return els[id]||(els[id]={id,innerHTML:'',textContent:'',style:{},hidden:false,dataset:{},children:[],disabled:false,
  value:'',clientWidth:640,classList:{add(){},remove(){},toggle(){}},addEventListener(){},setAttribute(){},
  querySelector(){return null},querySelectorAll(){return []},closest(){return null},scrollIntoView(){}});}
function resp(m,u){ if(m==='throw') throw new Error('net');
  let b=m.body; if(b&&u&&u.startsWith('/api/history')&&b.summary) b=Object.assign({},b,{window_sec:+u.split('window=')[1]});
  return {ok:m.ok!==false,status:m.status||200,json:async()=>b}; }
const ctx={document:{getElementById:el},window:{addEventListener(){},innerWidth:1400},sessionStorage:{getItem(){return null},setItem(){}},
  fetch:async u=>resp(u.startsWith('/api/state')?P.fetch.state:(u.startsWith('/api/history')?P.fetch.history:P.fetch.requests),u),
  setInterval(){},setTimeout(){return 1},clearTimeout(){},console,Date,Math,JSON,out:s=>process.stdout.write(s)};
vm.createContext(ctx);
const tail=`
;(async()=>{ curWin=${P.win};
  const res={};
  for(const w of ${JSON.stringify(P.wins||[])}){ curWin=w; await refresh(); res['w'+w]=$('cards').innerHTML; }
  await refresh(); res.health=$('health').innerHTML; res.cards=$('cards').innerHTML;
  if(${JSON.stringify(P.detail||null)}) res.detail=tlDetail(${JSON.stringify(P.detail||null)});
  out(JSON.stringify(res)); })().catch(e=>{console.error(String(e&&e.stack||e));process.exit(1);});`;
vm.runInContext(script+tail,ctx);
"""


def _script():
    m = re.search(r"<script>(.*)</script>", dash.render_page(), re.S)
    assert m
    return m.group(1)


def _state(**over):
    st = {
        "now": NOW,
        "health": {"model_up": True, "proxy_up": True, "slots_ok": True, "slots_busy": 0,
                   "slots_total": 1, "config_id": "x"},
        "health_v2": {"upstream": "up", "config_id": "x", "stall_threshold_s": 300},
        "system": {"mem_total_mb": 131072, "mem_free_mb": 20480, "swap_used_mb": 0},
        "energy": {"power_w": 21.0},
        "temps": {"cpu_c": 50, "gpu_c": 55, "nvme_c": 40, "warn_c": 80, "crit_c": 90},
        "slots": [],
        "cards": {},
    }
    st.update(over)
    return st


def _hist(**summary):
    return {"now": NOW, "window_sec": 900, "request_bucket_sec": 60, "sample_bucket_sec": 60,
            "cards": {}, "summary": summary, "samples": [], "request_series": [], "events": []}


def _run(tmp_path, win, state=None, history=None, fetch=None, wins=(), detail=None):
    if history is None:
        history = _hist()
    f = {"state": {"body": state if state is not None else _state()},
         "history": {"body": history},
         "requests": {"body": {"rows": [], "total": 0}}}
    f.update(fetch or {})
    (tmp_path / "h.js").write_text(HARNESS)
    (tmp_path / "page.js").write_text(_script())
    (tmp_path / "p.json").write_text(json.dumps({"win": win, "fetch": f, "wins": list(wins), "detail": detail}))
    r = subprocess.run(["node", str(tmp_path / "h.js"), str(tmp_path / "page.js"), str(tmp_path / "p.json")],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


def _card(html, label):
    m = re.search(r'<div class="card"><div class="lbl">' + re.escape(label) + r'</div><div class="big ([^"]*)">(.*?)</div><div class="sub">(.*?)</div>', html)
    assert m, label
    return {"cls": m.group(1).strip(), "big": m.group(2), "sub": m.group(3)}


@needs_node
@pytest.mark.parametrize(
    "fetch",
    [
        {"state": {"body": {"error": "boom"}}},
        {"state": {"ok": False, "status": 500, "body": {"error": "boom"}}},
        {"state": {"ok": False, "status": 500, "body": None}},
        {"state": {"body": None}},
    ],
)
def test_state_error_body_shows_indicator_without_throwing(tmp_path, fetch):
    res = _run(tmp_path, 900, fetch=fetch)
    assert "state error" in res["health"]


@needs_node
def test_bad_history_and_requests_keep_last_good_render(tmp_path):
    ok = _run(tmp_path, 900, wins=(900,))
    assert "Memory" in ok["w900"]
    for bad in ({"ok": False, "status": 500, "body": {"error": "x"}}, {"body": {"error": "x"}}, "throw"):
        res = _run(tmp_path, 3600, fetch={"history": bad, "requests": bad})
        assert "state error" not in res["health"] and "fetch failed" not in res["health"]
        assert res["cards"] == ""  # nothing drawn from a bad reply, nothing thrown


@needs_node
def test_memory_card_colours_from_current_values_except_on_short_windows(tmp_path):
    hist = _hist(min_mem_free_mb=600, max_swap_used_mb=0)  # 0.6 GB min free in window
    res = _run(tmp_path, 86400, history=hist, wins=(86400, 900, 3600))
    assert _card(res["w86400"], "Memory")["cls"] == ""
    assert "⚠" not in res["w86400"]
    assert _card(res["w900"], "Memory")["cls"] == "crit"
    assert _card(res["w3600"], "Memory")["cls"] == "crit"
    assert "⚠ up" in res["w900"]
    assert "0.6" in _card(res["w86400"], "Memory")["big"]  # headline stays the window minimum
    low = _state(system={"mem_total_mb": 131072, "mem_free_mb": 1024, "swap_used_mb": 0})
    for w in (86400, 259200, 604800):
        res = _run(tmp_path, w, state=low, history=_hist(min_mem_free_mb=30000), wins=(w,))
        assert _card(res[f"w{w}"], "Memory")["cls"] == "crit"
    sw = _state(system={"mem_total_mb": 131072, "mem_free_mb": 20480, "swap_used_mb": 2048})
    res = _run(tmp_path, 86400, state=sw, history=_hist(min_mem_free_mb=30000, max_swap_used_mb=0), wins=(86400,))
    assert _card(res["w86400"], "Memory")["cls"] == "crit"


@needs_node
def test_temperature_cards_colour_from_current_values_except_on_short_windows(tmp_path):
    hist = _hist(max_cpu_temp_c=95, max_gpu_temp_c=95, max_nvme_temp_c=95)
    res = _run(tmp_path, 86400, history=hist, wins=(86400, 3600))
    for lbl in ("CPU temp", "GPU temp", "NVMe temp"):
        assert _card(res["w86400"], lbl)["cls"] == ""
        assert _card(res["w3600"], lbl)["cls"] == "crit"
        assert "95.0" in _card(res["w86400"], lbl)["big"]
    hot = _state(temps={"cpu_c": 91, "gpu_c": 85, "nvme_c": 95, "warn_c": 80, "crit_c": 90})
    res = _run(tmp_path, 604800, state=hot, history=_hist(max_cpu_temp_c=60, max_gpu_temp_c=60, max_nvme_temp_c=60), wins=(604800,))
    html = res["w604800"]
    assert _card(html, "CPU temp")["cls"] == "crit"
    assert _card(html, "GPU temp")["cls"] == "warn"
    assert _card(html, "NVMe temp")["cls"] == "crit"


@needs_node
@pytest.mark.parametrize(
    ("kind", "big_unit", "sub"),
    [("llamacpp", "% cold", "warm 7 · cold 3"), ("gufo", "% missed", "memory 5 · disk 2 · miss 3")],
)
def test_cache_source_label_per_kind(tmp_path, kind, big_unit, sub):
    cs = {"kind": kind, "miss_pct": 30.0, "warm": 7, "cold": 3, "memory": 5, "disk": 2, "miss": 3}
    st = _state(cards={"cache_source": cs})
    res = _run(tmp_path, 900, state=st, wins=(900,))
    c = _card(res["w900"], "Cache source")
    assert big_unit in c["big"] and c["sub"] == sub
    assert ("cold 30%" in res["w900"]) == (kind == "llamacpp")


@needs_node
def test_power_card_units_stale_and_concurrency_estimate(tmp_path):
    def sub(kwh, en=None, cc=None, cards=None):
        st = _state(energy=en or {"power_w": 21.0}, cards=cards or {})
        res = _run(tmp_path, 86400, state=st, history=_hist(avg_power_w=20, energy_kwh=kwh), wins=(86400,))
        return _card(res["w86400"], "Power")["sub"]

    assert sub(0.0064) == "now 21 W · 6 Wh in window"
    assert sub(0.9996) == "now 21 W · 1000 Wh in window"
    assert sub(1.0) == "now 21 W · 1.00 kWh in window"
    assert sub(12.345) == "now 21 W · 12.35 kWh in window"
    assert sub(None).endswith("— in window")
    assert sub(0.0064, {"power_w": 21.0, "stale": True, "age_s": 10}).startswith("now 21 W")
    assert sub(0.0064, {"power_w": 21.0, "stale": True, "age_s": 31}).startswith("now — ·")
    assert sub(0.0064, {"power_w": 21.0, "stale": True, "age_s": None}).startswith("now — ·")
    assert sub(0.0064, {"power_w": 21.0, "stale": False, "age_s": 99}).startswith("now 21 W")
    cc = {"mean": 2.0, "peak": 3, "agg_decode_tps": 24.3}
    st = _state(cards={"concurrency": cc})
    res = _run(tmp_path, 900, state=st, wins=(900,))
    assert "≈ agg 24.3 t/s" in _card(res["w900"], "Concurrency")["sub"]


PROXY_RESTORE = {"source": "proxy", "p50": 670.0, "p95": 1860.0, "n": 64, "misses": 20, "save_ms_p50": 910.0,
                 "saves": 991, "save_failures": 7, "snapshot_ms_p50": 910.0, "snapshot_bytes_p50": 3 * 1048576,
                 "cacheram_restores": 666, "cacheram_tokens": 1234567}


@needs_node
def test_restore_card_proxy_and_upstream_shapes(tmp_path):
    cs = {"kind": "llamacpp", "miss_pct": 30.0, "warm": 7, "cold": 3}
    res = _run(tmp_path, 900, state=_state(cards={"cache_source": cs, "restore": PROXY_RESTORE}), wins=(900,))
    c = _card(res["w900"], "Restore")
    assert c["big"].startswith("670") and "ms" in c["big"]
    assert c["sub"] == ("p95 1,860 · 64 restores · 20 misses · save ~910 ms · snapshot ~3.0 MB"
                        "<br>RAM cache: 666 turns, 1,234,567 tokens".replace("1,860", "1860"))
    no_bytes = dict(PROXY_RESTORE, snapshot_bytes_p50=None)
    res = _run(tmp_path, 900, state=_state(cards={"restore": no_bytes}), wins=(900,))
    assert "snapshot" not in _card(res["w900"], "Restore")["sub"]
    up = {"source": "upstream", "p50": 12.0, "p95": 30.0, "n": 5, "snapshot_ms_p50": 40.0,
          "snapshot_bytes_p50": 2 * 1048576}
    res = _run(tmp_path, 900, state=_state(cards={"restore": up}), wins=(900,))
    assert _card(res["w900"], "Restore")["sub"] == "p95 30 · snapshot 40 ms, 2.0 MB"
    res = _run(tmp_path, 900, state=_state(cards={"restore": {"p50": 12.0, "p95": 30.0}}), wins=(900,))
    assert "RAM cache" not in res["w900"]  # no source field: the existing shape


@needs_node
def test_turn_detail_shows_ram_cache_restore(tmp_path):
    row = {"served": "warm", "cacheram_restore": 1, "restored_tokens": 4321}
    assert "RAM-cache restore</span> 4,321 tokens" in _run(tmp_path, 900, detail=row)["detail"]
    for r in ({"served": "warm", "cacheram_restore": 0, "restored_tokens": 0}, {"served": "warm"}):
        assert "RAM-cache" not in _run(tmp_path, 900, detail=r)["detail"]
