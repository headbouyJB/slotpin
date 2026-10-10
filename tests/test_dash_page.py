"""Tests for the dashboard page (turn list, slot tiles) and its API inputs."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import time
from datetime import UTC, datetime

import pytest

import slotpin.dash as dash

NOW = 1_800_000_000.0


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(dash, "DB_PATH", str(tmp_path / "rollup.db"))
    monkeypatch.setattr(dash, "METRICS_FILE", str(tmp_path / "metrics.jsonl"))
    monkeypatch.setattr(dash, "_offset", 0)
    monkeypatch.setattr(dash, "_stall", dash.StallTracker())
    monkeypatch.setattr(dash, "_stall_info", None)
    monkeypatch.setattr(dash, "_last_completion_ts", None)
    monkeypatch.setattr(dash, "_restart", dash.RestartDetector())
    monkeypatch.setattr(dash, "_live", dict(dash._live))
    dash._buckets.clear()
    dash._events.clear()
    dash._req_pending.clear()
    monkeypatch.setattr(time, "time", lambda: NOW)
    yield tmp_path
    dash._events.clear()
    dash._buckets.clear()


def _iso(epoch):
    return datetime.fromtimestamp(epoch, UTC).isoformat(timespec="seconds")


def _turn(age_s, **kw):
    d = {
        "ts": _iso(NOW - age_s),
        "route": "interactive",
        "slot": 0,
        "prefix": "abcd1234",
        "prompt_n": 1000,
        "cache_n": 900,
        "completion_n": 50,
        "dur_s": 4.0,
        "ttft_s": 1.0,
        "served": "warm",
        "ok": True,
    }
    d.update(kw)
    return d


def _script(html):
    m = re.search(r"<script>(.*)</script>", html, re.S)
    assert m
    return m.group(1)


def test_page_has_turn_list_headers_and_controls():
    html = dash.render_page()
    for h in (
        "time",
        "route",
        "status",
        "duration",
        "queue wait",
        "TTFT",
        "new tokens",
        "cached %",
        "generated",
        "decode t/s",
        "draft accept %",
        "stop reason",
    ):
        assert f">{h}</th>" in html
    for ctl in ('id="tlfilt"', 'id="tlroute"', 'id="tlprev"', 'id="tlnext"', 'id="tlpos"'):
        assert ctl in html
    for f in ("all", "missed cache", "slow", "failed"):
        assert f">{f}</button>" in html
    assert "all routes" in html
    assert "tr.warm" not in html


def test_page_script_uses_requests_api_and_stays_self_contained(monkeypatch):
    monkeypatch.setattr(dash, "BOXES", [])  # box nav links are configured, not page assets
    html = dash.render_page()
    js = _script(html)
    assert "/api/requests?window=" in js
    assert "<script src" not in html.lower()
    assert not re.search(r"https?://", html)
    assert "@import" not in html


def test_page_escapes_every_string_field():
    js = _script(dash.render_page())
    assert "function esc(" in js
    row = re.search(r"function tlRow\(r\)\{(.*?)\n\}\n", js, re.S).group(1)
    for field in ("r.route", "r.stop_reason", "r.fail_reason", "r.served", "r.upstream_status"):
        assert re.search(r"esc\([^;]*?" + re.escape(field), row), field
    det = re.search(r"function tlDetail\(r\)\{(.*?)\n\}\n", js, re.S).group(1)
    for field in ("r.upstream_cache", "r.upstream_cache_miss_reason"):
        assert re.search(r"esc\(" + re.escape(field), det), field
    # no raw API string is interpolated into a template
    for field in ("route", "stop_reason", "fail_reason", "served", "prefix"):
        assert "${r." + field not in row


def test_slot_tiles_use_stall_threshold():
    js = _script(dash.render_page())
    assert "stall_threshold_s" in js and "stT/2" in js


def test_state_exposes_stall_threshold(env, monkeypatch):
    monkeypatch.setattr(dash, "STALL_S", 120.0)
    assert dash.build_state()["health_v2"]["stall_threshold_s"] == 120.0


def test_requests_api_returns_markup_as_data(env):
    evil = "<script>alert(1)</script>"
    recs = [
        _turn(10, route=evil, stop_reason=evil, upstream_cache_miss_reason=evil, request_id="a"),
        {
            "ts": _iso(NOW - 5),
            "event": "turn_failed",
            "reason": evil,
            "route": evil,
            "request_id": "b",
        },
    ]
    (env / "metrics.jsonl").write_text("".join(json.dumps(r) + "\n" for r in recs))
    with dash._lock:
        dash._read_new_metrics()
    dash._flush_requests()
    code, body = dash.query_requests({"window": ["3600"]}, now=NOW)
    assert code == 200 and body["total"] == 2
    assert {r["route"] for r in body["rows"]} == {evil}
    by = {r["status"]: r for r in body["rows"]}
    assert by["ok"]["stop_reason"] == evil and by["ok"]["upstream_cache_miss_reason"] == evil
    assert by["failed"]["fail_reason"] == evil
    assert "slow_ttft_s" in body


GROUPS = ("Performance", "Cache", "Health", "System")
CARDS = (
    "Upstream",
    "Last completion",
    "Failures",
    "Memory",
    "Decode",
    "Prefill",
    "TTFT",
    "Requests",
    "Draft accept",
    "Concurrency",
    "Cache hit",
    "Cache source",
    "Restore",
    "GPU memory",
    "Power",
    "Energy / request",
    "CPU temp",
    "GPU temp",
    "NVMe temp",
)


def test_page_has_four_groups_in_order_and_every_card():
    js = _script(dash.render_page())
    pos = [js.index(f"'{g}'") for g in GROUPS]
    assert pos == sorted(pos)
    for c in CARDS:
        assert f"'{c}" in js, c
    # charts live inside their group now, not in a separate trends block
    html = dash.render_page()
    assert 'id="trends"' not in html
    # live slots come first, above the Health group (which is rendered into #cards)
    assert html.index('id="wsel"') < html.index('id="slots"') < html.index('id="cards"')
    assert html.index('id="cards"') < html.index('id="rngchip"')


def test_card_labels_do_not_depend_on_the_window():
    html = dash.render_page()
    js = _script(html)
    # the card code must not carry any window-specific label
    cards_only = html
    for old in ("TTFT (avg)", "TTFT p95", "Free RAM · low-water", "Decode · agg", "Decode · task"):
        assert old not in cards_only, old
    body = js[js.index("function renderCards") : js.index("function fbytes")]
    assert "curWin" not in body.replace("hi&&curWin!==900", "")  # only the chart toggle


def test_narrow_screen_collapse_and_escaping():
    html = dash.render_page()
    assert "@media(max-width:640px)" in html and "sessionStorage" in html
    js = _script(html)
    body = js[js.index("function renderCards") : js.index("function fbytes")]
    for field in ("hv.config_id", "CS.top_miss_reason", "F.top_reason", "DR.drafter"):
        assert re.search(r"esc\([^;]*?" + re.escape(field), body), field


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_page_script_parses(tmp_path):
    f = tmp_path / "page.js"
    f.write_text(_script(dash.render_page()))
    r = subprocess.run(["node", "--check", str(f)], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


# ---------------- charts ----------------
CHART_TITLES = {
    "health": ("Failures per bucket", "Memory"),
    "perf": ("Decode", "Prefill", "TTFT", "Requests per bucket", "Draft accept %"),
    "cache": ("Cache hit %", "Miss %", "Restore ms"),
    "system": ("GPU memory", "Power", "CPU temp", "GPU temp", "NVMe temp"),
}


def test_chart_titles_present_and_old_chart_code_gone():
    js = _script(dash.render_page())
    for group, titles in CHART_TITLES.items():
        for t in titles:
            assert f"'{t}'" in js, (group, t)
    # chart code sits in the page script, ordered like the groups
    for old in ("function spark", "function chart(", "function renderCharts", "spark(", 'id="trends"'):
        assert old not in dash.render_page(), old
    assert "request_series" in js and "id=\"rngchip\"" in dash.render_page()
    # inline SVG only: nothing external
    assert "<script src" not in dash.render_page() and "http://" not in js


NODE_HARNESS = r"""
const vm=require('vm'),fs=require('fs');
const script=fs.readFileSync(process.argv[2],'utf8'), P=JSON.parse(fs.readFileSync(process.argv[3],'utf8'));
const els={};
function el(id){return els[id]||(els[id]={id,innerHTML:'',textContent:'',style:{},hidden:false,dataset:{},children:[],disabled:false,
  value:'',clientWidth:P.cw||640,classList:{add(){},remove(){},toggle(){}},addEventListener(){},setAttribute(){},
  querySelector(){return null},querySelectorAll(){return []},closest(){return null},scrollIntoView(){}});}
const ctx={document:{getElementById:el},window:{addEventListener(){},innerWidth:P.iw},sessionStorage:{getItem(){return null},setItem(){}},
  fetch:async u=>({ok:true,json:async()=>u.startsWith('/api/state')?P.state:(u.startsWith('/api/history')?P.history:{rows:[],total:0})}),
  setInterval(){},setTimeout(){return 1},clearTimeout(){},console,Date,Math,JSON,out:s=>process.stdout.write(s)};
vm.createContext(ctx);
const tail=`
;(async()=>{ curWin=${P.win}; await refresh();
  const res={cards:$('cards').innerHTML,hits:{},reads:{}};
  (${JSON.stringify(P.hits||[])}).forEach(([id,vx])=>{res.hits[id+':'+vx]=CHW[id].hit(vx);});
  (${JSON.stringify(P.reads||[])}).forEach(([id,t])=>{res.reads[id]=CHW[id].rd(t);});
  const ids=[...$('cards').innerHTML.matchAll(/class="chart" data-id="([^"]+)"/g)].map(m=>m[1]);
  res.ws={};ids.forEach(id=>{res.ws[id]=CHW[id].W;});
  const stubs=ids.map(id=>{const ln={attrs:{},cls:new Set(),setAttribute(k,v){this.attrs[k]=v;},
    classList:{add:c=>ln.cls.add(c),remove:c=>ln.cls.delete(c)}};const rd={innerHTML:''};
    return {dataset:{id},querySelector:q=>q==='.xh'?ln:(q==='.rd'?rd:null),ln,rd};});
  $('cards').querySelectorAll=sel=>sel==='.chart'?stubs:[];
  if(${JSON.stringify(P.xh==null?null:P.xh)}!==null){xhairs(true,${JSON.stringify(P.xh)});
    res.xh=stubs.map(s=>({id:s.dataset.id,x:s.ln.attrs.x1,on:s.ln.cls.has('on'),rd:s.rd.innerHTML}));
    xhairs(false);res.off=stubs.every(s=>!s.ln.cls.has('on')&&s.rd.innerHTML==='');}
  out(JSON.stringify(res)); })().catch(e=>{console.error(String(e&&e.stack||e));process.exit(1);});`;
vm.runInContext(script+tail,ctx);
"""

NOW_P = 1_800_000_000


def _run_charts(tmp_path, win, rw, sw, rs, sm, events, hits=(), reads=(), cw=640, iw=1400, xh=None):
    payload = {
        "win": win,
        "cw": cw,
        "iw": iw,
        "xh": xh,
        "hits": list(hits),
        "reads": list(reads),
        "state": {
            "now": NOW_P,
            "health": {
                "model_up": True,
                "proxy_up": True,
                "slots_ok": True,
                "slots_busy": 0,
                "slots_total": 1,
                "config_id": "x",
            },
            "health_v2": {"upstream": "up", "config_id": "x", "stall_threshold_s": 300},
            "system": {"mem_total_mb": 131072},
            "energy": {},
            "temps": {},
            "slots": [],
            "cards": {},
        },
        "history": {
            "now": NOW_P,
            "window_sec": win,
            "request_bucket_sec": rw,
            "sample_bucket_sec": sw,
            "cards": {},
            "summary": {},
            "samples": sm,
            "request_series": rs,
            "events": events,
        },
    }
    (tmp_path / "h.js").write_text(NODE_HARNESS)
    (tmp_path / "page.js").write_text(_script(dash.render_page()))
    (tmp_path / "p.json").write_text(json.dumps(payload))
    r = subprocess.run(
        ["node", str(tmp_path / "h.js"), str(tmp_path / "page.js"), str(tmp_path / "p.json")],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert r.returncode == 0, r.stderr
    res = json.loads(r.stdout)
    charts = {}
    for chunk in res["cards"].split('<div class="chart" data-id="')[1:]:
        charts[chunk.split('"', 1)[0]] = chunk
    res["charts"] = charts
    return res


def _rq(t, **kw):
    d = {
        "t": t,
        "n": 1,
        "failed": 0,
        "decode_p50": 30.0,
        "decode_p95": 35.0,
        "prefill_p50": None,
        "prefill_p95": None,
        "ttft_p50": 1.0,
        "ttft_p95": 1.5,
        "restore_ms_p50": None,
        "restore_ms_p95": None,
        "cache_hit_pct": 90.0,
        "miss_pct": 50.0,
        "draft_accept_pct": 70.0,
        "miss_reasons": {},
    }
    d.update(kw)
    return d


def _sm(t, **kw):
    d = {
        "t": t,
        "power_w": 80.0,
        "cpu_temp_c": 50.0,
        "gpu_temp_c": 55.0,
        "cpu_temp_max_c": 60.0,
        "gpu_temp_max_c": 65.0,
        "nvme_temp_max_c": None,
        "mem_free_gb": 40.0,
        "mem_avail_gb": 50.0,
        "page_cache_gb": 20.0,
        "swap_used_gb": 0.0,
        "gtt_gb": 70.0,
    }
    d.update(kw)
    return d


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_chart_isolated_bucket_is_a_dot(tmp_path):
    only = _rq(NOW_P - 300)
    res = _run_charts(tmp_path, 900, 60, 60, [only], [], [])
    for cid in ("decode", "ttft", "cachehit", "miss", "draft"):
        assert "<circle" in res["charts"][cid], cid
    # the same bucket with a neighbour is a line (and no dot)
    res2 = _run_charts(tmp_path, 900, 60, 60, [only, _rq(NOW_P - 240)], [], [])
    assert "<circle" not in res2["charts"]["decode"]
    assert 'stroke-width="1.6"' in res2["charts"]["decode"]
    # a lone bucket in a stacked-area / line chart is a dot too
    res3 = _run_charts(tmp_path, 900, 60, 60, [], [_sm(NOW_P - 300)], [])
    assert "<circle" in res3["charts"]["memory"] and "<circle" in res3["charts"]["power"]
    assert "nvmetemp" not in res3["charts"]  # only when a sensor exists


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_chart_stall_marker_on_every_chart_and_detail_escaped(tmp_path):
    ev = [
        {"t": NOW_P - 200, "kind": "stall", "detail": "<script>alert(1)</script>"},
        {"t": NOW_P - 100, "kind": "down"},
    ]
    res = _run_charts(tmp_path, 900, 60, 60, [_rq(NOW_P - 300)], [_sm(NOW_P - 300)], ev)
    assert len(res["charts"]) >= 14
    for cid, html in res["charts"].items():
        assert 'class="mk mk-stall"' in html, cid
        assert 'class="mk mk-down"' in html, cid
    assert "<script>alert" not in res["cards"]
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in res["cards"]


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_chart_failure_markers_hits_and_miss_reasons(tmp_path):
    t = NOW_P - 300
    rs = [_rq(t, failed=2, miss_reasons={"evicted<i>": 2, "new_prefix": 1})]
    # two columns at 1400 px: W = (640 - 14) / 2 - 76 = 237, plot width 227, bucket centre at t + 30 s
    vx = 5 + (t + 30 - (NOW_P - 900)) / 900 * 227
    res = _run_charts(
        tmp_path, 900, 60, 60, rs, [], [], hits=[("failures", vx), ("failures", vx + 60)], reads=[("miss", t + 10)]
    )
    assert 'class="mk mk-fail"' in res["charts"]["decode"]
    assert 'class="mk mk-fail"' not in res["charts"]["failures"]  # the bars are the markers there
    assert res["hits"][f"failures:{vx}"] == {"from": t, "to": t + 60}
    assert res["hits"][f"failures:{vx + 60}"] is None
    rd = res["reads"]["miss"]
    assert "evicted&lt;i&gt; ×2" in rd and "new_prefix ×1" in rd and "<i>" not in rd


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
@pytest.mark.parametrize(
    ("win", "rw", "n"), [(900, 60, 15), (604800, 1800, 336), (86400, 300, 288)]
)
def test_chart_node_counts_stay_small(tmp_path, win, rw, n):
    t0 = NOW_P - win
    ev = [{"t": t0 + 100 + 997 * i, "kind": "stall"} for i in range(80)]
    # contiguous samples, and request buckets alternating so most points are isolated
    sm = [_sm(t0 + i * rw) for i in range(n)]
    rs = [_rq(t0 + i * rw, failed=i % 3, miss_reasons={"r": 1}) for i in range(0, n, 2)]
    res = _run_charts(tmp_path, win, rw, rw, rs, sm, ev)
    for cid, html in res["charts"].items():
        svg = html[html.index("<svg") : html.index("</svg>")]
        nodes = len(re.findall(r"<(?:path|circle|line|rect|text|title)\b", svg))
        assert nodes < 150, (cid, nodes)


# ---------------- layout: two-column chart grid, aligned plots, shared crosshair ----------------
def _groups(cards_html):
    """[(group id, [chart ids], has_grid_before_first_chart)] from the rendered cards."""
    out = []
    for chunk in re.split(r'<section class="grp', cards_html)[1:]:
        g = re.search(r'data-g="(\w+)"', chunk).group(1)
        ids = re.findall(r'class="chart" data-id="([^"]+)"', chunk)
        grid = chunk.find('<div class="chgrid">')
        out.append((g, ids, 0 <= grid < chunk.find('class="chart"')))
    return out


def test_chart_grid_css_two_columns_one_below_900():
    html = dash.render_page()
    assert ".chgrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr))" in html
    assert "@media(max-width:900px){.chgrid{grid-template-columns:minmax(0,1fr)}}" in html
    # nothing stretches a chart across both columns
    assert "grid-column" not in re.sub(r"\.leg\{[^}]*\}", "", html)
    assert "span 2" not in html and "grid-column:1/-1" not in html
    assert ".yax{flex:0 0 40px" in html  # fixed y-label gutter


def test_slots_container_precedes_health_group():
    html = dash.render_page()
    js = _script(html)
    assert html.index('id="slots"') < html.index('id="cards"')
    assert "grp('health'" in js  # the Health group is rendered into #cards, below the slots
    # tiles and cards share one four-column grid, so a lone tile is one column
    # wide and rows of 3, 5 or 6 cards keep the same card width
    four = "grid-template-columns:repeat(4,minmax(0,1fr))"
    assert four in re.search(r"\.slots\{([^}]*)\}", html).group(1)
    assert four in re.search(r"\.grid\{([^}]*)\}", html).group(1)


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_charts_in_grid_per_group_odd_count_leaves_last_cell_empty(tmp_path):
    t = NOW_P - 300
    res = _run_charts(tmp_path, 900, 60, 60, [_rq(t)], [_sm(t)], [])
    groups = _groups(res["cards"])
    assert [g for g, _i, _ok in groups] == ["perf", "cache", "health", "system"]
    counts = {g: len(ids) for g, ids, _ok in groups}
    assert counts == {"health": 2, "perf": 5, "cache": 3, "system": 4}
    assert all(ok for _g, _i, ok in groups)  # a grid container opens before the first chart
    # odd groups: still exactly one chart per cell, no chart marked wide
    assert counts["perf"] % 2 and counts["cache"] % 2
    assert "grid-column" not in res["cards"] and "span 2" not in res["cards"]
    # a sensor appears -> system has five charts (odd), still no wide cell
    sm = [_sm(t, nvme_temp_max_c=40.0)]
    res2 = _run_charts(tmp_path, 900, 60, 60, [_rq(t)], sm, [])
    assert {g: len(i) for g, i, _ok in _groups(res2["cards"])}["system"] == 5
    assert "grid-column" not in res2["cards"]


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
@pytest.mark.parametrize(("cw", "iw", "want"), [(1200, 1400, 517), (500, 600, 424), (350, 390, 274)])
def test_plot_width_and_gutter_identical_across_charts(tmp_path, cw, iw, want):
    t = NOW_P - 300
    res = _run_charts(tmp_path, 900, 60, 60, [_rq(t)], [_sm(t, nvme_temp_max_c=40.0)], [], cw=cw, iw=iw)
    assert len(res["charts"]) == 15
    assert set(res["ws"].values()) == {want}
    for cid, html in res["charts"].items():
        assert f'viewBox="0 0 {want} 60"' in html, cid
        assert '<div class="yax">' in html, cid  # same fixed gutter element in every chart
    # y gutter (46) + plot + chart chrome (30) fits the column, so no horizontal page scroll
    col = (cw - 14) / 2 if iw > 900 else cw
    assert want + 76 <= col


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_crosshair_same_time_on_charts_in_both_columns(tmp_path):
    t0 = NOW_P - 900
    rs = [_rq(t0 + 60 * i) for i in range(15)]
    sm = [_sm(t0 + 60 * i, nvme_temp_max_c=40.0) for i in range(15)]
    t = t0 + 60 * 7 + 20
    res = _run_charts(tmp_path, 900, 60, 60, rs, sm, [], xh=t)
    cols = {}
    for _g, ids, _ok in _groups(res["cards"]):
        for i, cid in enumerate(ids):
            cols[cid] = i % 2  # grid fills row by row: even index left, odd right
    assert set(cols.values()) == {0, 1}
    xs = {e["id"]: e for e in res["xh"]}
    assert set(xs) == set(cols)
    want = 5 + (t - t0) / 900 * (res["ws"]["decode"] - 10)
    for cid, e in xs.items():
        assert e["on"], cid
        assert abs(float(e["x"]) - want) < 0.06, (cid, cols[cid])  # computed from time
        assert e["rd"].startswith("<b>"), cid  # readout filled with the same moment
    stamp = re.match(r"<b>(.*?)</b>", xs["decode"]["rd"]).group(1)
    assert all(e["rd"].startswith(f"<b>{stamp}</b>") for e in xs.values())
    # charts from both columns carry values at that time, not just "no data"
    assert "no data" not in xs["decode"]["rd"] and "no data" not in xs["ttft"]["rd"]
    assert cols["decode"] == 0 and cols["prefill"] == 1 and cols["ttft"] == 0
    assert res["off"] is True  # leaving the charts clears every crosshair and readout
