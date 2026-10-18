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
  res.dreads={};res.dleg={};
  (${JSON.stringify(P.dreads||[])}).forEach(([id,i])=>{res.dreads[id+':'+i]=DPW[id].rd(i);res.dleg[id]=DPW[id].leg;});
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


def _run_charts(
    tmp_path, win, rw, sw, rs, sm, events, hits=(), reads=(), cw=640, iw=1400, xh=None, depth=None, dreads=()
):
    payload = {
        "win": win,
        "cw": cw,
        "iw": iw,
        "xh": xh,
        "hits": list(hits),
        "reads": list(reads),
        "dreads": list(dreads),
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
    if depth is not None:
        payload["history"]["depth"] = depth
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
    # the depth charts (class "chart depth") are collected separately
    depths = {}
    for chunk in res["cards"].split('<div class="chart depth" data-id="')[1:]:
        depths[chunk.split('"', 1)[0]] = chunk
    res["depth_charts"] = depths
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
    dense = [_rq(NOW_P - 60 * k) for k in range(2, 12)]  # 10 of 15 buckets: dense, plain line
    res2 = _run_charts(tmp_path, 900, 60, 60, dense, [], [])
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


def _decode_svg(res):
    html = res["charts"]["decode"]
    return html[html.index("<svg") : html.index("</svg>")]


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_sparse_series_has_faint_connector_and_dots_but_not_over_long_gap(tmp_path):
    win, rw = 86400, 300
    t0 = NOW_P - win
    # runs of two adjacent buckets; gaps of 1 h between them, then one 8 h gap (> window / 6)
    ts = [t0 + 3600, t0 + 3900, t0 + 7500, t0 + 7800, t0 + 11400, t0 + 11700, t0 + 11700 + 8 * 3600]
    res = _run_charts(tmp_path, win, rw, rw, [_rq(t) for t in ts], [], [])
    svg = _decode_svg(res)
    conn = re.search(r'<path class="cn" d="([^"]+)"', svg)
    assert conn and 'stroke-opacity=".3"' in svg
    assert conn.group(1).count("M") == 2  # two bridged gaps; the 8 h gap is left open
    assert svg.count("<circle") == len(ts)  # a dot on every populated bucket
    assert 'stroke-width="1.6"' in svg  # solid segments remain where buckets are adjacent
    assert 'fill-opacity=".2"' in svg  # p95 band stays
    # an upstream `down` event inside a short gap stops the bridge as well
    ev = [{"t": t0 + 5000, "kind": "down"}]
    res2 = _run_charts(tmp_path, win, rw, rw, [_rq(t) for t in ts], [], ev)
    conn2 = re.search(r'<path class="cn" d="([^"]+)"', _decode_svg(res2))
    assert conn2.group(1).count("M") == 1


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_dense_series_has_no_per_point_dots_or_connector(tmp_path):
    win, rw = 86400, 300
    t0 = NOW_P - win
    rs = [_rq(t0 + i * rw) for i in range(0, 288, 1) if i % 10]  # 90 % populated
    svg = _decode_svg(_run_charts(tmp_path, win, rw, rw, rs, [], []))
    assert "<circle" not in svg
    assert 'stroke-width="1.6"' in svg


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_sparse_connector_node_count_bounded(tmp_path):
    win, rw = 86400, 300
    t0 = NOW_P - win
    rs = [_rq(t0 + i * rw) for i in range(0, 288, 3)]  # a third populated, many dots
    res = _run_charts(tmp_path, win, rw, rw, rs, [], [])
    for cid in ("decode", "ttft", "cachehit"):
        html = res["charts"][cid]
        svg = html[html.index("<svg") : html.index("</svg>")]
        assert len(re.findall(r"<(?:path|circle|line|rect|text|title)\b", svg)) < 150, cid
        assert svg.count('class="cn"') == 1


def _cards_with(tmp_path, state_patch, hist_cards, summary=None, sm=None):
    """Render once with given card payloads; return the cards html."""
    import json as _json

    res = _run_charts(tmp_path, 900, 60, 60, [], [], [])
    assert res  # harness sanity
    p = tmp_path / "p.json"
    payload = _json.loads(p.read_text())
    payload["state"]["system"].update(state_patch.get("system", {}))
    payload["state"]["health_v2"].update(state_patch.get("health_v2", {}))
    payload["state"]["cards"] = hist_cards
    payload["history"]["cards"] = hist_cards
    payload["history"]["summary"] = summary or {}
    p.write_text(_json.dumps(payload))
    r = subprocess.run(
        ["node", str(tmp_path / "h.js"), str(tmp_path / "page.js"), str(p)],
        capture_output=True, text=True, timeout=60,
    )
    assert r.returncode == 0, r.stderr
    return _json.loads(r.stdout)["cards"]


def _gsum(cards, g):
    m = re.search(rf'data-g="{g}".*?<span class="gsum">(.*?)</span><span class="gcar"', cards, re.S)
    return m.group(1)


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_health_summary_marks_only_the_amber_or_red_items(tmp_path):
    fine = {"failures": {"count": 0, "pct": 0}}
    c = _cards_with(tmp_path, {"system": {"mem_free_mb": 30000}}, fine, {"min_mem_free_mb": 30000})
    assert _gsum(c, "health") == "up · 0 failed · 29.3 GB free"
    amber = {"failures": {"count": 4, "pct": 2.0}}
    c = _cards_with(tmp_path, {"system": {"mem_free_mb": 30000}}, amber, {"min_mem_free_mb": 30000})
    assert _gsum(c, "health") == 'up · <span class="sum-warn">⚠ 4 failed</span> · 29.3 GB free'
    c = _cards_with(tmp_path, {"system": {"mem_free_mb": 1200}}, fine, {"min_mem_free_mb": 1200})
    assert _gsum(c, "health") == 'up · 0 failed · <span class="sum-crit">⚠ 1.2 GB free</span>'
    c = _cards_with(
        tmp_path,
        {"system": {"mem_free_mb": 30000}, "health_v2": {"last_completion_age_s": 400, "busy_slots": 1}},
        amber,
        {"min_mem_free_mb": 30000},
    )
    assert _gsum(c, "health").startswith('<span class="sum-crit">⚠ stalled</span> · ')


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_group_summaries_are_short(tmp_path):
    cards = {
        "decode": {"p50": 31.44},
        "ttft": {"p50": 2.0},
        "requests": {"per_hour": 5.0},
        "cache_hit": {"pct": 92.0},
        "cache_source": {"miss_pct": 4.0},
        "restore": {"p50": 11.0},
    }
    c = _cards_with(tmp_path, {}, cards)
    assert _gsum(c, "perf") == "31.4 t/s · TTFT 2.0 s · 5.0/h"
    assert _gsum(c, "cache") == "hit 92% · miss 4% · 11 ms"


def test_phone_css_keeps_status_together_and_table_compact():
    html = dash.render_page()
    js = _script(html)
    assert re.search(r"\.slot \.st,\.nw\{white-space:nowrap\}", html)
    assert "●\\u00a0processing" in js  # no breakable space between dot and word
    assert "@media(max-width:640px){.slots.multi .slot .st{font-size:16px}" in html
    narrow = html[html.index("@media(max-width:640px){\n main{"):]
    narrow = narrow[: narrow.index("\n}")]
    assert "table-layout:fixed" in narrow and "th:nth-child(1){width:" in narrow
    widths = [int(w) for w in re.findall(r"th:nth-child\([1-6]\)\{width:(\d+)px", narrow)]
    assert len(widths) == 6 and sum(widths) <= 350  # time..TTFT fit a 390 px phone
    assert "text-overflow:ellipsis" in narrow
    assert 'class="rt" title="${esc(r.route' in js  # full route kept, escaped, in the title
    assert "ctag" in narrow  # tag stays beside the time, just tighter


# ---------------- depth charts: speed against context depth ----------------
def _bin(n, p50, lo=None, hi=None):
    if n == 0:
        return {"n": 0, "p25": None, "p50": None, "p75": None}
    return {"n": n, "p25": p50 if lo is None else lo, "p50": p50, "p75": p50 if hi is None else hi}


def _depth(dec_series, pre_bins, split=None):
    return {
        "window_sec": 900,
        "edges": list(dash.DEPTH_EDGES),
        "bins": list(dash.DEPTH_LABELS),
        "few_below": 3,
        "decode": {
            "n": sum(b["n"] for s in dec_series for b in s["bins"]),
            "min_completion_n": 64,
            "split": split,
            "series": dec_series,
        },
        "prefill": {
            "n": sum(b["n"] for b in pre_bins),
            "min_new_tokens": 2000,
            "series": [{"key": "all", "bins": pre_bins}] if pre_bins else [],
        },
    }


def _svg(html):
    return html[html.index("<svg") : html.index("</svg>")]


def test_depth_chart_titles_and_placement():
    js = _script(dash.render_page())
    assert "'Decode vs depth'" in js and "'Prefill vs depth'" in js
    assert "function depthChart(" in js and "class=\"chart depth\"" in js
    # the depth pair sits right after the Decode / Prefill time charts, inside the Performance grid
    perf = js[js.index("perf:cg(") : js.index("cache:cg(")]
    assert perf.index("'prefill'") < perf.index("depthDec,depthPre") < perf.index("'ttft'")


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_depth_charts_render_with_data_and_without(tmp_path):
    dec = [_bin(12, 40.0, 36.0, 44.0), _bin(30, 35.0, 31.0, 38.0), _bin(9, 30.0, 27.0, 33.0),
           _bin(2, 22.0, 20.0, 24.0), _bin(0, 0), _bin(0, 0), _bin(0, 0)]
    pre = [_bin(0, 0), _bin(8, 1200.0, 1100.0, 1300.0), _bin(5, 900.0, 850.0, 950.0),
           _bin(0, 0), _bin(1, 400.0), _bin(0, 0), _bin(0, 0)]
    t = NOW_P - 300
    res = _run_charts(
        tmp_path, 900, 60, 60, [_rq(t)], [_sm(t)], [],
        depth=_depth([{"key": "all", "bins": dec}], pre),
        dreads=[("depth_decode", 1), ("depth_decode", 3), ("depth_decode", 5), ("depth_prefill", 2)],
    )
    assert set(res["depth_charts"]) == {"depth_decode", "depth_prefill"}
    # time charts untouched: still 14 (no NVMe sensor here), same ids as before
    assert len(res["charts"]) == 14 and "depth_decode" not in res["charts"]
    d = res["depth_charts"]["depth_decode"]
    assert "Decode vs depth" in d and ">53 turns<" in d
    svg = _svg(d)
    assert f'viewBox="0 0 {res["ws"]["decode"]} 60"' in svg  # same plot width as the time charts
    assert 'class="dp"' in svg and 'class="xh"' not in svg  # no time crosshair line
    assert svg.count("<circle") == 4  # one dot per populated bin
    assert svg.count('class="few"') == 1 and 'fill="var(--panel)"' in svg  # the n=2 bin is hollow
    assert 'fill-opacity=".2"' in svg  # p25-p75 band for a single series
    assert 'stroke-width="1.6"' in svg
    assert "no data" not in svg
    # x labels at the six interior edges, in K
    xt = d[d.index('<div class="xt">') :]
    assert [m for m in re.findall(r"%\">([^<]+)</span>", xt)][:6] == ["4K", "8K", "16K", "32K", "64K", "128K"]
    # readouts name the bin, the median, the quartiles and n; "few" flagged
    rd = res["dreads"]["depth_decode:1"]
    assert rd.startswith("<b>4-8K</b>") and "35.0 t/s (31.0–38.0) · n=30" in rd and "few" not in rd
    assert "22.0 t/s (20.0–24.0) · n=2 few" in res["dreads"]["depth_decode:3"]
    assert "no data" in res["dreads"]["depth_decode:5"]
    assert "900 t/s (850–950) · n=5" in res["dreads"]["depth_prefill:2"]
    assert res["dleg"]["depth_decode"] == ""  # one series: no legend
    p = res["depth_charts"]["depth_prefill"]
    assert ">14 turns<" in p and _svg(p).count("<circle") == 3
    # no depth payload at all (older server): both charts render their empty state
    res2 = _run_charts(tmp_path, 900, 60, 60, [_rq(t)], [_sm(t)], [])
    for cid, html in res2["depth_charts"].items():
        assert "no data" in _svg(html) and "<circle" not in _svg(html), cid
        assert '<span class="cur">—</span>' in html
    assert len(res2["depth_charts"]) == 2
    # empty series lists: the same empty state
    res3 = _run_charts(tmp_path, 900, 60, 60, [], [], [], depth=_depth([], []))
    assert all("no data" in _svg(h) for h in res3["depth_charts"].values())


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_depth_decode_split_by_concurrency_has_legend_and_no_band(tmp_path):
    s1 = [_bin(20, 40.0, 36.0, 44.0), _bin(20, 35.0, 31.0, 38.0)] + [_bin(0, 0)] * 5
    s2 = [_bin(10, 30.0, 27.0, 33.0), _bin(10, 26.0, 24.0, 28.0)] + [_bin(0, 0)] * 5
    sq = [_bin(0, 0)] * 6 + [_bin(1, 9.0)]
    series = [{"key": "1", "bins": s1}, {"key": "2", "bins": s2}, {"key": "?", "bins": sq}]
    t = NOW_P - 300
    res = _run_charts(
        tmp_path, 900, 60, 60, [_rq(t)], [_sm(t)], [],
        depth=_depth(series, [], split="concurrency"), dreads=[("depth_decode", 0), ("depth_decode", 6)],
    )
    d = res["depth_charts"]["depth_decode"]
    svg = _svg(d)
    assert 'fill-opacity=".2"' not in svg  # several lines: no band
    assert svg.count('stroke-width="1.6"') == 2  # two lines (the "?" series has a single point)
    assert svg.count("<circle") == 5
    leg = res["dleg"]["depth_decode"]
    assert "1 slot</span>" in leg and "2 slots</span>" in leg and "conc. unknown</span>" in leg
    assert leg in d  # shown in the readout row while idle
    rd = res["dreads"]["depth_decode:0"]
    assert "1 slot 40.0 t/s (36.0–44.0) · n=20" in rd and "2 slots 30.0 t/s (27.0–33.0) · n=10" in rd
    assert "conc. unknown 9.0 t/s · n=1 few" in res["dreads"]["depth_decode:6"]
    # prefill with no rows shows its empty state beside it
    assert "no data" in _svg(res["depth_charts"]["depth_prefill"])


def test_depth_chart_pointer_handling_and_css():
    html = dash.render_page()
    js = _script(html)
    assert "svg.cp,svg.dp{" in html and ".dh.on{display:block}" in html
    # pointer moves over a depth chart highlight the bin on both depth charts and hold the cards still
    assert "closest('svg.cp,svg.dp')" in js and "dhairs(true,i)" in js and "cs.hover=Date.now()" in js
    assert "pointerleave',()=>{cs.hover=0;xhairs(false);dhairs(false);}" in js
    # every string from the payload goes through esc()
    body = js[js.index("function depthChart(") : js.index("function depthPos(")]
    assert "${esc(labels[i]" in body and "${esc(o.title)}" in body and "esc(kl(s.key))" in body
