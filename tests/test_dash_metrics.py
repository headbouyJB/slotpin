"""Tests for GET /metrics: exposition format, bounded labels, monotonic counters, state values."""

from __future__ import annotations

import json
import re
import threading
import time
import urllib.request
from datetime import UTC, datetime
from http.server import ThreadingHTTPServer

import pytest
from test_no_private_data import ROOT, _scan

import slotpin.dash as dash

NOW = 1_800_000_000.0

SAMPLE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})? (-?(?:\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|\+Inf|-Inf|NaN))$"
)
LABEL = re.compile(r'^([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"$')


def _iso(epoch):
    return datetime.fromtimestamp(epoch, UTC).isoformat(timespec="seconds")


_n = [0]


def _turn(age_s, **kw):
    _n[0] += 1
    d = {
        "ts": _iso(NOW - age_s),
        "request_id": f"m{_n[0]}",
        "route": "chat",
        "slot": 0,
        "prefix": "abcd1234",
        "prompt_n": 6000,
        "cache_n": 1000,
        "completion_n": 300,
        "dur_s": 4.0,
        "ttft_s": 0.8,
        "queue_wait_ms": 20.0,
        "decode_tps": 30.0,
        "prefill_tps": 900.0,
        "concurrency": 1.0,
        "draft_n": 100,
        "draft_n_accepted": 60,
        "energy_fair_wh": 0.5,
        "served": "warm",
        "ok": True,
    }
    d.update(kw)
    return d


def _failed(age_s, reason="timeout", route="chat"):
    _n[0] += 1
    return {
        "ts": _iso(NOW - age_s),
        "request_id": f"f{_n[0]}",
        "event": "turn_failed",
        "reason": reason,
        "route": route,
    }


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
    monkeypatch.setattr(dash, "read_config_slots", lambda: 1)
    dash._buckets.clear()
    dash._samples.clear()
    dash._events.clear()
    dash._req_pending.clear()
    monkeypatch.setattr(time, "time", lambda: NOW)
    yield tmp_path
    dash._buckets.clear()
    dash._samples.clear()
    dash._events.clear()


def _load(env, recs):
    (env / "metrics.jsonl").write_text("".join(json.dumps(r) + "\n" for r in recs))
    dash.backfill_requests()


def parse(text):
    """Minimal exposition-format checker -> {family: type}, [(name, {labels}, value)].

    Asserts the format rules this project relies on: a HELP and TYPE line per
    family before its samples, sample lines that match the grammar, label
    values quoted and escaped, a trailing newline.
    """
    assert text.endswith("\n")
    types, samples, cur = {}, [], None
    for line in text.splitlines():
        if line.startswith("# HELP "):
            fam = line.split()[2]
            assert fam not in types, f"family {fam} twice"
            cur = fam
            continue
        if line.startswith("# TYPE "):
            _h, _t, fam, typ = line.split(" ", 3)
            assert fam == cur and typ in ("counter", "gauge", "histogram")
            types[fam] = typ
            continue
        assert not line.startswith("#"), line
        m = SAMPLE.match(line)
        assert m, line
        name, labs, val = m.group(1), m.group(2), m.group(3)
        labels = {}
        if labs:
            for part in labs.split(","):
                lm = LABEL.match(part)
                assert lm, part
                labels[lm.group(1)] = lm.group(2)
        assert cur and types.get(cur), f"sample before TYPE: {line}"
        if types[cur] == "histogram":
            assert name in (cur + "_bucket", cur + "_sum", cur + "_count"), line
            if name.endswith("_bucket"):
                assert "le" in labels
        else:
            assert name == cur, line
        samples.append((name, labels, float("inf") if val == "+Inf" else float(val)))
    return types, samples


def _hist(samples, fam):
    """{plain labels: {"buckets": [(le, v)], "sum": x, "count": n}}"""
    out = {}
    for name, labels, v in samples:
        if not name.startswith(fam):
            continue
        plain = tuple(sorted((k, x) for k, x in labels.items() if k != "le"))
        g = out.setdefault(plain, {"buckets": [], "sum": None, "count": None})
        if name == fam + "_bucket":
            le = labels["le"]
            g["buckets"].append((float("inf") if le == "+Inf" else float(le), v))
        elif name == fam + "_sum":
            g["sum"] = v
        elif name == fam + "_count":
            g["count"] = v
    return out


def _val(samples, name, **labels):
    hits = [v for n, lab, v in samples if n == name and lab == labels]
    assert len(hits) == 1, (name, labels, hits)
    return hits[0]


def test_format_parses_and_histograms_are_cumulative(env):
    _load(
        env,
        [
            _turn(100, ttft_s=0.05),
            _turn(110, ttft_s=0.3, route="background"),
            _turn(120, ttft_s=7.0, prompt_n=40000, concurrency=2.4),
            _turn(130, ttft_s=90.0, prompt_n=200000, concurrency=None),
            _failed(50),
            _failed(60, reason="shed", route="background"),
        ],
    )
    text = dash.render_metrics()
    types, samples = parse(text)
    assert all(n.startswith("slotpin_") for n in types)
    assert {f for f, _t, _h in dash.PROM_FAMILIES if types.get(f) == "histogram"} == dash._PROM_HIST
    for fam, typ in types.items():
        if typ != "histogram":
            continue
        for plain, g in _hist(samples, fam).items():
            les = [le for le, _v in g["buckets"]]
            assert les == sorted(les) and les[-1] == float("inf"), (fam, plain)
            vals = [v for _le, v in g["buckets"]]
            assert vals == sorted(vals), (fam, plain)  # cumulative
            assert vals[-1] == g["count"], (fam, plain)  # +Inf == count
            assert g["sum"] is not None
            # every bucket is listed, zero counts included
            bounds = {
                "slotpin_ttft_seconds": dash.PROM_TTFT_BUCKETS,
                "slotpin_request_duration_seconds": dash.PROM_DURATION_BUCKETS,
                "slotpin_queue_wait_seconds": dash.PROM_QUEUE_BUCKETS,
                "slotpin_decode_tokens_per_second": dash.PROM_DECODE_BUCKETS,
                "slotpin_prefill_tokens_per_second": dash.PROM_PREFILL_BUCKETS,
            }[fam]
            assert les == [float(b) for b in bounds] + [float("inf")]
    tt = _hist(samples, "slotpin_ttft_seconds")[(("route", "chat"),)]
    assert tt["count"] == 3 and tt["sum"] == pytest.approx(97.05)
    assert dict(tt["buckets"])[0.1] == 1 and dict(tt["buckets"])[10.0] == 2
    assert dict(tt["buckets"])[60.0] == 2 and dict(tt["buckets"])[float("inf")] == 3


def test_counters_and_labels_from_rows(env):
    _load(
        env,
        [
            _turn(100),
            _turn(110, route="background", prompt_n=9000, cache_n=0, completion_n=50),  # decode: too short
            _turn(120, prompt_n=70000, cache_n=69000, prefill_tps=3000.0),  # prefill: < 2000 new
            _turn(130, concurrency=3.6, decode_tps=12.0),  # -> 4+
            _turn(140, concurrency=None, draft_n=None, energy_fair_wh=None, energy_turn_wh=1.5),
            _failed(50),
            _failed(60, reason="shed", route="background"),
            _failed(70, reason="timeout", route="background"),
        ],
    )
    _types, s = parse(dash.render_metrics())
    assert _val(s, "slotpin_requests_total", route="chat", status="ok") == 4
    assert _val(s, "slotpin_requests_total", route="background", status="ok") == 1
    assert _val(s, "slotpin_requests_total", route="chat", status="failed") == 1
    assert _val(s, "slotpin_requests_total", route="background", status="failed") == 2
    assert _val(s, "slotpin_failures_total", reason="timeout") == 2
    assert _val(s, "slotpin_failures_total", reason="shed") == 1
    assert _val(s, "slotpin_prompt_tokens_total", route="chat") == 6000 * 3 + 70000
    assert _val(s, "slotpin_cached_tokens_total", route="background") == 0
    assert _val(s, "slotpin_generated_tokens_total", route="background") == 50
    assert _val(s, "slotpin_draft_proposed_tokens_total") == 400
    assert _val(s, "slotpin_draft_accepted_tokens_total") == 240
    assert _val(s, "slotpin_turn_energy_wh_total", route="chat") == pytest.approx(0.5 * 3 + 1.5)
    dec = _hist(s, "slotpin_decode_tokens_per_second")
    assert set(dec) == {
        (("concurrency", "1"), ("depth", "4-8K")),
        (("concurrency", "?"), ("depth", "4-8K")),
        (("concurrency", "4+"), ("depth", "4-8K")),
        (("concurrency", "1"), ("depth", "64-128K")),
    }
    assert dec[(("concurrency", "4+"), ("depth", "4-8K"))]["sum"] == 12.0
    pre = _hist(s, "slotpin_prefill_tokens_per_second")
    assert set(pre) == {(("depth", "4-8K"),), (("depth", "8-16K"),)}
    assert pre[(("depth", "4-8K"),)]["count"] == 3  # the 70000-deep turn had < 2000 new tokens
    # every label key and the bounded ones' values
    keys = {k for _n, lab, _v in s for k in lab}
    assert keys <= {"route", "status", "reason", "depth", "concurrency", "le", "sensor"}
    depths = {lab["depth"] for _n, lab, _v in s if "depth" in lab}
    assert depths <= set(dash.DEPTH_LABELS)
    concs = {lab["concurrency"] for _n, lab, _v in s if "concurrency" in lab}
    assert concs <= {*dash.CONC_BUCKETS, "?"}
    for name, lab, _v in s:
        assert not any(k in lab for k in ("prefix", "slot", "request_id", "config_id")), name


def test_cardinality_bounded_whatever_the_rows(env):
    # 7 depth bins x 5 concurrency buckets x (10 buckets + Inf + sum + count) is the ceiling
    recs = [
        _turn(100 + i, prompt_n=(1 << (10 + i % 9)) + 1, concurrency=float(i % 7), decode_tps=5.0 * (i % 30 + 1),
              route=f"lane{i % 3}")
        for i in range(400)
    ]
    _load(env, recs)
    _types, s = parse(dash.render_metrics())
    dec = [x for x in s if x[0].startswith("slotpin_decode_tokens_per_second")]
    assert len(dec) <= 7 * 5 * (len(dash.PROM_DECODE_BUCKETS) + 3)
    assert len({lab["depth"] for _n, lab, _v in dec}) <= 7
    assert len({lab["concurrency"] for _n, lab, _v in dec}) <= 5
    assert len({lab["route"] for _n, lab, _v in s if "route" in lab}) == 3
    assert len(s) < 1500


def test_counters_survive_pruning_and_restart(env):
    _load(env, [_turn(100), _turn(200, route="background"), _failed(50)])
    before = parse(dash.render_metrics())[1]
    assert _val(before, "slotpin_requests_total", route="chat", status="ok") == 1
    # the retention prune runs 9 days later: every row is deleted
    old_time = time.time
    time.time = lambda: NOW + 9 * 86400
    try:
        dash._flush_db()
        con = dash._db()
        assert con.execute("SELECT COUNT(*) FROM requests").fetchone()[0] == 0
        assert con.execute("SELECT COUNT(*) FROM prom_base").fetchone()[0] > 0
        con.close()
        after = parse(dash.render_metrics())[1]
    finally:
        time.time = old_time
    counters = [
        (n, tuple(sorted(lab.items())), v)
        for n, lab, v in before
        if n.startswith("slotpin_") and not n.startswith(
            ("slotpin_proxy", "slotpin_upstream", "slotpin_slots", "slotpin_last", "slotpin_host",
             "slotpin_gpu", "slotpin_power", "slotpin_energy_kwh", "slotpin_temperature")
        )
    ]
    after_map = {(n, tuple(sorted(lab.items()))): v for n, lab, v in after}
    assert counters  # the fixture produced counter samples
    for n, lab, v in counters:
        assert after_map[(n, lab)] == v, (n, lab)  # nothing fell
    # new rows add on top of the folded base
    _load(env, [_turn(-9 * 86400 + 100), _turn(-9 * 86400 + 110)])  # two rows "now" (9 days on)
    time.time = lambda: NOW + 9 * 86400
    try:
        again = parse(dash.render_metrics())[1]
    finally:
        time.time = old_time
    assert _val(again, "slotpin_requests_total", route="chat", status="ok") == 3
    assert _val(again, "slotpin_failures_total", reason="timeout") == 1
    # a second prune pass with nothing to prune changes nothing
    time.time = lambda: NOW + 9 * 86400
    try:
        dash._flush_db()
        third = parse(dash.render_metrics())[1]
    finally:
        time.time = old_time
    assert _val(third, "slotpin_requests_total", route="chat", status="ok") == 3


def test_failed_fold_does_not_block_pruning(env, monkeypatch):
    _load(env, [_turn(100), _turn(200)])

    def boom(con, cutoff):
        raise RuntimeError("fold failed")

    monkeypatch.setattr(dash, "_prom_fold_expiring", boom)
    monkeypatch.setattr(time, "time", lambda: NOW + 9 * 86400)
    dash._flush_db()
    con = dash._db()
    assert con.execute("SELECT COUNT(*) FROM requests").fetchone()[0] == 0  # pruned anyway
    assert con.execute("SELECT COUNT(*) FROM prom_base").fetchone()[0] == 0  # fold undone
    con.close()


def test_gauges_match_api_state_and_http_content_type(env):
    _load(env, [_turn(100)])
    dash._live.update(
        {
            "proxy_up": True,
            "slots_ok": True,
            "slots": [{"id": 0, "is_processing": True, "n_decoded": 5}, {"id": 1, "is_processing": False}],
            "energy": {"power": 88.5, "total_energy": 412.345},
            "energy_ts": NOW - 2,
            "energy_ok": True,
            "mem": {
                "mem_total_mb": 131072, "mem_free_mb": 20000, "mem_avail_mb": 60000,
                "page_cache_mb": 40000, "swap_used_mb": 512, "swap_total_mb": 8192,
                "swap_free_mb": 7680, "gtt_used_mb": 60000,
            },
            "temps": {"cpu_c": 61.5, "gpu_c": 70.0, "nvme_c": None},
        }
    )
    st = dash.build_state()
    _types, s = parse(dash.render_metrics())
    assert _val(s, "slotpin_proxy_up") == 1 and _val(s, "slotpin_upstream_up") == 1
    assert _val(s, "slotpin_slots_total") == st["health"]["slots_total"] == 2
    assert _val(s, "slotpin_slots_busy") == st["health"]["slots_busy"] == 1
    assert _val(s, "slotpin_host_memory_free_bytes") == st["system"]["mem_free_mb"] * 1048576
    assert _val(s, "slotpin_host_memory_available_bytes") == 60000 * 1048576
    assert _val(s, "slotpin_host_swap_used_bytes") == 512 * 1048576
    assert _val(s, "slotpin_power_watts") == st["energy"]["power_w"] == 88.5
    assert _val(s, "slotpin_energy_kwh_total") == st["energy"]["total_kwh"] == 412.345
    assert _val(s, "slotpin_temperature_celsius", sensor="cpu") == st["temps"]["cpu_c"] == 61.5
    assert _val(s, "slotpin_temperature_celsius", sensor="gpu") == 70.0
    assert not [x for x in s if x[0] == "slotpin_temperature_celsius" and x[1] == {"sensor": "nvme"}]
    assert _val(s, "slotpin_last_completion_age_seconds") == st["health_v2"]["last_completion_age_s"]
    assert _val(s, "slotpin_gpu_gtt_used_bytes") == st["system"]["gtt_used_mb"] * 1048576 == 60000 * 1048576
    # a stale energy feed drops the power gauge (no last-known value pretending to be current)
    dash._live["energy_ok"] = False
    _types, s2 = parse(dash.render_metrics())
    assert not [x for x in s2 if x[0] == "slotpin_power_watts"]
    srv = ThreadingHTTPServer(("127.0.0.1", 0), dash.Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{srv.server_address[1]}/metrics") as r:
            assert r.status == 200
            assert r.headers["Content-Type"] == "text/plain; version=0.0.4; charset=utf-8"
            body = r.read().decode("utf-8")
        parse(body)
        assert "slotpin_requests_total" in body
    finally:
        srv.shutdown()
        srv.server_close()


def test_label_values_are_escaped_and_output_has_no_private_data(env):
    evil = 'la"ne\\x\nz'
    _load(env, [_turn(100, route=evil), _failed(50, reason=evil)])
    text = dash.render_metrics()
    assert 'route="la\\"ne\\\\x\\nz"' in text
    assert 'reason="la\\"ne\\\\x\\nz"' in text
    _types, s = parse(text)
    assert _val(s, "slotpin_requests_total", route='la\\"ne\\\\x\\nz', status="ok") == 1
    # nothing in a fixture-driven exposition matches the private-data deny list
    assert not _scan(ROOT / "metrics.txt", text)
