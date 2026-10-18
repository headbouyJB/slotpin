"""Tests for the depth curve: depth_series aggregation, /api/history `depth`, /api/depth."""

from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from http.server import ThreadingHTTPServer

import pytest

import slotpin.dash as dash

NOW = 1_800_000_000.0


def _iso(epoch):
    return datetime.fromtimestamp(epoch, UTC).isoformat(timespec="seconds")


_n = [0]


def _turn(age_s, **kw):
    _n[0] += 1
    d = {
        "ts": _iso(NOW - age_s),
        "request_id": f"d{_n[0]}",
        "route": "interactive",
        "slot": 0,
        "prefix": "abcd1234",
        "prompt_n": 6000,
        "cache_n": 1000,
        "completion_n": 200,
        "dur_s": 4.0,
        "ttft_s": 1.0,
        "decode_tps": 30.0,
        "prefill_tps": 900.0,
        "served": "warm",
        "ok": True,
    }
    d.update(kw)
    return d


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(dash, "DB_PATH", str(tmp_path / "rollup.db"))
    monkeypatch.setattr(dash, "METRICS_FILE", str(tmp_path / "metrics.jsonl"))
    monkeypatch.setattr(dash, "_offset", 0)
    monkeypatch.setattr(dash, "read_config_slots", lambda: 1)
    dash._buckets.clear()
    dash._events.clear()
    dash._req_pending.clear()
    monkeypatch.setattr(time, "time", lambda: NOW)
    yield tmp_path
    dash._buckets.clear()
    dash._events.clear()


def _load(env, recs):
    (env / "metrics.jsonl").write_text("".join(json.dumps(r) + "\n" for r in recs))
    dash.backfill_requests()


def _one(series, key="all"):
    (s,) = [s for s in series if s["key"] == key]
    return s["bins"]


def test_depth_bin_edges():
    assert dash.depth_bin(None) is None
    assert dash.depth_bin(0) == 0 and dash.depth_bin(4095) == 0
    assert dash.depth_bin(4096) == 1 and dash.depth_bin(8191) == 1
    assert dash.depth_bin(8192) == 2 and dash.depth_bin(65535) == 4
    assert dash.depth_bin(131071) == 5 and dash.depth_bin(131072) == 6 and dash.depth_bin(10**7) == 6
    assert len(dash.DEPTH_LABELS) == len(dash.DEPTH_EDGES) + 1 == 7


def test_conc_bucket_rounds_to_nearest_slot():
    assert dash.conc_bucket(None) == "?"
    assert dash.conc_bucket(1.0) == "1" and dash.conc_bucket(1.49) == "1"
    assert dash.conc_bucket(1.5) == "2" and dash.conc_bucket(2.4) == "2"
    assert dash.conc_bucket(3.0) == "3" and dash.conc_bucket(3.5) == "4+"
    assert dash.conc_bucket(7.0) == "4+" and dash.conc_bucket(0.2) == "1"


def test_empty_window_has_shape_and_no_series(env):
    d = dash.depth_series(3600, NOW)
    assert d["bins"] == list(dash.DEPTH_LABELS) and d["edges"] == list(dash.DEPTH_EDGES)
    assert d["few_below"] == 3
    assert d["decode"] == {
        "n": 0,
        "min_completion_n": 64,
        "split": None,
        "series": [],
    }
    assert d["prefill"] == {"n": 0, "min_new_tokens": 2000, "series": []}
    assert d["window_sec"] == 3600


def test_binning_percentiles_and_few_flag(env):
    recs = [_turn(100, prompt_n=5000, decode_tps=v) for v in (10, 20, 30, 40)]  # 4-8K
    recs += [_turn(100, prompt_n=40000, decode_tps=25.0)]  # 32-64K, one turn
    recs += [_turn(100, prompt_n=4095, decode_tps=50.0), _turn(100, prompt_n=4096, decode_tps=60.0)]
    _load(env, recs)
    d = dash.depth_series(3600, NOW)
    assert d["decode"]["n"] == 7 and d["decode"]["split"] is None
    bins = _one(d["decode"]["series"])
    assert len(bins) == 7
    assert bins[0] == {"n": 1, "p25": 50.0, "p50": 50.0, "p75": 50.0}  # 4095 is < 4K
    b1 = bins[1]
    assert b1["n"] == 5 and b1["p50"] == 30.0  # 10 20 30 40 60
    assert b1["p25"] == 20.0 and b1["p75"] == 40.0  # nearest rank, like the p95 elsewhere
    assert bins[4]["n"] == 1 and bins[4]["p50"] == 25.0  # returned, the page flags n < few_below
    assert bins[2] == {"n": 0, "p25": None, "p50": None, "p75": None}
    assert all(b["n"] == 0 for b in (bins[3], bins[5], bins[6]))


def test_decode_filters(env):
    _load(
        env,
        [
            _turn(100, completion_n=63, decode_tps=99.0),  # too short
            _turn(100, completion_n=64, decode_tps=30.0),  # counted
            _turn(100, decode_tps=0),  # no speed
            _turn(100, decode_tps=None),
            _turn(100, prompt_n=None, decode_tps=30.0),  # unknown depth
            {"ts": _iso(NOW - 50), "event": "turn_failed", "reason": "timeout", "prompt_n": 6000},
            _turn(5000, decode_tps=70.0),  # outside the window
        ],
    )
    d = dash.depth_series(3600, NOW)
    assert d["decode"]["n"] == 1
    assert _one(d["decode"]["series"])[1]["p50"] == 30.0


def test_prefill_needs_2000_new_tokens(env):
    _load(
        env,
        [
            _turn(100, prompt_n=6000, cache_n=4001, prefill_tps=100.0),  # 1999 new: excluded
            _turn(100, prompt_n=6000, cache_n=4000, prefill_tps=200.0),  # 2000 new: included
            _turn(100, prompt_n=6000, cache_n=None, prefill_tps=300.0),  # unknown cache
            _turn(100, prompt_n=6000, cache_n=0, prefill_tps=0),
            _turn(100, prompt_n=20000, cache_n=0, prefill_tps=400.0),
        ],
    )
    d = dash.depth_series(3600, NOW)
    assert d["prefill"]["n"] == 2 and d["prefill"]["min_new_tokens"] == dash.PREFILL_MIN_NEW
    bins = _one(d["prefill"]["series"])
    assert bins[1]["n"] == 1 and bins[1]["p50"] == 200.0
    assert bins[3]["n"] == 1 and bins[3]["p50"] == 400.0
    # the prefill rule is the same constant the Prefill card and request_series use
    assert dash.window_stats(3600, NOW)["prefill"]["n"] == 2
    (p,) = dash.request_series(3600, NOW)
    assert p["prefill_p50"] == 300.0


def test_concurrency_split_only_on_multi_slot_boxes(env, monkeypatch):
    recs = [
        _turn(100, concurrency=1.0, decode_tps=30.0),
        _turn(100, concurrency=1.2, decode_tps=32.0),
        _turn(100, concurrency=2.5, decode_tps=20.0),  # -> "3"
        _turn(100, concurrency=4.0, decode_tps=12.0),
        _turn(100, concurrency=6.0, decode_tps=10.0),  # -> "4+"
    ]
    recs.append(_turn(100, decode_tps=28.0))  # no concurrency field -> "?"
    _load(env, recs)
    one = dash.depth_series(3600, NOW)["decode"]
    assert one["split"] is None and [s["key"] for s in one["series"]] == ["all"]
    assert _one(one["series"])[1]["n"] == 6

    monkeypatch.setattr(dash, "read_config_slots", lambda: 4)
    d = dash.depth_series(3600, NOW)["decode"]
    assert d["split"] == "concurrency" and d["n"] == 6
    assert [s["key"] for s in d["series"]] == ["1", "3", "4+", "?"]  # fixed order, empties omitted
    assert _one(d["series"], "1")[1] == {"n": 2, "p25": 30.0, "p50": 31.0, "p75": 32.0}
    assert _one(d["series"], "3")[1]["p50"] == 20.0
    assert _one(d["series"], "4+")[1] == {"n": 2, "p25": 10.0, "p50": 11.0, "p75": 12.0}
    assert _one(d["series"], "?")[1]["p50"] == 28.0
    # the prefill series never splits
    assert [s["key"] for s in dash.depth_series(3600, NOW)["prefill"]["series"]] == ["all"]


def test_history_carries_depth_and_api_depth_matches(env):
    _load(env, [_turn(100), _turn(200, prompt_n=70000)])
    h = dash.build_history(3600)
    assert h["depth"] == dash.depth_series(3600, NOW)
    assert h["depth"]["decode"]["n"] == 2 and h["depth"]["prefill"]["n"] == 2
    srv = ThreadingHTTPServer(("127.0.0.1", 0), dash.Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    try:
        with urllib.request.urlopen(base + "/api/depth?window=3600") as r:
            body = json.load(r)
        assert body == h["depth"]
        with urllib.request.urlopen(base + "/api/depth?window=1") as r:
            assert json.load(r)["window_sec"] == 300  # clamped like /api/history
        with pytest.raises(urllib.error.HTTPError) as ei:
            urllib.request.urlopen(base + "/api/depth?window=1h")
        assert ei.value.code == 400 and "error" in json.load(ei.value)
    finally:
        srv.shutdown()
        srv.server_close()


def test_seven_days_stays_small(env):
    _load(env, [_turn(600 * i + 5, prompt_n=1000 * (i % 150)) for i in range(1, 1000)])
    d = dash.depth_series(604800, NOW)
    assert d["decode"]["n"] == 999
    assert len(json.dumps(d)) < 4000
