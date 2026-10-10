"""Tests for the chart data: request_series, /api/history keys, /api/requests from/to."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime

import pytest

import slotpin.dash as dash

NOW = 1_800_000_000.0  # a multiple of 1800, so buckets line up with NOW


def _iso(epoch):
    return datetime.fromtimestamp(epoch, UTC).isoformat(timespec="seconds")


_n = [0]


def _turn(age_s, **kw):
    _n[0] += 1
    d = {
        "ts": _iso(NOW - age_s),
        "request_id": f"r{_n[0]}",
        "route": "interactive",
        "slot": 0,
        "prefix": "abcd1234",
        "prompt_n": 1000,
        "cache_n": 900,
        "completion_n": 50,
        "dur_s": 4.0,
        "ttft_s": 1.0,
        "decode_tps": 30.0,
        "served": "warm",
        "ok": True,
    }
    d.update(kw)
    return d


def _failed(age_s, **kw):
    _n[0] += 1
    d = {
        "ts": _iso(NOW - age_s),
        "request_id": f"f{_n[0]}",
        "event": "turn_failed",
        "reason": "timeout",
    }
    d.update(kw)
    return d


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(dash, "DB_PATH", str(tmp_path / "rollup.db"))
    monkeypatch.setattr(dash, "METRICS_FILE", str(tmp_path / "metrics.jsonl"))
    monkeypatch.setattr(dash, "_offset", 0)
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


def test_bucket_width_shared_with_sample_series(env):
    for win, want in ((900, 60), (3600, 60), (86400, 300), (259200, 300), (604800, 1800)):
        assert dash.bucket_width(win) == want
        assert dash.sample_bucket_width(win) == want
        h = dash.build_history(win)
        assert h["request_bucket_sec"] == h["sample_bucket_sec"] == want
    _load(env, [_turn(130), _turn(4000)])
    for win in (3600, 86400, 604800):
        w = dash.bucket_width(win)
        assert all(p["t"] % w == 0 for p in dash.request_series(win, NOW))


def test_percentiles_and_hit_rate(env):
    recs = [
        _turn(130, decode_tps=v, ttft_s=v / 10, prompt_n=1000, cache_n=500)
        for v in (10, 20, 30, 40)
    ]
    _load(env, recs)
    (p,) = dash.request_series(3600, NOW)
    assert p["t"] == NOW - 180 and p["n"] == 4 and p["failed"] == 0
    assert p["decode_p50"] == 25.0  # mean of the two middle values
    assert p["decode_p95"] == 40.0  # dashboard _pctile (nearest rank)
    assert p["ttft_p50"] == 2.5 and p["ttft_p95"] == 4.0
    assert p["cache_hit_pct"] == 50.0
    assert p["prefill_p50"] is None and p["restore_ms_p50"] is None
    assert p["draft_accept_pct"] is None and p["miss_pct"] == 0.0 and p["miss_reasons"] == {}


def test_prefill_needs_2000_new_tokens(env):
    _load(
        env,
        [
            _turn(130, prompt_n=2999, cache_n=1000, prefill_tps=100.0),  # 1999: excluded
            _turn(130, prompt_n=3000, cache_n=1000, prefill_tps=200.0),  # 2000: included
            _turn(130, prompt_n=9000, cache_n=1000, prefill_tps=400.0),
            _turn(130, prefill_tps=999.0),  # tiny prompt
        ],
    )
    (p,) = dash.request_series(3600, NOW)
    assert p["prefill_p50"] == 300.0 and p["prefill_p95"] == 400.0


def test_empty_buckets_omitted_and_failed_counted(env):
    _load(env, [_turn(130), _failed(135), _failed(140, reason="shed"), _turn(1000)])
    pts = dash.request_series(3600, NOW)
    assert [p["t"] for p in pts] == [NOW - 1020, NOW - 180]  # nothing in between
    assert pts[1]["n"] == 1 and pts[1]["failed"] == 2
    assert pts[0]["failed"] == 0
    # a bucket holding only a failure is still a point (with no ok data)
    _load(env, [_failed(2000)])
    only = [p for p in dash.request_series(3600, NOW) if p["t"] == NOW - 2040]
    assert only and only[0]["n"] == 0 and only[0]["decode_p50"] is None


def test_miss_reasons_gufo_and_llamacpp(env):
    _load(
        env,
        [
            _turn(130, upstream_cache="memory"),
            _turn(130, upstream_cache="miss", upstream_cache_miss_reason="evicted"),
            _turn(130, upstream_cache="miss", upstream_cache_miss_reason="evicted"),
            _turn(130, upstream_cache="miss"),
            _turn(2000, served="cold", upstream_cache_miss_reason="new_prefix"),
            _turn(2000, served="warm", draft_n=10, draft_n_accepted=7),
        ],
    )
    pts = dash.request_series(3600, NOW)
    late = pts[-1]
    assert late["miss_reasons"] == {"evicted": 2, "unknown": 1}
    assert late["miss_pct"] == 75.0
    early = pts[0]
    assert early["miss_reasons"] == {"new_prefix": 1} and early["miss_pct"] == 50.0
    assert early["draft_accept_pct"] == 70.0


def test_seven_days_stays_small(env):
    recs = [_turn(600 * i + 5) for i in range(1, 1000)]  # one every 10 minutes
    _load(env, recs)
    pts = dash.request_series(604800, NOW)
    assert 300 <= len(pts) < 400
    assert len(json.dumps(pts)) < 200_000


def test_history_keeps_series_and_adds_request_series(env):
    _load(env, [_turn(130)])
    h = dash.build_history(3600)
    assert "series" in h and h["request_series"] and h["request_bucket_sec"] == 60


# ---- /api/requests from / to ----
def _q(**kw):
    return dash.query_requests({k: [str(v)] for k, v in kw.items()}, NOW)


def test_requests_from_to_range_and_total(env):
    _load(env, [_turn(a) for a in (100, 200, 300, 400, 500)] + [_failed(250)])
    code, j = _q(window=3600, **{"from": NOW - 350, "to": NOW - 150})
    assert code == 200
    assert j["total"] == 3 and len(j["rows"]) == 3  # ages 200, 250 (failed), 300
    assert {round(NOW - r["ts"]) for r in j["rows"]} == {200, 250, 300}
    assert j["from"] == NOW - 350 and j["to"] == NOW - 150
    # filters combine with the range
    _, jf = _q(window=3600, filter="failed", **{"from": NOW - 350, "to": NOW - 150})
    assert jf["total"] == 1
    # paging inside the range
    _, jp = _q(limit=2, offset=2, **{"from": NOW - 350, "to": NOW - 150})
    assert jp["total"] == 3 and len(jp["rows"]) == 1
    # only "from" runs to now; only "to" starts at the window edge
    assert _q(window=3600, **{"from": NOW - 250})[1]["total"] == 3  # 100, 200, 250
    assert _q(window=350, to=NOW - 250)[1]["total"] == 2  # 250 (failed) and 300
    # without a range nothing changes
    assert _q(window=3600)[1]["total"] == 6 and "from" not in _q(window=3600)[1]


def test_requests_range_clamped_to_retention(env):
    _load(env, [_turn(100)])
    code, j = _q(**{"from": 0, "to": NOW + 10_000})
    assert code == 200 and j["total"] == 1
    assert j["from"] == NOW - dash.RETENTION_DAYS * 86400 and j["to"] == NOW


@pytest.mark.parametrize(
    "bad",
    [
        {"from": "abc"},
        {"to": "x1"},
        {"from": "nan"},
        {"to": "inf"},
        {"from": NOW - 100, "to": NOW - 200},
    ],
)
def test_requests_bad_range_is_400(env, bad):
    code, j = _q(**bad)
    assert code == 400 and "error" in j
