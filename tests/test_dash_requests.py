"""Tests for slotpin-dash's requests table: ingestion, backfill, retention, API."""

from __future__ import annotations

import gzip
import json
import sqlite3
import time
from datetime import UTC, datetime

import pytest

import slotpin.dash as dash

NOW = 1_800_000_000.0


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
        "decode_tps": 30.0,
        "served": "warm",
        "ok": True,
    }
    d.update(kw)
    return d


def _failed(age_s, **kw):
    d = {"ts": _iso(NOW - age_s), "event": "turn_failed", "reason": "timeout"}
    d.update(kw)
    return d


def _write(path, recs, gz=False):
    text = "".join(json.dumps(r) + "\n" for r in recs)
    if gz:
        with gzip.open(path, "wt") as f:
            f.write(text)
    else:
        path.write_text(text)


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(dash, "DB_PATH", str(tmp_path / "rollup.db"))
    monkeypatch.setattr(dash, "METRICS_FILE", str(tmp_path / "metrics.jsonl"))
    monkeypatch.setattr(dash, "_offset", 0)
    dash._buckets.clear()
    dash._req_pending.clear()
    monkeypatch.setattr(time, "time", lambda: NOW)
    return tmp_path


def _rows():
    con = dash._db()
    con.row_factory = sqlite3.Row
    out = [dict(r) for r in con.execute("SELECT * FROM requests ORDER BY ts")]
    con.close()
    return out


def test_ingest_ok_and_failed(env):
    lines = [
        _turn(100, request_id="r1", upstream_cache="miss", upstream_restore_ms=12.5),
        _failed(50, route="interactive", slot=1, prefix="ffff0000", dur_s=9.5, upstream_status=502),
        _failed(40, reason="shed"),
    ]
    _write(env / "metrics.jsonl", lines)
    with dash._lock:
        dash._read_new_metrics()
    dash._flush_requests()
    rows = _rows()
    assert [r["status"] for r in rows] == ["ok", "failed", "failed"]
    ok, f1, f2 = rows
    assert ok["key"] == f"ok:r1:{int(NOW - 100)}" and ok["upstream_cache"] == "miss"
    assert ok["upstream_restore_ms"] == 12.5 and ok["draft_n"] is None
    assert f1["fail_reason"] == "timeout" and f1["dur_s"] == 9.5 and f1["slot"] == 1
    assert f2["fail_reason"] == "shed" and f2["route"] is None and f2["dur_s"] is None
    # existing bucket folding is unchanged (counts the 3 parsed records as before)
    assert sum(b["reqs"] for b in dash._buckets.values()) == 3
    # re-reading the same lines (e.g. after truncation) does not duplicate
    dash._offset = 0
    with dash._lock:
        dash._read_new_metrics()
    dash._flush_requests()
    assert len(_rows()) == 3


def test_kv_and_other_records_skipped(env):
    recs = [
        {"ts": _iso(NOW - 5), "kv_adaptive": "save", "slot": 0, "prefix": "a", "ok": True},
        {"ts": _iso(NOW - 5), "kv_adaptive": "restore", "slot": 0, "ok": False},
        {"ts": _iso(NOW - 5), "event": "kv_save", "slot": 0},
        {"ts": _iso(NOW - 5), "event": "something_else", "route": "x"},
        {"ts": _iso(NOW - 5), "event": "turn_failed"},  # no reason
        {"ts": _iso(NOW - 5), "unrelated": 1},
        _turn(5),
    ]
    _write(env / "metrics.jsonl", recs)
    dash.backfill_requests()
    assert [r["status"] for r in _rows()] == ["ok"]


def test_backfill_idempotent_across_rotated_files(env):
    _write(env / "metrics.jsonl", [_turn(10), _failed(20)])
    _write(env / "metrics.jsonl.1", [_turn(1000, request_id="a"), _turn(1100)])
    _write(env / "metrics.jsonl.2.gz", [_turn(2000, request_id="b"), _failed(2100)], gz=True)
    (env / "metrics.jsonl.3.gz").write_bytes(b"not gzip at all")  # corrupt: tolerated
    (env / "metrics.jsonl.bak").write_text(json.dumps(_turn(30)) + "\n")  # not a rotation
    with open(env / "metrics.jsonl.1", "a") as f:
        f.write("{malformed\n\n")
    dash.backfill_requests()
    assert len(_rows()) == 6
    dash.backfill_requests()
    assert len(_rows()) == 6
    # collector re-reading the live file afterwards adds nothing
    with dash._lock:
        dash._read_new_metrics()
    dash._flush_requests()
    assert len(_rows()) == 6


def test_backfill_ignores_records_older_than_retention(env):
    old = dash.RETENTION_DAYS * 86400 + 3600
    _write(env / "metrics.jsonl", [_turn(old), _turn(10)])
    dash.backfill_requests()
    assert len(_rows()) == 1


def test_retention_prunes_on_flush(env):
    con = dash._db()
    old = NOW - dash.RETENTION_DAYS * 86400 - 10
    for i, ts in enumerate((old, NOW - 10)):
        con.execute("INSERT INTO requests(key, ts, status) VALUES(?,?,'ok')", (f"k{i}", ts))
    con.commit()
    con.close()
    dash._flush_db()
    assert [r["key"] for r in _rows()] == ["k1"]


def _seed(env):
    recs = [
        _turn(100, ttft_s=0.5, request_id="a"),
        _turn(200, ttft_s=0.6, served="cold", cache_n=0, upstream_cache="miss"),
        _turn(300, ttft_s=9.0, route="cron", prompt_n=None, cache_n=None),
        _turn(400, ttft_s=0.7, route="cron"),
        _failed(500, route="cron"),
        _failed(600, reason="aborted_client"),
        _turn(10_000, ttft_s=0.8),  # outside a 1h window
    ]
    _write(env / "metrics.jsonl", recs)
    dash.backfill_requests()
    return recs


def test_endpoint_defaults_and_derived(env):
    _seed(env)
    code, out = dash.query_requests({})
    assert code == 200
    assert out["window_sec"] == 3600 and out["total"] == 6
    assert [r["status"] for r in out["rows"]] == ["ok", "ok", "ok", "ok", "failed", "failed"]
    first = out["rows"][0]
    assert first["new_tokens"] == 100 and first["cached_pct"] == 90.0
    assert out["rows"][2]["new_tokens"] is None and out["rows"][2]["cached_pct"] is None
    assert out["rows"][1]["new_tokens"] == 1000
    # no text-bearing fields
    assert not {"prompt", "reply", "text", "content", "messages"} & set(first)
    json.dumps(out)


def test_endpoint_total_matches_turn_records_in_window(env):
    recs = _seed(env)
    n = sum(1 for r in recs if NOW - dash._iso_epoch(r["ts"]) <= 3600)
    assert dash.query_requests({"window": ["3600"]})[1]["total"] == n
    assert dash.query_requests({"window": ["86400"]})[1]["total"] == len(recs)


def test_endpoint_paging(env):
    _seed(env)
    code, p1 = dash.query_requests({"limit": ["2"]})
    _, p2 = dash.query_requests({"limit": ["2"], "offset": ["2"]})
    _, p3 = dash.query_requests({"limit": ["2"], "offset": ["4"]})
    _, p4 = dash.query_requests({"limit": ["2"], "offset": ["6"]})
    assert p1["total"] == p2["total"] == 6
    ts = [r["ts"] for p in (p1, p2, p3) for r in p["rows"]]
    assert len(ts) == 6 and ts == sorted(ts, reverse=True) and len(set(ts)) == 6
    assert p4["rows"] == []


def test_endpoint_filters(env):
    _seed(env)
    _, miss = dash.query_requests({"filter": ["miss"]})
    assert miss["total"] == 1 and miss["rows"][0]["served"] == "cold"
    _, failed = dash.query_requests({"filter": ["failed"]})
    assert failed["total"] == 2 and {r["status"] for r in failed["rows"]} == {"failed"}
    # window ttft values: .5 .6 9.0 .7 -> p95 (nearest rank) is 9.0, nothing above it
    _, slow = dash.query_requests({"filter": ["slow"], "window": ["86400"]})
    # 5 values .5 .6 .7 .8 9.0 -> p95 index round(.95*4)=4 -> 9.0 -> none strictly above
    assert slow["total"] == 0
    for i in range(20):  # add a bulk of fast turns so the outlier exceeds p95
        _write(env / "metrics.jsonl.9", [_turn(50 + i, ttft_s=0.4, request_id=f"f{i}")])
    recs = [_turn(60 + i, ttft_s=0.4, request_id=f"g{i}") for i in range(30)]
    _write(env / "metrics.jsonl.9", recs + [_turn(70, ttft_s=9.0, request_id="slowone")])
    dash.backfill_requests()
    _, slow = dash.query_requests({"filter": ["slow"]})
    assert slow["total"] >= 1
    assert all(r["ttft_s"] >= 9.0 for r in slow["rows"])


def test_endpoint_route_filter(env):
    _seed(env)
    _, cron = dash.query_requests({"route": ["cron"]})
    assert cron["total"] == 3
    assert {r["route"] for r in cron["rows"]} == {"cron"}
    _, none = dash.query_requests({"route": ["nope"]})
    assert none["total"] == 0 and none["rows"] == []
    _, both = dash.query_requests({"route": ["cron"], "filter": ["failed"]})
    assert both["total"] == 1


def test_endpoint_parameter_validation(env):
    _seed(env)
    for q in (
        {"window": ["abc"]},
        {"offset": ["1.5"]},
        {"limit": ["x"]},
        {"filter": ["bogus"]},
        {"route": ["r" * 300]},
    ):
        code, out = dash.query_requests(q)
        assert code == 400 and "error" in out
    # out-of-range numbers are clamped, not rejected
    code, out = dash.query_requests({"limit": ["99999"], "offset": ["-5"], "window": ["1"]})
    assert code == 200 and out["window_sec"] == 300
    assert dash.query_requests({"limit": ["0"]})[1]["rows"] != []
    code, out = dash.query_requests({"limit": ["99999"]})
    assert code == 200 and len(out["rows"]) <= 200


def test_http_handler_returns_400_json(env):
    import threading
    import urllib.error
    import urllib.request
    from http.server import ThreadingHTTPServer

    _seed(env)
    srv = ThreadingHTTPServer(("127.0.0.1", 0), dash.Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    try:
        with urllib.request.urlopen(base + "/api/requests?limit=2") as r:
            body = json.load(r)
        assert body["total"] == 6 and len(body["rows"]) == 2
        with pytest.raises(urllib.error.HTTPError) as ei:
            urllib.request.urlopen(base + "/api/requests?filter=zzz")
        assert ei.value.code == 400
        assert "error" in json.load(ei.value)
        with pytest.raises(urllib.error.HTTPError) as ei:
            urllib.request.urlopen(base + "/api/history?window=1h")
        assert ei.value.code == 400
        assert "error" in json.load(ei.value)
    finally:
        srv.shutdown()
        srv.server_close()
