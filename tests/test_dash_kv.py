"""kv_events: ingestion of the proxy's kv_adaptive records, the llama.cpp Restore
card values, chart series, and in-place upgrades."""

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


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(dash, "DB_PATH", str(tmp_path / "rollup.db"))
    monkeypatch.setattr(dash, "METRICS_FILE", str(tmp_path / "metrics.jsonl"))
    monkeypatch.setattr(dash, "_offset", 0)
    dash._buckets.clear()
    dash._req_pending.clear()
    dash._kv_pending.clear()
    monkeypatch.setattr(time, "time", lambda: NOW)
    return tmp_path


def _kv(age, kind, ok=True, dur=0.5, **kw):
    d = {"ts": _iso(NOW - age), "kv_adaptive": kind, "slot": 0, "prefix": "abcd1234",
         "file": "abcd1234.bin", "ok": ok, "dur_s": dur}
    if kind == "restore":
        d["via_alias"] = False
    d.update(kw)
    return d


def _turn(age, rid, **kw):
    d = {"ts": _iso(NOW - age), "route": "r", "slot": 0, "prefix": "p", "prompt_n": 1000, "cache_n": 900,
         "completion_n": 10, "dur_s": 2.0, "ttft_s": 1.0, "decode_tps": 30.0, "served": "warm",
         "ok": True, "request_id": rid, "cacheram_restore": False, "restored_tokens": 0}
    d.update(kw)
    return d


def _write(path, recs, gz=False):
    text = "".join(json.dumps(r) + "\n" for r in recs)
    if gz:
        with gzip.open(path, "wt") as f:
            f.write(text)
    else:
        path.write_text(text)


def _count(table):
    con = dash._db()
    try:
        return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        con.close()


LLAMA = [
    # 3 ok restores (0.4, 0.6, 1.0 s), 1 miss, 3 ok saves (0.5 / 0.7 / 0.9 s), 1 failed save
    _kv(500, "restore", dur=0.4), _kv(400, "restore", dur=0.6), _kv(300, "restore", dur=1.0),
    _kv(200, "restore", ok=False, dur=0.05),
    _kv(450, "save", dur=0.5, bytes=1000), _kv(350, "save", dur=0.7, bytes=3000),
    _kv(250, "save", dur=0.9, bytes=2000), _kv(150, "save", ok=False, dur=0.1),
    _turn(500, "a", served="cold"),
    _turn(400, "b", cacheram_restore=True, restored_tokens=100),
    _turn(300, "c", cacheram_restore=True, restored_tokens=250),
    _turn(100, "d"),
]


def _load(env, recs):
    _write(env / "metrics.jsonl", recs)
    with dash._lock:
        dash._read_new_metrics()
    dash._flush_requests()


def test_kv_adaptive_ingested_to_kv_events_not_requests(env):
    _load(env, LLAMA)
    assert _count("kv_events") == 8 and _count("requests") == 4
    con = dash._db()
    row = con.execute("SELECT ts, kind, ok, dur_s, slot, prefix, bytes FROM kv_events ORDER BY ts LIMIT 1").fetchone()
    cols = [r[1] for r in con.execute("PRAGMA table_info(kv_events)")]
    assert row == (NOW - 500, "restore", 1, 0.4, 0, "abcd1234", None)
    assert "file" not in cols and "file" not in json.dumps(cols)
    # the same lines again (read from a rewound offset) stay one row each
    dash._offset = 0
    with dash._lock:
        dash._read_new_metrics()
    dash._flush_requests()
    assert _count("kv_events") == 8 and _count("requests") == 4


def test_backfill_is_idempotent_and_reads_rotated_gz(env):
    _write(env / "metrics.jsonl", LLAMA[:4] + LLAMA[8:10])
    _write(env / "metrics.jsonl.1.gz", LLAMA[4:8] + LLAMA[10:], gz=True)
    dash.backfill_requests()
    dash.backfill_requests()
    assert _count("kv_events") == 8 and _count("requests") == 4
    # ingesting live after the backfill does not duplicate either
    _load(env, LLAMA[:8] + LLAMA[8:])
    assert _count("kv_events") == 8 and _count("requests") == 4


def test_old_records_without_bytes_and_bad_records(env):
    old = _kv(60, "save")  # no bytes key (older proxy)
    bad = {"ts": "not-a-time", "kv_adaptive": "save", "ok": True}
    odd = {"ts": _iso(NOW - 5), "kv_adaptive": "evict"}
    _load(env, [old, bad, odd])
    con = dash._db()
    assert con.execute("SELECT bytes FROM kv_events").fetchall() == [(None,)]


def test_cacheram_columns_ingested_and_late_backfilled(env):
    t = _turn(100, "z", cacheram_restore=True, restored_tokens=77)
    _load(env, [t])
    con = dash._db()
    assert con.execute("SELECT cacheram_restore, restored_tokens FROM requests").fetchone() == (1, 77)
    con.close()


def test_llamacpp_window_stats_restore(env):
    _load(env, LLAMA)
    c = dash.window_stats(900, now=NOW)
    assert c["cache_source"]["kind"] == "llamacpp"
    r = c["restore"]
    assert r["source"] == "proxy"
    assert (r["p50"], r["n"], r["misses"]) == (600.0, 3, 1)
    assert r["p95"] == dash._r(dash._pctile([400.0, 600.0, 1000.0], 95), 1)
    assert (r["save_ms_p50"], r["saves"], r["save_failures"]) == (700.0, 3, 1)
    assert r["snapshot_bytes_p50"] == 2000
    assert (r["cacheram_restores"], r["cacheram_tokens"]) == (2, 350)
    # window excludes older events
    r = dash.window_stats(250, now=NOW)["restore"]
    assert (r["n"], r["misses"], r["saves"], r["save_failures"]) == (0, 1, 1, 1)
    assert r["p50"] is None


def test_snapshot_bytes_null_when_no_record_has_it(env):
    _load(env, [_kv(100, "save"), _turn(90, "a")])
    assert dash.window_stats(900, now=NOW)["restore"]["snapshot_bytes_p50"] is None


def test_gufo_restore_shape_unchanged(env):
    recs = [
        _turn(100, "g1", upstream_cache="disk", upstream_restore_ms=10.0, upstream_snapshot_ms=40.0,
              upstream_snapshot_bytes=1000),
        _turn(90, "g2", upstream_cache="memory", upstream_restore_ms=30.0, upstream_snapshot_ms=60.0,
              upstream_snapshot_bytes=3000),
        _kv(80, "restore", dur=9.0),  # present but irrelevant to a Gufo window
    ]
    _load(env, recs)
    c = dash.window_stats(900, now=NOW)
    assert c["cache_source"]["kind"] == "gufo"
    assert c["restore"] == {"source": "upstream", "p50": 20.0, "p95": 30.0, "n": 2,
                            "snapshot_ms_p50": 50.0, "snapshot_bytes_p50": 2000.0}


def test_mixed_window_prefers_most_recent_ok_turn(env):
    _load(env, [_turn(300, "g", upstream_cache="disk", upstream_restore_ms=10.0),
                _turn(100, "l"), _kv(50, "restore", dur=1.0)])
    assert dash.window_stats(900, now=NOW)["restore"]["source"] == "proxy"


def test_request_series_restore_from_kv_events(env):
    _load(env, LLAMA)
    pts = dash.request_series(900, now=NOW)
    ms = {p["t"]: (p["restore_ms_p50"], p["restore_ms_p95"]) for p in pts}
    w = dash.bucket_width(900)
    b = lambda age: int((NOW - age) // w) * w  # noqa: E731
    exp = {}
    for age, d in ((500, 400.0), (400, 600.0), (300, 1000.0)):
        exp.setdefault(b(age), []).append(d)
    for t, vals in exp.items():
        assert ms[t][0] == dash._r(dash._median(vals), 1)
    assert all(v == (None, None) for t, v in ms.items() if t not in exp)


def test_existing_db_upgrades_in_place(env):
    con = sqlite3.connect(dash.DB_PATH)
    cols = [c for c in dash.REQUEST_COLS if c not in ("cacheram_restore", "restored_tokens")]
    ddl = dash.REQUESTS_DDL.replace(", cacheram_restore INTEGER", "").replace(", restored_tokens INTEGER", "")
    con.execute(ddl)
    con.execute("INSERT INTO requests(key, ts, status) VALUES(?, ?, 'ok')", (f"ok:old:{int(NOW - 100)}", NOW - 100))
    con.commit()
    con.close()
    assert "cacheram_restore" not in cols
    c = dash._db()
    have = {r[1] for r in c.execute("PRAGMA table_info(requests)")}
    assert {"cacheram_restore", "restored_tokens"} <= have
    assert c.execute("SELECT COUNT(*) FROM kv_events").fetchone()[0] == 0
    c.close()
    # an interim kv_events table without bytes also gains the column
    con = sqlite3.connect(dash.DB_PATH)
    con.execute("DROP TABLE kv_events")
    con.execute("CREATE TABLE kv_events(id INTEGER PRIMARY KEY, key TEXT UNIQUE, ts REAL, kind TEXT, ok INTEGER, dur_s REAL, slot INTEGER, prefix TEXT)")
    con.commit()
    con.close()
    c = dash._db()
    assert "bytes" in {r[1] for r in c.execute("PRAGMA table_info(kv_events)")}
    c.close()
    # backfill fills the late columns on the existing row
    _write(env / "metrics.jsonl", [_turn(100, "old", cacheram_restore=True, restored_tokens=5)])
    dash.backfill_requests()
    c = dash._db()
    assert c.execute("SELECT cacheram_restore, restored_tokens FROM requests WHERE key=?",
                     (f"ok:old:{int(NOW - 100)}",)).fetchone() == (1, 5)


def test_retention_prunes_kv_events(env):
    _write(env / "metrics.jsonl", [_kv(10, "save"), _kv((dash.RETENTION_DAYS + 1) * 86400, "save")])
    dash.backfill_requests()
    assert _count("kv_events") == 1
