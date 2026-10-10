"""window_stats: every card value for a window, from the requests table."""

from __future__ import annotations

import json
import sqlite3

import pytest

import slotpin.dash as dash

NOW = 1_800_000_000.0


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(dash, "DB_PATH", str(tmp_path / "rollup.db"))
    monkeypatch.setattr(dash, "DRAFTER", "")
    monkeypatch.setattr(dash, "read_config_id", lambda: "plain")
    monkeypatch.setattr(dash, "_live", dict(dash._live, slots=[{"id": 0}]))
    return tmp_path


def _add(rows):
    con = dash._db()
    with con:
        for i, r in enumerate(rows):
            d = dict.fromkeys(dash.REQUEST_COLS)
            d.update(status="ok", ts=NOW - 60 - i)
            d.update(r)
            con.execute(
                dash._REQ_INSERT,
                (f"k{i}-{d['ts']}-{d['status']}", *[d[c] for c in dash.REQUEST_COLS]),
            )
    con.close()


def _slots(n):
    dash._live["slots"] = [{"id": i} for i in range(n)]


def test_empty_window_is_nulls_not_errors(env):
    s = dash.window_stats(900, NOW)
    assert s["decode"] == {"p50": None, "p95": None, "n": 0}
    assert s["requests"] == {"count": None, "per_hour": None}
    assert s["failures"]["count"] is None and s["failures"]["pct"] is None
    assert s["cache_hit"]["pct"] is None and s["cache_source"]["kind"] is None
    assert s["energy"]["wh_per_request"] is None and s["draft"]["pct"] is None
    assert s["concurrency"] is None  # single slot


def test_decode_ttft_medians_and_p95(env):
    _add(
        [
            {"decode_tps": float(v), "ttft_s": v / 10, "queue_wait_ms": float(v)}
            for v in range(1, 21)
        ]
    )
    s = dash.window_stats(3600, NOW)
    assert (s["decode"]["p50"], s["decode"]["p95"], s["decode"]["n"]) == (10.5, 19.0, 20)
    assert (s["ttft"]["p50"], s["ttft"]["p95"]) == (1.05, 1.9)
    assert s["ttft"]["queue_wait_ms_p50"] == 10.5
    assert s["requests"] == {"count": 20, "per_hour": 20.0}


def test_odd_count_median_and_failed_rows_excluded_from_measures(env):
    _add([{"decode_tps": v} for v in (10.0, 30.0, 20.0)])
    _add([{"status": "failed", "fail_reason": "timeout", "ts": NOW - 5, "decode_tps": 999.0}])
    s = dash.window_stats(900, NOW)
    assert s["decode"]["p50"] == 20.0 and s["decode"]["n"] == 3
    assert s["requests"]["count"] == 4
    assert s["failures"] == {"count": 1, "pct": 25.0, "top_reason": "timeout"}


def test_prefill_needs_2000_processed_tokens(env):
    _add(
        [
            {"prefill_tps": 100.0, "prompt_n": 2500, "cache_n": 500},  # 2000: counts
            {"prefill_tps": 300.0, "prompt_n": 5000, "cache_n": 0},  # counts
            {"prefill_tps": 9000.0, "prompt_n": 2500, "cache_n": 501},  # 1999: no
            {"prefill_tps": 9000.0, "prompt_n": 100, "cache_n": 0},  # no
            {"prefill_tps": 9000.0, "prompt_n": None, "cache_n": None},  # no
        ]
    )
    p = dash.window_stats(900, NOW)["prefill"]
    assert p == {"p50": 200.0, "p95": 300.0, "n": 2}


def test_energy_prefers_fair_falls_back_per_row(env):
    _add(
        [
            {"energy_fair_wh": 1.0, "energy_turn_wh": 4.0, "completion_n": 1000},
            {"energy_fair_wh": None, "energy_turn_wh": 3.0, "completion_n": 1000},
            {"energy_fair_wh": None, "energy_turn_wh": None, "completion_n": 5000},  # no energy
        ]
    )
    e = dash.window_stats(900, NOW)["energy"]
    assert e == {"wh_per_request": 2.0, "wh_per_1k_tokens": 2.0, "n": 2}


def test_cache_hit_totals(env):
    _add([{"prompt_n": 1000, "cache_n": 900}, {"prompt_n": 1000, "cache_n": 100}])
    assert dash.window_stats(900, NOW)["cache_hit"] == {
        "pct": 50.0,
        "cached_tokens": 1000,
        "prompt_tokens": 2000,
    }


def test_cache_source_gufo_shape(env):
    _add(
        [
            {"upstream_cache": "memory", "served": "warm"},
            {"upstream_cache": "disk", "served": "warm"},
            {
                "upstream_cache": "miss",
                "served": "cold",
                "upstream_cache_miss_reason": "prefix_changed",
            },
            {
                "upstream_cache": "miss",
                "served": "cold",
                "upstream_cache_miss_reason": "prefix_changed",
            },
            {"upstream_cache": "miss", "served": "cold", "upstream_cache_miss_reason": "evicted"},
        ]
    )
    c = dash.window_stats(900, NOW)["cache_source"]
    assert (c["kind"], c["memory"], c["disk"], c["miss"], c["n"]) == ("gufo", 1, 1, 3, 5)
    assert c["miss_pct"] == 60.0 and c["top_miss_reason"] == "prefix_changed"


def test_cache_source_llamacpp_shape(env):
    _add([{"served": "warm"}, {"served": "warm"}, {"served": "cold"}, {"served": None}])
    c = dash.window_stats(900, NOW)["cache_source"]
    assert (c["kind"], c["warm"], c["cold"], c["memory"], c["disk"]) == (
        "llamacpp",
        2,
        1,
        None,
        None,
    )
    assert c["miss_pct"] == pytest.approx(33.3)


def test_restore_and_snapshot(env):
    _add(
        [
            {
                "upstream_restore_ms": 100.0,
                "upstream_snapshot_ms": 10.0,
                "upstream_snapshot_bytes": 1000,
            },
            {
                "upstream_restore_ms": 300.0,
                "upstream_snapshot_ms": 30.0,
                "upstream_snapshot_bytes": 3000,
            },
        ]
    )
    r = dash.window_stats(900, NOW)["restore"]
    assert (r["p50"], r["p95"], r["snapshot_ms_p50"], r["snapshot_bytes_p50"]) == (
        200.0,
        300.0,
        20.0,
        2000.0,
    )


def test_draft_counts_and_label_rules(env, monkeypatch):
    _add(
        [
            {"draft_n": 100, "draft_n_accepted": 80},
            {"draft_n": 100, "draft_n_accepted": 60},
            {"draft_n": 0},
        ]
    )
    d = dash.window_stats(900, NOW)["draft"]
    assert (d["accepted"], d["proposed"], d["pct"], d["drafter"]) == (140, 200, 70.0, "Draft")
    for cid, want in [("qwen-MTP-q4", "MTP"), ("x-DFlash2", "DFlash2"), ("plain", "Draft")]:
        monkeypatch.setattr(dash, "read_config_id", lambda cid=cid: cid)
        assert dash.drafter_label() == want
    monkeypatch.setattr(dash, "read_config_id", lambda: "mtp")
    monkeypatch.setattr(dash, "DRAFTER", "Eagle")
    assert dash.window_stats(900, NOW)["draft"]["drafter"] == "Eagle"


def test_concurrency_only_on_multi_slot_boxes(env):
    _add([{"concurrency": 1.0, "decode_tps": 40.0}, {"concurrency": 3.0, "decode_tps": 20.0}])
    assert dash.window_stats(900, NOW)["concurrency"] is None
    _slots(2)
    c = dash.window_stats(900, NOW)["concurrency"]
    assert c == {"mean": 2.0, "peak": 3.0, "agg_decode_tps": 50.0, "n": 2}
    # idle multi-slot box: card still present, values null
    assert dash.window_stats(900, NOW + 100_000)["concurrency"]["mean"] is None


def test_same_keys_for_all_windows_and_live_matches(env):
    _add([{"decode_tps": 30.0}])
    _add([{"decode_tps": 50.0, "ts": NOW - 5000}])
    a, b = dash.window_stats(900, NOW), dash.window_stats(86400, NOW)

    def shape(x):
        return {k: shape(v) if isinstance(v, dict) else None for k, v in x.items()}

    assert shape(a) == shape(b)
    assert a["decode"]["n"] == 1 and b["decode"]["n"] == 2
    assert dash.build_state()["cards"].keys() == a.keys()
    assert dash.build_history(900)["cards"].keys() == a.keys()


def test_concurrency_column_added_and_backfilled_in_place(env, monkeypatch):
    con = sqlite3.connect(dash.DB_PATH)
    old_cols = [c for c in dash.REQUEST_COLS if c != "concurrency"]
    ddl = dash.REQUESTS_DDL.replace(", concurrency REAL", "")
    con.execute(ddl)
    con.execute("INSERT INTO requests(key, ts, status) VALUES(?, ?, 'ok')", (f"ok:r1:{int(NOW - 10)}", NOW - 10))
    con.commit()
    con.close()
    assert "concurrency" not in old_cols
    c = dash._db()  # opens, ALTERs
    assert "concurrency" in {r[1] for r in c.execute("PRAGMA table_info(requests)")}
    c.close()
    metrics = env / "metrics.jsonl"
    from datetime import UTC, datetime

    rec = {
        "ts": datetime.fromtimestamp(NOW - 10, UTC).isoformat(),
        "route": "r",
        "request_id": "r1",
        "concurrency": 2.0,
    }
    metrics.write_text(json.dumps(rec) + "\n")
    monkeypatch.setattr(dash, "METRICS_FILE", str(metrics))
    monkeypatch.setattr(dash.time, "time", lambda: NOW)
    dash.backfill_requests()
    c = dash._db()
    assert c.execute("SELECT concurrency FROM requests WHERE key=?", (f"ok:r1:{int(NOW - 10)}",)).fetchone()[0] == 2.0
    c.close()


# ---------------- configured slot count ----------------
def _cfg(env, monkeypatch, text):
    f = env / "config.toml"
    f.write_text(text)
    monkeypatch.setattr(dash, "SLOTPIN_CFG", str(f))


def test_concurrency_card_follows_configured_slots_not_upstream_state(env, monkeypatch):
    _add([{"concurrency": 2.0, "decode_tps": 30.0}])
    _cfg(env, monkeypatch, 'upstream_config_id = "x"\nslots = 4  # llama-server -np 4\n')
    # upstream unreachable: the collector never saw (or lost) a /slots list
    dash._live.update(slots=None, slots_ok=False)
    c = dash.window_stats(900, NOW)["concurrency"]
    assert c is not None and c["mean"] == 2.0
    dash._live.update(slots=[])
    assert dash.window_stats(900, NOW)["concurrency"] is not None
    # same through the live state and the history API
    assert dash.build_state()["cards"]["concurrency"] is not None
    assert dash.build_history(3600)["cards"]["concurrency"] is not None


def test_config_slots_one_overrides_a_longer_live_list_and_fallbacks(env, monkeypatch):
    _slots(3)
    _cfg(env, monkeypatch, "slots = 1\n")
    assert dash.window_stats(900, NOW)["concurrency"] is None
    # no usable value in the file: the live /slots length is the fallback
    _cfg(env, monkeypatch, 'upstream_config_id = "x"\n')
    assert dash.read_config_slots() is None
    assert dash.window_stats(900, NOW)["concurrency"] is not None
    _cfg(env, monkeypatch, "slots = 0\n")
    assert dash.read_config_slots() is None
    monkeypatch.setattr(dash, "SLOTPIN_CFG", str(env / "missing.toml"))
    assert dash.read_config_slots() is None
    assert dash.window_stats(900, NOW)["concurrency"] is not None
    # unparsable TOML elsewhere in the file still yields the value (line scan)
    _cfg(env, monkeypatch, "slots = 2\n[broken\n")
    assert dash.read_config_slots() == 2
