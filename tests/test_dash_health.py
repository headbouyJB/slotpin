"""Tests for slotpin-dash health: stall state machine, events detail, health_v2."""

from __future__ import annotations

import json
import sqlite3
import time
from datetime import UTC, datetime

import pytest

import slotpin.dash as dash

NOW = 1_800_000_000.0
LIMIT = 300.0


def _slot(sid=0, busy=True, proc=0, dec=0):
    return {
        "id": sid,
        "is_processing": busy,
        "n_prompt_tokens_processed": proc,
        "n_decoded": dec,
    }


def _iso(epoch):
    return datetime.fromtimestamp(epoch, UTC).isoformat(timespec="seconds")


# ---------------- pure state machine ----------------
def test_idle_is_up():
    t = dash.StallTracker(LIMIT)
    for i in range(5):
        r = t.update(NOW + i * 1000, [_slot(busy=False)], None)
        assert r["state"] == "up"


def test_busy_and_progressing_is_up():
    t = dash.StallTracker(LIMIT)
    for i in range(0, 1000, 10):
        r = t.update(NOW + i, [_slot(proc=100, dec=i)], None)
        assert r["state"] == "up"


def test_frozen_past_limit_is_stalled():
    t = dash.StallTracker(LIMIT)
    assert t.update(NOW, [_slot(proc=50, dec=3)], None)["state"] == "up"
    assert t.update(NOW + LIMIT - 1, [_slot(proc=50, dec=3)], None)["state"] == "up"
    r = t.update(NOW + LIMIT, [_slot(proc=50, dec=3)], None)
    assert r == {"state": "stalled", "stalled_s": LIMIT, "slot": 0}


def test_frozen_but_turn_completed_is_not_stalled():
    t = dash.StallTracker(LIMIT)
    t.update(NOW, [_slot(proc=50, dec=3)], None)
    r = t.update(NOW + 400, [_slot(proc=50, dec=3)], NOW + 200)
    assert r["state"] == "up"
    # the completion ages out while still frozen -> stalled
    assert t.update(NOW + 600, [_slot(proc=50, dec=3)], NOW + 200)["state"] == "stalled"


def test_stalled_then_tuple_changes_clears():
    t = dash.StallTracker(LIMIT)
    t.update(NOW, [_slot(proc=50, dec=3)], None)
    assert t.update(NOW + 350, [_slot(proc=50, dec=3)], None)["state"] == "stalled"
    assert t.update(NOW + 360, [_slot(proc=50, dec=4)], None)["state"] == "up"


def test_stalled_then_idle_clears():
    t = dash.StallTracker(LIMIT)
    t.update(NOW, [_slot(proc=50, dec=3)], None)
    assert t.update(NOW + 350, [_slot(proc=50, dec=3)], None)["state"] == "stalled"
    assert t.update(NOW + 360, [_slot(busy=False)], None)["state"] == "up"
    # busy again: the freeze clock restarts
    assert t.update(NOW + 370, [_slot(proc=50, dec=3)], None)["state"] == "up"


def test_unreachable_is_down_never_stalled():
    t = dash.StallTracker(LIMIT)
    t.update(NOW, [_slot(proc=50, dec=3)], None)
    assert t.update(NOW + 350, [_slot(proc=50, dec=3)], None)["state"] == "stalled"
    r = t.update(NOW + 360, None, None)
    assert r["state"] == "down" and r["stalled_s"] is None
    # recovery restarts tracking
    assert t.update(NOW + 370, [_slot(proc=50, dec=3)], None)["state"] == "up"
    # never-stalled path too
    assert dash.StallTracker(LIMIT).update(NOW, None, None)["state"] == "down"


def test_two_slots_only_one_frozen():
    t = dash.StallTracker(LIMIT)
    r = None
    for i in range(0, 400, 10):
        r = t.update(NOW + i, [_slot(0, proc=50, dec=3), _slot(1, proc=10, dec=i)], None)
    assert r["state"] == "stalled" and r["slot"] == 0
    snap = t.snapshot(NOW + 390)
    assert snap[0][1] == 390.0 and snap[1][1] == 0.0


def test_default_limit_comes_from_setting(monkeypatch):
    monkeypatch.setattr(dash, "STALL_S", 10.0)
    t = dash.StallTracker()
    t.update(NOW, [_slot()], None)
    assert t.update(NOW + 10, [_slot()], None)["state"] == "stalled"


# ---------------- events ----------------
@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(dash, "DB_PATH", str(tmp_path / "rollup.db"))
    monkeypatch.setattr(dash, "METRICS_FILE", str(tmp_path / "metrics.jsonl"))
    monkeypatch.setattr(dash, "STALL_S", LIMIT)
    monkeypatch.setattr(dash, "_offset", 0)
    monkeypatch.setattr(dash, "_stall", dash.StallTracker())
    monkeypatch.setattr(dash, "_stall_info", None)
    monkeypatch.setattr(dash, "_last_completion_ts", None)
    monkeypatch.setattr(dash, "_restart", dash.RestartDetector())
    dash._buckets.clear()
    dash._events.clear()
    dash._req_pending.clear()
    monkeypatch.setattr(dash, "_live", dict(dash._live))
    monkeypatch.setattr(time, "time", lambda: NOW + 500)
    yield tmp_path
    dash._events.clear()
    dash._buckets.clear()


def _kinds():
    return [k for _t, k, _d in dash._events]


def test_one_stall_and_one_cleared_per_episode(env):
    frozen = [_slot(proc=5, dec=2)]
    for i in range(0, 700, 10):
        dash._stall_step(NOW + i, True, frozen, None)
    assert _kinds() == ["stall"]
    dash._stall_step(NOW + 710, True, [_slot(proc=5, dec=3)], None)
    dash._stall_step(NOW + 720, True, [_slot(proc=5, dec=3)], None)
    assert _kinds() == ["stall", "stall_cleared"]
    # second episode
    for i in range(730, 1400, 10):
        dash._stall_step(NOW + i, True, [_slot(proc=5, dec=3)], None)
    dash._stall_step(NOW + 1410, True, [_slot(busy=False)], None)
    assert _kinds() == ["stall", "stall_cleared", "stall", "stall_cleared"]
    d = json.loads(dash._events[0][2])
    assert d["slot"] == 0 and d["stalled_s"] == LIMIT


def test_down_while_stalled_closes_episode_without_new_stall(env):
    for i in range(0, 400, 10):
        dash._stall_step(NOW + i, True, [_slot(proc=5, dec=2)], None)
    dash._stall_step(NOW + 410, False, None, None)
    dash._stall_step(NOW + 420, False, None, None)
    assert _kinds() == ["stall", "stall_cleared"]
    assert json.loads(dash._events[1][2])["reason"] == "down"


def test_completion_clears_stall(env):
    for i in range(0, 400, 10):
        dash._stall_step(NOW + i, True, [_slot(proc=5, dec=2)], None)
    dash._stall_step(NOW + 410, True, [_slot(proc=5, dec=2)], NOW + 405)
    assert _kinds() == ["stall", "stall_cleared"]


def test_events_detail_migration_and_roundtrip(env):
    con = sqlite3.connect(dash.DB_PATH)
    con.execute("CREATE TABLE events(ts REAL, kind TEXT)")
    con.execute("INSERT INTO events VALUES(?,?)", (NOW, "stall_cleared"))
    con.commit()
    con.close()
    dash._load_from_db()  # _db() migrates in place
    assert dash._events == [(NOW, "stall_cleared", None)]
    cols = [r[1] for r in sqlite3.connect(dash.DB_PATH).execute("PRAGMA table_info(events)")]
    assert "detail" in cols
    dash._events.append((NOW + 1, "stall", '{"slot": 0, "stalled_s": 300}'))
    dash._flush_db()
    dash._flush_db()  # delete-and-rewrite stays idempotent
    rows = sqlite3.connect(dash.DB_PATH).execute("SELECT ts,kind,detail FROM events").fetchall()
    assert rows == [(NOW, "stall_cleared", None), (NOW + 1, "stall", '{"slot": 0, "stalled_s": 300}')]


# ---------------- API ----------------
def test_health_v2_and_slot_fields(env, monkeypatch):
    now = NOW + 500
    raw = [
        {
            "id": 0,
            "is_processing": True,
            "prompt": "SECRET PROMPT TEXT",
            "n_prompt_tokens_processed": 7,
            "next_token": [{"n_decoded": 0, "n_remain": -1}],
        },
        {
            "id": 1,
            "is_processing": True,
            "prompt": "SECRET PROMPT TEXT",
            "next_token": [{"n_decoded": 12}],
        },
        {"id": 2, "is_processing": False, "prompt": "SECRET PROMPT TEXT"},
    ]
    slots = [dash._slot_fields(s) for s in raw]
    dash._live.update(slots=slots, slots_ok=True, poll_ts=now)
    for t in range(0, 500, 10):
        dash._stall_step(NOW + t, True, slots, None)
    dash._events.extend([(NOW - 5000, "up", None), (NOW - 100, "down", None)])
    dash._events.append((NOW + 100, "up", None))
    st = dash.build_state()
    h = st["health_v2"]
    assert h["upstream"] == "stalled" and h["stalled_s"] == 490.0
    assert h["busy_slots"] == 2 and h["busy_for_s"] == 500.0
    assert h["uptime_s"] == 400.0 and h["last_restart_ts"] == NOW + 100
    assert h["last_completion_ts"] is None and h["last_completion_age_s"] is None
    assert "config_id" in h
    by = {s["id"]: s for s in st["slots"]}
    assert by[0]["phase"] == "prefill" and by[0]["n_processed"] == 7 and by[0]["n_decoded"] == 0
    assert by[1]["phase"] == "generating" and by[1]["n_decoded"] == 12
    assert by[1]["n_processed"] is None
    assert by[2]["phase"] == "idle" and by[2]["busy_for_s"] is None and by[2]["stall_s"] is None
    assert by[0]["busy_for_s"] == 500.0 and by[0]["stall_s"] == 500.0
    assert "health" in st and "proxy_up" in st["health"]  # legacy object untouched
    assert "SECRET" not in json.dumps(st)


def test_health_v2_last_completion_from_table(env):
    rec = {"ts": _iso(NOW + 400), "route": "r", "served": "warm", "request_id": "x1", "ok": True}
    with open(dash.METRICS_FILE, "w") as f:
        f.write(json.dumps(rec) + "\n")
    with dash._lock:
        dash._read_new_metrics()
    dash._flush_requests()
    dash._live.update(slots=[], slots_ok=True, poll_ts=NOW)
    h = dash.build_state()["health_v2"]
    assert h["last_completion_ts"] == NOW + 400 and h["last_completion_age_s"] == 100.0
    assert h["upstream"] == "up" and h["stalled_s"] is None and h["busy_for_s"] is None


def test_history_summary_fields(env):
    now = NOW + 500
    con = dash._db()
    rows = [
        ("a", now - 50, "ok", None),
        ("b", now - 40, "failed", "timeout"),
        ("c", now - 30, "failed", "timeout"),
        ("d", now - 20, "failed", "shed"),
        ("e", now - 10, "ok", None),
        ("f", now - 99999, "failed", "unreachable"),  # outside window
    ]
    for k, ts, status, reason in rows:
        con.execute(
            "INSERT INTO requests(key, ts, status, fail_reason) VALUES(?,?,?,?)",
            (k, ts, status, reason),
        )
    con.commit()
    con.close()
    dash._events.extend(
        [
            (now - 60, "down", None),
            (now - 50, "up", None),
            (now - 40, "stall", '{"slot": 0, "stalled_s": 300}'),
            (now - 30, "stall_cleared", '{"slot": 0, "stalled_s": 310}'),
            (now - 99999, "up", None),
        ]
    )
    h = dash.build_history(3600)
    s = h["summary"]
    assert s["failed"] == 3 and s["failed_pct"] == 60.0 and s["top_fail_reason"] == "timeout"
    assert s["restarts"] == 1 and s["stalls"] == 1
    assert "reqs" in s and "decode_p95" in s  # existing keys intact
    ev = h["events"]
    assert [e["kind"] for e in ev] == ["down", "up", "stall", "stall_cleared"]
    assert set(ev[0]) == {"t", "kind"}
    assert json.loads(ev[2]["detail"]) == {"slot": 0, "stalled_s": 300}


def test_history_summary_empty(env):
    s = dash.build_history(3600)["summary"]
    assert s["failed"] == 0 and s["failed_pct"] is None and s["top_fail_reason"] is None
    assert s["restarts"] == 0 and s["stalls"] == 0


# ---------------- no prompt text ----------------
def test_slot_fields_drop_prompt_text(env):
    raw = {"id": 3, "is_processing": True, "prompt": "TOP SECRET", "n_ctx": 100}
    f = dash._slot_fields(raw)
    assert "prompt" not in f and "TOP SECRET" not in json.dumps(f)
    assert f["n_prompt_tokens_processed"] is None and f["n_decoded"] is None


def test_collector_path_never_stores_or_serves_prompt(env, monkeypatch, capsys):
    payload = [
        {
            "id": 0,
            "is_processing": True,
            "prompt": "TOP SECRET PROMPT",
            "n_prompt_tokens_processed": 4,
            "next_token": [{"n_decoded": 2, "text": "TOP SECRET REPLY"}],
        }
    ]

    class Stop(Exception):
        pass

    def fake_get(url, timeout=6):
        return payload

    def stop_sleep(_s):
        raise Stop

    monkeypatch.setattr(dash, "_get_json", fake_get)
    monkeypatch.setattr(
        dash.urllib.request, "urlopen", lambda *a, **k: (_ for _ in ()).throw(OSError())
    )
    monkeypatch.setattr(dash, "read_meminfo", lambda: None)
    monkeypatch.setattr(dash, "read_temps", lambda: {})
    monkeypatch.setattr(dash, "_tcp_up", lambda *a, **k: True)
    monkeypatch.setattr(dash.time, "sleep", stop_sleep)
    with pytest.raises(Stop):
        dash.collector()
    assert "TOP SECRET" not in json.dumps(dash._live["slots"])
    assert dash._live["slots"][0]["n_decoded"] == 2
    dash._flush_db()
    # not in the database file, the state or history endpoints
    raw = open(dash.DB_PATH, "rb").read()
    assert b"TOP SECRET" not in raw
    assert "TOP SECRET" not in json.dumps(dash.build_state())
    assert "TOP SECRET" not in json.dumps(dash.build_history(3600))
    out = capsys.readouterr()
    assert "TOP SECRET" not in out.out + out.err


# ---------------- restart detection ----------------
def _poll(det, ok=True, cfg="m1", build=None, ctr=None):
    return [(k, d) for k, d in det.observe(ok, cfg, build, ctr)]


def test_observed_down_then_up_records_both_and_nothing_on_first_poll():
    d = dash.RestartDetector()
    assert _poll(d) == []  # nothing known yet: no event, no invented restart
    assert _poll(d) == []
    assert _poll(d, ok=False) == [("down", None)]
    assert _poll(d, ok=False) == []
    assert _poll(d) == [("up", None)]
    assert _poll(d) == []


def test_dashboard_restart_alone_creates_no_event():
    old = dash.RestartDetector()
    _poll(old, ctr={"tokens_predicted_total": 500.0})
    new = dash.RestartDetector()
    new.load(old.dump())  # the new dashboard process picks up the persisted state
    assert _poll(new, ctr={"tokens_predicted_total": 500.0}) == []
    assert _poll(new, ctr={"tokens_predicted_total": 620.0}) == []
    # /metrics disabled on this upstream: still nothing invented
    third = dash.RestartDetector()
    third.load(new.dump())
    assert _poll(third, ctr={}) == []
    # a fresh database has no state: unknown, so no event either
    assert _poll(dash.RestartDetector()) == []


def test_upstream_restarted_while_dashboard_was_off_counter_reset():
    old = dash.RestartDetector()
    _poll(old, ctr={"prompt_tokens_total": 9000.0, "tokens_predicted_total": 700.0})
    new = dash.RestartDetector()
    new.load(old.dump())
    out = _poll(new, ctr={"prompt_tokens_total": 40.0, "tokens_predicted_total": 3.0})
    assert out == [("up", {"reason": "counter_reset"})]
    assert _poll(new, ctr={"prompt_tokens_total": 80.0, "tokens_predicted_total": 9.0}) == []


def test_restart_between_polls_with_no_down_seen_is_counter_reset():
    d = dash.RestartDetector()
    _poll(d, ctr={"tokens_predicted_total": 700.0})
    assert _poll(d, ctr={"tokens_predicted_total": 5.0}) == [("up", {"reason": "counter_reset"})]


def test_config_change_only_counts_for_the_first_poll_after_dashboard_start():
    old = dash.RestartDetector()
    _poll(old, cfg="m1", build="b1")
    new = dash.RestartDetector()
    new.load(old.dump())
    assert _poll(new, cfg="m2", build="b2") == [("up", {"reason": "config_changed+build_changed"})]
    # in one process a config edit alone is not a restart (the down/up that follows is)
    assert _poll(new, cfg="m3", build="b3") == []


def test_down_while_dashboard_off_and_dashboard_started_during_restart():
    # persisted down, upstream reachable now: the earlier down was recorded; record the up
    old = dash.RestartDetector()
    _poll(old)
    _poll(old, ok=False)
    new = dash.RestartDetector()
    new.load(old.dump())
    assert _poll(new) == [("up", None)]
    # dashboard and upstream restart together: persisted up, first poll unreachable
    old = dash.RestartDetector()
    _poll(old, ctr={"tokens_predicted_total": 700.0})
    new = dash.RestartDetector()
    new.load(old.dump())
    assert _poll(new, ok=False) == [("down", None)]
    assert _poll(new, ctr={"tokens_predicted_total": 1.0}) == [("up", None)]
    assert _poll(new, ctr={"tokens_predicted_total": 9.0}) == []  # baseline restarted, no echo


def test_counters_cleared_on_down_so_no_second_up_after_reconnect():
    d = dash.RestartDetector()
    _poll(d, ctr={"tokens_predicted_total": 700.0})
    _poll(d, ok=False)
    assert _poll(d, ctr={}) == [("up", None)]  # /metrics unreadable on the first poll back
    assert _poll(d, ctr={"tokens_predicted_total": 12.0}) == []


def test_metrics_text_parse():
    txt = (
        "# HELP llamacpp:prompt_tokens_total x\n"
        "llamacpp:prompt_tokens_total 1234\nllamacpp:tokens_predicted_total 56.5\n"
        "llamacpp:prompt_tokens_seconds 9\n"
    )
    assert dash._parse_counters(txt) == {"prompt_tokens_total": 1234.0, "tokens_predicted_total": 56.5}
    assert dash._parse_counters("garbage") == {}


def test_restart_events_persist_across_a_dashboard_restart(env):
    with dash._lock:
        dash._restart_step(NOW, True, {"tokens_predicted_total": 100.0}, "m1")
        assert not dash._restart_step(NOW + 3, True, {"tokens_predicted_total": 110.0}, "m1")
        assert dash._restart_step(NOW + 6, False, {}, "m1")  # down
    dash._flush_events()  # as the collector does right after a new event
    # the dashboard process restarts: memory is gone, the database is not
    dash._events.clear()
    dash._restart = dash.RestartDetector()
    dash._load_from_db()
    assert [k for _t, k, _d in dash._events] == ["down"]
    with dash._lock:
        added = dash._restart_step(NOW + 60, True, {"tokens_predicted_total": 4.0}, "m1")
    assert added and [k for _t, k, _d in dash._events] == ["down", "up"]
    assert dash._events[-1][0] == NOW + 60 and dash._events[-1][2] is None
    # and a restart of the dashboard alone, upstream untouched, adds nothing
    dash._flush_events()
    dash._events.clear()
    dash._restart = dash.RestartDetector()
    dash._load_from_db()
    n = len(dash._events)
    with dash._lock:
        assert not dash._restart_step(NOW + 90, True, {"tokens_predicted_total": 30.0}, "m1")
    assert len(dash._events) == n == 2


def test_last_restart_ts_from_detected_up_and_unknown_when_none(env):
    dash._live.update(slots=[], slots_ok=True, poll_ts=NOW)
    h = dash.build_state()["health_v2"]
    assert h["last_restart_ts"] is None and h["uptime_s"] is None  # unknown, not wrong
    with dash._lock:
        dash._restart_step(NOW + 100, True, {"tokens_predicted_total": 900.0}, "m1")
        dash._restart_step(NOW + 200, True, {"tokens_predicted_total": 5.0}, "m1")
    h = dash.build_state()["health_v2"]  # time.time() is NOW + 500 in this fixture
    assert h["last_restart_ts"] == NOW + 200 and h["uptime_s"] == 300.0


# ---------------- GPU memory now ----------------
def test_gtt_now_prefers_live_sample_over_last_request_record(env):
    rec = {"ts": _iso(NOW + 400), "route": "r", "served": "warm", "ok": True, "gtt_used_mb": 9000}
    (env / "metrics.jsonl").write_text(json.dumps(rec) + "\n")
    dash._live.update(slots=[], slots_ok=False, poll_ts=NOW, mem={"mem_free_mb": 1, "gtt_used_mb": 5000})
    assert dash.build_state()["system"]["gtt_used_mb"] == 5000  # upstream down, sample is live
    dash._live.update(mem={"mem_free_mb": 1, "gtt_used_mb": 0})
    assert dash.build_state()["system"]["gtt_used_mb"] == 0  # a real zero is still a sample
    dash._live.update(mem={"mem_free_mb": 1, "gtt_used_mb": None})
    assert dash.build_state()["system"]["gtt_used_mb"] == 9000  # no sample: request-derived
    dash._live.update(mem=None)
    assert dash.build_state()["system"]["gtt_used_mb"] == 9000
