"""Tests for slotpin-dash's 1-minute samples table, series merge and API keys."""

from __future__ import annotations

import json
import sqlite3
import time

import pytest

import slotpin.dash as dash

NOW = 1_800_000_000.0  # divisible by 60, 300 and 1800

OLD_HISTORY_KEYS = {"bucket_sec", "config_id", "events", "now", "series", "summary", "window_sec"}
OLD_SUMMARY_KEYS = {
    "avg_cpu_temp_c", "avg_decode_tps", "avg_gpu_temp_c", "avg_power_w", "cache_hit_pct",
    "cache_tokens", "cold", "completion_tokens", "decode_p50", "decode_p95", "draft_accepted",
    "draft_proposed", "energy_kwh", "failed", "failed_pct", "kwh_per_day", "max_cpu_temp_c",
    "max_gpu_temp_c", "max_nvme_temp_c", "min_mem_avail_mb", "mtp_accept_pct", "mtp_active",
    "prompt_tokens", "reqs", "reqs_per_hr", "restarts", "restored_tokens", "restores", "stalls",
    "top_fail_reason", "ttft_p50", "ttft_p95", "warm", "warm_pct", "wh_per_req",
}  # fmt: skip
OLD_SERIES_KEYS = {
    "t", "reqs", "decode", "ttft_p95", "cache_hit", "warm_pct", "mtp", "mem_gb", "power",
    "cpu_temp", "gpu_temp",
}  # fmt: skip
OLD_SYSTEM_KEYS = {"gtt_used_mb", "mem_avail_mb", "mem_total_mb", "swap_free_mb", "swap_total_mb"}
POINT_KEYS = {
    "t", "power_w", "cpu_temp_c", "gpu_temp_c", "cpu_temp_max_c", "gpu_temp_max_c",
    "nvme_temp_max_c", "mem_free_gb", "mem_avail_gb", "page_cache_gb", "swap_used_gb", "gtt_gb",
    "energy_kwh",
}  # fmt: skip


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(dash, "DB_PATH", str(tmp_path / "rollup.db"))
    monkeypatch.setattr(dash, "METRICS_FILE", str(tmp_path / "metrics.jsonl"))
    monkeypatch.setattr(dash, "_offset", 0)
    for d in (dash._buckets, dash._samples, dash._sample_dirty, dash._events, dash._req_pending):
        d.clear()
    monkeypatch.setattr(time, "time", lambda: NOW)
    return tmp_path


def _mem(free=1000, avail=5000, cache=2000, swap=0, gtt=3000):
    return {
        "mem_free_mb": free, "mem_avail_mb": avail, "page_cache_mb": cache,
        "swap_used_mb": swap, "gtt_used_mb": gtt,
    }  # fmt: skip


def _fold(t, power=100.0, kwh=1.0, cpu=50.0, gpu=60.0, nvme=40.0, **mem):
    dash._fold_sample_v2(
        {"power": power, "total_energy": kwh},
        _mem(**mem),
        {"cpu_c": cpu, "gpu_c": gpu, "nvme_c": nvme},
        t,
    )


def _count(table):
    con = sqlite3.connect(dash.DB_PATH)
    try:
        return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        con.close()


def _legacy(k, **kw):
    b = dash._new_bucket()
    b.update(kw)
    dash._buckets[k] = b


def test_read_meminfo_new_keys(tmp_path, monkeypatch):
    info = tmp_path / "meminfo"
    info.write_text(
        "MemTotal: 1048576 kB\nMemFree: 204800 kB\nMemAvailable: 512000 kB\n"
        "Cached: 307200 kB\nSwapTotal: 2097152 kB\nSwapFree: 1572864 kB\n"
    )
    real_open = open
    monkeypatch.setattr(
        "builtins.open", lambda p, *a, **k: real_open(info if p == "/proc/meminfo" else p, *a, **k)
    )
    m = dash.read_meminfo()
    assert m["mem_free_mb"] == 200 and m["page_cache_mb"] == 300 and m["swap_used_mb"] == 512
    assert m["mem_avail_mb"] == 500 and m["swap_free_mb"] == 1536  # existing keys kept


def test_fold_into_one_minute_rows(env):
    _fold(NOW + 1, power=100, kwh=1.0, cpu=50, free=900, avail=5000, cache=2000, swap=10, gtt=3000)
    _fold(
        NOW + 31, power=200, kwh=1.002, cpu=70, free=500, avail=4000, cache=2500, swap=30, gtt=3500
    )
    _fold(NOW + 61, power=300, kwh=1.004)
    assert sorted(dash._samples) == [NOW, NOW + 60]
    a = dash._samples[NOW]
    assert (a["pow_sum"], a["pow_n"]) == (300, 2)
    assert (a["e_first"], a["e_last"]) == (1.0, 1.002)
    assert (a["cpu_t_sum"], a["cpu_t_n"], a["cpu_t_max"]) == (120, 2, 70)
    assert a["nvme_t_max"] == 40
    assert (a["min_free"], a["min_avail"], a["max_cache"], a["max_swap"], a["max_gtt"]) == (
        500, 4000, 2500, 30, 3500,
    )  # fmt: skip
    assert dash._sample_dirty == {NOW, NOW + 60}
    assert dash._buckets == {}  # legacy folding is separate and untouched


def test_dirty_flush_writes_only_changed_rows(env):
    _fold(NOW + 1)
    _fold(NOW + 61)
    dash._flush_db()
    assert _count("samples") == 2 and not dash._sample_dirty
    # tamper with row 0 on disk: a later flush must not rewrite it
    con = sqlite3.connect(dash.DB_PATH)
    con.execute("UPDATE samples SET data='{\"marker\": 1}' WHERE bucket=?", (NOW,))
    con.commit()
    con.close()
    _fold(NOW + 62, power=500)  # touches only the second row
    assert dash._sample_dirty == {NOW + 60}
    dash._flush_db()
    con = sqlite3.connect(dash.DB_PATH)
    rows = dict(con.execute("SELECT bucket, data FROM samples"))
    con.close()
    assert json.loads(rows[NOW]) == {"marker": 1}
    assert json.loads(rows[NOW + 60])["pow_n"] == 2
    # nothing dirty: the next flush writes no sample rows
    dash._flush_db()
    assert _count("samples") == 2


def test_failed_flush_keeps_rows_dirty(env, monkeypatch):
    _fold(NOW + 1)

    def boom():
        raise sqlite3.OperationalError("locked")

    monkeypatch.setattr(dash, "_db", boom)
    dash._flush_db()
    assert dash._sample_dirty == {NOW}


def test_reload_from_db(env):
    _fold(NOW + 1, power=123.0, gtt=777)
    dash._flush_db()
    saved = {k: dict(v) for k, v in dash._samples.items()}
    dash._samples.clear()
    dash._load_from_db()
    assert dash._samples == saved
    assert not dash._sample_dirty


def test_reload_defaults_missing_fields(env):
    con = dash._db()
    con.execute("INSERT INTO samples VALUES(?,?)", (NOW, json.dumps({"pow_sum": 5.0, "pow_n": 1})))
    con.commit()
    con.close()
    dash._load_from_db()
    assert dash._samples[NOW]["max_gtt"] is None and dash._samples[NOW]["pow_n"] == 1


def test_prune_at_retention(env):
    old = NOW - (dash.RETENTION_DAYS * 86400 + 120)
    _fold(old)
    _fold(NOW - 60)
    dash._flush_db()
    assert _count("samples") == 1  # old row never written, or deleted
    con = dash._db()
    con.execute("INSERT INTO samples VALUES(?,?)", (old, "{}"))
    con.commit()
    con.close()
    dash._samples[old] = dash._new_sample()
    dash._sample_dirty.add(old)
    dash._prune_mem()
    assert old not in dash._samples and old not in dash._sample_dirty
    dash._flush_db()
    assert _count("samples") == 1


def test_bucket_width_by_window():
    w = dash.sample_bucket_width
    assert [w(900), w(3600), w(3601), w(86400), w(259200), w(259201), w(604800)] == [
        60, 60, 300, 300, 300, 1800, 1800,
    ]  # fmt: skip


def _fill_minutes(start, n, **kw):
    for i in range(n):
        _fold(start + i * 60 + 5, **kw)


def test_merge_300s_points(env):
    t0 = NOW - 3600
    # five minutes in one 300 s point: power 100..500, cpu 40..80, kwh counter rising
    for i in range(5):
        _fold(t0 + i * 60 + 5, power=100 * (i + 1), kwh=10 + i * 0.01, cpu=40 + 10 * i,
              gpu=60, nvme=30 + i, free=1000 - i * 100, avail=5000 + i, cache=100 * i,
              swap=i, gtt=1024 * (i + 1))  # fmt: skip
    # a second sample inside minute 0 weights its minute double
    _fold(
        t0 + 20,
        power=100,
        kwh=10.0,
        cpu=40,
        nvme=30,
        free=1000,
        avail=5000,
        cache=0,
        swap=0,
        gtt=1024,
    )
    pts = dash.sample_series(86400, NOW)
    assert len(pts) == 1
    p = pts[0]
    assert set(p) == POINT_KEYS and p["t"] == t0
    assert p["power_w"] == round((100 * 2 + 200 + 300 + 400 + 500) / 6, 1)
    assert p["cpu_temp_c"] == round((40 * 2 + 50 + 60 + 70 + 80) / 6, 1)
    assert p["cpu_temp_max_c"] == 80 and p["nvme_temp_max_c"] == 34
    assert p["mem_free_gb"] == round(600 / 1024, 2) and p["mem_avail_gb"] == round(5000 / 1024, 2)
    assert p["page_cache_gb"] == round(400 / 1024, 2) and p["swap_used_gb"] == round(4 / 1024, 2)
    assert p["gtt_gb"] == 5.0
    assert p["energy_kwh"] == 0.04  # 10.04 - 10.0


def test_merge_1800s_points(env):
    t0 = NOW - 6 * 86400  # 7-day window -> 30 min points
    for i in range(30):
        _fold(t0 + i * 60 + 1, power=100 + i, kwh=5 + i * 0.001, cpu=40 + (i == 7) * 20,
              free=2000 - i)  # fmt: skip
    _fold(t0 + 1800 + 1, power=999, kwh=6.0)
    pts = dash.sample_series(7 * 86400, NOW)
    assert [p["t"] for p in pts] == [t0, t0 + 1800]
    assert pts[0]["power_w"] == round(sum(100 + i for i in range(30)) / 30, 1)
    assert pts[0]["cpu_temp_max_c"] == 60 and pts[0]["mem_free_gb"] == round(1971 / 1024, 2)
    assert pts[0]["energy_kwh"] == 0.029 and pts[1]["power_w"] == 999


def test_one_minute_points_for_short_window(env):
    _fill_minutes(NOW - 300, 5, power=50.0)
    pts = dash.sample_series(900, NOW)
    assert len(pts) == 5 and pts[1]["t"] - pts[0]["t"] == 60


def test_spans_cutover_legacy_before_samples_after(env):
    first = NOW - 1800  # first samples row, on a 300 s edge
    # legacy covers 2 h ago to now (it keeps folding in parallel after cut-over)
    for k in range(int(NOW - 7200), int(NOW), 300):
        _legacy(k, pow_sum=300.0, pow_n=3, e_first=1.0, e_last=1.1, min_mem=8000,
                cpu_t_sum=150.0, cpu_t_n=3, cpu_t_max=55.0)  # fmt: skip
    _fill_minutes(first, 30, power=200.0, kwh=2.0)
    pts = dash.sample_series(7200, NOW)
    assert all(set(p) == POINT_KEYS for p in pts)
    pre = [p for p in pts if p["t"] < first]
    post = [p for p in pts if p["t"] >= first]
    assert len(pre) == 18 and len(post) == 6  # 300 s width; 90 min legacy, 30 min samples
    assert all(p["power_w"] == 100.0 and p["mem_avail_gb"] == round(8000 / 1024, 2) for p in pre)
    assert all(p["mem_free_gb"] is None and p["gtt_gb"] is None and p["swap_used_gb"] is None
               for p in pre)  # fmt: skip
    assert all(p["power_w"] == 200.0 and p["mem_free_gb"] is not None for p in post)
    assert len({p["t"] for p in pts}) == len(pts)  # no duplicate time slots


def test_cutover_mid_bucket_skips_straddling_legacy(env):
    first = NOW - 1800 + 120  # mid legacy bucket
    for k in range(int(NOW - 3600), int(NOW), 300):
        _legacy(k, pow_sum=300.0, pow_n=3)
    _fill_minutes(first, 28, power=200.0)
    pts = dash.sample_series(86400, NOW)
    legacy_ts = [p["t"] for p in pts if p["power_w"] == 100.0]
    assert max(legacy_ts) == NOW - 2100  # bucket at NOW-1800 straddles first: dropped
    # the straddling slot is served from samples only, at their own power
    assert [p["power_w"] for p in pts if p["t"] == NOW - 1800] == [200.0]


def test_cutover_across_1800s_group_merges_without_double_count(env):
    first = NOW - 900
    _legacy(NOW - 1800, pow_sum=600.0, pow_n=3, e_first=1.0, e_last=1.5)  # 200 W, 0.5 kWh
    _legacy(NOW - 1500, pow_sum=600.0, pow_n=3, e_first=1.5, e_last=2.0)
    _legacy(NOW - 1200, pow_sum=600.0, pow_n=3, e_first=2.0, e_last=2.5)
    _legacy(NOW - 900, pow_sum=999.0, pow_n=1, e_first=2.5, e_last=3.0)  # overlaps samples
    _fill_minutes(first, 15, power=400.0, kwh=3.0)
    pts = dash.sample_series(7 * 86400, NOW)
    assert len(pts) == 1
    # 3 legacy buckets x3 polls @200 W + 15 samples @400 W; the overlapping bucket is ignored
    assert pts[0]["power_w"] == round((600 * 3 + 400 * 15) / (9 + 15), 1)
    assert pts[0]["energy_kwh"] == 2.0  # 3.0 - 1.0


def test_no_samples_serves_legacy_only(env):
    _legacy(NOW - 600, pow_sum=100.0, pow_n=1, cpu_t_sum=70.0, cpu_t_n=1, cpu_t_max=70.0)
    pts = dash.sample_series(86400, NOW)
    assert len(pts) == 1 and pts[0]["power_w"] == 100.0 and pts[0]["cpu_temp_max_c"] == 70.0
    assert pts[0]["energy_kwh"] is None and pts[0]["gtt_gb"] is None


def test_api_history_new_and_old_keys(env):
    _fill_minutes(NOW - 600, 10, free=800, cache=3000, swap=40, gtt=4096)
    _fold(NOW - 30, free=300, cache=3500, swap=90, gtt=6000)
    _legacy(NOW - 1200, pow_sum=100.0, pow_n=1)
    h = dash.build_history(3600)
    assert set(h) == OLD_HISTORY_KEYS | {
        "samples",
        "sample_bucket_sec",
        "cards",
        "request_series",
        "request_bucket_sec",
    }
    assert h["sample_bucket_sec"] == 60 and h["samples"]
    new = {"min_mem_free_mb", "max_page_cache_mb", "max_swap_used_mb", "max_gtt_used_mb"}
    assert set(h["summary"]) == OLD_SUMMARY_KEYS | new
    s = h["summary"]
    assert (s["min_mem_free_mb"], s["max_page_cache_mb"]) == (300, 3500)
    assert (s["max_swap_used_mb"], s["max_gtt_used_mb"]) == (90, 6000)
    assert dash.build_history(86400)["sample_bucket_sec"] == 300
    assert dash.build_history(7 * 86400)["sample_bucket_sec"] == 1800
    assert set(json.loads(json.dumps(h))["samples"][0]) == POINT_KEYS


def test_api_summary_null_without_v2_samples(env):
    _legacy(NOW - 600, pow_sum=100.0, pow_n=1, min_mem=4000)
    h = dash.build_history(3600)
    s = h["summary"]
    for k in ("min_mem_free_mb", "max_page_cache_mb", "max_swap_used_mb", "max_gtt_used_mb"):
        assert k in s and s[k] is None
    assert s["min_mem_avail_mb"] == 4000
    assert set(h["series"][0]) == OLD_SERIES_KEYS


def test_api_state_system_keys(env):
    dash._live.update(mem={**_mem(free=700, cache=1500, swap=20), "mem_total_mb": 9999})
    sy = dash.build_state()["system"]
    assert set(sy) == OLD_SYSTEM_KEYS | {"mem_free_mb", "page_cache_mb", "swap_used_mb"}
    assert (sy["mem_free_mb"], sy["page_cache_mb"], sy["swap_used_mb"]) == (700, 1500, 20)
    assert sy["mem_total_mb"] == 9999


def test_legacy_only_db_upgrades_in_place(env):
    con = sqlite3.connect(dash.DB_PATH)
    con.execute("CREATE TABLE buckets(bucket INTEGER PRIMARY KEY, data TEXT)")
    con.execute("CREATE TABLE events(ts REAL, kind TEXT)")
    con.execute("CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT)")
    b = dash._new_bucket()
    b.update(pow_sum=240.0, pow_n=2, min_mem=6000, reqs=3)
    old = {k: v for k, v in b.items() if k != "nvme_t_max"}
    con.execute("INSERT INTO buckets VALUES(?,?)", (int(NOW - 600), json.dumps(old)))
    con.commit()
    con.close()
    dash._load_from_db()
    assert list(dash._buckets) == [NOW - 600] and dash._samples == {}
    h = dash.build_history(86400)
    assert h["summary"]["reqs"] == 3 and h["summary"]["min_mem_free_mb"] is None
    assert h["samples"][0]["power_w"] == 120.0
    _fold(NOW - 30)
    dash._flush_db()
    assert _count("samples") == 1 and _count("buckets") == 1
