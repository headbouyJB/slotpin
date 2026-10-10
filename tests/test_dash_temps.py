"""Tests for slotpin-dash's hwmon temperature reader and rollup folding.

read_temps() reads raw hwmon sysfs (no fan-control daemon dependency -- see
docs/dashboard.md and the PR notes: no such daemon is guaranteed present).
These tests build a fake hwmon tree under tmp_path rather than touching the
real /sys/class/hwmon.
"""

from __future__ import annotations

import json
import sqlite3

import slotpin.dash as dash


def _make_hwmon(root, dirname, sensor_name, temp1_milli_c=None):
    d = root / dirname
    d.mkdir()
    (d / "name").write_text(sensor_name)
    if temp1_milli_c is not None:
        (d / "temp1_input").write_text(str(temp1_milli_c))
    return d


def test_read_temps_finds_cpu_gpu_nvme(tmp_path, monkeypatch) -> None:
    hwmon = tmp_path / "hwmon"
    hwmon.mkdir()
    _make_hwmon(hwmon, "hwmon0", "acpitz", 79000)  # not one we track -- ignored
    _make_hwmon(hwmon, "hwmon1", "k10temp", 79375)
    _make_hwmon(hwmon, "hwmon2", "amdgpu", 81000)
    _make_hwmon(hwmon, "hwmon3", "nvme", 53850)

    monkeypatch.setattr(dash, "HWMON_ROOT", str(hwmon))
    t = dash.read_temps()
    assert t == {"cpu_c": 79.4, "gpu_c": 81.0, "nvme_c": 53.9}


def test_read_temps_missing_sensor_is_none(tmp_path, monkeypatch) -> None:
    hwmon = tmp_path / "hwmon"
    hwmon.mkdir()
    d = hwmon / "hwmon0"
    d.mkdir()
    (d / "name").write_text("acpitz")
    (d / "temp1_input").write_text("79000")

    monkeypatch.setattr(dash, "HWMON_ROOT", str(hwmon))
    t = dash.read_temps()
    assert t == {"cpu_c": None, "gpu_c": None, "nvme_c": None}


def test_read_temps_no_hwmon_root_fails_open(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(dash, "HWMON_ROOT", str(tmp_path / "does-not-exist"))
    assert dash.read_temps() == {"cpu_c": None, "gpu_c": None, "nvme_c": None}


def test_fold_sample_tracks_avg_and_max_per_bucket() -> None:
    dash._buckets.clear()
    now = 10_000.0
    dash._fold_sample(None, None, {"cpu_c": 70.0, "gpu_c": 75.0, "nvme_c": 40.0}, now)
    dash._fold_sample(None, None, {"cpu_c": 80.0, "gpu_c": 85.0, "nvme_c": 45.0}, now + 1)
    key = int(now // dash.BUCKET_SEC) * dash.BUCKET_SEC
    b = dash._buckets[key]
    assert b["cpu_t_sum"] == 150.0 and b["cpu_t_n"] == 2 and b["cpu_t_max"] == 80.0
    assert b["gpu_t_sum"] == 160.0 and b["gpu_t_n"] == 2 and b["gpu_t_max"] == 85.0
    assert b["nvme_t_max"] == 45.0
    dash._buckets.clear()


def test_fold_sample_missing_temps_is_noop() -> None:
    dash._buckets.clear()
    dash._fold_sample(None, None, None, 10_000.0)
    key = int(10_000.0 // dash.BUCKET_SEC) * dash.BUCKET_SEC
    b = dash._buckets[key]
    assert b["cpu_t_n"] == 0 and b["cpu_t_max"] is None
    dash._buckets.clear()


def test_parse_boxes_and_render_page_marks_active_box(monkeypatch) -> None:
    monkeypatch.setattr(dash, "BOXES", [("box-a", "http://a/"), ("box-b", "http://b/")])
    monkeypatch.setattr(dash, "DASH_LABEL", "box-b")
    page = dash.render_page()
    assert "__LABEL__" not in page and "__BOXNAV__" not in page
    assert 'href="http://a/"' in page and 'href="http://b/"' in page
    # the active box's tab carries the "on" class; the other does not
    assert '<a class="boxtab on" href="http://b/">box-b</a>' in page
    assert '<a class="boxtab" href="http://a/">box-a</a>' in page


def test_load_from_db_migrates_legacy_buckets_missing_temp_fields(tmp_path, monkeypatch) -> None:
    """Regression: a rollup.db written before temp-tracking existed has bucket
    JSON without cpu_t_sum/gpu_t_n/etc. Loading it must not KeyError the first
    time a live sample is folded into that bucket (this crashed the collector
    thread on a host whose rollup.db predates this feature)."""
    db_path = tmp_path / "rollup.db"
    con = sqlite3.connect(str(db_path))
    con.execute("CREATE TABLE buckets(bucket INTEGER PRIMARY KEY, data TEXT)")
    con.execute("CREATE TABLE events(ts REAL, kind TEXT)")
    con.execute("CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT)")
    legacy_bucket = {  # pre-temp-tracking schema: no cpu_t_*/gpu_t_*/nvme_t_* keys
        "reqs": 5,
        "warm": 2,
        "cold": 3,
        "restores": 0,
        "errors": 0,
        "prompt": 100,
        "completion": 50,
        "cache": 10,
        "restored": 0,
        "draft": 0,
        "draft_acc": 0,
        "dec_sum": 20.0,
        "dec_n": 2,
        "dec_res": [],
        "ttft_res": [],
        "min_mem": 5000,
        "e_first": 1.0,
        "e_last": 1.1,
        "pow_sum": 50.0,
        "pow_n": 1,
    }
    key = 12345 * dash.BUCKET_SEC
    con.execute(
        "INSERT INTO buckets(bucket,data) VALUES(?,?)", (key, json.dumps(legacy_bucket))
    )
    con.commit()
    con.close()

    dash._buckets.clear()
    dash._events.clear()
    monkeypatch.setattr(dash, "DB_PATH", str(db_path))
    try:
        dash._load_from_db()
        assert key in dash._buckets
        b = dash._buckets[key]
        assert b["reqs"] == 5  # legacy data preserved
        assert b["cpu_t_sum"] == 0.0 and b["cpu_t_n"] == 0 and b["cpu_t_max"] is None

        # This is exactly what the collector loop does every poll -- must not raise.
        dash._fold_sample(None, None, {"cpu_c": 72.0, "gpu_c": 80.0, "nvme_c": None}, key + 1)
        assert b["cpu_t_sum"] == 72.0 and b["cpu_t_n"] == 1
        assert b["gpu_t_sum"] == 80.0 and b["gpu_t_n"] == 1
    finally:
        dash._buckets.clear()
        dash._events.clear()


def test_parse_boxes_skips_malformed_entries() -> None:
    assert dash._parse_boxes("a=http://x, bad-entry, b=http://y") == [
        ("a", "http://x"),
        ("b", "http://y"),
    ]
    assert dash._parse_boxes("") == []
    assert dash._parse_boxes(None) == []
