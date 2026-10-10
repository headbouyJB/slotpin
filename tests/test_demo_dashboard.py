"""The demo dashboard tool: synthetic metrics validate, and the dashboard serves them."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import time
import urllib.request

import pytest
from test_no_private_data import ROOT, _scan

import slotpin.dash as dash

TOOL = ROOT / "tools" / "demo_dashboard.py"


def _load():
    spec = importlib.util.spec_from_file_location("demo_dashboard", TOOL)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def demo():
    return _load()


def _records(demo, seed=3, days=2.0):
    return demo.Scenario(seed=seed, now=time.time(), days=days).records()


def test_records_ingest_as_request_rows(demo):
    recs = _records(demo)
    rows = [dash._request_row(r, json.dumps(r)) for r in recs]
    assert all(r is not None for r in rows)
    status = [r[2] for r in rows]  # (key, ts, status, ...)
    assert status.count("ok") > 50
    assert status.count("failed") >= 3
    assert len({r[0] for r in rows}) == len(rows)  # unique keys
    reasons = {r["reason"] for r in recs if r.get("event") == "turn_failed"}
    assert len(reasons) >= 3
    ok = [r for r in recs if r.get("event") is None]
    assert {r["route"] for r in ok} == {"chat", "background"}
    assert {r["upstream_cache"] for r in ok} == {"memory", "disk", "miss"}
    acc = sorted(r["accept_rate"] for r in ok)
    assert 0.5 < acc[len(acc) // 2] < 0.75


def test_deterministic_for_a_seed(demo):
    now = 1_800_000_000.0
    a = demo.Scenario(seed=5, now=now, days=1).records()
    b = demo.Scenario(seed=5, now=now, days=1).records()
    c = demo.Scenario(seed=6, now=now, days=1).records()
    assert a == b
    assert a != c


def test_no_generated_value_matches_private_data_patterns(demo):
    scn = demo.Scenario(seed=3, now=time.time(), days=2.0)
    lines = [json.dumps(r) for r in scn.records()]
    lines += [json.dumps(e) for e in scn.events()]
    box = demo.FakeBox(scn.model)
    lines += box.energy_text().splitlines() + box.metrics_text().splitlines()
    lines += [json.dumps(box.slots())]
    lines += [demo.LABEL, demo.CONFIG_ID, demo.BUILD, demo.MODEL]
    hits = _scan(ROOT / "generated.txt", "\n".join(lines))
    assert not hits, hits


def test_dashboard_serves_the_demo(tmp_path):
    env = dict(os.environ, PYTHONPATH=str(ROOT / "src"), PYTHONUNBUFFERED="1")
    proc = subprocess.Popen(
        [sys.executable, str(TOOL), "--port", "0", "--days", "2", "--dir", str(tmp_path),
         "--live-interval", "0", "--once-seconds", "120"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )
    try:
        url = None
        end = time.time() + 60
        while time.time() < end and url is None:
            line = proc.stdout.readline()
            if not line and proc.poll() is not None:
                break
            if "http://127.0.0.1:" in line and "dashboard:" in line:
                url = line.split("dashboard:")[1].split()[0]
        assert url, "demo did not start"

        def get(path):
            with urllib.request.urlopen(url.rstrip("/") + path, timeout=10) as r:
                assert r.status == 200
                return json.load(r)

        time.sleep(4)  # let the collector poll the fake upstream once
        st = get("/api/state")
        assert st["health"]["slots_ok"] and st["health"]["slots_total"] == 4
        assert st["health"]["config_id"] == "gufo-demo-4slot"
        assert st["cards"]["decode"]["p50"] is not None
        hist = get("/api/history?window=86400")
        assert hist["summary"]["reqs"] > 0
        assert hist["samples"] and hist["request_series"]
        kinds = {e["kind"] for e in hist["events"]}
        assert {"down", "up", "stall", "stall_cleared"} <= kinds
        assert hist["cards"]["cache_source"]["kind"] == "gufo"
        assert hist["summary"]["failed"] > 0
        rq = get("/api/requests?window=86400")
        assert rq["total"] > 0 and rq["rows"]
        assert any(r["status"] == "failed" for r in get("/api/requests?window=86400&filter=failed")["rows"])
    finally:
        proc.terminate()
        proc.wait(10)
