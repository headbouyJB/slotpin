"""Tests for the ESPHome background energy poller (v7 metric layer).

Pollers are constructed with poll=False so no background thread starts; we
drive _read() (fetch+parse) and turn_metrics() (pure delta logic) directly.
"""

from __future__ import annotations

import time

import slotpin.energy as energy_mod
from slotpin.energy import EnergyPoller


class _FakeResp:
    def __init__(self, body: str) -> None:
        self._b = body.encode()

    def read(self) -> bytes:
        return self._b

    def __enter__(self) -> _FakeResp:
        return self

    def __exit__(self, *a: object) -> bool:
        return False


def _urlopen_returning(body: str):
    def _urlopen(req, timeout=None):  # noqa: ANN001
        return _FakeResp(body)

    return _urlopen


def _urlopen_raising(exc: Exception):
    def _urlopen(req, timeout=None):  # noqa: ANN001
        raise exc

    return _urlopen


_PROM = (
    "# TYPE esphome_sensor_value gauge\n"
    'esphome_sensor_value{id="power",node="box-a",name="Power",unit="W"} 142.0\n'
    'esphome_sensor_value{id="total_energy",node="box-a",name="Total Energy",unit="kWh"} 1.60100\n'
    'esphome_sensor_failed{id="total_energy",node="box-a",name="Total Energy"} 0\n'
)


# --- _read (fetch + parse) ---

def test_read_prometheus_gets_counter_and_power(monkeypatch) -> None:
    p = EnergyPoller("http://plug/metrics", metric="total_energy", poll=False)
    monkeypatch.setattr(energy_mod.urllib.request, "urlopen", _urlopen_returning(_PROM))
    r = p._read()
    assert r == {"energy_kwh": 1.601, "plug_power_w": 142.0}  # counter + power, right sensors


def test_read_json_endpoint(monkeypatch) -> None:
    p = EnergyPoller("http://plug/sensor/total_energy", poll=False)
    monkeypatch.setattr(energy_mod.urllib.request, "urlopen", _urlopen_returning('{"value": 2.5}'))
    r = p._read()
    assert r == {"energy_kwh": 2.5, "plug_power_w": None}  # JSON = one sensor, no power


def test_read_fail_open(monkeypatch) -> None:
    p = EnergyPoller("http://plug/metrics", metric="total_energy", poll=False)
    monkeypatch.setattr(energy_mod.urllib.request, "urlopen", _urlopen_raising(OSError("boom")))
    assert p._read() is None


def test_disabled_poller_has_no_snapshot() -> None:
    p = EnergyPoller(None)
    assert p.snapshot() is None
    assert p.turn_metrics(None, None) == {
        "energy_kwh": None,
        "energy_turn_wh": None,
        "energy_idle_wh": None,
        "plug_power_w": None,
    }


# --- turn_metrics (start/end delta logic) ---

def test_turn_metrics_isolates_turn_and_idle() -> None:
    p = EnergyPoller(None)
    # First turn: start 1.0000, end 1.0001 kWh -> 0.1 Wh inference; no prior end so idle=None.
    m1 = p.turn_metrics({"energy_kwh": 1.0000, "plug_power_w": 100.0},
                        {"energy_kwh": 1.0001, "plug_power_w": 130.0})
    assert m1["energy_turn_wh"] == 0.1
    assert m1["energy_idle_wh"] is None  # no previous turn yet
    assert m1["energy_kwh"] == 1.0001 and m1["plug_power_w"] == 130.0

    # Second turn: idle from prev end (1.0001) to this start (1.0005) = 0.4 Wh;
    # inference from 1.0005 to 1.0007 = 0.2 Wh.
    m2 = p.turn_metrics({"energy_kwh": 1.0005, "plug_power_w": 20.0},
                        {"energy_kwh": 1.0007, "plug_power_w": 140.0})
    assert m2["energy_idle_wh"] == 0.4
    assert m2["energy_turn_wh"] == 0.2


def test_turn_metrics_counter_reset_is_graceful() -> None:
    p = EnergyPoller(None)
    p.turn_metrics({"energy_kwh": 5.0, "plug_power_w": 100.0},
                   {"energy_kwh": 5.001, "plug_power_w": 100.0})  # sets last_end = 5.001
    # Midnight reset: this turn's counter is far below the previous end.
    m = p.turn_metrics({"energy_kwh": 0.0001, "plug_power_w": 100.0},
                       {"energy_kwh": 0.0003, "plug_power_w": 100.0})
    assert m["energy_idle_wh"] is None  # start (0.0001) < prev end (5.001) -> reset, suppressed
    assert m["energy_turn_wh"] == 0.2   # within-turn delta still valid


def test_turn_metrics_missing_snapshot_yields_none() -> None:
    p = EnergyPoller(None)
    m = p.turn_metrics(None, {"energy_kwh": 1.0, "plug_power_w": 5.0})
    assert m["energy_turn_wh"] is None  # no start snapshot
    assert m["energy_kwh"] == 1.0


# --- stale-reading fail-open (audit finding: dead plug must yield None, not 0.0) ---


def test_snapshot_returns_none_when_stale() -> None:
    p = EnergyPoller(None, poll=False)
    p._latest = {"energy_kwh": 1.0, "plug_power_w": 100.0}
    p._latest_mono = time.monotonic() - 100.0  # far older than _stale_after_s
    assert p.snapshot() is None  # stale -> None, not a bogus reused reading


def test_snapshot_returns_fresh_reading() -> None:
    p = EnergyPoller(None, poll=False)
    p._latest = {"energy_kwh": 1.0, "plug_power_w": 100.0}
    p._latest_mono = time.monotonic()
    assert p.snapshot() == {"energy_kwh": 1.0, "plug_power_w": 100.0}
