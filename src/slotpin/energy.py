"""Whole-box energy/power capture by polling an ESPHome smart-plug endpoint.

The reference deployment meters the whole box with an ESPHome smart plug
(ESP8266-based). With the plug's ``prometheus:``
(and required ``web_server:``) components enabled it serves ``/metrics`` with
lines like::

    esphome_sensor_value{id="power",...,unit="W"} 142
    esphome_sensor_value{id="total_energy",...,unit="kWh"} 1.60

**Design: background poller.** A daemon thread refreshes the plug's counter +
power into memory every ``interval_s`` (~1 s, matching the sensor's own update
rate). Per turn, the request path only reads that in-memory snapshot -- at
turn start and turn end -- so there is *zero HTTP on the request path*
(non-blocking) and a fixed ~1 req/s load on the ESP8266 regardless of request
throughput. Reading the cached value is no less accurate than a synchronous
fetch, since the sensor only updates once per interval anyway.

From the start/end snapshots the caller gets, per turn:

* ``energy_turn_wh`` -- counter(end) - counter(start): energy consumed serving
  THIS turn (arrival -> completion). Isolates inference from idle.
* ``energy_idle_wh`` -- counter(start) - counter(previous turn's end): energy
  burned idle since the last turn.
* ``energy_kwh`` -- the absolute counter at turn end.
* ``plug_power_w`` -- whole-box power at turn end (spot).

Counter resets (e.g. a daily counter rolling over at midnight) are handled
gracefully: any delta where the counter went backwards is reported as ``None``
rather than negative. Point ``energy_metric`` at a NON-resetting counter (an
ESPHome ``integration`` ``total_energy`` sensor) to avoid the gap entirely --
see docs/energy.md.

Stdlib-only (urllib), fail-open by contract: any failure yields ``None``
values and never touches the request path.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
import urllib.request

logger = logging.getLogger("slotpin.energy")

_TURN_NULL = {
    "energy_kwh": None,
    "energy_turn_wh": None,
    "energy_idle_wh": None,
    "plug_power_w": None,
}


def _prom_value(body: str, metric_id: str) -> float | None:
    """Extract esphome_sensor_value{...id="<metric_id>"...} <number>, or None."""
    pat = re.compile(
        r'^esphome_sensor_value\{[^}]*\bid="' + re.escape(metric_id) + r'"[^}]*\}\s+([0-9eE+.\-]+)',
        re.MULTILINE,
    )
    m = pat.search(body)
    return float(m.group(1)) if m else None


class EnergyPoller:
    """Background-polls an ESPHome plug; serves in-memory snapshots per turn."""

    def __init__(
        self,
        url: str | None,
        interval_s: float = 1.0,
        timeout_s: float = 1.0,
        metric: str | None = None,
        power_metric: str | None = "power",
        *,
        poll: bool = True,
    ) -> None:
        self._url = url
        self._interval = interval_s
        self._timeout = timeout_s
        self._metric = metric  # energy-counter sensor id (Prometheus mode)
        self._power_metric = power_metric  # power sensor id (Prometheus mode)
        self._lock = threading.Lock()
        self._latest: dict[str, float | None] | None = None
        self._latest_mono: float = 0.0  # time.monotonic() of the last successful poll
        # A snapshot older than this (plug went unreachable) is treated as
        # missing -> None, rather than serving a stale reading that would emit
        # bogus 0.0 Wh deltas. ~3 poll intervals of grace.
        self._stale_after_s = max(3.0 * interval_s, 3.0)
        self._last_end_kwh: float | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        if url and poll:
            self._thread = threading.Thread(target=self._loop, name="energy-poller", daemon=True)
            self._thread.start()

    # -- background polling ------------------------------------------------

    def _read(self) -> dict[str, float | None] | None:
        """One fetch+parse. Returns {energy_kwh, plug_power_w} or None on failure."""
        if not self._url:
            return None
        try:
            req = urllib.request.Request(self._url, headers={"Accept": "text/plain"})
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:  # noqa: S310 (LAN plug)
                body = resp.read().decode("utf-8", "ignore")
        except Exception:  # noqa: BLE001 -- fail-open
            logger.debug("energy poll failed for %s", self._url, exc_info=True)
            return None

        is_json = body.lstrip().startswith("{")
        energy_val: float | None = None
        power_val: float | None = None
        try:
            if is_json:
                energy_val = float(json.loads(body)["value"])
            else:
                if self._metric:
                    energy_val = _prom_value(body, self._metric)
                if self._power_metric:
                    power_val = _prom_value(body, self._power_metric)
        except Exception:  # noqa: BLE001 -- fail-open on a malformed body
            logger.debug("energy parse failed for %s", self._url, exc_info=True)
            return None
        return {"energy_kwh": energy_val, "plug_power_w": power_val}

    def _loop(self) -> None:
        # Prime immediately, then refresh every interval until stopped.
        while True:
            reading = self._read()
            if reading is not None:
                with self._lock:
                    self._latest = reading
                    self._latest_mono = time.monotonic()
            if self._stop.wait(self._interval):
                return

    def stop(self) -> None:
        self._stop.set()

    # -- per-turn API -----------------------------------------------------

    def snapshot(self) -> dict[str, float | None] | None:
        """Instant, in-memory copy of the most recent successful poll, or None if
        there was never one or the last one is stale (plug unreachable)."""
        with self._lock:
            if self._latest is None or (time.monotonic() - self._latest_mono) > self._stale_after_s:
                return None
            return dict(self._latest)

    @staticmethod
    def _delta_wh(start_kwh: float | None, end_kwh: float | None) -> float | None:
        """(end - start) in Wh; None if either missing or the counter reset (end < start)."""
        if start_kwh is None or end_kwh is None or end_kwh < start_kwh:
            return None
        return round((end_kwh - start_kwh) * 1000.0, 4)

    def turn_metrics(
        self,
        start: dict[str, float | None] | None,
        end: dict[str, float | None] | None,
    ) -> dict[str, float | None]:
        """Per-turn energy from start/end snapshots. Updates the idle baseline."""
        s_kwh = start.get("energy_kwh") if start else None
        e_kwh = end.get("energy_kwh") if end else None
        turn_wh = self._delta_wh(s_kwh, e_kwh)
        with self._lock:
            idle_wh = self._delta_wh(self._last_end_kwh, s_kwh)
            if e_kwh is not None:
                self._last_end_kwh = e_kwh
        return {
            "energy_kwh": round(e_kwh, 6) if e_kwh is not None else None,
            "energy_turn_wh": turn_wh,
            "energy_idle_wh": idle_wh,
            "plug_power_w": end.get("plug_power_w") if end else None,
        }
