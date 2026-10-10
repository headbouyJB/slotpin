# Whole-box energy capture

slotpin can record **whole-box energy per turn** by polling a smart plug that
meters the machine, so every request in the metrics JSONL carries the wall
power and the energy it consumed. Summed over a benchmark run and divided by
the number of correct answers, that gives **Wh per correct answer** — the
headline efficiency number.

This is optional and fail-open: if the plug is unreachable or unconfigured,
the energy columns are simply `null` and nothing else is affected.

## What you get

Per-turn columns in `metrics_file` (see `docs/metrics.md`):

| column | meaning |
|---|---|
| `plug_power_w` | whole-box instantaneous power (W) from the plug |
| `energy_kwh` | the plug's cumulative energy counter (kWh) |
| `energy_turn_wh` | whole-box energy over this turn's window: counter at turn end minus counter at turn start (Wh) |
| `energy_fair_wh` | `energy_turn_wh / concurrency`: this turn's share when turns overlap (equal to `energy_turn_wh` for sequential turns) |
| `energy_idle_wh` | energy consumed idle between the previous turn's end and this turn's start (Wh) |
| `soc_power_w` | on-box APU/GPU package power (W), read from amdgpu hwmon sysfs — no plug needed |

`energy_fair_wh` is the workhorse: `Σ energy_fair_wh over a run ÷ correct_count = Wh/correct`
(for a sequential run `energy_fair_wh == energy_turn_wh`).

## Requirements

- A smart plug metering the box, exposing an HTTP endpoint on your network. This
  guide is written for the **LocalBytes Smart Plug PM**
  (<https://www.mylocalbytes.com/products/smart-plug-pm>) — an ESPHome,
  HLW8012-based plug whose stock firmware pulls in the `localbytes.plug-pm`
  package; `deploy/esphome/localbytes-plug-energy.yaml` targets exactly it.
  Any other ESPHome plug (`web_server` JSON or Prometheus) works too, and other
  plugs can be supported with a small parser addition (see *Other plugs*).
- The plug reachable from the box the proxy runs on (same VLAN/subnet).

## 1. Prepare the plug (ESPHome)

Append `deploy/esphome/localbytes-plug-energy.yaml` to the plug's ESPHome
config and Install → OTA. It enables `web_server` + `prometheus` and adds a
**high-resolution** energy counter (`daily_energy_hires`, `accuracy_decimals: 5`
= 0.01 Wh) plus a 1 s power `update_interval`.

> **Why the hi-res counter:** the stock `total_daily_energy` reports 2 decimals
> (0.01 kWh = 10 Wh steps) — coarser than a whole run consumes, so its delta
> reads 0. The counter accumulates at full precision internally; the hi-res
> sensor just stops rounding it away.

## 2. Find the plug's address

Find the plug's address from the ESPHome dashboard (the device's IP is shown
there and in its logs), from your router's DHCP lease table (match the plug's
MAC), or by its mDNS name. A static IP or a DHCP reservation is the most
reliable for a long-running service. Then, from the box the proxy runs on,
confirm the endpoint (example address `192.0.2.10`):
```bash
curl -s http://192.0.2.10/metrics | grep esphome_sensor_value
# -> esphome_sensor_value{id="daily_energy_hires",...,unit="kWh"} 0.00029
```
(mDNS `http://<name>.local/metrics` also works if the box resolves mDNS.)

## 3. Configure the proxy

In `config.toml`:
```toml
energy_url    = "http://192.0.2.10/metrics"
energy_metric = "total_energy"     # the id= label from /metrics (non-resetting; recommended)
energy_timeout_s = 1.0
energy_poll_s = 1.0                # background poll cadence; match the plug's update_interval
```
A background thread polls the plug every `energy_poll_s` into memory; each turn
reads that snapshot at **start and end** (never an HTTP call on the request
path), giving `energy_turn_wh` (this turn) and `energy_idle_wh` (the gap before
it). A snapshot older than about three poll intervals (at least 3 s) is treated
as missing, so a plug that goes away yields `null` columns rather than stale
values or bogus zero deltas. Restart
the service, fire a request under load, and check a turn record:
```bash
tail -1 /var/lib/slotpin/metrics.jsonl | python3 -m json.tool | grep -E 'energy|power'
# energy_turn_wh: 0.17, energy_idle_wh: 0.02, plug_power_w: 131, energy_kwh: 41.6, soc_power_w: 85
# (illustrative values)
```

**Per-box note:** with several boxes, the plug address is the one value that
differs per box — each box's `energy_url` points at its own plug.

## Resolution & caveats

- **Time granularity** is bounded by the power sensor's `update_interval` (1 s
  here) and, underneath, by the metering chip: an HLW8012 measures via pulse
  frequency and needs a ~1–2 s window, so sub-second per-turn energy isn't
  reliable on this hardware regardless of firmware. Per-run energy is rock
  solid; per-turn is good under load, noisy near idle.
- **Midnight reset:** a `total_daily_energy` counter resets at 00:00. The
  poller handles this gracefully — any backwards jump yields `null`, never a
  negative — so at worst one turn's delta is dropped. To avoid it entirely,
  use the **non-resetting `integration` `total_energy`** sensor (in the snippet,
  and the recommended `energy_metric`): it never resets, so every per-turn
  delta stays valid.
- **Home Assistant** is not in this path. HA current-value reads are fast, but
  its recorder/history is heavy and its sensors are significant-change
  filtered — polling the plug directly is both fresher and lighter.

## Energy under concurrency

The plug meters the **whole box**, so when several turns run at once their
energy can't be told apart at the meter. Two levels of accounting:

- **Run-level Wh/correct is exact regardless of concurrency:** total run
  energy (`energy_kwh` last − first, or Σ over the run) ÷ correct answers. All
  the box's energy over the run ÷ all the work done — no attribution needed.
- **Per-turn energy under concurrency needs a divisor.** Each turn records
  `concurrency`, the mean of the live-turn count at its admission and at its
  completion (incl. itself), and `energy_fair_wh = energy_turn_wh / concurrency`.
  Sequential turns divide by 1; concurrent ones by 2 or more. It is approximate
  because concurrency can vary *within* a turn (turns start and end at different
  times).
- **Exact attribution (designed, not built):** because the background poller
  already ticks every ~1 s, the clean method is to split each energy increment
  among the turns live *at that instant* — at each tick, credit `Δenergy / N(t)`
  to every in-flight turn. That would handle partial overlap correctly; see
  `docs/concurrency-design.md`. Until then, with `max_inflight = 1`,
  `concurrency` is always 1 and `energy_turn_wh` is exact.

## URL forms and other plugs

`energy_url` is fetched with a plain HTTP GET every `energy_poll_s`, with
`energy_timeout_s` as the timeout. The body is parsed by shape:

- **Prometheus text** (the ESPHome `prometheus` component's `/metrics`; any body
  not starting with `{`): the counter is the `esphome_sensor_value` line whose
  `id="..."` equals `energy_metric`, and the power is the line with `id="power"`
  (so the plug's power sensor should have that id). If `energy_metric` is unset
  no counter is read.
- **ESPHome `web_server` JSON** (a body starting with `{`, e.g.
  `/sensor/<id>`, returning `{"value": …}`): that value is the counter;
  `energy_metric` is not used. One sensor per endpoint, so `plug_power_w` stays
  `null` in this mode (use the `/metrics` scrape for power).
- **Tasmota / others:** any HTTP endpoint returning the counter works with a
  small parser addition in `slotpin/energy.py` (e.g. Tasmota's
  `StatusSNS.ENERGY.Total`). The counter-delta contract is the same.

Failure handling is fail-open: a failed fetch or unparseable body leaves the
previous snapshot in place until it goes stale (see above); a counter that goes
backwards yields `null` for that delta, never a negative.

The dashboard (`slotpin-dash`) has its own, separate energy feed, `DASH_ENERGY`
(see `docs/dashboard.md`). It is unset by default (energy off) and, unlike the
proxy, understands only the Prometheus `/metrics` form, using the ids `power`,
`total_energy`, `daily_energy`, `voltage` and `current`.
