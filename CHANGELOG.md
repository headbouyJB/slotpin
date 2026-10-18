# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/) (pre-1.0:
`0a`/`1.0.0aN` pre-release identifiers denote alpha status; no API/config
stability guarantee yet).

## [Unreleased]

### Added -- dashboard: decode and prefill speed against context depth

- Two charts in the Performance group, under the Decode and Prefill time
  charts: median t/s per `prompt_n` bin (`<4K`, `4-8K`, ... `>=128K`, a log2
  axis) with the p25-p75 band, hollow dots for bins with fewer than three
  turns, one line per concurrency bucket on multi-slot boxes, a shared bin
  highlight and readout on hover. Decode counts turns with at least 64
  generated tokens; prefill keeps the cards' 2,000-new-token rule.
- `/api/history` gains `depth`; `GET /api/depth?window=` returns it alone.
  `depth_series()` in `slotpin.dash`; see `docs/dashboard.md`.
- The demo tool's synthetic speeds now fall with depth and a few sessions
  start from a long document, so the new charts have data.

## [1.0.0a7] - 2026-10-10

First public release. Everything since 1.0.0a1 is listed here.

### Added -- fail-fast on a silent upstream (off by default)

A stalled engine that has already sent `200`, a role-only event and then only
`: ping` comments kept the relay open indefinitely (observed: 55 h), so no timeout
fired and the client never fell back.

- `upstream_stall_timeout_s` (default `0` = off): for streaming chat
  completions, the longest time with no real output event (text, reasoning or
  tool-call delta) and no token progress in the upstream's `/slots` counters
  (probed every half limit; Gufo decodes tool calls silently) before the
  upstream is aborted. Fires on a silent upstream too. Client gets `504 upstream_stalled` if nothing was sent yet, else a cut
  stream.
- `hold_headers_until_first_output` (default `false`): send the client nothing
  until the first output event, so a prefill stall is the 504.
- New `turn_failed` reason `stalled`.

### Added -- `upstream_kind = "gufo"`: run in front of the Gufo engine

Gufo owns prompt caching (in-memory + disk continuation cache), so in front of
it slotpin keeps classification, scheduling, relay + disconnect abort and
metrics, and drops slot pinning, KV save/restore, prefix_restore, cache-ram
pool inference and `/props` reload detection. `"llamacpp"` (the default) is
unchanged.

- New config key `upstream_kind` (`"llamacpp"` | `"gufo"`); `"gufo"` with
  `kv_persist`, `kv_adaptive` or `prefix_restore` true is a config error.
  Folded into `config_fingerprint` only when not `llamacpp`.
- Gufo mode: no `id_slot` in the forwarded body (`include_usage` still forced);
  no save/restore/`/apply-template` calls.
- Turn records gain `upstream_cache` (`memory`/`disk`/`miss`),
  `upstream_cache_miss_reason`, `upstream_restore_ms`/`_bytes`,
  `upstream_snapshot_ms`/`_bytes`, `upstream_disk_queued_bytes` from
  `usage.gufo`; `served` is `"warm"` on a Gufo cache hit (it was always
  `"cold"`).
- The post-abort hold reads `/metrics` `llamacpp:requests_processing` (Gufo's
  `/slots` has no `is_processing`, so the hold never waited).
- Reload detection polls `/v1/models` (`data[0].id`, `context_length`) and
  `/metrics` counter regressions instead of `/props` (which 400s on Gufo every
  poll and left the detector blind).
- Version 1.0.0a7.

### Fixed -- prefix-index durability

- A relay that ends early (client disconnected, upstream aborted, or a
  Content-Length body cut short) is no longer tracked or saved as a served turn.
  Killed workers' half-processed turns were overwriting good snapshots, losing every index.
  The slot's contents are then recorded as unknown (next turn restores) instead
  of reverting to the previous prefix.
- A re-save of the same prefix hash that can't be indexed now keeps the existing
  `<hash>.idx.json`: same hash means same system message, so the saved KV still
  starts with the indexed head. (1.0.0a5 deleted it, losing the turn-1 index.)
- `/apply-template` failures are logged with status and error text (they were
  silent), and the call is bounded to 10s on the request path.

### Added -- longest-common-prefix disk restore (`prefix_restore`, off by default)

Disk snapshots are named by a hash of the request's WHOLE system message and
restored only on an exact match, so a client that keeps a large stable head and
a small changing tail in the system message (tools + instructions, then the
date, a per-task description and a runtime line) never got a disk restore. Observed:
batch workers re-prefilled ~34.6K tokens cold (260-590s each) every run with
the previous run's snapshots sitting unused on disk.

- New `prefix_index.py`: each saved `<hash>.bin` gets a `<hash>.idx.json`
  sidecar of chained hashes over fixed-size chunks of the RENDERED prompt
  (llama-server `/apply-template`, so tool definitions and template framing
  are in their real positions).
- On a cold turn with no exact snapshot, the snapshot sharing the longest
  leading run of chunks (same upstream model + `upstream_config_id` only,
  at least `prefix_restore_min_chars`) is restored; llama-server keeps the
  common prefix and prefills only the divergent tail. Order: exact ->
  longest prefix -> `latest_alias`.
- Fails safe: `/apply-template` unavailable, unknown upstream model, or a
  below-threshold match all fall back to the previous exact-only behaviour.
  llama-server decides reuse token-by-token, so a poor pick costs one wasted
  restore, never wrong output.
- New counters `kv_prefix_restores` / `kv_prefix_restore_misses`; restore
  metric records gain `via_prefix`, `prefix_source`, `match_chars`,
  `prompt_chars` (how deep the new prompt matched -- the number to watch).
- Sidecars are TTL-evicted with their snapshot and reloaded at startup.
- Only renders when it will be used (cold slot, or a save that is due), and
  re-confirms the upstream model with a fresh `/props` read before any prefix
  restore. A failed prefix restore is recorded `served: "cold"`.
- Caveats: dense-attention models only (hybrid/recurrent models can't truncate a
  restored slot); per-run-changing prompts need `recur_threshold = 1` to be
  saved at all (startup warning otherwise).

### Added -- dashboard: temperature card + multi-host nav

- `slotpin-dash` now polls CPU (`k10temp`/Tctl) and GPU (`amdgpu`/edge)
  temperature straight from hwmon sysfs (`/sys/class/hwmon/hwmon*/temp1_input`,
  matched by the sibling `name` file; NVMe composite too, if present) on the
  same cadence as the energy/mem samples, folds it into the 5-min rollup
  buckets, and shows it as a live + trend card (avg and max, amber/red near
  `DASH_TEMP_WARN_C`/`DASH_TEMP_CRIT_C`) plus a sparkline. No fan-control
  daemon dependency -- see `docs/dashboard.md`.
- New header host switcher: `DASH_LABEL` + `DASH_BOXES` (`name=url,...`, e.g.
  `box-a=http://192.0.2.10:8091,box-b=http://192.0.2.11:8091`) render
  cross-links between sibling hosts' dashboards, with the current host's tab
  highlighted.

### Added -- tmpfs / slot-save-path hardening

Guards against the class of misconfig where KV snapshots land on a RAM
filesystem (a `--slot-save-path` on `/tmp`), which balloons memory to OOM and
steals decode bandwidth on a unified-memory APU. See `docs/operations.md`
hard-rule 6.

- `sysmem.filesystem_type()` / `is_tmpfs()` -- fail-open `/proc/mounts` lookup.
- `config.runtime_path_warnings()` -- surfaced at `slotpin-proxy --check` and at
  startup: warns when `slot_save_path` (or the metrics dir) is on tmpfs/ramfs.
- `kvstore.save_now()` -- one-time warning if a save reports success but the
  file never appears in `slot_save_path` (the upstream `--slot-save-path`
  differs -- the exact tell that KV is going elsewhere, e.g. tmpfs).

### Changed

- Require Python **3.11+** (`requires-python = ">=3.11"`, ruff `target-version = py311`)
  and drop 3.10 from CI. The package already imported `tomllib` (3.11+ stdlib),
  so 3.10 never actually worked; this makes the declared support honest and CI
  green. Version bumped to `1.0.0a3`.

### Added -- `slotpin-dash` read-only dashboard

A stdlib-only web dashboard (`slotpin-dash` console entry point) that observes a
running proxy and serves a live view plus bucketed `1h/24h/72h/7d` trends. See
`docs/dashboard.md`.

- Live view: free-RAM (OOM early-warning), power, throughput, cache-hit %,
  decode t/s (aggregate + per-task when concurrency > 1), MTP acceptance %,
  TTFT, warm-lane %, Wh/req, live per-slot state, and a recent-request stream.
- Trends: a background collector folds requests + energy/mem samples into
  5-min buckets persisted to SQLite (`StateDirectory`), backfilled from
  `metrics.jsonl` on startup; windows show p50/p95 decode & TTFT, free-RAM
  low-water, kWh/day, and inline-SVG sparklines with model up/down markers.
- Reads only (`metrics.jsonl`, `/slots`, energy feed, `/proc/meminfo`); ships
  `deploy/systemd/slotpin-dash.service` + `.env.example` and
  `deploy/logrotate/slotpin-metrics` to cap the unbounded metric stream.

### Added -- v8 pooled multi-slot concurrency

Single-model, multi-slot pooled concurrency (Increment A of
`docs/concurrency-design.md`); multi-model routing is designed, not built.

- `slots` config (= `llama-server -np N`): turns are pooled across slots and
  routed post-admission to a free slot, preferring one already holding the
  prefix's KV (warm follow-up, no restore). No role is pinned to a slot.
- Slot held busy through a synchronous owns-slot save (`kvstore.save_now`) then
  freed before ticket release, so a save always captures its own turn's KV.
- Scheduler is admission-only; the server owns precise per-slot `slot_busy`.
- Concurrency-fair energy: per-turn `concurrency` (mean live turns) and
  `energy_fair_wh = energy_turn_wh / concurrency`.
- Config invariants enforced: `1 <= max_inflight <= slots`, and `slots > 1`
  requires `sched_enabled`.
- Hardening from an independent audit: per-waiter queue timeout (was a shared
  deadline shedding the whole queue); tracking/save gated on a 2xx status with
  `slot_last` reverted on error; fail-closed 500 on pre-response errors;
  keep-alive close on relay write failure; `warm_worthy` made a pure query with
  recurrence recorded once per turn; stale energy poll -> None; ISO-8601 metric
  timestamps.

### Added -- v7 metric-supervisor layer (`1.0.0a2`)

Passive per-turn capture extended to make the proxy the deterministic on-box
metric supervisor (measurement off the orchestrating agent's plate), plus a
reproducible durable install. All capture additions are fail-open and do not
change the request path's behaviour.

- `server.py`: **TTFT** (`ttft_s`) -- the first relayed chunk is timestamped
  in `_forward_and_relay` (reset per-call for keep-alive safety); emitted as
  `first_chunk - t0`, i.e. user-visible time-to-first-token (queue + any
  restore + prefill).
- `server.py` + `config.py`: **run/experiment id** (`run_id`) -- a per-request
  `X-Slotpin-Run-Id` header overrides a new optional `run_id` config default
  (new `str_or_none` config kind), so a benchmarking harness can tag runs
  without a config change; `null` for normal production traffic.
- `server.py`: **spec/MTP acceptance** (`draft_n`, `draft_n_accepted`,
  derived `accept_rate`) -- recovered from the same `timings` object as
  `predicted_per_second` (llama-server `server_slot_stats::to_json`, present
  only when `n_draft_tokens > 0`), so no separate `/slots` or `/metrics`
  side-read is needed. `null` when speculative decoding is inactive.
- `sysmem.py` (new): **fail-open GPU/host memory sampler** -- `gtt_used_mb`,
  `vram_used_mb`, `mem_available_mb`, sampled point-in-time at metric-write
  from sysfs (`/sys/class/drm/card*/device/mem_info_*`) + `/proc/meminfo`.
  Linux+amdgpu specific; all-`null` elsewhere. (Peak-over-turn sampling is a
  noted future refinement.)
- `deploy/install.sh` (new) + a box profile config (new): **reproducible
  durable install** for identical hosts -- `/opt/slotpin` venv,
  `/usr/local/bin/slotpin-proxy` symlink, `/etc/slotpin/config.toml`,
  `/var/lib/slotpin` data (run-user owned), and a systemd unit with
  `User=<run-user>` + matching `ReadWritePaths`. Idempotent; same command on
  every host.
- Tests: `test_sysmem.py`, `test_usage_parse.py` (acceptance extraction
  exactness, no `draft_n`/`draft_n_accepted` confusion, absent-when-inactive).

Deferred (documented for the next window): fetch + wire `llama-swap` and its
reload-pairing drop-in (gated on the release-binary download); live
end-to-end MTP acceptance validation (needs a compiled MTP build -- the
acceptance capture is source-confirmed + unit-tested here).

## Behavioural alignment with the original single-file proxy

The rewrite was diffed behaviour-by-behaviour against the original single-file
proxy it replaces; each divergence was ported, kept and documented, or dropped.
Highlights:

### Fixed

- `classify.py`: `prefix_hash` is now byte-identical to `fp` (the original's
  actual computation), not a separately-windowed 12-char hash.
- `config.py`: `bytes_per_token`/`cram_safety` defaults corrected to the
  originally deployed values (16900 / 0.85, was 17300 / 0.9).
- `kvstore.py`: interactive-lane alias maintenance is a symlink (the original
  mechanism), not a file copy.
- `server.py`: `served` warm/cold classification polarity corrected (a turn
  with a disk restore is "warm", not "cold" -- was inverted); `kv_persist` is
  now the master gate for all disk-warmth activity on both the restore and save
  paths (previously only partially gated, letting saves/restores fire with
  `kv_persist=false`).
- `scheduler.py`: ported the original's anti-starvation aging (a queued
  request's effective priority rises toward the top tier as it waits) and
  optional backpressure shedding -- the prior strict-tier-FIFO scheduler
  could starve a low-tier request indefinitely under sustained higher-tier
  traffic.
- `classify.py`/`config.example.toml`: ported the label-keyed scheduler tier
  (`sched_tier()`) via `LabelRule.tier` + `Config.default_tier` -- background
  jobs (synthesis/triage) previously shared a tier with user-facing scheduled
  jobs instead of running at the lower priority.
- `deploy/systemd/slotpin-proxy.service`: added the missing
  `After=`/`Wants=llama-swap.service` ordering.

### Documentation corrections

`reload_detect.py` and `kvstore.py`'s busy-save hardening were previously
described as ports of original behaviour. Both are new additions with no
counterpart in the original. Kept (neither contradicts anything it did, both
are reasoned improvements), but the attribution is corrected throughout.

## [1.0.0a1] - 2026-07-24

Initial rewrite of the original single-file proxy (`slotpin_proxy.py`) as a
package, reimplemented from its specification and later reconciled against the
original (see "Behavioural alignment" above).

### Added

- Packaged, modularised implementation (`src/slotpin/`) replacing the single-file
  `slotpin_proxy.py`: `config`, `classify`, `state`, `pool`, `slot_admin`,
  `kvstore`, `scheduler`, `reload_detect`, `metrics`, `stats`, `server`, `cli`.
  No framework dependency — stdlib only at runtime, by design.
- Validated configuration schema (`config.py`): TOML file (`tomllib`) + `SLOTPIN_*`
  env var overrides, with legacy env-var aliases preserved for systemd drop-in
  compatibility. Config errors are reported with the offending key name.
  Generalised classification: interactive lane, demote rules, and cosmetic
  labels all move to config (`[[classify.lanes]]`, `[[classify.demote]]`,
  `[[classify.labels]]`) — nothing gateway-specific is hard-coded;
  a reference profile ships as `config.example.toml`.
- Reset-on-reload detection (`reload_detect.py`): a background poller watches
  `llama-server`'s `/props` for a monotonic-signature regression (model
  path/name, process uptime/start-time, cumulative counters) and automatically
  clears the inferred cache-ram pool and per-slot occupant map on a detected
  reload. *(Note: the original's fix for this incident class was operational, not
  code. This module is a defense-in-depth addition, not a port.)*
- Busy-save hardening (`kvstore.py`): snapshot saves are refused (and retried
  ~2 s later) while the target slot is mid-turn. *(Note: no counterpart in the original proxy.)*
- Metrics file rotation (`metrics.py`): size-based rotation (`metrics_rotate_mb`)
  with a configurable number of retained generations (`metrics_keep`), replacing
  the previously-unbounded `jsonl` file.
- `/health` and `/ready` endpoints: `/health` returns an explicit
  `local_available` signal (200/`true` vs 503/`false`) derived from an upstream
  liveness probe, so callers (e.g. a gateway's provider config) can fail safe instead
  of silently falling back to cloud inference. `/ready` is a separate
  liveness/readiness split for systemd/orchestrator wiring.
- Formalised test suite (`tests/`) covering every module plus the previously-missing
  full-path integration test (`test_integration.py`), and CI
  (`.github/workflows/ci.yml`): `ruff check` + `pytest` across Python 3.10-3.13 (3.10 later dropped; see Unreleased).
- Deployment assets: hardened systemd unit (`deploy/systemd/slotpin-proxy.service`),
  legacy env-var drop-in template, and the llama-swap reload-pairing drop-in
  (`llama-swap-pair-slotpin.conf`).
- Documentation: `docs/architecture.md`, `docs/configuration.md`,
  `docs/operations.md`, `docs/metrics.md`.

### Status

Alpha (`1.0.0a1`) at the time of this entry; reconciled against the original
proxy afterwards (see "Behavioural alignment" above).
