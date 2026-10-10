# Metrics

Three things write to `metrics_file` (JSONL, one JSON object per line): a
per-turn event after every proxied inference request — OpenAI
`/v1/chat/completions`, OpenAI `/v1/completions`, or llama.cpp-native
`/completion` (raw-prompt) — a save/restore event for every KV snapshot
operation, and a `turn_failed` event for every inference turn that has no
per-turn record. All three inference endpoints get the same pin+instrument pipeline;
the native ones carry token counts at top level rather than in a `usage` object,
which the parser handles transparently. `MetricsWriter.write()`
never raises -- a metrics-write failure never breaks a request.

## Per-turn event schema

**Request success:** a per-turn record is written **only** for a request served
with a 2xx. A 4xx/5xx/shed/upstream-unreachable turn returns an HTTP error but
writes **no** per-turn record (recording it would pollute KV/warmth metrics);
it writes a separate `turn_failed` record instead (see below). So
record-existence for a `run_id` *is* the success signal, stated explicitly by the
`ok` field below. **Gate on `ok`, never on `served`** — `served` is KV warmth
(`"warm"`/`"cold"`), not success.

| Field | Type | Meaning |
|---|---|---|
| `ts` | str | Timestamp of the event. |
| `route` | str | The matched lane's configured `name` (for example the name of your interactive lane). The field is called `route` for historical reasons; the dashboard's route filter keys on it. |
| `slot` | int | The `id_slot` this turn was routed to. |
| `workload` | str | Cosmetic label (first `[[classify.labels]]` match, else lane name, else `other:<fp>`). |
| `run_id` | str \| null | Optional bench/experiment tag. Per-request `X-Slotpin-Run-Id` header, else the config `run_id` default, else `null`. Groups a whole bench. |
| `request_id` | str \| null | Per-request id — pass-through of the `X-Slotpin-Request-Id` header (distinct from `run_id`, which groups a bench). Makes each row individually addressable; `null` if the header is absent. |
| `config_fingerprint` | str | **Endpoint-independent** 16-hex identity of the serving config: `sha256` of proxy capacity/routing (`slots`, `max_inflight`, `cache_ram_mb`, upstream host:port) + upstream `model` + the explicit `upstream_config_id` (+ `upstream_kind` when not `llamacpp`, so llama.cpp values are unchanged). Same for chat / `/completion` / `/v1/completions` on one server; **differs for plain vs MTP** (via `upstream_config_id`); changes on any capacity/model/`upstream_config_id` change. Excludes the endpoint and the build by design. |
| `fingerprint_version` | int | Version of the `config_fingerprint` semantics (currently `2`; folded into the hash so versions never alias). |
| `upstream_build` | str \| null | Upstream build / `system_fingerprint` (e.g. `b10707-250b6144`), recorded **separately** as provenance — NOT part of the identity (a rebuild is not a different serving config). Present on chat responses; `null` when the build omits it (e.g. native `/completion`). |
| `fp` | str | sha1(system text)[:8] -- or sha1(body_text[:4000])[:8] if there's no system message. Per-turn identity. |
| `prefix` | str | `prefix_hash`, the adaptive cache key. Computed identically to `fp`, so `prefix == fp` in every record. (`prefix_hash_chars` is only the length of the body slice hashed when there is no system message.) |
| `cache_n` | int \| null | Tokens llama.cpp reused from its own in-slot/prefix cache this request. |
| `prompt_n` | int \| null | Total prompt tokens this request. |
| `completion_n` | int \| null | Completion tokens generated this request. |
| `dur_s` | float | Wall-clock duration of the whole turn (admission through relay completion). |
| `ttft_s` | float \| null | Time-to-first-token: first relayed chunk minus request arrival `t0` -- includes queue wait + any restore + prefill. `null` if no chunk was relayed. |
| `cacheram_restore` | bool | Whether the proxy inferred this turn's cache hit came from its modelled cache-ram pool (i.e. the slot had been overwritten since this prefix was last there, yet `cache_n` came back above baseline). |
| `restored_tokens` | int \| null | Tokens attributed to that inferred cache-ram restore, when `cacheram_restore` is true. |
| `decode_tps` | float \| null | Decode tokens/sec, from llama.cpp's `timings` in the final usage chunk. `null` if timings weren't present. |
| `prefill_tps` | float \| null | Prefill tokens/sec, same source. |
| `predicted_n` | int \| null | Predicted token count, same source. |
| `draft_n` | int \| null | Spec/MTP tokens drafted this turn, from `timings` (present only when speculative decoding is active). `null` otherwise. |
| `draft_n_accepted` | int \| null | Spec/MTP drafted tokens accepted, same source. |
| `accept_rate` | float \| null | Derived `draft_n_accepted / draft_n`; `null` when `draft_n` is absent/zero. |
| `stop_reason` | str \| null | Why generation stopped: chat `finish_reason` (`stop`/`length`/…) or native `/completion` `stop_type` (`eos`/`limit`/`word`/`none`). |
| `truncated` | bool | `True` when the **output** hit the generation budget (`stop_reason` is `limit`/`length`) — i.e. the answer was cut off at `n_predict`/`max_tokens`. Lets a consumer reject a budget-clipped turn as incomplete instead of scoring a chopped answer. (Distinct from llama.cpp's prompt-side `truncated`, which is not recorded.) |
| `ok` | bool | **Canonical request-success signal.** Always `true` in a written record (records exist only for 2xx turns; see the note above). Emitted explicitly — matching the save/restore record's `ok` — so consumers gate on `ok`, not on `served`. |
| `served` | `"warm"` \| `"cold"` | **KV warmth, NOT success.** `"warm"` iff this turn rode on preserved KV: a disk restore fired for it, or the proxy inferred a cache-ram restore (`cacheram_restore`). `"cold"` otherwise (the prompt had to be prefilled from scratch). Note that a turn that simply continues in the slot's own resident KV, with no occupant change and no restore, is recorded as `"cold"` by this rule even though its `cache_n` is high; read `cache_n`/`prompt_n` for the token-level hit. (Gufo: from its own cache report -- see below.) |
| `queue_wait_ms` | float | Milliseconds spent waiting for scheduler admission. |
| `tier` | int | The scheduler tier this turn was admitted at. |
| `concurrency` | float | Live turns during this turn = mean of the count at admission and at completion, incl. itself. The divisor for attributing shared whole-box energy under concurrency; 1.0 while `max_inflight=1`. (An exact time-weighted attribution by the poller is designed but not built; see `docs/concurrency-design.md`.) |
| `gtt_used_mb` | float \| null | GPU GTT memory in use — the **end**-of-turn value (latest poll). `null` on non-amdgpu/non-Linux hosts. |
| `gtt_used_start_mb` | float \| null | GTT in use at turn **start** (`MemPoller`, first poll in `[t0, end]`). |
| `gtt_used_avg_mb` | float \| null | Mean GTT in use over the turn window. |
| `gtt_used_max_mb` | float \| null | **Peak** GTT residency over the turn — the intra-turn high-water mark an end-of-turn spot sample misses (e.g. a large-context prefill spike). |
| `vram_used_mb` | float \| null | GPU VRAM in use (end-of-turn), same source. |
| `mem_available_mb` | float \| null | Host `MemAvailable` (`/proc/meminfo`) — **end**-of-turn value. Total RAM in use = `MemTotal − mem_available_mb`; on unified memory that total already includes the GTT-pinned pages. |
| `mem_available_start_mb` | float \| null | `MemAvailable` at turn start. |
| `mem_available_avg_mb` | float \| null | Mean `MemAvailable` over the turn window. |
| `mem_available_min_mb` | float \| null | **Minimum** available RAM over the turn = **peak RAM pressure** (worst-case fit point). |
| `soc_power_w` | float \| null | On-box APU/GPU package power, from amdgpu hwmon `power1_average`. Compute-side, instantaneous. |
| `plug_power_w` | float \| null | Whole-box power, scraped from the ESPHome smart-plug's `power` sensor. `null` unless `energy_url` is set. |
| `energy_kwh` | float \| null | Whole-box cumulative energy counter from the plug (`energy_metric`, kWh). Use a hi-res (`accuracy_decimals: 5`) ESPHome sensor -- the stock 2-decimal counter is too coarse. |
| `energy_turn_wh` | float \| null | Whole-box energy over this turn's window: plug counter delta from turn start to end (Wh). Under concurrency this counts ALL box energy in the window (shared by concurrent turns) — see `energy_fair_wh`. `null` if a snapshot was missing or the counter reset mid-turn. |
| `energy_fair_wh` | float \| null | This turn's concurrency-fair energy share: `energy_turn_wh / concurrency`. Equals `energy_turn_wh` for sequential turns (N=1). Sum over a run ÷ correct-count = Wh/correct even under load. |
| `energy_idle_wh` | float \| null | Whole-box energy consumed **idle since the previous turn**: counter delta from the previous turn's end to this turn's start (Wh). `null` on the first turn and across a counter reset. |

### Gufo fields (`upstream_kind = "gufo"` only)

With `upstream_kind = "gufo"` each turn record also carries Gufo's own cache
report, parsed from `usage.gufo` in the final chunk (or non-streamed body).
These keys are **absent** in `llamacpp` mode, whose records are unchanged. If a
reply carries no `usage.gufo` object, all seven are `null`. In this mode
`served` is `"warm"` iff `upstream_cache` is `"memory"` or `"disk"`, and
`cacheram_restore` / `restored_tokens` are always `false` / `0`; `slot` is
slotpin's internal admission index (Gufo picks its own session).

| Field | Type | Meaning |
|---|---|---|
| `upstream_cache` | `"memory"` \| `"disk"` \| `"miss"` \| null | `"disk"` when `cache_disk_hit`; `"memory"` when `cache_hit` and not a disk hit; `"miss"` otherwise. |
| `upstream_cache_miss_reason` | str \| null | Gufo's `cache_miss_reason` (e.g. `prefix_changed`); `null` on a hit. |
| `upstream_restore_ms` | float \| null | `cache_restore_ms`: time restoring cached KV for this turn. |
| `upstream_restore_bytes` | int \| null | `cache_restore_bytes`: KV bytes restored. |
| `upstream_snapshot_ms` | float \| null | `cache_snapshot_ms`: time snapshotting this turn's KV for reuse. |
| `upstream_snapshot_bytes` | int \| null | `cache_snapshot_bytes`: bytes snapshotted. |
| `upstream_disk_queued_bytes` | int \| null | `cache_disk_queued_bytes`: bytes queued for the disk cache writer. |

## Failed-turn event schema

Written once for each inference turn (the classified, scheduled path above; not
raw pass-through of non-inference paths) that does not produce a per-turn
record. It is a separate record kind: per-turn records are unchanged, and a turn
produces exactly one of the two. Consumers tell it apart by `event`; it carries
no `ok`, `kv_adaptive` or token-count keys. It never contains prompt or reply
text.

| Field | Type | Meaning |
|---|---|---|
| `event` | `"turn_failed"` | Record kind. |
| `ts` | str | Timestamp (same format as the per-turn record). |
| `reason` | str | Cause, one of the values below. |
| `route` | str | The matched lane's configured `name`. |
| `slot` | int \| null | Slot the turn was routed to; `null` for `shed` (never admitted). |
| `prefix` | str | `prefix_hash`, as in the per-turn record. |
| `dur_s` | float | Wall-clock seconds from request arrival to the failure being known (for `shed`, includes the queue wait). |
| `upstream_status` | int | HTTP status of the upstream reply. Omitted when the upstream never sent a status line. |

`prompt_n` is deliberately not recorded: a failed turn has no usage block.

| `reason` | Meaning |
|---|---|
| `aborted_client` | The client went away: disconnected before the upstream responded or mid-stream, or a write to it failed or timed out. The upstream is aborted. |
| `upstream_4xx` | The upstream replied with a 4xx status (for example a rejected request body). `upstream_status` has the code. |
| `upstream_5xx` | The upstream replied with any other non-2xx status. `upstream_status` has the code. |
| `upstream_incomplete` | The upstream replied 2xx but the body was cut short (declared length not delivered, connection closed, no terminal frame) or carried an in-stream error frame. |
| `timeout` | A socket timeout (`upstream_timeout_s`) waiting for the upstream response headers, or mid-stream while reading it. |
| `stalled` | A streaming chat turn produced no output event (text, reasoning or tool-call delta) and showed no token progress in the upstream's `/slots` counters for `upstream_stall_timeout_s`, before the first token or between tokens; keep-alive comments and the role-only first event do not count. The upstream is aborted; the client got a 504 `upstream_stalled` if nothing had been sent to it, else a cut-off stream. `upstream_status` is present when the upstream had sent its status line. Only written when `upstream_stall_timeout_s` is set (see `docs/configuration.md`). |
| `shed` | The scheduler refused admission (queue timeout, or the client left while queued); the client got a 503. |
| `unreachable` | The upstream could not be connected to or the request could not be sent; the client got a 502. |

When several apply, the first match wins in this order: `shed`, `aborted_client` (client gone before a status line), `stalled`/`timeout`/`unreachable` (before a status line), `aborted_client`, `stalled`, `timeout` (mid-stream), `upstream_4xx`/`upstream_5xx`, `upstream_incomplete`. Between `aborted_client` and `stalled`, whichever happened first names the record. `stalled` and `timeout` never both apply: whichever limit is reached first names the record (the stall limit, when it is set below `upstream_timeout_s`).

## Save/restore event schema

Written by `kvstore.py` around every `SlotAdmin.save`/`.restore` call.

| Field | Type | Meaning |
|---|---|---|
| `ts` | str | Timestamp. |
| `kv_adaptive` | `"save"` \| `"restore"` | Which operation this event describes. |
| `slot` | int | Target slot. |
| `prefix` | str | The `prefix_hash` (or alias name) this snapshot is keyed on. |
| `file` | str | Snapshot filename involved (`<prefix_hash>.bin`, `.bin.ckpt`, or the `latest_alias` file). |
| `ok` | bool | Whether the admin API call succeeded (2xx). |
| `dur_s` | float | Duration of the admin call. |
| `bytes` | int \| null | Size of the snapshot file involved, from one `os.stat` of `slot_save_path/<file>` after a successful save or restore (aliases are followed). `null` when the call failed or the file is not visible to the proxy (remote upstream, `--slot-save-path` elsewhere, stat error). Absent on records written by older versions. |

`restore` events carry extra fields: `via_alias` (bool; true when the restore
used the `latest_alias` fallback) and, for a longest-common-prefix restore
(`prefix_restore`), `via_prefix: true`, `prefix_source` (the snapshot's prefix
hash), `match_chars` (shared rendered characters) and `prompt_chars` (rendered
prompt length). `ts` here is local time without a UTC offset
(`YYYY-MM-DDTHH:MM:SS`), unlike the per-turn and `turn_failed` records, which
carry an offset.

A `restore` event with `ok: false` is a MISS (no snapshot for that key, or
the admin call itself failed) -- see `docs/troubleshooting.md` for how to read a
MISS in context. Saves are rate-limited per prefix (`prefix_save_min_interval_s`);
a save skipped by the rate limit writes no event.

## Rotation

Controlled by `metrics_rotate_mb` (default 32.0 MiB) and `metrics_keep`
(default 3). `MetricsWriter.maybe_rotate()` is checked around each write:
when the current file exceeds `metrics_rotate_mb`, it's rotated to `.1`,
the previous `.1` becomes `.2`, and so on, keeping at most `metrics_keep`
rotated generations (older ones are deleted). The dashboard backfill also reads
these rotated siblings. `deploy/logrotate/slotpin-metrics` is an alternative if
you prefer system log rotation.

## Privacy

No prompt or reply text is ever written to the metrics file: records carry
token counts, timings, hashes (`fp`, `prefix`) and the lane/label names only.
Tests assert this: `tests/test_fail_fast.py` plants a `SECRET-PROMPT-TEXT` in
the upstream `/slots` payload and asserts (helper `_no_prompt_text`) that it is
absent from the logs, the metrics file and the client responses, and
`tests/test_dash_health.py` does the same with `SECRET` / `TOP SECRET` markers
for the dashboard (`test_slot_fields_drop_prompt_text`,
`test_collector_path_never_stores_or_serves_prompt`).

## `/_stats` reference

`GET /_stats` returns JSON built by `stats.build_stats(cfg, state, pool, kv,
sched)`. It has seven blocks:

- **`config`** -- a safe subset of the running config: `upstream_kind`, `mode`,
  `sched_enabled`, `kv_persist`, `kv_adaptive`, `cache_ram_mb`,
  `cram_token_cap`, `max_inflight`, `slot_save_path`, `upstream`, `admin` and
  `listen` (the last three as `host:port`). It includes filesystem paths. The
  endpoint is unauthenticated (see `SECURITY.md`), so keep the proxy on a
  trusted interface.
- **`counters`** -- cumulative counters: `turns`, `restores`, `evictions`,
  `restored_tokens`, `kv_saves`, `kv_restores`, `kv_restore_misses`,
  `kv_prefix_restores`, `kv_prefix_restore_misses`, `reloads_detected`.
  (`restores`/`restored_tokens` count the proxy's *inferred* cache-ram
  restores; `kv_*` count disk snapshot operations.) The scheduler also adds a
  `shed` counter once a request has been shed.
- **`slots`** -- per-slot current state, keyed by slot index: `last_prefix`
  (last-known occupant), `last_workload` (its label) and `busy` (whether a turn
  currently holds the slot).
- **`inferred_cacheram_pool`** -- `CacheRamPool.snapshot()`: a most-recent-first
  list of `{"fp", "tokens", "evicted_ts"}` for the fingerprints the proxy
  believes are still warm in llama-server's host-RAM prompt cache.
- **`adaptive`** -- `KVStore.snapshot_stats()`: `tracked_prefixes`,
  `warm_prefixes`, `prefix_counts` (per-prefix occurrence counts) and
  `last_save_age_s` (seconds since each prefix's last successful save).
- **`scheduler`** -- `Scheduler.snapshot()`: `sched_enabled`, `max_inflight`,
  `inflight`, `max_wait_s`, `backpressure`, and a `tiers` map (keyed by raw
  tier) with `queued`, `shed`, `wait_ms_p50` and `wait_ms_p95`.
- **`recent`** -- aggregates over the last 200 served turns (the
  `state.recent` ring): `count`, `decode_tps_p50`, `warm`, `cold`, `warm_ratio`
  and `interactive_wait_ms_p95`.
