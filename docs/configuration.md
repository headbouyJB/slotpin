# Configuration reference

Every key in `Config` (`src/slotpin/config.py`), with its TOML name, environment variable, type, default and meaning. Precedence: built-in defaults < `config.toml` < `SLOTPIN_*` environment variables. Pass the file with `slotpin-proxy --config PATH`.

- Every scalar key can be set from the environment as `SLOTPIN_<UPPER_SNAKE_NAME>`. Two keys also accept an older spelling, shown as "legacy alias" in the table; both work.
- Boolean values (TOML or env) accept `true`/`false`, `on`/`off`, `1`/`0`, `yes`/`no` (case-insensitive) from the environment, and TOML booleans in the file.
- For keys typed "path or null" or "str or null", the env values `off`, `none`, `null`, `disabled` and the empty string mean `null`.
- Unknown top-level TOML keys are an error. `lanes`, `demote` and `labels` come from the `[[classify.lanes]]`, `[[classify.demote]]` and `[[classify.labels]]` tables and cannot be set from the environment.
- `SLOTPIN_LOG_LEVEL` (default `INFO`) sets the log level; it is not a config key.
- Validate a file with `slotpin-proxy --config config.toml --check` (see [Validation rules](#validation-rules)).
- Defaults in the table are written one way: strings in double quotes, booleans as `true`/`false`, numbers as Python prints them (`48.0`, `2048`), paths as the path, `None` as `null`, lists as `[]`. A test (`tests/test_config_reference.py`) fails if this page and the code disagree.

| TOML key | Env var | Type | Default | Meaning |
|---|---|---|---|---|
| `listen_host` | `SLOTPIN_LISTEN_HOST` | str | `"127.0.0.1"` | Address the proxy listens on. Loopback by default; there is no authentication, so only widen it behind a VPN or an authenticating reverse proxy (see `SECURITY.md`). |
| `listen_port` | `SLOTPIN_LISTEN_PORT` | int | `8090` | Port the proxy listens on. `0` binds an ephemeral port (used by tests). `--port` on the command line overrides it. |
| `upstream_host` | `SLOTPIN_UPSTREAM_HOST` | str | `"127.0.0.1"` | Host of the server that handles normal requests: `llama-server` directly, `llama-swap` in front of it, or Gufo. |
| `upstream_port` | `SLOTPIN_UPSTREAM_PORT` | int | `8080` | Port of that server. |
| `admin_host` | `SLOTPIN_ADMIN_HOST` | str | `"127.0.0.1"` | Host of the inference server itself, used for slot save/restore calls, `/props`, `/apply-template` and the `/slots` stall probe. With `llama-swap` this is the backend `llama-server`, not `llama-swap`. For Gufo it is Gufo again. |
| `admin_port` | `SLOTPIN_ADMIN_PORT` | int | `5802` | Port of the admin endpoint. The defaults describe a `llama-swap` on 8080 fronting a `llama-server` on 5802; with a plain `llama-server` set `admin_port` equal to `upstream_port` (the example configs use 8080 for both). |
| `upstream_kind` | `SLOTPIN_UPSTREAM_KIND` | str | `"llamacpp"` | What serves behind the proxy. `"llamacpp"`: `llama-server` (`id_slot` pinning, KV save/restore, `/props` reload detection). `"gufo"`: the Gufo engine, which owns prompt caching, so no `id_slot` is sent, there is no save/restore, reload detection reads Gufo's `/v1/models` and `/metrics` on `admin_host:admin_port`, and turn records gain `upstream_*` cache fields. Any other value is a config error. See [Gufo-incompatible keys](#upstream_kind--gufo-incompatible-keys) and `docs/operations.md`. |
| `mode` | `SLOTPIN_MODE` | str | `"single"` | Accepted values `"single"` and `"dual"` (anything else is a config error). Slots are now chosen from the pool at admission time, so this key no longer affects routing; it is kept for compatibility. |
| `interactive_slot` | `SLOTPIN_INTERACTIVE_SLOT` | int | `1` | Legacy slot number for the interactive lane in `dual` mode. No longer affects routing (see `mode`). Must be >= 0. |
| `default_slot` | `SLOTPIN_DEFAULT_SLOT` | int | `0` | Legacy default slot number. No longer affects routing (see `mode`). Must be >= 0. |
| `kv_persist` | `SLOTPIN_KV_PERSIST` | bool | `false` | Master switch for disk save/restore of KV snapshots. Saves the interactive lane's slot after each turn and restores it before the next turn if the slot was taken by something else. Requires `llama-server --slot-save-path` equal to `slot_save_path`. Must be `false` for `upstream_kind = "gufo"`. |
| `kv_adaptive` | `SLOTPIN_KV_ADAPTIVE` | bool | `false` | Extend persistence from the interactive lane to any prompt prefix that recurs (`recur_threshold` times). Needs `kv_persist`. Must be `false` for Gufo. |
| `sched_enabled` | `SLOTPIN_SCHED_ENABLED` (legacy alias `SLOTPIN_SCHED`) | bool | `false` | Enable the priority scheduler (tiers, aging, queue limit). When `false`, requests are admitted immediately and in-flight turns are only counted for stats. Required when `slots > 1`. |
| `slot_save_path` | `SLOTPIN_SLOT_SAVE_PATH` | path | `/var/lib/slotpin/kv-cache` | Directory for `<prefix_hash>.bin` (and `.bin.ckpt`) snapshots, `.idx.json` sidecars and the `latest_alias` link. Must be on persistent disk, not tmpfs, and must equal the `llama-server --slot-save-path`. `--check` warns on tmpfs. |
| `recur_threshold` | `SLOTPIN_RECUR_THRESHOLD` | int | `3` | How many times a prefix must be seen before a non-interactive lane's prefix is worth a snapshot. The interactive lane is warm-worthy on first sight. Must be a positive integer. |
| `snapshot_ttl_h` | `SLOTPIN_SNAPSHOT_TTL_H` | float | `48.0` | Age in hours after which a snapshot file is opportunistically deleted. Must be > 0. |
| `prefix_save_min_interval_s` | `SLOTPIN_PREFIX_SAVE_MIN_INTERVAL_S` | float | `300.0` | Minimum seconds between saves of the same prefix hash (rate limit). Must be >= 0. |
| `prefix_restore` | `SLOTPIN_PREFIX_RESTORE` | bool | `false` | Longest-common-prefix disk restore. Snapshots are keyed on a hash of the whole system message, so a changed tail (a date, a per-run brief) normally forfeits the snapshot. With this on, a cold request with no exact snapshot restores the saved snapshot whose rendered prompt (from `llama-server /apply-template`) shares the longest leading run of chunks, and `llama-server` prefills only the rest. Writes a `<hash>.idx.json` sidecar per snapshot. Requires `kv_persist`; applies to the interactive lane and, with `kv_adaptive`, to every lane. **Dense-attention models only**: hybrid or recurrent models cannot truncate a restored slot to the shared prefix, so a partial match re-prefills everything after paying the restore. Must be `false` for Gufo. See `docs/how-it-works.md`. |
| `prefix_chunk_chars` | `SLOTPIN_PREFIX_CHUNK_CHARS` | int | `4096` | Chunk size (rendered characters) for prefix matching; a match is a whole number of chunks. Changing it makes existing sidecars unusable (they are ignored, not misread). Must be a positive integer. |
| `prefix_restore_min_chars` | `SLOTPIN_PREFIX_RESTORE_MIN_CHARS` | int | `32768` | Minimum shared rendered prefix, in characters (about 4 per token), worth a restore. Below this, reading the snapshot costs more than the prefill it saves. Must be >= 0. |
| `cache_ram_mb` | `SLOTPIN_CACHE_RAM_MB` | int | `2048` | **Must equal** `llama-server`'s `--cache-ram` (MiB). With `bytes_per_token` and `cram_safety` it sets the token budget of the inferred cache-ram pool. Must be > 0. |
| `bytes_per_token` | `SLOTPIN_BYTES_PER_TOKEN` | int | `16900` | KV-cache bytes per token for your model and KV quantisation (the default is for one q8-KV model). Recompute it for your model; it only sizes the inferred pool. Must be > 0. |
| `cram_safety` | `SLOTPIN_CRAM_SAFETY` | float | `0.85` | Safety factor on the pool's token cap: `cram_token_cap = cache_ram_mb * 1024**2 / bytes_per_token * cram_safety`. Must be in (0, 1.5]. |
| `slots` | `SLOTPIN_SLOTS` | int | `1` | Number of KV slots the upstream serves (`llama-server -np N`; for Gufo, at most `--sessions`). Turns are pooled across `0..slots-1`. Must satisfy `max_inflight <= slots`; `slots > 1` requires `sched_enabled = true`. |
| `max_inflight` | `SLOTPIN_MAX_INFLIGHT` | int | `1` | Maximum turns admitted at once. Set it equal to `slots` for full concurrency. Must be a positive integer and at most `slots`. |
| `queue_timeout_s` | `SLOTPIN_QUEUE_TIMEOUT_S` | float | `600.0` | Longest a request may wait for admission before it is shed with a 503. Hard backstop in addition to aging. Must be > 0. |
| `max_wait_s` | `SLOTPIN_MAX_WAIT_S` | float | `60.0` | Anti-starvation aging horizon: a queued waiter's effective tier rises continuously toward the top priority and reaches it once it has waited `max_wait_s`. Must be > 0. |
| `sched_backpressure` | `SLOTPIN_SCHED_BACKPRESSURE` | bool | `false` | Shed low-priority waiters under sustained saturation (see `backpressure_min_tier`, `bp_drop_s`). |
| `backpressure_min_tier` | `SLOTPIN_BACKPRESSURE_MIN_TIER` | int | `20` | A waiter's tier must be >= this to be eligible for backpressure shedding. Must be >= 0. |
| `bp_drop_s` | `SLOTPIN_BP_DROP_S` | float | `45.0` | How long a backpressure-eligible waiter may queue before it is shed. Must be > 0. |
| `upstream_timeout_s` | `SLOTPIN_UPSTREAM_TIMEOUT_S` | float | `1800.0` | Socket timeout for the proxied upstream request (waiting for headers, and between reads). Must be > 0. |
| `upstream_stall_timeout_s` | `SLOTPIN_UPSTREAM_STALL_TIMEOUT_S` | float | `0.0` | Streaming chat completions only: longest time (seconds) with no real output event **and** no token progress upstream before the upstream request is aborted. `0` = off. Must be finite and >= 0, and should be below `upstream_timeout_s`. See [Fail-fast on a silent upstream](#fail-fast-on-a-silent-upstream). |
| `hold_headers_until_first_output` | `SLOTPIN_HOLD_HEADERS_UNTIL_FIRST_OUTPUT` | bool | `false` | Streaming chat completions only: send the client nothing (no status line, headers or body) until the first real output event, so a stall before it is a clean `504` rather than a cut `200` stream. See [Fail-fast on a silent upstream](#fail-fast-on-a-silent-upstream). |
| `admin_timeout_s` | `SLOTPIN_ADMIN_TIMEOUT_S` | float | `30.0` | Timeout for admin calls (slot save/restore, `/apply-template`). Must be > 0. |
| `downstream_write_timeout_s` | `SLOTPIN_DOWNSTREAM_WRITE_TIMEOUT_S` | float | `30.0` | `SO_SNDTIMEO` on the client-facing socket while relaying; a client that stops reading is cut off after this. Must be > 0. |
| `metrics_file` | `SLOTPIN_METRICS_FILE` (legacy alias `SLOTPIN_METRICS`) | path or null | `/var/lib/slotpin/metrics.jsonl` | JSONL metrics output path. `null` (or `off`, `none`, `disabled`, or an empty value in an env var) disables metrics writing. The dashboard reads this file. |
| `metrics_rotate_mb` | `SLOTPIN_METRICS_ROTATE_MB` | float | `32.0` | Size in MiB at which the metrics file is rotated. Must be > 0. |
| `metrics_keep` | `SLOTPIN_METRICS_KEEP` | int | `3` | Number of rotated generations (`.1`, `.2`, ...) kept. Must be a positive integer. |
| `run_id` | `SLOTPIN_RUN_ID` | str or null | `null` | Optional tag stamped on every per-turn record, for grouping a benchmark run. A per-request `X-Slotpin-Run-Id` header overrides it. |
| `upstream_config_id` | `SLOTPIN_UPSTREAM_CONFIG_ID` | str or null | `null` | Optional name for the upstream serving configuration, folded into `config_fingerprint` so configs that share a model and capacity but differ in serving flags (for example plain versus speculative decoding) do not collide. The proxy cannot derive this from `/props`. The dashboard also shows it. |
| `energy_url` | `SLOTPIN_ENERGY_URL` | str or null | `null` | Smart-plug endpoint polled for whole-box energy: an ESPHome Prometheus `/metrics` URL (set `energy_metric`) or a `/sensor/<id>` JSON URL. `null` disables energy capture. See `docs/energy.md`. |
| `energy_timeout_s` | `SLOTPIN_ENERGY_TIMEOUT_S` | float | `1.0` | HTTP timeout for each energy poll. |
| `energy_poll_s` | `SLOTPIN_ENERGY_POLL_S` | float | `1.0` | Background energy-poll interval in seconds; per-turn snapshots read the cached value. Match the plug sensor's own update interval. |
| `energy_metric` | `SLOTPIN_ENERGY_METRIC` | str or null | `null` | When `energy_url` is a Prometheus `/metrics` endpoint, the sensor `id` of the energy counter to read (for example `total_energy`). |
| `mem_poll_s` | `SLOTPIN_MEM_POLL_S` | float | `1.0` | Background memory-poll interval in seconds (GPU memory from sysfs, `MemAvailable` from `/proc/meminfo`), so each turn records start, average, and peak or minimum values. Linux only; the columns are empty elsewhere. Must be > 0. |
| `reload_detect` | `SLOTPIN_RELOAD_DETECT` | bool | `true` | Run the background reload detector, which clears the pool and slot-occupancy state when the upstream model or process changes. See `docs/how-it-works.md`. |
| `reload_poll_s` | `SLOTPIN_RELOAD_POLL_S` | float | `10.0` | Reload-detector poll interval in seconds (`/props`; for Gufo, `/v1/models` and `/metrics`). Must be > 0. |
| `prefix_hash_chars` | `SLOTPIN_PREFIX_HASH_CHARS` | int | `4000` | Length of the request-body slice hashed **only** when a request has no system message. Otherwise the prefix hash is the hash of the full system text. Must be a positive integer. |
| `latest_alias` | `SLOTPIN_LATEST_ALIAS` | str | `"interactive-latest"` | Name of the fallback snapshot alias (a symlink in `slot_save_path`) kept for the interactive lane; used when the turn's own snapshot is missing. |
| `capture_400` | `SLOTPIN_CAPTURE_400` | bool | `false` | Debugging aid: write upstream 400 responses to a file under `/tmp`. Leave off. |
| `default_tier` | `SLOTPIN_DEFAULT_TIER` | int | `10` | Scheduler tier for a request that matches no lane and no label with its own tier. Lower numbers run first. Must be >= 0. |
| `lanes` | *TOML only* (`[[classify.lanes]]`) | list of LaneRule | `[]` | Lane classification rules, in order; first match wins. With none configured every request is lane `"default"` at tier `default_tier`. |
| `demote` | *TOML only* (`[[classify.demote]]`) | list of DemoteRule | `[]` | Demote rules: move matching short turns (health checks, heartbeats) out of an interactive lane. |
| `labels` | *TOML only* (`[[classify.labels]]`) | list of LabelRule | `[]` | Workload labels (first match wins) with an optional per-label tier. |

`lanes`, `demote` and `labels` are structured lists and are not overridable by a single scalar env var; configure them in `config.toml`. The default `[]` means "none configured".

## Fail-fast on a silent upstream

A stalled engine can look alive: it has answered `200`, sent the role-only first event, and then sends an SSE comment (`: ping`) every few seconds for ever. Bytes keep arriving, so neither `upstream_timeout_s` (a socket timeout) nor the client's own timeout fires, and the client never falls back to another provider. Two keys, both off by default (with both off the relay is unchanged), bound this. They apply only to `POST .../v1/chat/completions` with `"stream": true` whose upstream reply is a 2xx `text/event-stream`; non-streaming requests, `/completion`, `/v1/completions` and every pass-through path are never affected.

**`upstream_stall_timeout_s`** is a limit on *no stream output and no token progress*. *Real output* is an SSE `data:` event carrying a text (`content`), reasoning (`reasoning_content`) or tool-call (`tool_calls`) delta; this is the same test that times `ttft_s`. Comment lines, the role-only first event, usage/finish frames and anything else do not count. The clock starts when the request has been sent upstream and restarts at each output event, so it covers both prefill (before the first token) and gaps between tokens. It does not depend on the upstream sending anything: a completely silent upstream trips it too.

Stream silence alone is not a stall. Some engines (Gufo among them) do not stream tool-call arguments: while one decodes a tool call the stream carries only pings, then one `tool_calls` delta, so a long tool write looks silent. Each time the stream has been silent for half the limit, the proxy therefore reads the upstream's `/slots` (on `admin_host:admin_port`, one GET, timeout `min(admin_timeout_s, 10)` seconds) and notes, for every entry with `is_processing` true, `n_prompt_tokens_processed` and `next_token[0].n_decoded`. Only those numbers are kept, never the prompt text the entries carry. The silence is a stall when two consecutive readings are equal, or when the later one shows no busy slot, fails or times out. So:

- nothing moving: the stall is declared at the limit (readings at half the limit and at the limit);
- counters advancing: the turn is left alone for as long as they advance, re-read every half limit;
- progress stops later: detection takes between 1x and about 1.5x the limit from the last progress, plus the probe's own duration.

Limits of the probe:

- It covers **all busy slots together**. With Gufo the proxy's slot number does not map to an upstream session (Gufo ignores `id_slot`), so on a multi-slot box one stalled request is not detected while any other request advances.
- A llama.cpp upstream that omits a counter reports it as unknown; unknown counters never differ, so there the stream-silence rule alone applies.
- Whether the prompt counter advances during prefill depends on the engine, so keep the limit above the longest cold prefill regardless (below).

On a stall the proxy aborts the upstream request (as for a client disconnect), logs one warning, writes one `turn_failed` record with reason `stalled`, never saves or tracks the turn, and

- if nothing has been sent to the client yet, replies `504 {"error": "upstream_stalled", "local_available": false, "stalled_s": N}` and closes the connection;
- if the response has started, cuts the stream (no terminal chunk, connection closed), so the client sees a truncated reply.

**`hold_headers_until_first_output`** decides which of the two a prefill stall gets. Some engines send their `200` and role event before prefill, so without the hold a prefill stall can only be a cut stream. With it, the proxy buffers what the upstream sent and sends the client nothing until the first output event, then flushes status, headers and the buffered bytes in order and relays as usual. If the upstream ends, fails or is not a 2xx stream before any output, the client gets exactly what it would have had without the hold. The buffer is capped at 256 KiB (then flushed regardless). The hold works without a stall limit, but then nothing aborts a stalled upstream, so set both.

Choosing values:

- Set the limit **above the longest legitimate cold prefill** on your hardware: measure a cold prefill of your longest realistic prompt and add generous headroom.
- Keep it **well below `upstream_timeout_s`**. At or above it the socket timeout wins on a silent upstream and the turn is recorded as `timeout` (`slotpin-proxy --check` warns).
- With the hold on, the client receives no response headers during prefill: its response-header timeout must be longer than the limit, or it will give up first (recorded as `aborted_client`, upstream aborted as usual).
- After a stall the turn's slot stays reserved until the upstream reports it idle, for at most `min(upstream_timeout_s, 600)` seconds (the post-abort hold). The client has its answer by then, but turns queued behind it wait.

## `upstream_kind = "gufo"`: incompatible keys

Gufo keeps its own in-memory and disk continuation cache and has no `llama-server` slot save/restore API. With `upstream_kind = "gufo"`, any of `kv_persist`, `kv_adaptive` or `prefix_restore` set `true` is a config error (the message names the offending keys). Keys that only feed slotpin's llama.cpp cache model (`cache_ram_mb`, `bytes_per_token`, `cram_safety`, `slot_save_path`, `latest_alias`, `recur_threshold`, `snapshot_ttl_h`, `prefix_save_min_interval_s`, the `prefix_*` keys) are accepted but unused. `slots` and `max_inflight` still cap admission; keep them at or below Gufo's `--sessions`. See the "Running in front of Gufo" section of `docs/operations.md`.

## `[[classify.lanes]]` fields

| Field | Type | Default | Meaning |
|---|---|---|---|
| `name` | str | required | Lane name; becomes the metrics `route` value. |
| `match` | list[str] | required | Regexes; ALL must match the request body text (AND) for this lane to win. |
| `match_system` | list[str] | `[]` | Optional additional regexes; ALL must match the system-message text (AND). |
| `tier` | int | `1` | Scheduler tier (lower = higher priority). |
| `interactive` | bool | `false` | Marks this the interactive lane. At most one lane may set this. |
| `demote_to` | str or null | `null` | Lane name to switch to when a `[[classify.demote]]` rule fires against a request this lane matched. |

Lanes are tried in file order; the first whose `match` and `match_system` patterns all match wins.

## `[[classify.demote]]` fields

| Field | Type | Default | Meaning |
|---|---|---|---|
| `name` | str | required | Label applied when this rule fires (for example `"heartbeat"`). |
| `first_user_match` | str | required | Regex tested against the first user message. |
| `max_messages` | int | `40` | The rule only fires when the request has at most this many messages. |
| `tier` | int | `2` | Tier applied when the rule fires. |

When a demote rule fires, the request's label becomes the rule's `name`, its tier becomes the rule's `tier`, it stops being interactive, and its lane becomes the matched lane's `demote_to` if that is set.

## `[[classify.labels]]` fields

| Field | Type | Default | Meaning |
|---|---|---|---|
| `match` | str | required | Regex tested against the request body text. |
| `label` | str | required | Label applied on match (first match in list order wins). |
| `tier` | int or null | `null` | When set, replaces the lane or `default_tier` tier for a matching request. `null` leaves the tier alone. |

A label is cosmetic (the `workload` field in metrics) unless it carries a `tier`. With no label match the label is the lane name, or `other:<fp>` when no lane matched.

## Derived values

- `cram_token_cap` is computed from `cache_ram_mb`, `bytes_per_token` and `cram_safety` (formula in the table). It is not a config key. It bounds the inferred cache-ram pool (see `docs/how-it-works.md`).

## Dashboard environment variables

`slotpin-dash` is configured only by environment variables (not by `config.toml`); for a systemd install they live in `/etc/slotpin/slotpin-dash.env`. See `docs/dashboard.md`. The data-source variables must point at the same deployment the proxy uses.

| Variable | Default | Meaning |
|---|---|---|
| `DASH_HOST` | `127.0.0.1` | Address the dashboard listens on. It has no authentication; widen it only behind a VPN or an authenticating reverse proxy. |
| `DASH_PORT` | `8091` | Port the dashboard listens on. |
| `DASH_PROXY` | `127.0.0.1:8090` | `host:port` of the proxy, probed with a TCP connect to show whether it is up. |
| `DASH_UPSTREAM` | `http://127.0.0.1:5802` | Base URL of the inference server's admin endpoint, where `/slots` is read. Match the proxy's `admin_host:admin_port` (the example configs use port 8080, so set `http://127.0.0.1:8080`). |
| `DASH_METRICS` | `/var/lib/slotpin/metrics.jsonl` | The proxy's metrics file (the proxy's `metrics_file`). Rotated generations next to it are read for history. |
| `DASH_DB` | `/var/lib/slotpin-dash/rollup.db` | SQLite file for the dashboard's historical rollups. The directory must be writable. |
| `DASH_SLOTPIN_CFG` | `/etc/slotpin/config.toml` | The proxy's config file, read (never written) for `slots` and `upstream_config_id`. |
| `DASH_CFG` | *(empty)* | Fallback serving-config name shown when the proxy config has no `upstream_config_id`. |
| `DASH_ENERGY` | *(unset)* | URL of the smart plug's Prometheus `/metrics` endpoint for the live power card. Unset means the energy feed is off. See `docs/energy.md`. |
| `DASH_LABEL` | *(machine hostname)* | This box's name, shown in the header and matched against `DASH_BOXES` to highlight the active tab. |
| `DASH_BOXES` | *(empty)* | Box switcher: `name=url,name=url,...` (for example `box-a=http://192.0.2.10:8091,box-b=http://192.0.2.11:8091`). Empty means no switcher. |
| `DASH_WINDOW_MIN` | `15` | Length in minutes of the live window used for the live cards. |
| `DASH_POLL_SEC` | `3` | Seconds between polls of the upstream, proxy and energy feed. |
| `DASH_STALL_S` | `300` | Seconds a busy slot's token counters may stay unchanged, with no turn completing, before the dashboard marks the upstream stalled. |
| `DASH_DRAFTER` | *(empty)* | Label for the draft-accept card. Empty means the label is guessed from the serving-config name (`MTP`, `DFlash2`, else `Draft`). |
| `DASH_HWMON` | `/sys/class/hwmon` | hwmon root scanned for CPU, GPU and NVMe temperatures. Linux only; the temperature panel is empty elsewhere. |
| `DASH_TEMP_WARN_C` | `85` | Temperature (degrees C) at which a reading is coloured as a warning. |
| `DASH_TEMP_CRIT_C` | `95` | Temperature (degrees C) at which a reading is coloured as critical. |

## Validation rules

`slotpin-proxy --check` loads the config exactly as a normal start does, then exits `0`, or exits `1` after printing `slotpin: config error: ...` on stderr, naming the offending key. It starts nothing, so it is safe as an `ExecStartPre`. These make it fail:

- an unknown top-level TOML key, invalid TOML, a value that cannot be converted to the key's type, or a boolean that is not one of the accepted spellings;
- `mode` not `single`/`dual`; `upstream_kind` not `llamacpp`/`gufo`;
- `upstream_kind = "gufo"` with `kv_persist`, `kv_adaptive` or `prefix_restore` true;
- more than one `[[classify.lanes]]` entry with `interactive = true`; a lane or demote rule missing `name`/`match`/`first_user_match`, a label missing `match`/`label`; any regex that does not compile;
- ports outside range: `listen_port` 0-65535, `upstream_port` and `admin_port` 1-65535;
- `interactive_slot`, `default_slot`, `prefix_restore_min_chars`, `prefix_save_min_interval_s`, `backpressure_min_tier`, `default_tier` below 0;
- non-positive values for `recur_threshold`, `snapshot_ttl_h`, `prefix_chunk_chars`, `cache_ram_mb`, `bytes_per_token`, `slots`, `max_inflight`, `queue_timeout_s`, `max_wait_s`, `bp_drop_s`, `upstream_timeout_s`, `admin_timeout_s`, `downstream_write_timeout_s`, `metrics_rotate_mb`, `metrics_keep`, `reload_poll_s`, `mem_poll_s`, `prefix_hash_chars`;
- `cram_safety` outside (0, 1.5];
- `max_inflight > slots`, or `slots > 1` with `sched_enabled = false`;
- `upstream_stall_timeout_s` negative or not finite.

Advisory warnings (printed to stderr by `--check` and, for the path and timeout ones, logged at startup; they never fail the check):

- `listen_host` is not a loopback address. The proxy has no authentication.
- `kv_persist` is on (llama.cpp mode) and `slot_save_path` does not exist or is not writable by the user running the check.
- `kv_persist` is on and `slot_save_path` is on a RAM filesystem (tmpfs/ramfs), so snapshots would consume RAM instead of disk. The check reads `/proc/mounts`, so it only fires on Linux.
- `metrics_file`'s directory does not exist or is not writable, or is on tmpfs (the stream would not survive a reboot).
- `upstream_stall_timeout_s` is not below `upstream_timeout_s`, so the socket timeout fires first and the turn is reported as a timeout, not a stall.
- No `[[classify.lanes]]` are configured, so every request classifies to lane `default`.

`--check` prints a short configuration summary to stderr. With `upstream_stall_timeout_s > 0`, or `hold_headers_until_first_output` on, it also prints a `slotpin: fail-fast: ...` line on stdout. The last stdout line on success is `slotpin: config OK`.
