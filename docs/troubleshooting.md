# Troubleshooting

Read the per-turn metrics file (`metrics_file`, JSONL; see `docs/metrics.md`)
first, always. It is the ground truth for what each turn actually did. Standing
rules and procedures are in `docs/operations.md`.

## Symptom → cause → fix

| Symptom | Likely cause | Fix |
|---|---|---|
| Cold turns (`served: "cold"`, low `cache_n`) right after a model reload | The proxy's slot/pool state still describes the old model; reload detection did not fire (it degrades to "no reload" when `/props` is unreachable or unrecognised) | `systemctl restart slotpin-proxy`. Check `counters.reloads_detected` in `/_stats`. Install `deploy/systemd/llama-swap-pair-slotpin.conf` if you run llama-swap. |
| Slow turns on a prefix that should be warm; restores seem suppressed | `cache_ram_mb` does not equal the server's `--cache-ram`, so the inferred pool is wrong (too high: restores suppressed; too low: needless restores) | Set `cache_ram_mb` (or `SLOTPIN_CACHE_RAM_MB`) to the same value as `--cache-ram`, then restart the proxy. See "The cache-ram lock-step rule" in `docs/operations.md`. |
| `slotpin-proxy --check` fails with `unknown config key in TOML: '<key>'` | A misspelt or removed key in `config.toml`; unknown TOML keys are errors | Fix the key (see `docs/configuration.md`). Note that a misspelt `SLOTPIN_*` environment variable is **not** reported; it is silently ignored. |
| `--check` fails with an incompatibility error for `upstream_kind = "gufo"` | `kv_persist`, `kv_adaptive` or `prefix_restore` is true; Gufo owns prompt caching | Set all three to `false` (see `config.example.gufo.toml`). |
| `--check` fails: `max_inflight` must be `<= slots`, or `slots > 1 requires sched_enabled` | Pooled admission cannot exceed the slot count | Lower `max_inflight`, raise `slots` to match the upstream `-np`, and enable `sched_enabled` for `slots > 1`. |
| HTTP 503 `{"error":"shed","reason":"queue_timeout_or_client_gone"}` | The request waited longer than `queue_timeout_s` (default 600 s), was shed by backpressure (`sched_backpressure`, `bp_drop_s`), or the client disconnected while queued | Look at the `scheduler` block of `/_stats` (queued/shed per tier) and the `queue_wait_ms` of recent turns. A long queue means the slots are saturated by long turns; add slots (and `max_inflight`), shorten the work, or accept the shed. A `turn_failed` record with `reason: shed` is written. |
| HTTP 502 `{"error":"upstream_unreachable","local_available":false}` | The proxy could not connect to or send to `upstream_host:upstream_port` | Check the upstream is running and the host/port are right; `curl http://127.0.0.1:8090/health`. Metrics record `reason: unreachable` (or `timeout` for a socket timeout). |
| HTTP 504 `{"error":"upstream_stalled",...}` | `upstream_stall_timeout_s` is set and a streaming chat turn produced no output and no token progress in the upstream's `/slots` counters for that long; the proxy aborted it | Inspect the upstream (hung decode, wedged GPU). A `turn_failed` record with `reason: stalled` carries the details; the proxy log lists the last token counters. Keep the stall limit below `upstream_timeout_s` (`--check` warns otherwise). |
| `GET /health` returns 503 | The upstream did not answer the proxy's probe (neither HTTP `/health` nor a TCP connect on `upstream_host:upstream_port`) | Start or fix the upstream. This is the intended "local model unavailable" signal for callers. |
| Dashboard header shows the proxy indicator as down (`health.proxy_up` false in `/api/state`) | The dashboard's TCP check of `DASH_PROXY` (default `127.0.0.1:8090`) failed | Set `DASH_PROXY` to the proxy's real `host:port` in `/etc/slotpin/slotpin-dash.env` and restart `slotpin-dash`. |
| Dashboard says the model/upstream is `down` | `GET <DASH_UPSTREAM>/slots` is unreachable (default `http://127.0.0.1:5802`) | Point `DASH_UPSTREAM` at the llama-server (or Gufo) that serves `/slots`. If the server runs with `--no-slots`, or a Gufo build older than gufo-org/gufo#389 is used, the slot card has nothing to show. |
| Dashboard energy / power cards are empty | `DASH_ENERGY` is unset, which turns the dashboard's energy feed off (the default) | Set `DASH_ENERGY` to the plug's Prometheus `/metrics` URL. This is separate from the proxy's `energy_url`; see `docs/energy.md`. |
| Per-turn energy columns are `null` | `energy_url` is unset, the plug is unreachable (a snapshot older than about three poll intervals is dropped), or `energy_metric` does not match an `id=` in `/metrics` | See `docs/energy.md`. |
| Memory or temperature cards show no data | Non-Linux host, no amdgpu/hwmon sensors, or `DASH_HWMON` points at the wrong root | Those readings come from `/proc/meminfo`, amdgpu sysfs and `/sys/class/hwmon` (`k10temp`, `amdgpu`, `nvme`). They are Linux-only. |
| Test run reports skipped tests (dashboard page tests) | Node.js is not installed; the dashboard page tests that execute the page's JavaScript are skipped via `needs_node` | Install Node.js to run them, or ignore the skips. |
| Warning at `--check` or startup: `slot_save_path ... is on a RAM filesystem (tmpfs/ramfs)` | The snapshot directory is tmpfs, so every KV snapshot consumes RAM | Use a persistent (NVMe) path and make the upstream `--slot-save-path` the same directory. See rule 6 in `docs/operations.md`. |
| Warning in the log: `save reported OK but ... is absent` | The upstream `--slot-save-path` differs from `slot_save_path` | Make the two paths identical. Until then, warm-lane bootstrap is empty and KV may be landing elsewhere (e.g. tmpfs). |

## Reading a slow turn

Look at these `metrics_file` fields, in order:

1. `served`: is it `"cold"` when you expected `"warm"`? If so, something
   suppressed a restore or the prefix was not warm-worthy yet.
2. `cache_n` versus `prompt_n`: a low `cache_n` on a repeat prefix indicates a
   prefix-invalidation problem (something volatile crept into the head of the
   system message) rather than a proxy configuration problem.
3. `queue_wait_ms` and `tier`: a high wait on a high-tier (interactive) turn
   usually means it landed behind a long-running lower-tier turn. Turn-boundary
   scheduling means it had to wait that turn out (see Known limitations in
   `docs/architecture.md`); this is not a bug.
4. `cacheram_restore` / `restored_tokens`: whether the proxy believed this turn
   came from its inferred RAM pool.

## Restore MISS

Logged by `kvstore.py` (and recorded as a `restore` event with `ok: false`) when
the admin API's restore call returns non-2xx: no snapshot on disk for that
prefix hash, or the admin call itself failed. For the interactive lane a miss
on the turn's own `<prefix_hash>.bin` is followed by a `latest_alias` fallback
attempt; a miss on that too means a cold prefill this turn. The alias is a
symlink and can dangle after a TTL eviction until the next save (see Known
limitations). For non-interactive lanes there is no alias fallback: a miss just
means a cold prefill, which is expected until `recur_threshold` is reached.

## Reload storms

Reload detected repeatedly in a short window: check `counters.reloads_detected`
via `/_stats`. Likely causes are `/props` itself flapping (a transient network
issue producing spurious signature changes; check the field values being
compared, not just the count) or llama-swap genuinely reloading repeatedly (a
config or health-check loop on the llama-swap side). Either way this is an
upstream/host investigation, not something to silence in the proxy: a real
reload that goes undetected is worse than an extra harmless pool clear.
