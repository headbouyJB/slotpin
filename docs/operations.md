# Operations

For symptom-driven diagnosis (a symptom, its cause and its fix), see
`docs/troubleshooting.md`. This page holds the standing rules and procedures.

## Install and service layout

```bash
sudo ./deploy/install.sh --config config.example.toml --user <service-user> --enable [--dash]
```

This creates a venv under `/opt/slotpin`, links `slotpin-proxy` (and
`slotpin-dash`) into `/usr/local/bin`, installs `/etc/slotpin/config.toml`,
the data directory (`/var/lib/slotpin`, with `kv-cache/` for snapshots and
`metrics.jsonl`), the `slotpin-proxy` systemd unit and a logrotate entry, and
validates the config with `--check` before arming. `--dash` also installs the
`slotpin-dash` unit (start it with `systemctl enable --now slotpin-dash`).
The proxy unit reads optional `SLOTPIN_*` overrides from
`/etc/slotpin/slotpin.env` and runs `slotpin-proxy --check` as an
`ExecStartPre`, so a bad config fails the unit at start rather than at the
first request. For Gufo use `config.example.gufo.toml` instead.

Neither service has authentication. The default bind is `127.0.0.1`; read
`SECURITY.md` before binding anything else.

## The rules

These six rules come from real, expensive failure modes. Each is marked
**automated** (this codebase handles it) or **manual** (you still have to do
it).

1. **Any llama-server reload must be followed by a state reset (pool +
   `slot_last`), or every turn looks warm to the proxy when it isn't, and
   restores get silently suppressed.**
   - **Automated (deploy asset):** `deploy/systemd/llama-swap-pair-slotpin.conf`
     is a systemd drop-in that restarts `slotpin-proxy` whenever the
     `llama-swap` *systemd unit* restarts. A full process restart trivially
     clears all in-memory state. Install it as
     `/etc/systemd/system/llama-swap.service.d/pair-slotpin.conf` if you run
     llama-swap.
   - **Automated (in-process):** `reload_detect.py` polls the admin host's
     `/props` every `reload_poll_s` and clears the pool and slot occupant map
     without a restart on a detected signature regression (model identity,
     uptime/start-time, or a cumulative counter going backwards). It is defence
     in depth on top of the drop-in: it catches the cases that hook cannot,
     such as llama-swap's own internal config poll or an in-place model swap,
     neither of which restarts the systemd unit. Disable with
     `reload_detect = false` if its heuristics ever misfire.
   - **Still manual, as an escape hatch:** reload detection intentionally
     degrades to "no reload happened" when `/props` is unreachable or its shape
     does not match what is expected (no false positives by design; see
     `docs/architecture.md`). If you suspect a reload went undetected, run
     `systemctl restart slotpin-proxy`.
2. **Never run two `llama-server` processes on the same GPU at once.** On some
   GPU/driver combinations (the reference Ryzen AI Max+ 395 / gfx1151 ROCm
   stack is one) this faults and takes the service down. **Manual:** this
   codebase never spawns or manages the `llama-server` process; it only talks
   to the admin API. Process supervision is out of scope by design.
3. **`cache_ram_mb` must equal the server's own `--cache-ram` flag.** See the
   lock-step rule below. **Manual.**
4. **Config edits need the matching restart.** See the restart matrix below.
   **Manual** (systemd makes it a single command per change, but nothing
   enforces you remembering which one).
5. **Memory-tight hosts: save spikes and context checkpoints compete with the
   model itself for RAM.** **Manual, ongoing tuning:** watch actual free memory
   after enabling `kv_persist`/`kv_adaptive`, and prefer trimming
   `cache_ram_mb` (in lock-step with the server flag) over letting the host run
   near zero headroom.
6. **KV-snapshot paths must be on persistent disk (NVMe), never tmpfs, and the
   upstream `--slot-save-path` must match `slot_save_path`.** llama-server
   writes each slot's KV under its own `--slot-save-path`; slotpin globs
   `slot_save_path` for the same `*.bin` files (warm-lane bootstrap and the
   alias symlink). If the upstream path is `/tmp` (tmpfs / RAM on most hosts),
   every snapshot silently consumes RAM instead of paging off disk, ballooning
   memory towards OOM and, on a unified-memory APU, stealing memory bandwidth
   from decode. If the two paths merely differ, warm-lane bootstrap comes up
   empty and restores misbehave. **Partly automated:** `slotpin-proxy --check`
   (and startup) warns when `slot_save_path` is on tmpfs, and the first save
   that reports success but whose file never appears in `slot_save_path` logs a
   one-time warning (the mismatch tell). The upstream `--slot-save-path` itself
   is set in the model launch (outside this repo), so keeping it equal to
   `slot_save_path` on persistent disk is still **manual**. Diagnose a RAM
   balloon with `df -h /tmp` and `du -sh <slot-save-path>` before blaming KV
   or caches.

## Running in front of Gufo

[Gufo](https://github.com/gufo-org/gufo) owns prompt caching itself (an
in-memory + disk continuation cache keyed on the prompt), so slotpin's
llama.cpp cache machinery is switched off and slotpin becomes a classifying,
scheduling, instrumenting relay. The starter config is
`config.example.gufo.toml`; the essential keys are:

```toml
upstream_kind = "gufo"
upstream_host = "127.0.0.1"
upstream_port = 8080          # Gufo's port
admin_host = "127.0.0.1"
admin_port = 8080             # same Gufo port: /v1/models + /metrics are read here
kv_persist = false            # required (ConfigError otherwise)
kv_adaptive = false           # required
prefix_restore = false        # required
sched_enabled = true
slots = 1                     # <= Gufo --sessions
max_inflight = 1              # <= slots, and <= Gufo --sessions
```

| slotpin still does | Gufo owns |
|---|---|
| lane/tier classification, scheduler admission (tier priority + aging, shedding) | which session/KV a request lands in (`id_slot` is not sent) |
| relay, `include_usage` forcing, client-disconnect abort, post-abort hold (`/metrics` `llamacpp:requests_processing`) | prompt-cache reuse, memory + disk snapshots, cache eviction |
| per-turn metrics (usage + `timings`, energy, memory) plus Gufo's cache report (`upstream_cache`, ...; see `docs/metrics.md`) | -- |
| reload detection: `/v1/models` `data[0].id` / `context_length` change, or a regression of the `/metrics` `llamacpp:prompt_tokens_total` / `llamacpp:tokens_predicted_total` counters (Gufo restarted) | -- |

Notes:

- **`slots` and `max_inflight` must not exceed Gufo's `--sessions`.** Admission
  is still slotpin's job; letting more turns through than Gufo has sessions
  just moves the queue into Gufo, where tiers and aging do not apply.
- `served` is `"warm"` when Gufo reports a cache hit (memory or disk), else
  `"cold"`; `cacheram_restore` is always `false` (no pool inference).
- The post-abort hold sees only Gufo's box-wide in-flight count; it subtracts
  the other turns slotpin has live, so direct (non-slotpin) traffic to Gufo can
  stretch a hold, bounded by `upstream_timeout_s` (max 600 s). Gufo cancels an
  aborted request within about a second, so in practice the hold is short.
- Gufo's `/v1/models` `created` is the request time, not process start, so it is
  not used as a restart signal. A Gufo restart with no traffic since the last
  poll (counters unchanged at 0) is not detectable, and also harmless.
- The dashboard's (`slotpin-dash`) slot card reads the upstream's `/slots`
  (`is_processing`, `n_ctx`). Gufo serves a compatible `/slots` from a build
  that includes gufo-org/gufo#389; against older Gufo builds the card shows no
  useful slot state.

## Restart matrix

| You changed... | Restart... |
|---|---|
| `config.toml` or `slotpin.env` (any `slotpin-proxy` key) | `systemctl restart slotpin-proxy` |
| `llama-server` flags (e.g. `--cache-ram`, `--ctx-checkpoints`) via llama-swap's config | Trigger the llama-swap reload/model-swap as usual; **also** verify `slotpin-proxy` picked up the change. Reload detection should auto-clear its state, but if `cache_ram_mb` itself needs to change to match a new `--cache-ram`, that is a `config.toml` edit too (see the first row) |
| The `llama-swap` **systemd unit** itself (e.g. `systemctl restart llama-swap`) | Nothing extra if `llama-swap-pair-slotpin.conf` is installed: its `ExecStartPost` restarts `slotpin-proxy` for you |
| `slotpin-dash.env` (any `DASH_*` variable) | `systemctl restart slotpin-dash` (the dashboard keeps its rollup database across restarts) |
| Client / gateway configuration (elsewhere) | Restart that client or gateway; out of scope for this proxy |

## The cache-ram lock-step rule

`cache_ram_mb` in `slotpin-proxy`'s config is not read from llama-server; it is
a number you tell the proxy so it can *infer* what llama-server's own
`--cache-ram` host-RAM prompt cache probably still holds. If the two values
diverge:

- Proxy's number too high relative to reality: the proxy over-estimates what is
  still warm, restores get suppressed, and turns that should have been fast
  cold-prefill instead. (A mismatch here once over-modelled the pool by 2.75x
  and caused a real slow-turn incident.)
- Proxy's number too low: unnecessary restores and wasted disk I/O; no
  correctness issue, but wasted latency.

**Rule:** whenever you change `--cache-ram` on the llama-server command line,
change `cache_ram_mb` in `slotpin-proxy`'s `config.toml` (or
`SLOTPIN_CACHE_RAM_MB`) to the same value, in the same change, and restart
`slotpin-proxy`.

## Memory-tight-host guidance

On unified-memory hosts running large models, free RAM after loading weights
may be single-digit GB. In that regime:

- Prefer a smaller `cache_ram_mb` (and matching server `--cache-ram`) over a
  generous one; the proxy's own save/restore and checkpoint I/O competes for
  the same pool the model needs.
- Watch for OOM risk from the *proxy's* side too: a save burst under load is a
  real memory/IO spike, not just a network operation. The systemd unit ships a
  commented `MemoryMax=` for exactly this reason; uncomment and tune it once
  you have observed normal operating headroom.
- If the host crashes under memory pressure independent of this proxy, that is
  an OS/host-level fix (earlyoom, kernel tuning), not something this proxy can
  paper over.

## The `/health` local-unavailable signal

`GET /health` returns:
- `200 {"status":"ok","local_available":true}` when `SlotAdmin.upstream_alive()`
  succeeds (a `GET /health` against the configured upstream that gets any HTTP
  reply, falling back to a bare TCP connect).
- `503 {"status":"degraded","local_available":false}` otherwise.

This exists so a caller can wire an explicit fail-safe: when the local model is
unreachable, some callers' provider-level fallback silently routes to a hosted
model instead of erroring loudly, which can be a privacy leak for a local-only
setup if nobody notices. Point your caller's health check at this endpoint and
treat 503 as "do not silently fall back" rather than assuming any 5xx from the
proxy means the same thing. `GET /ready` is separate and only indicates the
server itself is up and serving (a liveness/readiness split for systemd or
orchestrator wiring); it does not probe the upstream.
