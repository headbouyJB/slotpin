# Architecture

## Topology

```
Client (any OpenAI-compatible caller)
        |  POST /v1/chat/completions | /v1/completions | /completion
        v
+-----------------------------+
|  slotpin-proxy (:8090)      |  classify -> admit -> pick slot -> maybe restore
|                              |    -> forward -> relay -> save
+--------------+---------------+
               | upstream_host:upstream_port  (normal request path)
               v
+-----------------------------+
|  llama-server (:8080)       |  -np <slots>, --slot-save-path for disk
|  (or llama-swap in front)   |  snapshots; Gufo is the other supported upstream
+-----------------------------+
               ^
               | admin_host:admin_port  (the proxy talks here directly too:
               |   slot save/restore, /slots progress, /props)
```

If llama-swap sits in front of llama-server, point `upstream_host/port` at
llama-swap and `admin_host/port` at the llama-server itself (llama-swap does
not proxy the slot API). With a plain llama-server both pairs are the same.

A separate, optional process, `slotpin-dash` (see `docs/dashboard.md`), reads
the proxy's metrics file and the upstream's `/slots` and serves a read-only
web dashboard. It is not part of the request path.

The proxy owns these endpoints (all dispatched in `server.py`):

| Endpoint | Behaviour |
|---|---|
| `POST .../v1/chat/completions`, `POST .../v1/completions`, `POST .../completion` | The full pipeline below (matched by path suffix). |
| `GET /_stats` | Telemetry document (see `docs/metrics.md`). |
| `GET /health` | 200 when the upstream answers a probe, else 503 (see `docs/troubleshooting.md` and `docs/operations.md`). |
| `GET /ready` | 200 `{"status":"ready"}` once the listener is serving. |
| anything else | Transparent pass-through to `upstream_host:upstream_port`. A `POST` to an inference path with an empty or non-JSON body is also passed through untouched (no pinning, no tracking). |

Neither the proxy nor the dashboard has any authentication; see `SECURITY.md`.
The default `listen_host` is `127.0.0.1`.

### Component wiring (`cli.py`)

`cli.py` builds the components in dependency order: `Classifier`,
`ProxyState`, `CacheRamPool`, `SlotAdmin`, `MetricsWriter`, `KVStore`,
`Scheduler`, `ReloadDetector`. Then `KVStore.load_stats_from_disk()` rebuilds
the warm-prefix set from the `*.bin` files already in `slot_save_path`, the
reload detector starts, and `ProxyServer` serves until SIGTERM/SIGINT.
`slotpin-proxy --check` validates the configuration (and prints advisory
warnings) without starting anything.

### Gufo branch

With `upstream_kind = "gufo"` the same pipeline runs, but Gufo owns prompt
caching. Differences: `id_slot` is not sent (Gufo picks its own session; the
slot index is only slotpin's admission bookkeeping), restore and save steps
never run (`kv_persist`, `kv_adaptive` and `prefix_restore` must be false), the
cache-ram pool inference is skipped, `served` comes from Gufo's own cache
report, and reload detection polls `/v1/models` and `/metrics` instead of
`/props`. See "Running in front of Gufo" in `docs/operations.md`.

## Request lifecycle (inference POSTs)

Order here is behaviourally load-bearing: it exists to satisfy the invariants
below.

1. Read the body. An empty or non-JSON body is passed through untouched.
2. Classify (`Classifier.classify`, pure and read-only) into a
   `Classification`: lane, cosmetic label, `fp`, `prefix_hash`, scheduler tier,
   interactive flag and a nominal slot. `prefix_hash` and `fp` are the same
   value (sha1 of the system text, or of the first `prefix_hash_chars` of the
   body when there is no system message).
3. For a streaming `/v1/chat/completions`, force
   `stream_options.include_usage = true` so usage and timings survive
   streaming. (Native `/completion` emits its own `timings`.)
4. `sched.acquire(tier, client_gone)` blocks until admitted, the client
   disconnects, or the queue timeout / backpressure rule sheds it. A shed
   request gets HTTP 503 `{"error":"shed",...}`, is counted per tier and
   writes a `turn_failed` record (`reason: shed`).
5. **Only after admission**, pick a slot: a free slot already holding this
   prefix (warm follow-up, no restore), else the first free slot. Mark it busy
   and, for llama.cpp, inject `id_slot`. Admission caps live turns at
   `max_inflight <= slots`, so a free slot normally exists.
6. Decide, read-only, whether a restore is needed for the selected slot. The
   interactive lane trusts only the slot's last occupant
   (`kv.is_cold_interactive`); other warm-worthy prefixes (when `kv_adaptive`
   is on) also consult the inferred cache-ram pool (`kv.is_cold`). With
   `prefix_restore`, a cold turn may also find the saved snapshot sharing the
   longest rendered prefix (`prefix_index.py`), confirmed against a fresh
   `/props` model id before use.
7. Write `state.slot_last[slot]` for the selected slot. Classification never
   writes this.
8. If a restore was decided, log it unconditionally, then run
   `kv.restore_blocking(...)` now that the slot is exclusively ours (exact
   snapshot, then longest-prefix candidate, then the `latest_alias` fallback for
   the interactive lane).
9. Forward and relay (`_forward_and_relay`): chunked relay to the client,
   `SO_SNDTIMEO` on the downstream socket, a disconnect watcher that aborts the
   upstream call if the client goes away, the optional stall watchdog and
   header-holding (`upstream_stall_timeout_s`,
   `hold_headers_until_first_output`), and a 16 KB tail ring used to recover the
   final usage/timings chunk. An upstream connect failure returns 502
   `{"error":"upstream_unreachable","local_available":false}`; a detected stall
   with nothing yet sent to the client returns 504 `upstream_stalled`.
10. For a completed 2xx turn only: parse usage and timings, update counters,
    `fp_info`, the pool eviction/restore inference and the `recent` ring, write
    the per-turn metrics line, and, if the prefix is warm-worthy, save the
    snapshot **synchronously** (`kv.save_now`) while the turn still owns the
    slot. Any other outcome writes a `turn_failed` record instead and nothing is
    saved (a partial KV must not overwrite a good snapshot).
11. In a `finally`: if an aborted turn may still be running upstream, hold the
    slot until the upstream reports it idle; then clear the slot's busy flag
    (resetting `slot_last` to unknown when a failed turn may have changed its
    KV) and `sched.release(ticket)`. This always runs, even on error.

## Design rationale

**Why slot pinning.** llama-server holds one KV cache per slot, and a slot that
serves a different prompt evicts what it held. When several workloads (an
interactive conversation, periodic polls, batch jobs) share a small number of
slots, whichever runs evicts the others, and the interactive conversation is
the one that keeps paying for a full re-prefill. The proxy classifies each
request, tracks which prefix each slot holds, prefers a slot that is already
warm for the request, and restores a saved snapshot when none is.

**Why disk warmth (save/restore).** In-GPU KV eviction is unavoidable when
workloads outnumber slots; the KV state itself, however, can be snapshotted to
disk and restored. Restoring a long prompt from NVMe is typically orders of
magnitude cheaper than re-running its prefill, and disk warmth survives proxy
restarts and (when reload detection works) model reloads, which an in-memory
cache could not.

**Why turn-boundary priority, not preemption.** The scheduler admits by tier
priority with FIFO within a tier, and a higher-priority request waits for the
*current* turn to finish rather than interrupting it; llama.cpp's inference loop
cannot be preempted mid-generation. This bounds worst-case interactive latency
to "however long the turn ahead of it takes", not zero (see Known limitations).

**Why aging, not strict priority.** Tier priority alone can starve a low-tier
request indefinitely under sustained higher-tier traffic. A queued request's
*effective* tier therefore rises continuously toward the top priority as it
waits, reaching it once queued `>= max_wait_s` (default 60 s), so a long-queued
batch job eventually outranks a freshly arrived interactive turn instead of
waiting behind an unbroken stream of them. `queue_timeout_s` remains as an
additional hard backstop, and optional backpressure (`sched_backpressure`) can
shed low-tier waiters past `bp_drop_s` under sustained saturation (off by
default).

**Why the pool is an inference.** llama-server does not expose the contents of
its own host-RAM prompt cache (`--cache-ram`) over the API. The proxy keeps its
own model of what *should* still be warm there (`CacheRamPool`), built entirely
from its own observations (evictions it caused, restores it saw). The model is
only as good as its inputs: it must be told the exact `--cache-ram` value the
server was started with (`cache_ram_mb`), and it must be reset whenever the
server's own state resets out from under it (a reload), which is what reload
detection is for. The interactive lane deliberately ignores the pool, because
trusting an over-optimistic estimate there produced very slow cold prefills.

## Known limitations

- **No mid-turn preemption.** A high-priority (interactive) request that
  arrives while a long low-priority turn is running waits for that turn to
  finish; there is no abort-and-retry. Worst-case interactive queue latency
  tracks the longest non-interactive turn duration, not zero.
- **The cache-ram pool is an estimate, not a fact.** It is inferred from
  `cache_ram_mb`/`bytes_per_token`/`cram_safety` and the proxy's own
  eviction/restore observations, not read from llama-server. If `cache_ram_mb`
  drifts out of sync with the server's actual `--cache-ram` flag, the estimate
  becomes wrong in either direction (falsely warm = suppressed restores and cold
  turns; falsely cold = unnecessary restores). See `docs/operations.md` for the
  lock-step rule.
- **Reload detection degrades to "no reload" on uncertainty, by design.** If
  `/props` is unreachable or its fields do not match what is expected, the
  detector does not guess; it assumes nothing changed. This avoids false
  positives (needless pool/state clears) at the cost of occasionally missing a
  real reload if the upstream build changes its `/props` shape.
- **`ttl_evict` can orphan the `latest_alias` symlink.** The alias is a symlink
  to the most recent interactive snapshot; TTL garbage collection skips the
  alias itself but can delete the snapshot it points at, leaving the alias
  dangling. Alias-based restores miss until the next save rewrites it.
- **Per-prefix bookkeeping grows for the process lifetime.** The dictionaries
  keyed by prefix hash (`ProxyServer.prefix_tokens`, `ProxyState.fp_info`, and
  the `KVStore` occurrence counts, last-save timestamps and per-prefix locks)
  gain one small entry per distinct prefix and are not capped. The `KVStore`
  drops its counts and timestamps only for snapshots it TTL-evicts. A TTL/cap
  sweep is future work.
- **`Scheduler.release()` does not detect a double release.** It decrements the
  in-flight count (floored at zero) without checking the ticket. The server
  releases exactly once per admitted ticket, but a future caller bug would not
  be caught.
- **Unknown `SLOTPIN_*` environment variables are silently ignored**, while an
  unknown key in the TOML file is a configuration error. A misspelt override is
  therefore not reported by `--check`.
- **`KVStore.save_async` is not used on the server path.** The server saves
  synchronously (`save_now`) so a snapshot always captures the right turn's KV;
  `save_async` is retained only for its tests.
- **Multi-model routing is designed, not built.** There is one upstream per
  proxy and no model registry or per-model routing. See
  `docs/concurrency-design.md`.
- **The fail-fast `/slots` probe covers all busy slots together.** The stall
  watchdog compares the token counters of every busy slot as one signature, so
  one stalled request is masked while another slot on the same upstream keeps
  advancing.
- **Memory and temperature readings are Linux-only.** The proxy's memory
  sampler and the dashboard read `/proc/meminfo`, `/proc/mounts`, amdgpu sysfs
  and hwmon sysfs. On other platforms those fields are `null` and the dashboard
  memory and temperature cards show no data; proxying itself is unaffected.
