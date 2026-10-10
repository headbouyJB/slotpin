# How it works, and why

This page explains the mechanisms in slotpin and the reasoning behind them. For the list of every key see [configuration.md](configuration.md); for the request path step by step see [architecture.md](architecture.md).

## The problem

An inference server keeps the attention state (the KV cache) for a prompt in a *slot*. If the next request begins with the same text, the server reuses that state and only processes the new tokens. If the slot has been taken over by a different prompt in the meantime, the server must re-process the whole conversation from scratch: a cold prefill. On a long conversation that is minutes rather than seconds.

This bites hardest when slots are scarce. Large models, memory-limited hosts and some speculative-decoding setups push you toward one slot (`llama-server -np 1`). Then an interactive chat, a periodic health poll and a background job all share it, and each one evicts what the previous one left. The interactive user is the one who notices, because their next message turns into a long cold prefill.

slotpin sits between clients and the server and makes sure the right prompt is in the right slot, or can be put back cheaply, before a request is forwarded.

## Slot pinning

For `llama-server`, a request body may carry `id_slot`, which makes the server use that slot. slotpin sets it on every inference request (`/v1/chat/completions`, `/v1/completions`, `/completion`). `slots` tells slotpin how many slots the server has (`-np N`); turns are pooled across `0..slots-1`.

Slot choice happens after the request has been admitted by the scheduler. slotpin remembers which prompt it last put in each slot (the *occupant*, identified by a short hash of the system message, the `prefix_hash`). For a new turn it prefers a free slot whose occupant already matches, so a follow-up lands where its KV already is. Otherwise it takes the first free slot. Because admission never lets more than `max_inflight` turns run, and `max_inflight` may not exceed `slots`, two turns are never routed to the same slot.

The older `mode`, `interactive_slot` and `default_slot` keys are still accepted but no longer affect which slot is chosen.

## Classification and lanes

Before admission, each request is classified by regular expressions you write in `[[classify.*]]` tables. The classifier only reads the request; it changes nothing.

- A **lane** (`[[classify.lanes]]`) is the first rule whose `match` patterns all match the request body (and whose `match_system` patterns all match the system text). It gives the request a name (reported as `route` in metrics) and a scheduler `tier`. At most one lane may be `interactive = true`: that is the conversation a person is waiting on.
- A **demote** rule (`[[classify.demote]]`) catches short automated turns that look like they belong to a lane, for example a heartbeat that carries the interactive client's system prompt. If the first user message matches and the request has at most `max_messages` messages, the request gets the rule's label and tier, stops being interactive, and moves to the lane's `demote_to` if one is set.
- A **label** (`[[classify.labels]]`) names a workload in metrics (`workload`) and may override the tier. The first matching label wins.
- With no matching lane the request is lane `default` at `default_tier`.

Lower tier numbers are served first. The identity used for warmth tracking is the hash of the system message (or of the first `prefix_hash_chars` characters of the body when there is none), so requests that repeat the same system prompt are treated as the same conversation prefix.

## Warm and cold turns, and the inferred cache-ram pool

`llama-server` has a second, larger cache: `--cache-ram` holds evicted slot states in host memory so that a returning prompt can often be restored without re-processing. The server does not say what is in it. slotpin keeps its own estimate, `CacheRamPool`: when it sees a slot's occupant replaced, it records the old prompt as probably still in cache-ram, and it trims the estimate to a token budget, `cram_token_cap = cache_ram_mb * 1024**2 / bytes_per_token * cram_safety`.

That estimate is why `cache_ram_mb` must equal the server's `--cache-ram`. Too high, and slotpin believes prompts are warm that the server has already dropped, so it skips restores it should have done. Too low, and it restores from disk needlessly. Change them together and restart the proxy. `bytes_per_token` and `cram_safety` only size the estimate.

A turn is recorded as `served: "cold"` if slotpin ran a blocking disk restore for it or if the server reported a reused-token count (`cache_n`) below a baseline, and `"warm"` otherwise. For the interactive lane slotpin deliberately ignores its pool estimate and trusts only "what did I last put in this slot", because a wrong "still warm" guess there is the expensive mistake.

## KV save/restore to disk

With `kv_persist = true`, slotpin asks `llama-server` to write a slot's KV state to a file after an interactive turn (`POST /slots/{id}?action=save`) and to read it back (`action=restore`) before a later turn finds the slot taken. Files are named `<prefix_hash>.bin` under `slot_save_path`, which must be the same directory as the server's `--slot-save-path` (the server writes them; slotpin lists them). Reading tens of thousands of tokens of KV from NVMe takes a fraction of a second to a few seconds, against minutes to recompute.

`latest_alias` is a link to the most recent snapshot of the interactive lane. If the exact snapshot for a turn is missing (the system prompt changed slightly), restoring the alias still saves most of the prefill, because the server reuses whatever prefix matches.

`kv_adaptive = true` extends this beyond the interactive lane. slotpin counts how often each prefix hash appears; once it has been seen `recur_threshold` times it is worth a snapshot too. The interactive lane is worth one on first sight. Saves for the same prefix are rate-limited by `prefix_save_min_interval_s`, a save is deferred (once, about two seconds later) if the slot is busy, and files older than `snapshot_ttl_h` are removed. A restore is always done after admission, when the slot is exclusively the turn's, because a restore into a busy slot only times out.

Snapshot files are derived from prompts and replies, so treat `slot_save_path` like the conversation data it effectively is. It must be on disk, not tmpfs; `--check` warns otherwise.

## Longest-common-prefix restore

Snapshots are keyed by a hash of the whole system message. A client that keeps a large stable head and a small changing tail in it (a date, a per-run task note) produces a new hash every run, so the exact-match snapshot never applies even though almost all of the prompt is unchanged.

With `prefix_restore = true` (requires `kv_persist`), slotpin handles that case. After each save it also writes `<prefix_hash>.idx.json`, a chain of hashes over fixed-size chunks (`prefix_chunk_chars`) of the *rendered* prompt, meaning the exact text the model sees after the chat template is applied. It gets that text from the server's `/apply-template` endpoint, so tool definitions and template framing are in their true positions. When a request is cold and has no exact snapshot, slotpin renders its prompt the same way and picks the snapshot sharing the longest leading run of chunks. If the shared part is at least `prefix_restore_min_chars`, it restores that snapshot and forwards the request; the server keeps the common prefix and prefills only the rest. A wrong pick costs at most one wasted restore, never a wrong answer, because the server decides reuse token by token.

Caveat: this only helps models whose state can be cut back to a shared prefix. With hybrid or recurrent (state-space) layers, a restored slot cannot be truncated, so a partial match re-processes everything after paying the restore. Do not enable it for those models. Non-interactive prompts are indexed only once they are warm-worthy, so workloads whose system message changes on every run need `recur_threshold = 1`.

## The scheduler

With `sched_enabled = true`, requests wait for admission. At most `max_inflight` run at once. When a place frees up, the waiter with the best *effective tier* goes first, ties in arrival order.

Tiers alone would let a steady stream of high-priority traffic starve everything else. So a waiter's effective tier moves toward the top tier as it waits and reaches it after `max_wait_s`. A long-queued background job therefore eventually outranks fresh interactive requests instead of waiting forever. `queue_timeout_s` is a hard limit: a request still waiting after that is shed with a 503. Optionally, `sched_backpressure` sheds waiters whose tier number is at least `backpressure_min_tier` once they have queued `bp_drop_s`, so a saturated box drops background work rather than piling it up.

Admission is at turn boundaries; there is no preemption. A high-priority request that arrives during a long low-priority generation waits for it. Interrupting a generation mid-stream would throw away its work and the server cannot resume it, and freeing the slot would evict the very KV state slotpin is trying to protect. The trade-off is that the worst-case interactive wait is the length of the turn ahead of it. Keep long background turns on a lower tier and give the box enough slots (`slots` greater than 1 requires `sched_enabled`).

## Reset on reload

If the model is reloaded or the server restarts, the slots are empty and the cache-ram pool is gone, but slotpin's memory still says otherwise. Every turn would look warm when it is not, and restores would be skipped. With `reload_detect = true`, a background thread polls the server every `reload_poll_s` and compares a signature (the model identity, an uptime or start time, and cumulative task or token counters, whichever the server exposes). A changed model, a restart, or a counter that went backwards clears the pool and the slot-occupant map. If the poll fails or exposes nothing comparable, slotpin assumes nothing changed; an unnecessary reset costs a restore, a missed one costs wrong warmth decisions. If you front the server with `llama-swap`, `deploy/systemd/llama-swap-pair-slotpin.conf` additionally restarts the proxy whenever the `llama-swap` unit restarts.

## Gufo mode

With `upstream_kind = "gufo"` the engine behind the proxy is [Gufo](https://github.com/gufo-org/gufo). Gufo keeps its own memory and disk prompt cache and picks its own session for each request, so slotpin stops managing KV. It does not send `id_slot`, and `kv_persist`, `kv_adaptive` and `prefix_restore` must be `false` (the config check rejects them otherwise).

slotpin still does what is independent of the cache: classification and lanes, the scheduler (so tiers, aging and shedding still apply, with `slots` and `max_inflight` kept at or below Gufo's `--sessions`), relaying and client-disconnect abort, fail-fast, and the metric record, which gains Gufo's own cache report (`upstream_cache`, restore and snapshot timings and sizes). `served` is `warm` when Gufo reports a memory or disk hit. Reload detection reads `/v1/models` (model identity and context length) and `/metrics` (cumulative token counters, which reset when Gufo restarts) on `admin_host:admin_port`. See the "Running in front of Gufo" section of [operations.md](operations.md).

## Fail-fast

A hung engine can keep a connection alive by sending keep-alive comments, so a client waits for ever. With `upstream_stall_timeout_s` set, slotpin watches streaming chat completions and aborts the upstream request when there has been no real output (a text, reasoning or tool-call delta) for that long. Keep-alive comments and the role-only first event do not count as output.

Silence alone is not enough, since an engine decoding a long tool call may legitimately stream nothing. When the stream has been silent for half the limit, slotpin reads the upstream's `/slots` and compares the prompt-tokens-processed and tokens-decoded counters of busy slots across two readings. It declares a stall only if they have not moved, or the probe fails or shows nothing busy. Only those numbers are read, never the prompt text. Set the limit above your longest cold prefill and below `upstream_timeout_s`.

What the client sees depends on `hold_headers_until_first_output`. Off, a stall after the response has started cuts the stream (truncated reply). On, slotpin sends the client nothing, not even the status line, until the first real output event; a stall before that becomes a clean `504 {"error": "upstream_stalled", ...}` the client can act on, such as falling back to another provider. The cost is that the client sees no headers during prefill, so its header timeout must exceed the limit. After a stall the slot stays reserved until the upstream reports it idle (at most `min(upstream_timeout_s, 600)` seconds), so the abandoned work cannot collide with the next turn. Details are in [configuration.md](configuration.md#fail-fast-on-a-silent-upstream).

## The failure record

A turn that gets a 2xx and finishes writes one per-turn record. Every other inference turn writes one `turn_failed` record instead, so a failure is never silent and never mixed into warm/cold statistics. The `reason` is one of:

- `shed`: the scheduler refused admission (queue timeout, or the client left while queued); the client got a 503.
- `unreachable`: the upstream could not be connected to; 502.
- `timeout`: `upstream_timeout_s` expired waiting for headers or mid-stream.
- `stalled`: the fail-fast limit above fired.
- `aborted_client`: the client disconnected or stopped reading; the upstream request is aborted.
- `upstream_4xx` and `upstream_5xx`: the upstream returned an error status (included as `upstream_status`).
- `upstream_incomplete`: a 2xx reply whose body was cut short or carried an in-stream error.

[metrics.md](metrics.md) gives the precedence when more than one applies.

## What is recorded, and privacy

Each completed turn appends one JSON line to `metrics_file`: lane and workload labels, slot, tier, queue wait, time to first token, token counts, decode and prefill speed, speculative-decoding acceptance, warm or cold, restore details, the live turn count, GPU and host memory (start, average, peak), and, if you configure a smart plug, whole-box power and the energy used by the turn (with a concurrency-adjusted share). Saves and restores write their own records. The file rotates at `metrics_rotate_mb`. The fields are listed in [metrics.md](metrics.md) and the plug setup in [energy.md](energy.md).

The records contain **no prompt or reply text**. Prompts appear only as short hashes of the system message (`fp`, `prefix`), which identify a conversation prefix without revealing it. The `/_stats` endpoint reports aggregate counters. The dashboard reads `metrics_file`, summarises it, and never serves the prompt text that the upstream's `/slots` entries contain. The one place conversation content does live on disk is the KV snapshots under `slot_save_path` (see above). The proxy and dashboard have no authentication; see `SECURITY.md`.
