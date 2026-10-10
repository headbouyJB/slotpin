# Concurrency design (multi-slot built, multi-model designed)

Design of record for evolving slotpin from a single warm slot in front of one
model to a **pooled, multi-slot router in front of several co-resident models
on one box**: serving concurrent turns, keeping lanes warm via disk, and
measuring energy and contention per turn.

**Status.** The multi-slot half is built. The multi-model half is a design
only: `server.py` and `config.py` contain no model registry and no per-model
routing; a proxy instance talks to exactly one upstream. The status of each
step is in *Build order* below. Where this document describes something that
does not exist yet, it says so.

## Goals

- Serve **multiple models co-resident on one box** (for example a dense model
  plus an MoE model) and **multiple concurrent turns per model** (`-np K`).
- Keep interactive lanes **warm** without reserving idle memory.
- **Positively confirm** a bench's isolation (single-model, single-turn) and
  **quantify** concurrency's performance cost in production, from the metrics.
- Fair, contention-aware **energy attribution** per turn.

## Non-goals (explicitly out of scope here)

- **Multi-box** routing / failover: single box only for now.
- **Co-serving very large models:** a model that needs the whole box to itself
  is not a multi-model target.
- **Launching/stopping model servers:** that is llama-swap or another control
  plane. slotpin *routes to* upstreams that already exist.
- **Slot-pinning to roles:** deliberately deprioritised (see Warmth).

## Architecture

Target (multi-model) shape; today there is a single upstream:

```
                         ┌─ model "dense" → llama-server :5802  (-np K1, ctx C1)
Client → slotpin :8090 ──┤
   classify → route      └─ model "moe"   → llama-server :5803  (-np K2, ctx C2)
   → pooled slot         (admin: /slots save|restore + /props per model)
   → maybe restore
   → schedule → serve
        ▲
        └─ background poller (~1s): per-tick accounting engine
             (energy split + avg concurrency, per model & per box)
```

Five pieces:

### 1. Model registry (designed, not built)
Config would map each resident model to its server and shape:
```
model = "dense"  upstream = 127.0.0.1:5802   slots = K1   ctx_total = C1
model = "moe"    upstream = 127.0.0.1:5803   slots = K2   ctx_total = C2
```
A **lane/role → model** mapping (like today's `[[classify.lanes]]`) would decide
which model serves a request. Per-slot context is `ctx_total / slots` (see
Capacity). The proxy would need `ctx_total`/`slots` for cache-ram and warmth
accounting; it would not launch the servers.

### 2. Pooled slots (no role pinning) (built, single model)
Slots on the upstream are a **pool**, not reserved. Per turn (`_choose_slot` in
`server.py`):
1. **Prefix already resident in a free slot?** (immediate follow-up) → route
   there, warm, no restore.
2. Else → take the first **free** slot; if the lane is warm-worthy and a disk
   snapshot for the prefix exists, **restore it into that slot** first.
3. The **scheduler gives interactive tier priority** to grab a free/warm slot
   (with aging), so interactive turns are served first under load.

Rationale: slot-pinning was the pre-disk-warmth workaround; it strands KV in an
idle reserved slot. Disk save/restore decouples "warm" from "which slot", so
pooling gives full flexibility *and* warm hits: quiet periods keep last turn's
KV resident in whatever slot it used; busy periods use every slot.

Config validation enforces `max_inflight <= slots` and, for `slots > 1`,
`sched_enabled = true`, so admission can never route two turns to one slot.

### 3. Warmth (disk save/restore) (built, keyed by prefix)
KV snapshots are keyed by the prefix hash and are **slot-fungible**: restored
into whichever slot is free. The proxy tracks the resident prefix per slot
(`state.slot_last`) to detect warm follow-ups. Disk warmth is the enabler of the
pool, not a fallback: it is the hot path for a lane returning after its slot was
reused. Saves are synchronous and happen while the turn still owns its slot. In
the multi-model design the key would become `(model, prefix)` and the resident
prefix would be tracked per `(model, slot)`.

### 4. Scheduler (built, one queue)
`max_inflight` caps live turns (set it to the upstream's `-np K`). Priority and
anti-starvation aging are as described in `docs/architecture.md`. In the
multi-model design `max_inflight` would become per model and each model would
have its own queue.

### 5. Poller accounting engine (energy + concurrency) (designed, not built)
The background energy poller (~1 s) would become a small per-tick engine. Each
tick it would know `Δenergy` (whole-box counter delta) and, from the scheduler,
`N_model(t)` and `N_box(t)` (live turns per model / per box). For every
in-flight turn it would:
- credit `Δenergy / N_box(t)` (time-weighted, contention-fair energy share);
- accumulate time-weighted `concurrency_model` and `concurrency_box`.

At turn end the turn would have its fair energy share and its average
concurrencies: no per-request HTTP, correct across partial overlaps.

What exists today is a first-order approximation: each turn records
`concurrency` = the mean of the live-turn count at admission and at completion,
and `energy_fair_wh = energy_turn_wh / concurrency`.

## Capacity & context (the real constraint)

KV memory scales with **total tokens**, and with a non-unified KV cache
(`kv_unified=false`) `-c` is the TOTAL split evenly across slots → **per-slot
context = `-c / -np`**. A model's advertised maximum context is the
per-sequence architectural limit; a slot only gets it if `-c/-np` is at least
that large. The box budget:

```
Σ over resident models ( weights + (-c tokens × KV-bytes/token) )  ≤  box RAM
```

So **concurrency trades against context**: 2 slots at 128k ≈ 1 slot at 256k in
KV. Each registry entry would therefore carry `slots` and `ctx_total`; choosing
K versus per-slot context is a per-model capacity decision. Quantised KV
(`-ctk/-ctv q8_0`) roughly halves KV memory (more slots or context), but it has
been reported to interact badly with slot restore, a tension to test before
relying on it.

## Metrics (per turn)

Fields that exist today (see `docs/metrics.md`):

| field | meaning |
|---|---|
| `slot` | which slot served the turn |
| `concurrency` | mean live turns over the turn (admission and completion counts) |
| `energy_turn_wh` | whole-box energy over the turn's window (all concurrent turns) |
| `energy_fair_wh` | `energy_turn_wh / concurrency` |

Fields the multi-model design would add (not emitted today):

| field | meaning |
|---|---|
| `model` | which model served the turn |
| `models_served` | resident/serving models on the box at turn time |
| `concurrency_model` | time-weighted avg live turns on this model over the turn |
| `concurrency_box` | time-weighted avg live turns across the box over the turn |

**Isolation confirmation:** a clean single-model single-turn bench is provably
`models_served == 1 AND concurrency_box == 1 AND concurrency_model == 1`. Any
deviation flags a contended (untrustworthy) bench number. With a single model
and slot today, `concurrency == 1.0` on every record is the equivalent check.
In production, the concurrency fields quantify the throughput/latency cost of
load.

## Backend gate

`-np > 1` cross-request isolation is **clean on Vulkan** but broken on **HIP**
until llama.cpp #27311 lands (see also llama.cpp #25992). **Design and deploy
the co-serving topology Vulkan-first**; HIP concurrency waits on #27311.

## Build order (each a reviewable step)

1. **Model registry + routing** (not built): config for N models; route by
   lane/role → model → upstream; add `model` to metrics.
2. **Multi-slot pooled assignment** (built): per-upstream `-np K`;
   resident-prefix-else-free-slot selection; `max_inflight` relaxed to
   `<= slots`. Generalising `slot_last` to `(model, slot)` is part of step 1.
3. **Per-prefix disk warmth** (built, single model): restore into any free slot;
   save keyed by prefix. `(model, prefix)` keying is part of step 1.
4. **Per-model scheduler** (not built): per-model admission + interactive
   priority. Today there is one queue.
5. **Poller accounting engine** (not built): per-tick energy split + time-weighted
   concurrency. The first-order `concurrency` / `energy_fair_wh` fields are the
   interim.
6. **Validate on hardware** (open): Vulkan, two models co-resident; isolation
   guard under `-np K`; bench isolation versus concurrency deltas.

Steps 1-4 are the routing/serving core; 5 is the measurement layer; 6 proves it.
