"""Build the ``/_stats`` payload by combining snapshots from every component.

Kept as a single pure function (``build_stats``) with no state of its own so
it's trivially testable: feed it a config + the live components and it
returns a plain, JSON-serialisable ``dict``. Block names follow the
legacy ``/_stats`` shape: ``config``,
``counters``, ``slots``, ``inferred_cacheram_pool``, ``adaptive``,
``scheduler``, ``recent``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .state import pctl

if TYPE_CHECKING:
    from .config import Config
    from .kvstore import KVStore
    from .pool import CacheRamPool
    from .scheduler import Scheduler
    from .state import ProxyState


def build_stats(
    cfg: Config,
    state: ProxyState,
    pool: CacheRamPool,
    kv: KVStore,
    sched: Scheduler,
) -> dict[str, Any]:
    """Assemble the full ``/_stats`` document.

    Reads ``state`` under its lock once (a consistent snapshot), then defers
    to each component's own thread-safe ``snapshot``/``snapshot_stats``
    method for the ``inferred_cacheram_pool``, ``adaptive`` and ``scheduler``
    blocks.
    """
    with state.lock:
        counters = dict(state.counters)
        slot_last_snapshot = dict(state.slot_last)
        slot_busy_snapshot = dict(state.slot_busy)
        fp_info_snapshot = {k: dict(v) for k, v in state.fp_info.items()}
        recent_snapshot = list(state.recent)

    slots: dict[str, Any] = {}
    for slot, prefix in slot_last_snapshot.items():
        label = fp_info_snapshot.get(prefix, {}).get("label", prefix) if prefix else None
        slots[str(slot)] = {
            "last_prefix": prefix,
            "last_workload": label,
            "busy": slot_busy_snapshot.get(slot, False),
        }

    decode_tps_vals = [
        e["decode_tps"] for e in recent_snapshot if e.get("decode_tps") is not None
    ]
    warm = sum(1 for e in recent_snapshot if e.get("served") == "warm")
    cold = sum(1 for e in recent_snapshot if e.get("served") == "cold")
    total_served = warm + cold
    interactive_wait_vals = [
        e["queue_wait_ms"]
        for e in recent_snapshot
        if e.get("interactive") and e.get("queue_wait_ms") is not None
    ]

    recent_block = {
        "count": len(recent_snapshot),
        "decode_tps_p50": pctl(decode_tps_vals, 50) if decode_tps_vals else None,
        "warm": warm,
        "cold": cold,
        "warm_ratio": (warm / total_served) if total_served else None,
        "interactive_wait_ms_p95": pctl(interactive_wait_vals, 95) if interactive_wait_vals else None,
    }

    config_block = {
        "upstream_kind": cfg.upstream_kind,
        "mode": cfg.mode,
        "sched_enabled": cfg.sched_enabled,
        "kv_persist": cfg.kv_persist,
        "kv_adaptive": cfg.kv_adaptive,
        "cache_ram_mb": cfg.cache_ram_mb,
        "cram_token_cap": cfg.cram_token_cap,
        "max_inflight": cfg.max_inflight,
        "slot_save_path": str(cfg.slot_save_path),
        "upstream": f"{cfg.upstream_host}:{cfg.upstream_port}",
        "admin": f"{cfg.admin_host}:{cfg.admin_port}",
        "listen": f"{cfg.listen_host}:{cfg.listen_port}",
    }

    return {
        "config": config_block,
        "counters": counters,
        "slots": slots,
        "inferred_cacheram_pool": pool.snapshot(),
        "adaptive": kv.snapshot_stats(),
        "scheduler": sched.snapshot(),
        "recent": recent_block,
    }
