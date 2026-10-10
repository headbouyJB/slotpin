"""Configuration loading and validation for slotpin.

Precedence is dataclass defaults < TOML file < environment variables. TOML
top-level scalar keys map 1:1 onto :class:`Config` field names; the
``[[classify.lanes]]``, ``[[classify.demote]]`` and ``[[classify.labels]]``
array-of-tables map onto :class:`LaneRule`, :class:`DemoteRule` and
:class:`LabelRule` respectively (see ``config.example.toml`` for the
reference profile).

Every scalar field is also settable via ``SLOTPIN_<UPPER_SNAKE_NAME>``
(booleans accept ``on``/``off``/``1``/``0``/``true``/``false``, case
insensitive). A handful of legacy spellings used by the original
single-file proxy do not follow that mechanical derivation and are special-cased in
``LEGACY_ENV_ALIASES`` (``SLOTPIN_SCHED`` -> ``sched_enabled`` and
``SLOTPIN_METRICS`` -> ``metrics_file``; the others --
``SLOTPIN_MODE``, ``SLOTPIN_KV_PERSIST``, ``SLOTPIN_KV_ADAPTIVE``,
``SLOTPIN_CACHE_RAM_MB``, ``SLOTPIN_SLOT_SAVE_PATH`` -- already coincide with
the mechanical derivation, so they work without a special case, but are
listed too for documentation/robustness).

Note: the "sane ranges" enforced by :func:`_validate` (port ranges,
positivity checks, ``cram_safety`` bound of ``(0, 1.5]``) are best-judgement
bounds. If a legitimate value is rejected, relax the bound. Unknown
top-level TOML keys raise ``ConfigError`` rather than warning, which is
stricter than the original single-file proxy (it had no schema at all).
"""

from __future__ import annotations

import logging
import math
import os
import re
import tomllib
from collections.abc import Mapping
from dataclasses import MISSING, dataclass, field, fields
from pathlib import Path
from typing import Any

logger = logging.getLogger("slotpin.config")


class ConfigError(Exception):
    """Raised when configuration is missing, malformed, or out of range.

    The message always names the offending key so operators can find it in
    TOML or the systemd env drop-in immediately.
    """


@dataclass
class LaneRule:
    """One entry of ``[[classify.lanes]]``: a body/system regex match rule."""

    name: str
    match: list[str]
    match_system: list[str] = field(default_factory=list)
    tier: int = 1
    interactive: bool = False
    demote_to: str | None = None


@dataclass
class DemoteRule:
    """One entry of ``[[classify.demote]]``: heartbeat/canary style exclusions.

    When ``first_user_match`` hits the first user message AND the turn has
    ``<= max_messages`` messages, the classification is demoted: label
    becomes ``name``, tier becomes ``tier``, interactive is forced False, and
    lane becomes the matched lane's ``demote_to`` (if set).
    """

    name: str
    first_user_match: str
    max_messages: int = 40
    tier: int = 2


@dataclass
class LabelRule:
    """One entry of ``[[classify.labels]]``: a workload label, optionally with its own tier.

    A third tier band keyed on workload *label* sits on top of lane/demote:
    an interactive lane is HIGH, ops hooks such as heartbeats are LOW (via
    demote), and everything else is split by label (e.g. scheduled-delivery
    labels -> MEDIUM, background synthesis labels -> LOW; unknown -> MEDIUM).
    ``tier`` (optional; ``None`` means "don't override") generalises that:
    when a ``LabelRule`` matches, its ``tier`` (if set) replaces the
    lane/default tier, same as a demote rule would.
    """

    match: str
    label: str
    tier: int | None = None


@dataclass
class Config:
    """Fully resolved slotpin configuration (defaults < TOML < env)."""

    listen_host: str = "127.0.0.1"
    listen_port: int = 8090
    upstream_host: str = "127.0.0.1"
    upstream_port: int = 8080
    admin_host: str = "127.0.0.1"
    admin_port: int = 5802
    # What serves behind the proxy. "llamacpp": llama-server (slot pinning,
    # KV save/restore, /props reload detection). "gufo": the Gufo engine, which
    # owns prompt caching itself -- slotpin then only classifies, schedules,
    # relays and records metrics (see docs/operations.md).
    upstream_kind: str = "llamacpp"
    mode: str = "single"
    interactive_slot: int = 1
    default_slot: int = 0
    kv_persist: bool = False
    kv_adaptive: bool = False
    sched_enabled: bool = False
    slot_save_path: Path = field(default_factory=lambda: Path("/var/lib/slotpin/kv-cache"))
    recur_threshold: int = 3
    snapshot_ttl_h: float = 48.0
    prefix_save_min_interval_s: float = 300.0
    # Longest-common-prefix disk restore (see prefix_index.py). When a request
    # is cold and has no exact-hash snapshot, restore the saved snapshot whose
    # RENDERED prompt shares the longest leading run of chunks with this one,
    # so a changed system-message tail (date, per-run task text) no longer forfeits
    # the whole stable head. Off by default; requires kv_persist.
    prefix_restore: bool = False
    prefix_chunk_chars: int = 4096
    # Minimum shared rendered prefix (chars) worth a restore; below this the
    # snapshot read costs more than the prefill it saves. ~32K chars ~= 8K tokens.
    prefix_restore_min_chars: int = 32768
    cache_ram_mb: int = 2048
    bytes_per_token: int = 16900
    cram_safety: float = 0.85
    # Number of KV slots the upstream serves (llama-server -np). Pooled: turns
    # are routed to any free slot 0..slots-1 (v8 concurrency). Keep = max_inflight
    # for full concurrency; 1 = today's single-slot behaviour.
    slots: int = 1
    max_inflight: int = 1
    queue_timeout_s: float = 600.0
    # Anti-starvation aging horizon (SLOTPIN_MAX_WAIT_S, default 60):
    # a waiter's effective admission tier continuously rises toward the
    # highest priority as it queues, reaching it once waited >= max_wait_s.
    max_wait_s: float = 60.0
    # Optional backpressure shedding of low-priority waiters under sustained
    # saturation (SLOTPIN_SCHED_BACKPRESSURE, default off).
    sched_backpressure: bool = False
    # A waiter's tier must be >= this to be backpressure-eligible (20 is the
    # LOW tier on the reference profile's tier scale).
    backpressure_min_tier: int = 20
    # How long a backpressure-eligible waiter may queue before being shed
    # (SLOTPIN_BP_DROP_S, default 45).
    bp_drop_s: float = 45.0
    upstream_timeout_s: float = 1800.0
    # Fail fast on a silent upstream (streaming chat completions only; both off
    # by default -- see docs/configuration.md "Fail-fast on a silent upstream").
    # Longest time (s) with no real output event AND no token progress in the
    # upstream's /slots counters before the upstream request is aborted; SSE
    # comments (": ping") and the role-only first event are not output. 0 disables.
    upstream_stall_timeout_s: float = 0.0
    # Send nothing to the client (not even the status line) until the first
    # output event, so a stall before it becomes a clean 504 instead of a cut
    # 200 stream.
    hold_headers_until_first_output: bool = False
    admin_timeout_s: float = 30.0
    downstream_write_timeout_s: float = 30.0
    metrics_file: Path | None = field(default_factory=lambda: Path("/var/lib/slotpin/metrics.jsonl"))
    metrics_rotate_mb: float = 32.0
    metrics_keep: int = 3
    # Optional bench/experiment tag stamped on every per-turn metric record
    # (v7 metric layer). A per-request ``X-Slotpin-Run-Id`` header overrides
    # this default, so a benchmarking harness can label individual runs
    # without a config change; left None for normal production traffic.
    run_id: str | None = None
    # Explicit upstream serving-config identity, folded into `config_fingerprint`
    # so two configs that share model + capacity but differ in serving flags
    # (e.g. plain vs native-MTP/speculative) do NOT collide. The proxy cannot
    # reliably derive MTP/spec/flags from `/props`, so the operator/harness sets
    # this per serving config (e.g. "qwen36-plain" vs "qwen36-mtp-n2"). None ->
    # the fingerprint reflects model + capacity only.
    upstream_config_id: str | None = None
    # Whole-box energy capture (v7): an ESPHome web_server energy-counter URL
    # (e.g. "http://plug.example.com/sensor/daily_energy") the proxy polls
    # post-relay to record the counter value + per-turn Wh increment. None
    # disables it (all-None energy columns). See slotpin/energy.py.
    energy_url: str | None = None
    energy_timeout_s: float = 1.0
    # Background energy-poll cadence (s). The plug counter is refreshed into
    # memory this often; per-turn snapshots read that cache (non-blocking).
    # Match the plug sensor's own update_interval (~1s); no gain polling faster.
    energy_poll_s: float = 1.0
    # When energy_url points at an ESPHome Prometheus /metrics endpoint, the
    # sensor id to extract (e.g. "daily_energy"); unused for /sensor/<id> JSON.
    energy_metric: str | None = None
    # Background memory-poll cadence (s) for the MemPoller: sysfs GTT/VRAM +
    # /proc/meminfo sampled this often so each turn's record carries
    # start/avg/max/end residency (the intra-turn peak an end-of-turn spot
    # sample misses), not just an end snapshot. Local reads, so ~free.
    mem_poll_s: float = 1.0
    reload_detect: bool = True
    reload_poll_s: float = 10.0
    prefix_hash_chars: int = 4000
    latest_alias: str = "interactive-latest"
    capture_400: bool = False
    # Tier used when no lane matches AND no LabelRule with an explicit tier
    # matches. Falls back to MEDIUM (10) for any unrecognised label ("unknown ==
    # MEDIUM") rather than the lowest tier, so an unclassified workload
    # doesn't queue behind every recognised background job.
    default_tier: int = 10
    lanes: list[LaneRule] = field(default_factory=list)
    demote: list[DemoteRule] = field(default_factory=list)
    labels: list[LabelRule] = field(default_factory=list)

    @property
    def cram_token_cap(self) -> int:
        """Derived inferred cache-ram token budget.

        ``int(cache_ram_mb * 1024**2 / bytes_per_token * cram_safety)`` --
        MUST stay in lock-step with the llama-server ``--cache-ram`` value
        (the cache-ram sizing rule); a mismatch here silently over/under-models the
        real pool and suppresses or over-triggers restores.
        """
        return int(self.cache_ram_mb * 1024**2 / self.bytes_per_token * self.cram_safety)


# --- field kind registry (drives env/TOML scalar conversion) --------------

_SCALAR_FIELD_KINDS: dict[str, str] = {
    "listen_host": "str",
    "listen_port": "int",
    "upstream_host": "str",
    "upstream_port": "int",
    "admin_host": "str",
    "admin_port": "int",
    "upstream_kind": "str",
    "mode": "str",
    "interactive_slot": "int",
    "default_slot": "int",
    "kv_persist": "bool",
    "kv_adaptive": "bool",
    "sched_enabled": "bool",
    "slot_save_path": "path",
    "recur_threshold": "int",
    "snapshot_ttl_h": "float",
    "prefix_save_min_interval_s": "float",
    "prefix_restore": "bool",
    "prefix_chunk_chars": "int",
    "prefix_restore_min_chars": "int",
    "cache_ram_mb": "int",
    "bytes_per_token": "int",
    "cram_safety": "float",
    "slots": "int",
    "max_inflight": "int",
    "queue_timeout_s": "float",
    "max_wait_s": "float",
    "sched_backpressure": "bool",
    "backpressure_min_tier": "int",
    "bp_drop_s": "float",
    "upstream_timeout_s": "float",
    "upstream_stall_timeout_s": "float",
    "hold_headers_until_first_output": "bool",
    "admin_timeout_s": "float",
    "downstream_write_timeout_s": "float",
    "metrics_file": "path_or_none",
    "metrics_rotate_mb": "float",
    "metrics_keep": "int",
    "run_id": "str_or_none",
    "upstream_config_id": "str_or_none",
    "energy_url": "str_or_none",
    "energy_timeout_s": "float",
    "energy_poll_s": "float",
    "energy_metric": "str_or_none",
    "mem_poll_s": "float",
    "reload_detect": "bool",
    "reload_poll_s": "float",
    "prefix_hash_chars": "int",
    "latest_alias": "str",
    "capture_400": "bool",
    "default_tier": "int",
}

_STRUCTURED_FIELDS = {"lanes", "demote", "labels"}

# Legacy env spellings that do not fall out of the mechanical SLOTPIN_<UPPER>
# derivation, kept for systemd drop-in compatibility.
LEGACY_ENV_ALIASES: dict[str, str] = {
    "SLOTPIN_MODE": "mode",
    "SLOTPIN_KV_PERSIST": "kv_persist",
    "SLOTPIN_KV_ADAPTIVE": "kv_adaptive",
    "SLOTPIN_SCHED": "sched_enabled",
    "SLOTPIN_CACHE_RAM_MB": "cache_ram_mb",
    "SLOTPIN_SLOT_SAVE_PATH": "slot_save_path",
    "SLOTPIN_METRICS": "metrics_file",
}


def _parse_bool(raw: str, key: str) -> bool:
    v = raw.strip().lower()
    if v in ("on", "1", "true", "yes"):
        return True
    if v in ("off", "0", "false", "no"):
        return False
    raise ConfigError(f"{key}: not a boolean value: {raw!r}")


def _convert(kind: str, raw: Any, key: str) -> Any:
    try:
        if kind == "bool":
            if isinstance(raw, bool):
                return raw
            return _parse_bool(str(raw), key)
        if kind == "int":
            return int(raw)
        if kind == "float":
            return float(raw)
        if kind == "path":
            return Path(str(raw))
        if kind == "path_or_none":
            s = str(raw).strip()
            if s == "" or s.lower() in ("off", "none", "null", "disabled"):
                return None
            return Path(s)
        if kind == "str_or_none":
            s = str(raw).strip()
            if s == "" or s.lower() in ("off", "none", "null", "disabled"):
                return None
            return s
        return str(raw)
    except ConfigError:
        raise
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{key}: invalid value {raw!r} ({exc})") from exc


def _default_for(f: Any) -> Any:
    if f.default is not MISSING:
        return f.default
    if f.default_factory is not MISSING:  # type: ignore[misc]
        return f.default_factory()  # type: ignore[misc]
    raise ConfigError(f"field {f.name!r} has no default")


def _read_toml(path: Path) -> dict[str, Any]:
    try:
        with open(path, "rb") as fh:
            return tomllib.load(fh)
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"config file {path}: invalid TOML: {exc}") from exc


def _lane_from_toml(t: Mapping[str, Any]) -> LaneRule:
    if "name" not in t:
        raise ConfigError("classify.lanes entry missing required key 'name'")
    if "match" not in t:
        raise ConfigError(f"classify.lanes[{t['name']}] missing required key 'match'")
    return LaneRule(
        name=str(t["name"]),
        match=[str(p) for p in t["match"]],
        match_system=[str(p) for p in t.get("match_system", [])],
        tier=int(t.get("tier", 1)),
        interactive=bool(t.get("interactive", False)),
        demote_to=(str(t["demote_to"]) if t.get("demote_to") is not None else None),
    )


def _demote_from_toml(t: Mapping[str, Any]) -> DemoteRule:
    if "name" not in t:
        raise ConfigError("classify.demote entry missing required key 'name'")
    if "first_user_match" not in t:
        raise ConfigError(f"classify.demote[{t['name']}] missing required key 'first_user_match'")
    return DemoteRule(
        name=str(t["name"]),
        first_user_match=str(t["first_user_match"]),
        max_messages=int(t.get("max_messages", 40)),
        tier=int(t.get("tier", 2)),
    )


def _label_from_toml(t: Mapping[str, Any]) -> LabelRule:
    if "match" not in t or "label" not in t:
        raise ConfigError("classify.labels entry requires both 'match' and 'label'")
    return LabelRule(
        match=str(t["match"]),
        label=str(t["label"]),
        tier=(int(t["tier"]) if t.get("tier") is not None else None),
    )


def _compile_or_raise(pattern: str, where: str) -> None:
    try:
        re.compile(pattern)
    except re.error as exc:
        raise ConfigError(f"{where}: invalid regex {pattern!r}: {exc}") from exc


def _require_positive(value: float, key: str) -> None:
    if not (value > 0):
        raise ConfigError(f"{key}: must be > 0, got {value}")


def _require_nonneg(value: float, key: str) -> None:
    if value < 0:
        raise ConfigError(f"{key}: must be >= 0, got {value}")


def _require_positive_int(value: int, key: str) -> None:
    if not (isinstance(value, int) and value > 0):
        raise ConfigError(f"{key}: must be a positive integer, got {value}")


def _require_port(value: int, key: str) -> None:
    if not (isinstance(value, int) and 1 <= value <= 65535):
        raise ConfigError(f"{key}: must be a valid TCP port (1-65535), got {value}")


UPSTREAM_KINDS = ("llamacpp", "gufo")


def _validate(cfg: Config) -> None:
    """Validate a fully-assembled Config, raising ConfigError naming the key."""
    if cfg.mode not in ("single", "dual"):
        raise ConfigError(f"mode: must be 'single' or 'dual', got {cfg.mode!r}")
    if cfg.upstream_kind not in UPSTREAM_KINDS:
        raise ConfigError(
            f"upstream_kind: must be 'llamacpp' or 'gufo', got {cfg.upstream_kind!r}"
        )
    if cfg.upstream_kind == "gufo":
        # Gufo keeps its own in-memory + disk continuation cache and has no
        # llama-server slot save/restore API; slotpin's KV warmth would fight it.
        on = [k for k in ("kv_persist", "kv_adaptive", "prefix_restore") if getattr(cfg, k)]
        if on:
            raise ConfigError(
                f"upstream_kind = 'gufo' is incompatible with {', '.join(on)} = true: "
                "Gufo owns prompt caching itself; set them false"
            )

    interactive_lanes = [lane for lane in cfg.lanes if lane.interactive]
    if len(interactive_lanes) > 1:
        names = ", ".join(lane.name for lane in interactive_lanes)
        raise ConfigError(f"classify.lanes: at most one lane may set interactive=true, got: {names}")

    for lane in cfg.lanes:
        for pat in lane.match:
            _compile_or_raise(pat, f"classify.lanes[{lane.name}].match")
        for pat in lane.match_system:
            _compile_or_raise(pat, f"classify.lanes[{lane.name}].match_system")
    for demote in cfg.demote:
        _compile_or_raise(demote.first_user_match, f"classify.demote[{demote.name}].first_user_match")
    for label in cfg.labels:
        _compile_or_raise(label.match, f"classify.labels[{label.label}].match")

    # listen_port 0 = bind an ephemeral port (useful for tests / port-from-systemd setups)
    if not (isinstance(cfg.listen_port, int) and 0 <= cfg.listen_port <= 65535):
        raise ConfigError(f"listen_port: must be 0-65535, got {cfg.listen_port}")
    _require_port(cfg.upstream_port, "upstream_port")
    _require_port(cfg.admin_port, "admin_port")
    _require_nonneg(cfg.interactive_slot, "interactive_slot")
    _require_nonneg(cfg.default_slot, "default_slot")
    _require_positive_int(cfg.recur_threshold, "recur_threshold")
    _require_positive(cfg.snapshot_ttl_h, "snapshot_ttl_h")
    _require_nonneg(cfg.prefix_save_min_interval_s, "prefix_save_min_interval_s")
    _require_positive_int(cfg.prefix_chunk_chars, "prefix_chunk_chars")
    _require_nonneg(cfg.prefix_restore_min_chars, "prefix_restore_min_chars")
    _require_positive(cfg.cache_ram_mb, "cache_ram_mb")
    _require_positive(cfg.bytes_per_token, "bytes_per_token")
    if not (0 < cfg.cram_safety <= 1.5):
        raise ConfigError(f"cram_safety: must be in (0, 1.5], got {cfg.cram_safety}")
    _require_positive_int(cfg.max_inflight, "max_inflight")
    _require_positive_int(cfg.slots, "slots")
    # v8 concurrency invariant: pooled admission must never exceed the slot
    # count, or two turns could be routed to the same slot (double-free of
    # slot_busy + a save capturing another turn's KV). Admission caps live
    # turns at max_inflight, so require max_inflight <= slots.
    if cfg.max_inflight > cfg.slots:
        raise ConfigError(
            f"max_inflight ({cfg.max_inflight}) must be <= slots ({cfg.slots}): "
            "pooled admission cannot exceed the slot count"
        )
    # Multi-slot concurrency relies on the scheduler to cap admission; without
    # it, acquire() is a non-blocking no-op that ignores max_inflight, so
    # concurrent clients could exceed the slots.
    if cfg.slots > 1 and not cfg.sched_enabled:
        raise ConfigError("slots > 1 requires sched_enabled = true (admission caps concurrency)")
    _require_positive(cfg.queue_timeout_s, "queue_timeout_s")
    _require_positive(cfg.max_wait_s, "max_wait_s")
    _require_nonneg(cfg.backpressure_min_tier, "backpressure_min_tier")
    _require_positive(cfg.bp_drop_s, "bp_drop_s")
    _require_positive(cfg.upstream_timeout_s, "upstream_timeout_s")
    if not (cfg.upstream_stall_timeout_s >= 0 and math.isfinite(cfg.upstream_stall_timeout_s)):
        raise ConfigError(
            "upstream_stall_timeout_s: must be a finite number >= 0 (0 = off), "
            f"got {cfg.upstream_stall_timeout_s}"
        )
    _require_positive(cfg.admin_timeout_s, "admin_timeout_s")
    _require_positive(cfg.downstream_write_timeout_s, "downstream_write_timeout_s")
    _require_positive(cfg.metrics_rotate_mb, "metrics_rotate_mb")
    _require_positive_int(cfg.metrics_keep, "metrics_keep")
    _require_positive(cfg.reload_poll_s, "reload_poll_s")
    _require_positive(cfg.mem_poll_s, "mem_poll_s")
    _require_positive_int(cfg.prefix_hash_chars, "prefix_hash_chars")
    _require_nonneg(cfg.default_tier, "default_tier")


def runtime_path_warnings(cfg: Config) -> list[str]:
    """Advisory (non-fatal) warnings about on-disk paths (and one timeout pairing).

    Separate from :func:`validate` (which raises on hard invariants). The one
    that matters: a KV-snapshot dir on a RAM filesystem (tmpfs). llama-server
    writes each slot's KV under its ``--slot-save-path`` and slotpin globs
    ``slot_save_path`` for the same ``*.bin`` files -- if that path is tmpfs,
    every snapshot eats RAM and (on a unified-memory APU) steals memory
    bandwidth from decode, instead of paging off NVMe as the warm lane intends.
    Surfacing it at ``--check`` and at startup turns a silent RAM balloon into a
    visible warning. See ``docs/operations.md``.
    """
    from slotpin import sysmem

    warnings: list[str] = []
    if cfg.kv_persist and sysmem.is_tmpfs(cfg.slot_save_path):
        warnings.append(
            f"slot_save_path {cfg.slot_save_path} is on a RAM filesystem (tmpfs/ramfs): "
            "KV snapshots will consume RAM and steal decode bandwidth instead of paging off "
            "disk. Use a persistent (NVMe) path, and ensure it matches the upstream "
            "llama-server --slot-save-path."
        )
    if cfg.metrics_file is not None and sysmem.is_tmpfs(cfg.metrics_file.parent):
        warnings.append(
            f"metrics_file dir {cfg.metrics_file.parent} is on tmpfs: the metric stream "
            "will not survive a reboot."
        )
    # Not a path, but the same advisory channel (--check + startup log).
    if cfg.upstream_stall_timeout_s >= cfg.upstream_timeout_s:
        warnings.append(
            f"upstream_stall_timeout_s ({cfg.upstream_stall_timeout_s:g}) is not below "
            f"upstream_timeout_s ({cfg.upstream_timeout_s:g}): on a silent upstream the socket "
            "timeout fires first and the turn is reported as a timeout, not a stall."
        )
    return warnings


def load_config(path: Path | None = None, env: Mapping[str, str] | None = None) -> Config:
    """Load configuration: dataclass defaults < TOML file < environment.

    :param path: optional TOML file path (``--config`` in the CLI).
    :param env: environment mapping to read overrides from; defaults to
        ``os.environ`` (injectable for tests).
    :raises ConfigError: on any parse, type, or validation failure. The
        message always names the offending key.
    """
    values: dict[str, Any] = {
        f.name: _default_for(f) for f in fields(Config) if f.name not in _STRUCTURED_FIELDS
    }
    lanes: list[LaneRule] = []
    demote: list[DemoteRule] = []
    labels: list[LabelRule] = []

    if path is not None:
        toml_data = _read_toml(path)
        for key, raw in toml_data.items():
            if key == "classify":
                if not isinstance(raw, Mapping):
                    raise ConfigError("classify: must be a table")
                lanes = [_lane_from_toml(t) for t in raw.get("lanes", [])]
                demote = [_demote_from_toml(t) for t in raw.get("demote", [])]
                labels = [_label_from_toml(t) for t in raw.get("labels", [])]
                continue
            kind = _SCALAR_FIELD_KINDS.get(key)
            if kind is None:
                raise ConfigError(f"unknown config key in TOML: {key!r}")
            values[key] = _convert(kind, raw, key)
        logger.debug("config: loaded TOML overrides from %s", path)

    env_map: Mapping[str, str] = os.environ if env is None else env
    for field_name, kind in _SCALAR_FIELD_KINDS.items():
        env_name = f"SLOTPIN_{field_name.upper()}"
        if env_name in env_map:
            values[field_name] = _convert(kind, env_map[env_name], env_name)
    for alias, field_name in LEGACY_ENV_ALIASES.items():
        if alias in env_map:
            values[field_name] = _convert(_SCALAR_FIELD_KINDS[field_name], env_map[alias], alias)

    cfg = Config(**values, lanes=lanes, demote=demote, labels=labels)
    _validate(cfg)
    return cfg
