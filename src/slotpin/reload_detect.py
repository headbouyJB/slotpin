"""Detect llama-server model reloads/restarts and reset stale proxy warmth state.

Relationship to the restart-pairing hook: stale warmth after a reload can
also be handled operationally via a systemd drop-in whose
``ExecStartPost=-+systemctl --no-block restart slotpin-proxy.service`` forces
a full slotpin-proxy PROCESS restart on every llama-swap (re)start, which
trivially clears all in-memory state. This repo ships that drop-in
(``deploy/systemd/llama-swap-pair-slotpin.conf``).

This module is defense-in-depth on top of that hook:
in-process detection catches cases the restart-pairing hook cannot (llama-
swap's own internal ~2s config-file poll or an in-place ``/upstream`` model
swap, neither of which restarts the llama-swap systemd *unit* and so never
fires ``ExecStartPost``). It is gated by
``cfg.reload_detect`` (default on) so it can be disabled if its heuristics
ever cause an issue the operational hook alone would not.

``ReloadDetector`` builds its signature from whichever of the following
``GET /props`` fields are present (all optional; missing fields degrade to
"can't tell", which per contract must NEVER be treated as a reload). These
field names are best-effort guesses at the llama-server ``/props`` shape,
which varies between builds:

* **model identity** -- first of ``model_path``, ``model``,
  ``default_generation_settings.model`` found in the ``/props`` payload.
  A reload is flagged if this value is present in both the previous and the
  current poll and differs.
* **uptime/start-time-like counter** -- first of ``uptime_s``, ``uptime``,
  ``start_time``, ``started_at`` found. Values >= 1e9 are treated as an
  absolute Unix epoch "process started at" timestamp (a reload changes this
  value); smaller values are treated as a monotonically-increasing
  "seconds since start" counter (a reload resets it back towards zero, i.e.
  the new reading is lower than the previous one). A reload is flagged only
  when both readings are present.
* **cumulative task/token counter** -- first of ``total_tasks``,
  ``n_tasks_processed``, ``n_decode_total``, ``total_decoded_tokens``,
  ``tokens_predicted_total`` found. Cumulative counters must never legitimately
  decrease while the same process is alive; a decrease is flagged as a reload.
  A reload is flagged only when both readings are present.

If *none* of the three signals are comparable across the two polls (either
because ``/props`` is unreachable, or because the live build simply doesn't
expose any of these field names), ``check_once`` returns ``False``: an
inconclusive read is never a false-positive reload. If your llama-server build
exposes different field names, update the candidate key lists above.

**Gufo** (``upstream_kind = "gufo"``) has no usable ``/props`` (400 without
``?model=``, and no model path or counters with it), so the detector polls
``GET /v1/models`` for identity (``data[0].id`` + ``context_length``; its
``created`` is the request time, not process start, so it is ignored) and
``GET /metrics`` for the cumulative ``llamacpp:prompt_tokens_total`` /
``llamacpp:tokens_predicted_total`` counters, which reset when Gufo restarts.
A changed identity or a regressed counter is a reload. A poll where either
endpoint fails is inconclusive and leaves the baseline untouched.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .config import Config
    from .pool import CacheRamPool
    from .slot_admin import SlotAdmin
    from .state import ProxyState

logger = logging.getLogger("slotpin.reload_detect")

_MODEL_KEYS: tuple[str, ...] = ("model_path", "model")
_UPTIME_KEYS: tuple[str, ...] = ("uptime_s", "uptime", "start_time", "started_at")
_COUNTER_KEYS: tuple[str, ...] = (
    "total_tasks",
    "n_tasks_processed",
    "n_decode_total",
    "total_decoded_tokens",
    "tokens_predicted_total",
)

_EPOCH_LIKE_THRESHOLD = 1_000_000_000.0  # values at/above this are treated as absolute epoch
_UPTIME_REGRESSION_TOLERANCE_S = 1.0
_EPOCH_CHANGE_TOLERANCE_S = 1.0


@dataclass(frozen=True)
class _Signature:
    """The subset of ``/props`` state used to detect a reload; see module docstring."""

    model_id: str | None
    uptime_like: float | None
    task_counter: int | None


@dataclass(frozen=True)
class _GufoSignature:
    """Gufo reload signature: ``/v1/models`` identity + ``/metrics`` counters."""

    model_id: str | None
    context_length: int | None
    prompt_tokens_total: float | None
    tokens_predicted_total: float | None


_GUFO_COUNTERS: tuple[str, ...] = ("prompt_tokens_total", "tokens_predicted_total")


def _gufo_identity(models: dict[str, Any]) -> tuple[str | None, int | None]:
    """``(data[0].id, data[0].context_length)`` from a ``/v1/models`` payload."""
    data = models.get("data")
    if not isinstance(data, list) or not data or not isinstance(data[0], dict):
        return None, None
    model_id = data[0].get("id")
    ctx = data[0].get("context_length")
    return (
        str(model_id) if model_id is not None else None,
        ctx if isinstance(ctx, int) else None,
    )


def _compare_gufo(prev: _GufoSignature, cur: _GufoSignature) -> str | None:
    """Reload reason for two Gufo signatures, else ``None`` (same both-present rule)."""
    if prev.model_id is not None and cur.model_id is not None and prev.model_id != cur.model_id:
        return f"model changed: {prev.model_id!r} -> {cur.model_id!r}"
    if (
        prev.context_length is not None
        and cur.context_length is not None
        and prev.context_length != cur.context_length
    ):
        return f"context_length changed: {prev.context_length} -> {cur.context_length}"
    for name in _GUFO_COUNTERS:
        a, b = getattr(prev, name), getattr(cur, name)
        if a is not None and b is not None and b < a:
            return f"llamacpp:{name} regressed: {a:g} -> {b:g}"
    return None


def _first_present(d: dict[str, Any], keys: tuple[str, ...]) -> Any | None:
    """Return the first non-``None`` value among ``keys`` in ``d`` (shallow lookup)."""
    for key in keys:
        if key in d and d[key] is not None:
            return d[key]
    return None


def _extract_signature(props: dict[str, Any]) -> _Signature:
    """Build a comparable signature from a single ``/props`` payload."""
    model_id = _first_present(props, _MODEL_KEYS)
    if model_id is None:
        nested = props.get("default_generation_settings")
        if isinstance(nested, dict):
            model_id = nested.get("model")
    if model_id is not None:
        model_id = str(model_id)

    uptime_raw = _first_present(props, _UPTIME_KEYS)
    uptime_like: float | None
    try:
        uptime_like = float(uptime_raw) if uptime_raw is not None else None
    except (TypeError, ValueError):
        uptime_like = None

    counter_raw = _first_present(props, _COUNTER_KEYS)
    task_counter: int | None
    try:
        task_counter = int(counter_raw) if counter_raw is not None else None
    except (TypeError, ValueError):
        task_counter = None

    return _Signature(model_id=model_id, uptime_like=uptime_like, task_counter=task_counter)


def _compare(prev: _Signature, cur: _Signature) -> str | None:
    """Return a human-readable reason if ``cur`` indicates a reload vs ``prev``, else ``None``.

    Only compares fields present on *both* sides; an absent field on either
    side is simply skipped (never counted as evidence of a reload).
    """
    if prev.model_id is not None and cur.model_id is not None and prev.model_id != cur.model_id:
        return f"model changed: {prev.model_id!r} -> {cur.model_id!r}"

    if prev.uptime_like is not None and cur.uptime_like is not None:
        if prev.uptime_like >= _EPOCH_LIKE_THRESHOLD or cur.uptime_like >= _EPOCH_LIKE_THRESHOLD:
            if abs(cur.uptime_like - prev.uptime_like) > _EPOCH_CHANGE_TOLERANCE_S:
                return f"start-time changed: {prev.uptime_like} -> {cur.uptime_like}"
        else:
            if cur.uptime_like < prev.uptime_like - _UPTIME_REGRESSION_TOLERANCE_S:
                return f"uptime regressed: {prev.uptime_like} -> {cur.uptime_like}"

    if prev.task_counter is not None and cur.task_counter is not None:
        if cur.task_counter < prev.task_counter:
            return f"cumulative counter regressed: {prev.task_counter} -> {cur.task_counter}"

    return None


class ReloadDetector:
    """Polls the upstream on a daemon thread and resets warmth state on reload."""

    def __init__(self, cfg: Config, admin: SlotAdmin, pool: CacheRamPool, state: ProxyState) -> None:
        """Store collaborators; does not start polling until :meth:`start` is called."""
        self._cfg = cfg
        self._admin = admin
        self._pool = pool
        self._state = state
        self._gufo = cfg.upstream_kind == "gufo"
        self._sig_lock = threading.Lock()
        self._last_signature: _Signature | _GufoSignature | None = None
        self._fallback_sig: tuple[float, _Signature | _GufoSignature] | None = None
        self._stop_evt = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the daemon polling thread (no-op if disabled or already running)."""
        if not self._cfg.reload_detect:
            logger.info("reload detection disabled (cfg.reload_detect=False)")
            return
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(target=self._run, name="reload-detect", daemon=True)
        self._thread.start()
        logger.info("reload detector started (poll interval=%.1fs)", self._cfg.reload_poll_s)

    def current_model_id(self) -> str | None:
        """Upstream model identity (GGUF path; Gufo: model id) for the
        prefix-restore hot path.

        Uses the poller's last signature when polling is running.
        Otherwise (right after startup, or ``reload_detect = false``) reads
        the upstream directly and caches it for ``reload_poll_s`` so a busy proxy
        doesn't pay an admin round trip on every turn. ``None`` when the upstream
        is unreachable or reports no model.
        """
        with self._sig_lock:
            sig = self._last_signature
            cached = self._fallback_sig
        if sig is not None:
            return sig.model_id
        now = time.monotonic()
        if cached is not None and now - cached[0] < self._cfg.reload_poll_s:
            return cached[1].model_id
        model_id = self.fresh_model_id()
        return model_id

    def fresh_model_id(self) -> str | None:
        """Uncached read of the upstream model identity (``/props``; Gufo:
        ``/v1/models`` ``data[0].id``).

        Called right before a prefix restore so a model swapped since the last
        poll can't have its predecessor's KV restored into it.
        """
        sig: _Signature | _GufoSignature
        if self._gufo:
            models = self._admin.models()
            if models is None:
                return None
            model_id, ctx = _gufo_identity(models)
            sig = _GufoSignature(model_id, ctx, None, None)
        else:
            props = self._admin.props()
            if props is None:
                return None
            sig = _extract_signature(props)
        with self._sig_lock:
            self._fallback_sig = (time.monotonic(), sig)
        return sig.model_id

    def _poll_gufo(self) -> _GufoSignature | None:
        """One Gufo signature, or ``None`` if either endpoint failed (inconclusive)."""
        models = self._admin.models()
        metrics = self._admin.upstream_metrics() if models is not None else None
        if models is None or metrics is None:
            return None
        model_id, ctx = _gufo_identity(models)
        return _GufoSignature(
            model_id,
            ctx,
            metrics.get("llamacpp:prompt_tokens_total"),
            metrics.get("llamacpp:tokens_predicted_total"),
        )

    def check_once(self) -> bool:
        """Poll the upstream once; return ``True`` iff a reload was detected on this call."""
        cur: _Signature | _GufoSignature | None
        if self._gufo:
            cur = self._poll_gufo()
        else:
            props = self._admin.props()
            cur = _extract_signature(props) if props is not None else None
        if cur is None:
            logger.debug("reload-detect: upstream unavailable this poll; treating as inconclusive")
            return False

        with self._sig_lock:
            prev = self._last_signature
            self._last_signature = cur

        if prev is None:
            logger.debug("reload-detect: baseline signature established: %r", cur)
            return False

        if isinstance(prev, _GufoSignature) and isinstance(cur, _GufoSignature):
            reason = _compare_gufo(prev, cur)
        elif isinstance(prev, _Signature) and isinstance(cur, _Signature):
            reason = _compare(prev, cur)
        else:
            reason = None
        if reason is None:
            return False

        logger.warning(
            "RELOAD DETECTED (%s) -- clearing inferred cache-ram pool and slot state", reason
        )
        with self._state.lock:
            self._state.counters["reloads_detected"] = (
                self._state.counters.get("reloads_detected", 0) + 1
            )
        self._pool.clear()
        self._state.reset_for_reload()
        return True

    def stop(self) -> None:
        """Signal the polling thread to stop and join it (best-effort, bounded wait)."""
        self._stop_evt.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def _run(self) -> None:
        """Daemon thread body: establish a baseline immediately, then poll periodically."""
        try:
            self.check_once()
        except Exception:
            logger.exception("reload-detect: error establishing baseline")
        while not self._stop_evt.wait(self._cfg.reload_poll_s):
            try:
                self.check_once()
            except Exception:
                logger.exception("reload-detect: unexpected error during poll")
