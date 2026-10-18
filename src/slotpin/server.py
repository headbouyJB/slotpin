"""The HTTP proxy server: the 10-step chat pipeline plus stats/health/passthrough.

Ordering inside the chat pipeline is behaviourally load-bearing (see
the pipeline steps and invariants below). In short:

1. classify (read-only, never mutates shared state);
2. inject ``id_slot`` (llama.cpp only; Gufo picks its own session) + force
   ``stream_options.include_usage``;
3. decide (read-only) whether a KV restore will be needed;
4. block on the scheduler for admission;
5. write ``state.slot_last`` -- only now, post-admission;
6. if a restore was decided, log unconditionally, then execute it;
7. forward the (possibly slot-pinned) request and relay the response;
8. always release the scheduler ticket;
9. best-effort, exception-swallowing post-relay tracking + metrics + save;
10. a 502 JSON body on upstream connect failure.

The relay mechanics (chunked transfer, ``SO_SNDTIMEO`` on the downstream
socket, a disconnect watcher that aborts the upstream connection, and a
16 KiB tail ring used to recover the trailing usage/timings JSON) are ported
from the original single-file proxy, whose relay mechanics proved reliable.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import select
import socket
import struct
import threading
import time
from datetime import datetime
from http import client as http_client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any

from .energy import EnergyPoller
from .prefix_index import chunk_hashes
from .stats import build_stats
from .sysmem import MemPoller

if TYPE_CHECKING:
    from .classify import Classification, Classifier
    from .config import Config
    from .kvstore import KVStore
    from .metrics import MetricsWriter
    from .pool import CacheRamPool
    from .reload_detect import ReloadDetector
    from .scheduler import SchedTicket, Scheduler
    from .slot_admin import SlotAdmin
    from .state import ProxyState

logger = logging.getLogger("slotpin.server")

# A streamed delta that carries output (text, reasoning or a tool call). TTFT is
# timed from the first one: Gufo sends a role-only event before prefill, which
# would otherwise read as an instant first token.
_TOKEN_DELTA = re.compile(rb'"(?:content|reasoning_content)":\s*"[^"]|"tool_calls":\s*\[')

# hold_headers_until_first_output: most upstream bytes buffered with no output
# event seen before the hold is given up and everything is flushed (safety valve).
_HOLD_MAX_BYTES = 256 * 1024

# Headers that must never be blindly relayed between proxy hops (RFC 7230 §6.1
# hop-by-hop set, plus Content-Length since we recompute it for the forwarded body).
_HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
        "content-length",
    }
)

# served="cold" heuristic baselines (FULL_BASELINE/MIN_BASELINE):
# the shared prefix any same-class predecessor is expected to leave resident in
# the slot; a post-relay cache_n below this (after an occupant change) implies
# the turn did NOT ride on leftover/restored KV and should be labelled cold.
_INTERACTIVE_COLD_BASELINE = 16000
_DEFAULT_COLD_BASELINE = 2000

# POST paths that carry an inference turn and get the full pin+instrument
# pipeline: OpenAI chat, OpenAI text-completions, and llama.cpp's native
# `/completion` (raw-prompt) endpoint. All three emit a `timings` block the
# metric layer parses; the native ones put token counts at top level rather
# than in a `usage` object (handled in _parse_usage_tail). `/completion`
# (singular) is llama.cpp-native; `/v1/completions` (plural) is the OpenAI form.
_INFERENCE_SUFFIXES = ("/v1/chat/completions", "/completion", "/v1/completions")


class _UpstreamUnreachable(Exception):
    """Raised internally when the upstream connect/request itself fails (contract step 10)."""


class _UpstreamStalled(Exception):
    """Raised internally when a streaming chat upstream produced no output for
    longer than ``upstream_stall_timeout_s``. Leaves :meth:`_forward_and_relay`
    only when nothing has been sent to the client yet (the caller sends a 504)."""


class _OutputScanner:
    """Incremental SSE scan: does a read carry a real output event?

    Real output is a ``data:`` line with a :data:`_TOKEN_DELTA` match (text,
    reasoning or tool-call delta). Comment lines (``: ping``), the role-only
    first event, usage/finish/``[DONE]`` frames and error frames are not. One
    read is not one event: lines are reassembled across reads, and the unfinished
    last line is kept (bounded) so a delta that straddles reads is still seen.
    """

    def __init__(self) -> None:
        self._line = bytearray()  # the current, not yet terminated, line

    def feed(self, chunk: bytes) -> bool:
        """Add one read; True when it completed or extended an output event."""
        self._line.extend(chunk)
        *lines, rest = self._line.split(b"\n")
        found = any(ln.startswith(b"data:") and _TOKEN_DELTA.search(ln) for ln in lines)
        if not found and rest.startswith(b"data:") and _TOKEN_DELTA.search(rest):
            found = True
        if len(rest) > 4096:
            # A long line was searched above: keep its "data:" marker and enough
            # of the end for a delta key that straddles the next read.
            rest = rest[:5] + rest[-256:]
        self._line = rest
        return found


def _abort_upstream(
    upstream_conn: http_client.HTTPConnection, sock: socket.socket | None = None
) -> None:
    """Tear the upstream request down so the engine stops work. Call with the
    relay's ``teardown`` lock held. ``sock`` is the connection's socket when the
    caller kept it (http.client drops ``conn.sock`` for a close-delimited reply)."""
    # close() alone does NOT interrupt a read already blocked in another
    # thread (the buffered reader holds the socket; the close is deferred
    # until bytes arrive -- i.e. never, mid-prefill). shutdown() wakes the
    # blocked recv and sends FIN, so llama-server cancels the task and
    # frees the slot.
    if sock is None:
        sock = getattr(upstream_conn, "sock", None)
    if sock is not None:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
    try:
        upstream_conn.close()
    except Exception:
        pass


def _filtered_headers(items: Any) -> list[tuple[str, str]]:
    """Drop hop-by-hop headers before relaying a header set to the other side."""
    return [(k, v) for k, v in items if k.lower() not in _HOP_BY_HOP]


def _set_downstream_write_timeout(sock: socket.socket, seconds: float) -> None:
    """Best-effort ``SO_SNDTIMEO`` so a stalled/slow client can't hang a relay thread forever."""
    try:
        tv = struct.pack("ll", int(seconds), 0)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO, tv)
    except OSError as exc:
        logger.warning("SO_SNDTIMEO unavailable: %s", exc)


def _start_disconnect_watcher(
    downstream_sock: socket.socket,
    upstream_conn: http_client.HTTPConnection,
    done_event: threading.Event,
    aborted: threading.Event,
    teardown: threading.Lock,
    upstream_sock: socket.socket | None = None,
) -> threading.Thread:
    """Watch for the client going away and abort upstream generation.

    Limitations (by design -- none of the fleet's clients do either): a client
    that half-closes its write side after sending the request is
    indistinguishable from a killed one (both read as EOF) and is aborted; a
    pipelining client's buffered next request masks its own disconnect.

    Without this, an abandoned client (killed sub-agent, closed tab, dead SSH
    tunnel) would leave llama-server prefilling/generating into the void,
    holding the slot. Runs from the moment the request is sent until the relay
    finishes (``done_event``).
    """
    def _abort(reason: str) -> None:
        with teardown:
            if done_event.is_set():
                return  # relay already finished and closed its upstream socket
            logger.info("disconnect-watcher: downstream %s; aborting upstream", reason)
            # Set BEFORE tearing down: once the upstream socket is shut, the
            # relay's read() may return b"" exactly like a normal end of stream
            # -- this flag is what tells them apart.
            aborted.set()
            _abort_upstream(upstream_conn, upstream_sock)

    def _watch() -> None:
        while not done_event.wait(0.5):
            try:
                readable, _, _ = select.select([downstream_sock], [], [], 1.0)
                if readable:
                    peeked = downstream_sock.recv(1, socket.MSG_PEEK)
                    if not peeked:
                        _abort("closed")
                        return
            except (ConnectionError, TimeoutError):
                # A killed client usually RSTs (unread response bytes) rather
                # than closing cleanly; a vanished host surfaces as ETIMEDOUT.
                # Both mean the client is gone.
                _abort("reset/gone")
                return
            except (OSError, ValueError):
                return  # our own socket was closed (relay finished)

    t = threading.Thread(target=_watch, daemon=True, name="disconnect-watcher")
    t.start()
    return t


class _StallWatch:
    """What a relay shares with its stall watchdog (:func:`_start_stall_watchdog`)."""

    def __init__(self, limit_s: float) -> None:
        self.limit_s = limit_s
        self.last_output = time.monotonic()  # request sent / last output event (relay writes)
        self.stop = threading.Event()  # relay: finished, or not a stream after all
        self.fired = threading.Event()  # watchdog: stall confirmed, upstream aborted
        self.stalled_s = 0.0
        # /slots token counters at the last two probes (set when fired)
        self.probes: list[list[tuple[int | None, int | None]] | None] = []


def _start_stall_watchdog(
    watch: _StallWatch,
    admin: SlotAdmin,
    upstream_conn: http_client.HTTPConnection,
    upstream_sock: socket.socket | None,
    aborted: threading.Event,
    teardown: threading.Lock,
) -> threading.Thread:
    """Abort an upstream that has stopped producing (``upstream_stall_timeout_s``).

    Stream silence alone is not a stall: Gufo sends nothing but ``: ping`` while
    it decodes a whole tool call, then one ``tool_calls`` delta. So each time the
    stream has gone half the limit without an output event, the upstream's token
    counters are read (``/slots``); the silence is a stall once two consecutive
    readings are equal, or the later one shows no busy slot or cannot be read.
    That is at the limit when nothing moves, and at most ~1.5x the limit after
    progress really stops. An output event starts over.

    A thread of its own, like the disconnect watcher, because the relay is
    blocked in the upstream read: it needs no bytes from the upstream to fire
    (a completely silent upstream trips it too), the probe never delays the
    relay, and no lock is held while probing. It aborts exactly as that watcher
    does and sets ``watch.fired`` first, which is how the relay tells a stall
    from an ordinary end of stream.
    """
    half = watch.limit_s / 2

    def _watch() -> None:
        probes: list[list[tuple[int | None, int | None]] | None] = []
        seen = watch.last_output
        probe_at = seen + half
        while not watch.stop.wait(max(0.0, probe_at - time.monotonic())):
            if watch.last_output != seen:  # output since: the silence starts over
                seen = watch.last_output
                probe_at = seen + half
                probes.clear()
                continue
            if time.monotonic() < probe_at:
                continue
            sig = admin.slots_progress()
            if not probes or (sig and sig != probes[-1]):
                if probes:
                    logger.debug("upstream silent but decoding: %s then %s", probes[-1], sig)
                probes.append(sig)
                probe_at = time.monotonic() + half
                continue
            with teardown:
                if watch.stop.is_set() or aborted.is_set():
                    return  # relay finished, or the client left first (aborted_client)
                if watch.last_output != seen:
                    continue
                watch.stalled_s = time.monotonic() - seen
                watch.probes = [probes[-1], sig]
                watch.fired.set()
                _abort_upstream(upstream_conn, upstream_sock)
            return

    t = threading.Thread(target=_watch, daemon=True, name="stall-watchdog")
    t.start()
    return t


def _stream_terminated(tail: bytes | bytearray) -> bool:
    """Does a relayed body end with its generator's terminal frame?

    OpenAI-style chat/completions streams end with ``data: [DONE]``; llama.cpp's
    native ``/completion`` stream ends with a ``"stop": true`` frame.
    """
    t = bytes(tail[-4096:])
    return b"data: [DONE]" in t or b'"stop":true' in t or b'"stop": true' in t


def _upstream_ended(resp: http_client.HTTPResponse, tail: bytes | bytearray) -> bool:
    """Did the upstream reach its real end: a fully read Content-Length body, or
    a stream carrying its terminal frame?"""
    if resp.length is not None:
        # A declared length is authoritative: never let content that merely
        # contains "stop": true (e.g. a non-streaming /completion body) mask a
        # short read.
        return resp.length == 0
    return _stream_terminated(tail)


def _stream_errored(tail: bytes | bytearray) -> bool:
    """Did the upstream report an error inside a (200) stream? Matches the JSON
    key form only; error text inside generated content is escaped (\\")."""
    t = bytes(tail[-4096:])
    return (
        b'data: {"error"' in t
        or b'data:{"error"' in t
        # older llama-server builds: a named SSE event "error: {...}", anchored
        # to a frame boundary so generated text can't match at the window edge
        or b"\n\nerror: {" in t
        or (len(tail) < 16384 and bytes(tail).startswith(b"error: {"))
    )


def _parse_usage_tail(tail: bytes) -> dict[str, Any]:
    """Recover usage + llama.cpp ``timings`` fields from the last ~16 KiB of a response.

    Deliberately regex-based (not a full JSON parse): the tail is a rolling
    window over a chunked SSE stream and is not guaranteed to start on a frame
    boundary, so treating it as one JSON document is unsafe. This also recovers
    ``timings`` (decode/prefill tokens-per-second, predicted_n) when the
    upstream build emits them in the final usage chunk.
    """
    text = tail.decode("utf-8", "ignore")
    usage_seg = text.rsplit('"usage"', 1)[-1] if '"usage"' in text else ""
    timings_seg = text.rsplit('"timings"', 1)[-1] if '"timings"' in text else ""

    def _grab_int(segment: str, key: str) -> int | None:
        m = re.search(rf'"{re.escape(key)}"\s*:\s*(\d+)', segment)
        return int(m.group(1)) if m else None

    def _grab_float(segment: str, key: str) -> float | None:
        m = re.search(rf'"{re.escape(key)}"\s*:\s*([0-9.]+)', segment)
        return float(m.group(1)) if m else None

    cache_n = _grab_int(usage_seg, "cached_tokens")
    prompt_n = _grab_int(usage_seg, "prompt_tokens")
    completion_n = _grab_int(usage_seg, "completion_tokens")

    # Native llama.cpp /completion has no `usage` object -- its counts sit at top
    # level / in `timings`. Resolve the reuse count FIRST: `timings.cache_n` is
    # prompt tokens served from cache (NOT top-level `tokens_cached`, which is the
    # post-turn KV size, not a cache hit).
    if cache_n is None:
        cache_n = _grab_int(timings_seg, "cache_n")
    # `prompt_n` is TOTAL prompt tokens (matching chat `usage.prompt_tokens`,
    # which includes cached). The native fields give only the EVALUATED-this-turn
    # count -- `tokens_evaluated` (== `timings.prompt_n`), which EXCLUDES the
    # cached prefix -- so total = evaluated + cached. Without this a cache_prompt
    # /completion turn recorded prompt_n=0 (tokens_evaluated absent -> None ->
    # `or 0`), dropping the context depth on native-endpoint runs.
    if prompt_n is None:
        evaluated = _grab_int(text, "tokens_evaluated")
        if evaluated is None:
            evaluated = _grab_int(timings_seg, "prompt_n")
        if evaluated is not None:
            prompt_n = evaluated + (cache_n or 0)
    if completion_n is None:
        completion_n = _grab_int(text, "tokens_predicted")
    if completion_n is None:
        completion_n = _grab_int(timings_seg, "predicted_n")

    decode_tps = _grab_float(timings_seg, "predicted_per_second")
    prefill_tps = _grab_float(timings_seg, "prompt_per_second")
    predicted_n = _grab_int(timings_seg, "predicted_n")
    # NB: do NOT fall back cache_n to timings.prompt_n -- that is tokens
    # *processed this turn* (i.e. NOT cached), which would invert the warm/cold
    # inference on builds that omit usage.cached_tokens. Absent -> None -> cold.

    # v7 metric layer: spec/MTP acceptance. llama-server emits draft_n +
    # draft_n_accepted inside the SAME timings object as predicted_per_second
    # (server_slot_stats::to_json, only when n_draft_tokens > 0), so they ride
    # the existing tail with no separate /slots or /metrics side-read. Absent
    # (None) whenever speculative decoding is inactive. The '"draft_n"' pattern
    # carries its own closing quote, so it does not match '"draft_n_accepted"'.
    draft_n = _grab_int(timings_seg, "draft_n")
    draft_n_accepted = _grab_int(timings_seg, "draft_n_accepted")

    # Upstream model/build identity for config_fingerprint: every OpenAI-compat
    # chunk carries top-level "model" (the GGUF path) and "system_fingerprint"
    # (llama.cpp build tag, e.g. "b10707-250b6144"). Grabbed from the whole tail
    # (not the usage/timings sub-segments); consistent across chunks so the
    # first match is representative. Absent -> None (degrades gracefully).
    def _grab_str(segment: str, key: str) -> str | None:
        m = re.search(rf'"{re.escape(key)}"\s*:\s*"([^"]*)"', segment)
        return m.group(1) if m else None

    model = _grab_str(text, "model")
    system_fingerprint = _grab_str(text, "system_fingerprint")

    # Why generation stopped. Chat emits a quoted `finish_reason`
    # (stop|length|tool_calls|...) only in the final chunk (intermediate chunks
    # are unquoted `null`, which the quoted-string grab skips); native
    # /completion emits `stop_type` (eos|limit|word|none). `"limit"`/`"length"`
    # mean the OUTPUT hit the generation budget (n_predict/max_tokens).
    finish_reason = _grab_str(text, "finish_reason")
    stop_type = _grab_str(text, "stop_type")

    return {
        "cache_n": cache_n,
        "prompt_n": prompt_n,
        "completion_n": completion_n,
        "decode_tps": decode_tps,
        "prefill_tps": prefill_tps,
        "predicted_n": predicted_n,
        "draft_n": draft_n,
        "draft_n_accepted": draft_n_accepted,
        "model": model,
        "system_fingerprint": system_fingerprint,
        "finish_reason": finish_reason,
        "stop_type": stop_type,
    }


_GUFO_CACHE_RE = re.compile(r'"gufo"\s*:\s*\{')


def _json_object_at(text: str, start: int) -> dict[str, Any] | None:
    """Parse the JSON object whose ``{`` is at ``text[start]``; None if cut off or invalid."""
    depth = 0
    in_str = esc = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        elif ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    obj = json.loads(text[start : i + 1])
                except ValueError:
                    return None
                return obj if isinstance(obj, dict) else None
    return None


def _parse_gufo_tail(tail: bytes) -> dict[str, Any] | None:
    """Recover Gufo's own cache report (``usage.gufo``) from a response tail.

    Uses the LAST ``"gufo": {...}`` object in the tail (the trailing ``timings``
    repeats some of these key names). Newer Gufo builds nest objects inside it
    (``component_cache``), so the object is parsed as JSON with brace matching;
    if it is cut off or malformed, the top-level keys are searched for in the
    text after the marker instead. ``None`` when the tail carries no such object.
    """
    text = tail.decode("utf-8", "ignore")
    matches = list(_GUFO_CACHE_RE.finditer(text))
    if not matches:
        return None
    obj = _json_object_at(text, matches[-1].end() - 1)
    if obj is not None:

        def _num(key: str) -> float | None:
            v = obj.get(key)
            return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None

        def _flag(key: str) -> bool:
            return obj.get(key) is True

        reason = obj.get("cache_miss_reason")
        reason = reason if isinstance(reason, str) else None
    else:
        seg = text[matches[-1].end() :]

        def _num(key: str) -> float | None:
            m = re.search(rf'"{re.escape(key)}"\s*:\s*(-?[0-9.]+(?:[eE][-+]?\d+)?)', seg)
            return float(m.group(1)) if m else None

        def _flag(key: str) -> bool:
            return re.search(rf'"{re.escape(key)}"\s*:\s*true', seg) is not None

        m = re.search(r'"cache_miss_reason"\s*:\s*"([^"]*)"', seg)
        reason = m.group(1) if m else None

    def _int(key: str) -> int | None:
        v = _num(key)
        return int(v) if v is not None else None

    if _flag("cache_disk_hit"):
        cache = "disk"
    elif _flag("cache_hit"):
        cache = "memory"
    else:
        cache = "miss"
    return {
        "upstream_cache": cache,
        "upstream_cache_miss_reason": reason,
        "upstream_restore_ms": _num("cache_restore_ms"),
        "upstream_restore_bytes": _int("cache_restore_bytes"),
        "upstream_snapshot_ms": _num("cache_snapshot_ms"),
        "upstream_snapshot_bytes": _int("cache_snapshot_bytes"),
        "upstream_disk_queued_bytes": _int("cache_disk_queued_bytes"),
    }


_GUFO_EMPTY: dict[str, Any] = {
    "upstream_cache": None,
    "upstream_cache_miss_reason": None,
    "upstream_restore_ms": None,
    "upstream_restore_bytes": None,
    "upstream_snapshot_ms": None,
    "upstream_snapshot_bytes": None,
    "upstream_disk_queued_bytes": None,
}


_FINGERPRINT_VERSION = 2  # config_fingerprint semantics; folded into the hash


def _config_fingerprint(cfg: Config, model: str | None) -> str:
    """Endpoint-independent identity of the serving configuration (v2).

    Hashes the proxy capacity/routing config + the upstream ``model`` + the
    explicit ``cfg.upstream_config_id`` -- the operator/harness-set tag that
    distinguishes otherwise-identical setups the proxy cannot introspect (e.g.
    **plain vs native-MTP/speculative**, which share model + capacity but are
    different serving configs and must NOT collide).

    Deliberately excludes two things:
    - the request **endpoint** -- so chat, ``/completion`` and ``/v1/completions``
      on one server share a fingerprint (the endpoint is not the serving config);
    - the upstream **build** -- a rebuild is not a different serving config; the
      build is recorded separately as ``upstream_build``.

    Stable for identical setups; changes on any capacity, model, or
    ``upstream_config_id`` change. The version is folded into the hash so v1 and
    v2 values never alias. ``model`` comes from the response chunks; ``None`` ->
    config-only identity (still stable).
    """
    payload = {
        "v": _FINGERPRINT_VERSION,
        "slots": cfg.slots,
        "max_inflight": cfg.max_inflight,
        "cache_ram_mb": cfg.cache_ram_mb,
        "upstream": f"{cfg.upstream_host}:{cfg.upstream_port}",
        "model": model,
        "upstream_config_id": cfg.upstream_config_id,
    }
    # Only a non-default upstream kind is folded in, so every existing llama.cpp
    # fingerprint keeps its value.
    upstream_kind = getattr(cfg, "upstream_kind", "llamacpp")
    if upstream_kind != "llamacpp":
        payload["upstream_kind"] = upstream_kind
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def _serving_id(cfg: Config, model_id: str | None) -> str | None:
    """Identity a prefix-index entry must share to be restorable (model + config).

    ``None`` when the upstream model is unknown -- prefix restore then stays off
    for that turn rather than risk restoring another model's KV.

    Deliberately narrower than :func:`_config_fingerprint`: KV compatibility
    depends on the model and the upstream serving config (captured by the
    operator-set ``upstream_config_id``, e.g. cache type / context size), not on
    proxy-side capacity knobs such as ``slots`` or ``cache_ram_mb``. Bump
    ``upstream_config_id`` whenever the upstream KV layout changes.
    """
    if not model_id:
        return None
    blob = json.dumps(
        {"model": model_id, "upstream_config_id": cfg.upstream_config_id}, sort_keys=True
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def _tools_id(obj: dict[str, Any]) -> str:
    """Identity of a request's tool set (rendered ahead of the system text)."""
    blob = json.dumps(obj.get("tools") or [], sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def _choose_slot(
    slot_busy: dict[int, bool], slot_last: dict[int, str | None], n: int, prefix: str
) -> int:
    """Pure pooled slot selection (v8): index of the slot to use for ``prefix``.

    1. a free slot already holding ``prefix`` (warm follow-up -> no restore);
    2. else the first free slot;
    3. else 0 (fallback; only reached if live turns exceed ``n`` -- a misconfig,
       since admission caps concurrency at max_inflight <= slots).
    Does NOT mutate; the caller marks the returned slot busy under the lock.
    """
    for s in range(n):
        if not slot_busy.get(s, False) and slot_last.get(s) == prefix:
            return s
    for s in range(n):
        if not slot_busy.get(s, False):
            return s
    return 0


class _ProxyHTTPServer(ThreadingHTTPServer):
    """A ``ThreadingHTTPServer`` that carries the wired-up components for its Handler."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        addr: tuple[str, int],
        handler_cls: type[BaseHTTPRequestHandler],
        *,
        cfg: Config,
        classifier: Classifier,
        kv: KVStore,
        sched: Scheduler,
        pool: CacheRamPool,
        state: ProxyState,
        metrics: MetricsWriter,
        admin: SlotAdmin,
        reload_detector: ReloadDetector,
    ) -> None:
        """Bind the socket (via the base class) and stash every collaborator."""
        super().__init__(addr, handler_cls)
        self.cfg = cfg
        self.classifier = classifier
        self.kv = kv
        self.sched = sched
        self.pool = pool
        self.state = state
        self.metrics = metrics
        self.admin = admin
        self.reload_detector = reload_detector
        # v7 whole-box energy poller (ESPHome web_server counter); disabled
        # when cfg.energy_url is None. Built here so it isn't threaded through
        # every constructor -- it holds only its own last-counter state.
        self.energy = EnergyPoller(
            cfg.energy_url, cfg.energy_poll_s, cfg.energy_timeout_s, cfg.energy_metric
        )
        # v7+ background memory poller (local sysfs/proc; always on -- no
        # external dependency). Feeds per-turn start/avg/max/end residency.
        self.mem_poller = MemPoller(cfg.mem_poll_s)
        self.ready = False
        # Private per-prefix token-count cache used only for pool eviction
        # bookkeeping (see Handler._post_relay_tracking); not part of the
        # contracted ProxyState surface, guarded by state.lock.
        self.prefix_tokens: dict[str, int] = {}


class Handler(BaseHTTPRequestHandler):
    """Request handler implementing the chat pipeline, stats/health/ready, and passthrough."""

    protocol_version = "HTTP/1.1"
    server: _ProxyHTTPServer  # narrows the inherited `server` attribute's type for readers

    def log_message(self, fmt: str, *args: Any) -> None:
        """Route BaseHTTPRequestHandler's access logging through stdlib logging."""
        logger.debug("%s - %s", self.address_string(), fmt % args)

    def do_GET(self) -> None:
        """Dispatch a GET request."""
        self._dispatch()

    def do_POST(self) -> None:
        """Dispatch a POST request."""
        self._dispatch()

    def do_DELETE(self) -> None:
        """Dispatch a DELETE request (llama.cpp exposes some admin bits over DELETE)."""
        self._dispatch()

    def do_PUT(self) -> None:
        """Dispatch a PUT request."""
        self._dispatch()

    def do_PATCH(self) -> None:
        """Dispatch a PATCH request."""
        self._dispatch()

    # -- routing -----------------------------------------------------------------

    def _dispatch(self) -> None:
        """Route to the matching endpoint handler; unknown paths fall through to passthrough."""
        try:
            if self.path.startswith("/_stats"):
                self._handle_stats()
            elif self.path.startswith("/health"):
                self._handle_health()
            elif self.path.startswith("/ready"):
                self._handle_ready()
            elif self.command == "POST" and any(
                self.path.endswith(sfx) for sfx in _INFERENCE_SUFFIXES
            ):
                self._handle_chat()
            else:
                self._handle_passthrough()
        except (BrokenPipeError, ConnectionResetError):
            logger.debug("client disconnected mid-request")
        except Exception:
            logger.exception("unhandled error dispatching %s %s", self.command, self.path)
            # Fail closed: a handler that raised before sending anything (e.g. a
            # malformed request that trips classification, or a scheduler error)
            # must not leave the client hanging. _send_json swallows the
            # already-responded case, so a double-send is harmless.
            self._send_json(500, {"error": "internal", "reason": "unhandled_error"})

    def _handle_stats(self) -> None:
        """``GET /_stats`` -> the full telemetry document."""
        srv = self.server
        data = build_stats(srv.cfg, srv.state, srv.pool, srv.kv, srv.sched)
        self._send_json(200, data)

    def _handle_health(self) -> None:
        """``GET /health`` -> 200 if upstream is reachable, else 503 degraded (fail-safe signal)."""
        srv = self.server
        if srv.admin.upstream_alive():
            self._send_json(200, {"status": "ok", "local_available": True})
        else:
            self._send_json(503, {"status": "degraded", "local_available": False})

    def _handle_ready(self) -> None:
        """``GET /ready`` -> 200 once the server is actively serving requests."""
        self._send_json(200, {"status": "ready" if self.server.ready else "starting"})

    def _handle_passthrough(self) -> None:
        """Transparent proxy for every path that isn't the chat-completions pipeline."""
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        self._proxy_raw(body)

    # -- the chat pipeline ---------------------------------------------------------

    def _handle_chat(self) -> None:
        """An inference turn (chat OR native ``/completion``) -> the full
        pin+instrument pipeline (see module docstring). Handles any path in
        :data:`_INFERENCE_SUFFIXES`; the only chat-specific step (forcing
        ``stream_options.include_usage``) is guarded on the path below."""
        srv = self.server
        cfg, classifier = srv.cfg, srv.classifier
        kv, sched, state = srv.kv, srv.sched, srv.state

        length = int(self.headers.get("Content-Length") or 0)
        raw_body = self.rfile.read(length) if length else b""
        t0 = time.time()
        # v7: snapshot the whole-box energy counter at turn START (in-memory,
        # non-blocking). Paired with the end snapshot in step 9 to isolate this
        # turn's inference energy from inter-turn idle energy.
        energy_start = srv.energy.snapshot()
        # Relay outcome flags (see _forward_and_relay) -- reset per request so a
        # previous keep-alive request's values can't leak into this turn.
        self._upstream_status = 0
        self._request_sent = False
        self._upstream_finished = False
        self._relay_failure = None
        self._stall = None

        if not raw_body:
            self._proxy_raw(raw_body)
            return
        try:
            obj = json.loads(raw_body)
        except Exception:
            logger.warning("chat body is not valid JSON; forwarding untouched (no pin/tracking)")
            self._proxy_raw(raw_body)
            return
        body_text = raw_body.decode("utf-8", "ignore")

        # step 1: classify (read-only; classify() must never mutate shared state -- invariant 6)
        cls: Classification = classifier.classify(obj, body_text)

        # step 2: force usage on streaming CHAT responses. stream_options is an
        # OpenAI chat concept; native /completion emits its `timings` block on
        # its own and ignores stream_options, so scope this to the chat endpoint.
        stream_chat = self.path.endswith("/v1/chat/completions") and obj.get("stream") is True
        if stream_chat:
            stream_opts = obj.get("stream_options") or {}
            stream_opts["include_usage"] = True
            obj["stream_options"] = stream_opts

        # step 4: block for admission (concurrency cap = max_inflight)
        ticket = sched.acquire(cls.tier, self._client_disconnected)
        if ticket is None:
            with state.lock:
                state.counters["shed"] = state.counters.get("shed", 0) + 1
            self._send_json(503, {"error": "shed", "reason": "queue_timeout_or_client_gone"})
            self._write_turn_failed(cls, None, t0, "shed")
            return

        # v7: live-turn count at admission (includes this turn) -- the divisor for
        # attributing shared whole-box energy under concurrency.
        concurrency = sched.inflight()

        slot: int | None = None
        tail = bytearray()
        upstream_ok = False
        failure: str | None = None
        served_2xx = False
        restore_attempted = False
        prev_prefix: str | None = None
        try:
            # step 4b (v8): POOLED slot selection, post-admission. Prefer a free
            # slot already holding this prefix (warm follow-up -> no restore); else
            # any free slot. Marks it busy so no concurrent turn can take it, then
            # pins id_slot. (slots=1 -> always slot 0 = today's single-slot path.)
            # Gufo assigns its own sessions (and ignores id_slot): the slot is
            # then only slotpin's admission bookkeeping, never sent upstream.
            slot = self._select_slot(cls.prefix_hash)
            if cfg.upstream_kind != "gufo":
                obj["id_slot"] = slot

            # step 3: decide restore, READ-ONLY, for the SELECTED slot (invariants
            # 1 & 2). kv_persist is the master gate for all disk-warmth activity.
            # A warm follow-up on the same slot is not cold (no restore); a fresh
            # slot is (restore the prefix's snapshot into it).
            if cfg.kv_persist and cls.interactive:
                cold = kv.is_cold_interactive(slot, cls.prefix_hash)
                exact_restore = cold
            elif cfg.kv_persist and cfg.kv_adaptive:
                cold = kv.is_cold(slot, cls.prefix_hash)
                exact_restore = kv.warm_worthy(cls.prefix_hash, cls.interactive) and cold
            else:
                cold = False
                exact_restore = False

            # step 3b: longest-common-prefix candidate (prefix_index.py). The
            # exact snapshot is keyed on the WHOLE system message, so a changed
            # tail (date, per-run task text) never matches; render this request's
            # prompt and find the snapshot sharing the longest leading chunks.
            # Rendered once here and reused for the step-9 save's sidecar -- and
            # only when something will use it: a cold slot (to find a candidate)
            # or a save that is actually due (to index it). Warm follow-ups
            # inside the save rate limit skip the render entirely.
            prefix_hashes: list[str] | None = None
            serving_id: str | None = None
            head_id: str | None = None
            prefix_candidate: tuple[str, int, int] | None = None
            if (
                cfg.kv_persist
                and cfg.prefix_restore
                and (cls.interactive or cfg.kv_adaptive)
                and isinstance(obj.get("messages"), list)
            ):
                # Identities are cheap (cached model id + tools hash): compute
                # them for every eligible turn, so a save that passes its rate
                # limit at turn END still knows whether an existing index
                # remains valid even when the render below was skipped.
                serving_id = _serving_id(cfg, srv.reload_detector.current_model_id())
                head_id = _tools_id(obj)
            if serving_id is not None and (cold or kv.save_due(cls.prefix_hash)):
                rendered = srv.admin.apply_template(obj)
                if rendered:
                    prefix_hashes = chunk_hashes(rendered, cfg.prefix_chunk_chars)
                    if cold:
                        match = kv.index.best_match(prefix_hashes, serving_id, exclude=cls.prefix_hash)
                        if match is not None:
                            match_chars = match[1] * cfg.prefix_chunk_chars
                            if match_chars >= cfg.prefix_restore_min_chars:
                                prefix_candidate = (match[0], match_chars, len(rendered))
                if prefix_candidate is not None:
                    # The poll-based model id can lag an in-place model swap;
                    # confirm against a fresh /props before restoring foreign KV.
                    fresh_id = _serving_id(cfg, srv.reload_detector.fresh_model_id())
                    if fresh_id is None:
                        # Couldn't re-check (upstream busy/unreachable): the model
                        # may have changed, so neither restore nor index this
                        # turn under a possibly stale identity. (This turn's own
                        # ph is almost always new, so there's no index to lose.)
                        logger.warning(
                            "prefix-restore: fresh /props check failed; skipping candidate %s",
                            prefix_candidate[0],
                        )
                        prefix_candidate = None
                        prefix_hashes = None
                        serving_id = None
                    elif fresh_id != serving_id:
                        logger.warning(
                            "prefix-restore: upstream model changed since last poll; "
                            "skipping candidate %s",
                            prefix_candidate[0],
                        )
                        prefix_candidate = None
                        prefix_hashes = None
                        serving_id = fresh_id
            deferred_restore = exact_restore or prefix_candidate is not None

            # step 5: write slot_last for the selected slot (post-admission)
            with state.lock:
                prev_prefix = state.slot_last.get(slot)
                state.slot_last[slot] = cls.prefix_hash

            # step 6: deferred restore -- log unconditionally, THEN execute (invariant 3)
            restore_attempted = bool(deferred_restore)
            if deferred_restore:
                logger.warning(
                    "kv-deferred: executing restore slot=%s prefix=%s lane=%s",
                    slot,
                    cls.prefix_hash,
                    cls.lane,
                )
                latest_alias = cfg.latest_alias if cls.interactive else None
                restored_ok = kv.restore_blocking(
                    slot,
                    cls.prefix_hash,
                    latest_alias,
                    try_exact=exact_restore,
                    prefix_candidate=prefix_candidate,
                )
                if prefix_candidate is not None and not restored_ok:
                    # Only a restore that landed counts toward "warm"; the
                    # exact-path attempt semantics are
                    # unchanged when no prefix candidate was involved.
                    deferred_restore = False

            body = json.dumps(obj).encode("utf-8")

            # step 7: forward + relay
            try:
                tail = self._forward_and_relay(body, stream_chat=stream_chat)
                upstream_ok = True
            except _UpstreamUnreachable as exc:
                logger.warning("upstream unreachable for chat completion: %s", exc)
                failure = "timeout" if isinstance(exc.__cause__, TimeoutError) else "unreachable"
                self._send_json(502, {"error": "upstream_unreachable", "local_available": False})
            except _UpstreamStalled:
                # Nothing was sent to the client: a clean error it can fall back on.
                failure = "stalled"
                self._send_json(
                    504,
                    {
                        "error": "upstream_stalled",
                        "local_available": False,
                        "stalled_s": round(self._stall[1], 1),
                    },
                )
                # The post-abort slot hold below can last minutes: close now so
                # a keep-alive client's next request does not sit behind it.
                self.close_connection = True
                try:
                    self.connection.shutdown(socket.SHUT_WR)
                except OSError:
                    pass
            if self._stall is not None:
                # Token counters only -- (prompt tokens processed, tokens decoded)
                # per busy upstream slot, at the last two probes.
                logger.warning(
                    "upstream stalled: slot=%s prefix=%s phase=%s first output, "
                    "%.1f s without output; token counters %s; upstream aborted",
                    slot,
                    cls.prefix_hash,
                    self._stall[0],
                    self._stall[1],
                    " then ".join(str(p) for p in self._stall[2]) or "not read",
                )

            # step 9: post-relay tracking + SYNCHRONOUS save -- run while this turn
            # still OWNS the slot (busy), so the save captures this turn's KV
            # before the slot can be reused. ONLY for a real 2xx generation: a
            # 4xx/5xx produced no KV, so tracking it would pollute metrics and
            # (via slot_last) wrongly mark the slot warm. Best-effort.
            # A relay that ended early (client disconnected, upstream aborted or
            # timed out mid-stream) is NOT a served turn even under a 200 status
            # line: the slot holds a partial, possibly still-growing KV, and
            # saving it would overwrite a good snapshot (seen in production:
            # killed fan-out workers' aborted turns were saved over their
            # turn-1 caches).
            served_2xx = (
                upstream_ok and self._upstream_finished and 200 <= self._upstream_status < 300
            )
            if upstream_ok and not self._upstream_finished:
                logger.info(
                    "turn not tracked/saved: relay incomplete (slot=%s prefix=%s)",
                    slot,
                    cls.prefix_hash,
                )
            if not served_2xx:
                self._write_turn_failed(cls, slot, t0, failure or self._relay_failure)
            if served_2xx:
                try:
                    self._post_relay_tracking(
                        cls, ticket, prev_prefix, bytes(tail), t0, deferred_restore, energy_start,
                        concurrency, slot, prefix_hashes=prefix_hashes, serving_id=serving_id,
                        head_id=head_id,
                    )
                except Exception:
                    logger.exception("post-relay tracking failed")
        finally:
            # An aborted/incomplete turn that reached the upstream may still be
            # running there: llama-server only notices a closed connection on its
            # next write, so a killed mid-prefill request keeps the slot busy for
            # minutes (seen: ~2 min). Keep the slot reserved until the upstream
            # reports it idle, so the next turn isn't pinned to a busy slot.
            if slot is not None and self._request_sent and not self._upstream_finished:
                self._await_upstream_slot_idle(slot)
            # step 8: free the slot, THEN release admission -- coupled so a newly
            # admitted turn always finds a free slot; never leak either.
            if slot is not None:
                with state.lock:
                    # Undo the optimistic step-5 slot_last claim on a failed turn.
                    # If the upstream processed any of this turn, or a restore
                    # replaced the slot's KV, its contents are now unknown: record
                    # None (next turn restores) rather than prev_prefix, which
                    # would wrongly skip a needed restore. Only an upstream that
                    # was never reached leaves the slot untouched.
                    if not served_2xx:
                        # Anything that reached the upstream -- an aborted or
                        # cut-off relay, a response timeout after the request
                        # was sent, most error replies -- may have changed the
                        # slot's KV.
                        # Only 503 (model loading / unavailable) is known to be
                        # returned before any slot work; other errors (500 decode
                        # or KV-space failure mid-prefill, a 400 after a cache
                        # swap) may have changed the slot -- treat as touched.
                        # Cost of being wrong: one extra restore next turn.
                        rejected = self._upstream_finished and self._upstream_status == 503
                        touched = restore_attempted or (self._request_sent and not rejected)
                        state.slot_last[slot] = None if touched else prev_prefix
                    state.slot_busy[slot] = False
            sched.release(ticket)

    def _write_turn_failed(
        self, cls: Classification, slot: int | None, t0: float, reason: str | None
    ) -> None:
        """Emit one ``turn_failed`` metrics record for a turn with no per-turn record.

        ``reason`` is the cause found by the caller (``shed``, ``unreachable``,
        ``timeout``, ``aborted_client``, ``stalled``); ``None`` means the upstream replied but the
        turn is not a served 2xx: ``upstream_4xx`` / ``upstream_5xx`` by status
        class, else ``upstream_incomplete`` (short body, missing terminal
        frame, in-stream error). Never raises; carries no prompt or reply text.
        """
        try:
            status = self._upstream_status
            if reason is None:
                if 200 <= status < 300:
                    reason = "upstream_incomplete"
                else:
                    reason = "upstream_4xx" if 400 <= status < 500 else "upstream_5xx"
            event: dict[str, Any] = {
                "event": "turn_failed",
                "ts": datetime.now().astimezone().isoformat(timespec="seconds"),
                "reason": reason,
                "route": cls.lane,
                "slot": slot,
                "prefix": cls.prefix_hash,
                "dur_s": round(time.time() - t0, 3),
            }
            if status:
                event["upstream_status"] = status
            self.server.metrics.write(event)
        except Exception:
            logger.exception("turn_failed record failed")

    def _await_upstream_slot_idle(self, slot: int) -> None:
        """Block (bounded) until llama-server reports ``slot`` idle. Best-effort:
        gives up if ``/slots`` is unavailable or after the upstream timeout."""
        admin, state = self.server.admin, self.server.state
        deadline = time.monotonic() + min(self.server.cfg.upstream_timeout_s, 600.0)
        waited = False
        while time.monotonic() < deadline:
            # Gufo reports only a box-wide in-flight count: discount the other
            # turns this proxy has live (llama.cpp ignores this, it's per slot).
            with state.lock:
                others = sum(1 for s, b in state.slot_busy.items() if b and s != slot)
            busy = admin.slot_processing(slot, others)
            if not busy:  # False (idle) or None (can't tell): stop waiting
                break
            waited = True
            time.sleep(1.0)
        if waited:
            logger.info("slot %s: held until upstream finished the aborted task", slot)

    def _select_slot(self, prefix: str) -> int:
        """Pooled slot selection (v8): pick + mark-busy a slot for this turn.

        Wraps the pure :func:`_choose_slot` under the state lock. Admission
        (max_inflight) caps live turns at <= slots, so a free slot normally
        always exists; the fallback only guards a misconfiguration.
        """
        srv = self.server
        state = srv.state
        n = max(1, srv.cfg.slots)
        with state.lock:
            s = _choose_slot(state.slot_busy, state.slot_last, n, prefix)
            if state.slot_busy.get(s, False):
                logger.warning("no free slot (slots=%d, max_inflight too high?) -- reusing %d", n, s)
            state.slot_busy[s] = True
            return s

    def _post_relay_tracking(
        self,
        cls: Classification,
        ticket: SchedTicket,
        prev_prefix: str | None,
        tail: bytes,
        t0: float,
        did_restore: bool,
        energy_start: dict[str, float | None] | None,
        concurrency: int,
        slot: int,
        *,
        prefix_hashes: list[str] | None = None,
        serving_id: str | None = None,
        head_id: str | None = None,
    ) -> None:
        """Step 9: parse usage/timings, update counters/pool/ring, emit metrics, save.

        Runs while the turn still owns ``slot`` (busy), so the synchronous save
        (:meth:`KVStore.save_now`) captures this turn's KV before the slot frees.
        """
        srv = self.server
        state, pool, metrics, kv, cfg = srv.state, srv.pool, srv.metrics, srv.kv, srv.cfg

        usage = _parse_usage_tail(tail)
        cache_n = usage.get("cache_n") or 0
        prompt_n = usage.get("prompt_n") or 0
        completion_n = usage.get("completion_n")
        decode_tps = usage.get("decode_tps")
        prefill_tps = usage.get("prefill_tps")
        predicted_n = usage.get("predicted_n")
        draft_n = usage.get("draft_n")
        draft_n_accepted = usage.get("draft_n_accepted")
        accept_rate = (
            round(draft_n_accepted / draft_n, 4)
            if draft_n and draft_n_accepted is not None
            else None
        )

        # Gufo reports its own cache outcome; the slot-occupancy / cache-ram
        # pool inference below models llama-server and is skipped for it.
        gufo = cfg.upstream_kind == "gufo"
        gufo_cache = (_parse_gufo_tail(tail) or _GUFO_EMPTY) if gufo else None
        occupant_changed = (
            not gufo and prev_prefix is not None and prev_prefix != cls.prefix_hash
        )
        baseline = _INTERACTIVE_COLD_BASELINE if cls.interactive else _DEFAULT_COLD_BASELINE

        restored_tokens = 0
        cacheram_restore = False
        with state.lock:
            state.fp_info.setdefault(cls.fp, {})
            state.fp_info[cls.fp]["label"] = cls.label
            state.fp_info[cls.fp]["tokens"] = prompt_n
            state.fp_info[cls.fp]["last_seen"] = time.time()
            state.counters["turns"] = state.counters.get("turns", 0) + 1

            prev_tokens = srv.prefix_tokens.get(prev_prefix, 0) if prev_prefix else 0
            srv.prefix_tokens[cls.prefix_hash] = prompt_n

            if occupant_changed:
                state.counters["evictions"] = state.counters.get("evictions", 0) + 1
                pool.note_eviction(prev_prefix, prev_tokens)
                if cache_n > baseline:
                    restored_tokens = cache_n - baseline
                    cacheram_restore = True
                    state.counters["restores"] = state.counters.get("restores", 0) + 1
                    state.counters["restored_tokens"] = (
                        state.counters.get("restored_tokens", 0) + restored_tokens
                    )
                    pool.mark_restored(cls.prefix_hash)

            # Warm iff EITHER a disk KV restore fired this turn (did_restore, step 6) OR
            # the cache-ram/L2 pool covered the prefix (cacheram_restore, just
            # above) -- i.e. warm means "this turn rode on some form of
            # preserved KV state", cold means "the prompt had to be
            # (re-)prefilled from scratch, the pain disk-warmth/MTP exist to
            # eliminate".
            served = "warm" if (cacheram_restore or did_restore) else "cold"
            if gufo_cache is not None:
                served = "warm" if gufo_cache["upstream_cache"] in ("memory", "disk") else "cold"

            state.recent.append(
                {
                    "ts": time.time(),
                    "route": cls.lane,
                    "slot": slot,
                    "workload": cls.label,
                    "tier": cls.tier,
                    "interactive": cls.interactive,
                    "decode_tps": decode_tps,
                    "served": served,
                    "queue_wait_ms": ticket.queue_wait_ms,
                }
            )

        # v7 metric layer additions: TTFT (from request arrival t0 to the first
        # relayed chunk carrying output -- for streams, the first text, reasoning
        # or tool-call delta; includes queue wait + any restore + prefill, i.e.
        # user-visible time-to-first-token), an optional bench/experiment run
        # id (per-request header wins over the config default), and a
        # point-in-time GPU/host memory sample. All fail-open: ttft_s/run_id
        # may be None, and sample_mem() never raises.
        first_chunk_t = getattr(self, "_first_chunk_t", None)
        ttft_s = round(first_chunk_t - t0, 3) if first_chunk_t is not None else None
        run_id = self.headers.get("X-Slotpin-Run-Id") or cfg.run_id
        # Per-request id (distinct from run_id, which groups a whole bench): a
        # pass-through of the caller's X-Slotpin-Request-Id header, so each row
        # is individually addressable by the exact-metrics helper.
        request_id = self.headers.get("X-Slotpin-Request-Id")
        # Immutable identity of the serving config (proxy capacity + upstream
        # model/build) for this turn -- see _config_fingerprint.
        config_fingerprint = _config_fingerprint(cfg, usage.get("model"))
        # Upstream build (system fingerprint) is provenance, recorded SEPARATELY
        # -- not folded into the config identity. Null-safe: present on chat
        # responses, usually absent on native /completion.
        upstream_build = usage.get("system_fingerprint")
        # Why generation stopped + whether the OUTPUT was cut off at the
        # generation budget (n_predict/max_tokens). `truncated` lets a benchmark harness
        # reject a budget-clipped turn as incomplete instead of scoring a chopped
        # answer -- NOT llama.cpp's prompt-side `truncated`, which we don't record.
        stop_reason = usage.get("finish_reason") or usage.get("stop_type")
        truncated = stop_reason in ("length", "limit")
        # Per-turn memory over the [t0, now] window from the background poller:
        # start/avg/max GTT + start/avg/min available RAM, plus the latest as the
        # end (backward-compatible gtt_used_mb / mem_available_mb keys). Captures
        # the intra-turn peak (e.g. a large-context prefill spike) a single
        # end-of-turn sample_mem() would miss.
        mem = srv.mem_poller.turn_stats(t0, time.time())
        # v7: end-of-turn energy snapshot (in-memory) paired with the start
        # snapshot -> this turn's inference energy vs inter-turn idle energy.
        energy = srv.energy.turn_metrics(energy_start, srv.energy.snapshot())

        # v8: concurrency-fair energy. The whole-box counter can't tell
        # concurrent turns apart, so each turn's energy_turn_wh (its window
        # delta) counts ALL box energy in that window -- an N-fold overcount
        # under load. Divide by the live-turn count to get this turn's fair
        # share. `concurrency` is averaged over the turn (admission count +
        # end count) / 2, a first-order stand-in for the time-weighted average
        # (the exact per-tick attribution is the poller-engine item in
        # docs/concurrency-design.md). Sequential turns (N=1) -> fair == turn.
        conc_end = srv.sched.inflight()
        conc_avg = round((concurrency + conc_end) / 2.0, 2)
        _turn_wh = energy["energy_turn_wh"]
        energy_fair_wh = round(_turn_wh / max(1.0, conc_avg), 4) if _turn_wh is not None else None

        metrics.write(
            {
                "ts": datetime.now().astimezone().isoformat(timespec="seconds"),
                "route": cls.lane,
                "slot": slot,
                "workload": cls.label,
                "run_id": run_id,
                "request_id": request_id,
                "config_fingerprint": config_fingerprint,
                "fingerprint_version": _FINGERPRINT_VERSION,
                "upstream_build": upstream_build,
                "fp": cls.fp,
                "prefix": cls.prefix_hash,
                "cache_n": cache_n,
                "prompt_n": prompt_n,
                "completion_n": completion_n,
                "dur_s": round(time.time() - t0, 3),
                "ttft_s": ttft_s,
                "cacheram_restore": cacheram_restore,
                "restored_tokens": restored_tokens,
                "decode_tps": decode_tps,
                "prefill_tps": prefill_tps,
                "predicted_n": predicted_n,
                "draft_n": draft_n,
                "draft_n_accepted": draft_n_accepted,
                "accept_rate": accept_rate,
                "stop_reason": stop_reason,
                "truncated": truncated,
                # Canonical request-success signal. This block runs only when
                # served_2xx (see _handle_chat step 9), and a per-turn record is
                # written iff the request was served with a 2xx -- so `ok` is
                # always True in a written record today, and record-existence is
                # itself the success signal. It is emitted explicitly (matching
                # the save/restore record's `ok`) so consumers gate on `ok`, not
                # on `served` (which is KV warmth: "warm"|"cold", NOT success).
                "ok": True,
                "served": served,
                "queue_wait_ms": ticket.queue_wait_ms,
                "tier": cls.tier,
                "concurrency": conc_avg,
                "gtt_used_mb": mem["gtt_used_mb"],
                "gtt_used_start_mb": mem["gtt_used_start_mb"],
                "gtt_used_avg_mb": mem["gtt_used_avg_mb"],
                "gtt_used_max_mb": mem["gtt_used_max_mb"],
                "vram_used_mb": mem["vram_used_mb"],
                "mem_available_mb": mem["mem_available_mb"],
                "mem_available_start_mb": mem["mem_available_start_mb"],
                "mem_available_avg_mb": mem["mem_available_avg_mb"],
                "mem_available_min_mb": mem["mem_available_min_mb"],
                "soc_power_w": mem["soc_power_w"],
                "plug_power_w": energy["plug_power_w"],
                "energy_kwh": energy["energy_kwh"],
                "energy_turn_wh": energy["energy_turn_wh"],
                "energy_fair_wh": energy_fair_wh,
                "energy_idle_wh": energy["energy_idle_wh"],
                # Gufo-only keys (absent for llama.cpp, whose lines are unchanged).
                **(gufo_cache or {}),
            }
        )

        # Same kv_persist master gate as step 3: saves require `kv_persist and ...`
        # (both the adaptive-on and interactive-only/adaptive-off cases), so
        # saves never fire with kv_persist=False even if kv_adaptive is on.
        if cfg.kv_persist and (cfg.kv_adaptive or cls.interactive):
            # Record this turn's occurrence exactly once (warm_worthy is now a
            # pure query; recurrence used to be double-counted via warm_worthy on
            # both the decision and save paths).
            kv.record_prefix(cls.prefix_hash)
            if kv.warm_worthy(cls.prefix_hash, cls.interactive):
                alias = cfg.latest_alias if cls.interactive else None
                # Synchronous save while we still own the slot (v8): guarantees
                # the snapshot captures THIS turn's KV before the slot is freed.
                kv.save_now(
                    slot,
                    cls.prefix_hash,
                    alias,
                    prefix_hashes=prefix_hashes,
                    serving_id=serving_id,
                    head_id=head_id,
                )

    # -- shared relay/forward machinery ---------------------------------------------

    def _forward_and_relay(self, body: bytes, stream_chat: bool = False) -> bytearray:
        """Forward ``body`` upstream and relay the response back, chunked, to the client.

        Returns the last <=16 KiB of the response body (the "tail") so the
        caller can recover trailing usage/timings JSON. Raises
        ``_UpstreamUnreachable`` if the upstream connect/request itself fails
        (contract step 10 -- the caller turns this into a 502).

        ``stream_chat`` (a ``stream: true`` chat-completions request) enables the
        two fail-fast options, ``upstream_stall_timeout_s`` and
        ``hold_headers_until_first_output``; both are off by default and never
        apply to any other request. Raises ``_UpstreamStalled`` on a stall with
        nothing yet sent to the client (the caller sends a 504).
        """
        cfg = self.server.cfg
        # Per-call relay outcome (read by _handle_chat; reset here AND at the top of
        # _handle_chat since the handler instance is reused across keep-alive requests):
        #   _upstream_status   response status line (0 = never got one)
        #   _request_sent      the request reached the upstream (slot may be touched)
        #   _upstream_finished the upstream response was read to its true end --
        #                      not cut short by a disconnect, abort or short body
        #   _relay_failure     "aborted_client"/"timeout"/"stalled" when the relay ended
        #                      early for that cause (None = unknown; see _write_turn_failed)
        #   _stall             (phase, seconds, last /slots readings) when the stall
        #                      limit fired, else None
        self._upstream_status = 0
        self._request_sent = False
        self._upstream_finished = False
        self._relay_failure = None
        self._stall = None
        # v7 metric layer: timestamp the first non-empty relayed chunk so the
        # caller can derive TTFT. Reset per call (keep-alive handler reuse).
        self._first_chunk_t = None
        tail = bytearray()
        conn = http_client.HTTPConnection(
            cfg.upstream_host, cfg.upstream_port, timeout=cfg.upstream_timeout_s
        )
        done_event = threading.Event()
        aborted = threading.Event()
        # Serialises the watcher's abort with our own teardown, so it can never
        # shut down a socket (fd) that has already been closed and reused.
        teardown = threading.Lock()
        # Stall limit (0 = off; see _start_stall_watchdog). `watch` is None
        # whenever the limit does not apply to this request.
        stall_s = cfg.upstream_stall_timeout_s if stream_chat else 0.0
        watch: _StallWatch | None = None
        up_sock: socket.socket | None = None

        def _stalled() -> None:
            phase = "before" if self._first_chunk_t is None else "after"
            self._stall = (phase, watch.stalled_s, watch.probes)
            self._relay_failure = "stalled"

        try:
            try:
                fwd_headers = dict(_filtered_headers(self.headers.items()))
                fwd_headers["Content-Length"] = str(len(body))
                conn.request(self.command, self.path, body=body, headers=fwd_headers)
            except Exception as exc:
                raise _UpstreamUnreachable(str(exc)) from exc
            self._request_sent = True
            # Kept only for the fail-fast options: http.client drops conn.sock
            # once a close-delimited reply starts, and a stall abort or an abort
            # during a held (silent to the client) prefill needs the socket.
            if stall_s or (stream_chat and cfg.hold_headers_until_first_output):
                up_sock = conn.sock
            if stall_s:
                watch = _StallWatch(stall_s)
                _start_stall_watchdog(watch, self.server.admin, conn, up_sock, aborted, teardown)
            # Watch from the moment the request is sent: llama-server withholds
            # streaming headers until prefill completes, so a client that dies
            # mid-prefill must be caught while we are still in getresponse().
            _start_disconnect_watcher(
                self.connection, conn, done_event, aborted, teardown, up_sock
            )
            try:
                resp = conn.getresponse()
            except Exception as exc:
                if watch is not None and watch.fired.is_set():
                    _stalled()
                    raise _UpstreamStalled(str(exc)) from exc
                if aborted.is_set():
                    logger.info("relay ended early: client disconnected before the upstream responded")
                    self.close_connection = True
                    self._relay_failure = "aborted_client"
                    return tail  # _upstream_finished stays False -> not tracked/saved
                raise _UpstreamUnreachable(str(exc)) from exc
            self._upstream_status = resp.status

            is_chat = self.path.endswith("/v1/chat/completions")
            is_sse = "text/event-stream" in (resp.getheader("Content-Type") or "")
            if watch is not None and not (is_sse and 200 <= resp.status < 300):
                # Not a stream after all (e.g. a JSON error reply): no limit.
                with teardown:
                    watch.stop.set()
                if not watch.fired.is_set():
                    watch = None
            if resp.status == 400 and is_chat and cfg.capture_400:
                with teardown:  # stop the watcher: the helper closes conn itself
                    done_event.set()
                self._relay_captured_400(body, resp, conn)
                self._upstream_finished = True  # full 400 body read and relayed
                return tail

            # hold_headers_until_first_output: `held` collects what the upstream
            # sent while the client has been sent nothing, not even the status
            # line; None once the headers are out (always, with the option off).
            held: list[bytes] | None = None
            if (
                stream_chat
                and cfg.hold_headers_until_first_output
                and is_sse
                and 200 <= resp.status < 300
            ):
                held = []
            held_n = 0
            scanner = _OutputScanner()
            stall_unsent = False
            to_client = True  # a failure now is on the client side (write), not the upstream's
            try:
                if held is None:
                    self._send_relay_headers(resp)
                while True:
                    # read1, not read: on a chunked stream read(n) blocks until n
                    # bytes arrive, which held SSE tokens back in ~8 KB bursts.
                    to_client = False
                    chunk = resp.read1(8192)
                    if not chunk:
                        break
                    to_client = True
                    output = False
                    if is_sse and (self._first_chunk_t is None or watch is not None):
                        output = scanner.feed(chunk)
                    if self._first_chunk_t is None and (output or not is_sse):
                        self._first_chunk_t = time.time()
                    if output and watch is not None:
                        watch.last_output = time.monotonic()
                    # Into the tail BEFORE the client write: if that write fails
                    # we still know whether the upstream reached its end.
                    tail.extend(chunk)
                    if len(tail) > 16384:
                        del tail[:-16384]
                    if held is not None:
                        held.append(chunk)
                        held_n += len(chunk)
                        if output or held_n > _HOLD_MAX_BYTES:
                            pending, held = held, None
                            self._send_relay_headers(resp)
                            for part in pending:
                                self._relay_chunk(part)
                        continue
                    self._relay_chunk(chunk)
                # http.client's read(amt) returns b"" instead of raising when an
                # upstream that declared Content-Length closes early; treat any
                # undelivered remainder as a truncated relay. (Chunked upstreams
                # already raise IncompleteRead.)
                if resp.length:
                    raise http_client.IncompleteRead(b"", resp.length)
                # Checked BEFORE the abort flag so a client that closes right
                # after [DONE] (the watcher may fire just then) keeps its turn.
                if watch is not None and watch.fired.is_set() and not _upstream_ended(resp, tail):
                    # Watchdog tore the upstream down, as the watcher does below.
                    raise _UpstreamStalled("no output or token progress within the stall limit")
                if aborted.is_set() and not _upstream_ended(resp, tail):
                    # Watcher tore the upstream down: read() returned b"" because
                    # the connection was closed, not because generation finished.
                    raise ConnectionAbortedError("client disconnected; upstream aborted")
                self._mark_upstream_outcome(resp, tail)
                to_client = True
                if held is not None:
                    # The upstream ended without any output event: relay what
                    # it sent, exactly as without the hold.
                    pending, held = held, None
                    self._send_relay_headers(resp)
                    for part in pending:
                        self._relay_chunk(part)
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
            except Exception as exc:
                logger.info("relay ended early: %s", exc)
                stalled = (
                    watch is not None
                    and watch.fired.is_set()
                    and not to_client
                    and not _upstream_ended(resp, tail)
                )
                if stalled:
                    _stalled()
                elif aborted.is_set() or to_client:
                    self._relay_failure = "aborted_client"
                elif isinstance(exc, TimeoutError):
                    self._relay_failure = "timeout"
                if self._stall is not None:
                    # Never a served turn, whatever the tail holds. With the
                    # response started, end it as any truncated relay ends -- but
                    # now, not when the handler returns: the post-abort slot hold
                    # in _handle_chat can outlast the client's patience.
                    stall_unsent = held is not None
                    if not stall_unsent:
                        try:
                            self.connection.shutdown(socket.SHUT_WR)
                        except OSError:
                            pass
                else:
                    # The upstream may still have completed: its terminal frame can
                    # be in the tail even though writing it to a departed client
                    # failed. Its KV is then a clean turn.
                    if _upstream_ended(resp, tail):
                        self._mark_upstream_outcome(resp, tail)
                    if held is not None and not aborted.is_set():
                        # The upstream failed before any output event: the client
                        # gets the status, headers and bytes it would have had
                        # without the hold, then the same truncated ending.
                        try:
                            self._send_relay_headers(resp)
                            for part in held:
                                self._relay_chunk(part)
                        except Exception:
                            logger.debug("failed to flush held response (client gone)")
                # A mid-stream write failure leaves the chunked response truncated
                # (no terminal 0-chunk). Close the connection so a keep-alive client
                # can't read the leftover bytes as the reply to its next request.
                # (Not for a stall with nothing sent: that gets a complete 504.)
                if not stall_unsent:
                    self.close_connection = True
            if stall_unsent:
                raise _UpstreamStalled("no output or token progress within the stall limit")
        finally:
            with teardown:
                done_event.set()
                if watch is not None:
                    watch.stop.set()
                conn.close()
        return tail

    def _send_relay_headers(self, resp: http_client.HTTPResponse) -> None:
        """Start the relayed response: upstream status and end-to-end headers, chunked."""
        self.send_response(resp.status)
        for k, v in resp.getheaders():
            if k.lower() not in _HOP_BY_HOP:
                self.send_header(k, v)
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        _set_downstream_write_timeout(self.connection, self.server.cfg.downstream_write_timeout_s)

    def _relay_chunk(self, chunk: bytes) -> None:
        """Write one upstream read to the client as one chunk."""
        self.wfile.write(f"{len(chunk):x}\r\n".encode("ascii"))
        self.wfile.write(chunk)
        self.wfile.write(b"\r\n")
        self.wfile.flush()

    def _mark_upstream_outcome(self, resp: http_client.HTTPResponse, tail: bytearray) -> None:
        """Record a complete upstream reply as finished -- unless it carried an
        in-stream error frame (llama-server reports mid-stream failures under a
        200 status), whose slot KV is not a clean turn."""
        if _stream_errored(tail):
            logger.warning("upstream reported an in-stream error; turn not tracked/saved")
        else:
            self._upstream_finished = True

    def _relay_captured_400(
        self, body: bytes, resp: http_client.HTTPResponse, conn: http_client.HTTPConnection
    ) -> None:
        """Debug aid dump request+response for a 400 to /tmp, off by default.

        Gated by ``cfg.capture_400`` (default False); production deployments
        should leave this off. Never lets a capture failure prevent the 400
        from reaching the client.
        """
        try:
            response_body = resp.read()
            filename = f"/tmp/slotpin-400-{int(time.time())}.json"
            with open(filename, "wb") as fh:
                fh.write(b"REQUEST:\n" + body + b"\n\nRESPONSE:\n" + response_body)
            logger.warning("400 captured -> %s", filename)
        except Exception:
            logger.exception("400 capture failed")
            response_body = b""
        finally:
            conn.close()
        self.send_response(400)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        try:
            self.wfile.write(response_body)
        except Exception:
            pass

    def _proxy_raw(self, body: bytes) -> None:
        """Forward+relay with no classification/scheduling/tracking (non-chat paths)."""
        try:
            self._forward_and_relay(body)
        except _UpstreamUnreachable:
            self._send_json(502, {"error": "upstream_unreachable", "local_available": False})

    def _client_disconnected(self) -> bool:
        """Cheap non-blocking peek used by the scheduler to poll for client-gone while queued."""
        try:
            readable, _, _ = select.select([self.connection], [], [], 0)
            if readable:
                data = self.connection.recv(1, socket.MSG_PEEK)
                return not data
        except (OSError, ValueError):
            # ValueError: select on a closed socket (fd == -1). Treat as gone.
            return True
        return False

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        """Send a JSON response, swallowing write errors from an already-gone client."""
        body = json.dumps(payload).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception:
            logger.debug("failed to send response to client (likely disconnected)")


class ProxyServer:
    """Top-level slotpin HTTP proxy server -- wires a threading HTTP server to ``Handler``."""

    def __init__(
        self,
        cfg: Config,
        *,
        classifier: Classifier,
        kv: KVStore,
        sched: Scheduler,
        pool: CacheRamPool,
        state: ProxyState,
        metrics: MetricsWriter,
        admin: SlotAdmin,
        reload_detector: ReloadDetector,
    ) -> None:
        """Bind the listening socket and stash every collaborator for the Handler to use."""
        self._cfg = cfg
        self._httpd = _ProxyHTTPServer(
            (cfg.listen_host, cfg.listen_port),
            Handler,
            cfg=cfg,
            classifier=classifier,
            kv=kv,
            sched=sched,
            pool=pool,
            state=state,
            metrics=metrics,
            admin=admin,
            reload_detector=reload_detector,
        )

    @property
    def port(self) -> int:
        """The actually-bound listen port (differs from cfg.listen_port when that is 0)."""
        return int(self._httpd.server_address[1])

    def serve_forever(self) -> None:
        """Mark the server ready and block, serving requests until :meth:`shutdown`."""
        self._httpd.ready = True
        logger.info("slotpin proxy listening on %s:%d", self._cfg.listen_host, self.port)
        self._httpd.serve_forever()

    def shutdown(self) -> None:
        """Stop serving, stop the energy + memory pollers, and release the socket.

        Safe to call from another thread."""
        self._httpd.energy.stop()
        self._httpd.mem_poller.stop()
        self._httpd.shutdown()
        self._httpd.server_close()
