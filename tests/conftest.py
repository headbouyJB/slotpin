"""Shared pytest fixtures: a fast-tuned Config factory and a fake llama-swap/
llama-server double that serves chat completions, slot admin actions,
/props and /health on one ephemeral-port ThreadingHTTPServer.

Kept dependency-free (stdlib + pytest only), per the "stdlib only"
constraint and the "no network beyond localhost ephemeral
ports" rule.
"""

from __future__ import annotations

import itertools
import json
import re
import select
import socket
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from slotpin.config import Config, load_config

# ---------------------------------------------------------------------------
# make_config
# ---------------------------------------------------------------------------

_counter = itertools.count()

# Tuned so no test needs to sleep more than ~0.2s waiting on a configured
# interval; individual tests override any of these via **overrides.
_FAST_DEFAULTS: dict[str, Any] = {
    "listen_host": "127.0.0.1",
    "listen_port": 0,
    "admin_timeout_s": 2.0,
    "upstream_timeout_s": 5.0,
    "downstream_write_timeout_s": 5.0,
    "queue_timeout_s": 1.0,
    "reload_poll_s": 0.05,
    "prefix_save_min_interval_s": 0.05,
    "snapshot_ttl_h": 0.02,  # ~72s -- small but positive; tests needing tighter
    # TTLs override this explicitly.
    "metrics_rotate_mb": 1.0,
    "metrics_keep": 3,
    "cache_ram_mb": 64,
    "bytes_per_token": 100,
    "cram_safety": 0.9,
    "recur_threshold": 3,
}


def _toml_scalar(value: Any) -> str:
    """Render one Python value as a TOML scalar literal."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return '"off"'
    if isinstance(value, Path):
        return json.dumps(str(value))
    if isinstance(value, (list, tuple)):
        return json.dumps(list(value))
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, int):
        return str(value)
    return json.dumps(str(value))


def _render_table(header: str, fields: dict) -> str:
    lines = [header]
    for k, v in fields.items():
        if v is None:
            continue
        lines.append(f"{k} = {_toml_scalar(v)}")
    return "\n".join(lines)


@pytest.fixture
def make_config(tmp_path):
    """Factory fixture: ``make_config(**overrides) -> Config``.

    Builds a real TOML file under ``tmp_path`` (defaults tuned for speed:
    ephemeral listen_port, tiny intervals, tmp slot_save_path/metrics_file)
    and loads it through the real :func:`slotpin.config.load_config`, so
    every test exercises the real TOML/validation path. ``lanes``,
    ``demote``, and ``labels`` overrides take lists of plain dicts (rendered
    as ``[[classify.lanes]]`` etc. array-of-tables); every other override is
    a top-level scalar.
    """

    def _make(**overrides: Any) -> Config:
        merged = dict(_FAST_DEFAULTS)
        merged.setdefault("slot_save_path", tmp_path / "kv-cache")
        merged.setdefault("metrics_file", tmp_path / "metrics.jsonl")
        merged.update(overrides)

        lanes = merged.pop("lanes", None)
        demote = merged.pop("demote", None)
        labels = merged.pop("labels", None)

        lines = [f"{k} = {_toml_scalar(v)}" for k, v in merged.items()]
        if lanes:
            for lane in lanes:
                lines.append("")
                lines.append(_render_table("[[classify.lanes]]", lane))
        if demote:
            for d in demote:
                lines.append("")
                lines.append(_render_table("[[classify.demote]]", d))
        if labels:
            for lb in labels:
                lines.append("")
                lines.append(_render_table("[[classify.labels]]", lb))

        toml_path = tmp_path / f"cfg-{next(_counter)}.toml"
        toml_path.write_text("\n".join(lines) + "\n")
        return load_config(toml_path, env={})

    return _make


# ---------------------------------------------------------------------------
# FakeUpstream: emulates llama-swap (chat completions, /health) AND
# llama-server's admin API (/slots/{id}?action=save|restore, /props) on one
# ThreadingHTTPServer, since tests point both upstream_* and admin_* at it.
# ---------------------------------------------------------------------------


class _FakeState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.chat_calls: list[dict[str, Any]] = []
        self.admin_calls: list[dict[str, Any]] = []
        self.props: dict[str, Any] = {"model_path": "model-a", "uptime_s": 100.0}
        self.props_status: int | None = None  # None -> 200
        self.chat_delay = 0.0
        self.chat_hold: threading.Event | None = None
        self.stream = True
        self.chat_status = 200
        self.usage: dict[str, Any] = {
            "prompt_tokens": 10,
            "completion_tokens": 5,
            "cached_tokens": 0,
        }
        self.timings: dict[str, Any] = {
            "predicted_per_second": 20.0,
            "prompt_per_second": 500.0,
            "predicted_n": 5,
        }
        self.completion_stop_type: str = "eos"  # native /completion stop reason
        self.health_ok = True
        self.save_creates_file = False
        self.save_dir: Path | None = None
        self.admin_fail_slots: set[tuple[int, str]] = set()
        self.hang_slots: set[tuple[int, str]] = set()
        self.hang_seconds = 2.0
        self.fail_restore_filenames: set[str] = set()
        # /apply-template (prefix restore): deterministic "render" of the chat
        # body -- role-tagged message contents concatenated in order.
        self.apply_template_status = 200
        # Stream only the first chunk of a declared-longer body, then drop the
        # connection (an upstream aborted/killed mid-reply).
        self.truncate_stream = False
        # Chunked (llama-server-style) stream that sends one chunk, then waits
        # on this event before finishing -- lets a test disconnect mid-stream.
        self.chunked_hold: threading.Event | None = None
        # Gufo-style stream: a role-only event first, then this many seconds
        # before the first content delta (prefill happens in between).
        self.role_first_delay = 0.0
        # 200 SSE stream whose last frame is an llama-server error event.
        self.stream_error = False
        # GET /slots reports every slot is_processing until this monotonic time.
        self.slots_busy_until = 0.0
        self.apply_template_calls: list[dict[str, Any]] = []
        # Gufo emulation (all off by default): GET /v1/models and /metrics
        # payloads (None -> 404), a /slots override, and a raw chat reply body
        # (e.g. a captured Gufo SSE stream) sent verbatim instead of the
        # synthesised one.
        self.models: dict[str, Any] | None = None
        self.metrics_text: str | None = None
        self.slots_payload: list[dict[str, Any]] | None = None
        self.chat_raw: bytes | None = None
        self.get_paths: list[str] = []
        # Timed SSE script for the chat reply: (delay_s, data) steps, each sent
        # as one write after its delay; data None drops the connection there.
        # Chunked unless chat_script_chunked is False (then close-delimited).
        self.chat_script: list[tuple[float, bytes | None]] | None = None
        self.chat_script_chunked = True
        # GET /slots scripted over time: called per request, returns the payload
        # or an int status to fail with. Calls are counted in slots_calls.
        self.slots_fn: Callable[[], Any] | None = None
        self.slots_calls = 0
        # Set when the peer closed the connection while a script was running.
        self.chat_peer_closed = threading.Event()


_SLOT_RE = re.compile(r"/slots/(\d+)\?action=(\w+)")


class _FakeHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: D401 - silence test logs
        pass

    @property
    def _state(self) -> _FakeState:
        return self.server.fake_state  # type: ignore[attr-defined]

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass

    # -- GET: /props, /health --------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802
        st = self._state
        with st.lock:
            st.get_paths.append(self.path)
        if self.path.startswith("/v1/models") and st.models is not None:
            self._send_json(200, st.models)
        elif self.path.startswith("/metrics") and st.metrics_text is not None:
            data = st.metrics_text.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        elif self.path.startswith("/slots") and st.slots_fn is not None:
            with st.lock:
                st.slots_calls += 1
            payload = st.slots_fn()
            if isinstance(payload, int):
                self._send_json(payload, {"error": "slots_unavailable"})
            else:
                self._send_json(200, payload)
        elif self.path.startswith("/slots") and st.slots_payload is not None:
            self._send_json(200, st.slots_payload)  # type: ignore[arg-type]
        elif self.path.startswith("/props"):
            if st.props_status is not None and not (200 <= st.props_status < 300):
                self._send_json(st.props_status, {"error": "props_unavailable"})
            else:
                with st.lock:
                    props = dict(st.props)
                self._send_json(200, props)
        elif self.path.startswith("/slots"):
            busy = time.monotonic() < st.slots_busy_until
            self._send_json(200, [{"id": i, "is_processing": busy} for i in range(4)])  # type: ignore[arg-type]
        elif self.path.startswith("/health"):
            if st.health_ok:
                self._send_json(200, {"status": "ok"})
            else:
                self._send_json(503, {"status": "degraded"})
        else:
            self._send_json(404, {"error": "not_found", "path": self.path})

    # -- POST: /v1/chat/completions, /slots/{id}?action=... --------------------

    def do_POST(self) -> None:  # noqa: N802
        st = self._state
        m = _SLOT_RE.search(self.path)
        if m is not None:
            self._handle_slot_action(st, int(m.group(1)), m.group(2))
            return
        if self.path.endswith("/v1/chat/completions"):
            self._handle_chat(st)
            return
        if self.path.endswith("/completion") or self.path.endswith("/v1/completions"):
            self._handle_completion(st)
            return
        if self.path.endswith("/apply-template"):
            self._handle_apply_template(st)
            return
        self._read_body()
        self._send_json(200, {"ok": True})

    def _handle_apply_template(self, st: _FakeState) -> None:
        body = self._read_body()
        try:
            payload = json.loads(body) if body else {}
        except ValueError:
            payload = {}
        with st.lock:
            st.apply_template_calls.append({"ts": time.monotonic(), "body": payload})
            status = st.apply_template_status
        if not (200 <= status < 300):
            self._send_json(status, {"error": "apply_template_failed"})
            return
        # Qwen-style: tool definitions render FIRST, then the messages.
        tools = payload.get("tools")
        prompt = (f"<|tools|>{json.dumps(tools, sort_keys=True)}" if tools else "") + "".join(
            f"<|{m.get('role')}|>{m.get('content')}" for m in payload.get("messages") or []
        )
        self._send_json(200, {"prompt": prompt})

    def _handle_slot_action(self, st: _FakeState, slot: int, action: str) -> None:
        body = self._read_body()
        try:
            payload = json.loads(body) if body else {}
        except ValueError:
            payload = {}
        filename = payload.get("filename")

        with st.lock:
            hang = (slot, action) in st.hang_slots
            hang_seconds = st.hang_seconds
            fail = (slot, action) in st.admin_fail_slots
            fail_by_name = action == "restore" and filename in st.fail_restore_filenames
            st.admin_calls.append(
                {"action": action, "slot": slot, "filename": filename, "ts": time.monotonic()}
            )
            save_creates_file = st.save_creates_file
            save_dir = st.save_dir

        if hang:
            time.sleep(hang_seconds)
        if fail or fail_by_name:
            self._send_json(500, {"error": "slot_busy_or_missing"})
            return
        if action == "save" and save_creates_file and filename and save_dir:
            try:
                save_dir.mkdir(parents=True, exist_ok=True)
                (save_dir / filename).write_bytes(b"fake-kv-snapshot")
            except OSError:
                pass
        self._send_json(200, {"ok": True})

    def _handle_chat(self, st: _FakeState) -> None:
        body = self._read_body()
        try:
            obj = json.loads(body) if body else {}
        except ValueError:
            obj = {}
        with st.lock:
            st.chat_calls.append({"body": obj, "ts": time.monotonic()})
            delay = st.chat_delay
            hold = st.chat_hold
            stream = st.stream
            status = st.chat_status
            usage = dict(st.usage)
            timings = dict(st.timings)

        if hold is not None:
            hold.wait(timeout=15.0)
        elif delay:
            time.sleep(delay)

        if st.chat_script is not None:
            self._send_script(st, status)
            return
        if st.chat_raw is not None:
            ctype = "text/event-stream" if obj.get("stream") else "application/json"
            try:
                self.send_response(status)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(st.chat_raw)))
                self.end_headers()
                self.wfile.write(st.chat_raw)
            except (BrokenPipeError, ConnectionResetError):
                pass
            return
        if stream:
            self._send_stream(status, usage, timings)
        else:
            self._send_nonstream(status, usage, timings)

    def _send_script(self, st: _FakeState, status: int) -> None:
        chunked = st.chat_script_chunked
        self.close_connection = True
        try:
            self.send_response(status)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Transfer-Encoding" if chunked else "Connection",
                             "chunked" if chunked else "close")
            self.end_headers()
            for delay, data in st.chat_script or []:
                # Wait out the delay, but notice the peer closing meanwhile.
                end = time.monotonic() + delay
                while (left := end - time.monotonic()) > 0:
                    if select.select([self.connection], [], [], left)[0]:
                        if not self.connection.recv(1, socket.MSG_PEEK):
                            st.chat_peer_closed.set()
                            return
                if data is None:
                    return
                self.wfile.write(f"{len(data):x}\r\n".encode() + data + b"\r\n" if chunked else data)
                self.wfile.flush()
            if chunked:
                self.wfile.write(b"0\r\n\r\n")
        except OSError:
            st.chat_peer_closed.set()

    def _send_stream(self, status: int, usage: dict, timings: dict) -> None:
        chunk1 = {
            "id": "cmpl-1",
            "object": "chat.completion.chunk",
            "choices": [{"index": 0, "delta": {"content": "hi"}, "finish_reason": None}],
        }
        final = {
            "id": "cmpl-1",
            "object": "chat.completion.chunk",
            "model": "/models/fake.gguf",
            "system_fingerprint": "fake-build",
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            "usage": usage,
            "timings": timings,
        }
        body = (
            f"data: {json.dumps(chunk1)}\n\n".encode()
            + f"data: {json.dumps(final)}\n\n".encode()
            + b"data: [DONE]\n\n"
        )
        if self._state.stream_error:
            err = {"error": {"code": 500, "message": "decode failed", "type": "server_error"}}
            body = f"data: {json.dumps(chunk1)}\n\n".encode() + f"data: {json.dumps(err)}\n\n".encode()
        if self._state.role_first_delay > 0:
            role = {"id": "cmpl-1", "object": "chat.completion.chunk",
                    "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]}
            first = f"data: {json.dumps(role)}\n\n".encode()
            self.send_response(status)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            self.wfile.write(f"{len(first):x}\r\n".encode() + first + b"\r\n")
            self.wfile.flush()
            time.sleep(self._state.role_first_delay)
            self.wfile.write(f"{len(body):x}\r\n".encode() + body + b"\r\n0\r\n\r\n")
            self.wfile.flush()
            return
        hold = self._state.chunked_hold
        if hold is not None:
            try:
                self.send_response(status)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                first = f"data: {json.dumps(chunk1)}\n\n".encode()
                self.wfile.write(f"{len(first):x}\r\n".encode() + first + b"\r\n")
                self.wfile.flush()
                hold.wait(timeout=10.0)
                rest = body[len(first):]
                self.wfile.write(f"{len(rest):x}\r\n".encode() + rest + b"\r\n0\r\n\r\n")
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            return
        try:
            self.send_response(status)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self._state.truncate_stream:
                self.wfile.write(f"data: {json.dumps(chunk1)}\n\n".encode())
                self.wfile.flush()
                self.close_connection = True
                return
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _send_nonstream(self, status: int, usage: dict, timings: dict) -> None:
        payload = {
            "id": "cmpl-1",
            "object": "chat.completion",
            "model": "/models/fake.gguf",
            "system_fingerprint": "fake-build",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "hi"},
                    "finish_reason": "stop",
                }
            ],
            "usage": usage,
            "timings": timings,
        }
        self._send_json(status, payload)

    def _handle_completion(self, st: _FakeState) -> None:
        """Native llama.cpp /completion: no `usage` object; tokens at top level."""
        body = self._read_body()
        try:
            obj = json.loads(body) if body else {}
        except ValueError:
            obj = {}
        with st.lock:
            st.chat_calls.append({"body": obj, "ts": time.monotonic()})
            status = st.chat_status
            usage = dict(st.usage)
            timings = dict(st.timings)
            stop_type = st.completion_stop_type
        payload = {
            "content": "native completion content",
            "model": "/models/fake.gguf",
            "id_slot": obj.get("id_slot", 0),
            "tokens_evaluated": usage.get("prompt_tokens"),
            "tokens_predicted": usage.get("completion_tokens"),
            "tokens_cached": 0,
            "stop_type": stop_type,
            "timings": timings,
        }
        self._send_json(status, payload)


class _FakeHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class FakeUpstream:
    """Test double for llama-swap + llama-server, on one ephemeral port."""

    def __init__(self) -> None:
        self.state = _FakeState()
        self._httpd = _FakeHTTPServer(("127.0.0.1", 0), _FakeHandler)
        self._httpd.fake_state = self.state  # type: ignore[attr-defined]
        self.host = "127.0.0.1"
        self.port = self._httpd.server_address[1]
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, name="fake-upstream", daemon=True
        )
        self._thread.start()

    # -- introspection -----------------------------------------------------

    @property
    def chat_calls(self) -> list[dict[str, Any]]:
        with self.state.lock:
            return list(self.state.chat_calls)

    @property
    def admin_calls(self) -> list[dict[str, Any]]:
        with self.state.lock:
            return list(self.state.admin_calls)

    def admin_calls_matching(self, **kw: Any) -> list[dict[str, Any]]:
        return [c for c in self.admin_calls if all(c.get(k) == v for k, v in kw.items())]

    # -- configuration -------------------------------------------------------

    def set_chat_delay(self, seconds: float) -> None:
        self.state.chat_delay = seconds

    def set_chat_hold(self, event: threading.Event | None) -> None:
        self.state.chat_hold = event

    def set_stream(self, flag: bool) -> None:
        self.state.stream = flag

    def set_chat_status(self, status: int) -> None:
        self.state.chat_status = status

    def set_usage(self, **kw: Any) -> None:
        self.state.usage.update(kw)

    def set_timings(self, **kw: Any) -> None:
        self.state.timings.update(kw)

    def set_health(self, ok: bool) -> None:
        self.state.health_ok = ok

    def set_props(self, **kw: Any) -> None:
        with self.state.lock:
            self.state.props.update(kw)

    def replace_props(self, props: dict[str, Any]) -> None:
        with self.state.lock:
            self.state.props = dict(props)

    def set_slots_busy_for(self, seconds: float) -> None:
        self.state.slots_busy_until = time.monotonic() + seconds

    def set_stream_error(self, flag: bool) -> None:
        self.state.stream_error = flag

    def set_role_first_delay(self, seconds: float) -> None:
        self.state.role_first_delay = seconds

    def set_chunked_hold(self, event: threading.Event | None) -> None:
        self.state.chunked_hold = event

    def set_truncate_stream(self, flag: bool) -> None:
        self.state.truncate_stream = flag

    def set_apply_template_status(self, status: int) -> None:
        self.state.apply_template_status = status

    @property
    def apply_template_calls(self) -> list[dict[str, Any]]:
        with self.state.lock:
            return list(self.state.apply_template_calls)

    def set_models(self, models: dict[str, Any] | None) -> None:
        self.state.models = models

    def set_metrics_text(self, text: str | None) -> None:
        self.state.metrics_text = text

    def set_slots_payload(self, payload: list[dict[str, Any]] | None) -> None:
        self.state.slots_payload = payload

    def set_slots_fn(self, fn: Callable[[], Any] | None) -> None:
        self.state.slots_fn = fn

    def set_chat_raw(self, body: bytes | None) -> None:
        self.state.chat_raw = body

    def set_chat_script(
        self, steps: list[tuple[float, bytes | None]] | None, chunked: bool = True
    ) -> None:
        self.state.chat_script = steps
        self.state.chat_script_chunked = chunked

    @property
    def get_paths(self) -> list[str]:
        with self.state.lock:
            return list(self.state.get_paths)

    def set_props_status(self, status: int | None) -> None:
        self.state.props_status = status

    def fail_admin(self, slot: int, action: str) -> None:
        self.state.admin_fail_slots.add((slot, action))

    def unfail_admin(self, slot: int, action: str) -> None:
        self.state.admin_fail_slots.discard((slot, action))

    def hang_admin(self, slot: int, action: str, seconds: float | None = None) -> None:
        if seconds is not None:
            self.state.hang_seconds = seconds
        self.state.hang_slots.add((slot, action))

    def fail_restore_filename(self, filename: str) -> None:
        self.state.fail_restore_filenames.add(filename)

    def enable_save_files(self, path: Path) -> None:
        self.state.save_creates_file = True
        self.state.save_dir = Path(path)

    # -- lifecycle -----------------------------------------------------------

    def wait_for_chat_calls(self, n: int, deadline: float = 3.0) -> list[dict[str, Any]]:
        return _poll_until(lambda: self.chat_calls if len(self.chat_calls) >= n else None, deadline)

    def wait_for_admin_calls(self, n: int, deadline: float = 3.0) -> list[dict[str, Any]]:
        return _poll_until(
            lambda: self.admin_calls if len(self.admin_calls) >= n else None, deadline
        )

    def shutdown(self) -> None:
        try:
            self._httpd.shutdown()
            self._httpd.server_close()
        except Exception:  # noqa: BLE001 - best-effort in test teardown
            pass


def _poll_until(check: Callable[[], Any], deadline_s: float, interval: float = 0.02) -> Any:
    """Poll ``check`` until it returns a truthy value or ``deadline_s`` elapses.

    Used throughout the suite instead of fixed sleeps, per the testing
    contract's "no flaky sleeps -- poll with deadlines" rule.
    """
    start = time.monotonic()
    while True:
        result = check()
        if result:
            return result
        if time.monotonic() - start > deadline_s:
            raise AssertionError(f"condition not met within {deadline_s}s")
        time.sleep(interval)


@pytest.fixture
def poll_until():
    """Expose the polling helper to tests as a fixture."""
    return _poll_until


@pytest.fixture
def fake_upstream():
    fu = FakeUpstream()
    try:
        yield fu
    finally:
        fu.shutdown()
