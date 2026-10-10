"""Fail-fast on a silent upstream: ``upstream_stall_timeout_s`` and
``hold_headers_until_first_output`` (docs/configuration.md).

Real HTTP through a running ProxyServer against FakeUpstream's timed SSE
script. Limits are kept short (0.3-1 s) so the file stays fast.
"""

from __future__ import annotations

import http.client
import json
import logging
import math
import socket
import threading
import time

import pytest
from test_integration import (
    REFERENCE_LANES,
    _assert_failed_record,
    _bulk_body,
    _failed_rows,
    _metric_rows,
    _reference_config,
    _start_harness,
    _stop_harness,
)

from slotpin import cli
from slotpin.config import ConfigError, load_config, runtime_path_warnings
from slotpin.server import _OutputScanner
from slotpin.slot_admin import SlotAdmin


def _event(delta: dict, **extra) -> bytes:
    obj = {"id": "c1", "choices": [{"index": 0, "delta": delta, "finish_reason": None}], **extra}
    return f"data: {json.dumps(obj)}\n\n".encode()


ROLE = _event({"role": "assistant"})  # Gufo sends this before prefill
PING = b": ping\n\n"
TOK_A = _event({"content": "Hel"})
TOK_B = _event({"content": "lo"})
FINAL = (
    b'data: {"id": "c1", "model": "/models/fake.gguf", "choices": [{"index": 0, "delta": {}, '
    b'"finish_reason": "stop"}], "usage": {"prompt_tokens": 10, "completion_tokens": 2}}\n\n'
    b"data: [DONE]\n\n"
)
ERROR = b'data: {"error": {"code": 500, "message": "decode failed", "type": "server_error"}}\n\n'
PINGS_FOR_EVER = [(0.1, PING)] * 300  # 30 s: far beyond any limit used here


def _cfg(make_config, fake_upstream, **kw):
    kw.setdefault("kv_persist", False)
    kw.setdefault("kv_adaptive", False)
    kw.setdefault("upstream_timeout_s", 30.0)
    return _reference_config(make_config, fake_upstream, **kw)


def _stream(port: int, body: dict, timeout: float = 10.0) -> dict:
    """POST a chat request; report what the client saw and when."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    t0 = time.monotonic()
    try:
        conn.request("POST", "/v1/chat/completions", body=json.dumps(body),
                     headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        t_headers = time.monotonic() - t0
        complete = True
        try:
            raw = resp.read()
        except (http.client.IncompleteRead, ConnectionError) as exc:
            complete = False  # cut off: no terminal chunk
            raw = getattr(exc, "partial", b"")
        return {
            "status": resp.status,
            "ctype": resp.getheader("Content-Type"),
            "body": raw,
            "complete": complete,
            "t_headers": t_headers,
            "t_end": time.monotonic() - t0,
        }
    finally:
        conn.close()


def _turn_rows(cfg) -> list[dict]:
    return [r for r in _metric_rows(cfg) if "event" not in r and "ok" in r]


def _stall_logs(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if "upstream stalled" in r.getMessage()]


# -- 1: both keys off -> relayed exactly as before ---------------------------------


def test_keys_off_stream_is_relayed_unchanged(make_config, fake_upstream, poll_until):
    steps = [(0, ROLE), (0.15, PING), (0.15, PING), (0.15, TOK_A), (0.05, TOK_B), (0.05, FINAL)]
    fake_upstream.set_chat_script(steps)
    cfg = _cfg(make_config, fake_upstream)
    assert cfg.upstream_stall_timeout_s == 0 and cfg.hold_headers_until_first_output is False
    h = _start_harness(cfg)
    try:
        got = _stream(h.port, _bulk_body("off", stream=True))
        assert got["status"] == 200 and got["complete"]
        assert got["ctype"] == "text/event-stream"
        assert got["body"] == b"".join(d for _, d in steps)
        assert got["t_headers"] < 0.35, "headers must not wait for the first token (0.45 s)"
        poll_until(lambda: _turn_rows(cfg), deadline_s=3.0)
        assert _turn_rows(cfg)[0]["ttft_s"] >= 0.4
        assert _failed_rows(cfg) == []
    finally:
        _stop_harness(h)


def test_keys_on_leave_an_ordinary_stream_alone(make_config, fake_upstream, poll_until):
    # The fake's default (Content-Length) SSE reply, with and without the keys.
    seen = []
    for i, kw in enumerate(({}, {"upstream_stall_timeout_s": 1.0,
                                 "hold_headers_until_first_output": True})):
        cfg = _cfg(make_config, fake_upstream, metrics_file=cfg_path(make_config, i), **kw)
        h = _start_harness(cfg)
        try:
            got = _stream(h.port, _bulk_body("plain", stream=True))
            poll_until(lambda cfg=cfg: _turn_rows(cfg), deadline_s=3.0)
            assert _failed_rows(cfg) == []
            seen.append((got["status"], got["ctype"], got["body"], got["complete"]))
        finally:
            _stop_harness(h)
    assert seen[0] == seen[1] and seen[0][0] == 200 and seen[0][3]


def cfg_path(make_config, i: int):
    """A distinct metrics file per harness when one test starts several."""
    return make_config().metrics_file.with_name(f"metrics-{i}.jsonl")


# -- 2: limit on, hold off: prefill stall -> stream cut ------------------------------


def test_stall_before_output_cuts_the_stream(make_config, fake_upstream, poll_until, caplog):
    fake_upstream.set_chat_script([(0, ROLE), *PINGS_FOR_EVER])
    cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=0.5)
    h = _start_harness(cfg)
    try:
        with caplog.at_level(logging.WARNING, logger="slotpin.server"):
            got = _stream(h.port, _bulk_body("stall", stream=True))
            assert got["status"] == 200 and not got["complete"]
            assert got["body"].startswith(ROLE + PING)
            assert got["body"].replace(PING, b"") == ROLE
            assert 0.45 <= got["t_end"] < 3.0
            assert fake_upstream.state.chat_peer_closed.wait(3.0), "upstream was not aborted"
            _assert_failed_record(cfg, poll_until, "stalled", upstream_status=200)
        (line,) = _stall_logs(caplog)
        assert "slot=0" in line and "phase=before first output" in line
        assert "BULKTAG" not in line
        with h.state.lock:
            assert h.state.slot_last.get(0) is None  # aborted turn: slot KV unknown
    finally:
        _stop_harness(h)


def test_stalled_stream_is_cut_while_the_slot_is_still_held(make_config, fake_upstream, poll_until):
    # The post-abort slot hold must not delay the client seeing the cut.
    fake_upstream.set_chat_script([(0, ROLE), *PINGS_FOR_EVER])
    fake_upstream.set_slots_busy_for(2.0)
    cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=0.3)
    h = _start_harness(cfg)
    try:
        got = _stream(h.port, _bulk_body("held", stream=True))
        assert not got["complete"] and got["t_end"] < 1.5
        with h.state.lock:
            assert h.state.slot_busy.get(0) is True
        poll_until(lambda: not h.state.slot_busy.get(0), deadline_s=5.0)
        _assert_failed_record(cfg, poll_until, "stalled", upstream_status=200)
    finally:
        _stop_harness(h)


# -- 3: limit on, hold on: prefill stall -> 504, no partial 200 ----------------------


def test_stall_before_output_with_hold_is_a_504(make_config, fake_upstream, poll_until, caplog):
    fake_upstream.set_chat_script([(0, ROLE), *PINGS_FOR_EVER])
    cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=0.5,
               hold_headers_until_first_output=True)
    h = _start_harness(cfg)
    try:
        with caplog.at_level(logging.WARNING, logger="slotpin.server"):
            got = _stream(h.port, _bulk_body("stall", stream=True))
            assert got["status"] == 504 and got["complete"]
            assert got["ctype"] == "application/json"
            payload = json.loads(got["body"])
            assert payload["error"] == "upstream_stalled"
            assert payload["local_available"] is False
            assert 0.45 <= payload["stalled_s"] < 3.0
            assert set(payload) == {"error", "local_available", "stalled_s"}
            assert fake_upstream.state.chat_peer_closed.wait(3.0), "upstream was not aborted"
            _assert_failed_record(cfg, poll_until, "stalled", upstream_status=200)
        (line,) = _stall_logs(caplog)
        assert "phase=before first output" in line
    finally:
        _stop_harness(h)


def test_stall_before_upstream_headers_is_a_504(make_config, fake_upstream, poll_until):
    # llama-server style: no status line until prefill is done. Nothing has been
    # sent to the client, so it is a 504 even without the hold.
    hold = threading.Event()
    fake_upstream.set_chat_hold(hold)
    cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=0.4)
    h = _start_harness(cfg)
    try:
        got = _stream(h.port, _bulk_body("noheaders", stream=True))
        assert got["status"] == 504
        assert json.loads(got["body"])["error"] == "upstream_stalled"
        _assert_failed_record(cfg, poll_until, "stalled")  # no upstream status known
    finally:
        hold.set()
        _stop_harness(h)


# -- 4: limit on, hold on: slow prefill under the limit -> complete 200 --------------


def test_hold_releases_at_first_output(make_config, fake_upstream, poll_until):
    steps = [(0, ROLE), (0.2, PING), (0.2, PING), (0.1, TOK_A), (0.05, TOK_B), (0.05, FINAL)]
    fake_upstream.set_chat_script(steps)
    cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=1.0,
               hold_headers_until_first_output=True)
    h = _start_harness(cfg)
    try:
        got = _stream(h.port, _bulk_body("held", stream=True))
        assert got["status"] == 200 and got["complete"]
        assert got["ctype"] == "text/event-stream"
        assert got["body"] == b"".join(d for _, d in steps)
        assert got["t_headers"] >= 0.45, "headers were sent before the first output event"
        poll_until(lambda: _turn_rows(cfg), deadline_s=3.0)
        (turn,) = _turn_rows(cfg)
        assert turn["ok"] is True and turn["ttft_s"] >= 0.45
        assert turn["prompt_n"] == 10 and turn["stop_reason"] == "stop"
        assert _failed_rows(cfg) == []
    finally:
        _stop_harness(h)


def test_hold_gives_up_at_the_buffer_cap(make_config, fake_upstream):
    blob = b": " + b"x" * (150 * 1024) + b"\n\n"  # comments only: never an output event
    steps = [(0, ROLE), (0.05, blob), (0.05, blob), (0.8, TOK_A), (0.05, FINAL)]
    fake_upstream.set_chat_script(steps)
    cfg = _cfg(make_config, fake_upstream, hold_headers_until_first_output=True)
    h = _start_harness(cfg)
    try:
        got = _stream(h.port, _bulk_body("cap", stream=True))
        assert got["status"] == 200 and got["complete"]
        assert got["body"] == b"".join(d for _, d in steps)
        assert got["t_headers"] < 0.6, "the hold outlived its 256 KiB cap"
    finally:
        _stop_harness(h)


# -- 5: stall mid-generation ----------------------------------------------------------


@pytest.mark.parametrize("hold", [False, True])
def test_stall_after_output_cuts_the_stream(make_config, fake_upstream, poll_until, caplog, hold):
    fake_upstream.set_chat_script([(0, ROLE), (0.05, TOK_A), (0.05, TOK_B), *PINGS_FOR_EVER])
    cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=0.5,
               hold_headers_until_first_output=hold)
    h = _start_harness(cfg)
    try:
        with caplog.at_level(logging.WARNING, logger="slotpin.server"):
            got = _stream(h.port, _bulk_body("mid", stream=True))
            assert got["status"] == 200 and not got["complete"]
            assert got["body"].startswith(ROLE + TOK_A + TOK_B + PING)
            assert 0.5 <= got["t_end"] < 3.0
            assert fake_upstream.state.chat_peer_closed.wait(3.0)
            _assert_failed_record(cfg, poll_until, "stalled", upstream_status=200)
        (line,) = _stall_logs(caplog)
        assert "phase=after first output" in line
    finally:
        _stop_harness(h)


# -- 6: a completely silent upstream (no pings) ---------------------------------------


@pytest.mark.parametrize("chunked", [True, False])
@pytest.mark.parametrize("hold", [False, True])
def test_silent_upstream_still_trips_the_limit(
    make_config, fake_upstream, poll_until, hold, chunked
):
    fake_upstream.set_chat_script([(0, ROLE), (30.0, None)], chunked=chunked)
    cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=0.5,
               hold_headers_until_first_output=hold)
    h = _start_harness(cfg)
    try:
        got = _stream(h.port, _bulk_body("silent", stream=True))
        assert 0.45 <= got["t_end"] < 3.0
        if hold:
            assert got["status"] == 504
        else:
            assert got["status"] == 200 and not got["complete"] and got["body"] == ROLE
        assert fake_upstream.state.chat_peer_closed.wait(3.0), "upstream was not aborted"
        _assert_failed_record(cfg, poll_until, "stalled", upstream_status=200)
    finally:
        _stop_harness(h)


# -- 7: slow but alive -----------------------------------------------------------------


def test_slow_but_alive_stream_is_not_aborted(make_config, fake_upstream, poll_until):
    steps = [(0, ROLE), *[(0.35, TOK_A), (0.0, PING)] * 6, (0.05, FINAL)]  # 2.1 s, limit 0.7
    fake_upstream.set_chat_script(steps)
    cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=0.7,
               hold_headers_until_first_output=True)
    h = _start_harness(cfg)
    try:
        got = _stream(h.port, _bulk_body("slow", stream=True))
        assert got["status"] == 200 and got["complete"]
        assert got["body"] == b"".join(d for _, d in steps)
        assert got["t_end"] >= 2.0
        poll_until(lambda: _turn_rows(cfg), deadline_s=3.0)
        assert _failed_rows(cfg) == []
    finally:
        _stop_harness(h)


# -- 8: hold on, client gives up during the held prefill ------------------------------


@pytest.mark.parametrize("chunked", [True, False])
def test_client_disconnect_during_hold_is_aborted_client(
    make_config, fake_upstream, poll_until, chunked
):
    fake_upstream.set_chat_script([(0, ROLE), *PINGS_FOR_EVER], chunked=chunked)
    cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=10.0,
               hold_headers_until_first_output=True)
    h = _start_harness(cfg)
    try:
        payload = json.dumps(_bulk_body("gone", stream=True)).encode()
        s = socket.create_connection(("127.0.0.1", h.port), timeout=5)
        s.sendall(
            b"POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n"
            + f"Content-Length: {len(payload)}\r\n\r\n".encode()
            + payload
        )
        fake_upstream.wait_for_chat_calls(1)
        s.settimeout(0.4)
        with pytest.raises(TimeoutError):
            s.recv(64)  # held: not a byte while the upstream only pings
        s.close()
        assert fake_upstream.state.chat_peer_closed.wait(4.0), "upstream was not aborted"
        _assert_failed_record(cfg, poll_until, "aborted_client", upstream_status=200)
    finally:
        _stop_harness(h)


# -- 9: hold on, upstream fails before any output -> same as without the hold --------


@pytest.mark.parametrize(
    ("case", "reason", "status"),
    [
        ("http_500", "upstream_5xx", 500),
        ("closes_early", "upstream_incomplete", 200),
        ("stream_error", "upstream_incomplete", 200),
        ("no_output", None, 200),
    ],
)
def test_hold_does_not_change_non_stall_outcomes(
    make_config, fake_upstream, poll_until, case, reason, status
):
    if case == "http_500":
        fake_upstream.set_chat_status(500)
    elif case == "closes_early":
        fake_upstream.set_chat_script([(0, ROLE), (0.05, PING), (0.1, None)])
    elif case == "stream_error":
        fake_upstream.set_chat_script([(0, ROLE), (0.05, ERROR)])
    else:  # a complete reply that never carried an output delta
        fake_upstream.set_chat_script([(0, ROLE), (0.05, FINAL)])
    seen = []
    for i, hold in enumerate((False, True)):
        cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=3.0,
                   hold_headers_until_first_output=hold, metrics_file=cfg_path(make_config, i))
        h = _start_harness(cfg)
        try:
            got = _stream(h.port, _bulk_body(case, stream=True))
            assert got["status"] == status and got["t_end"] < 2.0
            if reason is None:
                poll_until(lambda cfg=cfg: _turn_rows(cfg), deadline_s=3.0)
                assert _failed_rows(cfg) == []
            else:
                _assert_failed_record(cfg, poll_until, reason, upstream_status=status)
            seen.append((got["status"], got["ctype"], got["body"], got["complete"]))
        finally:
            _stop_harness(h)
    assert seen[0] == seen[1]
    assert seen[0][2], "expected the upstream's bytes to reach the client"


# -- 10: non-streaming requests are out of scope --------------------------------------


def test_non_streaming_request_ignores_the_limit(make_config, fake_upstream, poll_until):
    fake_upstream.set_chat_delay(1.0)  # silent for three limits before replying
    fake_upstream.set_stream(False)
    cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=0.3,
               hold_headers_until_first_output=True)
    h = _start_harness(cfg)
    try:
        got = _stream(h.port, _bulk_body("nostream", stream=False))
        assert got["status"] == 200 and got["complete"]
        assert json.loads(got["body"])["choices"][0]["message"]["content"] == "hi"
        poll_until(lambda: _turn_rows(cfg), deadline_s=3.0)
        assert _failed_rows(cfg) == []
    finally:
        _stop_harness(h)


def test_non_chat_stream_ignores_the_limit(make_config, fake_upstream):
    # Pass-through paths (here /v1/responses) never get the limit or the hold.
    cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=0.3,
               hold_headers_until_first_output=True)
    h = _start_harness(cfg)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", h.port, timeout=5)
        conn.request("POST", "/v1/responses", body=json.dumps({"stream": True}),
                     headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        assert resp.status == 200 and json.loads(resp.read()) == {"ok": True}
        conn.close()
    finally:
        _stop_harness(h)


# -- 11: events vs reads ---------------------------------------------------------------


def test_scanner_classifies_events_not_reads():
    sc = _OutputScanner()
    assert sc.feed(ROLE) is False  # role-only first event
    assert sc.feed(PING) is False  # comment
    assert sc.feed(_event({"role": "assistant", "content": ""})) is False  # empty content
    assert sc.feed(FINAL) is False  # finish + usage + [DONE]
    assert sc.feed(ERROR) is False
    assert sc.feed(b': "content": "not an event"\n\n') is False  # comment that looks like one
    assert sc.feed(TOK_A) is True
    assert sc.feed(_event({"reasoning_content": "hm"})) is True
    assert sc.feed(_event({"tool_calls": [{"index": 0, "function": {"arguments": "{"}}]})) is True
    assert sc.feed(PING + TOK_A) is True  # a ping and a token in one read
    assert sc.feed(PING + PING) is False

    # One event split across two reads, at every byte position: seen exactly
    # when the delta arrives, never lost.
    for cut in range(1, len(TOK_A)):
        sc = _OutputScanner()
        first = sc.feed(TOK_A[:cut])
        second = sc.feed(TOK_A[cut:])
        assert first or second, cut
        assert sc.feed(PING) is False, cut  # nothing left over to match again

    # A comment split across reads stays a comment.
    sc = _OutputScanner()
    assert sc.feed(b": pi") is False and sc.feed(b"ng\n\n") is False

    # A very long line is bounded in memory but still classified.
    sc = _OutputScanner()
    assert sc.feed(b'data: {"x": "' + b"y" * 20000) is False
    assert len(sc._line) < 1024
    assert sc.feed(b'", "choices": [{"delta": {"content": "z"}}]}\n\n') is True
    sc = _OutputScanner()
    assert sc.feed(b": " + b"y" * 20000) is False
    assert sc.feed(b' "content": "z"\n\n') is False  # still the same comment line


def test_split_event_and_shared_read_through_the_relay(make_config, fake_upstream, poll_until):
    # Output completes at 0.6 s (split token) and 0.9 s (token behind a ping);
    # the reply ends at 1.5 s. Every gap is under the 0.8 s limit only if both
    # are counted as output.
    steps = [(0, ROLE), (0.3, PING + TOK_A[:25]), (0.3, TOK_A[25:] + PING), (0.3, PING + TOK_B),
             (0.6, FINAL)]
    fake_upstream.set_chat_script(steps)
    cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=0.8,
               hold_headers_until_first_output=True)
    h = _start_harness(cfg)
    try:
        got = _stream(h.port, _bulk_body("split", stream=True))
        assert got["status"] == 200 and got["complete"]
        assert got["body"] == b"".join(d for _, d in steps)
        poll_until(lambda: _turn_rows(cfg), deadline_s=3.0)
        assert _failed_rows(cfg) == []
    finally:
        _stop_harness(h)


# -- 13: stream silence is a stall only without token progress upstream ---------------

SECRET = "SECRET-PROMPT-TEXT"
TOOL = _event({"tool_calls": [{"index": 0, "id": "t1", "type": "function",
                               "function": {"name": "write", "arguments": "{\"a\": 1}"}}]})


def _slot_entry(n_prompt, n_decoded, busy=True) -> dict:
    """One /slots entry as Gufo reports it, including the prompt text."""
    return {"id": 0, "is_processing": busy, "n_prompt_tokens_processed": n_prompt,
            "prompt": SECRET, "next_token": [{"has_next_token": True, "n_decoded": n_decoded}]}


def _decoding(fake_upstream, until: float = math.inf, busy: bool = True):
    """/slots whose n_decoded climbs with time since the chat request (100/s),
    freezing ``until`` seconds in."""

    def _slots():
        calls = fake_upstream.chat_calls
        t = time.monotonic() - calls[-1]["ts"] if calls else 0.0
        return [_slot_entry(2232, int(min(t, until) * 100), busy)]

    return _slots


def _no_prompt_text(caplog, cfg, *blobs: bytes) -> None:
    assert SECRET not in "\n".join(r.getMessage() for r in caplog.records)
    assert SECRET not in (cfg.metrics_file.read_text() if cfg.metrics_file.exists() else "")
    for blob in blobs:
        assert SECRET.encode() not in blob


@pytest.mark.parametrize("hold", [False, True])
def test_silent_stream_with_token_progress_is_not_a_stall(
    make_config, fake_upstream, poll_until, caplog, hold
):
    # Gufo during a tool call: only pings for 2.2 s (3.7 limits), counters climbing.
    steps = [(0, ROLE), *[(0.1, PING)] * 22, (0.0, TOOL), (0.05, FINAL)]
    fake_upstream.set_chat_script(steps)
    fake_upstream.set_slots_fn(_decoding(fake_upstream))
    cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=0.6,
               hold_headers_until_first_output=hold)
    h = _start_harness(cfg)
    try:
        with caplog.at_level(logging.DEBUG):
            got = _stream(h.port, _bulk_body("tool", stream=True))
            assert got["status"] == 200 and got["complete"]
            assert got["body"] == b"".join(d for _, d in steps)
            if hold:
                assert got["t_headers"] >= 2.1, "bytes reached the client before the delta"
            poll_until(lambda: _turn_rows(cfg), deadline_s=3.0)
            assert _failed_rows(cfg) == []
        assert not _stall_logs(caplog)
        assert any("silent but decoding" in r.getMessage() and r.levelno == logging.DEBUG
                   for r in caplog.records)
        # One GET per half limit of silence, no more: 2.2 s / 0.3 s.
        assert 5 <= fake_upstream.state.slots_calls <= 8
        _no_prompt_text(caplog, cfg, got["body"])
    finally:
        _stop_harness(h)


@pytest.mark.parametrize("hold", [False, True])
def test_silent_stream_with_frozen_counters_is_a_stall(
    make_config, fake_upstream, poll_until, caplog, hold
):
    fake_upstream.set_chat_script([(0, ROLE), *PINGS_FOR_EVER])
    fake_upstream.set_slots_fn(lambda: [_slot_entry(2232, 1)])  # the incident's reading
    cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=0.6,
               hold_headers_until_first_output=hold)
    h = _start_harness(cfg)
    try:
        with caplog.at_level(logging.DEBUG):
            got = _stream(h.port, _bulk_body("frozen", stream=True))
            assert 0.6 <= got["t_end"] <= 1.02  # 1.0x - 1.7x the limit
            if hold:
                assert got["status"] == 504
                assert json.loads(got["body"])["error"] == "upstream_stalled"
            else:
                assert got["status"] == 200 and not got["complete"]
            assert fake_upstream.state.chat_peer_closed.wait(3.0), "upstream was not aborted"
            _assert_failed_record(cfg, poll_until, "stalled", upstream_status=200)
        (line,) = _stall_logs(caplog)
        assert "token counters [(2232, 1)] then [(2232, 1)]" in line
        # Probes at half the limit and at the limit, + the post-abort slot-idle check.
        assert fake_upstream.state.slots_calls == 3
        _no_prompt_text(caplog, cfg, got["body"])
    finally:
        _stop_harness(h)


@pytest.mark.parametrize("failure", ["refused", "http_503", "garbage"])
def test_silent_stream_with_unreadable_slots_is_a_stall(
    make_config, fake_upstream, poll_until, caplog, failure
):
    fake_upstream.set_chat_script([(0, ROLE), *PINGS_FOR_EVER])
    admin_port = fake_upstream.port
    if failure == "refused":
        with socket.socket() as s:  # a port nothing listens on
            s.bind(("127.0.0.1", 0))
            admin_port = s.getsockname()[1]
    elif failure == "http_503":
        fake_upstream.set_slots_fn(lambda: 503)
    else:
        fake_upstream.set_slots_fn(lambda: {"slots": "not a list"})
    cfg = make_config(
        upstream_host=fake_upstream.host, upstream_port=fake_upstream.port,
        admin_host=fake_upstream.host, admin_port=admin_port, lanes=REFERENCE_LANES,
        sched_enabled=True, upstream_timeout_s=30.0, upstream_stall_timeout_s=0.6,
    )
    h = _start_harness(cfg)
    try:
        with caplog.at_level(logging.WARNING, logger="slotpin.server"):
            got = _stream(h.port, _bulk_body("blind", stream=True))
            assert got["status"] == 200 and not got["complete"]
            assert 0.6 <= got["t_end"] <= 1.1
            _assert_failed_record(cfg, poll_until, "stalled", upstream_status=200)
        (line,) = _stall_logs(caplog)
        assert "token counters None then None" in line
    finally:
        _stop_harness(h)


def test_progress_then_freeze_is_a_stall_half_a_limit_after_confirmation(
    make_config, fake_upstream, poll_until, caplog
):
    # Counters climb for 0.45 s, then freeze. Readings at 0.3 s (30), 0.6 s (45,
    # differs: alive) and 0.9 s (45, equal: stalled).
    fake_upstream.set_chat_script([(0, ROLE), *PINGS_FOR_EVER])
    fake_upstream.set_slots_fn(_decoding(fake_upstream, until=0.45))
    cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=0.6,
               hold_headers_until_first_output=True)
    h = _start_harness(cfg)
    try:
        with caplog.at_level(logging.WARNING, logger="slotpin.server"):
            got = _stream(h.port, _bulk_body("freeze", stream=True))
            assert got["status"] == 504
            assert 0.85 <= got["t_end"] <= 1.3
            assert 0.85 <= json.loads(got["body"])["stalled_s"] <= 1.3
            _assert_failed_record(cfg, poll_until, "stalled", upstream_status=200)
        (line,) = _stall_logs(caplog)
        assert "token counters [(2232, 45)] then [(2232, 45)]" in line
        assert fake_upstream.state.slots_calls == 4  # three probes + the slot-idle check
    finally:
        _stop_harness(h)


def test_silent_stream_with_no_busy_slot_is_a_stall(make_config, fake_upstream, poll_until):
    # Counters would differ, but nothing is processing: the upstream is not working on it.
    fake_upstream.set_chat_script([(0, ROLE), *PINGS_FOR_EVER])
    fake_upstream.set_slots_fn(_decoding(fake_upstream, busy=False))
    cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=0.6,
               hold_headers_until_first_output=True)
    h = _start_harness(cfg)
    try:
        got = _stream(h.port, _bulk_body("idle", stream=True))
        assert got["status"] == 504 and 0.6 <= got["t_end"] <= 1.02
        _assert_failed_record(cfg, poll_until, "stalled", upstream_status=200)
    finally:
        _stop_harness(h)


def test_token_progress_before_upstream_headers_is_not_a_stall(
    make_config, fake_upstream, poll_until
):
    # llama-server style (no status line during prefill), counters advancing.
    hold = threading.Event()
    fake_upstream.set_chat_hold(hold)
    fake_upstream.set_slots_fn(_decoding(fake_upstream))
    cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=0.4)
    h = _start_harness(cfg)
    try:
        threading.Timer(1.3, hold.set).start()  # headers after more than 3 limits
        got = _stream(h.port, _bulk_body("prefill", stream=True))
        assert got["status"] == 200 and got["complete"] and got["t_end"] >= 1.2
        poll_until(lambda: _turn_rows(cfg), deadline_s=3.0)
        assert _failed_rows(cfg) == []
    finally:
        hold.set()
        _stop_harness(h)


def test_slots_progress_copies_counters_only(make_config, fake_upstream):
    admin = SlotAdmin(make_config(admin_host=fake_upstream.host, admin_port=fake_upstream.port))
    fake_upstream.set_slots_fn(lambda: [
        _slot_entry(900, 7),
        {"id": 1, "is_processing": False, "n_prompt_tokens_processed": 5, "prompt": SECRET},
        {"id": 2, "is_processing": True, "prompt": SECRET},  # llama.cpp: no counters
        {"id": 3, "is_processing": True, "n_prompt_tokens_processed": 40,
         "next_token": {"n_decoded": 3}},  # older builds: an object, not a list
    ])
    sig = admin.slots_progress()
    assert sig == [(40, 3), (900, 7), (None, None)]
    assert SECRET not in repr(sig)
    fake_upstream.set_slots_fn(lambda: [])
    assert admin.slots_progress() == []
    fake_upstream.set_slots_fn(lambda: 500)
    assert admin.slots_progress() is None
    fake_upstream.set_slots_fn(lambda: {"error": "x"})
    assert admin.slots_progress() is None


def test_504_closes_the_connection_ahead_of_the_slot_hold(make_config, fake_upstream, poll_until):
    fake_upstream.set_chat_script([(0, ROLE), *PINGS_FOR_EVER])
    fake_upstream.set_slots_busy_for(2.5)  # busy, counters unknown: stall, then a held slot
    cfg = _cfg(make_config, fake_upstream, upstream_stall_timeout_s=0.3,
               hold_headers_until_first_output=True)
    h = _start_harness(cfg)
    try:
        payload = json.dumps(_bulk_body("close", stream=True)).encode()
        s = socket.create_connection(("127.0.0.1", h.port), timeout=1.5)
        s.sendall(
            b"POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n"
            + f"Content-Length: {len(payload)}\r\n\r\n".encode()
            + payload
        )
        raw = b""
        while chunk := s.recv(4096):  # a 1.5 s timeout here = left open behind the hold
            raw += chunk
        s.close()
        assert raw.startswith(b"HTTP/1.1 504 ") and b"upstream_stalled" in raw
        with h.state.lock:
            assert h.state.slot_busy.get(0) is True
        poll_until(lambda: not h.state.slot_busy.get(0), deadline_s=5.0)
    finally:
        _stop_harness(h)


# -- 12: config ------------------------------------------------------------------------


def test_config_defaults_and_overrides(make_config, tmp_path):
    cfg = make_config()
    assert cfg.upstream_stall_timeout_s == 0.0
    assert cfg.hold_headers_until_first_output is False
    cfg = make_config(upstream_stall_timeout_s=180, hold_headers_until_first_output=True)
    assert cfg.upstream_stall_timeout_s == 180.0 and cfg.hold_headers_until_first_output is True
    cfg = load_config(None, env={"SLOTPIN_UPSTREAM_STALL_TIMEOUT_S": "900",
                                 "SLOTPIN_HOLD_HEADERS_UNTIL_FIRST_OUTPUT": "on"})
    assert cfg.upstream_stall_timeout_s == 900.0 and cfg.hold_headers_until_first_output is True


@pytest.mark.parametrize("bad", [-1, -0.001, math.nan, math.inf])
def test_config_rejects_a_negative_stall_timeout(make_config, bad):
    with pytest.raises(ConfigError, match="upstream_stall_timeout_s"):
        make_config(upstream_stall_timeout_s=float(bad))


def test_config_rejects_bad_types():
    with pytest.raises(ConfigError, match="SLOTPIN_UPSTREAM_STALL_TIMEOUT_S"):
        load_config(None, env={"SLOTPIN_UPSTREAM_STALL_TIMEOUT_S": "soon"})
    with pytest.raises(ConfigError, match="SLOTPIN_HOLD_HEADERS_UNTIL_FIRST_OUTPUT"):
        load_config(None, env={"SLOTPIN_HOLD_HEADERS_UNTIL_FIRST_OUTPUT": "maybe"})


def test_stall_timeout_not_below_upstream_timeout_warns(make_config):
    assert not [w for w in runtime_path_warnings(make_config()) if "stall" in w]
    ok = make_config(upstream_stall_timeout_s=3.0, upstream_timeout_s=5.0)
    assert not [w for w in runtime_path_warnings(ok) if "stall" in w]
    late = make_config(upstream_stall_timeout_s=5.0, upstream_timeout_s=5.0)
    assert [w for w in runtime_path_warnings(late) if "upstream_stall_timeout_s" in w]


def _check(tmp_path, monkeypatch, capsys, text: str) -> tuple[int, str, str]:
    for key in ("SLOTPIN_UPSTREAM_STALL_TIMEOUT_S", "SLOTPIN_HOLD_HEADERS_UNTIL_FIRST_OUTPUT"):
        monkeypatch.delenv(key, raising=False)
    path = tmp_path / "check.toml"
    path.write_text(f'metrics_file = "off"\n{text}')
    rc = cli.main(["--config", str(path), "--check"])
    out = capsys.readouterr()
    return rc, out.out, out.err


def test_check_reports_the_fail_fast_keys(tmp_path, monkeypatch, capsys):
    rc, out, _ = _check(tmp_path, monkeypatch, capsys, "")
    assert rc == 0 and out == "slotpin: config OK\n"  # defaults: output unchanged

    rc, out, err = _check(
        tmp_path, monkeypatch, capsys,
        "upstream_stall_timeout_s = 180\nhold_headers_until_first_output = true\n",
    )
    assert rc == 0 and "config OK" in out
    assert "upstream_stall_timeout_s=180 hold_headers_until_first_output=true" in out
    assert "stall" not in err

    rc, out, _ = _check(tmp_path, monkeypatch, capsys, "hold_headers_until_first_output = true\n")
    assert rc == 0 and "nothing aborts a stalled upstream" in out

    rc, _, err = _check(tmp_path, monkeypatch, capsys, "upstream_stall_timeout_s = 1800\n")
    assert rc == 0 and "WARNING: upstream_stall_timeout_s (1800) is not below" in err

    rc, _, err = _check(tmp_path, monkeypatch, capsys, "upstream_stall_timeout_s = -5\n")
    assert rc == 1 and "upstream_stall_timeout_s" in err
