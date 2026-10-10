"""Longest-common-prefix disk restore (prefix_index.py + the step-3b pipeline hook).

The motivating case (observed in production): nightly fan-out workers send a
~34K-token system message whose only change night-to-night is a small tail
(date, per-run task text). The exact-hash snapshot key changes every night, so
last night's snapshot was never restored. These tests replay that shape: same
stable head, different tail on "night 2" -> night 1's snapshot is restored.
"""

from __future__ import annotations

import hashlib
import json
import time

from test_integration import _post_chat, _reference_config, _start_harness, _stop_harness

from slotpin.kvstore import KVStore
from slotpin.metrics import MetricsWriter
from slotpin.pool import CacheRamPool
from slotpin.prefix_index import IDX_SUFFIX, PrefixIndex, chunk_hashes, common_chunks
from slotpin.slot_admin import SlotAdmin
from slotpin.state import ProxyState

STABLE = "".join(f"stable instruction line {i}: tools, safety, workspace files\n" for i in range(120))


def _worker_body(date: str, brief: str = "read the calendar surface") -> dict:
    return {
        "model": "x",
        "stream": False,
        "messages": [
            {"role": "system", "content": f"{STABLE}## Temporal Context\nCurrent date: {date}\n{brief}\n"},
            {"role": "user", "content": "go"},
        ],
    }


def _ph(body: dict) -> str:
    system = "".join(m["content"] for m in body["messages"] if m["role"] == "system")
    return hashlib.sha1(system.encode("utf-8")).hexdigest()[:8]


def _prefix_cfg(make_config, fake_upstream, **overrides):
    overrides.setdefault("lanes", [])
    overrides.setdefault("recur_threshold", 1)
    overrides.setdefault("prefix_restore", True)
    overrides.setdefault("prefix_chunk_chars", 64)
    overrides.setdefault("prefix_restore_min_chars", 256)
    overrides.setdefault("prefix_save_min_interval_s", 0.0)
    cfg = _reference_config(make_config, fake_upstream, **overrides)
    fake_upstream.enable_save_files(cfg.slot_save_path)
    return cfg


def _wait_file(path, deadline: float = 3.0) -> None:
    end = time.monotonic() + deadline
    while not path.exists():
        if time.monotonic() > end:
            raise AssertionError(f"{path} never appeared")
        time.sleep(0.02)


def _restore_records(cfg) -> list[dict]:
    out = []
    for line in cfg.metrics_file.read_text().splitlines():
        rec = json.loads(line)
        if rec.get("kv_adaptive") == "restore":
            out.append(rec)
    return out


# ---------------------------------------------------------------------------
# chunk hashing
# ---------------------------------------------------------------------------


def test_chunk_hashes_full_chunks_only_and_chained():
    a = chunk_hashes("x" * 250, 100)
    assert len(a) == 2  # trailing 50-char partial chunk is ignored
    b = chunk_hashes("y" + "x" * 249, 100)
    # A change in chunk 0 changes EVERY later hash (chained), even though
    # chunk 1's own bytes are identical -- so a leading run implies a prefix.
    assert a[0] != b[0] and a[1] != b[1]
    assert common_chunks(a, b) == 0


def test_common_chunks_counts_leading_run():
    base = "".join(f"line {i}\n" for i in range(100))
    a = chunk_hashes(base + "tail-A" * 50, 32)
    b = chunk_hashes(base + "tail-B" * 50, 32)
    n = common_chunks(a, b)
    assert n == len(base) // 32
    assert common_chunks(a, a) == len(a)


# ---------------------------------------------------------------------------
# PrefixIndex
# ---------------------------------------------------------------------------


def _touch_bin(d, ph):
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{ph}.bin").write_bytes(b"kv")


def test_index_best_match_prefers_longest_then_newest(tmp_path):
    d = tmp_path / "kv"
    idx = PrefixIndex(d, 16)
    head = "H" * 160
    for ph in ("short", "long", "long2"):
        _touch_bin(d, ph)
    idx.record("short", chunk_hashes(head[:64] + "z" * 200, 16), "sid")
    idx.record("long", chunk_hashes(head + "a" * 64, 16), "sid")
    time.sleep(0.01)
    idx.record("long2", chunk_hashes(head + "b" * 64, 16), "sid")
    query = chunk_hashes(head + "c" * 64, 16)
    assert idx.best_match(query, "sid") == ("long2", 10)  # tie on length -> newest


def test_index_requires_same_serving_id_and_existing_bin(tmp_path):
    d = tmp_path / "kv"
    idx = PrefixIndex(d, 16)
    h = chunk_hashes("H" * 160, 16)
    _touch_bin(d, "a")
    idx.record("a", h, "model-a")
    assert idx.best_match(h, "model-b") is None  # other model's KV: never
    assert idx.best_match(h, "model-a", exclude="a") is None
    (d / "a.bin").unlink()
    assert idx.best_match(h, "model-a") is None  # snapshot gone -> not a candidate


def test_index_persists_and_reloads(tmp_path):
    d = tmp_path / "kv"
    h = chunk_hashes("H" * 160, 16)
    _touch_bin(d, "a")
    PrefixIndex(d, 16).record("a", h, "sid")
    assert (d / f"a{IDX_SUFFIX}").exists()

    fresh = PrefixIndex(d, 16)
    assert fresh.load_from_disk() == 1
    assert fresh.best_match(h, "sid") == ("a", 10)
    # Different chunking -> hashes are incomparable -> ignored on load.
    assert PrefixIndex(d, 32).load_from_disk() == 0

    fresh.forget("a")
    assert not (d / f"a{IDX_SUFFIX}").exists()


# ---------------------------------------------------------------------------
# KVStore restore ordering: exact -> prefix -> alias
# ---------------------------------------------------------------------------


def _kv(make_config, fake_upstream, **overrides):
    cfg = make_config(
        admin_host=fake_upstream.host,
        admin_port=fake_upstream.port,
        upstream_host=fake_upstream.host,
        upstream_port=fake_upstream.port,
        **overrides,
    )
    state = ProxyState()
    kv = KVStore(cfg, SlotAdmin(cfg), CacheRamPool(cfg), state, MetricsWriter(cfg))
    return cfg, kv, state


def test_restore_prefix_only_when_exact_skipped(make_config, fake_upstream):
    cfg, kv, state = _kv(make_config, fake_upstream)
    ok = kv.restore_blocking(0, "new", None, try_exact=False, prefix_candidate=("old", 4096, 5000))
    assert ok is True
    calls = fake_upstream.admin_calls_matching(action="restore")
    assert [c["filename"] for c in calls] == ["old.bin"]
    assert state.counters.get("kv_prefix_restores") == 1
    assert state.counters.get("kv_restores") == 0


def test_restore_exact_hit_skips_prefix(make_config, fake_upstream):
    cfg, kv, state = _kv(make_config, fake_upstream)
    kv.restore_blocking(0, "ph", "alias", prefix_candidate=("old", 4096, 5000))
    assert [c["filename"] for c in fake_upstream.admin_calls_matching(action="restore")] == ["ph.bin"]


def test_restore_exact_miss_falls_to_prefix_then_alias(make_config, fake_upstream):
    cfg, kv, state = _kv(make_config, fake_upstream)
    fake_upstream.fail_restore_filename("ph.bin")
    fake_upstream.fail_restore_filename("old.bin")
    kv.restore_blocking(0, "ph", "alias", prefix_candidate=("old", 4096, 5000))
    names = [c["filename"] for c in fake_upstream.admin_calls_matching(action="restore")]
    assert names == ["ph.bin", "old.bin", "alias.bin"]
    assert state.counters.get("kv_prefix_restore_misses") == 1


def test_save_writes_sidecar_and_ttl_evict_removes_it(make_config, fake_upstream):
    cfg, kv, state = _kv(make_config, fake_upstream, prefix_restore=True, snapshot_ttl_h=0.0001)
    fake_upstream.enable_save_files(cfg.slot_save_path)
    kv.save_now(0, "ph", None, prefix_hashes=["a", "b"], serving_id="sid")
    sidecar = cfg.slot_save_path / f"ph{IDX_SUFFIX}"
    assert sidecar.exists()
    assert json.loads(sidecar.read_text())["hashes"] == ["a", "b"]

    import os

    old = time.time() - 3600
    os.utime(cfg.slot_save_path / "ph.bin", (old, old))
    kv._last_ttl_scan = 0.0
    kv.ttl_evict()
    assert not (cfg.slot_save_path / "ph.bin").exists()
    assert not sidecar.exists()


# ---------------------------------------------------------------------------
# End-to-end: night 1 saves, night 2 (new date) restores night 1's snapshot
# ---------------------------------------------------------------------------


def test_new_tail_restores_previous_nights_snapshot(make_config, fake_upstream):
    cfg = _prefix_cfg(make_config, fake_upstream)
    night1, night2 = _worker_body("2026-09-24"), _worker_body("2026-09-25")
    ph1 = _ph(night1)
    assert ph1 != _ph(night2)  # the exact key changes nightly -- the bug
    h = _start_harness(cfg)
    try:
        assert _post_chat(h.port, night1)[0] == 200
        _wait_file(cfg.slot_save_path / f"{ph1}{IDX_SUFFIX}")
        assert fake_upstream.admin_calls_matching(action="restore") == []

        # Unrelated workload takes the slot in between (daytime traffic).
        assert _post_chat(h.port, _worker_body("x", brief="") | {
            "messages": [{"role": "system", "content": "totally different"}, {"role": "user", "content": "hi"}]
        })[0] == 200

        n_chat_before = len(fake_upstream.chat_calls)
        assert _post_chat(h.port, night2)[0] == 200
        restores = fake_upstream.admin_calls_matching(action="restore")
        assert [r["filename"] for r in restores] == [f"{ph1}.bin"]
        # Restore lands before night 2's forward (it's the cold-start of that turn).
        assert restores[0]["ts"] <= fake_upstream.chat_calls[n_chat_before]["ts"]

        recs = [r for r in _restore_records(cfg) if r.get("via_prefix")]
        assert len(recs) == 1
        assert recs[0]["prefix_source"] == ph1
        assert recs[0]["match_chars"] >= len(STABLE) - 64
        assert h.state.counters.get("kv_prefix_restores") == 1
    finally:
        _stop_harness(h)


def test_prefix_restore_off_is_unchanged_behaviour(make_config, fake_upstream):
    cfg = _prefix_cfg(make_config, fake_upstream, prefix_restore=False)
    h = _start_harness(cfg)
    try:
        _post_chat(h.port, _worker_body("2026-09-24"))
        _post_chat(h.port, _worker_body("2026-09-25"))
        assert fake_upstream.admin_calls_matching(action="restore") == []
        assert fake_upstream.apply_template_calls == []
        assert list(cfg.slot_save_path.glob(f"*{IDX_SUFFIX}")) == []
    finally:
        _stop_harness(h)


def test_short_shared_prefix_below_threshold_does_not_restore(make_config, fake_upstream):
    cfg = _prefix_cfg(make_config, fake_upstream, prefix_restore_min_chars=10_000_000)
    h = _start_harness(cfg)
    try:
        _post_chat(h.port, _worker_body("2026-09-24"))
        _wait_file(cfg.slot_save_path / f"{_ph(_worker_body('2026-09-24'))}{IDX_SUFFIX}")
        _post_chat(h.port, _worker_body("2026-09-25"))
        assert fake_upstream.admin_calls_matching(action="restore") == []
    finally:
        _stop_harness(h)


def test_model_change_blocks_cross_model_restore(make_config, fake_upstream):
    cfg = _prefix_cfg(make_config, fake_upstream)
    h = _start_harness(cfg)
    try:
        _post_chat(h.port, _worker_body("2026-09-24"))
        _wait_file(cfg.slot_save_path / f"{_ph(_worker_body('2026-09-24'))}{IDX_SUFFIX}")
        fake_upstream.replace_props({"model_path": "model-b", "uptime_s": 100.0})
        _post_chat(h.port, _worker_body("2026-09-25"))
        assert fake_upstream.admin_calls_matching(action="restore") == []
    finally:
        _stop_harness(h)


def test_apply_template_failure_degrades_to_exact_only(make_config, fake_upstream):
    cfg = _prefix_cfg(make_config, fake_upstream)
    fake_upstream.set_apply_template_status(404)  # older llama-server without the endpoint
    h = _start_harness(cfg)
    try:
        assert _post_chat(h.port, _worker_body("2026-09-24"))[0] == 200
        assert _post_chat(h.port, _worker_body("2026-09-25"))[0] == 200
        assert fake_upstream.admin_calls_matching(action="restore") == []
        assert list(cfg.slot_save_path.glob(f"*{IDX_SUFFIX}")) == []
    finally:
        _stop_harness(h)


# ---------------------------------------------------------------------------
# Review fixes (PR #17)
# ---------------------------------------------------------------------------


def _with_tools(body: dict, names: list[str]) -> dict:
    tools = [
        {"type": "function", "function": {"name": n, "description": "d", "parameters": {"type": "object"}}}
        for n in names
    ]
    return body | {"tools": tools}


def test_tools_are_sent_to_apply_template_and_part_of_the_match(make_config, fake_upstream):
    """Tools render first (Qwen-style), so a different tool set shares no prefix."""
    cfg = _prefix_cfg(make_config, fake_upstream)
    night1 = _with_tools(_worker_body("2026-09-24"), ["read_mail", "read_cal"])
    night2 = _with_tools(_worker_body("2026-09-25"), ["read_notes"])
    h = _start_harness(cfg)
    try:
        _post_chat(h.port, night1)
        _wait_file(cfg.slot_save_path / f"{_ph(night1)}{IDX_SUFFIX}")
        assert fake_upstream.apply_template_calls[0]["body"].get("tools")
        _post_chat(h.port, night2)
        assert fake_upstream.admin_calls_matching(action="restore") == []
    finally:
        _stop_harness(h)


def test_unindexed_resave_keeps_index_only_for_same_model_and_tools(make_config, fake_upstream):
    cfg, kv, state = _kv(make_config, fake_upstream, prefix_restore=True, prefix_save_min_interval_s=0.0)
    fake_upstream.enable_save_files(cfg.slot_save_path)
    sidecar = cfg.slot_save_path / f"ph{IDX_SUFFIX}"

    def seed(head="tools1"):
        kv.save_now(0, "ph", None, prefix_hashes=["a", "b"], serving_id="sid", head_id=head)

    seed()
    kv.save_now(0, "ph", None, serving_id="sid", head_id="tools1")  # same model+tools: keep
    assert sidecar.exists()
    assert kv.index.best_match(["a", "b"], "sid") == ("ph", 2)

    kv.save_now(0, "ph", None, serving_id="sid", head_id="tools2")  # tools changed: drop
    assert not sidecar.exists()

    seed()
    kv.save_now(0, "ph", None, serving_id="other-model", head_id="tools1")  # model changed: drop
    assert not sidecar.exists()

    seed()
    kv.save_now(0, "ph", None)  # model identity unknown: drop
    assert not sidecar.exists()

    seed(head=None)  # entry written before head_id existed: tools can't be checked -> drop
    kv.save_now(0, "ph", None, serving_id="sid", head_id="tools1")
    assert not sidecar.exists()


def test_failed_prefix_restore_is_recorded_cold(make_config, fake_upstream, poll_until):
    cfg = _prefix_cfg(make_config, fake_upstream)
    night1, night2 = _worker_body("2026-09-24"), _worker_body("2026-09-25")
    h = _start_harness(cfg)
    try:
        _post_chat(h.port, night1)
        _wait_file(cfg.slot_save_path / f"{_ph(night1)}{IDX_SUFFIX}")
        _post_chat(h.port, _worker_body("x") | {
            "messages": [{"role": "system", "content": "totally different"}, {"role": "user", "content": "hi"}]
        })
        fake_upstream.fail_restore_filename(f"{_ph(night1)}.bin")
        _post_chat(h.port, night2)
        assert h.state.counters.get("kv_prefix_restore_misses") == 1

        def night2_turns():
            # The per-turn record is written post-relay, after the client
            # already has its response -- poll for it.
            recs = [json.loads(line) for line in cfg.metrics_file.read_text().splitlines()]
            return [r for r in recs if r.get("prefix") == _ph(night2) and "served" in r]

        turns = poll_until(night2_turns, 3.0)
        assert turns[-1]["served"] == "cold"
    finally:
        _stop_harness(h)


def test_warm_follow_up_inside_rate_limit_skips_render(make_config, fake_upstream):
    cfg = _prefix_cfg(make_config, fake_upstream, prefix_save_min_interval_s=300.0)
    body = _worker_body("2026-09-24")
    h = _start_harness(cfg)
    try:
        _post_chat(h.port, body)
        _wait_file(cfg.slot_save_path / f"{_ph(body)}{IDX_SUFFIX}")
        n = len(fake_upstream.apply_template_calls)
        _post_chat(h.port, body)  # same slot, same prefix -> warm; save rate-limited
        assert len(fake_upstream.apply_template_calls) == n
    finally:
        _stop_harness(h)


def test_model_id_fallback_is_cached(make_config, fake_upstream):
    from slotpin.reload_detect import ReloadDetector

    cfg = make_config(
        admin_host=fake_upstream.host, admin_port=fake_upstream.port, reload_poll_s=60.0
    )
    admin = SlotAdmin(cfg)
    calls = []
    real_props = admin.props

    def counting_props():
        calls.append(1)
        return real_props()

    admin.props = counting_props  # type: ignore[method-assign]
    rd = ReloadDetector(cfg, admin, CacheRamPool(cfg), ProxyState())  # poller not started
    assert rd.current_model_id() == "model-a"
    assert rd.current_model_id() == "model-a"
    assert len(calls) == 1
    fake_upstream.replace_props({"model_path": "model-b", "uptime_s": 1.0})
    assert rd.fresh_model_id() == "model-b"  # the pre-restore check bypasses the cache


# ---------------------------------------------------------------------------
# Index durability (every worker index was once lost)
# ---------------------------------------------------------------------------


def test_truncated_relay_is_not_saved_and_slot_marked_unknown(make_config, fake_upstream):
    cfg = _prefix_cfg(make_config, fake_upstream)
    body = _worker_body("2026-09-24") | {"stream": True}
    fake_upstream.set_truncate_stream(True)
    h = _start_harness(cfg)
    try:
        try:
            _post_chat(h.port, body)
        except Exception:
            pass  # the client sees a cut-off reply; that's the point
        end = time.monotonic() + 3
        while time.monotonic() < end:  # wait for the handler to finish the turn
            with h.state.lock:
                if not h.state.slot_busy.get(0, False) and 0 in h.state.slot_last:
                    break
            time.sleep(0.02)
        assert fake_upstream.admin_calls_matching(action="save") == []
        assert h.state.counters.get("turns") == 0
        with h.state.lock:
            assert h.state.slot_last.get(0) is None  # contents unknown, not prev_prefix
    finally:
        _stop_harness(h)


def test_render_failure_is_logged(make_config, fake_upstream, caplog):
    import logging

    cfg = _prefix_cfg(make_config, fake_upstream)
    fake_upstream.set_apply_template_status(500)
    h = _start_harness(cfg)
    try:
        with caplog.at_level(logging.WARNING, logger="slotpin.slot_admin"):
            assert _post_chat(h.port, _worker_body("2026-09-24"))[0] == 200
        assert any("/apply-template returned 500" in r.getMessage() for r in caplog.records)
    finally:
        _stop_harness(h)


def test_turn1_index_survives_later_unrenderable_turn(make_config, fake_upstream):
    """The 09-26 failure end to end: turn 1 indexed, turn 2 can't render and
    re-saves the same ph -> the turn-1 index must still find the snapshot."""
    cfg = _prefix_cfg(make_config, fake_upstream)
    night1 = _worker_body("2026-09-24")
    h = _start_harness(cfg)
    try:
        _post_chat(h.port, night1)
        _wait_file(cfg.slot_save_path / f"{_ph(night1)}{IDX_SUFFIX}")
        fake_upstream.set_apply_template_status(500)
        turn2 = night1 | {"messages": night1["messages"] + [
            {"role": "assistant", "content": "calling a tool"},
            {"role": "user", "content": "tool result " * 200},
        ]}
        n_saves = len(fake_upstream.admin_calls_matching(action="save"))
        _post_chat(h.port, turn2)
        end = time.monotonic() + 3
        while len(fake_upstream.admin_calls_matching(action="save")) == n_saves and time.monotonic() < end:
            time.sleep(0.02)
        assert len(fake_upstream.admin_calls_matching(action="save")) > n_saves  # re-save happened
        assert (cfg.slot_save_path / f"{_ph(night1)}{IDX_SUFFIX}").exists()
    finally:
        _stop_harness(h)


def _raw_stream_then_disconnect(port: int, body: dict, read_bytes: int = 64) -> None:
    """Send a streaming chat request on a raw socket, read a little, then close."""
    import socket

    payload = json.dumps(body).encode()
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.sendall(
        b"POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n"
        + f"Content-Length: {len(payload)}\r\n\r\n".encode()
        + payload
    )
    if read_bytes:
        s.recv(read_bytes)
    else:
        time.sleep(1.0)  # request is in flight upstream ("mid-prefill")
    s.close()


def test_client_disconnect_on_chunked_stream_is_not_saved(make_config, fake_upstream, caplog):
    """The production shape: llama-server streams chunked; the worker is killed
    mid-reply (usually an RST, since response bytes are unread). The watcher must
    abort the upstream promptly and the turn must not be saved."""
    import logging
    import threading as _t

    cfg = _prefix_cfg(make_config, fake_upstream, upstream_timeout_s=60.0)
    hold = _t.Event()
    fake_upstream.set_chunked_hold(hold)
    h = _start_harness(cfg)
    try:
        with caplog.at_level(logging.INFO, logger="slotpin.server"):
            t0 = time.monotonic()
            _raw_stream_then_disconnect(h.port, _worker_body("2026-09-24") | {"stream": True})
            _settled(h, deadline=10.0)
            elapsed = time.monotonic() - t0
        assert elapsed < 8.0, "turn only ended via timeout -- the watcher never aborted"
        # Chunks are relayed as they arrive, so the relay's own write can hit
        # the closed client before the watcher does; either path must abort.
        assert any(
            "disconnect-watcher" in r.getMessage() or "relay ended early" in r.getMessage()
            for r in caplog.records
        )
        assert fake_upstream.admin_calls_matching(action="save") == []
        assert h.state.counters.get("turns") == 0
        with h.state.lock:
            assert h.state.slot_last.get(0) is None
    finally:
        hold.set()
        _stop_harness(h)


def _settled(h, slot=0, deadline=3.0):
    end = time.monotonic() + deadline
    while time.monotonic() < end:  # slot_last settles in the handler's finally
        with h.state.lock:
            if not h.state.slot_busy.get(slot, False):
                return
        time.sleep(0.02)


def test_503_rejection_keeps_slot_record_but_other_errors_mark_unknown(make_config, fake_upstream):
    cfg = _prefix_cfg(make_config, fake_upstream)
    other = _worker_body("x") | {
        "messages": [{"role": "system", "content": "other"}, {"role": "user", "content": "hi"}]
    }
    h = _start_harness(cfg)
    try:
        first = _worker_body("2026-09-24")
        _post_chat(h.port, first)
        _wait_file(cfg.slot_save_path / f"{_ph(first)}{IDX_SUFFIX}")
        fake_upstream.set_chat_status(503)  # loading: never touched the slot
        _post_chat(h.port, other)
        _settled(h)
        with h.state.lock:
            assert h.state.slot_last.get(0) == _ph(first)
        fake_upstream.set_chat_status(500)  # may have failed mid-prefill
        _post_chat(h.port, other)
        _settled(h)
        with h.state.lock:
            assert h.state.slot_last.get(0) is None
    finally:
        _stop_harness(h)


def test_stream_terminal_and_error_frame_detection():
    from slotpin.server import _stream_errored, _stream_terminated

    assert _stream_terminated(b'data: {"choices":[]}\n\ndata: [DONE]\n\n')
    assert _stream_terminated(b'data: {"content":"","stop":true}\n\n')
    assert not _stream_terminated(b'data: {"choices":[{"delta":{"content":"hi"}}]}\n\n')
    assert _stream_errored(b'data: {"error":{"code":500,"message":"decode failed"}}\n\n')
    assert _stream_errored(b'data: {"choices":[]}\n\nerror: {"code":500,"message":"x"}\n\n')
    # error text inside generated content is JSON-escaped -> not an error frame
    assert not _stream_errored(b'data: {"choices":[{"delta":{"content":"{\\"error\\": 1}"}}]}\n\n')


def test_in_stream_error_frame_is_not_saved(make_config, fake_upstream):
    cfg = _prefix_cfg(make_config, fake_upstream)
    fake_upstream.set_stream_error(True)
    h = _start_harness(cfg)
    try:
        body = _worker_body("2026-09-24") | {"stream": True}
        try:
            _post_chat(h.port, body)
        except Exception:
            pass
        _settled(h)
        assert fake_upstream.admin_calls_matching(action="save") == []
        with h.state.lock:
            assert h.state.slot_last.get(0) is None
    finally:
        _stop_harness(h)


def test_client_killed_mid_prefill_before_headers_is_aborted(make_config, fake_upstream, caplog):
    """llama-server withholds streaming headers until prefill completes. A client
    killed during that window must be caught while the proxy is still waiting
    for the response (watcher runs from request-sent), not minutes later."""
    import logging

    cfg = _prefix_cfg(make_config, fake_upstream, upstream_timeout_s=60.0)
    fake_upstream.set_chat_delay(20.0)  # "prefill": no headers for 20s
    h = _start_harness(cfg)
    try:
        with caplog.at_level(logging.INFO, logger="slotpin.server"):
            t0 = time.monotonic()
            _raw_stream_then_disconnect(h.port, _worker_body("2026-09-24") | {"stream": True}, read_bytes=0)
            _settled(h, deadline=12.0)
            elapsed = time.monotonic() - t0
        assert elapsed < 8.0, "the watcher didn't abort while waiting for headers"
        assert any("disconnect-watcher" in r.getMessage() for r in caplog.records)
        assert fake_upstream.admin_calls_matching(action="save") == []
        with h.state.lock:
            assert h.state.slot_last.get(0) is None
    finally:
        _stop_harness(h)


def test_aborted_turn_holds_slot_until_upstream_idle(make_config, fake_upstream):
    """llama-server keeps a killed request's slot busy until it notices (~2 min
    in the lab). The proxy must not hand that slot to the next turn meanwhile."""
    cfg = _prefix_cfg(make_config, fake_upstream, upstream_timeout_s=60.0)
    fake_upstream.set_chat_delay(20.0)
    h = _start_harness(cfg)
    try:
        fake_upstream.set_slots_busy_for(4.5)  # upstream still "processing" for 4.5s
        t0 = time.monotonic()
        _raw_stream_then_disconnect(h.port, _worker_body("2026-09-24") | {"stream": True}, read_bytes=0)
        time.sleep(3.0)  # watcher has aborted (~1.5s) but upstream still busy
        with h.state.lock:
            assert h.state.slot_busy.get(0) is True, "slot released while upstream still busy"
        _settled(h, deadline=10.0)
        assert time.monotonic() - t0 >= 4.0
        with h.state.lock:
            assert h.state.slot_busy.get(0) is False
    finally:
        _stop_harness(h)


def test_declared_length_overrides_stream_markers():
    from slotpin.server import _upstream_ended

    class _Resp:
        def __init__(self, length):
            self.length = length

    body = b'{"content":"x","stop":true,'  # truncated non-streaming /completion
    assert _upstream_ended(_Resp(40), body) is False
    assert _upstream_ended(_Resp(0), body) is True
    assert _upstream_ended(_Resp(None), b"data: [DONE]\n\n") is True


def test_error_frame_not_matched_at_window_edge():
    from slotpin.server import _stream_errored

    long_stream = b'data: {"choices":[{"delta":{"content":"a"}}]}\n\n' * 400  # > 16 KiB
    assert not _stream_errored(long_stream[-16384:][:-10] + b"error: {x}")  # mid-content
    assert _stream_errored(long_stream + b'\n\nerror: {"code":500}\n\n')
