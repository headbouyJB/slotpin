"""upstream_kind = "gufo": slotpin in front of the Gufo engine.

Gufo owns prompt caching (in-memory + disk continuation cache), so slotpin
keeps classification, scheduling, relay and metrics and drops slot pinning,
KV save/restore and /props reload detection. The fixtures under
fixtures/gufo/ are real Gufo (main 1071b36) responses to synthetic prompts.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest
from test_integration import _post_chat, _start_harness, _stop_harness
from test_prefix_restore import _raw_stream_then_disconnect, _settled

from slotpin.config import ConfigError, load_config
from slotpin.pool import CacheRamPool
from slotpin.reload_detect import ReloadDetector
from slotpin.server import _parse_gufo_tail, _parse_usage_tail
from slotpin.slot_admin import SlotAdmin
from slotpin.state import ProxyState

FIXTURES = Path(__file__).parent / "fixtures" / "gufo"
GUFO_KEYS = {
    "upstream_cache",
    "upstream_cache_miss_reason",
    "upstream_restore_ms",
    "upstream_restore_bytes",
    "upstream_snapshot_ms",
    "upstream_snapshot_bytes",
    "upstream_disk_queued_bytes",
}


def _fx(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _metrics_text(prompt: int = 2058, predicted: int = 460, processing: int = 0) -> str:
    text = _fx("gufo_metrics.txt").decode()
    text = text.replace("llamacpp:prompt_tokens_total 2058", f"llamacpp:prompt_tokens_total {prompt}")
    text = text.replace(
        "llamacpp:tokens_predicted_total 460", f"llamacpp:tokens_predicted_total {predicted}"
    )
    return text.replace(
        "llamacpp:requests_processing 0", f"llamacpp:requests_processing {processing}"
    )


def _models(model_id: str = "fast-rocmfp4", context_length: int = 200192) -> dict:
    models = json.loads(_fx("gufo_models.json"))
    models["data"][0]["id"] = model_id
    models["data"][0]["context_length"] = context_length
    return models


def _gufo_upstream(fake_upstream) -> None:
    """Make the fake answer the probes the way Gufo does."""
    fake_upstream.set_models(_models())
    fake_upstream.set_metrics_text(_metrics_text())
    fake_upstream.set_slots_payload(json.loads(_fx("gufo_slots.json")))
    fake_upstream.set_props_status(400)  # Gufo: missing_model without ?model=


def _gufo_cfg(make_config, fake_upstream, **overrides):
    overrides.setdefault("upstream_kind", "gufo")
    overrides.setdefault("sched_enabled", True)
    return make_config(
        upstream_host=fake_upstream.host,
        upstream_port=fake_upstream.port,
        admin_host=fake_upstream.host,
        admin_port=fake_upstream.port,
        **overrides,
    )


def _body(stream: bool = True) -> dict:
    return {
        "model": "fast-rocmfp4",
        "stream": stream,
        "messages": [
            {"role": "system", "content": "You are terse."},
            {"role": "user", "content": "Name one ocean in a single word."},
        ],
    }


def _turn_records(cfg, n: int, deadline: float = 3.0) -> list[dict]:
    end = time.monotonic() + deadline
    while True:
        recs = []
        if cfg.metrics_file.exists():
            recs = [
                r
                for r in map(json.loads, cfg.metrics_file.read_text().splitlines())
                if "served" in r
            ]
        if len(recs) >= n:
            return recs
        if time.monotonic() > end:
            raise AssertionError(f"expected {n} turn records, got {len(recs)}")
        time.sleep(0.02)


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------


def test_upstream_kind_defaults_to_llamacpp():
    assert load_config(None, env={}).upstream_kind == "llamacpp"


def test_upstream_kind_gufo_and_env(tmp_path):
    path = tmp_path / "c.toml"
    path.write_text('upstream_kind = "gufo"\n')
    assert load_config(path, env={}).upstream_kind == "gufo"
    assert load_config(None, env={"SLOTPIN_UPSTREAM_KIND": "gufo"}).upstream_kind == "gufo"


def test_unknown_upstream_kind_raises(tmp_path):
    path = tmp_path / "c.toml"
    path.write_text('upstream_kind = "vllm"\n')
    with pytest.raises(ConfigError, match="upstream_kind"):
        load_config(path, env={})


@pytest.mark.parametrize("key", ["kv_persist", "kv_adaptive", "prefix_restore"])
def test_gufo_rejects_slotpin_caching(tmp_path, key):
    path = tmp_path / "c.toml"
    path.write_text(f'upstream_kind = "gufo"\n{key} = true\n')
    with pytest.raises(ConfigError, match=rf"gufo.*{key}.*owns prompt caching"):
        load_config(path, env={})


# ---------------------------------------------------------------------------
# usage.gufo parsing on the captured responses
# ---------------------------------------------------------------------------


def test_parse_cold_stream_is_a_miss():
    tail = _fx("gufo_stream_cold.sse")[-16384:]
    g = _parse_gufo_tail(tail)
    assert g == {
        "upstream_cache": "miss",
        "upstream_cache_miss_reason": "prefix_changed",
        "upstream_restore_ms": 0.0,
        "upstream_restore_bytes": 0,
        "upstream_snapshot_ms": 40.524071,
        "upstream_snapshot_bytes": 289615780,
        "upstream_disk_queued_bytes": 289623392,
    }
    u = _parse_usage_tail(tail)
    assert (u["cache_n"], u["prompt_n"], u["completion_n"]) == (0, 769, 17)
    assert (u["draft_n"], u["draft_n_accepted"]) == (18, 12)


def test_parse_warm_stream_is_a_memory_hit():
    tail = _fx("gufo_stream_warm.sse")[-16384:]
    g = _parse_gufo_tail(tail)
    assert g is not None
    assert g["upstream_cache"] == "memory"
    assert g["upstream_cache_miss_reason"] is None  # Gufo omits it on a hit
    assert g["upstream_restore_ms"] == 6.185878
    assert g["upstream_restore_bytes"] == 144829660
    assert g["upstream_snapshot_bytes"] == 0
    u = _parse_usage_tail(tail)
    assert (u["cache_n"], u["prompt_n"]) == (769, 769)


def test_parse_disk_hit():
    tail = _fx("gufo_stream_warm.sse").replace(b'"cache_disk_hit":false', b'"cache_disk_hit":true')
    assert _parse_gufo_tail(tail)["upstream_cache"] == "disk"


def test_parse_tool_call_stream():
    tail = _fx("gufo_stream_toolcall.sse")[-16384:]
    g = _parse_gufo_tail(tail)
    assert g["upstream_cache"] == "miss"
    assert g["upstream_snapshot_bytes"] == 258395388
    assert _parse_usage_tail(tail)["finish_reason"] == "tool_calls"


def test_parse_non_stream_body():
    g = _parse_gufo_tail(_fx("gufo_nonstream.json"))
    assert g["upstream_cache"] == "miss"
    assert g["upstream_cache_miss_reason"] == "prefix_changed"
    assert g["upstream_snapshot_ms"] == pytest.approx(39.182734)
    assert g["upstream_disk_queued_bytes"] == 289582432


def test_parse_without_gufo_object_is_none():
    assert _parse_gufo_tail(b'data: {"usage":{"prompt_tokens":3}}\n\ndata: [DONE]\n\n') is None


# ---------------------------------------------------------------------------
# request path + metrics through a real ProxyServer
# ---------------------------------------------------------------------------


def test_gufo_request_path_no_id_slot_and_cache_metrics(make_config, fake_upstream):
    _gufo_upstream(fake_upstream)
    cfg = _gufo_cfg(make_config, fake_upstream)
    h = _start_harness(cfg)
    try:
        fake_upstream.set_chat_raw(_fx("gufo_stream_cold.sse"))
        assert _post_chat(h.port, _body())[0] == 200
        fake_upstream.set_chat_raw(_fx("gufo_stream_warm.sse"))
        assert _post_chat(h.port, _body())[0] == 200
        cold, warm = _turn_records(cfg, 2)

        body = fake_upstream.wait_for_chat_calls(2)[-1]["body"]
        assert "id_slot" not in body
        assert body["stream_options"]["include_usage"] is True

        assert cold["served"] == "cold"
        assert cold["upstream_cache"] == "miss"
        assert cold["upstream_cache_miss_reason"] == "prefix_changed"
        assert (cold["cache_n"], cold["prompt_n"]) == (0, 769)
        assert warm["served"] == "warm"
        assert warm["upstream_cache"] == "memory"
        assert warm["upstream_restore_bytes"] == 144829660
        assert (warm["cache_n"], warm["draft_n"], warm["draft_n_accepted"]) == (769, 18, 11)
        assert warm["cacheram_restore"] is False and warm["restored_tokens"] == 0
        # Gufo owns caching: no save/restore/render calls, no /props or /slots reads.
        assert fake_upstream.admin_calls == []
        assert fake_upstream.apply_template_calls == []
        assert not [p for p in fake_upstream.get_paths if p.startswith(("/props", "/slots"))]
    finally:
        _stop_harness(h)


def test_gufo_non_stream_turn(make_config, fake_upstream):
    _gufo_upstream(fake_upstream)
    cfg = _gufo_cfg(make_config, fake_upstream)
    fake_upstream.set_stream(False)
    fake_upstream.set_chat_raw(_fx("gufo_nonstream.json"))
    h = _start_harness(cfg)
    try:
        status, payload = _post_chat(h.port, _body(stream=False))
        assert status == 200
        assert payload["choices"][0]["message"]["content"] == "Mediterranean"
        (rec,) = _turn_records(cfg, 1)
        assert rec["served"] == "cold" and rec["upstream_cache"] == "miss"
        assert rec["completion_n"] == 20 and rec["stop_reason"] == "stop"
        assert "id_slot" not in fake_upstream.chat_calls[-1]["body"]
    finally:
        _stop_harness(h)


def test_llamacpp_mode_keeps_id_slot_and_has_no_gufo_keys(make_config, fake_upstream):
    cfg = _gufo_cfg(make_config, fake_upstream, upstream_kind="llamacpp")
    h = _start_harness(cfg)
    try:
        assert _post_chat(h.port, _body())[0] == 200
        (rec,) = _turn_records(cfg, 1)
        assert fake_upstream.wait_for_chat_calls(1)[-1]["body"]["id_slot"] == 0
        assert not GUFO_KEYS & rec.keys()
    finally:
        _stop_harness(h)


def test_gufo_changes_config_fingerprint(make_config, fake_upstream):
    from slotpin.server import _config_fingerprint

    llama = _gufo_cfg(make_config, fake_upstream, upstream_kind="llamacpp")
    gufo = _gufo_cfg(make_config, fake_upstream)
    assert _config_fingerprint(llama, "m") != _config_fingerprint(gufo, "m")


# ---------------------------------------------------------------------------
# post-abort hold via /metrics requests_processing
# ---------------------------------------------------------------------------


def test_slot_processing_reads_requests_processing(make_config, fake_upstream):
    _gufo_upstream(fake_upstream)
    admin = SlotAdmin(_gufo_cfg(make_config, fake_upstream))
    assert admin.slot_processing(0) is False
    fake_upstream.set_metrics_text(_metrics_text(processing=1))
    assert admin.slot_processing(0) is True
    assert admin.slot_processing(0, others=1) is False  # the one running is another turn's
    fake_upstream.set_metrics_text(None)  # /metrics unavailable -> can't tell
    assert admin.slot_processing(0) is None


def test_gufo_aborted_turn_holds_until_upstream_idle(make_config, fake_upstream):
    _gufo_upstream(fake_upstream)
    cfg = _gufo_cfg(make_config, fake_upstream, upstream_timeout_s=60.0)
    fake_upstream.set_chat_delay(20.0)
    fake_upstream.set_metrics_text(_metrics_text(processing=1))
    h = _start_harness(cfg)
    try:
        threading.Timer(
            4.5, lambda: fake_upstream.set_metrics_text(_metrics_text(processing=0))
        ).start()
        t0 = time.monotonic()
        _raw_stream_then_disconnect(h.port, _body(), read_bytes=0)
        time.sleep(3.0)  # watcher has aborted (~1.5s) but Gufo still reports it running
        with h.state.lock:
            assert h.state.slot_busy.get(0) is True, "released while upstream still busy"
        _settled(h, deadline=10.0)
        assert time.monotonic() - t0 >= 4.0
        with h.state.lock:
            assert h.state.slot_busy.get(0) is False
    finally:
        _stop_harness(h)


# ---------------------------------------------------------------------------
# reload detection from /v1/models + /metrics
# ---------------------------------------------------------------------------


def _detector(make_config, fake_upstream):
    _gufo_upstream(fake_upstream)
    cfg = _gufo_cfg(make_config, fake_upstream)
    state, pool = ProxyState(), CacheRamPool(cfg)
    rd = ReloadDetector(cfg, SlotAdmin(cfg), pool, state)
    assert rd.check_once() is False  # baseline
    return rd, state, pool


def test_gufo_counters_advancing_is_not_a_reload(make_config, fake_upstream):
    rd, state, _ = _detector(make_config, fake_upstream)
    fake_upstream.set_metrics_text(_metrics_text(prompt=3000, predicted=900))
    assert rd.check_once() is False
    assert not [p for p in fake_upstream.get_paths if p.startswith("/props")]


@pytest.mark.parametrize("counters", [{"prompt": 12}, {"predicted": 3}])
def test_gufo_counter_regression_is_a_reload(make_config, fake_upstream, counters):
    rd, state, pool = _detector(make_config, fake_upstream)
    pool.note_eviction("some-fp", 100)
    with state.lock:
        state.slot_last[0] = "some-prefix"
    fake_upstream.set_metrics_text(_metrics_text(**counters))
    assert rd.check_once() is True
    assert pool.snapshot() == []
    with state.lock:
        assert state.slot_last[0] is None
        assert state.counters["reloads_detected"] == 1


def test_gufo_model_change_is_a_reload(make_config, fake_upstream):
    rd, _, _ = _detector(make_config, fake_upstream)
    assert rd.current_model_id() == "fast-rocmfp4"
    fake_upstream.set_models(_models("qwen38-27b"))
    assert rd.fresh_model_id() == "qwen38-27b"
    assert rd.check_once() is True
    fake_upstream.set_models(_models("qwen38-27b", context_length=65536))
    assert rd.check_once() is True


def test_gufo_unavailable_probe_is_inconclusive(make_config, fake_upstream):
    rd, _, _ = _detector(make_config, fake_upstream)
    fake_upstream.set_metrics_text(None)
    assert rd.check_once() is False
    fake_upstream.set_metrics_text(_metrics_text())
    fake_upstream.set_models(None)
    assert rd.check_once() is False
    fake_upstream.set_models(_models())
    assert rd.check_once() is False  # back, unchanged
