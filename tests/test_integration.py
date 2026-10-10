"""The full-path integration test: a real ProxyServer, threaded, talking to
FakeUpstream, exercising the chat pipeline end to end.

This is the full-path test: it drives real HTTP through a running ProxyServer rather than
calling any Handler internals directly.
"""

from __future__ import annotations

import http.client
import json
import threading
import time
from dataclasses import dataclass

from slotpin.classify import Classifier
from slotpin.kvstore import KVStore
from slotpin.metrics import MetricsWriter
from slotpin.pool import CacheRamPool
from slotpin.reload_detect import ReloadDetector
from slotpin.scheduler import Scheduler
from slotpin.server import ProxyServer
from slotpin.slot_admin import SlotAdmin
from slotpin.state import ProxyState

REFERENCE_LANES = [
    {
        "name": "interactive",
        "match": ["channel=chat", "## Execution Bias"],
        "tier": 0,
        "interactive": True,
        "demote_to": "default",
    },
    {"name": "bulk", "match": ["BULKTAG"], "tier": 2},
]


@dataclass
class Harness:
    cfg: object
    server: ProxyServer
    state: ProxyState
    pool: CacheRamPool
    kv: KVStore
    sched: Scheduler
    metrics: MetricsWriter
    admin: SlotAdmin
    thread: threading.Thread

    @property
    def port(self) -> int:
        return self.server.port


def _start_harness(cfg) -> Harness:
    classifier = Classifier(cfg)
    state = ProxyState()
    pool = CacheRamPool(cfg)
    admin = SlotAdmin(cfg)
    metrics = MetricsWriter(cfg)
    kv = KVStore(cfg, admin, pool, state, metrics)
    sched = Scheduler(cfg, state)
    reload_detector = ReloadDetector(cfg, admin, pool, state)
    server = ProxyServer(
        cfg,
        classifier=classifier,
        kv=kv,
        sched=sched,
        pool=pool,
        state=state,
        metrics=metrics,
        admin=admin,
        reload_detector=reload_detector,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True, name="test-proxy")
    thread.start()
    h = Harness(cfg, server, state, pool, kv, sched, metrics, admin, thread)
    _wait_ready(h)
    return h


def _wait_ready(h: Harness, deadline: float = 3.0) -> None:
    import time

    start = time.monotonic()
    while True:
        try:
            status, _ = _get(h.port, "/ready")
            if status == 200:
                return
        except (ConnectionRefusedError, OSError):
            pass
        if time.monotonic() - start > deadline:
            raise AssertionError("proxy server never became ready")
        time.sleep(0.02)


def _stop_harness(h: Harness) -> None:
    h.server.shutdown()
    h.thread.join(timeout=5)


def _post_chat(port: int, body: dict, timeout: float = 5.0) -> tuple[int, dict]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        data = json.dumps(body).encode("utf-8")
        conn.request(
            "POST", "/v1/chat/completions", body=data, headers={"Content-Type": "application/json"}
        )
        resp = conn.getresponse()
        raw = resp.read()
        payload = json.loads(raw) if raw and resp.getheader("Content-Type", "").startswith(
            "application/json"
        ) else {}
        return resp.status, payload
    finally:
        conn.close()


def _post_chat_raw(port: int, body: dict, timeout: float = 5.0) -> tuple[int, bytes]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        data = json.dumps(body).encode("utf-8")
        conn.request(
            "POST", "/v1/chat/completions", body=data, headers={"Content-Type": "application/json"}
        )
        resp = conn.getresponse()
        raw = resp.read()
        return resp.status, raw
    finally:
        conn.close()


def _post_path(
    port: int, path: str, body: dict, headers: dict | None = None, timeout: float = 5.0
) -> tuple[int, bytes]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        data = json.dumps(body).encode("utf-8")
        hdrs = {"Content-Type": "application/json"}
        if headers:
            hdrs.update(headers)
        conn.request("POST", path, body=data, headers=hdrs)
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


def _get(port: int, path: str, timeout: float = 5.0) -> tuple[int, bytes]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        raw = resp.read()
        return resp.status, raw
    finally:
        conn.close()


def _reference_config(make_config, fake_upstream, **overrides):
    overrides.setdefault("lanes", REFERENCE_LANES)
    overrides.setdefault("kv_persist", True)
    overrides.setdefault("kv_adaptive", True)
    overrides.setdefault("sched_enabled", True)
    overrides.setdefault("max_inflight", 1)
    overrides.setdefault("queue_timeout_s", 5.0)
    overrides.setdefault("latest_alias", "test-latest")
    overrides.setdefault("recur_threshold", 3)
    return make_config(
        upstream_host=fake_upstream.host,
        upstream_port=fake_upstream.port,
        admin_host=fake_upstream.host,
        admin_port=fake_upstream.port,
        **overrides,
    )


def _bulk_body(marker: str, stream: bool = False) -> dict:
    return {
        "model": "x",
        "stream": stream,
        "messages": [{"role": "user", "content": f"BULKTAG {marker}"}],
    }


def _interactive_body(marker: str, stream: bool = False) -> dict:
    return {
        "model": "x",
        "stream": stream,
        "messages": [
            {"role": "system", "content": "## Execution Bias\nshared interactive system prompt"},
            {"role": "user", "content": f"channel=chat {marker}"},
        ],
    }


# ---------------------------------------------------------------------------
# (a) id_slot injected, (b) include_usage forced on streaming requests
# ---------------------------------------------------------------------------


def test_id_slot_injected_and_stream_include_usage_forced(make_config, fake_upstream):
    cfg = _reference_config(make_config, fake_upstream)
    h = _start_harness(cfg)
    try:
        status, _ = _post_chat(h.port, _bulk_body("one", stream=True))
        assert status == 200
        calls = fake_upstream.wait_for_chat_calls(1)
        body = calls[-1]["body"]
        assert body["id_slot"] == cfg.default_slot
        assert body["stream_options"]["include_usage"] is True
    finally:
        _stop_harness(h)


def test_native_completion_endpoint_is_instrumented(make_config, fake_upstream, poll_until):
    # llama.cpp native /completion (raw-prompt) must get the full pipeline and a
    # metric row, exactly like /v1/chat/completions -- benches use it heavily.
    cfg = _reference_config(make_config, fake_upstream)
    h = _start_harness(cfg)
    try:
        status, _ = _post_path(
            h.port,
            "/completion",
            {"prompt": "hello", "n_predict": 8, "stream": False},
            headers={"X-Slotpin-Run-Id": "rid-comp", "X-Slotpin-Request-Id": "req-comp-1"},
        )
        assert status == 200

        # The per-turn record is written post-relay (after the HTTP response
        # returns), so poll for it rather than reading immediately.
        def _rows() -> list[dict]:
            if not cfg.metrics_file.exists():
                return []
            return [
                json.loads(line)
                for line in cfg.metrics_file.read_text().splitlines()
                if line.strip()
            ]

        poll_until(
            lambda: any(r.get("run_id") == "rid-comp" for r in _rows()), deadline_s=2.0
        )
        turns = [r for r in _rows() if r.get("run_id") == "rid-comp"]
        assert turns, "no metric row written for the native /completion turn"
        ev = turns[-1]
        assert ev["ok"] is True
        assert ev["request_id"] == "req-comp-1"
        assert ev["prompt_n"] == 10  # from top-level tokens_evaluated
        assert ev["completion_n"] == 5  # from top-level tokens_predicted
        assert ev["decode_tps"] == 20.0
        assert ev["prefill_tps"] == 500.0
        assert "config_fingerprint" in ev
        assert ev["stop_reason"] == "eos"  # native /completion stop_type
        assert ev["truncated"] is False
    finally:
        _stop_harness(h)


def test_truncated_turn_is_flagged(make_config, fake_upstream, poll_until):
    # A turn that hits the generation budget (stop_type "limit" / finish_reason
    # "length") must be flagged `truncated: True` so a benchmark harness can reject it as
    # incomplete rather than scoring a chopped answer.
    cfg = _reference_config(make_config, fake_upstream)
    fake_upstream.state.completion_stop_type = "limit"
    h = _start_harness(cfg)
    try:
        _post_path(
            h.port,
            "/completion",
            {"prompt": "x", "n_predict": 4, "stream": False},
            headers={"X-Slotpin-Run-Id": "trunc", "X-Slotpin-Request-Id": "t1"},
        )

        def _rows() -> list[dict]:
            if not cfg.metrics_file.exists():
                return []
            return [
                json.loads(line)
                for line in cfg.metrics_file.read_text().splitlines()
                if line.strip()
            ]

        poll_until(lambda: any(r.get("run_id") == "trunc" for r in _rows()), deadline_s=2.0)
        ev = next(r for r in _rows() if r.get("run_id") == "trunc")
        assert ev["stop_reason"] == "limit"
        assert ev["truncated"] is True
    finally:
        _stop_harness(h)


def test_config_fingerprint_is_endpoint_independent(make_config, fake_upstream, poll_until):
    # Serving contract: chat, /completion and /v1/completions on the SAME serving
    # config must produce the same config_fingerprint (endpoint is not part of
    # the serving identity), while upstream_build is recorded separately and is
    # null-safe (present on chat, absent on native /completion here).
    cfg = _reference_config(make_config, fake_upstream)
    h = _start_harness(cfg)
    try:
        _post_path(
            h.port,
            "/v1/chat/completions",
            {"messages": [{"role": "user", "content": "hi"}], "stream": False},
            headers={"X-Slotpin-Run-Id": "cfp", "X-Slotpin-Request-Id": "chat-1"},
        )
        _post_path(
            h.port,
            "/completion",
            {"prompt": "hi", "n_predict": 8, "stream": False},
            headers={"X-Slotpin-Run-Id": "cfp", "X-Slotpin-Request-Id": "comp-1"},
        )

        def _rows() -> list[dict]:
            if not cfg.metrics_file.exists():
                return []
            return [
                json.loads(line)
                for line in cfg.metrics_file.read_text().splitlines()
                if line.strip()
            ]

        poll_until(
            lambda: len([r for r in _rows() if r.get("run_id") == "cfp"]) >= 2, deadline_s=2.0
        )
        by_req = {r["request_id"]: r for r in _rows() if r.get("run_id") == "cfp"}
        chat, comp = by_req["chat-1"], by_req["comp-1"]
        # endpoint-independent identity
        assert chat["config_fingerprint"] == comp["config_fingerprint"]
        assert chat["fingerprint_version"] == comp["fingerprint_version"] == 2
        # build recorded separately, null-safe
        assert chat["upstream_build"] == "fake-build"
        assert comp["upstream_build"] is None
    finally:
        _stop_harness(h)


def test_config_fingerprint_distinguishes_plain_vs_mtp(make_config, fake_upstream, poll_until):
    # Same model + capacity, different upstream_config_id -> different fingerprint.
    def _fp(config_id: str) -> str:
        cfg = _reference_config(make_config, fake_upstream, upstream_config_id=config_id)
        h = _start_harness(cfg)
        try:
            _post_path(
                h.port,
                "/completion",
                {"prompt": "hi", "n_predict": 8, "stream": False},
                headers={"X-Slotpin-Run-Id": config_id, "X-Slotpin-Request-Id": "r1"},
            )

            def _rows() -> list[dict]:
                if not cfg.metrics_file.exists():
                    return []
                return [
                    json.loads(line)
                    for line in cfg.metrics_file.read_text().splitlines()
                    if line.strip()
                ]

            poll_until(
                lambda: any(r.get("run_id") == config_id for r in _rows()), deadline_s=2.0
            )
            return next(r for r in _rows() if r.get("run_id") == config_id)["config_fingerprint"]
        finally:
            _stop_harness(h)

    assert _fp("qwen36-plain") != _fp("qwen36-mtp-n2")


def test_non_streaming_request_does_not_force_include_usage(make_config, fake_upstream):
    cfg = _reference_config(make_config, fake_upstream)
    h = _start_harness(cfg)
    try:
        status, _ = _post_chat(h.port, _bulk_body("two", stream=False))
        assert status == 200
        calls = fake_upstream.wait_for_chat_calls(1)
        body = calls[-1]["body"]
        assert "stream_options" not in body
    finally:
        _stop_harness(h)


# ---------------------------------------------------------------------------
# (c)/(d)/(e)/(f)/(h)/(i): the full concurrency + warmth story
# ---------------------------------------------------------------------------


def test_full_pipeline_ordering_and_warmth(make_config, fake_upstream, poll_until):
    cfg = _reference_config(make_config, fake_upstream)
    h = _start_harness(cfg)
    try:
        hold = threading.Event()
        fake_upstream.set_chat_hold(hold)

        results: dict[str, tuple[int, dict]] = {}

        def send(name: str, body: dict) -> None:
            results[name] = _post_chat(h.port, body)

        # A: bulk (tier 2) occupies the single inflight slot and is held at
        # the upstream so it stays "in flight" while we queue the others.
        t_a = threading.Thread(target=send, args=("A", _bulk_body("first")))
        t_a.start()
        fake_upstream.wait_for_chat_calls(1)  # A reached upstream => it was admitted

        # B: a second bulk (tier 2) request queues behind A.
        t_b = threading.Thread(target=send, args=("B", _bulk_body("second")))
        t_b.start()
        poll_until(
            lambda: json.loads(_get(h.port, "/_stats")[1])["scheduler"]["tiers"]
            .get("2", {})
            .get("queued")
            == 1
            or None,
            deadline_s=3.0,
        )

        # C: the interactive (tier 0) cold turn queues too -- higher priority
        # than B, and must NOT have triggered a restore yet (invariant 2:
        # restore only runs post-admission).
        t_c = threading.Thread(target=send, args=("C", _interactive_body("hello")))
        t_c.start()
        poll_until(
            lambda: json.loads(_get(h.port, "/_stats")[1])["scheduler"]["tiers"]
            .get("0", {})
            .get("queued")
            == 1
            or None,
            deadline_s=3.0,
        )
        assert fake_upstream.admin_calls_matching(action="restore") == []

        # Release A. C (tier 0) must be admitted next, ahead of the
        # earlier-queued B (tier 2), and its restore must land AFTER
        # admission but BEFORE its own chat request reaches upstream.
        hold.set()

        fake_upstream.wait_for_chat_calls(2)  # C's turn reaching upstream
        restore_calls = fake_upstream.wait_for_admin_calls(1)
        assert restore_calls[0]["action"] == "restore"

        t_a.join(timeout=5)
        t_c.join(timeout=5)
        t_b.join(timeout=5)

        assert results["A"][0] == 200
        assert results["C"][0] == 200
        assert results["B"][0] == 200

        chat_calls = fake_upstream.chat_calls
        assert len(chat_calls) == 3
        # (h) tier-0 (C) admitted next, ahead of the earlier-queued tier-2 (B).
        second_body = chat_calls[1]["body"]
        assert "channel=chat" in json.dumps(second_body)
        third_body = chat_calls[2]["body"]
        assert "BULKTAG second" in json.dumps(third_body)

        # (c) restore-before-forward ordering for C specifically.
        restore_ts = restore_calls[0]["ts"]
        c_chat_ts = chat_calls[1]["ts"]
        assert restore_ts <= c_chat_ts

        # (d) a save fires after the turn (interactive lane is warm-worthy
        # instantly, kv_persist/kv_adaptive are on). Allow enough deadline
        # for kvstore's busy-slot retry (a fixed ~2s) in case B has already
        # grabbed the shared slot by the time C's post-relay save fires.
        save_calls = fake_upstream.wait_for_admin_calls(2, deadline=4.0)  # restore + >=1 save
        assert any(c["action"] == "save" for c in save_calls)

        # (e) per-turn metrics line written with route/tier/served fields.
        metrics_lines = cfg.metrics_file.read_text().splitlines()
        turn_events = [json.loads(line) for line in metrics_lines if "route" in json.loads(line)]
        interactive_events = [e for e in turn_events if e["route"] == "interactive"]
        assert interactive_events, "expected a metrics line for the interactive turn"
        ev = interactive_events[-1]
        for key in ("route", "tier", "served", "slot", "queue_wait_ms", "ok"):
            assert key in ev
        assert ev["tier"] == 0
        # Canonical request-success signal: a per-turn record is written iff the
        # turn was served 2xx, so `ok` is True and is what consumers/gates key
        # on -- NOT `served`, which is KV warmth.
        assert ev["ok"] is True
        assert ev["served"] in ("warm", "cold")
        # Warm iff a disk restore fired OR cache-ram
        # covered it. This turn triggered a deferred restore -> "warm" (the
        # restore is what disk-warmth exists to provide; "cold" means no
        # restore happened at all and the prompt was fully re-prefilled).
        assert ev["served"] == "warm"

        # (f) /_stats coherent counters.
        stats_status, stats_raw = _get(h.port, "/_stats")
        assert stats_status == 200
        stats = json.loads(stats_raw)
        assert stats["counters"]["turns"] >= 3
    finally:
        _stop_harness(h)


def test_second_identical_interactive_turn_is_warm_no_second_restore(make_config, fake_upstream):
    # (i) Isolated from any other-lane traffic: since mode="single" puts every
    # lane on the same physical slot, intervening non-interactive turns would
    # legitimately evict the interactive session's warmth (a different,
    # correct behaviour) -- so this check uses back-to-back identical
    # interactive turns only, with nothing else touching the slot.
    cfg = _reference_config(make_config, fake_upstream)
    h = _start_harness(cfg)
    try:
        status1, _ = _post_chat(h.port, _interactive_body("hello"))
        assert status1 == 200
        fake_upstream.wait_for_chat_calls(1)
        fake_upstream.wait_for_admin_calls(1)
        assert len(fake_upstream.admin_calls_matching(action="restore")) == 1

        status2, _ = _post_chat(h.port, _interactive_body("hello-again"))
        assert status2 == 200
        fake_upstream.wait_for_chat_calls(2)

        assert len(fake_upstream.admin_calls_matching(action="restore")) == 1
    finally:
        _stop_harness(h)


# ---------------------------------------------------------------------------
# kv_persist is the master gate for ANY disk-warmth activity (every
# restore/save path requires it). An interactive-lane request must not attempt
# a restore with kv_persist=False, and the adaptive save branch requires
# kv_persist AND kv_adaptive.
# ---------------------------------------------------------------------------


def test_kv_persist_false_disables_restore_even_for_interactive_lane(make_config, fake_upstream):
    cfg = _reference_config(make_config, fake_upstream, kv_persist=False)
    h = _start_harness(cfg)
    try:
        status, _ = _post_chat(h.port, _interactive_body("hello"))
        assert status == 200
        fake_upstream.wait_for_chat_calls(1)
        assert fake_upstream.admin_calls_matching(action="restore") == []
    finally:
        _stop_harness(h)


def test_kv_persist_false_disables_save_even_with_kv_adaptive_true(
    make_config, fake_upstream, poll_until
):
    cfg = _reference_config(make_config, fake_upstream, kv_persist=False, kv_adaptive=True)
    h = _start_harness(cfg)
    try:
        for i in range(3):  # >= recur_threshold, would be warm-worthy if kv_persist were on
            status, _ = _post_chat(h.port, _bulk_body(f"turn-{i}"))
            assert status == 200
        fake_upstream.wait_for_chat_calls(3)
        import time as _time

        _time.sleep(0.2)  # give any (incorrect) async save a chance to fire
        assert fake_upstream.admin_calls_matching(action="save") == []
    finally:
        _stop_harness(h)


# ---------------------------------------------------------------------------
# passthrough + upstream-down behaviour (dedicated harnesses: these tests
# shut down / never rely on the shared FakeUpstream instance staying alive)
# ---------------------------------------------------------------------------


def test_non_chat_get_passes_through_transparently(make_config, fake_upstream):
    cfg = _reference_config(make_config, fake_upstream)
    h = _start_harness(cfg)
    try:
        status, raw = _get(h.port, "/v1/models")
        assert status == 404  # FakeUpstream's do_GET default response
        payload = json.loads(raw)
        assert payload["error"] == "not_found"
        # Passthrough never touches the chat/admin pipeline.
        assert fake_upstream.chat_calls == []
        assert fake_upstream.admin_calls == []
    finally:
        _stop_harness(h)


def test_health_ok_when_upstream_reachable(make_config, fake_upstream):
    cfg = _reference_config(make_config, fake_upstream)
    h = _start_harness(cfg)
    try:
        status, raw = _get(h.port, "/health")
        assert status == 200
        assert json.loads(raw) == {"status": "ok", "local_available": True}
    finally:
        _stop_harness(h)


def test_health_flips_to_503_when_upstream_down(make_config, fake_upstream):
    cfg = _reference_config(make_config, fake_upstream)
    h = _start_harness(cfg)
    try:
        fake_upstream.shutdown()
        status, raw = _get(h.port, "/health")
        assert status == 503
        assert json.loads(raw) == {"status": "degraded", "local_available": False}
    finally:
        _stop_harness(h)


def test_upstream_down_chat_returns_502_json(make_config, fake_upstream):
    cfg = _reference_config(make_config, fake_upstream)
    h = _start_harness(cfg)
    try:
        fake_upstream.shutdown()
        status, payload = _post_chat(h.port, _bulk_body("whatever"))
        assert status == 502
        assert payload == {"error": "upstream_unreachable", "local_available": False}
    finally:
        _stop_harness(h)


# ---------------------------------------------------------------------------
# v8: end-to-end pooled concurrency -- two held turns get distinct slots and
# both slots are freed afterwards (audit's top test gap)
# ---------------------------------------------------------------------------


def test_pooled_concurrency_distinct_slots_and_cleanup(make_config, fake_upstream, poll_until):
    cfg = _reference_config(make_config, fake_upstream, slots=2, max_inflight=2)
    h = _start_harness(cfg)
    hold = threading.Event()
    results: list = []
    results_lock = threading.Lock()

    def fire(marker: str) -> None:
        r = _post_chat(h.port, _bulk_body(marker))
        with results_lock:
            results.append(r)

    try:
        fake_upstream.set_chat_hold(hold)  # upstream blocks each chat until released
        t1 = threading.Thread(target=fire, args=("c1",))
        t2 = threading.Thread(target=fire, args=("c2",))
        t1.start()
        t2.start()

        # Both turns admitted concurrently and held at the upstream -> two live
        # chat calls, each pinned to a DISTINCT pooled slot.
        calls = fake_upstream.wait_for_chat_calls(2)
        slots = sorted(c["body"]["id_slot"] for c in calls[-2:])
        assert slots == [0, 1]

        hold.set()  # release both
        t1.join(timeout=5)
        t2.join(timeout=5)
        assert len(results) == 2 and all(status == 200 for status, _ in results)

        # Every slot freed after completion (no leak / no stuck busy flag).
        poll_until(
            lambda: (not any(h.state.slot_busy.get(s, False) for s in (0, 1))) or None,
            deadline_s=2.0,
        )
        assert not any(h.state.slot_busy.get(s, False) for s in (0, 1))
    finally:
        fake_upstream.set_chat_hold(None)
        _stop_harness(h)


# ---------------------------------------------------------------------------
# Streamed chunks are relayed as they arrive, not batched into 8 KB reads
# ---------------------------------------------------------------------------


def test_stream_chunks_relay_before_upstream_finishes(make_config, fake_upstream):
    cfg = _reference_config(make_config, fake_upstream)
    hold = threading.Event()
    fake_upstream.set_chunked_hold(hold)
    h = _start_harness(cfg)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", h.port, timeout=5.0)
        conn.request(
            "POST", "/v1/chat/completions", body=json.dumps(_bulk_body("relay", stream=True)),
            headers={"Content-Type": "application/json"},
        )
        resp = conn.getresponse()
        # The upstream has sent one small chunk and is now waiting on `hold`.
        first = resp.read1(65536)
        assert b'"content": "hi"' in first, "first chunk was held until the upstream finished"
        hold.set()
        rest = resp.read()
        assert b"[DONE]" in first + rest
        conn.close()
    finally:
        hold.set()
        _stop_harness(h)


def test_ttft_waits_for_the_first_output_delta(make_config, fake_upstream, poll_until):
    # Gufo streams a role-only event before prefill; TTFT must not count it.
    cfg = _reference_config(make_config, fake_upstream)
    fake_upstream.set_role_first_delay(0.6)
    h = _start_harness(cfg)
    try:
        status, _ = _post_chat(h.port, _bulk_body("ttft", stream=True))
        assert status == 200

        def _rows() -> list[dict]:
            if not cfg.metrics_file.exists():
                return []
            return [json.loads(x) for x in cfg.metrics_file.read_text().splitlines() if x.strip()]

        poll_until(lambda: any(r.get("ttft_s") is not None for r in _rows()), deadline_s=3.0)
        ttft = [r for r in _rows() if r.get("ttft_s") is not None][-1]["ttft_s"]
        assert ttft >= 0.5, f"ttft {ttft} s counted the role-only event"
    finally:
        fake_upstream.set_role_first_delay(0.0)
        _stop_harness(h)



# ---------------------------------------------------------------------------
# turn_failed: one record for every turn that has no per-turn record
# ---------------------------------------------------------------------------


def _metric_rows(cfg) -> list[dict]:
    if not cfg.metrics_file.exists():
        return []
    return [json.loads(ln) for ln in cfg.metrics_file.read_text().splitlines() if ln.strip()]


def _failed_rows(cfg) -> list[dict]:
    return [r for r in _metric_rows(cfg) if r.get("event") == "turn_failed"]


def _assert_failed_record(cfg, poll_until, reason, upstream_status=None) -> dict:
    poll_until(lambda: _failed_rows(cfg), deadline_s=5.0)
    time.sleep(0.2)  # a duplicate write would land here
    rows = _failed_rows(cfg)
    assert len(rows) == 1, rows
    rec = rows[0]
    assert rec["reason"] == reason
    assert rec["route"] in ("interactive", "bulk")
    assert len(rec["prefix"]) == 8
    assert rec["dur_s"] >= 0
    assert rec.get("upstream_status") == upstream_status
    assert "prompt_n" not in rec
    assert "BULKTAG" not in json.dumps(rec)  # no prompt text
    assert not [r for r in _metric_rows(cfg) if "ok" in r and "event" not in r]
    return rec


def test_turn_failed_unreachable(make_config, fake_upstream, poll_until):
    cfg = _reference_config(make_config, fake_upstream)
    h = _start_harness(cfg)
    try:
        fake_upstream.shutdown()
        assert _post_chat(h.port, _bulk_body("whatever"))[0] == 502
        rec = _assert_failed_record(cfg, poll_until, "unreachable")
        assert rec["slot"] == 0 and rec["route"] == "bulk"
    finally:
        _stop_harness(h)


def test_turn_failed_upstream_5xx(make_config, fake_upstream, poll_until):
    cfg = _reference_config(make_config, fake_upstream)
    fake_upstream.set_chat_status(500)
    h = _start_harness(cfg)
    try:
        assert _post_chat(h.port, _bulk_body("boom"))[0] == 500
        _assert_failed_record(cfg, poll_until, "upstream_5xx", upstream_status=500)
    finally:
        _stop_harness(h)


def test_turn_failed_upstream_incomplete(make_config, fake_upstream, poll_until):
    cfg = _reference_config(make_config, fake_upstream)
    fake_upstream.set_truncate_stream(True)
    h = _start_harness(cfg)
    try:
        try:
            _post_chat(h.port, _bulk_body("cut", stream=True))
        except Exception:
            pass  # the client sees a cut-off reply; that's the point
        _assert_failed_record(cfg, poll_until, "upstream_incomplete", upstream_status=200)
    finally:
        _stop_harness(h)


def test_turn_failed_timeout(make_config, fake_upstream, poll_until):
    cfg = _reference_config(make_config, fake_upstream, upstream_timeout_s=0.3)
    fake_upstream.set_chat_delay(1.5)  # headers arrive after the upstream socket timeout
    h = _start_harness(cfg)
    try:
        assert _post_chat(h.port, _bulk_body("slow"))[0] == 502
        _assert_failed_record(cfg, poll_until, "timeout")
    finally:
        _stop_harness(h)


def test_turn_failed_shed(make_config, fake_upstream, poll_until):
    cfg = _reference_config(make_config, fake_upstream, queue_timeout_s=0.3)
    hold = threading.Event()
    fake_upstream.set_chat_hold(hold)
    h = _start_harness(cfg)
    first = threading.Thread(target=lambda: _post_chat(h.port, _bulk_body("one"), timeout=20.0))
    try:
        first.start()
        fake_upstream.wait_for_chat_calls(1)
        assert _post_chat(h.port, _bulk_body("two"))[0] == 503
        rec = _assert_failed_record(cfg, poll_until, "shed")
        assert rec["slot"] is None
    finally:
        hold.set()
        first.join(timeout=10)
        _stop_harness(h)


def test_turn_failed_aborted_client(make_config, fake_upstream, poll_until):
    from test_prefix_restore import _raw_stream_then_disconnect

    cfg = _reference_config(make_config, fake_upstream, upstream_timeout_s=60.0)
    hold = threading.Event()
    fake_upstream.set_chunked_hold(hold)
    h = _start_harness(cfg)
    try:
        _raw_stream_then_disconnect(h.port, _bulk_body("gone", stream=True))
        _assert_failed_record(cfg, poll_until, "aborted_client", upstream_status=200)
    finally:
        hold.set()
        _stop_harness(h)


def test_successful_turn_writes_no_turn_failed(make_config, fake_upstream, poll_until):
    cfg = _reference_config(make_config, fake_upstream)
    h = _start_harness(cfg)
    try:
        assert _post_chat(h.port, _bulk_body("fine"))[0] == 200
        poll_until(lambda: _metric_rows(cfg), deadline_s=2.0)
        time.sleep(0.2)
        assert _failed_rows(cfg) == []
        assert all("event" not in r for r in _metric_rows(cfg))
    finally:
        _stop_harness(h)
