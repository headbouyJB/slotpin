"""Tests for _parse_usage_tail, incl. the v7 spec/MTP acceptance fields.

Acceptance stats (draft_n / draft_n_accepted) are emitted by llama-server
inside the same ``timings`` object as predicted_per_second
(server_slot_stats::to_json, only when n_draft_tokens > 0). These tests use
realistic response tails to confirm the parse recovers them and does not
confuse ``draft_n`` with ``draft_n_accepted``.
"""

from __future__ import annotations

from types import SimpleNamespace

from slotpin.server import _config_fingerprint, _parse_usage_tail


def _cfg(**kw):
    base = dict(slots=1, max_inflight=1, cache_ram_mb=2048,
                upstream_host="127.0.0.1", upstream_port=5802, upstream_config_id=None)
    base.update(kw)
    return SimpleNamespace(**base)


def test_parse_extracts_model_and_build() -> None:
    tail = (
        b'data: {"choices":[{"delta":{"content":"hi"}}],'
        b'"model":"/models/Qwen3.8.gguf","system_fingerprint":"b10707-250b6144",'
        b'"object":"chat.completion.chunk"}\n\n'
    )
    r = _parse_usage_tail(tail)
    assert r["model"] == "/models/Qwen3.8.gguf"
    assert r["system_fingerprint"] == "b10707-250b6144"


def test_parse_stop_reason_chat_and_native() -> None:
    # Chat: only the final chunk carries a quoted finish_reason; earlier
    # unquoted `null`s are skipped by the quoted-string grab.
    chat = (
        b'data: {"choices":[{"finish_reason":null,"delta":{"content":"hi"}}]}\n\n'
        b'data: {"choices":[{"finish_reason":"length"}],"usage":{"completion_tokens":8}}\n\n'
    )
    r = _parse_usage_tail(chat)
    assert r["finish_reason"] == "length"
    assert r["stop_type"] is None
    # Native /completion: stop_type, no finish_reason.
    comp = b'{"content":"x","stop_type":"eos","tokens_predicted":3}'
    r2 = _parse_usage_tail(comp)
    assert r2["stop_type"] == "eos"
    assert r2["finish_reason"] is None


def test_parse_native_completion_top_level_tokens() -> None:
    # llama.cpp native /completion (non-streamed): no `usage` object; token
    # counts at top level; `timings` block identical to chat. The metric layer
    # must record the same fields as a chat turn.
    tail = (
        b'{"content":"...","model":"/models/Qwen3.6.gguf",'
        b'"tokens_predicted":96,"tokens_evaluated":12,"tokens_cached":107,'
        b'"timings":{"cache_n":0,"prompt_n":12,"prompt_per_second":167.46,'
        b'"predicted_n":96,"predicted_per_second":63.46}}'
    )
    r = _parse_usage_tail(tail)
    assert r["prompt_n"] == 12  # evaluated 12 + cached 0
    assert r["completion_n"] == 96  # from tokens_predicted
    assert r["cache_n"] == 0  # from timings.cache_n (reuse), NOT tokens_cached=107
    assert r["decode_tps"] == 63.46
    assert r["prefill_tps"] == 167.46
    assert r["model"] == "/models/Qwen3.6.gguf"


def test_parse_native_prompt_n_is_evaluated_plus_cached() -> None:
    # A cache_prompt turn: only 2048 tokens EVALUATED this turn, 30000 reused
    # from cache. prompt_n is TOTAL prompt (== chat usage.prompt_tokens), so it
    # must be evaluated + cached = 32048, not the 2048 the native field reports.
    # (Before the fix this recorded 0, dropping context depth on native runs.)
    tail = (
        b'{"content":"x","tokens_predicted":40,"tokens_evaluated":2048,'
        b'"timings":{"cache_n":30000,"prompt_n":2048,"predicted_n":40,'
        b'"predicted_per_second":30.0}}'
    )
    r = _parse_usage_tail(tail)
    assert r["cache_n"] == 30000
    assert r["prompt_n"] == 32048  # 2048 evaluated + 30000 cached
    assert r["completion_n"] == 40


def test_parse_native_prompt_n_falls_back_to_timings_when_no_tokens_evaluated() -> None:
    # Build variant that omits top-level tokens_evaluated/tokens_predicted; the
    # counts come from the timings block. prompt_n = timings.prompt_n + cache_n,
    # completion_n = timings.predicted_n.
    tail = (
        b'{"content":"x","stop_type":"eos",'
        b'"timings":{"cache_n":10000,"prompt_n":1500,"predicted_n":64,'
        b'"predicted_per_second":28.0}}'
    )
    r = _parse_usage_tail(tail)
    assert r["prompt_n"] == 11500  # 1500 (timings.prompt_n) + 10000 cached
    assert r["completion_n"] == 64  # from timings.predicted_n


def test_parse_model_build_absent_is_none() -> None:
    r = _parse_usage_tail(b'"timings":{"predicted_per_second":10.0}')
    assert r["model"] is None
    assert r["system_fingerprint"] is None


def test_config_fingerprint_stable() -> None:
    a = _config_fingerprint(_cfg(), "m.gguf")
    assert a == _config_fingerprint(_cfg(), "m.gguf")
    assert len(a) == 16 and all(c in "0123456789abcdef" for c in a)


def test_config_fingerprint_distinguishes_serving_configs() -> None:
    # plain vs native-MTP-n2: SAME model + capacity, different upstream_config_id
    # -> MUST NOT collide (the whole reason upstream_config_id exists).
    plain = _config_fingerprint(_cfg(upstream_config_id="qwen36-plain"), "m.gguf")
    mtp = _config_fingerprint(_cfg(upstream_config_id="qwen36-mtp-n2"), "m.gguf")
    assert plain != mtp


def test_config_fingerprint_changes_on_model_or_capacity() -> None:
    base = _config_fingerprint(_cfg(), "m.gguf")
    assert base != _config_fingerprint(_cfg(slots=4, max_inflight=4), "m.gguf")
    assert base != _config_fingerprint(_cfg(cache_ram_mb=4096), "m.gguf")
    assert base != _config_fingerprint(_cfg(upstream_port=8080), "m.gguf")
    assert base != _config_fingerprint(_cfg(), "other.gguf")


def test_config_fingerprint_takes_no_endpoint_or_build() -> None:
    # The fingerprint function takes neither the request endpoint nor the build,
    # so a different endpoint or a rebuild on the same serving config cannot
    # change it -- endpoint/build independence is structural, not incidental.
    same = _config_fingerprint(_cfg(upstream_config_id="x"), "m.gguf")
    assert same == _config_fingerprint(_cfg(upstream_config_id="x"), "m.gguf")


def test_config_fingerprint_tolerates_missing_model() -> None:
    a = _config_fingerprint(_cfg(), None)
    assert len(a) == 16 and a == _config_fingerprint(_cfg(), None)


def test_parse_extracts_acceptance_from_timings() -> None:
    tail = (
        b'data: {"choices":[],'
        b'"usage":{"prompt_tokens":41,"completion_tokens":24,'
        b'"prompt_tokens_details":{"cached_tokens":3}},'
        b'"timings":{"prompt_n":41,"prompt_per_second":181.6,'
        b'"predicted_n":24,"predicted_per_second":27.7,'
        b'"draft_n":128,"draft_n_accepted":97}}\n\n'
    )
    r = _parse_usage_tail(tail)
    assert r["draft_n"] == 128
    assert r["draft_n_accepted"] == 97  # not misread as draft_n's 128
    assert r["predicted_n"] == 24
    assert r["decode_tps"] == 27.7
    assert r["prefill_tps"] == 181.6
    assert r["prompt_n"] == 41
    assert r["cache_n"] == 3


def test_parse_no_acceptance_when_spec_inactive() -> None:
    # A non-speculative build (e.g. plain Gemma) omits the draft_* keys.
    tail = (
        b'{"usage":{"prompt_tokens":10,"completion_tokens":5},'
        b'"timings":{"prompt_per_second":100.0,"predicted_n":5,'
        b'"predicted_per_second":30.0}}'
    )
    r = _parse_usage_tail(tail)
    assert r["draft_n"] is None
    assert r["draft_n_accepted"] is None
    assert r["decode_tps"] == 30.0


def test_acceptance_rate_zero_accepted() -> None:
    # draft_n > 0 but nothing accepted -> the fields still parse (rate is
    # computed by the caller; here we just confirm extraction is exact).
    tail = b'"timings":{"predicted_per_second":10.0,"draft_n":64,"draft_n_accepted":0}'
    r = _parse_usage_tail(tail)
    assert r["draft_n"] == 64
    assert r["draft_n_accepted"] == 0
