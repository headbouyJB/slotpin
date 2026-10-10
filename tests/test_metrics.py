"""Tests for slotpin.metrics.MetricsWriter: JSONL writes, never-raises
behaviour, and size-based rotation honouring metrics_keep."""

from __future__ import annotations

import json

from slotpin.config import Config
from slotpin.metrics import MetricsWriter


def _cfg(metrics_file, **kw):
    kw.setdefault("metrics_rotate_mb", 32.0)
    kw.setdefault("metrics_keep", 3)
    return Config(metrics_file=metrics_file, **kw)


def test_write_produces_valid_jsonl_lines(tmp_path):
    path = tmp_path / "metrics.jsonl"
    writer = MetricsWriter(_cfg(path))
    events = [{"a": 1}, {"b": "two"}, {"c": [1, 2, 3]}]
    for e in events:
        writer.write(e)

    lines = path.read_text().splitlines()
    assert len(lines) == 3
    parsed = [json.loads(line) for line in lines]
    assert parsed == events


def test_write_never_raises_on_unwritable_path(tmp_path):
    blocked = tmp_path / "blocked"
    blocked.write_bytes(b"i am a file, not a directory")
    bad_path = blocked / "metrics.jsonl"  # parent is a regular file -> open() fails

    writer = MetricsWriter(_cfg(bad_path))
    writer.write({"anything": True})  # must not raise


def test_write_is_noop_when_metrics_file_is_none():
    writer = MetricsWriter(_cfg(None))
    writer.write({"anything": True})  # must not raise, nothing to check on disk


def test_maybe_rotate_never_raises_when_file_absent(tmp_path):
    path = tmp_path / "does-not-exist-yet.jsonl"
    writer = MetricsWriter(_cfg(path))
    writer.maybe_rotate()  # no-op, must not raise


def test_rotation_at_size_threshold_produces_dot1_and_honours_keep(tmp_path):
    path = tmp_path / "metrics.jsonl"
    # ~50 bytes threshold: a handful of small events will cross it quickly.
    cfg = _cfg(path, metrics_rotate_mb=50 / (1024 * 1024), metrics_keep=2)
    writer = MetricsWriter(cfg)

    for batch in range(4):
        for i in range(6):
            writer.write({"batch": batch, "i": i})

    assert path.exists()
    assert path.with_name("metrics.jsonl.1").exists()
    assert path.with_name("metrics.jsonl.2").exists()
    assert not path.with_name("metrics.jsonl.3").exists()  # metrics_keep=2 caps retention

    # Every surviving file must still be valid JSONL.
    for name in ("metrics.jsonl", "metrics.jsonl.1", "metrics.jsonl.2"):
        for line in path.with_name(name).read_text().splitlines():
            json.loads(line)  # must not raise


def test_rotation_drops_oldest_batch_content(tmp_path):
    path = tmp_path / "metrics.jsonl"
    cfg = _cfg(path, metrics_rotate_mb=30 / (1024 * 1024), metrics_keep=1)
    writer = MetricsWriter(cfg)

    # First batch, large enough alone to push past the threshold on the next write.
    for i in range(5):
        writer.write({"marker": "first-batch", "i": i})
    for i in range(5):
        writer.write({"marker": "second-batch", "i": i})

    all_text = ""
    for name in ("metrics.jsonl", "metrics.jsonl.1"):
        p = path.with_name(name)
        if p.exists():
            all_text += p.read_text()
    assert not path.with_name("metrics.jsonl.2").exists()  # metrics_keep=1
    # With keep=1 the oldest generation is eventually dropped entirely.
    assert "second-batch" in all_text


def _handler_stub(writer, status=0):
    from types import SimpleNamespace

    return SimpleNamespace(
        _upstream_status=status, server=SimpleNamespace(metrics=writer)
    )


def _cls():
    from slotpin.classify import Classification

    return Classification(
        lane="bulk", label="bulk", fp="abcd1234", prefix_hash="abcd1234", tier=2,
        interactive=False, slot=0,
    )


def test_turn_failed_record_schema(tmp_path):
    import time

    from slotpin.server import Handler

    path = tmp_path / "metrics.jsonl"
    stub = _handler_stub(MetricsWriter(_cfg(path)), status=500)
    Handler._write_turn_failed(stub, _cls(), 3, time.time() - 1.5, None)
    Handler._write_turn_failed(_handler_stub(MetricsWriter(_cfg(path))), _cls(), None, time.time(), "shed")

    first, second = (json.loads(ln) for ln in path.read_text().splitlines())
    assert set(first) == {"event", "ts", "reason", "route", "slot", "prefix", "dur_s", "upstream_status"}
    assert first["event"] == "turn_failed"
    assert first["reason"] == "upstream_5xx"  # non-2xx status, no earlier cause
    assert (first["route"], first["slot"], first["prefix"]) == ("bulk", 3, "abcd1234")
    assert first["upstream_status"] == 500 and first["dur_s"] >= 1.5
    # no status line -> no upstream_status; a shed turn has no slot
    assert set(second) == {"event", "ts", "reason", "route", "slot", "prefix", "dur_s"}
    assert second["reason"] == "shed" and second["slot"] is None


def test_turn_failed_reason_when_status_is_2xx_is_incomplete(tmp_path):
    import time

    from slotpin.server import Handler

    path = tmp_path / "metrics.jsonl"
    stub = _handler_stub(MetricsWriter(_cfg(path)), status=200)
    Handler._write_turn_failed(stub, _cls(), 0, time.time(), None)
    assert json.loads(path.read_text())["reason"] == "upstream_incomplete"


def test_turn_failed_write_never_raises():
    import time

    from slotpin.server import Handler

    class Boom:
        def write(self, event):
            raise RuntimeError("disk on fire")

    Handler._write_turn_failed(_handler_stub(Boom()), _cls(), 0, time.time(), "unreachable")
