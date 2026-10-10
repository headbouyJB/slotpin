"""Tests for ``slotpin-proxy --check`` output, the shipped example configs and the version."""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

import slotpin
from slotpin import cli

ROOT = Path(__file__).resolve().parent.parent


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "config.toml"
    p.write_text(body)
    return p


def _isolated_env(monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    for k in list(os.environ):
        if k.startswith("SLOTPIN_"):
            monkeypatch.delenv(k)


def test_check_stdout_unchanged(capsys, monkeypatch):
    _isolated_env(monkeypatch)
    assert cli.main(["--check"]) == 0
    out, _err = capsys.readouterr()
    assert out == "slotpin: config OK\n"


def test_check_summary_on_stderr(tmp_path, capsys, monkeypatch):
    _isolated_env(monkeypatch)
    cfg = _write(
        tmp_path,
        f'''
kv_persist = true
slot_save_path = "{tmp_path}"
metrics_file = "{tmp_path}/m.jsonl"
[[classify.lanes]]
name = "chat"
match = ["x"]
interactive = true
''',
    )
    assert cli.main(["--config", str(cfg), "--check"]) == 0
    out, err = capsys.readouterr()
    assert out == "slotpin: config OK\n"
    assert "slotpin: upstream_kind=llamacpp listen=" in err
    assert "slots=1 max_inflight=1" in err
    assert f"slot_save_path={tmp_path}" in err
    assert "energy=off" in err
    assert "lanes=1 (interactive: chat) demote=0 labels=0" in err


def test_nonloopback_listen_warns(tmp_path, capsys, monkeypatch):
    _isolated_env(monkeypatch)
    cfg = _write(tmp_path, f'listen_host = "0.0.0.0"\nmetrics_file = "{tmp_path}/m.jsonl"\n')
    assert cli.main(["--config", str(cfg), "--check"]) == 0
    _out, err = capsys.readouterr()
    assert "WARNING: listen_host=0.0.0.0 is not loopback" in err
    assert "no authentication" in err and "SECURITY.md" in err


def test_loopback_listen_does_not_warn(tmp_path, capsys, monkeypatch):
    _isolated_env(monkeypatch)
    cfg = _write(tmp_path, f'listen_host = "127.0.0.1"\nmetrics_file = "{tmp_path}/m.jsonl"\n')
    cli.main(["--config", str(cfg), "--check"])
    assert "not loopback" not in capsys.readouterr().err


def test_missing_save_path_warns(tmp_path, capsys, monkeypatch):
    _isolated_env(monkeypatch)
    missing = tmp_path / "nope"
    cfg = _write(
        tmp_path,
        f'kv_persist = true\nslot_save_path = "{missing}"\nmetrics_file = "{tmp_path}/m.jsonl"\n',
    )
    assert cli.main(["--config", str(cfg), "--check"]) == 0
    _out, err = capsys.readouterr()
    assert "WARNING: kv_persist is on but slot_save_path" in err
    assert "--slot-save-path" in err


def test_missing_metrics_dir_warns(tmp_path, capsys, monkeypatch):
    _isolated_env(monkeypatch)
    cfg = _write(tmp_path, f'metrics_file = "{tmp_path}/absent/m.jsonl"\n')
    assert cli.main(["--config", str(cfg), "--check"]) == 0
    assert "WARNING: metrics_file directory" in capsys.readouterr().err


def test_no_lanes_info_line(tmp_path, capsys, monkeypatch):
    _isolated_env(monkeypatch)
    cfg = _write(tmp_path, f'default_tier = 7\nmetrics_file = "{tmp_path}/m.jsonl"\n')
    cli.main(["--config", str(cfg), "--check"])
    assert "no lanes configured: every request classifies to lane 'default' (tier 7)" in (
        capsys.readouterr().err
    )


def test_missing_config_file_exits_1(tmp_path, capsys, monkeypatch):
    _isolated_env(monkeypatch)
    rc = cli.main(["--config", str(tmp_path / "missing.toml"), "--check"])
    out, err = capsys.readouterr()
    assert rc == 1
    assert out == ""
    assert "slotpin: config error: config file not found" in err


def test_unknown_key_hint(tmp_path, capsys, monkeypatch):
    _isolated_env(monkeypatch)
    cfg = _write(tmp_path, "bogus_key = 1\n")
    assert cli.main(["--config", str(cfg), "--check"]) == 1
    err = capsys.readouterr().err
    assert "unknown config key" in err
    assert "docs/configuration.md" in err


@pytest.mark.parametrize("name", ["config.example.toml", "config.example.gufo.toml"])
def test_example_configs_pass_check(name, capsys, monkeypatch):
    _isolated_env(monkeypatch)
    assert cli.main(["--config", str(ROOT / name), "--check"]) == 0
    out, err = capsys.readouterr()
    assert out == "slotpin: config OK\n"
    assert "slotpin: upstream_kind=" in err
    assert "not loopback" not in err


def test_version_matches_pyproject():
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    assert slotpin.__version__ == data["project"]["version"]
