"""Tests for slotpin.config: precedence, legacy env aliases, boolean parsing,
cram_token_cap derivation, and validation error messages."""

from __future__ import annotations

import os

import pytest

from slotpin import sysmem
from slotpin.config import Config, ConfigError, load_config, runtime_path_warnings


def _write(tmp_path, text: str, name: str = "cfg.toml"):
    path = tmp_path / name
    path.write_text(text)
    return path


# ---------------------------------------------------------------------------
# precedence: defaults < TOML < env
# ---------------------------------------------------------------------------


def test_defaults_only():
    cfg = load_config(None, env={})
    defaults = Config()
    assert cfg.mode == defaults.mode == "single"
    assert cfg.listen_port == defaults.listen_port == 8090
    assert cfg.cache_ram_mb == defaults.cache_ram_mb
    assert cfg.lanes == []
    assert cfg.demote == []
    assert cfg.labels == []


def test_toml_overrides_defaults(tmp_path):
    path = _write(tmp_path, 'mode = "dual"\ncache_ram_mb = 4096\n')
    cfg = load_config(path, env={})
    assert cfg.mode == "dual"
    assert cfg.cache_ram_mb == 4096
    # Untouched fields keep their dataclass defaults.
    assert cfg.upstream_port == Config().upstream_port


def test_env_overrides_toml(tmp_path):
    path = _write(tmp_path, 'mode = "dual"\n')
    cfg = load_config(path, env={"SLOTPIN_MODE": "single"})
    assert cfg.mode == "single"


def test_env_without_toml_overrides_defaults():
    cfg = load_config(None, env={"SLOTPIN_UPSTREAM_PORT": "9999"})
    assert cfg.upstream_port == 9999


def test_full_precedence_chain(tmp_path):
    # default upstream_port=8080; TOML sets 8081; env overrides to 8082.
    path = _write(tmp_path, "upstream_port = 8081\n")
    cfg = load_config(path, env={"SLOTPIN_UPSTREAM_PORT": "8082"})
    assert cfg.upstream_port == 8082


# ---------------------------------------------------------------------------
# legacy env aliases
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "alias,value,field_name,expected",
    [
        ("SLOTPIN_MODE", "dual", "mode", "dual"),
        ("SLOTPIN_KV_PERSIST", "on", "kv_persist", True),
        ("SLOTPIN_KV_ADAPTIVE", "on", "kv_adaptive", True),
        ("SLOTPIN_SCHED", "on", "sched_enabled", True),
        ("SLOTPIN_CACHE_RAM_MB", "8192", "cache_ram_mb", 8192),
    ],
)
def test_legacy_env_alias(alias, value, field_name, expected):
    cfg = load_config(None, env={alias: value})
    assert getattr(cfg, field_name) == expected


def test_legacy_slot_save_path_alias(tmp_path):
    target = tmp_path / "kv"
    cfg = load_config(None, env={"SLOTPIN_SLOT_SAVE_PATH": str(target)})
    assert cfg.slot_save_path == target


def test_legacy_metrics_alias(tmp_path):
    target = tmp_path / "metrics.jsonl"
    cfg = load_config(None, env={"SLOTPIN_METRICS": str(target)})
    assert cfg.metrics_file == target


def test_legacy_metrics_alias_can_disable():
    cfg = load_config(None, env={"SLOTPIN_METRICS": "off"})
    assert cfg.metrics_file is None


def test_sched_alias_maps_to_sched_enabled_not_a_literal_sched_field():
    # SLOTPIN_SCHED is a legacy spelling; there is no "sched" field, only
    # "sched_enabled" -- make sure the alias table, not mechanical
    # derivation, is what's doing the mapping.
    cfg = load_config(None, env={"SLOTPIN_SCHED": "false"})
    assert cfg.sched_enabled is False


# ---------------------------------------------------------------------------
# boolean forms
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", ["on", "1", "true", "TRUE", "On", "yes"])
def test_boolean_truthy_forms(raw):
    cfg = load_config(None, env={"SLOTPIN_KV_PERSIST": raw})
    assert cfg.kv_persist is True


@pytest.mark.parametrize("raw", ["off", "0", "false", "FALSE", "Off", "no"])
def test_boolean_falsy_forms(raw):
    cfg = load_config(None, env={"SLOTPIN_KV_PERSIST": raw})
    assert cfg.kv_persist is False


def test_boolean_invalid_form_raises():
    with pytest.raises(ConfigError, match="SLOTPIN_KV_PERSIST"):
        load_config(None, env={"SLOTPIN_KV_PERSIST": "maybe"})


def test_boolean_toml_native_bool(tmp_path):
    path = _write(tmp_path, "kv_persist = true\n")
    cfg = load_config(path, env={})
    assert cfg.kv_persist is True


# ---------------------------------------------------------------------------
# cram_token_cap derivation
# ---------------------------------------------------------------------------


def test_cram_token_cap_derivation_default():
    cfg = load_config(None, env={})
    expected = int(cfg.cache_ram_mb * 1024**2 / cfg.bytes_per_token * cfg.cram_safety)
    assert cfg.cram_token_cap == expected
    assert cfg.cram_token_cap > 0


def test_cram_token_cap_derivation_custom(tmp_path):
    path = _write(
        tmp_path,
        "cache_ram_mb = 2048\nbytes_per_token = 17300\ncram_safety = 0.9\n",
    )
    cfg = load_config(path, env={})
    expected = int(2048 * 1024**2 / 17300 * 0.9)
    assert cfg.cram_token_cap == expected


def test_cram_token_cap_changes_with_bytes_per_token(tmp_path):
    path = _write(tmp_path, "cache_ram_mb = 100\nbytes_per_token = 1000\ncram_safety = 1.0\n")
    cfg = load_config(path, env={})
    assert cfg.cram_token_cap == int(100 * 1024**2 / 1000 * 1.0)


# ---------------------------------------------------------------------------
# validation errors name the offending key
# ---------------------------------------------------------------------------


def test_invalid_mode_names_key(tmp_path):
    path = _write(tmp_path, 'mode = "turbo"\n')
    with pytest.raises(ConfigError, match="mode"):
        load_config(path, env={})


def test_more_than_one_interactive_lane_raises(tmp_path):
    path = _write(
        tmp_path,
        """
[[classify.lanes]]
name = "a"
match = ["x"]
interactive = true

[[classify.lanes]]
name = "b"
match = ["y"]
interactive = true
""",
    )
    with pytest.raises(ConfigError, match="interactive"):
        load_config(path, env={})


def test_single_interactive_lane_is_fine(tmp_path):
    path = _write(
        tmp_path,
        """
[[classify.lanes]]
name = "a"
match = ["x"]
interactive = true
""",
    )
    cfg = load_config(path, env={})
    assert cfg.lanes[0].interactive is True


def test_bad_regex_in_lane_match_names_key(tmp_path):
    path = _write(
        tmp_path,
        """
[[classify.lanes]]
name = "broken"
match = ["("]
""",
    )
    with pytest.raises(ConfigError, match="classify.lanes\\[broken\\].match"):
        load_config(path, env={})


def test_bad_regex_in_demote_names_key(tmp_path):
    path = _write(
        tmp_path,
        """
[[classify.demote]]
name = "heartbeat"
first_user_match = "("
""",
    )
    with pytest.raises(ConfigError, match="classify.demote\\[heartbeat\\]"):
        load_config(path, env={})


def test_bad_port_names_key(tmp_path):
    path = _write(tmp_path, "upstream_port = 99999\n")
    with pytest.raises(ConfigError, match="upstream_port"):
        load_config(path, env={})


def test_zero_upstream_port_rejected(tmp_path):
    path = _write(tmp_path, "upstream_port = 0\n")
    with pytest.raises(ConfigError, match="upstream_port"):
        load_config(path, env={})


def test_listen_port_zero_is_allowed_ephemeral(tmp_path):
    path = _write(tmp_path, "listen_port = 0\n")
    cfg = load_config(path, env={})
    assert cfg.listen_port == 0


def test_unknown_toml_key_raises(tmp_path):
    path = _write(tmp_path, "not_a_real_key = 1\n")
    with pytest.raises(ConfigError, match="not_a_real_key"):
        load_config(path, env={})


def test_missing_config_file_raises(tmp_path):
    with pytest.raises(ConfigError):
        load_config(tmp_path / "does-not-exist.toml", env={})


def test_lane_missing_match_raises(tmp_path):
    path = _write(tmp_path, '[[classify.lanes]]\nname = "x"\n')
    with pytest.raises(ConfigError, match="match"):
        load_config(path, env={})


# ---------------------------------------------------------------------------
# runtime_path_warnings: tmpfs slot_save_path (the RAM-balloon footgun)
# ---------------------------------------------------------------------------


def test_runtime_path_warnings_flags_tmpfs_slot_save_path(make_config, tmp_path, monkeypatch):
    ram = tmp_path / "ram"
    ram.mkdir()
    ram_r = os.path.realpath(str(ram))
    mounts = tmp_path / "mounts"
    mounts.write_text(f"rootfs / ext4 rw 0 0\ntmpfs {ram_r} tmpfs rw 0 0\n")
    monkeypatch.setattr(sysmem, "MOUNTS_PATH", str(mounts))
    cfg = make_config(kv_persist=True, slot_save_path=str(ram))
    warns = runtime_path_warnings(cfg)
    assert any("slot_save_path" in w for w in warns)
    assert any(("tmpfs" in w) or ("RAM filesystem" in w) for w in warns)


def test_runtime_path_warnings_silent_on_persistent_disk(make_config, tmp_path, monkeypatch):
    mounts = tmp_path / "mounts"
    mounts.write_text("rootfs / ext4 rw 0 0\n")  # everything resolves to ext4 root
    monkeypatch.setattr(sysmem, "MOUNTS_PATH", str(mounts))
    cfg = make_config(kv_persist=True)
    assert runtime_path_warnings(cfg) == []
