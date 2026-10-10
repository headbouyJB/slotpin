"""Defaults that must not embed any deployment-specific values."""

from __future__ import annotations

import importlib

import pytest

import slotpin.dash as dash
from slotpin.config import Config


class _Stop(Exception):
    pass


def test_dash_defaults_are_neutral(monkeypatch):
    for k in ("DASH_HOST", "DASH_PROXY", "DASH_UPSTREAM", "DASH_ENERGY", "DASH_CFG", "DASH_BOXES"):
        monkeypatch.delenv(k, raising=False)
    try:
        fresh = importlib.reload(dash)
        assert fresh.LISTEN_HOST == "127.0.0.1"
        assert fresh.PROXY_ADDR == "127.0.0.1:8090"
        assert fresh.UPSTREAM == "http://127.0.0.1:5802"
        assert fresh.ENERGY_URL == ""
        assert fresh.CONFIG_ID == ""
        assert fresh.DASH_BOXES == ""
        assert fresh.BOXES == []
    finally:
        monkeypatch.undo()
        importlib.reload(dash)


def test_listen_host_default_is_loopback():
    assert Config().listen_host == "127.0.0.1"


def test_empty_energy_url_skips_polling(tmp_path, monkeypatch):
    monkeypatch.setattr(dash, "ENERGY_URL", "")
    monkeypatch.setattr(dash, "METRICS_FILE", str(tmp_path / "m.jsonl"))
    monkeypatch.setattr(dash, "SLOTPIN_CFG", str(tmp_path / "none.toml"))
    monkeypatch.setattr(dash, "_get_json", lambda *a, **k: (_ for _ in ()).throw(OSError("down")))
    monkeypatch.setattr(dash, "_tcp_up", lambda *a, **k: False)

    def _no_request(*a, **k):
        raise AssertionError("energy endpoint must not be contacted when DASH_ENERGY is empty")

    monkeypatch.setattr(dash.urllib.request, "urlopen", _no_request)
    monkeypatch.setattr(dash.time, "sleep", lambda *_: (_ for _ in ()).throw(_Stop()))
    with pytest.raises(_Stop):
        dash.collector()
    assert dash._live["energy"] is None
    assert dash._live["energy_ok"] is False


def test_empty_boxes_render_no_tabs(monkeypatch):
    monkeypatch.setattr(dash, "BOXES", dash._parse_boxes(""))
    page = dash.render_page()
    assert "__BOXNAV__" not in page
    assert '<nav class="boxnav"></nav>' in page
    assert '<a class="boxtab' not in page


def test_empty_config_id_is_harmless(tmp_path, monkeypatch):
    monkeypatch.setattr(dash, "SLOTPIN_CFG", str(tmp_path / "none.toml"))
    monkeypatch.setattr(dash, "CONFIG_ID", "")
    monkeypatch.setattr(dash, "DRAFTER", "")
    assert dash.read_config_id() == ""
    assert dash.drafter_label() == "Draft"
    assert "h.config_id||'unknown'" in dash.PAGE_TEMPLATE
