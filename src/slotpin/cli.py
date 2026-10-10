"""Command-line entry point: argument parsing, component wiring, and the run loop.

``slotpin-proxy [--config PATH] [--port N] [--check]``. ``--check`` validates the config and exits 0/1
without starting anything, so it's safe to use as a systemd
``ExecStartPre``. On a normal start this module builds every component in
dependency order, calls ``kv.load_stats_from_disk()``, starts the reload
detector, logs a one-line config summary, and then serves until it receives
SIGTERM/SIGINT, at which point it shuts down cleanly.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
from collections.abc import Sequence
from pathlib import Path

from .classify import Classifier
from .config import Config, ConfigError, load_config, runtime_path_warnings
from .kvstore import KVStore
from .metrics import MetricsWriter
from .pool import CacheRamPool
from .reload_detect import ReloadDetector
from .scheduler import Scheduler
from .server import ProxyServer
from .slot_admin import SlotAdmin
from .state import ProxyState

logger = logging.getLogger("slotpin.cli")


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse ``slotpin-proxy`` command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="slotpin-proxy",
        description="Slot-pinning, KV-warmth-preserving reverse proxy for llama.cpp stacks.",
    )
    parser.add_argument(
        "--config", type=Path, default=None, help="Path to a TOML config file."
    )
    parser.add_argument(
        "--port", type=int, default=None, help="Override the configured listen_port."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate the configuration and exit (0=ok, 1=error); starts nothing.",
    )
    return parser.parse_args(argv)


def _configure_logging() -> None:
    """Set up stdlib logging once, honouring SLOTPIN_LOG_LEVEL (default INFO)."""
    level_name = os.environ.get("SLOTPIN_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _build_components(cfg: Config) -> tuple[
    Classifier, ProxyState, CacheRamPool, SlotAdmin, MetricsWriter, KVStore, Scheduler, ReloadDetector
]:
    """Construct every component in the dependency order."""
    classifier = Classifier(cfg)
    state = ProxyState()
    pool = CacheRamPool(cfg)
    admin = SlotAdmin(cfg)
    metrics = MetricsWriter(cfg)
    kv = KVStore(cfg, admin, pool, state, metrics)
    sched = Scheduler(cfg, state)
    reload_detector = ReloadDetector(cfg, admin, pool, state)
    return classifier, state, pool, admin, metrics, kv, sched, reload_detector


_LOOPBACK_HOSTS = ("127.0.0.1", "::1", "localhost")


def _is_loopback(host: str) -> bool:
    """True for a loopback listen host (``127.0.0.0/8``, ``::1``, ``localhost``)."""
    h = host.strip().lower()
    return h in _LOOPBACK_HOSTS or h.startswith("127.")


def _dir_writable(path: Path) -> bool:
    """True if ``path`` is an existing, writable directory."""
    return path.is_dir() and os.access(path, os.W_OK | os.X_OK)


def check_summary(cfg: Config) -> list[str]:
    """Human-readable configuration summary lines for ``--check`` (stderr)."""
    energy = f"energy={cfg.energy_url}" if cfg.energy_url else "energy=off"
    lines = [
        f"upstream_kind={cfg.upstream_kind} listen={cfg.listen_host}:{cfg.listen_port} "
        f"upstream={cfg.upstream_host}:{cfg.upstream_port} "
        f"admin={cfg.admin_host}:{cfg.admin_port}",
        f"slots={cfg.slots} max_inflight={cfg.max_inflight} "
        f"sched_enabled={str(cfg.sched_enabled).lower()} "
        f"kv_persist={str(cfg.kv_persist).lower()} "
        f"kv_adaptive={str(cfg.kv_adaptive).lower()} "
        f"prefix_restore={str(cfg.prefix_restore).lower()}",
        f"slot_save_path={cfg.slot_save_path} metrics_file={cfg.metrics_file or 'off'} {energy}",
    ]
    interactive = [lane.name for lane in cfg.lanes if lane.interactive]
    lines.append(
        f"lanes={len(cfg.lanes)} (interactive: {', '.join(interactive) or 'none'}) "
        f"demote={len(cfg.demote)} labels={len(cfg.labels)}"
    )
    return lines


def check_warnings(cfg: Config) -> list[str]:
    """Non-fatal advisory messages for ``--check``, beyond ``runtime_path_warnings``."""
    out: list[str] = []
    if not _is_loopback(cfg.listen_host):
        out.append(
            f"listen_host={cfg.listen_host} is not loopback: the proxy has no authentication; "
            "only expose it behind a firewall, VPN or authenticating reverse proxy "
            "(see SECURITY.md)"
        )
    if cfg.upstream_kind == "llamacpp" and cfg.kv_persist and not _dir_writable(
        cfg.slot_save_path
    ):
        out.append(
            f"kv_persist is on but slot_save_path {cfg.slot_save_path} does not exist or is not "
            "writable by this user; it must be a writable directory equal to llama-server "
            "--slot-save-path"
        )
    if cfg.metrics_file is not None:
        parent = cfg.metrics_file.parent
        if not _dir_writable(parent):
            out.append(
                f"metrics_file directory {parent} does not exist or is not writable by this "
                "user; metrics cannot be recorded there"
            )
    return out


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point. Returns a process exit code (0 success, 1 config error)."""
    args = _parse_args(argv)

    try:
        cfg = load_config(args.config, env=os.environ)
    except ConfigError as exc:
        print(f"slotpin: config error: {exc}", file=sys.stderr)
        if "unknown config key" in str(exc):
            print("slotpin: see docs/configuration.md for the key list", file=sys.stderr)
        return 1

    if args.port is not None:
        cfg.listen_port = args.port

    if args.check:
        for line in check_summary(cfg):
            print(f"slotpin: {line}", file=sys.stderr)
        for warning in [*runtime_path_warnings(cfg), *check_warnings(cfg)]:
            print(f"slotpin: WARNING: {warning}", file=sys.stderr)
        if not cfg.lanes:
            print(
                "slotpin: no lanes configured: every request classifies to lane 'default' "
                f"(tier {cfg.default_tier})",
                file=sys.stderr,
            )
        if cfg.upstream_stall_timeout_s > 0:
            print(
                f"slotpin: fail-fast: upstream_stall_timeout_s={cfg.upstream_stall_timeout_s:g} "
                f"hold_headers_until_first_output={str(cfg.hold_headers_until_first_output).lower()}"
                " (the limit is on no stream output and no token progress in the upstream's /slots)"
            )
        elif cfg.hold_headers_until_first_output:
            print(
                "slotpin: fail-fast: hold_headers_until_first_output=true with no "
                "upstream_stall_timeout_s (headers are held; nothing aborts a stalled upstream)"
            )
        print("slotpin: config OK")
        return 0

    _configure_logging()

    for warning in runtime_path_warnings(cfg):
        logger.warning("%s", warning)

    classifier, state, pool, admin, metrics, kv, sched, reload_detector = _build_components(cfg)

    logger.info(
        "slotpin starting: upstream_kind=%s mode=%s sched_enabled=%s kv_persist=%s "
        "kv_adaptive=%s upstream=%s:%d admin=%s:%d listen=%s:%d save_path=%s cram_token_cap=%d",
        cfg.upstream_kind,
        cfg.mode,
        cfg.sched_enabled,
        cfg.kv_persist,
        cfg.kv_adaptive,
        cfg.upstream_host,
        cfg.upstream_port,
        cfg.admin_host,
        cfg.admin_port,
        cfg.listen_host,
        cfg.listen_port,
        cfg.slot_save_path,
        cfg.cram_token_cap,
    )

    kv.load_stats_from_disk()
    reload_detector.start()

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

    stop_evt = threading.Event()

    def _handle_signal(signum: int, _frame: object) -> None:
        logger.info("received signal %d; shutting down", signum)
        stop_evt.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    server_thread = threading.Thread(target=server.serve_forever, name="slotpin-http", daemon=True)
    server_thread.start()

    try:
        stop_evt.wait()
    finally:
        logger.info("shutting down slotpin proxy")
        reload_detector.stop()
        server.shutdown()
        server_thread.join(timeout=5.0)

    return 0
