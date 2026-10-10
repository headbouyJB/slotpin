#!/usr/bin/env bash
# Durable install of slotpin-proxy (and optionally slotpin-dash) as systemd
# services. Idempotent: safe to re-run.
#
# Layout:
#   /opt/slotpin/venv            isolated venv with the package installed
#   /usr/local/bin/slotpin-proxy symlink -> the venv entrypoint (systemd ExecStart)
#   /usr/local/bin/slotpin-dash  symlink -> the dashboard entrypoint
#   /etc/slotpin/config.toml     config (installed from --config, not overwritten if present unless --force-config)
#   /var/lib/slotpin/kv-cache    KV snapshots (owned by the run user; == llama-server --slot-save-path)
#   /var/lib/slotpin/metrics.jsonl  per-turn metric JSONL
#   /etc/systemd/system/slotpin-proxy.service  unit (User=<run-user>, ReadWritePaths=<data-dir>)
#   /etc/systemd/system/slotpin-dash.service   unit, with --dash
#   /etc/slotpin/slotpin-dash.env              dashboard env file (created from the example if absent), with --dash
#   /etc/logrotate.d/slotpin-metrics           metrics rotation, if /etc/logrotate.d exists
#
# Usage:
#   sudo ./deploy/install.sh --config /path/to/config.toml [--user slotpin] \
#        [--data-dir /var/lib/slotpin] [--repo <repo-root>] [--force-config] \
#        [--dash] [--enable]
#
# --user     account the services run as (default: the sudo caller). It must exist.
# --dash     also install the dashboard unit (slotpin-dash.service).
# --enable   also `systemctl enable --now` the installed units (default: install + daemon-reload only).
#
# NOTE: this installs slotpin only. llama-server / llama-swap (the upstream)
# and the optional reload-pairing drop-in
# (deploy/systemd/llama-swap-pair-slotpin.conf) are a separate step.
set -euo pipefail

RUN_USER="${SUDO_USER:-$(id -un)}"
DATA_DIR="/var/lib/slotpin"
CONFIG_SRC=""
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FORCE_CONFIG=0
ENABLE=0
DASH=0

while [ $# -gt 0 ]; do
  case "$1" in
    --config)       CONFIG_SRC="$2"; shift 2 ;;
    --user)         RUN_USER="$2"; shift 2 ;;
    --data-dir)     DATA_DIR="$2"; shift 2 ;;
    --repo)         REPO_DIR="$2"; shift 2 ;;
    --force-config) FORCE_CONFIG=1; shift ;;
    --dash)         DASH=1; shift ;;
    --enable)       ENABLE=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [ "$(id -u)" -ne 0 ]; then echo "must run as root (use sudo)" >&2; exit 1; fi
if [ -z "$CONFIG_SRC" ] || [ ! -f "$CONFIG_SRC" ]; then
  echo "--config <file> is required and must exist" >&2; exit 2
fi

echo "==> slotpin install: user=$RUN_USER data=$DATA_DIR repo=$REPO_DIR"

# 1. venv + package
python3 -m venv /opt/slotpin/venv
/opt/slotpin/venv/bin/pip install -q --upgrade pip
/opt/slotpin/venv/bin/pip install -q "$REPO_DIR"
ln -sf /opt/slotpin/venv/bin/slotpin-proxy /usr/local/bin/slotpin-proxy
ln -sf /opt/slotpin/venv/bin/slotpin-dash /usr/local/bin/slotpin-dash
if /usr/local/bin/slotpin-proxy --help >/dev/null 2>&1; then
  echo "    installed slotpin-proxy: OK"
else
  echo "    slotpin-proxy --help failed" >&2; exit 1
fi

# 2. data dir (writable by the run user; == llama-server --slot-save-path)
install -d -o "$RUN_USER" -g "$RUN_USER" "$DATA_DIR" "$DATA_DIR/kv-cache"

# 3. config
install -d /etc/slotpin
if [ -f /etc/slotpin/config.toml ] && [ "$FORCE_CONFIG" -ne 1 ]; then
  echo "    /etc/slotpin/config.toml exists; keeping it (use --force-config to overwrite)"
else
  install -m 0644 "$CONFIG_SRC" /etc/slotpin/config.toml
  echo "    installed /etc/slotpin/config.toml from $CONFIG_SRC"
fi

# 4. systemd unit (derived from deploy/systemd/slotpin-proxy.service with the
#    run user + data dir substituted in)
cat > /etc/systemd/system/slotpin-proxy.service <<UNIT
[Unit]
Description=slotpin-proxy - KV-warmth-preserving reverse proxy for llama.cpp
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$RUN_USER
EnvironmentFile=-/etc/slotpin/slotpin.env
ExecStartPre=/usr/local/bin/slotpin-proxy --config /etc/slotpin/config.toml --check
ExecStart=/usr/local/bin/slotpin-proxy --config /etc/slotpin/config.toml
Restart=always
RestartSec=2
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=read-only
PrivateTmp=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictSUIDSGID=true
ReadWritePaths=$DATA_DIR

[Install]
WantedBy=multi-user.target
UNIT

# 5. optional: dashboard unit, env file, logrotate
if [ "$DASH" -eq 1 ]; then
  DASH_UNIT_SRC="$REPO_DIR/deploy/systemd/slotpin-dash.service"
  if [ ! -f "$DASH_UNIT_SRC" ]; then echo "missing $DASH_UNIT_SRC" >&2; exit 1; fi
  sed -e "s|^User=.*|User=$RUN_USER|" \
      -e "s|^ExecStart=.*|ExecStart=/opt/slotpin/venv/bin/slotpin-dash|" \
      -e "s|^ReadOnlyPaths=.*|ReadOnlyPaths=$DATA_DIR|" \
      "$DASH_UNIT_SRC" > /etc/systemd/system/slotpin-dash.service
  if [ ! -f /etc/slotpin/slotpin-dash.env ]; then
    install -m 0644 "$REPO_DIR/deploy/systemd/slotpin-dash.env.example" /etc/slotpin/slotpin-dash.env
    echo "    created /etc/slotpin/slotpin-dash.env from the example (all values commented out)"
  fi
  echo "    installed slotpin-dash.service"
fi
if [ -d /etc/logrotate.d ] && [ -f "$REPO_DIR/deploy/logrotate/slotpin-metrics" ]; then
  sed "s|^/var/lib/slotpin/metrics.jsonl |$DATA_DIR/metrics.jsonl |" \
    "$REPO_DIR/deploy/logrotate/slotpin-metrics" > /etc/logrotate.d/slotpin-metrics
  chmod 0644 /etc/logrotate.d/slotpin-metrics
  echo "    installed /etc/logrotate.d/slotpin-metrics"
fi

# 6. validate config before arming
/usr/local/bin/slotpin-proxy --config /etc/slotpin/config.toml --check

systemctl daemon-reload
if [ "$ENABLE" -eq 1 ]; then
  systemctl enable --now slotpin-proxy
  echo "==> enabled + started slotpin-proxy"
  if [ "$DASH" -eq 1 ]; then
    systemctl enable --now slotpin-dash
    echo "==> enabled + started slotpin-dash"
  fi
else
  UNITS="slotpin-proxy"
  if [ "$DASH" -eq 1 ]; then UNITS="slotpin-proxy slotpin-dash"; fi
  echo "==> installed (not enabled). Start with: systemctl enable --now $UNITS"
fi
