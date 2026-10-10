"""Thin client for the llama-server slot-admin API and upstream liveness probe.

Talks directly to ``admin_host:admin_port`` (llama-server, bypassing
llama-swap) for ``/slots/{slot}?action=save|restore`` and ``/props``, per
the llama-server slot API. Never raises -- every public method
catches all exceptions and reports failure via its return value, since a
slot-admin hiccup must never take down the proxy's request path.

Notes:
- ``upstream_alive()`` probes ``GET /health`` on the upstream (llama-swap).
  It falls back to a raw TCP connect if the HTTP probe fails outright
  (connection refused etc.), so a llama-swap without /health still correctly
  reports "alive" as long as it accepts the TCP connection.
- ``props()``'s expected JSON shape (used by reload_detect.py, out of this
  module's scope) may vary between llama-server builds.

With ``upstream_kind = "gufo"`` only the read-only probes are used:
``models()`` (``/v1/models``) and ``upstream_metrics()`` (``/metrics``) for
reload detection, and ``slot_processing()`` via ``/metrics``.
"""

from __future__ import annotations

import http.client
import json
import logging
import socket
from typing import Any

from .config import Config

logger = logging.getLogger("slotpin.slot_admin")

# Upper bound for request-path admin calls (/apply-template) -- see apply_template.
_RENDER_TIMEOUT_S = 10.0


class SlotAdmin:
    """Never-raising client for the llama-server admin API + upstream probe."""

    def __init__(self, cfg: Config) -> None:
        """Bind to ``cfg`` for admin/upstream host:port and timeouts."""
        self._cfg = cfg

    def _admin_request(
        self, method: str, path: str, body: bytes | None = None, timeout: float | None = None
    ) -> tuple[int | None, bytes]:
        """POST/GET against the admin API. Returns (status, body) or (None, error-bytes)."""
        conn: http.client.HTTPConnection | None = None
        try:
            conn = http.client.HTTPConnection(
                self._cfg.admin_host,
                self._cfg.admin_port,
                timeout=timeout if timeout is not None else self._cfg.admin_timeout_s,
            )
            headers = {"Content-Type": "application/json"} if body is not None else {}
            conn.request(method, path, body=body, headers=headers)
            resp = conn.getresponse()
            data = resp.read()
            return resp.status, data
        except Exception as exc:  # noqa: BLE001 - never raise out of slot_admin
            logger.warning("slot_admin: %s %s failed: %s", method, path, exc)
            return None, str(exc).encode("utf-8", "ignore")
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass

    def save(self, slot: int, filename: str) -> tuple[bool, str]:
        """POST ``/slots/{slot}?action=save`` with ``{"filename": filename}``."""
        body = json.dumps({"filename": filename}).encode("utf-8")
        status, data = self._admin_request("POST", f"/slots/{slot}?action=save", body)
        return self._result(status, data)

    def restore(self, slot: int, filename: str) -> tuple[bool, str]:
        """POST ``/slots/{slot}?action=restore`` with ``{"filename": filename}``."""
        body = json.dumps({"filename": filename}).encode("utf-8")
        status, data = self._admin_request("POST", f"/slots/{slot}?action=restore", body)
        return self._result(status, data)

    def apply_template(self, obj: dict[str, Any]) -> str | None:
        """Render a chat request to the exact prompt text the model will see.

        POSTs the request body to llama-server ``/apply-template`` (same parser
        as ``/v1/chat/completions``, so tools/template kwargs land where the
        real turn puts them). Returns ``None`` on any failure -- callers treat
        that as "no prefix information" and fall back to exact-hash behaviour.
        """
        body = dict(obj)
        body.pop("id_slot", None)
        body["stream"] = False
        # On the request path, before forwarding: bound it well below the
        # save/restore admin timeout so a wedged upstream can't stall the turn.
        status, data = self._admin_request(
            "POST",
            "/apply-template",
            json.dumps(body).encode("utf-8"),
            timeout=min(self._cfg.admin_timeout_s, _RENDER_TIMEOUT_S),
        )
        if status is None:
            return None  # transport failure, already logged by _admin_request
        if not (200 <= status < 300):
            logger.warning(
                "slot_admin: /apply-template returned %s (%d messages): %s",
                status,
                len(obj.get("messages") or []),
                data[:300].decode("utf-8", "ignore"),
            )
            return None
        try:
            prompt = json.loads(data).get("prompt")
        except (ValueError, UnicodeDecodeError, AttributeError) as exc:
            logger.warning("slot_admin: /apply-template returned unparseable body: %s", exc)
            return None
        if not isinstance(prompt, str):
            logger.warning("slot_admin: /apply-template response has no 'prompt' string")
            return None
        return prompt

    @staticmethod
    def _result(status: int | None, data: bytes) -> tuple[bool, str]:
        detail = data[:300].decode("utf-8", "ignore")
        if status is None:
            return False, detail
        return 200 <= status < 300, f"{status} {detail}"

    def slot_processing(self, slot: int, others: int = 0) -> bool | None:
        """Is llama-server still working on ``slot``? (``GET /slots``; needs
        ``--slots``). ``None`` when it can't be determined.

        Gufo's ``/slots`` has no ``is_processing`` and it assigns sessions
        itself, so there it reads ``/metrics`` instead: busy while more requests
        are processing than the ``others`` the caller knows are live.
        """
        if self._cfg.upstream_kind == "gufo":
            m = self.upstream_metrics()
            n = m.get("llamacpp:requests_processing") if m is not None else None
            return None if n is None else n > others
        status, data = self._admin_request(
            "GET", "/slots", timeout=min(self._cfg.admin_timeout_s, _RENDER_TIMEOUT_S)
        )
        if status is None or not (200 <= status < 300):
            return None
        try:
            for s in json.loads(data):
                if s.get("id") == slot:
                    return bool(s.get("is_processing"))
        except (ValueError, UnicodeDecodeError, TypeError, AttributeError):
            return None
        return None

    def slots_progress(self) -> list[tuple[int | None, int | None]] | None:
        """Token counters of every busy upstream slot (``GET /slots``), sorted:
        ``(n_prompt_tokens_processed, next_token[0].n_decoded)`` per entry with
        ``is_processing`` true; a missing counter is ``None``. ``None`` when
        ``/slots`` is unreachable, errors or is unparseable.

        Two equal readings some time apart mean no token progress (the stall
        probe in server.py). Not per slot: Gufo assigns sessions itself, so the
        proxy cannot tell which entry is its request. Only the counters are
        copied out -- the entries also carry the prompt text.
        """
        status, data = self._admin_request(
            "GET", "/slots", timeout=min(self._cfg.admin_timeout_s, _RENDER_TIMEOUT_S)
        )
        if status is None or not (200 <= status < 300):
            return None

        def _count(value: Any) -> int | None:
            return value if isinstance(value, int) and not isinstance(value, bool) else None

        try:
            out = []
            for s in json.loads(data):
                if not s.get("is_processing"):
                    continue
                nxt = s.get("next_token")
                if isinstance(nxt, list):  # one entry per sequence; older builds: an object
                    nxt = nxt[0] if nxt else None
                decoded = nxt.get("n_decoded") if isinstance(nxt, dict) else None
                out.append((_count(s.get("n_prompt_tokens_processed")), _count(decoded)))
        except (ValueError, UnicodeDecodeError, TypeError, AttributeError):
            return None
        return sorted(out, key=lambda t: (t[0] is None, t[0] or 0, t[1] is None, t[1] or 0))

    def props(self) -> dict[str, Any] | None:
        """GET ``/props`` from llama-server. Returns ``None`` on any error."""
        status, data = self._admin_request("GET", "/props")
        if status is None or not (200 <= status < 300):
            return None
        try:
            parsed = json.loads(data)
        except (ValueError, UnicodeDecodeError) as exc:
            logger.warning("slot_admin: /props returned invalid JSON: %s", exc)
            return None
        return parsed if isinstance(parsed, dict) else None

    def models(self) -> dict[str, Any] | None:
        """GET ``/v1/models``. Returns ``None`` on any error."""
        status, data = self._admin_request("GET", "/v1/models")
        if status is None or not (200 <= status < 300):
            return None
        try:
            parsed = json.loads(data)
        except (ValueError, UnicodeDecodeError) as exc:
            logger.warning("slot_admin: /v1/models returned invalid JSON: %s", exc)
            return None
        return parsed if isinstance(parsed, dict) else None

    def upstream_metrics(self) -> dict[str, float] | None:
        """GET Prometheus-text ``/metrics`` as ``{sample name: value}`` (labels,
        if any, stay part of the name). Returns ``None`` on any error."""
        status, data = self._admin_request(
            "GET", "/metrics", timeout=min(self._cfg.admin_timeout_s, _RENDER_TIMEOUT_S)
        )
        if status is None or not (200 <= status < 300):
            return None
        out: dict[str, float] = {}
        for line in data.decode("utf-8", "ignore").splitlines():
            parts = line.split()
            if len(parts) < 2 or parts[0].startswith("#"):
                continue
            try:
                out[parts[0]] = float(parts[1])
            except ValueError:
                continue
        return out

    def upstream_alive(self) -> bool:
        """Cheap liveness probe of the upstream (llama-swap), never raises."""
        timeout = min(self._cfg.admin_timeout_s, 5.0)
        try:
            conn = http.client.HTTPConnection(
                self._cfg.upstream_host, self._cfg.upstream_port, timeout=timeout
            )
            try:
                conn.request("GET", "/health")
                resp = conn.getresponse()
                resp.read()
                return True
            finally:
                conn.close()
        except Exception:  # noqa: BLE001
            pass
        # Fall back to a bare TCP connect in case /health isn't implemented
        # upstream -- a refused/unreachable connection still means "down".
        try:
            with socket.create_connection(
                (self._cfg.upstream_host, self._cfg.upstream_port), timeout=timeout
            ):
                return True
        except OSError:
            return False
