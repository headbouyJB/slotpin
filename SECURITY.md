# Security policy

## Supported versions

`slotpin` is alpha software (`1.0.0aN`). Only the `main` branch receives
fixes; there are no maintained release branches.

## Threat model

Read this before exposing either service beyond the machine it runs on.

- **No authentication, no TLS, no rate limiting.** Neither the proxy
  (`src/slotpin/server.py`, default `127.0.0.1:8090`) nor the dashboard
  (`src/slotpin/dash.py`, default `127.0.0.1:8091`) checks who is calling. Any
  client that can reach the socket is fully trusted. Traffic is plain HTTP.
- **The proxy forwards everything else to the upstream.** Only `/_stats`,
  `/health` and `/ready` are answered locally, and chat/completion POSTs are
  handled by the pipeline. Every other path and method (GET, POST, PUT, DELETE,
  PATCH) goes through the pass-through handler to the upstream unchanged.
  Exposing the proxy therefore exposes the upstream's entire HTTP API,
  including model-management and slot endpoints such as `/slots` save/restore
  and `/props`, if the upstream serves them.
- **`/_stats` leaks configuration.** It reports configuration (filesystem paths,
  upstream hosts and ports) and aggregates over recent turns.
- **The dashboard is read-only but leaks operational detail.** It serves
  metrics history, slot state, memory/power/temperature readings and
  request-level timing. It never writes to the upstream, but it tells a reader
  when and how hard the box is being used.
- **Local files.** The proxy writes KV snapshots to `slot_save_path` and metrics
  to a JSONL file; the dashboard keeps a SQLite database. Protect those
  directories with ordinary filesystem permissions. The shipped systemd units
  run as an unprivileged user with a restricted `ReadWritePaths`.

## Safe deployment

- Keep the defaults (loopback) unless you have a reason not to.
- To serve other machines, put the services behind one of: a host firewall
  that allows only trusted addresses, a VPN such as WireGuard or Tailscale, or
  an authenticating reverse proxy. Do not publish either port to the internet.
- Bind to a specific interface, not `0.0.0.0`, when you do open it up.

Example: basic auth in front of the dashboard (nginx). Create the password
file with `htpasswd -c /etc/nginx/slotpin.htpasswd alice`.

```nginx
server {
    listen 443 ssl;
    server_name dash.example.com;
    # ssl_certificate / ssl_certificate_key ...
    location / {
        auth_basic "slotpin";
        auth_basic_user_file /etc/nginx/slotpin.htpasswd;
        proxy_pass http://127.0.0.1:8091;
    }
}
```

The same with Caddy (`caddy hash-password` produces the hash):

```caddy
dash.example.com {
    basic_auth {
        alice <bcrypt-hash>
    }
    reverse_proxy 127.0.0.1:8091
}
```

If you also front the proxy itself (port 8090), the chat endpoints stream
Server-Sent Events. In nginx set `proxy_buffering off;` (and
`proxy_read_timeout` high enough for long generations) on that location, or
tokens arrive in bursts and stall detection and client disconnect handling
behave badly. Caddy streams by default. Restrict the paths you forward: the
pass-through means an authenticated user can still reach the whole upstream API.

## Privacy properties

Both are enforced by tests:

- **Metrics and logs contain no prompt or reply text.** Per-turn records hold
  timings, token counts, hashes and labels only. The stall probe reads only
  numeric counters from the upstream's `/slots`. In
  `tests/test_fail_fast.py`, `test_slots_progress_copies_counters_only` shows
  the probe output never contains a `SECRET` prompt string present in the
  upstream's `/slots`, and `test_silent_stream_with_token_progress_is_not_a_stall`
  and `test_silent_stream_with_frozen_counters_is_a_stall` assert that string is
  absent from the logs, the metrics file and the failure (`turn_failed`) path.
- **The dashboard never serves upstream prompt text.** Upstream `/slots`
  entries include the prompt; the dashboard drops it on ingest. In
  `tests/test_dash_health.py`, `test_slot_fields_drop_prompt_text`,
  `test_health_v2_and_slot_fields` and
  `test_collector_path_never_stores_or_serves_prompt` assert that `"SECRET"` /
  `"TOP SECRET"` text reaches none of `/api/state`, the history API, the page
  HTML or stdout.

Note that request and response bodies do pass through the proxy in memory, and
the upstream itself may log or store them; these properties cover what
`slotpin` records and serves.

## Reporting a vulnerability

Please report privately using GitHub's private vulnerability reporting
(Security tab, "Report a vulnerability", i.e. a security advisory) on
`headbouyJB/slotpin`. Do not open a public issue for a vulnerability.
Include the version or commit, the configuration involved (redacted) and steps
to reproduce. This is a small project: expect a best-effort response.
