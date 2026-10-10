# Contributing

Thanks for helping. This is a small, alpha-stage project; keep changes focused.

## Before you open a pull request

- **Open an issue first for anything bigger than a bug fix**, so the approach
  is agreed before you write it.
- **One change per pull request.** No drive-by refactors, renames or
  reformatting alongside a fix.
- **Say what you tested, and against what** (llama-server, Gufo, or only the
  fake upstream in the tests). "Not tested against a real server" is fine;
  leaving it out is not.
- **AI-assisted changes are welcome.** Say which tool or model you used and
  what you verified yourself. You are responsible for the change either way.
- Default behaviour should not change without a reason stated in the pull
  request.

The pull request template asks for these.

## Dev setup

Python 3.11 or newer. Node.js is optional but needed for the dashboard page tests.

```sh
# with uv
uv sync --extra dev

# or with pip
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev]'
```

The runtime has no dependencies. The dev extras are `pytest` and `ruff`.

## Running tests and lint

```sh
uv run --extra dev pytest -q -p no:cacheprovider      # or: pytest -q
ruff check .
```

CI (GitHub Actions) runs `ruff check .` and `pytest` on Python 3.11, 3.12 and
3.13, with Node installed. Ruff config is in `pyproject.toml`: line length 100,
rules `E, F, W, I, UP, B`.

### Dashboard page tests (Node)

The dashboard (`src/slotpin/dash.py`) renders one page with inline JavaScript.
`tests/test_dash_page.py` extracts the `<script>` body from
`dash.render_page()` and (a) runs `node --check` on it, and (b) evaluates the
chart-rendering functions in a Node `vm` context against a stub DOM and fixture
`/api/state` and `/api/history` payloads, then asserts on the produced SVG/HTML
(markers, node counts, alignment, escaping). These tests skip when
`shutil.which("node")` is `None`, so they are silent on machines without Node;
install it before changing the page script.

### Repository checks

- `tests/test_config_reference.py` keeps `docs/configuration.md` in sync with
  the `Config` dataclass. When you add, rename or remove a config key, update
  `docs/configuration.md` or this test fails.
- `tests/test_no_private_data.py` is a deny-list: no private addresses,
  hostnames or personal paths anywhere in the tree. Use documentation
  addresses (`192.0.2.x`) and `example.com` in docs, examples and tests.

## Seeing the dashboard with demo data

`python tools/demo_dashboard.py` starts the dashboard on invented data (fake
upstream, fake energy feed, seven days of synthetic metrics) on 127.0.0.1, with
no server needed; use it when changing the page. `python tools/capture_screenshots.py`
regenerates `docs/img/*.png` from it using headless Chrome. Both are stdlib-only
dev tools and everything they show is invented (the guard test covers them).

## Adding a config key

1. Add the field (with a default and a short comment) to the `Config`
   dataclass in `src/slotpin/config.py`.
2. Register it in `_SCALAR_FIELD_KINDS` so TOML and `SLOTPIN_*` env overrides
   convert it correctly.
3. Add validation (including cross-key invariants) so bad values produce an
   error naming the key.
4. Add a row to `docs/configuration.md`.
5. Add a commented example to `config.example.toml`.
6. Add a test and a `CHANGELOG.md` entry under `[Unreleased]`.

## Rules for pull requests

- **Stdlib-only runtime.** This is deliberate. Do not add runtime
  dependencies; dev-only tools go in the `dev` extra.
- **Never break streaming.** The proxy relays Server-Sent Events chunk by
  chunk and must stop the upstream when the client disconnects. Do not buffer a
  streamed response.
- **Fail open on observability, fail safe on the request path.** Metric, memory
  and energy sampling must never turn a working request into an error.
- **No prompt or reply text** in logs, metrics or the dashboard (see
  `SECURITY.md`).
- **Tests are required** for behaviour changes and bug fixes.
- **Small commits**, one logical change each, with messages saying why.
- **No private data**: no real hostnames, LAN addresses or personal paths.

## Running the proxy locally against a fake upstream

`tests/conftest.py` defines `FakeUpstream`, a stdlib HTTP server that mimics
`llama-server` (`/slots`, `/props`, chat completions with SSE, slot save/restore)
and is exposed as the `fake_upstream` fixture; `make_config` builds a
fast-tuned `Config` pointing at it. Copy how `tests/test_integration.py` wires
those together to start a real proxy in-process. Alternatively point
`slotpin-proxy --config your.toml` at a real `llama-server` on loopback, and
run `slotpin-proxy --config your.toml --check` first to validate the config.
