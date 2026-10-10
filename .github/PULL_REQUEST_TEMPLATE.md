<!--
Thanks for the PR. Small and focused gets reviewed fastest:
one change per PR, no drive-by refactors or reformatting.
For anything larger than a bug fix, please open an issue first so we can agree the approach.
-->

## What and why

<!-- What does this change, and what problem does it solve? Link the issue if there is one. -->

## How it was tested

<!-- What you actually ran, and against what. "Not tested against a real server" is a fine answer; say so. -->

- Upstream: <!-- llama-server / Gufo / fake upstream only -->
- Commands and results:

## Behaviour and config

- [ ] No change to default behaviour, **or** the change is described above
- [ ] New or changed config keys are in `docs/configuration.md` and `config.example.toml`

## Checklist

- [ ] `ruff check .` and `pytest` pass locally
- [ ] Tests added or updated for the change
- [ ] Docs updated where behaviour changed
- [ ] `CHANGELOG.md` entry under `[Unreleased]`
- [ ] No private data (real hostnames, LAN addresses, personal paths, prompt or reply text)
- [ ] Runtime stays stdlib-only; the streaming path is not buffered

## AI assistance

<!-- Fine to use, just say so: which tool or model, and what you checked yourself. -->
