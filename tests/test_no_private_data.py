"""Fail if deployment-specific or personal data reappears in tracked text files.

Deny patterns are assembled from fragments so this file does not match itself
(it is also skipped explicitly).
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
THIS = Path(__file__).resolve()

_SKIP_DIRS = {".git", ".venv", "node_modules", "__pycache__", "dist", "build"}
_SKIP_FILES = {"uv.lock"}
_NAME_EXEMPT_FILES: set[str] = set()
_NAME_PATTERNS = {"personal first name", "personal surname"}
_SECRET_PATTERNS = {"secret shape", "private key", "email address", "session id"}


def _p(*parts: str) -> str:
    return "".join(parts)


_HOSTS = "|".join(
    [_p("ai", "beast"), _p("ai", "hydra"), _p("warden", "mac"), _p("la", "don"),
     _p("war", "den"), _p("her", "mes"), _p("ci", "ru"), _p("bos", "game")]
)

DENY: dict[str, re.Pattern[str]] = {
    "private LAN address": re.compile(_p(r"192\.", r"168\.", r"\d+\.\d+")),
    "tailnet/CGNAT address": re.compile(r"\b100\.(6[4-9]|[7-9]\d|1[01]\d|12[0-7])\.\d+\.\d+\b"),
    "tailnet hostname": re.compile(_p(r"\.ts", r"\.net\b"), re.I),
    "owner hostname": re.compile(rf"\b({_HOSTS})\b", re.I),
    "home directory path": re.compile(_p(r"/ho", r"me/[a-z][a-z0-9_-]*"), re.I),
    "macOS user path": re.compile(_p(r"/Us", r"ers/[a-z]"), re.I),
    "user-data mount": re.compile(_p(r"/mnt/", r"user-data")),
    "email address": re.compile(r"[A-Za-z0-9._%+-]+@(gmail|icloud|outlook|yahoo)\.com", re.I),
    "personal first name": re.compile(_p(r"\bje", r"sse\b"), re.I),
    "personal surname": re.compile(_p(r"\bbr", r"yce\b"), re.I),
    "assistant session link": re.compile(_p(r"claude\.ai", r"/code"), re.I),
    "session id": re.compile(_p(r"session_", r"01[A-Za-z0-9]{20,}")),
    "MAC address": re.compile(r"\b([0-9a-f]{2}:){5}[0-9a-f]{2}\b", re.I),
    "secret shape": re.compile(
        "|".join(
            [
                _p("sk", "-[A-Za-z0-9]{16,}"),
                _p("hf", "_[A-Za-z0-9]{16,}"),
                _p("gh", "p_[A-Za-z0-9]{16,}"),
                _p("xo", r"x[bp]-"),
                _p("AK", "IA[A-Z0-9]{12,}"),
            ]
        )
    ),
    "private key": re.compile(_p("-----BEGIN [A-Z ]*", "PRIVATE KEY")),
    "private gateway name": re.compile(_p(r"Open", "Claw"), re.I),
    "private benchmark name": re.compile(_p(r"Halo", "Bench"), re.I),
}

ALLOW_LINE = re.compile(
    r"192\.0\.2\.|198\.51\.100\.|203\.0\.113\.|example\.(com|net|org)", re.I
)


def _git_files() -> list[Path] | None:
    try:
        out = subprocess.run(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT, capture_output=True, check=True, timeout=30
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    names = [n for n in out.decode("utf-8", "replace").split("\0") if n]
    return [ROOT / n for n in names] if names else None


def _walk_files() -> list[Path]:
    found: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS and not d.endswith(".egg-info")]
        found.extend(Path(dirpath) / f for f in filenames)
    return found


def _candidate_files() -> list[Path]:
    files = _git_files() or _walk_files()
    return [f for f in files if f.is_file() and f.resolve() != THIS and f.name not in _SKIP_FILES]


def _scan(path: Path, text: str) -> list[str]:
    rel = path.relative_to(ROOT).as_posix()
    hits: list[str] = []
    for lineno, line in enumerate(text.splitlines(), 1):
        if ALLOW_LINE.search(line):
            continue
        for name, rx in DENY.items():
            if name in _NAME_PATTERNS and path.name in _NAME_EXEMPT_FILES:
                continue
            m = rx.search(line)
            if not m:
                continue
            if name in _SECRET_PATTERNS:
                hits.append(f"{rel}:{lineno}: {name}")
            else:
                a = max(0, m.start() - 10)
                ctx = line[a : a + 20].strip()
                hits.append(f"{rel}:{lineno}: {name} [{ctx}]")
    return hits


def test_no_private_data_in_tracked_files():
    violations: list[str] = []
    for path in _candidate_files():
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        violations.extend(_scan(path, text))
    assert not violations, "private/owner-specific data found:\n" + "\n".join(violations)


def test_scanner_detects_and_allows():
    p = ROOT / "x.txt"
    assert _scan(p, _p("host ", "192.", "168.1.5"))
    assert not _scan(p, "host 192.0.2.5")
    assert _scan(p, _p("ssh ai", "beast"))
    assert not _scan(p, "ssh box-a")
    assert _scan(p, "key " + _p("sk", "-") + "A" * 20) == ["x.txt:1: secret shape"]
    assert _scan(ROOT / "LICENSE", _p("Copyright Je", "sse Br", "yce"))
    assert _scan(ROOT / "CHANGELOG.md", _p("Je", "sse"))
