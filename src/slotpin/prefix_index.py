"""Longest-common-prefix index over disk KV snapshots.

WHY THIS EXISTS: disk snapshots are named by ``prefix_hash`` -- a hash of the
request's WHOLE system message (see classify.py). A snapshot is therefore only
ever restored when a later request's system message is byte-identical. Clients
that keep a large stable head plus a small volatile tail in the system message
(agent frameworks commonly do this: tools/instructions/AGENTS.md first, then the date,
the per-spawn task text and a runtime line) get a brand-new hash on every run,
so disk warmth never fires for them even though ~all of the prompt is unchanged.
Observed in production: nightly fan-out workers re-prefilled ~34.6K
tokens cold (260-590s) every night while last night's snapshots sat on disk.

WHAT IT DOES: alongside each saved ``<ph>.bin`` we write ``<ph>.idx.json`` --
a chain of hashes over fixed-size chunks of the RENDERED prompt (the exact text
the model saw, from llama-server ``/apply-template``, so tool definitions and
template framing are in their true positions). On a cold request we render its
prompt the same way and pick the snapshot sharing the longest leading run of
chunks. Restoring that snapshot and forwarding the request lets llama-server's
own prompt cache keep the common prefix and prefill only the divergent tail.
llama-server decides reuse token-by-token, so a wrong or partial pick can never
produce wrong output -- at worst it costs one wasted restore.

Chained hashes (``h_i = H(h_{i-1} || chunk_i)``) make equality at index ``i``
imply equality of the whole prefix up to ``i``, so the match is a simple
leading-run comparison. Only FULL chunks are hashed; the trailing partial chunk
is ignored (a match is always a whole number of chunks).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger("slotpin.prefix_index")

IDX_SUFFIX = ".idx.json"
_IDX_VERSION = 1


def chunk_hashes(text: str, chunk_chars: int) -> list[str]:
    """Chained hashes of each full ``chunk_chars``-sized chunk of ``text``."""
    out: list[str] = []
    prev = b""
    n_full = len(text) // chunk_chars
    for i in range(n_full):
        chunk = text[i * chunk_chars : (i + 1) * chunk_chars].encode("utf-8", "surrogatepass")
        prev = hashlib.sha1(prev + chunk).digest()
        out.append(prev.hex()[:16])
    return out


def common_chunks(a: list[str], b: list[str]) -> int:
    """Length of the leading run on which two chained-hash lists agree."""
    n = 0
    for x, y in zip(a, b, strict=False):
        if x != y:
            break
        n += 1
    return n


class PrefixIndex:
    """In-memory map ``prefix_hash -> chunk hashes``, persisted as sidecar files."""

    def __init__(self, save_dir: Path, chunk_chars: int) -> None:
        self._dir = save_dir
        self._chunk_chars = chunk_chars
        self._lock = threading.Lock()
        # ph -> {"hashes": [...], "serving_id": str, "head_id": str|None, "saved_at": float}
        self._entries: dict[str, dict[str, Any]] = {}

    def load_from_disk(self, skip_stem: str | None = None) -> int:
        """Rebuild the index from ``*.idx.json`` sidecars whose ``.bin`` still exists."""
        if not self._dir.exists():
            return 0
        loaded = 0
        try:
            paths = sorted(self._dir.glob(f"*{IDX_SUFFIX}"))
        except OSError as exc:
            logger.warning("prefix-index: scan of %s failed: %s", self._dir, exc)
            return 0
        for p in paths:
            ph = p.name[: -len(IDX_SUFFIX)]
            if ph == skip_stem or not (self._dir / f"{ph}.bin").exists():
                continue
            try:
                data = json.loads(p.read_text())
            except (OSError, ValueError) as exc:
                logger.warning("prefix-index: unreadable sidecar %s: %s", p.name, exc)
                continue
            if data.get("v") != _IDX_VERSION or data.get("chunk_chars") != self._chunk_chars:
                continue  # different chunking -> hashes are not comparable
            with self._lock:
                self._entries[ph] = {
                    "hashes": list(data.get("hashes") or []),
                    "serving_id": data.get("serving_id"),
                    "head_id": data.get("head_id"),
                    "saved_at": float(data.get("saved_at") or p.stat().st_mtime),
                }
            loaded += 1
        return loaded

    def record(
        self, ph: str, hashes: list[str], serving_id: str, head_id: str | None = None
    ) -> None:
        """Remember (and persist) the rendered-prompt chunk hashes for ``<ph>.bin``.

        ``head_id`` identifies the non-system part of the rendered head (the
        tool set): ``ph`` hashes only the system message, so it is recorded to
        tell whether a later same-``ph`` snapshot still starts with this head.
        """
        entry = {
            "hashes": list(hashes),
            "serving_id": serving_id,
            "head_id": head_id,
            "saved_at": time.time(),
        }
        with self._lock:
            self._entries[ph] = entry
        payload = {"v": _IDX_VERSION, "chunk_chars": self._chunk_chars, **entry}
        final = self._dir / f"{ph}{IDX_SUFFIX}"
        # Unique temp name: two slots can save the same ph concurrently.
        tmp: Path | None = None
        try:
            fd, tmp_name = tempfile.mkstemp(prefix=f".{ph}.", suffix=".tmp", dir=self._dir)
            tmp = Path(tmp_name)
            with os.fdopen(fd, "w") as fh:
                fh.write(json.dumps(payload, separators=(",", ":")))
            os.replace(tmp, final)
        except OSError:
            logger.exception("prefix-index: failed writing %s", final.name)
            if tmp is not None:
                tmp.unlink(missing_ok=True)

    def get(self, ph: str) -> dict[str, Any] | None:
        """Copy of ``ph``'s entry, or None."""
        with self._lock:
            e = self._entries.get(ph)
            return dict(e) if e is not None else None

    def forget_unless_same_head(
        self, ph: str, serving_id: str | None, head_id: str | None
    ) -> bool:
        """Atomically drop ``ph``'s entry unless it still describes the head of a
        re-saved ``<ph>.bin``: same model/config (``serving_id`` known and equal)
        and same tool set (``head_id`` known on both sides and equal -- entries
        written before head_id existed are dropped, since their tool set can
        never be checked). Returns True if dropped.
        Done under the lock so a concurrent indexed save of the same ``ph``
        can't be deleted by a stale check.
        """
        with self._lock:
            e = self._entries.get(ph)
            if e is None:
                return False
            same_model = serving_id is not None and e.get("serving_id") == serving_id
            old_head = e.get("head_id")
            same_head = head_id is not None and old_head == head_id
            if same_model and same_head:
                return False
            self._entries.pop(ph, None)
            (self._dir / f"{ph}{IDX_SUFFIX}").unlink(missing_ok=True)
            return True

    def forget(self, ph: str) -> None:
        """Drop ``ph`` from memory and disk (called when its snapshot is evicted)."""
        with self._lock:
            self._entries.pop(ph, None)
        (self._dir / f"{ph}{IDX_SUFFIX}").unlink(missing_ok=True)

    def best_match(
        self, hashes: list[str], serving_id: str, exclude: str | None = None
    ) -> tuple[str, int] | None:
        """Snapshot sharing the most leading chunks with ``hashes``.

        Only snapshots saved under the same ``serving_id`` (upstream model +
        serving config) are eligible. Ties go to the most recently saved.
        Returns ``(ph, n_common_chunks)`` or None when nothing shares a chunk.
        """
        best: tuple[str, int, float] | None = None
        with self._lock:
            items = list(self._entries.items())
        for ph, e in items:
            if ph == exclude or e.get("serving_id") != serving_id:
                continue
            if not (self._dir / f"{ph}.bin").exists():
                continue
            n = common_chunks(hashes, e["hashes"])
            if n == 0:
                continue
            key = (n, e["saved_at"])
            if best is None or key > (best[1], best[2]):
                best = (ph, n, e["saved_at"])
        return (best[0], best[1]) if best else None

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)
