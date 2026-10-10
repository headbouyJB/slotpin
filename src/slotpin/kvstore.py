"""Disk-warmth KV store: persist (interactive lane) + adaptive (any recurring
prefix) snapshot save/restore, rate limiting, alias maintenance, and TTL GC.

Design notes:

- ``__init__(self, cfg, admin, pool, state, metrics)`` takes a
  :class:`~slotpin.pool.CacheRamPool` reference because ``is_cold(slot, ph)``
  is ``state.slot_last[slot] != ph AND ph not in pool``. The module dependency
  direction is ``config <- (classify, pool, slot_admin, metrics, state) <-
  (kvstore, scheduler, reload_detect, stats) <- server <- cli``.
- Alias maintenance is a real ``os.symlink`` (relative link, target is the
  bare ``<ph>.bin``/``.bin.ckpt`` filename), written via a temp-name-then-
  ``os.replace`` for atomicity -- NOT a file copy. Alias updates therefore
  never duplicate the (potentially large, recurrent state x checkpoints)
  snapshot bytes on disk.
- The "ONE retry ~2s later" busy-save hardening (``state.slot_busy`` refusal
  + ``threading.Timer(2.0, ...)`` retry) exists because a save issued while
  the slot is mid-generation would otherwise just time out silently after
  ``admin_timeout_s``. Restores are likewise deferred to post-admission (see
  ``server.py``'s module docstring) since restoring into a busy slot times out.
- ``ttl_evict`` internally throttles itself to run at most once per 60s
  wall-clock (an "opportunistic" cadence, since it is invoked from the same
  code path as every successful save) to avoid an ``os.stat``/glob sweep of
  ``slot_save_path`` on every single turn.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

from .config import Config
from .metrics import MetricsWriter
from .pool import CacheRamPool
from .prefix_index import PrefixIndex
from .slot_admin import SlotAdmin
from .state import ProxyState

logger = logging.getLogger("slotpin.kvstore")

_BUSY_RETRY_DELAY_S = 2.0
_TTL_SCAN_MIN_INTERVAL_S = 60.0


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


class KVStore:
    """Disk warmth: startup bootstrap, recurrence tracking, save/restore, TTL GC."""

    def __init__(
        self,
        cfg: Config,
        admin: SlotAdmin,
        pool: CacheRamPool,
        state: ProxyState,
        metrics: MetricsWriter,
    ) -> None:
        """Bind collaborators. See module docstring re: the added ``pool`` param."""
        self._cfg = cfg
        self._admin = admin
        self._pool = pool
        self._state = state
        self._metrics = metrics
        self._lock = threading.Lock()
        self._prefix_counts: dict[str, int] = {}
        self._warm_prefixes: set[str] = set()
        self._last_save_ts: dict[str, float] = {}
        self._last_ttl_scan = 0.0
        # Serialises save + index update per prefix hash, so two slots saving
        # the same ph can't interleave a snapshot with the other's index.
        self._ph_locks: dict[str, threading.Lock] = {}
        # One-time guard: warn if a save reports success but the file never
        # lands in slot_save_path (upstream --slot-save-path mismatch).
        self._warned_save_path_mismatch = False
        # Longest-common-prefix index over snapshots (prefix_index.py); only
        # populated/consulted when cfg.prefix_restore is on.
        self.index = PrefixIndex(cfg.slot_save_path, cfg.prefix_chunk_chars)
        if cfg.prefix_restore and cfg.kv_adaptive and cfg.recur_threshold > 1:
            logger.warning(
                "prefix_restore: recur_threshold=%d -- non-interactive prompts are only "
                "saved (and indexed) after recurring that many times with an IDENTICAL "
                "system message, so prompts whose system message changes every run "
                "(dates, per-run task text) will never be indexed. Set recur_threshold = 1 "
                "on boxes that serve such workloads.",
                cfg.recur_threshold,
            )

    def load_stats_from_disk(self) -> None:
        """Startup bootstrap: rebuild the warm-prefix set from ``*.bin`` files.

        Restart durability -- without this, every prefix would need to
        re-accumulate ``recur_threshold`` occurrences after every proxy
        restart even though its snapshot already exists on disk.
        """
        d = self._cfg.slot_save_path
        if not d.exists():
            logger.info("kvstore: slot_save_path %s does not exist yet; nothing to bootstrap", d)
            return
        loaded = 0
        try:
            for p in sorted(d.glob("*.bin")):
                stem = p.stem
                if stem == self._cfg.latest_alias:
                    continue  # the alias file, not a real prefix snapshot
                with self._lock:
                    self._prefix_counts[stem] = max(
                        self._prefix_counts.get(stem, 0), self._cfg.recur_threshold
                    )
                    self._warm_prefixes.add(stem)
                loaded += 1
        except OSError as exc:
            logger.warning("kvstore: failed scanning %s for bootstrap: %s", d, exc)
            return
        logger.info("kvstore: bootstrapped %d warm prefixes from disk (%s)", loaded, d)
        if self._cfg.prefix_restore:
            n_idx = self.index.load_from_disk(skip_stem=self._cfg.latest_alias)
            logger.info("kvstore: loaded %d prefix-index sidecars", n_idx)

    def record_prefix(self, ph: str) -> bool:
        """Record one occurrence of prefix ``ph``; True once count >= recur_threshold."""
        with self._lock:
            count = self._prefix_counts.get(ph, 0) + 1
            self._prefix_counts[ph] = count
            warm = count >= self._cfg.recur_threshold
            if warm:
                self._warm_prefixes.add(ph)
            return warm

    def warm_worthy(self, ph: str, interactive: bool) -> bool:
        """Is ``ph`` worth a disk snapshot? PURE query -- does NOT record.

        Interactive-lane prefixes are instantly warm-worthy (no
        recurrence threshold); everything else is warm-worthy once it has
        recurred to ``recur_threshold`` (tracked via :meth:`record_prefix`,
        which must be called exactly once per turn -- see server.py step 9).
        Previously this method itself called ``record_prefix`` and was invoked
        on both the restore-decision and save-gate paths, double-counting
        recurrence (threshold reached in 2 turns not 3) and making the
        "decide is read-only" claim false.
        """
        if interactive:
            return True
        with self._lock:
            return ph in self._warm_prefixes

    def is_cold(self, slot: int, ph: str) -> bool:
        """Generic cold-resume gate: slot occupant changed AND not in the cache-ram pool."""
        with self._state.lock:
            last = self._state.slot_last.get(slot)
        return last != ph and ph not in self._pool

    def is_cold_interactive(self, slot: int, ph: str) -> bool:
        """Interactive cold-resume gate: IGNORES the inferred pool.

        A pool "warm" guess was repeatedly wrong for the interactive lane in
        production (7-minute cold prefills despite the pool believing the
        session was still in host cache-ram) -- so the interactive gate only
        ever trusts ``slot_last``.
        """
        with self._state.lock:
            last = self._state.slot_last.get(slot)
        return last != ph

    def restore_blocking(
        self,
        slot: int,
        ph: str,
        latest_alias: str | None,
        *,
        try_exact: bool = True,
        prefix_candidate: tuple[str, int, int] | None = None,
    ) -> bool:
        """Blocking restore into ``slot``, first hit wins:

        1. ``<ph>.bin`` -- the exact-hash snapshot (skipped when ``try_exact``
           is False, i.e. no exact snapshot is known to exist);
        2. ``prefix_candidate`` = ``(source_ph, match_chars, prompt_chars)`` --
           the snapshot sharing the longest rendered prefix (prefix_index.py);
           llama-server then keeps only the common prefix and prefills the rest;
        3. ``<latest_alias>.bin``.

        Logs and records metrics + counters unconditionally for every attempt.
        """
        if try_exact:
            ok = self._restore_one(slot, ph, f"{ph}.bin", via_alias=False)
            if ok:
                self._pool.mark_restored(ph)
                return True
        if prefix_candidate is not None:
            src_ph, match_chars, prompt_chars = prefix_candidate
            ok = self._restore_one(
                slot, ph, f"{src_ph}.bin", via_alias=False,
                via_prefix=(src_ph, match_chars, prompt_chars),
            )
            if ok:
                return True
        if latest_alias:
            ok2 = self._restore_one(slot, ph, f"{latest_alias}.bin", via_alias=True)
            if ok2:
                self._pool.mark_restored(ph)
            return ok2
        return False

    def _snapshot_bytes(self, filename: str, ok: bool) -> int | None:
        """Size of the snapshot file for a metrics record: one ``os.stat``.

        None when the call failed or the file is not visible to the proxy
        (remote upstream, upstream ``--slot-save-path`` elsewhere, stat error).
        Never raises. Aliases are symlinks; stat follows them to the real file.
        """
        if not ok:
            return None
        try:
            return int(os.stat(self._cfg.slot_save_path / filename).st_size)
        except Exception:
            return None

    def _restore_one(
        self,
        slot: int,
        ph: str,
        filename: str,
        *,
        via_alias: bool,
        via_prefix: tuple[str, int, int] | None = None,
    ) -> bool:
        t0 = time.time()
        ok, detail = self._admin.restore(slot, filename)
        dur = time.time() - t0
        if via_alias:
            how = " (via latest_alias)"
        elif via_prefix is not None:
            how = f" (via longest prefix: {via_prefix[1]}/{via_prefix[2]} rendered chars shared)"
        else:
            how = ""
        logger.info(
            "kv-restore: slot=%s file=%s ok=%s detail=%s dur=%.3fs%s",
            slot,
            filename,
            ok,
            detail,
            dur,
            how,
        )
        record: dict[str, Any] = {
            "ts": _now_iso(),
            "kv_adaptive": "restore",
            "slot": slot,
            "prefix": ph,
            "file": filename,
            "ok": ok,
            "dur_s": round(dur, 3),
            "via_alias": via_alias,
            "bytes": self._snapshot_bytes(filename, ok),
        }
        if via_prefix is not None:
            record["via_prefix"] = True
            record["prefix_source"] = via_prefix[0]
            record["match_chars"] = via_prefix[1]
            record["prompt_chars"] = via_prefix[2]
        self._metrics.write(record)
        with self._state.lock:
            if via_prefix is not None:
                key = "kv_prefix_restores" if ok else "kv_prefix_restore_misses"
            else:
                key = "kv_restores" if ok else "kv_restore_misses"
            self._state.counters[key] = self._state.counters.get(key, 0) + 1
        return ok

    def save_async(self, slot: int, ph: str, latest_alias: str | None) -> None:
        """Fire-and-forget rate-limited save of ``slot``'s KV as ``<ph>.bin``.

        DEPRECATED under v8 pooled routing: the server now uses :meth:`save_now`
        (synchronous, owns-slot) so a save always captures the right turn's KV.
        This busy-check/retry variant is retained only for the busy-save-
        hardening test coverage; do NOT use it on the pooled path (after its
        ~2s retry the slot may hold a different turn's KV).

        Refuses (and reschedules exactly one retry ~2s later) while
        ``state.slot_busy[slot]`` is True. Maintains ``latest_alias`` and runs
        :meth:`ttl_evict` after a successful save.
        """
        now = time.time()
        with self._lock:
            last = self._last_save_ts.get(ph, 0.0)
            if now - last < self._cfg.prefix_save_min_interval_s:
                return

        def _attempt(is_retry: bool = False) -> None:
            with self._state.lock:
                busy = self._state.slot_busy.get(slot, False)
            if busy:
                if not is_retry:
                    logger.warning(
                        "kv-save: slot=%s busy, ph=%s -- deferring one retry in %.0fs",
                        slot,
                        ph,
                        _BUSY_RETRY_DELAY_S,
                    )
                    timer = threading.Timer(_BUSY_RETRY_DELAY_S, _attempt, kwargs={"is_retry": True})
                    timer.daemon = True
                    timer.start()
                else:
                    logger.warning(
                        "kv-save: slot=%s still busy on retry, ph=%s -- giving up this cycle",
                        slot,
                        ph,
                    )
                return
            filename = f"{ph}.bin"
            t0 = time.time()
            ok, detail = self._admin.save(slot, filename)
            dur = time.time() - t0
            logger.info(
                "kv-save: slot=%s file=%s ok=%s detail=%s dur=%.3fs", slot, filename, ok, detail, dur
            )
            self._metrics.write(
                {
                    "ts": _now_iso(),
                    "kv_adaptive": "save",
                    "slot": slot,
                    "prefix": ph,
                    "file": filename,
                    "ok": ok,
                    "dur_s": round(dur, 3),
                    "bytes": self._snapshot_bytes(filename, ok),
                }
            )
            if ok:
                with self._lock:
                    self._last_save_ts[ph] = now  # stamp only on success -> a failed save can retry
                with self._state.lock:
                    self._state.counters["kv_saves"] = self._state.counters.get("kv_saves", 0) + 1
                if latest_alias:
                    self._update_alias(ph, latest_alias)
                self.ttl_evict()

        threading.Thread(target=_attempt, daemon=True, name=f"kv-save-slot{slot}").start()

    def save_due(self, ph: str) -> bool:
        """Would :meth:`save_now` for ``ph`` pass its rate limit right now? (pure)"""
        with self._lock:
            last = self._last_save_ts.get(ph, 0.0)
        return time.time() - last >= self._cfg.prefix_save_min_interval_s

    def save_now(
        self,
        slot: int,
        ph: str,
        latest_alias: str | None,
        *,
        prefix_hashes: list[str] | None = None,
        serving_id: str | None = None,
        head_id: str | None = None,
    ) -> None:
        """Synchronous save of ``slot``'s KV as ``<ph>.bin`` -- for the pooled
        (v8 concurrency) path where the caller still OWNS the slot (generation
        settled, slot marked busy by this turn) and frees it only afterwards.

        No busy-check: unlike :meth:`save_async` (whose busy-check defers a save
        while some *other* turn holds the slot), here the caller is the owner and
        the KV is exactly this turn's, so we save immediately and block (~tens of
        ms) until it lands, guaranteeing the snapshot captures this turn's KV
        before the slot can be reused. Rate-limited identically to save_async.

        With ``prefix_hashes`` + ``serving_id`` (prefix_restore on), a
        successful save also writes the ``<ph>.idx.json`` sidecar so later
        requests can find this snapshot by longest rendered prefix.
        """
        with self._lock:
            ph_lock = self._ph_locks.setdefault(ph, threading.Lock())
        with ph_lock:
            self._save_now_locked(slot, ph, latest_alias, prefix_hashes, serving_id, head_id)

    def _save_now_locked(
        self,
        slot: int,
        ph: str,
        latest_alias: str | None,
        prefix_hashes: list[str] | None,
        serving_id: str | None,
        head_id: str | None,
    ) -> None:
        now = time.time()
        with self._lock:
            last = self._last_save_ts.get(ph, 0.0)
            if now - last < self._cfg.prefix_save_min_interval_s:
                return

        filename = f"{ph}.bin"
        t0 = time.time()
        ok, detail = self._admin.save(slot, filename)
        dur = time.time() - t0
        logger.info(
            "kv-save(sync): slot=%s file=%s ok=%s detail=%s dur=%.3fs", slot, filename, ok, detail, dur
        )
        self._metrics.write(
            {
                "ts": _now_iso(),
                "kv_adaptive": "save",
                "slot": slot,
                "prefix": ph,
                "file": filename,
                "ok": ok,
                "dur_s": round(dur, 3),
                "bytes": self._snapshot_bytes(filename, ok),
            }
        )
        if ok:
            # Integrity guard: llama-server reported the save OK, so the file
            # must now be under slot_save_path (slotpin globs it for warm-lane
            # bootstrap + writes the alias symlink here). If it is absent, the
            # upstream --slot-save-path differs from slot_save_path -- the exact
            # misconfig that silently sends KV to tmpfs/RAM. Warn once.
            expected = self._cfg.slot_save_path / filename
            if not self._warned_save_path_mismatch and not expected.exists():
                self._warned_save_path_mismatch = True
                logger.warning(
                    "kv-save: save reported OK but %s is absent -- llama-server's "
                    "--slot-save-path likely differs from slot_save_path (%s). Warm-lane "
                    "bootstrap will be empty and KV may be landing elsewhere (e.g. tmpfs/RAM). "
                    "Point the upstream --slot-save-path at slot_save_path on persistent disk.",
                    expected,
                    self._cfg.slot_save_path,
                )
            with self._lock:
                self._last_save_ts[ph] = now  # stamp only on success -> a failed save can retry
            with self._state.lock:
                self._state.counters["kv_saves"] = self._state.counters.get("kv_saves", 0) + 1
            if prefix_hashes and serving_id:
                self.index.record(ph, prefix_hashes, serving_id, head_id)
            elif self._cfg.prefix_restore:
                # This save couldn't be indexed (render failed / prompt < 1 chunk).
                # KEEP an existing <ph>.idx.json only if the re-saved KV provably
                # starts with the head it describes: same ph (= same system
                # message), same model/config (serving_id) and same tool set
                # (head_id) -- the template renders tools + system first. That is
                # the multi-turn worker case (~94% of the prompt); dropping the
                # index there lost every worker index. Otherwise
                # (model or tools changed, or unknown) the old index would
                # advertise KV the file no longer holds -> forget it.
                self.index.forget_unless_same_head(ph, serving_id, head_id)
            if latest_alias:
                self._update_alias(ph, latest_alias)
            self.ttl_evict()

    def _update_alias(self, ph: str, alias: str) -> None:
        """Maintain ``<alias>.bin`` (+ ``.ckpt`` sidecar) as a SYMLINK to ``<ph>.bin``.

        A relative ``os.symlink(fname, tmp)`` then
        ``os.replace(tmp, final)`` per extension, for each of ``.bin`` and
        ``.bin.ckpt``. The temp-name-then-replace makes each individual
        symlink swap atomic; a concurrent restore never sees a half-written
        link. This intentionally does NOT copy bytes (see module docstring).
        """
        try:
            for ext, target_name in ((".bin", f"{ph}.bin"), (".bin.ckpt", f"{ph}.bin.ckpt")):
                target_path = self._cfg.slot_save_path / target_name
                final = self._cfg.slot_save_path / f"{alias}{ext}"
                if ext == ".bin.ckpt" and not target_path.exists():
                    # No .ckpt sidecar for this snapshot (not every save has one) --
                    # drop a stale alias .ckpt rather than leave it pointing at
                    # the wrong lineage.
                    final.unlink(missing_ok=True)
                    continue
                tmp = final.with_name(final.name + ".tmp")
                if tmp.exists() or tmp.is_symlink():
                    tmp.unlink()
                tmp.symlink_to(target_name)  # relative link, bare filename as target
                tmp.replace(final)
            logger.info("kv-alias: updated %s.bin(.ckpt) -> %s.bin", alias, ph)
        except OSError:
            logger.exception("kv-alias: update failed for alias=%s ph=%s", alias, ph)

    def ttl_evict(self) -> None:
        """Opportunistic snapshot GC: delete ``*.bin`` (+ ``.ckpt``) files older
        than ``snapshot_ttl_h``. Self-throttled to avoid a disk sweep on every call."""
        now = time.time()
        with self._lock:
            if now - self._last_ttl_scan < _TTL_SCAN_MIN_INTERVAL_S:
                return
            self._last_ttl_scan = now

        d = self._cfg.slot_save_path
        if not d.exists():
            return
        ttl_s = self._cfg.snapshot_ttl_h * 3600.0
        try:
            candidates = list(d.glob("*.bin"))
        except OSError as exc:
            logger.warning("kvstore: ttl_evict scan of %s failed: %s", d, exc)
            return

        for p in candidates:
            try:
                if p.stem == self._cfg.latest_alias:
                    continue
                age_s = now - p.stat().st_mtime
                if age_s <= ttl_s:
                    continue
                p.unlink(missing_ok=True)
                ckpt = p.with_name(p.name + ".ckpt")
                ckpt.unlink(missing_ok=True)
                self.index.forget(p.stem)
                with self._lock:
                    self._prefix_counts.pop(p.stem, None)
                    self._warm_prefixes.discard(p.stem)
                    self._last_save_ts.pop(p.stem, None)
                    # _ph_locks entries are deliberately kept: popping one a
                    # concurrent save holds would split its serialisation.
                logger.info("kv-ttl-evict: removed %s (age=%.1fh)", p.name, age_s / 3600.0)
            except OSError:
                logger.exception("kv-ttl-evict: failed to evict %s", p)

    def snapshot_stats(self) -> dict[str, Any]:
        """Snapshot for the ``/_stats`` "adaptive" block."""
        with self._lock:
            now = time.time()
            return {
                "tracked_prefixes": len(self._prefix_counts),
                "warm_prefixes": len(self._warm_prefixes),
                "prefix_counts": dict(self._prefix_counts),
                "last_save_age_s": {
                    ph: round(now - ts, 1) for ph, ts in self._last_save_ts.items()
                },
            }
