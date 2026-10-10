"""Pure request classification: body/system text -> lane, tier, slot, label.

``Classifier.classify`` MUST be pure (invariant): it never
mutates shared state (no ``state.slot_last`` writes here -- those happen
post-admission in ``server.py``). It only reads the immutable ``Config`` it
was built with plus the request body/obj passed in.

Design notes:

- ``prefix_hash`` is byte-identical to ``fp``: it is the exact same
  computation as ``fingerprint(obj, body_text)``'s ``fp`` -- same system-text
  join, same ``body_text[:4000]`` fallback, same ``sha1(...)[:8]``
  truncation. fp and prefix_hash are one identifier under two names, one for
  routing/pool bookkeeping, one for the adaptive recurrence key.
  ``prefix_hash_chars`` is the fallback-slice length (the ONLY truncation,
  applied only when there is no system message, not to the hash output).
- Demote-rule semantics (label <- demote.name, tier <- demote.tier,
  interactive forced False, lane <- matched_lane.demote_to if set) generalise
  the original proxy's inline heartbeat/ops-hook detection (first user-message
  regex + ``len(messages) <= 40``) into the config-driven
  ``LaneRule``/``DemoteRule`` shape.
- The scheduler tier is chosen by workload LABEL, not just lane: an
  interactive lane is HIGH, heartbeat/ops hooks are LOW (demote), a configured
  set of scheduled-delivery labels is MEDIUM, background-synthesis labels are
  LOW, and everything else defaults to MEDIUM ("unknown == MEDIUM"). This is
  expressed via ``LabelRule.tier`` (per-label override, mirrors demote's
  tier-override shape) plus ``Config.default_tier`` (the "unknown -> MEDIUM"
  fallback).
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from typing import Any

from .config import Config, DemoteRule, LabelRule, LaneRule

logger = logging.getLogger("slotpin.classify")


@dataclass(frozen=True)
class Classification:
    """Immutable result of classifying one chat-completions request."""

    lane: str
    label: str
    fp: str
    prefix_hash: str
    tier: int
    interactive: bool
    slot: int


def _extract_text(content: Any) -> str:
    """Extract plain text from an OpenAI-style message ``content`` field.

    Handles both the plain-string form and the list-of-content-blocks form
    (each block a dict with a "text" key) used by OpenAI-style
    clients.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        # A block's "text" may be missing OR explicitly null; coerce non-str to
        # "" so a `{"text": null}` block can't raise TypeError in the join.
        parts = []
        for block in content:
            if isinstance(block, dict):
                t = block.get("text")
                parts.append(t if isinstance(t, str) else "")
        return " ".join(parts)
    return ""


def _system_text(messages: list[dict]) -> str:
    return "".join(
        _extract_text(m.get("content", "")) for m in messages if m.get("role") == "system"
    )


def _first_user_text(messages: list[dict]) -> str:
    for m in messages:
        if m.get("role") == "user":
            return _extract_text(m.get("content", ""))
    return ""


class Classifier:
    """Classifies chat-completions requests into a lane/tier/slot decision."""

    def __init__(self, cfg: Config) -> None:
        """Pre-compile all configured lane/demote/label regexes."""
        self._cfg = cfg
        self._lanes: list[tuple[LaneRule, list[re.Pattern[str]], list[re.Pattern[str]]]] = [
            (
                lane,
                [re.compile(p) for p in lane.match],
                [re.compile(p) for p in lane.match_system],
            )
            for lane in cfg.lanes
        ]
        self._demote: list[tuple[DemoteRule, re.Pattern[str]]] = [
            (d, re.compile(d.first_user_match)) for d in cfg.demote
        ]
        self._labels: list[tuple[LabelRule, re.Pattern[str]]] = [
            (lb, re.compile(lb.match)) for lb in cfg.labels
        ]

    def classify(self, obj: dict, body_text: str) -> Classification:
        """Classify one request. Pure: reads only ``obj``/``body_text``/cfg."""
        messages = obj.get("messages") or []
        system_text = _system_text(messages)
        # prefix_hash_chars names the fallback-slice length (4000 by
        # default); config-driven rather than hardcoded.
        text_for_hash = system_text if system_text else body_text[: self._cfg.prefix_hash_chars]

        # prefix_hash() is byte-for-byte the same computation as
        # fingerprint()'s fp (see module docstring) -- both identify "this
        # turn's workload" via the system-message hash, with the same
        # 4000-char fallback slice used only when there is no system message.
        fp = hashlib.sha1(text_for_hash.encode("utf-8", "ignore")).hexdigest()[:8]
        prefix_hash = fp

        matched_lane: LaneRule | None = None
        for lane, match_res, match_sys_res in self._lanes:
            if all(r.search(body_text) for r in match_res) and all(
                r.search(system_text) for r in match_sys_res
            ):
                matched_lane = lane
                break

        if matched_lane is None:
            lane_name = "default"
            tier = self._cfg.default_tier
            interactive = False
            demote_to: str | None = None
        else:
            lane_name = matched_lane.name
            tier = matched_lane.tier
            interactive = matched_lane.interactive
            demote_to = matched_lane.demote_to

        demoted_label: str | None = None
        if self._demote:
            first_user = _first_user_text(messages)
            for d, pattern in self._demote:
                if pattern.search(first_user) and len(messages) <= d.max_messages:
                    demoted_label = d.name
                    tier = d.tier
                    interactive = False
                    if demote_to:
                        lane_name = demote_to
                    break

        if demoted_label is not None:
            label = demoted_label
        else:
            label = None
            for lb, pattern in self._labels:
                if pattern.search(body_text):
                    label = lb.label
                    # Tier is ultimately chosen by
                    # workload LABEL, not just lane -- e.g. background
                    # synthesis labels are LOW even though they share
                    # the same (unmatched) lane as everything else. A
                    # LabelRule's tier, when set, overrides the lane/default
                    # tier the same way a demote rule would.
                    if lb.tier is not None:
                        tier = lb.tier
                    break
            if label is None:
                label = lane_name if matched_lane is not None else f"other:{fp}"

        # VESTIGIAL under v8 pooled routing: the server no longer reads
        # Classification.slot -- it selects a slot from the pool post-admission
        # (server._select_slot), so `mode="dual"`/`interactive_slot` no longer
        # affect routing. Kept for now for metrics/back-compat; a follow-up
        # removes the field, `mode`, and the *_slot config keys. (See the v8
        # audit debt item.)
        if self._cfg.mode == "dual":
            slot = self._cfg.interactive_slot if interactive else self._cfg.default_slot
        else:
            slot = self._cfg.default_slot

        return Classification(
            lane=lane_name,
            label=label,
            fp=fp,
            prefix_hash=prefix_hash,
            tier=tier,
            interactive=interactive,
            slot=slot,
        )
