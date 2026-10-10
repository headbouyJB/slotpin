"""Tests for slotpin.classify: pure request -> Classification derivation."""

from __future__ import annotations

import hashlib

from slotpin.classify import Classifier
from slotpin.config import Config, DemoteRule, LabelRule, LaneRule


def _body(messages, **extra):
    obj = {"model": "x", "messages": messages, **extra}
    return obj


def _cfg(**kw):
    return Config(**kw)


# ---------------------------------------------------------------------------
# system text extraction: str vs list-of-blocks
# ---------------------------------------------------------------------------


def test_system_text_extraction_str_form():
    cfg = _cfg()
    clf = Classifier(cfg)
    messages = [
        {"role": "system", "content": "SYS TEXT"},
        {"role": "user", "content": "hi"},
    ]
    obj = _body(messages)
    cls = clf.classify(obj, str(obj))
    expected_fp = hashlib.sha1(b"SYS TEXT").hexdigest()[:8]
    assert cls.fp == expected_fp


def test_system_text_extraction_list_of_blocks_form():
    cfg = _cfg()
    clf = Classifier(cfg)
    messages = [
        {
            "role": "system",
            "content": [{"type": "text", "text": "SYS"}, {"type": "text", "text": "TEXT"}],
        },
        {"role": "user", "content": "hi"},
    ]
    obj = _body(messages)
    cls = clf.classify(obj, str(obj))
    expected_fp = hashlib.sha1(b"SYS TEXT").hexdigest()[:8]
    assert cls.fp == expected_fp


def test_str_and_list_forms_agree_when_equivalent():
    cfg = _cfg()
    clf = Classifier(cfg)
    m_str = [{"role": "system", "content": "A B"}, {"role": "user", "content": "hi"}]
    m_list = [
        {"role": "system", "content": [{"text": "A"}, {"text": "B"}]},
        {"role": "user", "content": "hi"},
    ]
    cls_str = clf.classify(_body(m_str), str(_body(m_str)))
    cls_list = clf.classify(_body(m_list), str(_body(m_list)))
    assert cls_str.fp == cls_list.fp


def test_no_system_message_falls_back_to_body_text():
    cfg = _cfg()
    clf = Classifier(cfg)
    messages = [{"role": "user", "content": "hi"}]
    obj = _body(messages)
    body_text = str(obj)
    cls = clf.classify(obj, body_text)
    expected_fp = hashlib.sha1(body_text[:4000].encode("utf-8", "ignore")).hexdigest()[:8]
    assert cls.fp == expected_fp


# ---------------------------------------------------------------------------
# lane AND-matching
# ---------------------------------------------------------------------------


def test_lane_requires_all_match_regexes():
    cfg = _cfg(lanes=[LaneRule(name="chat", match=["channel=chat", "## Execution Bias"])])
    clf = Classifier(cfg)
    messages = [{"role": "user", "content": "channel=chat only"}]
    cls = clf.classify(_body(messages), "channel=chat only, no marker")
    assert cls.lane == "default"  # missing "## Execution Bias" -> no match


def test_lane_matches_when_all_regexes_hit():
    cfg = _cfg(lanes=[LaneRule(name="chat", match=["channel=chat", "## Execution Bias"])])
    clf = Classifier(cfg)
    body_text = "channel=chat\n## Execution Bias\nsome content"
    cls = clf.classify(_body([{"role": "user", "content": body_text}]), body_text)
    assert cls.lane == "chat"


def test_lane_match_system_also_required():
    cfg = _cfg(
        lanes=[
            LaneRule(
                name="special",
                match=["trigger"],
                match_system=["SPECIAL-SYS-MARKER"],
            )
        ]
    )
    clf = Classifier(cfg)
    messages_without_marker = [
        {"role": "system", "content": "plain system"},
        {"role": "user", "content": "trigger"},
    ]
    body_text = "trigger plain system"
    cls = clf.classify(_body(messages_without_marker), body_text)
    assert cls.lane == "default"

    messages_with_marker = [
        {"role": "system", "content": "SPECIAL-SYS-MARKER"},
        {"role": "user", "content": "trigger"},
    ]
    body_text2 = "trigger SPECIAL-SYS-MARKER"
    cls2 = clf.classify(_body(messages_with_marker), body_text2)
    assert cls2.lane == "special"


def test_no_lanes_configured_everything_is_default_tier():
    """The scheduler falls back to the MEDIUM tier for an unrecognised
    workload ("sane default: unknown == MEDIUM"); Config.default_tier (10)
    is implemented via ``default_tier`` rather than a hardcoded 1."""
    cfg = _cfg(lanes=[])
    clf = Classifier(cfg)
    cls = clf.classify(_body([{"role": "user", "content": "anything"}]), "anything")
    assert cls.lane == "default"
    assert cls.tier == cfg.default_tier == 10
    assert cls.interactive is False


def test_default_tier_is_configurable():
    cfg = _cfg(lanes=[], default_tier=7)
    clf = Classifier(cfg)
    cls = clf.classify(_body([{"role": "user", "content": "anything"}]), "anything")
    assert cls.tier == 7


# ---------------------------------------------------------------------------
# demote rules: heartbeat first-user-match + max_messages boundary
# ---------------------------------------------------------------------------


def _interactive_cfg(max_messages=4):
    return _cfg(
        lanes=[
            LaneRule(
                name="interactive",
                match=["channel=chat"],
                tier=0,
                interactive=True,
                demote_to="default",
            )
        ],
        demote=[
            DemoteRule(
                name="heartbeat", first_user_match="heartbeat poll", max_messages=max_messages, tier=2
            )
        ],
    )


def test_demote_fires_at_boundary_max_messages_inclusive():
    cfg = _interactive_cfg(max_messages=4)
    clf = Classifier(cfg)
    messages = [
        {"role": "user", "content": "heartbeat poll"},
        {"role": "assistant", "content": "ack"},
        {"role": "user", "content": "more"},
        {"role": "assistant", "content": "ack2"},
    ]
    assert len(messages) == 4
    body_text = "channel=chat heartbeat poll"
    cls = clf.classify(_body(messages), body_text)
    assert cls.label == "heartbeat"
    assert cls.tier == 2
    assert cls.interactive is False
    assert cls.lane == "default"  # demote_to applied


def test_demote_does_not_fire_above_max_messages():
    cfg = _interactive_cfg(max_messages=4)
    clf = Classifier(cfg)
    messages = [
        {"role": "user", "content": "heartbeat poll"},
        {"role": "assistant", "content": "ack"},
        {"role": "user", "content": "more"},
        {"role": "assistant", "content": "ack2"},
        {"role": "user", "content": "extra"},
    ]
    assert len(messages) == 5
    body_text = "channel=chat heartbeat poll"
    cls = clf.classify(_body(messages), body_text)
    assert cls.label != "heartbeat"
    assert cls.tier == 0
    assert cls.interactive is True
    assert cls.lane == "interactive"


def test_demote_requires_first_user_message_match():
    cfg = _interactive_cfg(max_messages=40)
    clf = Classifier(cfg)
    messages = [
        {"role": "user", "content": "a real conversation opener"},
    ]
    body_text = "channel=chat a real conversation opener"
    cls = clf.classify(_body(messages), body_text)
    assert cls.label != "heartbeat"
    assert cls.interactive is True


# ---------------------------------------------------------------------------
# label rules
# ---------------------------------------------------------------------------


def test_label_rule_first_match_wins():
    cfg = _cfg(
        lanes=[LaneRule(name="x", match=["trigger"])],
        labels=[
            LabelRule(match="synthesis", label="synthesis"),
            LabelRule(match="synth", label="wrong-should-not-fire-second"),
        ],
    )
    clf = Classifier(cfg)
    cls = clf.classify(_body([{"role": "user", "content": "trigger synthesis run"}]), "trigger synthesis run")
    assert cls.label == "synthesis"


def test_label_falls_back_to_lane_name_when_no_label_rule_matches():
    cfg = _cfg(lanes=[LaneRule(name="interactive", match=["trigger"])], labels=[])
    clf = Classifier(cfg)
    cls = clf.classify(_body([{"role": "user", "content": "trigger"}]), "trigger")
    assert cls.label == "interactive"


def test_label_falls_back_to_other_fp_when_no_lane_matched():
    cfg = _cfg(lanes=[LaneRule(name="interactive", match=["nomatch-marker"])], labels=[])
    clf = Classifier(cfg)
    body_text = "unrelated content"
    cls = clf.classify(_body([{"role": "user", "content": body_text}]), body_text)
    assert cls.lane == "default"
    assert cls.label == f"other:{cls.fp}"


# ---------------------------------------------------------------------------
# label-based tier override (label-keyed LOW tier)
# ---------------------------------------------------------------------------


def test_label_rule_tier_overrides_default_tier():
    cfg = _cfg(
        default_tier=10,
        labels=[LabelRule(match="dreaming", label="dreaming", tier=20)],
    )
    clf = Classifier(cfg)
    cls = clf.classify(_body([{"role": "user", "content": "dreaming run"}]), "dreaming run")
    assert cls.label == "dreaming"
    assert cls.tier == 20  # LabelRule.tier wins over default_tier


def test_label_rule_without_tier_keeps_default_tier():
    cfg = _cfg(
        default_tier=10,
        labels=[LabelRule(match="commute", label="commute")],  # tier=None
    )
    clf = Classifier(cfg)
    cls = clf.classify(_body([{"role": "user", "content": "commute update"}]), "commute update")
    assert cls.label == "commute"
    assert cls.tier == 10  # no override -> falls through to default_tier


def test_label_rule_tier_does_not_apply_when_a_lane_already_set_one():
    """A LabelRule can still override an explicit lane tier -- last word on
    tier belongs to whichever of lane/label matched, same as the original proxy where
    label is the finer-grained signal layered on top of lane/route."""
    cfg = _cfg(
        lanes=[LaneRule(name="bulk", match=["BULKTAG"], tier=5)],
        labels=[LabelRule(match="dreaming", label="dreaming", tier=20)],
    )
    clf = Classifier(cfg)
    body_text = "BULKTAG dreaming payload"
    cls = clf.classify(_body([{"role": "user", "content": body_text}]), body_text)
    assert cls.lane == "bulk"
    assert cls.label == "dreaming"
    assert cls.tier == 20


# ---------------------------------------------------------------------------
# fp / prefix_hash stability and difference
# ---------------------------------------------------------------------------


def test_fp_stable_across_identical_calls():
    cfg = _cfg()
    clf = Classifier(cfg)
    messages = [{"role": "system", "content": "same system text"}, {"role": "user", "content": "hi"}]
    obj = _body(messages)
    body_text = str(obj)
    cls1 = clf.classify(obj, body_text)
    cls2 = clf.classify(dict(obj), body_text)
    assert cls1.fp == cls2.fp
    assert cls1.prefix_hash == cls2.prefix_hash


def test_fp_differs_for_different_system_text():
    cfg = _cfg()
    clf = Classifier(cfg)
    m1 = [{"role": "system", "content": "system A"}, {"role": "user", "content": "hi"}]
    m2 = [{"role": "system", "content": "system B"}, {"role": "user", "content": "hi"}]
    cls1 = clf.classify(_body(m1), str(_body(m1)))
    cls2 = clf.classify(_body(m2), str(_body(m2)))
    assert cls1.fp != cls2.fp
    assert cls1.prefix_hash != cls2.prefix_hash


def test_prefix_hash_is_byte_identical_to_fp():
    """prefix_hash() is literally the same computation as fp (see
    classify.py's module docstring): both are an identical
    sha1(system-text or body_text[:4000])[:8]. There is no separate
    prefix_hash-only truncation window."""
    cfg = _cfg()
    clf = Classifier(cfg)
    m1 = [{"role": "system", "content": "AAAAA-tail-one"}, {"role": "user", "content": "hi"}]
    m2 = [{"role": "system", "content": "AAAAA-tail-two"}, {"role": "user", "content": "hi"}]
    cls1 = clf.classify(_body(m1), str(_body(m1)))
    cls2 = clf.classify(_body(m2), str(_body(m2)))
    assert cls1.prefix_hash == cls1.fp
    assert cls2.prefix_hash == cls2.fp
    # Full text differs beyond any shared prefix -> both fp and prefix_hash differ.
    assert cls1.prefix_hash != cls2.prefix_hash
    assert cls1.fp != cls2.fp


def test_prefix_hash_chars_controls_the_no_system_message_fallback_slice():
    """prefix_hash_chars only ever gates the body_text[:N] fallback used when
    there is no system message (N=4000 by default); it does not create
    a second, independently-truncated hash."""
    cfg = _cfg(prefix_hash_chars=5)
    clf = Classifier(cfg)
    m1 = [{"role": "user", "content": "AAAAA-tail-one"}]
    m2 = [{"role": "user", "content": "AAAAA-tail-two"}]
    body1, body2 = "AAAAA-tail-one", "AAAAA-tail-two"
    cls1 = clf.classify(_body(m1), body1)
    cls2 = clf.classify(_body(m2), body2)
    # Same first 5 chars of body_text feed both fp and prefix_hash -> identical, and equal.
    assert cls1.fp == cls2.fp == cls1.prefix_hash == cls2.prefix_hash


# ---------------------------------------------------------------------------
# purity (classify never touches ProxyState)
# ---------------------------------------------------------------------------


def test_classifier_holds_no_state_reference():
    cfg = _cfg()
    clf = Classifier(cfg)
    assert not hasattr(clf, "state")
    assert not hasattr(clf, "_state")


def test_classify_is_side_effect_free_repeated_calls_are_equal():
    cfg = _cfg(lanes=[LaneRule(name="interactive", match=["trigger"], tier=0, interactive=True)])
    clf = Classifier(cfg)
    obj = _body([{"role": "user", "content": "trigger"}])
    body_text = "trigger"
    cls1 = clf.classify(obj, body_text)
    cls2 = clf.classify(obj, body_text)
    assert cls1 == cls2  # frozen dataclass equality; no hidden mutation drifted the result


# ---------------------------------------------------------------------------
# slot selection single vs dual
# ---------------------------------------------------------------------------


def test_slot_selection_single_mode_always_default_slot():
    cfg = _cfg(
        mode="single",
        default_slot=0,
        interactive_slot=1,
        lanes=[LaneRule(name="interactive", match=["trigger"], interactive=True)],
    )
    clf = Classifier(cfg)
    cls_interactive = clf.classify(_body([{"role": "user", "content": "trigger"}]), "trigger")
    cls_other = clf.classify(_body([{"role": "user", "content": "other"}]), "other")
    assert cls_interactive.slot == 0
    assert cls_other.slot == 0


def test_slot_selection_dual_mode_routes_interactive_to_interactive_slot():
    cfg = _cfg(
        mode="dual",
        default_slot=0,
        interactive_slot=1,
        lanes=[LaneRule(name="interactive", match=["trigger"], interactive=True)],
    )
    clf = Classifier(cfg)
    cls_interactive = clf.classify(_body([{"role": "user", "content": "trigger"}]), "trigger")
    cls_other = clf.classify(_body([{"role": "user", "content": "other"}]), "other")
    assert cls_interactive.slot == 1
    assert cls_other.slot == 0
