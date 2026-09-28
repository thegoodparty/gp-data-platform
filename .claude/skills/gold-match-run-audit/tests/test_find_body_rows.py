"""find_body_rows answers the audit's row-existence question for each in-class abstain: does ANY row in the
state's universe, of any type, carry the body's anchor tokens? Catches: a same-named parent row going unreported,
a body with no row being reported as having one, and an office with no anchors at all being counted as zero-hit."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import find_body_rows as fbr  # noqa: E402

UNIVERSE = [
    ("OR", "State", "OR"),
    ("OR", "City", "TOLEDO"),
    ("OR", "Port_District", "PORT OF TOLEDO"),
    ("OR", "City_Ward", "SALEM WARD 1"),
    ("FL", "County", "LEE"),
    ("FL", "Hamlet_Community_Area", "UNION PARK COMMUNITY (EST.)"),
    ("FL", "County_Commissioner_District", "LEE CNTY COMM DIST 5"),
]


def shadow(br_database_id, name, state, rule_class="R2_slice_body_absent_abstain"):
    return dict(br_database_id=br_database_id, name=name, state=state, rule_class=rule_class)


def test_same_named_parent_row_is_reported_as_a_hit_and_listed():
    [r] = fbr.body_rows([shadow("1", "Toledo City Council - Position 5", "OR")], UNIVERSE)
    assert r["anchors"] == "TOLEDO"
    assert r["anchor_hits"] == 2
    assert r["anchor_hit_rows"] == "City: TOLEDO; Port_District: PORT OF TOLEDO"
    assert r["top_candidates"].startswith("City: TOLEDO")


def test_body_with_no_row_anywhere_reports_zero_hits():
    [r] = fbr.body_rows(
        [shadow("2", "WildBlue Community Development District Board - Seat 5", "FL")], UNIVERSE
    )
    assert r["anchors"] == "WILDBLUE"
    assert r["anchor_hits"] == 0 and r["anchor_hit_rows"] == ""


def test_office_without_anchor_tokens_is_flagged_not_counted_as_zero_hit():
    [r] = fbr.body_rows(
        [shadow("5", "Board of Trustees - Seat 1", "OR", "R2_slice_zero_subtype_abstain")], UNIVERSE
    )
    assert r["anchors"] == ""
    assert r["anchor_hits"] == -1


def test_only_in_class_abstains_are_audited():
    rows = fbr.body_rows(
        [
            shadow("3", "Toledo City Mayor", "OR", rule_class="pass_through"),
            shadow("4", "Any Judge", "OR", "R1_judicial_abstain"),
        ],
        UNIVERSE,
    )
    assert rows == []
