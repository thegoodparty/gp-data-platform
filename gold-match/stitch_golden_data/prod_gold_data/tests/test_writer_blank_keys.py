"""Blank district labels must fail the writer's validation: a '' label
passes an is-None check, persists as a "match" that can never join the
universe, and the pending list's dead-label rule then reopens the office on
every build with no 30-day clock. Legit matcher output can never carry ''
(labels are universe-verbatim), so tightening is safe."""

import pytest

from stitch_golden_data.prod_gold_data.l2_br_match_writer import validate_results
from stitch_golden_data.prod_gold_data.l2_br_matcher import MatchResult


def test_all_blank_district_fields_fail():
    with pytest.raises(ValueError, match="blank"):
        validate_results([MatchResult(1, "", "", "", confidence=90)])


def test_mixed_blank_district_field_fails():
    with pytest.raises(ValueError, match="blank"):
        validate_results([MatchResult(2, "CA", "City_Ward", "", confidence=90)])


def test_whitespace_only_label_fails():
    with pytest.raises(ValueError, match="blank"):
        validate_results([MatchResult(3, "CA", "  ", "Ward 1", confidence=90)])


def test_legit_match_and_abstention_still_pass():
    validate_results(
        [
            MatchResult(4, "CA", "City_Ward", "Ward 1", confidence=90),
            MatchResult(5, None, None, None, confidence=None),
        ]
    )
