"""body_presence: the body-level sub-row test behind candidate 2 class A. Each test names the failure it catches."""

import pytest

from stitch_golden_data.prod_gold_data import body_presence as bp

# The type vocabulary is part of the rule (type words are generic), so the fixture carries the types a real
# CA universe has for these bodies, including the parent types whose words (RECREATION, HEALTH) must be generic.
CA_TYPES = [
    "State",
    "County",
    "County_Supervisorial_District",
    "Sanitary_District",
    "Sanitary_SubDistrict",
    "Recreation_District",
    "Park_SubDistrict",
    "Health_District",
    "Hospital_SubDistrict",
    "City",
    "City_Council_Commissioner_District",
    "Unified_School_District",
    "Unified_School_SubDistrict",
    "High_School_SubDistrict",
]
CA_NAMES = [
    "CA",
    "KERN",
    "KERN CNTY SUP DIST 1",
    "MT VIEW SANITARY",
    "MT VIEW SANITARY DIST AREA 3",
    "SHAFTER REC AND PARK",
    "SHAFTER REC AND PARK DIST DIV 2",
    "CAMARILLO HEALTHCARE",
    "CAMARILLO HEALTHCARE DIST DIV 1",
    "HEALDSBURG CITY",
    "BIG BEAR LAKE CITY CNCL 2 (2022)",
    "MONTEBELLO USD",
    "MONTEBELLO USD TA 1",
    "TAFT UNION HS DIST TA 1",
]
GENERIC = bp.GENERIC_WORDS | bp.type_words(CA_TYPES)


def has(name):
    return bp.body_has_sub_rows(name, CA_TYPES, CA_NAMES)


def test_absent_body_is_absent():
    """Catches: a body with no sub-level row anywhere in the state reading as present (the trap)."""
    assert has("Pleasant Valley School Board - Area 1") is False


def test_abbreviated_row_name_still_counts_as_present():
    """Catches: REC vs RECREATION or WTR vs WATER producing a false absence (lost correct match)."""
    assert has("Shafter Recreation and Park District Board - Area 2") is True


def test_compound_row_token_carries_its_parts():
    """Catches: HEALTHCARE failing to satisfy the anchor CARE (Camarillo, a false absence)."""
    assert has("Camarillo Health Care District Board - Zone 1") is True


def test_special_district_filed_under_county_code_finds_its_own_sub_rows():
    """Catches: family-scoped search missing Sanitary_SubDistrict rows for a county-coded office."""
    assert has("Mt. View Sanitary District Board - Area 3") is True


def test_parenthetical_in_office_name_is_ignored():
    """Catches: '(Orange County)' becoming an anchor and forcing a false absence."""
    assert bp.anchor_tokens("Ocean View School Board (Orange County) - Area 1", GENERIC) == {"OCEAN", "VIEW"}


def test_generic_only_body_falls_back_to_its_full_name():
    """Catches: a body named in generic words (Union Park) having no anchors and never firing."""
    assert bp.anchor_tokens("Union Park Community Development District Board - Seat 3", GENERIC) == {
        "UNION",
        "PARK",
        "COMMUNITY",
        "DEVELOPMENT",
    }
    assert has("Union Park Community Development District Board - Seat 3") is False


def test_same_anchor_body_counts_as_present_by_design():
    """Catches: over-eager absence on a same-state body sharing the anchor (Taft City vs Taft Union): a miss is
    acceptable, a false absence is not."""
    assert has("Taft City School Board - Area 1") is True


def test_proper_noun_abbreviation_expands_both_ways():
    """Catches: 'Mt. View' vs 'MT VIEW' or 'MOUNT VIEW' disagreeing."""
    assert bp.anchor_tokens("Mt. View Sanitary District Board - Area 3", GENERIC) == {"MOUNT", "VIEW"}


@pytest.mark.parametrize(
    "t,expected",
    [
        ("Sanitary_SubDistrict", True),
        ("City_Ward", True),
        ("Town_Ward", True),
        ("School_Subdistrict", True),
        ("County_Supervisorial_District", True),
        ("Judicial_Justice_of_the_Peace", True),
        ("Water_District", False),
        ("City", False),
        ("State", False),
        ("Unified_School_District", False),
    ],
)
def test_sub_level_type_detection(t, expected):
    """Catches: a parent type counted as a sub-row (would hide the trap) or a sub type dropped (false absence)."""
    assert bp.is_sub_level_type(t) is expected


def test_type_words_come_from_the_loaded_vocabulary():
    """Catches: GENERIC drifting from L2's vocabulary; SANITARY must be generic once the type exists."""
    assert "SANITARY" in bp.type_words(CA_TYPES) and "MONTEBELLO" not in bp.type_words(CA_TYPES)
