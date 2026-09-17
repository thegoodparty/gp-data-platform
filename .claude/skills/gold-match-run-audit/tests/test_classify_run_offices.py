"""classify_run_offices mirrors the matcher's classifier for the audit. Catches: a label drifting from the
matcher's actual branch, and the two new body-level labels not being produced."""

import csv
import sys
from pathlib import Path

SKILL = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SKILL))

import classify_run_offices as cro  # noqa: E402

TYPES = [
    "State",
    "City",
    "City_Council_Commissioner_District",
    "Unified_School_District",
    "Unified_School_SubDistrict",
]
NAMES = ["CA", "MONTEBELLO CITY", "MONTEBELLO CITY CNCL 1", "MONTEBELLO USD", "MONTEBELLO USD TA 1"]


def office(name, **kw):
    o = dict(
        br_database_id="1",
        name=name,
        state="CA",
        mtfcc="G4110",
        geo_id="0648592001",
        sub_area_name="District",
        sub_area_value="1",
        is_judicial="false",
        has_unknown_boundaries="false",
    )
    o.update(kw)
    return o


def test_labels_follow_the_matcher_branches():
    assert (
        cro.rule_class_for(office("Montebello City Council - District 1"), TYPES, NAMES)
        == "R2_slice_asserted"
    )
    assert (
        cro.rule_class_for(office("Pleasant Valley City Council - District 1"), TYPES, NAMES)
        == "R2_slice_body_absent_abstain"
    )
    assert (
        cro.rule_class_for(
            office(
                "Montebello Unified School Board - Area 1",
                mtfcc="G5420",
                geo_id="0624840",
                has_unknown_boundaries="true",
                sub_area_name="Area",
            ),
            TYPES,
            NAMES,
        )
        == "R2_school_flagged_slice_asserted"
    )
    assert (
        cro.rule_class_for(
            office(
                "Pleasant Valley School Board - Area 1",
                mtfcc="G5420",
                geo_id="0624840",
                has_unknown_boundaries="true",
                sub_area_name="Area",
            ),
            TYPES,
            NAMES,
        )
        == "R2_school_flagged_body_absent_abstain"
    )
    assert (
        cro.rule_class_for(office("Montebello City Mayor", sub_area_name="", sub_area_value=""), TYPES, NAMES)
        == "pass_through"
    )
    assert cro.rule_class_for(office("Any Judge", is_judicial="true"), TYPES, NAMES) == "R1_judicial_abstain"


def test_whole_school_label_flips_with_the_assertion_flag():
    whole_school_office = office("Montebello Unified School Board", mtfcc="G5420", geo_id="0624840")
    assert cro.rule_class_for(whole_school_office, TYPES, NAMES) == "R2_whole_school_gated"
    assert (
        cro.rule_class_for(whole_school_office, TYPES, NAMES, school_whole_assertion_enabled=True)
        == "R2_whole_asserted"
    )


def test_cli_writes_one_labeled_row_per_office(tmp_path):
    offs = tmp_path / "o.csv"
    uni = tmp_path / "u.csv"
    out = tmp_path / "out.csv"
    with open(offs, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(office("x").keys()))
        w.writeheader()
        w.writerow(office("Montebello City Council - District 1"))
    with open(uni, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["state_postal_code", "district_type", "district_name"])
        for t, n in zip(TYPES, NAMES, strict=True):
            w.writerow(["CA", t, n])
    cro.main([str(offs), str(uni), str(out)])
    rows = list(csv.DictReader(open(out)))
    assert rows[0]["rule_class"] == "R2_slice_asserted" and rows[0]["br_database_id"] == "1"


def test_office_in_a_state_missing_from_the_universe_is_flagged(tmp_path, capsys):
    offs = tmp_path / "o.csv"
    uni = tmp_path / "u.csv"
    out = tmp_path / "out.csv"
    with open(offs, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(office("x").keys()))
        w.writeheader()
        w.writerow(office("Ghost County Board", state="ZZ"))
    with open(uni, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["state_postal_code", "district_type", "district_name"])
        for t, n in zip(TYPES, NAMES, strict=True):
            w.writerow(["CA", t, n])
    cro.main([str(offs), str(uni), str(out)])
    rows = list(csv.DictReader(open(out)))
    assert rows[0]["rule_class"] == "UNIVERSE_STATE_MISSING"
    assert "ZZ" in capsys.readouterr().err
