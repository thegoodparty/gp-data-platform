#!/usr/bin/env python3
"""Python mirror of the matcher's geography classifier for the run audit (Step 2).

The SQL mirror in SKILL.md cannot express the body-level test (candidate 2 class A), so the audit labels
offices here by calling the matcher's own classifier on the run's offices and a universe export. Run from
gold-match/ so the matcher imports:

    cd gold-match && uv run python ../.claude/skills/gold-match-run-audit/classify_run_offices.py \
        <offices.csv> <universe.csv> <out.csv>
"""

from __future__ import annotations

import csv
import sys
from collections import defaultdict

from stitch_golden_data.prod_gold_data import l2_br_matcher as M


def _opt(v):
    v = (v or "").strip()
    return None if v.lower() in ("", "null") else v


def rule_class_for(
    o: dict,
    types: list[str],
    names: list[str],
    *,
    school_whole_assertion_enabled: bool = M.SCHOOL_WHOLE_ASSERTION_ENABLED,
) -> str:
    mtfcc = _opt(o.get("mtfcc")) or ""
    judicial = (o.get("is_judicial") or "").strip().lower() == "true"
    flagged = (o.get("has_unknown_boundaries") or "").strip().lower() == "true"
    geo_id, san, sav = _opt(o.get("geo_id")), _opt(o.get("sub_area_name")), _opt(o.get("sub_area_value"))
    v = M._classify_office_geography(
        mtfcc=mtfcc,
        is_judicial=judicial,
        has_unknown_boundaries=flagged,
        geo_id=geo_id,
        sub_area_name=san,
        sub_area_value=sav,
        state_district_types=types,
        school_whole_assertion_enabled=school_whole_assertion_enabled,
        office_name=o.get("name") or "",
        state_district_names=names,
    )
    if mtfcc == M._PARTY_COMMITTEE_MTFCC:
        return "R0_party_committee"
    if judicial:
        return "R1_judicial_abstain" if v.abstain else "R1_judicial_menu"
    family = M._FAMILY_BY_MTFCC.get(mtfcc) if (san or sav) else None
    if family is None:
        return "pass_through"
    if family == "school" and flagged:
        if not (M._SCHOOL_FAMILY_PRESENCE_TYPES & set(types)):
            return "school_flag_no_school_rows"
        if not (M._FAMILY_SUB_TYPES["school"] & set(types)):
            return "R2_slice_zero_subtype_abstain"
        return "R2_school_flagged_body_absent_abstain" if v.abstain else "R2_school_flagged_slice_asserted"
    level = "slice" if flagged else M._geo_id_family_format(geo_id, M._FAMILY_PARENT_GEOID_LENGTH[family])
    if level == "malformed":
        return "pass_through"
    if level == "whole":
        if family == "school" and v.eligible_indices is None:
            return "R2_whole_school_gated"
        return "R2_whole_asserted"
    if not (M._FAMILY_SUB_TYPES[family] & set(types)):
        return "R2_slice_zero_subtype_abstain"
    if v.abstain:
        return "R2_slice_body_absent_abstain"
    return "R2_slice_asserted"


def main(argv=None):
    offices_path, universe_path, out_path = (argv or sys.argv[1:])[:3]
    by_state = defaultdict(lambda: ([], []))
    for u in csv.DictReader(open(universe_path, newline="", encoding="utf-8-sig")):
        t, n = by_state[u["state_postal_code"].strip().upper()]
        t.append(u["district_type"])
        n.append(u["district_name"])
    rows = list(csv.DictReader(open(offices_path, newline="", encoding="utf-8-sig")))
    # A state with zero universe rows silently reads as "no sub-types anywhere", which
    # mislabels every office in it rather than surfacing the gap -- flag it instead.
    missing_states = {o["state"].strip().upper() for o in rows} - by_state.keys()
    if missing_states:
        print(
            f"WARNING: no universe rows for state(s): {', '.join(sorted(missing_states))}",
            file=sys.stderr,
        )
    with open(out_path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["br_database_id", "rule_class"])
        w.writeheader()
        for o in rows:
            state = o["state"].strip().upper()
            if state in missing_states:
                rule_class = "UNIVERSE_STATE_MISSING"
            else:
                types, names = by_state[state]
                rule_class = rule_class_for(o, types, names)
            w.writerow({"br_database_id": o["br_database_id"], "rule_class": rule_class})
    print(f"labeled {len(rows)} offices -> {out_path}")


if __name__ == "__main__":
    main()
