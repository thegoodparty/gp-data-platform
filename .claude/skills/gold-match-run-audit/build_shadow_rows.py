#!/usr/bin/env python3
"""Join one run's offices (with production's outcome) to the mirror's rule classes and render the rows for
the supervised-monitoring table (SKILL.md, "Supervised monitoring of a matcher rule").

    uv run python build_shadow_rows.py run-<date>-offices.csv classes-<date>.csv universe-<date>.csv \
        --run-key '2026-09-18 14:30:00' --image-git-sha <40-hex> --shape B_live --out shadow-<date>

writes <out>.csv (for the record) and <out>.sql (one INSERT; run it with dbsql.py -f on the owner's go).
"""

from __future__ import annotations

import argparse
import csv

TABLE = "goodparty_data_catalog.dbt_sroberts.gm_body_rule_shadow"
# The body-level rule's in-class abstain labels; R1_judicial_abstain predates the rule.
ABSTAIN_LABELS = {
    "R2_slice_body_absent_abstain",
    "R2_school_flagged_body_absent_abstain",
    "R2_slice_zero_subtype_abstain",
}
COLUMNS = [
    "run_key",
    "image_git_sha",
    "shape",
    "br_database_id",
    "name",
    "state",
    "mtfcc",
    "geo_id",
    "sub_area_name",
    "sub_area_value",
    "is_judicial",
    "has_unknown_boundaries",
    "rule_class",
    "universe_loaded_at",
    "prod_l2_district_type",
    "prod_l2_district_name",
    "prod_confidence",
    "divergent",
]
TIMESTAMP_COLUMNS = {"run_key", "universe_loaded_at"}


def _opt(v):
    return None if (v or "").strip() == "" else v


def _flag(v):
    return (v or "").strip().lower() == "true"


def shadow_rows(offices, classes, *, run_key, image_git_sha, shape, universe_loaded_at):
    rows = []
    for o in offices:
        rule_class = classes[o["br_database_id"]]
        served_type = _opt(o["l2_district_type"])
        rows.append(
            {
                "run_key": run_key,
                "image_git_sha": image_git_sha,
                "shape": shape,
                "br_database_id": int(o["br_database_id"]),
                "name": _opt(o["name"]),
                "state": _opt(o["state"]),
                "mtfcc": _opt(o["mtfcc"]),
                "geo_id": _opt(o["geo_id"]),
                "sub_area_name": _opt(o["sub_area_name"]),
                "sub_area_value": _opt(o["sub_area_value"]),
                "is_judicial": _flag(o["is_judicial"]),
                "has_unknown_boundaries": _flag(o["has_unknown_boundaries"]),
                "rule_class": rule_class,
                "universe_loaded_at": universe_loaded_at.get((o["state"] or "").strip().upper()),
                "prod_l2_district_type": served_type,
                "prod_l2_district_name": _opt(o["l2_district_name"]),
                "prod_confidence": int(o["confidence"]) if _opt(o["confidence"]) else None,
                "divergent": rule_class in ABSTAIN_LABELS and served_type is not None,
            }
        )
    return rows


def _literal(column, v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if column in TIMESTAMP_COLUMNS:
        return f"timestamp'{v}'"
    # Statement-API SQL: '' does not escape a quote; backslash does.
    return "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"


def insert_sql(table, rows):
    values = ",\n".join("(" + ", ".join(_literal(c, r[c]) for c in COLUMNS) + ")" for r in rows)
    return f"insert into {table} ({', '.join(COLUMNS)}) values\n{values}"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("offices_csv")
    ap.add_argument("classes_csv")
    ap.add_argument("universe_csv")
    ap.add_argument("--run-key", required=True)
    ap.add_argument("--image-git-sha", required=True)
    ap.add_argument("--shape", required=True, choices=["A_shadow", "B_live"])
    ap.add_argument("--out", required=True, help="path prefix; writes <out>.csv and <out>.sql")
    a = ap.parse_args(argv)
    offices = list(csv.DictReader(open(a.offices_csv, newline="", encoding="utf-8-sig")))
    classes = {r["br_database_id"]: r["rule_class"] for r in csv.DictReader(open(a.classes_csv, newline=""))}
    # The universe loads per state, so each office is stamped with its own state's version.
    universe_loaded_at: dict[str, str] = {}
    for u in csv.DictReader(open(a.universe_csv, newline="", encoding="utf-8-sig")):
        st = u["state_postal_code"].strip().upper()
        universe_loaded_at[st] = max(universe_loaded_at.get(st, ""), u["loaded_at"])
    rows = shadow_rows(
        offices,
        classes,
        run_key=a.run_key,
        image_git_sha=a.image_git_sha,
        shape=a.shape,
        universe_loaded_at=universe_loaded_at,
    )
    with open(f"{a.out}.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)
    with open(f"{a.out}.sql", "w") as fh:
        fh.write(insert_sql(TABLE, rows))
    in_class = sum(r["rule_class"] in ABSTAIN_LABELS for r in rows)
    divergent = sum(r["divergent"] for r in rows)
    print(f"{len(rows)} rows, {in_class} in-class abstains, {divergent} divergent -> {a.out}.csv / .sql")


if __name__ == "__main__":
    main()
