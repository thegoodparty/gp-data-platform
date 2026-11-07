"""Regenerate every family's intake doc after an intake.py change.

The mechanical half above the decisions marker is refreshed (picking up newly added checks,
e.g. the DATA-2239 null worksheet / cross-family covariance / hygiene sections, 2026-08-12);
human-written decision sections are preserved verbatim by write_intake. Metadata mirrors each
family's build script so headers stay truthful without re-querying the warehouse.

Run: uv run --with pyarrow python projects/legit_candidate_predictor/regen_intakes.py
"""

import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from intake import write_intake  # noqa: E402

FAMILIES = [
    dict(
        name="br_position_features",
        key="br_position_id",
        source="dbt.stg_airbyte_source__ballotready_api_position + "
        "int__ballotready_position_election_frequency + "
        "stg_airbyte_source__ballotready_s3_office_holders_v3",
        join="br_position_id -> working_set.br_position_id",
        pit_class="B (static race properties; current-snapshot caveat)",
    ),
    dict(
        name="incumbency_features",
        key="user_id",
        source="dbt.stg_airbyte_source__ballotready_s3_office_holders_v3 (person grain)",
        join="(state, first name, last name) -> office holders, gated start_at <= signup",
        pit_class="B (term must begin at or before signup; later terms excluded as outcome " "leakage)",
    ),
    dict(
        name="name_features",
        key="user_id",
        source="L2 name-frequency dimension tables + a-priori token/placeholder/fictional "
        "lists (build_name_plausibility.py)",
        join="user_id (computed from name/email text; no person match needed)",
        pit_class="B (name text fixed at signup; lists written a priori, never label-pruned)",
    ),
    dict(
        name="gp_user_details_features",
        key="user_id",
        source="dbt.stg_airbyte_source__gp_api_db_user + gp_api_db_campaign.details "
        "(latest campaign per user)",
        join="user_id -> user table; campaign_id -> details map, row_number=1 by " "campaign created_at desc",
        pit_class="B claimed (creation-time writes); validated by "
        "audit_details_mutability.py on the post-2026-04-13 CDC window",
    ),
]

if __name__ == "__main__":
    for f in FAMILIES:
        p = HERE / "datasets" / f"{f['name']}.parquet"
        if not p.exists():
            print(f"!! {p.name} missing, skipped")
            continue
        df = pd.read_parquet(p)
        write_intake(
            df, f["name"], key=f["key"], source=f["source"], join=f["join"], pit_class=f["pit_class"]
        )
