"""EXPERIMENTAL fuzzy entity resolution (Splink + DuckDB), EVALUATED THEN DROPPED.

This was trialed to recover elected officials who are in HubSpot under a slightly different
email/name than our record. A 50-record audit found it leaked false positives -- same name and
state but a different person or locality (e.g. two same-named officials in one state, or a
resigned officeholder matched to their successor's contact record). It was removed from the final
deliverable, which uses deterministic tiers only. Kept here so reviewers can see the approach and
the guardrails, and why the precision was still insufficient.

Design mirrors matcha's elected_official config: link_only across two sources, JaroWinkler on
names, exact email/phone/state, fuzzy office name, standalone email/phone blocking. It matches
only the GAP (records the deterministic pass missed) against unclaimed contacts, then folds
accepted 1:1 matches into the export CSV.

Run from analytics/ with --with pandas databricks-sql-connector databricks-sdk splink rapidfuzz.
"""

import os
import sys
from pathlib import Path

import pandas as pd
import splink.comparison_library as cl
from rapidfuzz.distance import JaroWinkler
from splink import DuckDBAPI, Linker, SettingsCreator, block_on

sys.path.insert(0, str(Path(__file__).parents[1] / "lib"))
import databricks_conn as dbc

CAT = "goodparty_data_catalog"
OUT_DIR = Path(os.environ.get("EO_EXPORT_DIR", Path(__file__).parent / "output"))
DET_CSV = OUT_DIR / "elected_officials_hubspot_export.csv"  # deterministic export (input)
OUT_CSV = OUT_DIR / "elected_officials_hubspot_export_with_fuzzy.csv"

# Acceptance guards, tuned after the first pass leaked same-surname strangers.
ACCEPT_PROB = 0.92  # Splink match-probability floor
LN_JARO_MIN = 0.92  # last-name similarity floor
FN_JARO_MIN = 0.82  # first-name similarity floor (rejects e.g. "george" vs "patty")

# In-scope elected officials, normalized for matching. Scope here must match the dbt analysis.
EO_SQL = f"""
select
  e.gp_elected_official_id,
  e.br_candidate_id                                 as person_id,
  lower(trim(e.first_name))                         as first_name,
  lower(trim(e.last_name))                          as last_name,
  lower(trim(coalesce(e.mailing_state, e.state)))   as state,
  lower(trim(e.email))                              as email,
  nullif(regexp_replace(e.phone, '[^0-9]', ''), '') as phone,
  lower(trim(e.candidate_office))                   as office_name,
  lower(trim(coalesce(e.mailing_city, e.city)))     as city
from {CAT}.mart_civics.elected_officials e
join {CAT}.mart_civics.elected_official_terms t
  on e.selected_gp_elected_official_term_id = t.gp_elected_official_term_id
where (e.term_end_date >= current_date or e.term_end_date is null)
  and (e.email is not null or e.phone is not null)
  and e.party_affiliation is not null
  and e.party_affiliation not in ('Republican', 'Democrat')
"""

HS_SQL = f"""
select
  id                                                          as hubspot_contact_id,
  lower(trim(properties_firstname))                           as first_name,
  lower(trim(properties_lastname))                            as last_name,
  lower(trim(properties_state))                               as state,
  lower(trim(properties_email))                               as email,
  nullif(regexp_replace(properties_phone, '[^0-9]', ''), '')  as phone,
  lower(trim(coalesce(properties_official_office_name, properties_candidate_office))) as office_name,
  lower(trim(properties_city))                                as city
from {CAT}.airbyte_source.hubspot_api_contacts
where properties_lastname is not null and properties_state is not null
"""


def _sim(col):
    def f(row):
        a, b = row[f"{col}_l"], row[f"{col}_r"]
        return JaroWinkler.normalized_similarity(str(a), str(b)) if a and b else 0.0

    return f


def main() -> None:
    det = pd.read_csv(DET_CSV, dtype={"hubspot_contact_id": "string", "person_id": "string"})
    det_matched = det[det["hubspot_contact_id"].notna()]
    used_contacts = set(det_matched["hubspot_contact_id"])
    matched_persons = set(det_matched["person_id"])

    eo = dbc.run_query(EO_SQL)
    hs = dbc.run_query(HS_SQL)
    eo["person_id"] = eo["person_id"].astype("string")
    hs["hubspot_contact_id"] = hs["hubspot_contact_id"].astype("string")

    # Match only the gap: EOs the deterministic pass missed, contacts not yet claimed.
    eo = eo[~eo["person_id"].isin(matched_persons)].copy()
    hs = hs[~hs["hubspot_contact_id"].isin(used_contacts)].copy()
    print(f"fuzzy candidates -> EO: {len(eo):,}; HubSpot: {len(hs):,}")

    # Unified schema so Splink retains both keys across the two link_only sources.
    eo["source"] = "eo"
    eo["hubspot_contact_id"] = pd.NA
    eo["unique_id"] = "eo|" + eo["gp_elected_official_id"].astype(str)
    hs["source"] = "hs"
    hs["gp_elected_official_id"] = pd.NA
    hs["person_id"] = pd.NA
    hs["unique_id"] = "hs|" + hs["hubspot_contact_id"].astype(str)

    cols = [
        "unique_id",
        "source",
        "first_name",
        "last_name",
        "state",
        "email",
        "phone",
        "office_name",
        "city",
        "gp_elected_official_id",
        "person_id",
        "hubspot_contact_id",
    ]
    eo_df = eo[cols].where(eo[cols].notna(), None)
    hs_df = hs[cols].where(hs[cols].notna(), None)

    settings = SettingsCreator(
        link_type="link_only",
        unique_id_column_name="unique_id",
        comparisons=[
            cl.JaroWinklerAtThresholds("last_name", [0.95, 0.88]).configure(term_frequency_adjustments=True),
            cl.JaroWinklerAtThresholds("first_name", [0.92]),
            cl.ExactMatch("email"),
            cl.ExactMatch("phone"),
            cl.ExactMatch("state"),
            cl.JaroWinklerAtThresholds("office_name", [0.9, 0.8]),
            cl.ExactMatch("city"),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("email"),
            block_on("phone"),
            block_on("state", "last_name"),
        ],
        retain_intermediate_calculation_columns=True,
        additional_columns_to_retain=[
            "gp_elected_official_id",
            "person_id",
            "hubspot_contact_id",
            "first_name",
            "last_name",
            "office_name",
        ],
    )

    linker = Linker([eo_df, hs_df], settings, DuckDBAPI())
    linker.training.estimate_u_using_random_sampling(max_pairs=5_000_000)
    for blk in [("state", "last_name"), ("first_name", "state"), ("phone",)]:
        try:
            linker.training.estimate_parameters_using_expectation_maximisation(
                block_on(*blk), fix_u_probabilities=True
            )
        except Exception as e:
            print(f"EM block {blk} failed: {e}")

    preds = linker.inference.predict(threshold_match_probability=0.5).as_pandas_dataframe()

    def coalesce(a, b):
        return a if pd.notna(a) and a is not None else b

    preds["eo_id"] = [
        coalesce(a, b)
        for a, b in zip(preds["gp_elected_official_id_l"], preds["gp_elected_official_id_r"], strict=False)
    ]
    preds["contact_id"] = [
        coalesce(a, b)
        for a, b in zip(preds["hubspot_contact_id_l"], preds["hubspot_contact_id_r"], strict=False)
    ]
    preds = preds[preds["eo_id"].notna() & preds["contact_id"].notna()].copy()
    preds["ln_sim"] = preds.apply(_sim("last_name"), axis=1)
    preds["fn_sim"] = preds.apply(_sim("first_name"), axis=1)
    preds["state_eq"] = [
        a is not None and a == b for a, b in zip(preds["state_l"], preds["state_r"], strict=False)
    ]

    keep = preds[
        (preds["match_probability"] >= ACCEPT_PROB)
        & (preds["ln_sim"] >= LN_JARO_MIN)
        & (preds["fn_sim"] >= FN_JARO_MIN)
        & (preds["state_eq"])
    ].copy()
    keep = keep.sort_values("match_probability", ascending=False)
    keep = keep.drop_duplicates("eo_id").drop_duplicates("contact_id")  # strict 1:1
    print(f"accepted fuzzy 1:1 matches: {len(keep):,}")

    eo_person = eo.set_index("gp_elected_official_id")["person_id"].astype(str).to_dict()
    keep["person_id"] = keep["eo_id"].astype(str).map(eo_person)
    fuzzy = keep[["person_id", "contact_id"]].rename(columns={"contact_id": "fuzzy_contact_id"})

    out = det.merge(fuzzy, on="person_id", how="left")
    fill = out["hubspot_contact_id"].isna() & out["fuzzy_contact_id"].notna()
    out.loc[fill, "hubspot_contact_id"] = out.loc[fill, "fuzzy_contact_id"]
    out.loc[fill, "match_method"] = "fuzzy_splink"
    out = out.drop(columns=["fuzzy_contact_id"])
    out.to_csv(OUT_CSV, index=False)
    print(f"folded {int(fill.sum()):,} fuzzy matches -> {OUT_CSV}")


if __name__ == "__main__":
    main()
