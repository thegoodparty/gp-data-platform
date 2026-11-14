"""QC: stratified random sample of matched records + data-side identity check.

Draws a sample stratified by match_method (to stress-test the riskier tiers rather than let the
dominant email tier drown them out), pulls the matched HubSpot contact for each, and reports how
well the attached contact's name/state agrees with our person. Emits audit_sample.json for the
separate web-verification step (a fan-out of small-model agents that confirm each person is a
currently-serving officeholder; that step is manual/agent-driven, not in this file).
"""

import json
import sys
from pathlib import Path

import pandas as pd
from rapidfuzz.distance import JaroWinkler

sys.path.insert(0, str(Path(__file__).parents[1] / "lib"))
import databricks_conn as dbc

HERE = Path(__file__).parent
CSV = HERE / "output" / "elected_officials_hubspot_export.csv"
CAT = "goodparty_data_catalog"

# Per-method sample sizes across the four deterministic tiers (stratified, not
# proportional to volume; over-weights the smaller tiers to stress-test them).
QUOTA = {
    "email": 17,
    "phone_lastname": 15,
    "br_candidacy_id": 10,
    "gp_api_user": 8,
}


def jw(a, b):
    a, b = str(a or "").lower().strip(), str(b or "").lower().strip()
    return JaroWinkler.normalized_similarity(a, b) if a and b else 0.0


def main() -> None:
    df = pd.read_csv(CSV, dtype={"hubspot_contact_id": "string", "person_id": "string"})
    matched = df[df["hubspot_contact_id"].notna()]

    parts = [
        matched[matched["match_method"] == m].sample(
            min(n, (matched["match_method"] == m).sum()), random_state=42
        )
        for m, n in QUOTA.items()
    ]
    sample = pd.concat(parts).reset_index(drop=True)
    print("sampled by method:\n" + sample["match_method"].value_counts().to_string())

    ids = sample["hubspot_contact_id"].dropna().unique().tolist()
    in_list = ",".join(f"'{i}'" for i in ids)
    hs = dbc.run_query(f"""
      select id as hubspot_contact_id,
             properties_firstname as hs_first, properties_lastname as hs_last,
             properties_full_name as hs_full, properties_state as hs_state,
             coalesce(properties_official_office_name, properties_candidate_office) as hs_office
      from {CAT}.airbyte_source.hubspot_api_contacts
      where id in ({in_list})
    """)
    hs["hubspot_contact_id"] = hs["hubspot_contact_id"].astype("string")
    s = sample.merge(hs, on="hubspot_contact_id", how="left")

    s["name_sim"] = [
        jw(f"{r['First Name']} {r['Last Name']}", r["hs_full"] or f"{r['hs_first']} {r['hs_last']}")
        for _, r in s.iterrows()
    ]
    print("\n=== data-side identity check (our record vs attached HubSpot contact) ===")
    print(f"name_sim >= 0.90: {(s['name_sim'] >= 0.90).mean():.1%}")
    weak = s[s["name_sim"] < 0.90]
    print(f"{len(weak)} records with weak name agreement:")
    for _, r in weak.iterrows():
        print(
            f"  [{r['match_method']}] {r['First Name']} {r['Last Name']} ({r['State/Region']}) "
            f"-> hs: {r['hs_first']} {r['hs_last']} name_sim={r['name_sim']:.2f}"
        )

    records = [
        {
            "n": i + 1,
            "match_method": r["match_method"],
            "name": f"{r['First Name']} {r['Last Name']}",
            "office": r["Candidate Office"],
            "state": r["State/Region"],
            "city": r["City"] if pd.notna(r["City"]) else "",
            "name_sim": round(r["name_sim"], 2),
        }
        for i, r in s.iterrows()
    ]
    (HERE / "output" / "audit_sample.json").write_text(json.dumps(records, indent=2))
    print(f"\nwrote audit_sample.json ({len(records)} records)")


if __name__ == "__main__":
    main()
