"""Check why 4,325 TS records have br_race_id values not found in BR input."""

import pandas as pd

inp = pd.read_csv("data/input.csv", dtype=str).replace({"null": None, "": None})

ts_known = inp[
    (inp["source_name"] == "techspeed") & (inp["br_race_id"] != "ts_found_race_net_new")
]
br = inp[inp["source_name"] == "ballotready"]

br_race_ids_in_input = set(br["br_race_id"])
ts_missing = ts_known[~ts_known["br_race_id"].isin(br_race_ids_in_input)]

distinct_race_ids = ts_missing["br_race_id"].nunique()
print(f"TS records whose br_race_id has NO BR record in input: {len(ts_missing)}")
print(f"Distinct br_race_id values involved: {distinct_race_ids}")

# How many TS records per missing race?
ts_per_race = ts_missing.groupby("br_race_id").size()
print(f"\nTS records per missing br_race_id:")
print(ts_per_race.describe())

print(f"\nSample missing br_race_ids (with TS record details):")
for race_id in ts_missing["br_race_id"].unique()[:15]:
    rows = ts_missing[ts_missing["br_race_id"] == race_id]
    for _, row in rows.iterrows():
        print(
            f"  race_id={race_id}  {row['first_name']} {row['last_name']} | "
            f"{row['state']} | {row['candidate_office']} | city={row.get('city', '')}"
        )
