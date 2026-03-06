import pandas as pd

inp = pd.read_csv("data/input.csv", dtype=str).replace({"null": None, "": None})

ts_known = inp[
    (inp["source_name"] == "techspeed") & (inp["br_race_id"] != "ts_found_race_net_new")
]
br = inp[inp["source_name"] == "ballotready"]
br_race_ids_in_input = set(br["br_race_id"])
ts_missing = ts_known[~ts_known["br_race_id"].isin(br_race_ids_in_input)]

sample = ts_missing.drop_duplicates("br_race_id").head(10)
for _, r in sample.iterrows():
    print(
        f'{r["first_name"]} {r["last_name"]} | {r["state"]} | {r["candidate_office"]} | {r["city"]}'
    )
    print(f'  source_id:  {r["source_id"]}')
    print(f'  br_race_id: {r["br_race_id"]}')
    print()
