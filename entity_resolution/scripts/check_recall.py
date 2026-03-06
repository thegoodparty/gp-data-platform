"""Check br_race_id recall — how many expected matches did Splink find?"""

import pandas as pd

inp = pd.read_csv("data/input.csv", dtype=str)
inp = inp.replace({"null": None, "": None})
clust = pd.read_csv("results/clustered_candidacies.csv", dtype=str)

# TS records with known br_race_id (not net new)
ts = inp[
    (inp["source_name"] == "techspeed") & (inp["br_race_id"] != "ts_found_race_net_new")
]
print(f"TS records with known br_race_id: {len(ts)}")

# Which of those have a BR record in the input?
br_race_ids_in_input = set(inp[inp["source_name"] == "ballotready"]["br_race_id"])
ts_with_br_present = ts[ts["br_race_id"].isin(br_race_ids_in_input)]
print(f"  with BR counterpart in input: {len(ts_with_br_present)}")

# Build cluster lookup
cluster_map = dict(zip(clust["unique_id"], clust["cluster_id"]))

# For each TS record, find its cluster, then check if any BR record
# with the same br_race_id is in the same cluster
n_correct = 0
missed = []
for _, row in ts_with_br_present.iterrows():
    ts_cluster = cluster_map.get(row["unique_id"])
    # Find BR records with same race id
    br_matches = inp[
        (inp["source_name"] == "ballotready") & (inp["br_race_id"] == row["br_race_id"])
    ]
    found = False
    for _, br in br_matches.iterrows():
        br_cluster = cluster_map.get(br["unique_id"])
        if ts_cluster and br_cluster and ts_cluster == br_cluster:
            found = True
            break
    if found:
        n_correct += 1
    else:
        missed.append((row, br_matches.iloc[0] if len(br_matches) > 0 else None))

n_total = len(ts_with_br_present)
print(
    f"  correctly matched: {n_correct} / {n_total} ({n_correct / n_total * 100:.1f}%)"
)
print(f"  missed: {n_total - n_correct}")

# Show missed examples
print("\n--- Missed matches (first 15) ---")
for ts_row, br_row in missed[:15]:
    if br_row is not None:
        print(
            f"\n  TS: {ts_row['first_name']} {ts_row['last_name']} | "
            f"{ts_row['state']} | {ts_row.get('candidate_office', '')} | "
            f"gen={ts_row.get('general_election_date', '')} pri={ts_row.get('primary_election_date', '')} | "
            f"city={ts_row.get('city', '')}"
        )
        print(
            f"  BR: {br_row['first_name']} {br_row['last_name']} | "
            f"{br_row['state']} | {br_row.get('candidate_office', '')} | "
            f"gen={br_row.get('general_election_date', '')} pri={br_row.get('primary_election_date', '')} | "
            f"city={br_row.get('city', '')}"
        )
