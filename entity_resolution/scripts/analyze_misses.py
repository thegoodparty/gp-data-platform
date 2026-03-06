"""Analyze why TS records with a known br_race_id fail to match BR records."""

import pandas as pd

inp = pd.read_csv("data/input.csv", dtype=str).replace({"null": None, "": None})
clust = pd.read_csv("results/clustered_candidacies.csv", dtype=str)
pw = pd.read_csv("results/pairwise_predictions.csv", dtype=str)

# TS records with known br_race_id
ts = inp[
    (inp["source_name"] == "techspeed") & (inp["br_race_id"] != "ts_found_race_net_new")
].copy()
br = inp[inp["source_name"] == "ballotready"]
print(f"Total TS records with known br_race_id: {len(ts)}")

# Which have a BR counterpart in the input?
br_race_ids_in_input = set(br["br_race_id"])
ts["br_in_input"] = ts["br_race_id"].isin(br_race_ids_in_input)
n_no_br = (~ts["br_in_input"]).sum()
n_has_br = ts["br_in_input"].sum()

# Build lookup structures
cluster_map = dict(zip(clust["unique_id"], clust["cluster_id"]))

# Index pairwise predictions for fast lookup
pw_set = set()
for _, r in pw.iterrows():
    pw_set.add((r["unique_id_l"], r["unique_id_r"]))
    pw_set.add((r["unique_id_r"], r["unique_id_l"]))

pw_prob = {}
for _, r in pw.iterrows():
    key1 = (r["unique_id_l"], r["unique_id_r"])
    key2 = (r["unique_id_r"], r["unique_id_l"])
    pw_prob[key1] = float(r["match_probability"])
    pw_prob[key2] = float(r["match_probability"])

ts_with_br = ts[ts["br_in_input"]].copy()

n_matched = 0
n_diff_person = 0
n_same_missed = 0
miss_reasons: dict[str, int] = {}

for _, row in ts_with_br.iterrows():
    ts_cluster = cluster_map.get(row["unique_id"])
    br_matches = br[br["br_race_id"] == row["br_race_id"]]

    # Did it cluster with any BR record sharing the same race?
    found_cluster = False
    for _, b in br_matches.iterrows():
        if ts_cluster and cluster_map.get(b["unique_id"]) == ts_cluster:
            found_cluster = True
            break

    if found_cluster:
        n_matched += 1
        continue

    # Not matched — is there a same-name BR record? (same person)
    ts_last = (row.get("last_name") or "").lower().strip()
    same_name_brs = [
        b
        for _, b in br_matches.iterrows()
        if (b.get("last_name") or "").lower().strip() == ts_last
    ]

    if not same_name_brs:
        n_diff_person += 1
        continue

    # Same person missed — figure out why
    n_same_missed += 1
    b = same_name_brs[0]
    pair_key = (row["unique_id"], b["unique_id"])
    prob = pw_prob.get(pair_key)

    if prob is not None:
        reason = f"below_threshold (scored {prob:.3f})"
    else:
        # Not blocked together — figure out why
        parts = []
        # Block 1: state + last_name — should always work if same last name & state
        same_state = row.get("state") == b.get("state")
        same_ln = ts_last == (b.get("last_name") or "").lower().strip()
        if not same_state:
            parts.append("state differs")
        if not same_ln:
            parts.append("last_name differs")
        # If state+last_name match, they SHOULD be blocked... unless nulls
        if same_state and same_ln:
            if not row.get("last_name") or not b.get("last_name"):
                parts.append("last_name is null")
            else:
                parts.append("blocked but not in pairwise (unexpected)")
        # Block 2: state + first_name + candidate_office
        fn_match = (row.get("first_name") or "").lower() == (
            b.get("first_name") or ""
        ).lower()
        off_match = row.get("candidate_office") == b.get("candidate_office")
        if not fn_match:
            parts.append(
                f"first_name: '{row.get('first_name')}' vs '{b.get('first_name')}'"
            )
        if not off_match:
            parts.append(
                f"office: '{row.get('candidate_office')}' vs '{b.get('candidate_office')}'"
            )
        reason = "not_blocked (" + "; ".join(parts) + ")"

    miss_reasons[reason] = miss_reasons.get(reason, 0) + 1

# Print full picture
print(f"\n{'='*60}")
print(f"FULL PICTURE: {len(ts)} TS records with known br_race_id")
print(f"{'='*60}")
print(
    f"  BR record NOT in input (can never match): {n_no_br:>5} ({n_no_br/len(ts)*100:.1f}%)"
)
print(
    f"  BR record IS in input:                    {n_has_br:>5} ({n_has_br/len(ts)*100:.1f}%)"
)
print()
print(f"  Of {n_has_br} where BR is in input:")
print(
    f"    Matched correctly:               {n_matched:>5} ({n_matched/n_has_br*100:.1f}%)"
)
print(
    f"    Different person (correct miss):  {n_diff_person:>5} ({n_diff_person/n_has_br*100:.1f}%)"
)
print(
    f"    Same person, failed to match:     {n_same_missed:>5} ({n_same_missed/n_has_br*100:.1f}%)"
)

print(f"\n{'='*60}")
print(f"SAME-PERSON MISS REASONS ({n_same_missed} total)")
print(f"{'='*60}")
for reason, count in sorted(miss_reasons.items(), key=lambda x: -x[1]):
    print(f"  {count:4d}  {reason}")
