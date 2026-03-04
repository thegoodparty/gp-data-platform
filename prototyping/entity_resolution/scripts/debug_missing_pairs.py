"""Check specific cases labeled 'blocked but not in pairwise'."""

import pandas as pd

inp = pd.read_csv("data/input.csv", dtype=str).replace({"null": None, "": None})
pw = pd.read_csv("results/pairwise_predictions.csv", dtype=str)

# Build pairwise lookup
pw_lookup = {}
for _, r in pw.iterrows():
    pw_lookup[(r["unique_id_l"], r["unique_id_r"])] = float(r["match_probability"])
    pw_lookup[(r["unique_id_r"], r["unique_id_l"])] = float(r["match_probability"])

br = inp[inp["source_name"] == "ballotready"]
ts_known = inp[
    (inp["source_name"] == "techspeed") & (inp["br_race_id"] != "ts_found_race_net_new")
]
br_race_ids_in_input = set(br["br_race_id"])
ts_with_br = ts_known[ts_known["br_race_id"].isin(br_race_ids_in_input)]

# Find cases where same last name + state but "not in pairwise"
count = 0
examples = []
for _, ts_row in ts_with_br.iterrows():
    ts_last = (ts_row["last_name"] or "").lower().strip()
    br_matches = br[br["br_race_id"] == ts_row["br_race_id"]]
    same_name_brs = [
        b
        for _, b in br_matches.iterrows()
        if (b.get("last_name") or "").lower().strip() == ts_last
    ]

    for b in same_name_brs:
        pair_key = (ts_row["unique_id"], b["unique_id"])
        if pair_key not in pw_lookup:
            count += 1
            if len(examples) < 10:
                examples.append(
                    {
                        "ts_id": ts_row["unique_id"],
                        "br_id": b["unique_id"],
                        "ts_name": f"{ts_row['first_name']} {ts_row['last_name']}",
                        "br_name": f"{b['first_name']} {b['last_name']}",
                        "ts_state": ts_row["state"],
                        "br_state": b["state"],
                        "ts_office": ts_row["candidate_office"],
                        "br_office": b["candidate_office"],
                        "ts_city": ts_row.get("city"),
                        "br_city": b.get("city"),
                        "ts_gen": ts_row.get("general_election_date"),
                        "br_gen": b.get("general_election_date"),
                        "ts_pri": ts_row.get("primary_election_date"),
                        "br_pri": b.get("primary_election_date"),
                    }
                )

print(f"Total same-person pairs NOT in pairwise: {count}")
print(f"\nExamples:")
for e in examples:
    print(
        f"\n  TS: {e['ts_name']} | {e['ts_state']} | {e['ts_office']} | city={e['ts_city']} | gen={e['ts_gen']} pri={e['ts_pri']}"
    )
    print(
        f"  BR: {e['br_name']} | {e['br_state']} | {e['br_office']} | city={e['br_city']} | gen={e['br_gen']} pri={e['br_pri']}"
    )
    print(f"  ts_id: {e['ts_id']}")
    print(f"  br_id: {e['br_id']}")
    # Check all blocking rules
    same_state = e["ts_state"] == e["br_state"]
    same_last = True  # already filtered for this
    same_first = (e["ts_name"].split()[0] or "").lower() == (
        e["br_name"].split()[0] or ""
    ).lower()
    same_office = e["ts_office"] == e["br_office"]
    block1 = same_state and same_last  # state + last_name
    block2 = same_state and same_first and same_office  # state + first_name + office
    print(
        f"  Blocking: state+last={block1} (state={same_state}, last=True) | state+first+office={block2} (first={same_first}, office={same_office})"
    )
