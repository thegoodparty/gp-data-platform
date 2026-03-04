"""Debug why same state+last_name pairs aren't appearing in pairwise predictions."""

import pandas as pd

inp = pd.read_csv("data/input.csv", dtype=str).replace({"null": None, "": None})
pw = pd.read_csv("results/pairwise_predictions.csv", dtype=str)

# Check a specific case: don postler WI
ts_rec = inp[
    (inp["source_name"] == "techspeed") & (inp["last_name"].str.lower() == "postler")
]
br_rec = inp[
    (inp["source_name"] == "ballotready") & (inp["last_name"].str.lower() == "postler")
]

print("=== don postler debug ===")
for label, df in [("TS", ts_rec), ("BR", br_rec)]:
    for _, r in df.iterrows():
        print(f"  {label}: unique_id={r['unique_id']}")
        print(
            f"       first={r['first_name']} last={r['last_name']} state={r['state']}"
        )
        print(f"       office={r['candidate_office']} city={r['city']}")
        print()

# Check if they appear in pairwise
for _, ts in ts_rec.iterrows():
    for _, br in br_rec.iterrows():
        match = pw[
            (
                (pw["unique_id_l"] == ts["unique_id"])
                & (pw["unique_id_r"] == br["unique_id"])
            )
            | (
                (pw["unique_id_r"] == ts["unique_id"])
                & (pw["unique_id_l"] == br["unique_id"])
            )
        ]
        if len(match) > 0:
            print(f"  FOUND in pairwise: prob={match.iloc[0]['match_probability']}")
        else:
            print(f"  NOT in pairwise: {ts['unique_id']} vs {br['unique_id']}")
            # Check if last names match exactly after HumanName parsing
            print(f"    TS last_name raw: '{ts['last_name']}'")
            print(f"    BR last_name raw: '{br['last_name']}'")

# Now check a broader pattern: are there pairs that share state+last_name
# but are missing from pairwise entirely?
print("\n=== Checking 'Board Of Supervisor' vs 'County Legislature' pattern ===")
ts_bos = inp[
    (inp["source_name"] == "techspeed")
    & (inp["candidate_office"].str.contains("Board", case=False, na=False))
].head(3)

for _, ts in ts_bos.iterrows():
    br_same = inp[
        (inp["source_name"] == "ballotready")
        & (inp["last_name"].str.lower() == (ts["last_name"] or "").lower())
        & (inp["state"] == ts["state"])
    ]
    if len(br_same) > 0:
        for _, br in br_same.iterrows():
            pair = pw[
                (
                    (pw["unique_id_l"] == ts["unique_id"])
                    & (pw["unique_id_r"] == br["unique_id"])
                )
                | (
                    (pw["unique_id_r"] == ts["unique_id"])
                    & (pw["unique_id_l"] == br["unique_id"])
                )
            ]
            status = (
                f"prob={pair.iloc[0]['match_probability']}"
                if len(pair) > 0
                else "MISSING"
            )
            print(
                f"  {ts['first_name']} {ts['last_name']} ({ts['state']}) vs {br['first_name']} {br['last_name']}: {status}"
            )
