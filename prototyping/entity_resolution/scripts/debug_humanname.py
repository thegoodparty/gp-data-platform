"""Check if HumanName parsing is mangling last names, causing blocking failures."""

import pandas as pd
from nameparser import HumanName

inp = pd.read_csv("data/input.csv", dtype=str).replace({"null": None, "": None})


# Simulate HumanName parsing like the main script does
def parse_name(first, last):
    full = f"{first or ''} {last or ''}"
    name = HumanName(full)
    parsed_first = name.first.lower().strip() if name.first else first
    parsed_last = name.last.lower().strip() if name.last else last
    return parsed_first, parsed_last


# Check cases from the miss list where state+last_name should block
test_cases = [
    # (TS first, TS last, BR first, BR last)
    (
        "dave",
        "belsky",
        "laura",
        "belsky",
    ),  # should work - but "Board Of Supervisor" pattern
    ("tom", "donkers", "thomas", "donkers"),
    ("connie", "seefeldt", "connie", "seefeldt"),
    ("fielding", "poe", "fielding", "poe"),
    ("kiersten", "cira", "kierstin", "cira"),
    ("jim", "james", "james", "james"),  # hypothetical
]

# Actually check real cases from data where office = "Board Of Supervisor" vs "County Legislature"
ts_bos = inp[
    (inp["source_name"] == "techspeed")
    & (
        inp["candidate_office"].str.contains(
            "Board Of Supervisor|Board of Supervisor", case=False, na=False
        )
    )
]

print(f"TS 'Board of Supervisor' records: {len(ts_bos)}")
print("\nChecking HumanName parsing on sample TS records:")

mismatches = 0
for _, row in ts_bos.head(20).iterrows():
    raw_first, raw_last = row["first_name"], row["last_name"]
    parsed_first, parsed_last = parse_name(raw_first, raw_last)
    if parsed_last != (raw_last or "").lower().strip():
        mismatches += 1
        print(
            f"  MISMATCH: '{raw_first} {raw_last}' -> parsed_last='{parsed_last}' (raw='{raw_last}')"
        )

print(f"\n{mismatches} / {min(20, len(ts_bos))} had last name changed by HumanName")

# Check all records for last name changes
print("\n=== Full dataset HumanName impact ===")
changed = 0
examples = []
for _, row in inp.iterrows():
    raw_last = (
        (str(row["last_name"]) if pd.notna(row["last_name"]) else "").lower().strip()
    )
    _, parsed_last = parse_name(row["first_name"], row["last_name"])
    if parsed_last != raw_last and raw_last:
        changed += 1
        if len(examples) < 20:
            examples.append(
                (row["source_name"], row["first_name"], row["last_name"], parsed_last)
            )

print(f"Records where HumanName changed last name: {changed} / {len(inp)}")
print("\nExamples:")
for src, first, last, parsed in examples:
    print(f"  [{src}] '{first} {last}' -> last='{parsed}'")
