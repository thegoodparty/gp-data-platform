"""DATA-2239 Area 2 build 1: BallotReady position attributes.

The cost of entry to a race is the most direct legitimacy proxy available, and none of it is in
the model today: the working set carries only `br_normalized_position_type`, `office_type`,
`election_level`, and `voter_count`. BR's position record carries filing fees, filing-requirement
text, signature requirements, minimum age, seats, salary, election frequency, and appointment
status. Measured coverage: 18,018 of 20,478 rows (88.0%), which is exactly the decidable
population, so the missing side is the already-rule-decided bucket rather than a new hole.

These are static race properties rather than candidate behaviour, so the family is pass-B by
construction. One caveat recorded in the intake review: BR position records are a current
snapshot, so a fee or salary edited after signup would be read at its current value. Same class
as `voter_count` and `br_normalized_position_type`, which v1 already accepts on that basis.

Two things need real handling rather than a straight select:
- `salary` is free text ("$127,134/year", "Only expenses", "Unpaid"), so it is parsed into an
  amount, a period, and a three-way paid/expenses/unpaid class.
- `election_frequencies` is an array of id structs; the frequency value itself lives in
  `int__ballotready_position_election_frequency`, whose `database_id` is the
  PositionElectionFrequency id, NOT the position id. Joining it directly to a position id returns
  zero rows, which is what an earlier probe did before this was traced.

Outputs `datasets/br_position_features.parquet` at br_position_id grain, plus a profile and an
intake review.

Run: uv run --with pyarrow python projects/legit_candidate_predictor/build_br_position.py
"""

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "lib"))
from profile import write_profile  # noqa: E402

from databricks_conn import run_query  # noqa: E402
from intake import write_intake  # noqa: E402

SQL = """
with ours as (
  select distinct cast(ballotready_position_id as bigint) as br_position_id
  from goodparty_data_catalog.mart_analytics.users_win_candidacy
  where is_latest_version and not is_demo and ballotready_position_id is not null
),
p as (
  select cast(pos.database_id as bigint) as br_position_id,
         pos.seats, pos.selections_allowed, pos.tier, pos.minimum_age, pos.maximum_filing_fee,
         pos.salary, pos.employment_type, pos.partisan_type, pos.level,
         pos.has_primary, pos.has_majority_vote_primary, pos.has_ranked_choice_general,
         pos.is_partisan, pos.is_judicial, pos.is_appointed, pos.is_retention,
         pos.is_staggered_term, pos.has_unknown_boundaries,
         pos.must_be_registered_voter, pos.must_be_resident,
         pos.must_have_professional_experience,
         pos.filing_requirements, pos.eligibility_requirements, pos.paperwork_instructions,
         pos.filing_address, pos.filing_phone,
         pos.election_frequencies
  from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_position pos
  join ours o on o.br_position_id = cast(pos.database_id as bigint)
),
-- election_frequencies holds PositionElectionFrequency ids; the values live in the int model.
-- Explode in its own CTE: a lateral view cannot be followed by a join in the same FROM clause.
freq_ids as (
  select br_position_id, ef.databaseid as freq_id
  from p
  lateral view explode(p.election_frequencies) as ef
),
freq as (
  select fi.br_position_id,
         max(f.frequency)      as freq_raw,
         max(f.seats)          as freq_seats,
         max(f.reference_year) as freq_reference_year
  from freq_ids fi
  join goodparty_data_catalog.dbt.int__ballotready_position_election_frequency f
    on cast(f.database_id as string) = cast(fi.freq_id as string)
  group by fi.br_position_id
),
holders as (
  select cast(br_position_id as bigint) as br_position_id,
         count(*)                                             as holder_count,
         max(case when is_vacant then 1 else 0 end)            as holder_any_vacant,
         count(distinct br_office_holder_id)                   as holder_distinct
  from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_s3_office_holders_v3
  group by 1
)
select p.* except (election_frequencies),
       f.freq_raw, f.freq_seats, f.freq_reference_year,
       h.holder_count, h.holder_any_vacant, h.holder_distinct
from p
left join freq f on f.br_position_id = p.br_position_id
left join holders h on h.br_position_id = p.br_position_id
"""

MONEY = re.compile(r"\$\s*([\d,]+(?:\.\d+)?)")
PERIODS = ("year", "month", "week", "day", "hour", "meeting", "session", "term", "annum")
# The boolean "mentions signatures" is 93.8% true, so it cannot discriminate. The COUNT is the
# feature: 25 signatures and 2,000 signatures are different barriers entirely.
SIGNATURE_COUNT = re.compile(r"([\d][\d,]*)\s*(?:valid\s+|registered\s+voter\s+)?signatures?", re.I)


def _scalar(v):
    """Collapse an array-valued cell to a number. BR frequency arrives as array<int>."""
    if isinstance(v, list | tuple | np.ndarray):
        vals = [x for x in v if x is not None and not (isinstance(x, float) and np.isnan(x))]
        return float(max(vals)) if vals else np.nan
    return v


def parse_salary(s: pd.Series) -> pd.DataFrame:
    """Free text to (amount, period, class). Unparseable text stays NaN rather than becoming 0."""
    low = s.fillna("").str.lower()
    amount = s.fillna("").str.extract(MONEY, expand=False).str.replace(",", "", regex=False)
    amount = pd.to_numeric(amount, errors="coerce")

    period = pd.Series(np.nan, index=s.index, dtype=object)
    for p in PERIODS:
        period = period.where(period.notna() | ~low.str.contains(p, regex=False), p)

    cls = pd.Series(np.nan, index=s.index, dtype=object)
    cls = cls.where(~low.str.contains("unpaid|no salary|none|volunteer", regex=True), "unpaid")
    cls = cls.where(~low.str.contains("expense", regex=True) | cls.notna(), "expenses_only")
    cls = cls.where(amount.isna() | cls.notna(), "paid")
    return pd.DataFrame({"pos_salary_amount": amount, "pos_salary_period": period, "pos_salary_class": cls})


def build(raw: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({"br_position_id": raw["br_position_id"]})

    # Numeric race-structure facts. NaN stays NaN: an unknown filing fee is not a zero fee, which
    # is the sentinel lesson from n_br_candidacies_by_deadline.
    out["pos_seats"] = raw["seats"]
    # pos_selections_allowed dropped: 0.9989 correlated with seats (intake review).
    out["pos_tier"] = raw["tier"]
    out["pos_min_age"] = raw["minimum_age"]
    out["pos_max_filing_fee"] = raw["maximum_filing_fee"]
    out["pos_filing_fee_known"] = raw["maximum_filing_fee"].notna().astype(int)
    out["pos_filing_fee_is_zero"] = np.where(
        raw["maximum_filing_fee"].notna(), raw["maximum_filing_fee"].eq(0).astype(float), np.nan
    )

    # Cost-of-entry text. Length is a crude but honest proxy for procedural burden.
    # Lengths only. The *_present indicators were dropped after the intake review: all four sat at
    # 99.8% present, so their minority class is far too small for a split.
    for col, name in (
        ("filing_requirements", "filing_req"),
        ("eligibility_requirements", "eligibility"),
        ("paperwork_instructions", "paperwork"),
    ):
        txt = raw[col].fillna("")
        out[f"pos_{name}_len"] = np.where(raw[col].notna(), txt.str.len(), np.nan)

    sig = raw["filing_requirements"].fillna("").str.extract(SIGNATURE_COUNT, expand=False)
    sig = pd.to_numeric(sig.str.replace(",", "", regex=False), errors="coerce")
    out["pos_signature_count"] = sig
    out["pos_signature_count_known"] = sig.notna().astype(int)

    # Booleans arrive as nullable; keep three states (true / false / unknown).
    # is_appointed dropped: constant false across all 25,167 positions BR gives us, despite the
    # catalog expecting it to be a near-tautological signal. `users_win_candidacy.is_appointed` is
    # the populated one; this BR field is not.
    for col, name in (
        ("has_primary", "has_primary"),
        ("has_majority_vote_primary", "majority_vote_primary"),
        ("has_ranked_choice_general", "ranked_choice_general"),
        ("is_partisan", "is_partisan"),
        ("is_judicial", "is_judicial"),
        # is_retention dropped (Tristan, 2026-08-06): 10 positives in the working
        # set, and is_judicial already covers the judicial signal at 340 rows.
        ("is_staggered_term", "is_staggered_term"),
        ("has_unknown_boundaries", "unknown_boundaries"),
        ("must_be_registered_voter", "must_be_registered_voter"),
        ("must_be_resident", "must_be_resident"),
        ("must_have_professional_experience", "must_have_prof_experience"),
    ):
        out[f"pos_{name}"] = raw[col].map({True: 1.0, False: 0.0}).astype(float)

    out = pd.concat([out, parse_salary(raw["salary"])], axis=1)
    out["pos_salary_known"] = raw["salary"].notna().astype(int)

    out["pos_employment_type"] = raw["employment_type"]
    out["pos_partisan_type"] = raw["partisan_type"]
    out["pos_br_level"] = raw["level"]

    # `frequency` is an array in the int model (a position can carry several cadences).
    # freq_seats dropped: 100% null for every position we pull.
    out["pos_election_frequency_years"] = raw["freq_raw"].map(_scalar)

    # Office-holder block: KEPT by decision (Tristan, 2026-08-06). Rationale: holder records are
    # written during a term, so they predate any filing deadline and are comfortably knowable at
    # signup, and an incumbent is almost certainly a real candidate.
    # Recorded caveat: these columns are POSITION-level, so they say how many holders a seat has,
    # not whether this signup is one of them. And where BR has no holder record, 99.4% of rows also
    # have no BR candidacy record (corroboration 25.1% vs 67.9%), so the block is largely nested in
    # the coverage-proxy channel the sentinel fix removed; the forward split was flat at 0.8912 with
    # and without it. The person-level version of the argument is the parked incumbency feature.
    #
    # NOT fillna(0). A position with no office-holder record has an UNKNOWN holder count, not zero
    # holders, and filling it would recreate the sentinel conflation that cost 0.005 AUC on
    # n_br_candidacies_by_deadline. The intake sentinel scan caught this on the first run.
    out["pos_holder_count"] = raw["holder_count"]
    out["pos_holder_known"] = raw["holder_count"].notna().astype(int)
    out["pos_holder_any_vacant"] = raw["holder_any_vacant"]

    # pos_fee_per_seat dropped: 0.982 correlated with the raw fee, because seats is 1 for most
    # positions. The informative ratio is fee per voter, which needs voter_count from the working
    # set and is therefore built at merge time in the experiment.
    return out


if __name__ == "__main__":
    raw = run_query(SQL)
    print(f"positions pulled: {len(raw):,}  distinct: {raw.br_position_id.nunique():,}")
    assert raw["br_position_id"].is_unique, "position id is not unique in the pull"

    feats = build(raw)
    print(f"features built: {feats.shape[1] - 1}")
    print("salary parse:", feats["pos_salary_class"].value_counts(dropna=False).to_dict())
    print(
        "frequency years:", feats["pos_election_frequency_years"].value_counts(dropna=False).head(6).to_dict()
    )

    out = HERE / "datasets" / "br_position_features.parquet"
    feats.to_parquet(out, index=False)
    print(f"saved -> {out}")

    write_profile(
        feats,
        "br_position_features",
        note="BallotReady position attributes at position grain. Static race properties, "
        "pass-B by construction; current-snapshot caveat in the intake review.",
    )
    write_intake(
        feats,
        "br_position_features",
        key="br_position_id",
        source="dbt.stg_airbyte_source__ballotready_api_position + "
        "int__ballotready_position_election_frequency + "
        "stg_airbyte_source__ballotready_s3_office_holders_v3",
        join="br_position_id -> working_set.br_position_id",
        pit_class="B (static race properties; current-snapshot caveat)",
    )
