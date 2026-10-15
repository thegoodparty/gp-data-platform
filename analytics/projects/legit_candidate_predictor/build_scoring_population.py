"""Build the LIVE scoring population: signups whose election has not happened yet.

The working set is, by construction, the rows we can label — completed elections only. That
is the complement of the rows sales actually works. This builder is the other half: the same
SQL, the same joins, the same PIT gates, with the completion filter flipped.

Sharing `build_working_set.SQL` is deliberate. If the training rows and the scored rows were
assembled by two hand-maintained queries they would drift, and the drift would show up as
unexplainable score behaviour months later.

Two things about this population that must travel with any score drawn from it:

1. **`corroborated` here is not a label.** It is renamed `corroborated_so_far` and is close to
   meaningless: these elections have not happened, so the roster process that produces
   corroboration has barely started. It is carried only as a diagnostic. Never fit on it,
   never evaluate against it.
2. **`n_br_candidacies_by_deadline` is right-censored for rows whose filing deadline has not
   passed.** In training every deadline is in the past, so the count is final. Here it is
   partial for part of the population, which is genuine train/serve skew. `deadline_passed`
   is emitted so the effect can be measured rather than assumed.

Run:
  cd analytics && uv run --with pyarrow --with databricks-sql-connector python \
      projects/legit_candidate_predictor/build_scoring_population.py
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "lib"))
from build_working_set import SQL, UPCOMING  # noqa: E402
from databricks_conn import run_query  # noqa: E402

OUT = HERE / "datasets" / "scoring_population.parquet"


def main(start: str, end: str) -> None:
    df = run_query(SQL.format(start=start, end=end, completion=UPCOMING))
    df = df.rename(columns={"corroborated": "corroborated_so_far"})

    elec = pd.to_datetime(df["general_election_date"])
    dl = pd.to_datetime(df["filing_deadline"])
    today = pd.Timestamp.utcnow().tz_localize(None).normalize()
    df["deadline_passed"] = dl.notna() & (dl < today)
    df["lead_time_days"] = (elec - pd.to_datetime(df["signup_date"])).dt.days
    df["days_to_election"] = (elec - today).dt.days

    print(f"rows: {len(df):,}  users: {df.user_id.nunique():,}")
    print(f"election dates: {elec.min().date()} .. {elec.max().date()}")
    print(
        f"deadline present: {dl.notna().mean():.1%}   "
        f"deadline already passed: {df.deadline_passed.mean():.1%}"
    )
    print(
        f"lead time days: median {df.lead_time_days.median():.0f}  "
        f"p10 {df.lead_time_days.quantile(.1):.0f}  p90 {df.lead_time_days.quantile(.9):.0f}"
    )
    print(f"corroborated_so_far (NOT a label): {df.corroborated_so_far.mean():.1%}")

    print("\ntop election dates:")
    print(elec.dt.date.value_counts().head(8).to_string())

    OUT.parent.mkdir(exist_ok=True)
    df.to_parquet(OUT, index=False)
    print(f"\nsaved -> {OUT}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-01-01")
    ap.add_argument("--end", default="2050-01-01")
    a = ap.parse_args()
    main(a.start, a.end)
