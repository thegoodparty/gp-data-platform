"""Land the scored live population into the model_predictions schema.

Dated and immutable, matching the web_verify_verdicts convention: a later scoring run becomes
a new dated table, never an edit to this one. Safe to re-run — it recreates the table from the
parquet and refuses to finish if the landed row count does not match.

Every row carries `model_id` and `mlflow_run_id`, which is the whole point: a cell in a
dashboard resolves to a model definition (the manifest, also attached to that MLflow run) and
to the fitted estimator that produced the number.

Auth is OAuth via the ~/.databrickscfg profile (databricks_conn.get_connection), so no
PAT is needed and 1Password does not have to be unlocked.

Run: cd analytics && uv run --with pyarrow --with databricks-sql-connector \\
       --with databricks-sdk python \
       projects/legit_candidate_predictor/load_scored_population.py [--schema <your_dev_schema>]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "lib"))
from databricks_conn import get_connection  # noqa: E402

SCORED = HERE / "datasets" / "scored_population.parquet"
CATALOG = "goodparty_data_catalog"
TABLE = "win_candidate_scores_20260814"

DDL = """
create or replace table {fqn} (
    user_id bigint comment 'gp_api user. Deliberately NOT unique: one person can hold two campaigns and each is scored on its own race.',
    campaign_id bigint comment 'gp_api campaign the score was computed for. The grain of this table, and unique in it.',
    p_corroborated double comment 'Raw model probability that this signup will be corroborated by BallotReady, TechSpeed or DDHQ. Uncalibrated for this population: training base rate is 58.9% against a live population whose median lead time is 421 days. Use expected_corroboration_rate or tier for anything quotable; this column is the underlying rank.',
    expected_corroboration_rate double comment 'Share of LABELLED rows at the same within-cohort rank and lead-time bucket that were corroborated. This is the quotable number: 0.73 means roughly 73 of 100 people ranked like this ended up on an external roster. Fitted by isotonic regression per lead-time bucket. Weakest for 365d+, where 454 labelled rows stand in for 6,353 live ones.',
    tier string comment 'Three-way cut on expected_corroboration_rate: likely_corroborated (>=0.60), unsure (0.10-0.60), unlikely_corroborated (<0.10). Validated on this population as corroborations arrive: rows already corroborated at scoring time run 9.5% in likely, 2.4% in unsure, 0.4% in unlikely — a 24x separation on live data, not just on the training set.',
    score_bucket string comment 'Five absolute probability bands on expected_corroboration_rate: p_under_5, p_5_15, p_15_35, p_35_60, p_60_plus. Comparable across cohorts, so the table can be sliced by anything. Live validation, share already corroborated at scoring time: 0.4%, 0.9%, 2.0%, 7.1%, 9.5% — monotone, 22x span. Prefer this over tier for working analysis; tier is the same information collapsed to three groups for a headline.',
    score_pct_within_cohort double comment 'Percentile of p_corroborated within cohort_key, 0-100. This is the transferable quantity: ranking survives the 2025-to-2026 composition shift, absolute values do not.',
    band string comment 'Quintile within cohort_key. Q5 = highest-scoring fifth of comparable rows. Deliberately neutral names: Q1 means lowest ranked among peers, never "not a candidate".',
    cohort_key string comment 'Comparison group: signup-lead bucket x office type, or the bucket alone where the fine cell held under 100 rows. signup_lead_* is how far AHEAD OF THEIR ELECTION the person signed up, i.e. how speculative the signup was. It is NOT time remaining: 88% of this table votes within 90 days regardless of bucket.',
    cohort_n bigint comment 'Rows in this cohort. Percentiles from small cohorts are coarse.',
    cohort_fallback boolean comment 'True where the office-level cell was too thin and the row was banded against its signup-lead bucket alone.',
    lead_time_days bigint comment 'Days from signup to general election. How far ahead the person signed up. The dominant driver of eventual corroboration rate, because early signups are more speculative.',
    days_to_election bigint comment 'Days from scored_at to the general election. How much time is left, which is a different question from lead_time_days. Median 81: most of this table votes on 2026-11-03.',
    general_election_date date comment 'Election the campaign points at. All in the future as of scored_at.',
    deadline_passed boolean comment 'Whether the filing deadline had passed at scoring time. Where false, the by-deadline BR arrivals feature is right-censored, so the score rests on less information.',
    model_id string comment 'Model definition hash. Resolves to the manifest attached to mlflow_run_id, which lists every feature used and every column deliberately excluded with its reason.',
    mlflow_run_id string comment 'MLflow run holding the fitted estimator and the manifest. runs:/<id>/model loads the exact model that produced p_corroborated.',
    profile_p_corroborated double comment 'Raw probability from the PROFILE model, which never saw campaign_state, office_type, incumbency or voter_count.',
    profile_expected_corroboration_rate double comment 'Calibrated probability from the PROFILE model. Live validation by bucket: 0.2%, 0.3%, 1.6%, 5.8%, 13.2% already corroborated — a 77x span, slightly sharper separation than the serving model.',
    profile_score_bucket string comment 'USE THIS to profile groups by state, office type, incumbency or district size. Same five bands as score_bucket but from the model blind to those variables, so a claim about them is a finding rather than a restatement of the scoring rule. Its cohort deliberately excludes office type too (see profile_cohort_key).',
    profile_score_pct_within_cohort double comment 'Percentile within profile_cohort_key.',
    profile_cohort_key string comment 'Signup-lead bucket only, with NO office split. Ranking inside office cohorts would force every office type to hold 20% of every band and flatten the distribution the report is trying to show.',
    profile_model_id string comment 'Model definition hash for the profile variant. Resolves to its manifest, which lists the 20 masked columns with reasons.',
    profile_mlflow_run_id string comment 'MLflow run holding the fitted profile estimator and its manifest.',
    ws_version string comment 'Fingerprint of the working set the model was trained on.',
    scored_at date comment 'Day the scoring run executed.'
)
comment 'Corroboration scores for Win signups whose election has NOT yet happened, as of 2026-08-14. The complement of the predictor training set, which by construction holds only completed elections. 88% of these rows vote within 90 days (median 81; 2026-11-03). Use `tier` for a three-way cut and `expected_corroboration_rate` for anything quotable; p_corroborated is the underlying rank and is not calibrated for this population. Reading rules: (1) the score predicts CORROBORATION by an external roster, which is a good first-pass proxy for a real candidacy but not the same thing — the bottom decile of the training population was still 44-52% real, so treat unlikely_corroborated as deprioritise, not exclude; (2) compare within cohort_key, never across, because the same rank means very different things at different signup lead times; (3) 78% of these rows have no BallotReady roster yet, so the model has less to work with than it did in training, and the tier mix will shift as rosters land — re-score periodically. Model definition and fitted estimator: see model_id and mlflow_run_id.'
"""

COLUMNS = [
    "user_id",
    "campaign_id",
    "p_corroborated",
    "expected_corroboration_rate",
    "tier",
    "score_bucket",
    "score_pct_within_cohort",
    "band",
    "cohort_key",
    "cohort_n",
    "cohort_fallback",
    "lead_time_days",
    "days_to_election",
    "general_election_date",
    # Order MUST match the parquet: rows are zipped positionally against it below.
    "deadline_passed",
    "model_id",
    "mlflow_run_id",
    "ws_version",
    "scored_at",
    "profile_p_corroborated",
    "profile_expected_corroboration_rate",
    "profile_score_bucket",
    "profile_score_pct_within_cohort",
    "profile_cohort_key",
    "profile_model_id",
    "profile_mlflow_run_id",
]
INT_COLS = {"user_id", "campaign_id", "cohort_n", "lead_time_days", "days_to_election"}
FLOAT_COLS = {
    "p_corroborated",
    "score_pct_within_cohort",
    "expected_corroboration_rate",
    "profile_p_corroborated",
    "profile_expected_corroboration_rate",
    "profile_score_pct_within_cohort",
}
BOOL_COLS = {"cohort_fallback", "deadline_passed"}


def main(schema: str) -> None:
    fqn = f"{CATALOG}.{schema}.{TABLE}"
    df = pd.read_parquet(SCORED)
    assert list(df.columns) == COLUMNS, f"parquet columns drifted from the DDL: {list(df.columns)}"
    assert (
        df["mlflow_run_id"].notna().all()
    ), "rows without an mlflow_run_id would not resolve to a fitted model — do not land"
    # Grain is the campaign, not the person: 14 users hold two campaigns and each is scored
    # against its own race. Asserting on user_id here would be asserting the wrong grain.
    assert df["campaign_id"].is_unique, "campaign_id is not unique in the parquet"
    print(f"loading {len(df):,} rows -> {fqn}")

    def cell(col: str, v: object) -> object:
        if v is None or pd.isna(v):
            return None
        if col in INT_COLS:
            return int(v)
        if col in FLOAT_COLS:
            return float(v)
        if col in BOOL_COLS:
            return bool(v)
        return str(v)

    rows = [
        tuple(cell(k, v) for k, v in zip(COLUMNS, rec, strict=False))
        for rec in df.itertuples(index=False, name=None)
    ]
    placeholders = "(" + ", ".join(["?"] * len(COLUMNS)) + ")"

    # OAuth via the ~/.databrickscfg profile, not the 1Password PAT. get_connection is the
    # repo's shared path and is documented for exactly this (a cursor for writes), and the PAT
    # route silently stops resolving whenever 1Password is locked.
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(DDL.format(fqn=fqn))
        for i in range(0, len(rows), 200):
            batch = rows[i : i + 200]
            cur.execute(
                f"insert into {fqn} ({', '.join(COLUMNS)}) values " + ", ".join([placeholders] * len(batch)),
                [v for row in batch for v in row],
            )
            print(f"  inserted {min(i + 200, len(rows)):,}/{len(rows):,}")

        cur.execute(
            f"select count(*), count(distinct campaign_id), "
            f"count(distinct user_id), count(distinct model_id) from {fqn}"
        )
        n, n_campaigns, n_users, n_models = cur.fetchone()
        print(f"\nlanded: {n:,} rows, {n_campaigns:,} campaigns, {n_users:,} users, " f"{n_models} model_id")
        assert n == len(df), f"landed {n} rows, parquet has {len(df)} — do not ship"
        assert n_campaigns == n, "campaign_id is not unique in the landed table"
        assert n_models == 1, "more than one model_id landed in a single scoring run"

        cur.execute(
            f"select tier, count(*), "
            f"round(avg(expected_corroboration_rate), 3) "
            f"from {fqn} group by 1 order by 2 desc"
        )
        print("\ntier distribution in the landed table:")
        for tier, cnt, avg in cur.fetchall():
            print(f"  {tier:<24} {cnt:>6,}  mean expected corroboration {avg}")

    print("\nOK — landed row count matches the parquet.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default="model_predictions")
    main(ap.parse_args().schema)
