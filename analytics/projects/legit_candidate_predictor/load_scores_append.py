"""Append the current scoring run into the long-format score tables, and refresh the view.

Replaces the dated-snapshot pattern for anything ongoing. `win_candidate_scores_20260814` stays
as the 2026-08-14 snapshot; this is the shape everything after it uses.

Why long and append-only: a wide table with one column set per model has to be rewritten to add
a model or a scoring date, and it loses history. Re-scoring monthly is the plan (the tier mix
shifts as filing deadlines pass and BallotReady rosters land), and watching a campaign move
between buckets over time is a thing the report will want in October. So:

  win_candidate_scores          (campaign_id, model_id, scored_at) — one row per model per run
  win_candidate_score_context   (campaign_id, scored_at)           — campaign facts, not scores
  win_candidate_scores_latest   view                                — latest run pivoted wide

Idempotent: re-running deletes this (model_id, scored_at) slice before inserting, so a repeated
run corrects itself rather than doubling rows.

Auth is OAuth via the ~/.databrickscfg profile (databricks_conn.get_connection), so no
PAT is needed and 1Password does not have to be unlocked.

Run: cd analytics && uv run --with pyarrow --with databricks-sql-connector \\
       --with databricks-sdk python \
       projects/legit_candidate_predictor/load_scores_append.py [--schema <your_dev_schema>]
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
SCORES, CONTEXT, VIEW = ("win_candidate_scores", "win_candidate_score_context", "win_candidate_scores_latest")

SCORES_DDL = """
create table if not exists {fqn} (
    campaign_id bigint comment 'gp_api campaign. With model_id and scored_at, the grain of this table.',
    user_id bigint comment 'gp_api user. Not unique: a person can hold two campaigns.',
    model_id string comment 'Model definition hash. Resolves to a manifest listing every feature used and every column held out with its reason, attached to mlflow_run_id.',
    model_role string comment 'serving = every admissible feature, use for prioritising outreach. profile = blind to campaign_state, office_type, incumbency and district size, use when the ANALYSIS is about one of those.',
    scored_at date comment 'Day this scoring run executed. Re-scoring appends a new date rather than overwriting.',
    p_corroborated double comment 'Raw model probability. The underlying rank; not calibrated for the live population.',
    expected_corroboration_rate double comment 'Calibrated probability: the share of labelled rows at the same within-cohort rank and signup-lead bucket that were corroborated. The quotable number.',
    score_bucket string comment 'Five bands on expected_corroboration_rate: p_under_5, p_5_15, p_15_35, p_35_60, p_60_plus.',
    score_pct_within_cohort double comment 'Percentile within cohort_key.',
    cohort_key string comment 'Comparison group. signup_lead_* is how far AHEAD of their election the person signed up, not time remaining. The profile role deliberately omits office type from the cohort so office distributions are not flattened by construction.',
    mlflow_run_id string comment 'MLflow run holding the fitted estimator and the manifest. runs:/<id>/model loads the exact model.',
    ws_version string comment 'Fingerprint of the working set the model was trained on.'
)
comment 'Corroboration scores for Win signups whose election has not yet happened. Append-only: one row per campaign per model per scoring run. The score predicts confirmation by an external roster (BallotReady, TechSpeed, DDHQ), which is a good first-pass proxy for a real candidacy but not the same thing — the bottom band catches junk signups AND genuine long-shot candidates, so treat it as deprioritise, never exclude. Join win_candidate_score_context for campaign facts, or use the win_candidate_scores_latest view.'
"""

CONTEXT_DDL = """
create table if not exists {fqn} (
    campaign_id bigint comment 'gp_api campaign. With scored_at, the grain of this table.',
    user_id bigint comment 'gp_api user.',
    scored_at date comment 'Scoring run this snapshot belongs to.',
    lead_time_days bigint comment 'Days from signup to general election: how far ahead the person signed up. Drives the base rate.',
    days_to_election bigint comment 'Days from scored_at to the election: how much time is left. A different question from lead_time_days.',
    general_election_date date comment 'Election the campaign points at.',
    deadline_passed boolean comment 'Whether the filing deadline had passed at scoring time. Where false the BR arrivals feature is right-censored.'
)
comment 'Campaign facts as of each scoring run. Separate from win_candidate_scores because these describe the campaign, not any model, and would otherwise be duplicated once per model.'
"""

VIEW_DDL = """
create or replace view {fqn} comment
  'Latest scoring run, pivoted wide. The convenience surface for reporting. Use profile_* whenever the analysis is ABOUT campaign_state, office_type, incumbency or district size, because the serving model uses all four as inputs and profiling its output on them would restate the scoring rule. Use serving_* to prioritise outreach.'
as
with latest as (select max(scored_at) d from {scores})
select
    s.campaign_id, s.user_id, s.scored_at,
    max(case when s.model_role = 'serving' then s.expected_corroboration_rate end) as serving_expected_rate,
    max(case when s.model_role = 'serving' then s.score_bucket end)                as serving_bucket,
    max(case when s.model_role = 'serving' then s.score_pct_within_cohort end)     as serving_pct_within_cohort,
    max(case when s.model_role = 'serving' then s.cohort_key end)                  as serving_cohort_key,
    max(case when s.model_role = 'serving' then s.model_id end)                    as serving_model_id,
    max(case when s.model_role = 'profile' then s.expected_corroboration_rate end) as profile_expected_rate,
    max(case when s.model_role = 'profile' then s.score_bucket end)                as profile_bucket,
    max(case when s.model_role = 'profile' then s.score_pct_within_cohort end)     as profile_pct_within_cohort,
    max(case when s.model_role = 'profile' then s.model_id end)                    as profile_model_id,
    max(c.lead_time_days) as lead_time_days, max(c.days_to_election) as days_to_election,
    max(c.general_election_date) as general_election_date,
    max(c.deadline_passed) as deadline_passed
from {scores} s
join latest l on s.scored_at = l.d
left join {context} c on c.campaign_id = s.campaign_id and c.scored_at = s.scored_at
group by s.campaign_id, s.user_id, s.scored_at
"""

SCORE_COLS = [
    "campaign_id",
    "user_id",
    "model_id",
    "model_role",
    "scored_at",
    "p_corroborated",
    "expected_corroboration_rate",
    "score_bucket",
    "score_pct_within_cohort",
    "cohort_key",
    "mlflow_run_id",
    "ws_version",
]
CONTEXT_COLS = [
    "campaign_id",
    "user_id",
    "scored_at",
    "lead_time_days",
    "days_to_election",
    "general_election_date",
    "deadline_passed",
]
INT_COLS = {"campaign_id", "user_id", "lead_time_days", "days_to_election"}
FLOAT_COLS = {"p_corroborated", "expected_corroboration_rate", "score_pct_within_cohort"}
BOOL_COLS = {"deadline_passed"}


def long_frames(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split the wide scored parquet into one row per (campaign, model) plus the context row."""
    base = {
        "campaign_id": df["campaign_id"],
        "user_id": df["user_id"],
        "scored_at": df["scored_at"],
        "ws_version": df["ws_version"],
    }
    serving = pd.DataFrame(
        {
            **base,
            "model_id": df["model_id"],
            "model_role": "serving",
            "p_corroborated": df["p_corroborated"],
            "expected_corroboration_rate": df["expected_corroboration_rate"],
            "score_bucket": df["score_bucket"],
            "score_pct_within_cohort": df["score_pct_within_cohort"],
            "cohort_key": df["cohort_key"],
            "mlflow_run_id": df["mlflow_run_id"],
        }
    )
    profile = pd.DataFrame(
        {
            **base,
            "model_id": df["profile_model_id"],
            "model_role": "profile",
            "p_corroborated": df["profile_p_corroborated"],
            "expected_corroboration_rate": df["profile_expected_corroboration_rate"],
            "score_bucket": df["profile_score_bucket"],
            "score_pct_within_cohort": df["profile_score_pct_within_cohort"],
            "cohort_key": df["profile_cohort_key"],
            "mlflow_run_id": df["profile_mlflow_run_id"],
        }
    )
    scores = pd.concat([serving, profile], ignore_index=True)[SCORE_COLS]
    context = df[CONTEXT_COLS].copy()
    return scores, context


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


def insert(cur, fqn: str, cols: list[str], frame: pd.DataFrame) -> None:
    rows = [
        tuple(cell(k, v) for k, v in zip(cols, rec, strict=False))
        for rec in frame[cols].itertuples(index=False, name=None)
    ]
    ph = "(" + ", ".join(["?"] * len(cols)) + ")"
    for i in range(0, len(rows), 200):
        batch = rows[i : i + 200]
        cur.execute(
            f"insert into {fqn} ({', '.join(cols)}) values " + ", ".join([ph] * len(batch)),
            [v for row in batch for v in row],
        )
        print(f"    {min(i + 200, len(rows)):,}/{len(rows):,}")


def main(schema: str) -> None:
    scores_fqn = f"{CATALOG}.{schema}.{SCORES}"
    context_fqn = f"{CATALOG}.{schema}.{CONTEXT}"
    view_fqn = f"{CATALOG}.{schema}.{VIEW}"

    df = pd.read_parquet(SCORED)
    scores, context = long_frames(df)
    run_date = str(df["scored_at"].iloc[0])
    model_ids = sorted(scores["model_id"].unique())
    assert scores["mlflow_run_id"].notna().all(), "a score row has no MLflow run — do not land"
    assert not scores.duplicated(
        ["campaign_id", "model_id", "scored_at"]
    ).any(), "duplicate (campaign_id, model_id, scored_at) in the frame"
    print(
        f"appending {len(scores):,} score rows ({len(model_ids)} models) "
        f"+ {len(context):,} context rows for {run_date}"
    )

    # OAuth via the ~/.databrickscfg profile, not the 1Password PAT. get_connection is the
    # repo's shared path and is documented for exactly this (a cursor for writes), and the PAT
    # route silently stops resolving whenever 1Password is locked.
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(SCORES_DDL.format(fqn=scores_fqn))
        cur.execute(CONTEXT_DDL.format(fqn=context_fqn))

        # Idempotence: clear only this run's slice, never the history.
        for mid in model_ids:
            cur.execute(f"delete from {scores_fqn} where scored_at = '{run_date}' " f"and model_id = '{mid}'")
        cur.execute(f"delete from {context_fqn} where scored_at = '{run_date}'")

        print("  scores:")
        insert(cur, scores_fqn, SCORE_COLS, scores)
        print("  context:")
        insert(cur, context_fqn, CONTEXT_COLS, context)

        cur.execute(VIEW_DDL.format(fqn=view_fqn, scores=scores_fqn, context=context_fqn))

        cur.execute(
            f"select scored_at, model_role, count(*), "
            f"count(distinct campaign_id) from {scores_fqn} "
            f"group by 1, 2 order by 1 desc, 2"
        )
        print("\nscore rows by run and role:")
        for d, role, n, nc in cur.fetchall():
            print(f"  {d}  {role:<8} {n:>7,} rows / {nc:>7,} campaigns")
            assert n == nc, f"{role} has duplicate campaigns in run {d}"

        cur.execute(f"select count(*) from {view_fqn}")
        (n_view,) = cur.fetchone()
        print(f"\nview {VIEW}: {n_view:,} rows")
        assert n_view == len(context), "view row count does not match the run"

        cur.execute(
            f"select profile_bucket, count(*), round(avg(profile_expected_rate), 3) "
            f"from {view_fqn} group by 1 order by 3"
        )
        print("\nprofile buckets through the view:")
        for b, n, e in cur.fetchall():
            print(f"  {b:<12} {n:>6,}  expected {e}")

    print("\nOK — appended, view refreshed.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default="model_predictions")
    main(ap.parse_args().schema)
