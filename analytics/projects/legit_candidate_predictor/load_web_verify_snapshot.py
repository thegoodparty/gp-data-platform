"""Load the web-verification verdict snapshot into the model_predictions schema.

The snapshot is immutable once landed: the table name carries its date, and a later
verification round becomes a new dated table rather than an edit to this one. The script
is nonetheless safe to re-run — it recreates the table from the parquet each time, and
refuses to finish if the landed row count does not match the parquet.

Run: cd analytics && uv run --with pyarrow python \
       projects/legit_candidate_predictor/load_web_verify_snapshot.py [--schema private_tristan]
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
from databricks import sql

HERE = Path(__file__).resolve().parent
SNAPSHOT = HERE / "datasets" / "web_verify_verdicts_snapshot.parquet"
CATALOG = "goodparty_data_catalog"
TABLE = "web_verify_verdicts_20260813"

DDL = """
create or replace table {fqn} (
    verify_id string comment 'Batch bookkeeping id; V pilot, S stratified, F sweep. Mop-up rows keep their original S id.',
    campaign_id bigint comment 'gp_api campaign under verification. Unique within the snapshot.',
    user_id bigint comment 'gp_api user who owns the campaign. Not unique: one person can hold two campaigns.',
    wave string comment 'Which verification run produced the verdict: pilot, stratified, sweep, mopup.',
    method_version string comment 'Protocol version and wave. One protocol (web_verify_v1) across all four waves.',
    verdict string comment 'CONFIRMED, LIKELY, CONTRADICTED, NOT_FOUND or NOT_FOUND_BUDGET.',
    evidence_url string comment 'Source backing the verdict. Null in the pilot/stratified waves, which recorded no url, and null for not-found verdicts.',
    source_type string comment 'Kind of evidence: official_filing, certified_results, ballot, ballotpedia, news, campaign_site, other.',
    notes string comment 'Verifier narrative. Sweep and mop-up waves only.',
    adjudication_note string comment 'Set only where a verdict or url was changed after the run, with the reason. Null means the verifier''s call stands as recorded.',
    verified_at date comment 'Day the wave was completed.',
    oof_hgb double comment 'Out-of-fold model score the row was sampled on. Sampling metadata, never a feature.',
    is_cleanlab_flagged boolean comment 'Row was flagged as a probable label error. Sampling metadata.',
    is_high_confidence_flag boolean comment 'Row was in the high-confidence flag population. Sampling metadata.',
    score_bin string comment 'Score bin for the stratified frame only; null where the wave was not bin-sampled.',
    bin_population bigint comment 'Population of the row''s score bin. Stratified frame only.'
)
comment 'Web verification of uncorroborated Win signups against official election sources, as of 2026-08-13. Label evidence, not model input: every verdict was established by looking up what happened at or after the election. The four waves are separate sampling frames and must not be pooled into a rate — condition on wave / oof_hgb / score_bin. Grain is the campaign, so a person holding two campaigns appears twice.'
"""

COLUMNS = [
    "verify_id",
    "campaign_id",
    "user_id",
    "wave",
    "method_version",
    "verdict",
    "evidence_url",
    "source_type",
    "notes",
    "adjudication_note",
    "verified_at",
    "oof_hgb",
    "is_cleanlab_flagged",
    "is_high_confidence_flag",
    "score_bin",
    "bin_population",
]


def main(schema: str) -> None:
    fqn = f"{CATALOG}.{schema}.{TABLE}"
    snap = pd.read_parquet(SNAPSHOT)
    assert list(snap.columns) == COLUMNS, "snapshot columns drifted from the DDL"
    print(f"loading {len(snap)} rows -> {fqn}")

    # The connector cannot infer types from numpy/pandas scalars, so cast each cell to a
    # Python native and send dates as ISO strings.
    int_cols = {"campaign_id", "user_id", "bin_population"}
    bool_cols = {"is_cleanlab_flagged", "is_high_confidence_flag"}

    def cell(col: str, v: object) -> object:
        if v is None or pd.isna(v):
            return None
        if col in int_cols:
            return int(v)
        if col in bool_cols:
            return bool(v)
        if col == "oof_hgb":
            return float(v)
        return str(v)

    rows = [
        tuple(cell(k, v) for k, v in zip(COLUMNS, rec, strict=False))
        for rec in snap.itertuples(index=False, name=None)
    ]
    placeholders = "(" + ", ".join(["?"] * len(COLUMNS)) + ")"

    with (
        sql.connect(
            server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_API_KEY"],
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(DDL.format(fqn=fqn))
        for i in range(0, len(rows), 100):
            batch = rows[i : i + 100]
            cur.execute(
                f"insert into {fqn} ({', '.join(COLUMNS)}) values " + ", ".join([placeholders] * len(batch)),
                [v for row in batch for v in row],
            )
            print(f"  inserted {min(i + 100, len(rows))}/{len(rows)}")

        cur.execute(
            f"select count(*) n, count(distinct campaign_id) n_campaigns, "
            f"count(distinct verify_id) n_verify from {fqn}"
        )
        n, n_campaigns, n_verify = cur.fetchone()
        print(f"\nlanded: {n} rows, {n_campaigns} campaigns, {n_verify} verify_ids")
        assert n == len(snap), f"landed {n} rows, parquet has {len(snap)} — do not ship"
        assert n_campaigns == n, "campaign_id is not unique in the landed table"
        assert n_verify == n, "verify_id is not unique in the landed table"

        cur.execute(f"select verdict, count(*) from {fqn} group by 1 order by 2 desc")
        print("\nverdict counts in the landed table:")
        for verdict, cnt in cur.fetchall():
            print(f"  {verdict:<18} {cnt}")

        cur.execute(f"select wave, count(*) from {fqn} group by 1 order by 2 desc")
        print("\nwave counts in the landed table:")
        for wave, cnt in cur.fetchall():
            print(f"  {wave:<12} {cnt}")

    print("\nOK — landed row count matches the parquet.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default="model_predictions")
    main(ap.parse_args().schema)
