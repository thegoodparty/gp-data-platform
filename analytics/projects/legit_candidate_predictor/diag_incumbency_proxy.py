"""Is person-level incumbency a real signal, or a BallotReady coverage proxy? (86ak0je29)

The open question from the DATA-2312 rebuild was why incumbency appeared to quadruple. That
turned out to be a dropped `--exclude` (see SESSION_NOTES 2026-08-14), so the family is back
to +0.0036. The underlying worry still deserves an answer: incumbency sources
`ballotready_s3_office_holders_v3`, and matchability to a BallotReady feed correlates with
being in BR's universe at all, while `br_candidacy_id` is one of the three ingredients of the
label. That is the coverage-proxy pattern the no-proxy floor exists to catch.

The decisive test splits the label by which source corroborated the row. `corroborated` is an
OR over br_candidacy_id / ts_source_candidate_id / ddhq_candidate_id in
mart_civics.candidacy_stage. If incumbency only predicts BR-sourced corroboration and goes
flat on TS/DDHQ-only corroboration, matchability is the mechanism and the family is a proxy.
If it predicts both, it describes the person and can be banked.

The label used by the model never changes. This is a diagnostic split only.

Run:
  uv run --with scikit-learn --with pyarrow python \
      projects/legit_candidate_predictor/diag_incumbency_proxy.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "lib"))
import harness  # noqa: E402
from databricks_conn import run_query  # noqa: E402
from mvp_model import FAMILY_SPECS  # noqa: E402

SQL = """
select gp_candidacy_id,
       max(case when br_candidacy_id        is not null then 1 else 0 end) as corrob_br,
       max(case when ts_source_candidate_id is not null then 1 else 0 end) as corrob_ts,
       max(case when ddhq_candidate_id      is not null then 1 else 0 end) as corrob_ddhq
from goodparty_data_catalog.mart_civics.candidacy_stage
group by gp_candidacy_id
"""

CACHE = HERE / "datasets" / "corroboration_sources.parquet"


def sources() -> pd.DataFrame:
    if CACHE.exists():
        return pd.read_parquet(CACHE)
    df = run_query(SQL)
    df.to_parquet(CACHE, index=False)
    return df


def auc(y: pd.Series, s: pd.Series) -> float:
    ok = s.notna()
    if y[ok].nunique() < 2 or ok.sum() < 100:
        return float("nan")
    return float(roc_auc_score(y[ok], s[ok]))


def main() -> None:
    ws = pd.read_parquet(harness.WORKING_SET)
    src = sources()
    df = ws.merge(src, on="gp_candidacy_id", how="left", validate="many_to_one")
    for c in ("corrob_br", "corrob_ts", "corrob_ddhq"):
        df[c] = df[c].fillna(0).astype(int)

    # Sanity: the OR of the three must reproduce the label the model trains on.
    rebuilt = df[["corrob_br", "corrob_ts", "corrob_ddhq"]].max(axis=1)
    agree = (rebuilt == df["corroborated"]).mean()
    print(f"label reproduced from the three source flags: {agree:.4%} of rows")
    assert agree > 0.999, "source split does not reproduce the label; the join drifted"

    spec = FAMILY_SPECS["incumbency"]
    fam = pd.read_parquet(HERE / "datasets" / spec.file)
    cols = [c for c in fam.columns if c != spec.key and c not in spec.exclude]
    df = df.merge(fam[[spec.key, *cols]], on=spec.key, how="left")

    print("\n=== how the label is sourced ===")
    n = len(df)
    pos = df["corroborated"].eq(1)
    print(f"rows {n:,}  corroborated {pos.sum():,} ({pos.mean():.1%})")
    for c in ("corrob_br", "corrob_ts", "corrob_ddhq"):
        print(f"  {c}: {df[c].sum():,} ({df[c].mean():.1%} of all rows)")
    br_only = pos & df.corrob_br.eq(1) & df.corrob_ts.eq(0) & df.corrob_ddhq.eq(0)
    no_br = pos & df.corrob_br.eq(0)
    print(f"  BR-only positives:  {br_only.sum():,}")
    print(f"  positives with NO BR ingredient: {no_br.sum():,}")

    # --- Test 1: does incumbency track BR presence itself, label aside? ---
    print("\n=== test 1: incumbency vs BR-universe membership (label-free) ===")
    print("If the family is a coverage proxy it should predict 'is this row in BR at all'.")
    for c in ("inc_is_officeholder_at_signup", "inc_serving_at_signup"):
        rate_in = df.loc[df[c].eq(1), "corrob_br"].mean()
        rate_out = df.loc[df[c].eq(0), "corrob_br"].mean()
        print(f"  P(BR-sourced | {c}=1) = {rate_in:.1%}   vs =0: {rate_out:.1%}")

    # --- Test 2: the decisive one. Same feature, two label definitions. ---
    # Restrict to rows where the non-BR channels could even fire, so the comparison is
    # about the feature and not about which rows the channel covers.
    print("\n=== test 2: incumbency's discrimination by label source ===")
    single = pd.Series(np.where(df.inc_is_officeholder_at_signup.eq(1), 1.0, 0.0), index=df.index)
    tenure = pd.to_numeric(df["inc_years_in_office_at_signup"], errors="coerce")

    variants = {
        "full label (any source)": df["corroborated"].astype(int),
        "BR-sourced only": df["corrob_br"].astype(int),
        "TS or DDHQ (BR ignored)": df[["corrob_ts", "corrob_ddhq"]].max(axis=1).astype(int),
    }
    rows = []
    for label, yv in variants.items():
        rows.append(
            {
                "label": label,
                "positives": int(yv.sum()),
                "rate": round(float(yv.mean()), 4),
                "auc_officeholder_flag": round(auc(yv, single), 4),
                "auc_years_in_office": round(auc(yv, tenure), 4),
            }
        )
    print(pd.DataFrame(rows).to_string(index=False))

    # --- Test 3: within the non-BR-corroborated world, does incumbency still separate? ---
    # Drop every row whose corroboration came through BR. What is left is a population where
    # the label cannot have been written by the same feed the feature was matched against.
    print("\n=== test 3: BR-corroborated rows removed entirely ===")
    keep = df.corrob_br.eq(0)
    sub = df[keep]
    ysub = sub[["corrob_ts", "corrob_ddhq"]].max(axis=1).astype(int)
    print(f"rows kept {len(sub):,}  positive rate {ysub.mean():.1%}")
    print(
        f"  P(corroborated | officeholder=1) = "
        f"{ysub[sub.inc_is_officeholder_at_signup.eq(1)].mean():.1%}  "
        f"vs =0: {ysub[sub.inc_is_officeholder_at_signup.eq(0)].mean():.1%}"
    )
    print(f"  AUC officeholder flag: {auc(ysub, sub.inc_is_officeholder_at_signup.astype(float)):.4f}")
    print(
        f"  AUC years in office:   "
        f"{auc(ysub, pd.to_numeric(sub['inc_years_in_office_at_signup'], errors='coerce')):.4f}"
    )

    # --- Test 4: the control that makes test 3 readable. ---
    # Dropping BR-corroborated rows leaves a harder, lower-base-rate population, so ANY
    # feature attenuates there. The question is whether incumbency attenuates MORE than
    # features that have no BallotReady matchability story at all. If it attenuates like
    # them, the drop is population difficulty, not a coverage proxy.
    print("\n=== test 4: control features, same two populations ===")
    det = pd.read_parquet(HERE / "datasets" / "gp_user_details_features.parquet")
    df = df.merge(
        det[["user_id", "det_n_selfreport_keys", "det_has_running_against"]], on="user_id", how="left"
    )
    df["days_signup_to_election"] = (
        pd.to_datetime(df["general_election_date"]) - pd.to_datetime(df["signup_date"])
    ).dt.days
    sub = df[df.corrob_br.eq(0)]
    ysub = sub[["corrob_ts", "corrob_ddhq"]].max(axis=1).astype(int)
    yall = df["corroborated"].astype(int)

    controls = {
        "inc_is_officeholder_at_signup": "incumbency (under test)",
        "inc_years_in_office_at_signup": "incumbency (under test)",
        "det_n_selfreport_keys": "control: self-report effort, no BR match path",
        "det_has_running_against": "control: self-report effort, no BR match path",
        "days_signup_to_election": "control: timing, no BR match path",
    }
    rows = []
    for c, what in controls.items():
        full = auc(yall, pd.to_numeric(df[c], errors="coerce"))
        nobr = auc(ysub, pd.to_numeric(sub[c], errors="coerce"))
        # Signal above chance, and how much of it survives dropping the BR-corroborated rows.
        lift_full, lift_nobr = abs(full - 0.5), abs(nobr - 0.5)
        rows.append(
            {
                "column": c,
                "what": what,
                "auc_full": round(full, 4),
                "auc_no_br_rows": round(nobr, 4),
                "survives": f"{lift_nobr / lift_full:.0%}" if lift_full > 0.005 else "n/a",
            }
        )
    print(pd.DataFrame(rows).to_string(index=False))


if __name__ == "__main__":
    main()
