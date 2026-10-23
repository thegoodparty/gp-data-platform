"""Area 4: does 2026 resemble 2025, and is 2025 itself one population?

The model trains on 2025 and scores 2026, so two things have to hold. The feature distributions must
be comparable across the boundary, and the training period must be internally coherent. 2024 was
already tested and excluded (it degraded 2026 out-of-time), and the product has shifted repeatedly
inside the window: election_level stopped being written around 2025-11, campaign_party was dropped
from the flow in 2025, the onboarding flow changed in 2026, P2V matching was removed in 2026-04.

Five tests, cheapest first:

1. Label base rate by cohort. If the positive rate moves, the score's calibration moves with it.
2. Fill rate per feature by quarter. This is the coverage concern stated numerically.
3. Adversarial validation, 2025 vs 2026. Train a classifier to tell the eras apart using only the
   pass-B features. AUC near 0.5 means the eras are interchangeable; near 1.0 means a model cannot
   transfer, and the features that separate them are the ones doing the damage.
4. The same test inside 2025 (H1 vs H2), to ask whether the training period is one population.
5. Rolling-origin evaluation. Train on everything up to quarter Q, test on Q+1. Real forward
   performance decays differently from cross-validated performance.

Population note: the working set keeps only COMPLETED elections, so in-cycle 2026 signups are absent
from it. For drift that is exactly the wrong filter, because in-cycle 2026 IS the scoring set. This
script therefore re-pulls with the completed-election gate removed and marks rows as label-known or
not, so feature drift is measured on the real scoring population while performance tests stay on the
labelled subset.

Run: uv run --with scikit-learn --with pyarrow python projects/legit_candidate_predictor/era_drift.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import StratifiedGroupKFold

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "lib"))
from build_working_set import SQL as WS_SQL  # noqa: E402
from databricks_conn import run_query  # noqa: E402
from mvp_model import CAT_COLS, NUM_COLS, engineer, make_hgb_frame  # noqa: E402

GATE = "  and c.general_election_date < current_date()"
assert GATE in WS_SQL, "working-set SQL changed; re-check the completed-election gate"
DRIFT_SQL = WS_SQL.replace(GATE, "").format(start="2025-01-01", end="2050-01-01")

HGB = dict(
    categorical_features="from_dtype",
    random_state=7,
    max_iter=300,
    early_stopping=True,
    validation_fraction=0.15,
)
REPORT = HERE / "era_drift_report.md"

# Columns that separate any two time periods by construction, so they inflate the adversarial test
# without indicating a population change.
DATE_MECHANICAL = [
    "days_signup_to_election",
    "days_signup_to_deadline",
    "signed_up_after_deadline",
    "has_deadline",
    "n_br_by_deadline",
    "any_br_by_deadline",
    "br_arrivals_missing",
]
# geo_features.parquet was built for working-set users only, so in-cycle rows are NaN for pipeline
# reasons rather than product ones. Excluded from drift claims to avoid reporting an artefact.
GEO_ARTEFACT = [
    "zip_campaign_state_match",
    "geo_state_match",
    "geo_zip_state_match",
    "geo_country_nonus",
    "geo_missing",
]


def cohort(d: pd.Series) -> pd.Series:
    q = pd.PeriodIndex(pd.to_datetime(d), freq="Q")
    return pd.Series(q.astype(str), index=d.index)


def adversarial(x: pd.DataFrame, is_late: pd.Series, groups: pd.Series, label: str) -> tuple:
    """Can a model tell the two eras apart from the features alone? Grouped CV, so no user leaks."""
    y = is_late.astype(int)
    if y.nunique() < 2 or min(y.sum(), len(y) - y.sum()) < 50:
        return np.nan, pd.Series(dtype=float)
    cv = StratifiedGroupKFold(n_splits=4, shuffle=True, random_state=7)
    oof = np.zeros(len(x))
    for tr, va in cv.split(x, y, groups):
        m = HistGradientBoostingClassifier(**HGB).fit(x.iloc[tr], y.iloc[tr])
        oof[va] = m.predict_proba(x.iloc[va])[:, 1]
    auc = roc_auc_score(y, oof)

    # Which columns carry the separation: single-feature AUC, so the answer is readable.
    per_col = {}
    for c in x.columns:
        s = x[c]
        v = pd.to_numeric(s, errors="coerce") if not isinstance(s.dtype, pd.CategoricalDtype) else None
        if v is None or v.notna().sum() < 100 or v.nunique() < 2:
            # categorical or degenerate: use missingness as the signal
            v = s.isna().astype(float)
            if v.nunique() < 2:
                continue
        try:
            per_col[c] = roc_auc_score(y, v.fillna(v.median() if v.notna().any() else 0))
        except ValueError:
            continue
    sep = pd.Series(per_col).sub(0.5).abs().add(0.5).sort_values(ascending=False)
    print(f"\n=== adversarial: {label} ===")
    print(f"era-separation AUC: {auc:.4f}   (0.50 = interchangeable, 1.00 = fully separable)")
    print("most era-specific columns (single-column AUC, distance from 0.5):")
    print(sep.head(10).to_string())
    return auc, sep


if __name__ == "__main__":
    df = run_query(DRIFT_SQL)
    df["signup_date"] = pd.to_datetime(df["signup_date"])
    df["cohort"] = cohort(df["signup_date"])
    df["election_year"] = pd.to_datetime(df["general_election_date"]).dt.year
    df["label_known"] = pd.to_datetime(df["general_election_date"]) < pd.Timestamp.today()
    df = df[df["signup_date"] >= "2025-01-01"].copy()
    print(
        f"rows 2025+: {len(df):,}   label known: {df.label_known.sum():,}   "
        f"in-cycle (no label yet): {(~df.label_known).sum():,}"
    )

    lines = [
        "# Era drift: does 2026 resemble 2025?",
        "",
        f"Generated from `era_drift.py`. Rows from 2025-01-01: {len(df):,} "
        f"({df.label_known.sum():,} with a completed election, "
        f"{(~df.label_known).sum():,} in-cycle).",
        "",
    ]

    # 1. Label base rate by cohort (labelled rows only).
    lab = df[df.label_known]
    base = (
        lab.groupby("cohort").agg(n=("corroborated", "size"), positive_rate=("corroborated", "mean")).round(3)
    )
    print("\n=== 1. label base rate by signup quarter (completed elections only) ===")
    print(base.to_string())
    lines += [
        "## 1. Label base rate by signup quarter",
        "",
        "Only rows whose election has happened.",
        "",
        "```",
        base.to_string(),
        "```",
        "",
    ]

    # 2. Fill rate per feature by cohort, on ALL rows including in-cycle.
    eng_all = engineer(df)
    feat = CAT_COLS + NUM_COLS
    fill = (
        eng_all.assign(cohort=df["cohort"].values)
        .groupby("cohort")[feat]
        .apply(lambda g: g.notna().mean().round(3))
    )
    # engineer() fills some categoricals with the literal 'missing', so count that too.
    miss_token = (
        eng_all.assign(cohort=df["cohort"].values)
        .groupby("cohort")[CAT_COLS]
        .apply(lambda g: (g.astype(str) == "missing").mean().round(3))
    )
    print("\n=== 2. feature fill rate by signup quarter (all rows, incl. in-cycle) ===")
    print(fill.to_string())
    print("\nshare of categorical features sitting at the literal 'missing' token:")
    print(miss_token.to_string())
    lines += [
        "## 2. Feature fill rate by signup quarter",
        "",
        "All rows including in-cycle, because in-cycle 2026 is the real scoring set.",
        "",
        "```",
        fill.to_string(),
        "```",
        "",
        "### Categoricals sitting at the literal `missing` token",
        "",
        "```",
        miss_token.to_string(),
        "```",
        "",
    ]

    # 3 + 4. Adversarial validation.
    x_all = make_hgb_frame(eng_all)
    groups = df["user_id"]
    is_2026 = df["signup_date"].dt.year >= 2026
    auc_2026, sep_2026 = adversarial(x_all, is_2026, groups, "2025 vs 2026 signups")

    in_2025 = df["signup_date"].dt.year == 2025
    auc_h, sep_h = adversarial(
        x_all[in_2025.to_numpy()],
        df.loc[in_2025, "signup_date"].dt.month.ge(7),
        groups[in_2025],
        "2025 H1 vs H2",
    )

    # The raw number is inflated by calendar-mechanical columns. The tell is that a within-year
    # split scores the same, which cannot be a bigger population change than a cross-year one.
    substantive = [c for c in x_all.columns if c not in DATE_MECHANICAL + GEO_ARTEFACT]
    auc_sub, _ = adversarial(x_all[substantive], is_2026, groups, "2025 vs 2026, substantive columns only")
    auc_h_sub, _ = adversarial(
        x_all.loc[in_2025.to_numpy(), substantive],
        df.loc[in_2025, "signup_date"].dt.month.ge(7),
        groups[in_2025],
        "2025 H1 vs H2, substantive columns only",
    )

    lines += [
        "## 3. Adversarial validation: 2025 vs 2026",
        "",
        f"All features **{auc_2026:.4f}**, substantive columns only **{auc_sub:.4f}**. "
        f"Within-2025 control: all features {auc_h:.4f}, substantive {auc_h_sub:.4f}. "
        "The within-year control scoring nearly as high is the key reading: continuous drift "
        "is this dataset's normal state, not a 2026-specific break.",
        "",
        f"Era-separation AUC **{auc_2026:.4f}** (0.50 = interchangeable, 1.00 = fully "
        "separable). A high value means a model trained on one era is extrapolating in the "
        "other.",
        "",
        "```",
        sep_2026.head(15).to_string(),
        "```",
        "",
        "## 4. Adversarial validation inside 2025 (H1 vs H2)",
        "",
        f"Era-separation AUC **{auc_h:.4f}**. This asks whether the training period is one "
        "population or two.",
        "",
        "```",
        sep_h.head(15).to_string(),
        "```",
        "",
    ]

    # 6. Label maturity. Corroboration lands ~40 days after a filing deadline, so a signup whose
    # election is imminent has a more complete label than one whose election is a year out. Splitting
    # signup quarter by election year separates "different candidates" from "less mature label".
    mat = lab.pivot_table(
        index="cohort", columns="election_year", values="corroborated", aggfunc=["size", "mean"]
    ).round(3)
    lead = (
        lab.assign(lead_days=(pd.to_datetime(lab["general_election_date"]) - lab["signup_date"]).dt.days)
        .groupby("cohort")["lead_days"]
        .describe()[["count", "25%", "50%", "75%"]]
        .round(0)
    )
    completion = df.groupby("cohort")["label_known"].agg(n="size", share_labelled="mean").round(3)
    print("\n=== 6. base rate by signup quarter x election year (completed only) ===")
    print(mat.to_string())
    print("\nlead time from signup to election, by signup quarter:")
    print(lead.to_string())
    print("\nshare of each signup quarter that is labellable at all:")
    print(completion.to_string())
    lines += [
        "## 6. Label maturity and the selection effect",
        "",
        "Base rate by signup quarter and election year, then lead time, then how much of each "
        "quarter can be labelled at all.",
        "",
        "```",
        mat.to_string(),
        "```",
        "",
        "```",
        lead.to_string(),
        "```",
        "",
        "```",
        completion.to_string(),
        "```",
        "",
    ]

    # 5. Rolling origin on labelled rows: train on everything before quarter Q, test on Q.
    print("\n=== 5. rolling-origin performance (train < Q, test = Q) ===")
    eng_lab = engineer(lab)
    x_lab = make_hgb_frame(eng_lab)
    y_lab = eng_lab["corroborated"].astype(int)
    coh = lab["cohort"].to_numpy()
    rows = []
    for q in sorted(pd.unique(coh))[1:]:
        tr, te = coh < q, coh == q
        if te.sum() < 100 or tr.sum() < 500 or y_lab[te].nunique() < 2:
            rows.append(
                {
                    "test_quarter": q,
                    "n_train": int(tr.sum()),
                    "n_test": int(te.sum()),
                    "roc_auc": np.nan,
                    "positive_rate": round(float(y_lab[te].mean()), 3),
                }
            )
            continue
        m = HistGradientBoostingClassifier(**HGB).fit(x_lab[tr], y_lab[tr])
        p = m.predict_proba(x_lab[te])[:, 1]
        rows.append(
            {
                "test_quarter": q,
                "n_train": int(tr.sum()),
                "n_test": int(te.sum()),
                "roc_auc": round(float(roc_auc_score(y_lab[te], p)), 4),
                "positive_rate": round(float(y_lab[te].mean()), 3),
            }
        )
    roll = pd.DataFrame(rows)
    print(roll.to_string(index=False))
    lines += [
        "## 5. Rolling-origin performance",
        "",
        "Train on every earlier quarter, test on the next one. This is what forward scoring "
        "actually looks like, as opposed to cross-validation within one pooled period.",
        "",
        "```",
        roll.to_string(index=False),
        "```",
        "",
    ]

    REPORT.write_text("\n".join(lines) + "\n")
    print(f"\nwrote -> {REPORT.relative_to(HERE)}")
