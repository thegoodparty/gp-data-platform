"""DATA-2225 MVP steps 3-7: features (pass B), two models, group CV, metrics.

Feature policy (brief 3b): allowlist only, everything computable as-of the filing
deadline anchor. No product-activity, no post-signup candidacy fields, no viability.
"""

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, precision_recall_curve, roc_auc_score
from sklearn.model_selection import StratifiedGroupKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

HERE = Path(__file__).resolve().parent
FREE_DOMAINS = {
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "aol.com",
    "icloud.com",
    "msn.com",
    "comcast.net",
    "live.com",
    "protonmail.com",
    "proton.me",
    "me.com",
    "att.net",
    "sbcglobal.net",
    "verizon.net",
    "ymail.com",
    "mac.com",
    "cox.net",
}

# election_level was dropped at the DATA-2312 rebuild. It is 100% missing for 2026
# signups (details:level stopped being written 2025-11, see DATA-2243), so it acted as
# an era marker rather than a level signal: removing it cost -0.0007 pooled and
# IMPROVED forward. pos_br_level in the BR position family is the era-stable
# replacement, with the full seven-value vocabulary instead of the app's four.
CAT_COLS = [
    "campaign_state",
    "office_type",
    "br_normalized_position_type",
    "campaign_party",
    "email_domain_class",
]
NUM_COLS = [
    "days_signup_to_election",
    "days_signup_to_deadline",
    "signed_up_after_deadline",
    "has_deadline",
    "log_voter_count",
    "voter_count_missing",
    "n_br_by_deadline",
    "any_br_by_deadline",
    "br_arrivals_missing",
    "name_n_tokens",
    "name_len",
    "name_has_digit",
    "name_has_nonalpha",
    "name_single_char_token",
    "zip_campaign_state_match",
    "geo_state_match",
    "geo_zip_state_match",
    "geo_country_nonus",
    "geo_missing",
]

GEO_FILE = Path(__file__).resolve().parent / "datasets" / "geo_features.parquet"


# --- Feature-family registry (composition, 86ak0je29) -------------------------------------
# The accepted column set of every family lives HERE, never in a shell invocation. Until
# 2026-08-14 the exclusion lists were typed as --exclude on the command line, and the
# post-DATA-2312 re-scores silently dropped them: incumbency reported +0.0181 instead of
# its true +0.0036 because the name-commonness leak channel came back unnoticed. A list you
# have to remember is a list that eventually gets forgotten.


@dataclass(frozen=True)
class FamilySpec:
    """One feature family: where it lives, how it joins, and what never enters any model."""

    file: str
    key: str
    exclude: Mapping[str, str] = field(default_factory=dict)  # column -> why it is out


FAMILY_SPECS: dict[str, FamilySpec] = {
    "br_position": FamilySpec(
        file="br_position_features.parquet",
        key="br_position_id",
        # All 32 columns accepted 2026-08-06 (first accepted Area 2 family).
    ),
    "gp_user_details": FamilySpec(
        file="gp_user_details_features.parquet",
        key="user_id",
        exclude={
            "usr_has_password": "auth-path latent (|r|=0.956 with knowRun); entry channel, not behavior",
            "det_know_run_yes": "auth-path latent, same construct as has_password",
            "det_pledged_true": "pure flow-completion (6 false in 60k); reserve-marked for claims",
            "det_pledged_missing": "companion indicator to the pledged latent",
            "det_days_signup_to_product_deadline": "product-side deadline; BR deadline preferred (one concept, one source)",
            "det_product_deadline_missing": "companion indicator to the product-side deadline",
            "det_office_term_length_years": "0.974 twin of pos_election_frequency_years; BR preferred",
            "usr_phone_toll_free_or_premium": "rule candidate, not a model feature",
            "diag_mart_is_pledged": "diagnostic column, never a feature",
        },
    ),
    "incumbency": FamilySpec(
        file="incumbency_features.parquet",
        key="user_id",
        exclude={
            "inc_name_ambiguity": "name-commonness leak into label construction (re-verified 2026-08-14)",
            "inc_match_is_unique": "same leak channel; matchability, not the person",
        },
    ),
    # Substitution basis for the analysis tier, never part of the serving composition:
    # measured a wash on top of the baseline (2026-08-12), which is exactly what makes it
    # a candidate to BACKFILL discrimination when a claim variable is masked out.
    "l2_easy": FamilySpec(file="l2_features.parquet", key="user_id"),
    # Pass A only. Never enters a pass-B composed model or the analysis tier.
    "amplitude_scaffold": FamilySpec(file="amplitude_family_scaffold.parquet", key="user_id"),
}

# The v2 serving composition: every admissible pass-B family.
COMPOSED_V2 = ("br_position", "gp_user_details", "incumbency")

# --- Analysis-tier claim masks (two-model design, 2026-08-07; claim set chosen 2026-08-14) ---
# A model input cannot be evidence about the group the score defines. Each entry names a claim
# someone downstream wants to make and lists every column that must leave the model for that
# claim to say something rather than restate the scoring rule. Masks span baseline and family
# columns alike, which is why the runner masks the assembled frame rather than one source.
CLAIM_MASKS: dict[str, tuple[str, ...]] = {
    # "high scorers are disproportionately sitting officeholders". The BR position family
    # carries its own office-holder block at position grain, so the claim leaks through it
    # unless those three go too.
    "incumbency": (
        "inc_is_officeholder_at_signup",
        "inc_serving_at_signup",
        "inc_holds_same_position",
        "inc_n_offices",
        "inc_years_in_office_at_signup",
        "inc_days_to_term_end",
        "inc_term_ends_within_1y",
        "pos_holder_count",
        "pos_holder_known",
        "pos_holder_any_vacant",
    ),
    # "high scorers concentrate in local / city races". pos_tier is BR's own level ordinal,
    # so it restates the claim directly.
    "office_level": ("office_type", "br_normalized_position_type", "pos_br_level", "pos_tier"),
    # "high scorers run in larger (or smaller) districts". The covariance check (2026-08-14)
    # found two BR columns that carry district size mechanically: signature requirements
    # (r=0.96 with voter_count) and salary (r=0.72). A mask that leaves them in does not
    # protect the claim, so both go, and the extra AUC cost is measured rather than assumed.
    "district_size": (
        "log_voter_count",
        "voter_count_missing",
        "pos_signature_count",
        "pos_signature_count_known",
        "pos_salary_amount",
    ),
    # "high scorers concentrate in these states"
    "geography": ("campaign_state",),
    # The combined mask the 2026-08-14 sales/product report needs: state, office, incumbency
    # and district size all held out at once, so one score supports every cut those teams
    # actually asked for. Costs more than any single mask, which is the price of profiling
    # four things with one model instead of running four.
    # Deliberately NOT masked: the geo-consistency flags (they encode whether zip and state
    # AGREE, not which state), and the BR position attributes that merely correlate with
    # state through filing law. Masking everything correlated would leave nothing to score on;
    # the residual correlation is a caveat for the report, not a leak of the claim variable.
    "sales_profile": (
        "campaign_state",
        "office_type",
        "br_normalized_position_type",
        "pos_br_level",
        "pos_tier",
        "inc_is_officeholder_at_signup",
        "inc_serving_at_signup",
        "inc_holds_same_position",
        "inc_n_offices",
        "inc_years_in_office_at_signup",
        "inc_days_to_term_end",
        "inc_term_ends_within_1y",
        "pos_holder_count",
        "pos_holder_known",
        "pos_holder_any_vacant",
        "log_voter_count",
        "voter_count_missing",
        "pos_signature_count",
        "pos_signature_count_known",
        "pos_salary_amount",
    ),
    # "high scorers put more into their profile"
    "selfreport_effort": (
        "det_n_selfreport_keys",
        "det_has_occupation",
        "det_has_past_experience",
        "det_has_website",
        "det_has_running_against",
        "det_has_campaign_committee",
        "det_has_custom_issues",
        "det_has_fun_fact",
    ),
}

# Baseline training spec since 2026-08-11 (Tristan): exponential recency weights on the
# signup clock, half-life 240d, no hard window cutoff. 240 over the sweep's 120 because
# the OOF yardstick cost is -0.0012 vs -0.0040 and the forward difference is within
# noise (ledger "recency re-base" rows). Weight scale is arbitrary (only ratios matter),
# so subsetting rows after calling this is fine.
RECENCY_HALF_LIFE_DAYS = 240


def recency_weights(df: pd.DataFrame, half_life_days: float = RECENCY_HALF_LIFE_DAYS) -> np.ndarray:
    signup = pd.to_datetime(df["signup_date"])
    age_days = (signup.max() - signup).dt.days.clip(lower=0).to_numpy(dtype=float)
    return np.power(0.5, age_days / half_life_days)


def engineer(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    elec = pd.to_datetime(out["general_election_date"])
    dl = pd.to_datetime(out["filing_deadline"])
    signup = pd.to_datetime(out["signup_date"])

    out["days_signup_to_election"] = (elec - signup).dt.days
    out["has_deadline"] = dl.notna().astype(int)
    out["days_signup_to_deadline"] = (dl - signup).dt.days  # NaN when no deadline
    out["signed_up_after_deadline"] = ((signup > dl).fillna(False)).astype(int)

    dom = out["email_domain"].fillna("")
    out["email_domain_class"] = np.select(
        [dom.eq("goodparty.org"), dom.isin(FREE_DOMAINS), dom.eq("")],
        ["goodparty", "free", "missing"],
        default="custom",
    )

    name = (out["user_first_name"].fillna("") + " " + out["user_last_name"].fillna("")).str.strip()
    out["name_n_tokens"] = name.str.split().str.len().fillna(0)
    out["name_len"] = name.str.len()
    out["name_has_digit"] = name.str.contains(r"\d", regex=True).astype(int)
    out["name_has_nonalpha"] = name.str.contains(r"[^A-Za-z .'\-]", regex=True).astype(int)
    out["name_single_char_token"] = name.str.split().apply(
        lambda toks: int(any(len(t) == 1 for t in toks)) if isinstance(toks, list) else 0
    )

    out["voter_count_missing"] = out["voter_count"].isna().astype(int)
    out["log_voter_count"] = np.log1p(out["voter_count"].astype(float))

    # by_deadline=0 with no known deadline is a sentinel from the SQL (the CASE can
    # never count), not a real zero; a real zero needs a deadline to count against.
    # NaN both that case and no-roster rows so "unknown" never masquerades as 0.
    n_bd = out["n_br_candidacies_by_deadline"].astype(float)
    n_bd = n_bd.mask(dl.isna() & n_bd.eq(0))
    out["n_br_by_deadline"] = n_bd
    out["br_arrivals_missing"] = n_bd.isna().astype(int)
    out["any_br_by_deadline"] = (n_bd > 0).astype(float).mask(n_bd.isna())

    # Geo-consistency features (build_geo.py; signup-tier gated, NaN when unknown).
    geo = pd.read_parquet(GEO_FILE)
    out = out.merge(geo, on="user_id", how="left")
    assert len(out) == len(df), "geo merge fanned out"

    out["cycle_year"] = elec.dt.year
    for c in CAT_COLS:
        out[c] = out[c].fillna("missing").astype(str)
    return out


def make_lr() -> Pipeline:
    pre = ColumnTransformer(
        [
            ("cat", OneHotEncoder(handle_unknown="infrequent_if_exist", min_frequency=30), CAT_COLS),
            (
                "num",
                Pipeline(
                    [
                        ("imp", SimpleImputer(strategy="median", add_indicator=True)),
                        ("sc", StandardScaler()),
                    ]
                ),
                NUM_COLS,
            ),
        ]
    )
    return Pipeline(
        [
            ("pre", pre),
            # elastic-net via l1_ratio alone: penalty="elasticnet" is deprecated in
            # sklearn 1.8 and removed in 1.10
            ("lr", LogisticRegression(solver="saga", l1_ratio=0.5, C=1.0, max_iter=8000)),
        ]
    )


def make_hgb_frame(df: pd.DataFrame) -> pd.DataFrame:
    x = df[CAT_COLS + NUM_COLS].copy()
    for c in CAT_COLS:
        x[c] = x[c].astype("category")
    return x


def frontier(y, p) -> pd.DataFrame:
    prec, rec, _thr = precision_recall_curve(y, p)
    rows = []
    for target_r in (0.95, 0.9, 0.8, 0.7, 0.6, 0.5):
        mask = rec >= target_r
        rows.append({"at": f"recall>={target_r}", "precision": float(prec[mask].max())})
    for target_p in (0.7, 0.8, 0.85, 0.9, 0.95):
        mask = prec >= target_p
        rows.append({"at": f"precision>={target_p}", "recall": float(rec[mask].max()) if mask.any() else 0.0})
    return pd.DataFrame(rows)


def reliability(y, p, bins=10) -> pd.DataFrame:
    q = pd.qcut(p, bins, duplicates="drop")
    return pd.DataFrame(
        {
            "pred_mean": pd.Series(p).groupby(q, observed=True).mean().round(3),
            "obs_rate": pd.Series(y.values).groupby(q, observed=True).mean().round(3),
            "n": pd.Series(y.values).groupby(q, observed=True).size(),
        }
    )


def main() -> None:
    df = engineer(pd.read_parquet(HERE / "datasets" / "working_set.parquet"))
    y = df["corroborated"].astype(int)
    groups = df["user_id"]
    print(f"n={len(df):,}  positive={y.mean():.3f}")

    cv = StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=7)
    oof = {"lr": np.zeros(len(df)), "hgb": np.zeros(len(df))}
    fold_metrics = []
    x_hgb = make_hgb_frame(df)
    x_lr = df[CAT_COLS + NUM_COLS]
    last_fold = None

    for i, (tr, va) in enumerate(cv.split(x_lr, y, groups)):
        lr = make_lr().fit(x_lr.iloc[tr], y.iloc[tr])
        oof["lr"][va] = lr.predict_proba(x_lr.iloc[va])[:, 1]
        hgb = HistGradientBoostingClassifier(
            categorical_features="from_dtype",
            random_state=7,
            max_iter=400,
            early_stopping=True,
            validation_fraction=0.15,
        ).fit(x_hgb.iloc[tr], y.iloc[tr])
        oof["hgb"][va] = hgb.predict_proba(x_hgb.iloc[va])[:, 1]
        fold_metrics.append(
            {
                "fold": i,
                "lr_auc": roc_auc_score(y.iloc[va], oof["lr"][va]),
                "hgb_auc": roc_auc_score(y.iloc[va], oof["hgb"][va]),
            }
        )
        last_fold = (tr, va, hgb)

    fm = pd.DataFrame(fold_metrics)
    print("\n=== per-fold ROC-AUC ===")
    print(fm.to_string(index=False))

    results = {}
    for m in ("lr", "hgb"):
        results[m] = {
            "roc_auc": float(roc_auc_score(y, oof[m])),
            "pr_auc": float(average_precision_score(y, oof[m])),
        }
    print("\n=== pooled OOF ===")
    print(pd.DataFrame(results).T.round(4).to_string())

    winner = max(results, key=lambda m: results[m]["roc_auc"])
    print(f"\nwinner: {winner}")
    p = oof[winner]

    print("\n=== precision-recall frontier (winner, OOF) ===")
    print(frontier(y, p).to_string(index=False))

    print("\n=== reliability (winner, OOF deciles) ===")
    print(reliability(y, p).to_string())

    # Baselines
    print("\n=== baselines ===")
    print(f"majority (all real): precision={y.mean():.3f} recall=1.000")
    heur = ((df.email_domain_class != "goodparty") & (df.signed_up_after_deadline == 0)).astype(int)
    tp = int(((heur == 1) & (y == 1)).sum())
    print(
        f"heuristic (non-GP domain & not after deadline): "
        f"precision={tp / max(heur.sum(), 1):.3f} recall={tp / y.sum():.3f} flagged={heur.mean():.1%}"
    )

    # Slices at a reference threshold: max-F1 on OOF (reference only, no blessed point)
    prec, rec, thr = precision_recall_curve(y, p)
    f1 = 2 * prec * rec / np.clip(prec + rec, 1e-9, None)
    t_star = float(thr[np.argmax(f1[:-1])])
    df["_pred"] = (p >= t_star).astype(int)
    df["_p"] = p

    def slice_table(dim: str, top: int | None = None) -> pd.DataFrame:
        g = (
            df.groupby(dim, observed=True)
            .apply(
                lambda s: pd.Series(
                    {
                        "n": len(s),
                        "pos_rate": s["corroborated"].mean(),
                        "precision": (s["corroborated"][s["_pred"] == 1]).mean()
                        if (s["_pred"] == 1).any()
                        else np.nan,
                        "recall": (s["_pred"][s["corroborated"] == 1]).mean()
                        if (s["corroborated"] == 1).any()
                        else np.nan,
                        "auc": roc_auc_score(s["corroborated"], s["_p"])
                        if s["corroborated"].nunique() == 2 and len(s) >= 30
                        else np.nan,
                    }
                ),
                include_groups=False,
            )
            .sort_values("n", ascending=False)
        )
        return (g.head(top) if top else g).round(3)

    print(f"\n=== slices at reference threshold {t_star:.3f} (max-F1, OOF) ===")
    for dim, top in (("cycle_year", None), ("election_level", None), ("campaign_state", 10)):
        print(f"\n-- {dim} --")
        print(slice_table(dim, top).to_string())

    # Importances: permutation on the last fold's validation split (HGB)
    tr, va, hgb = last_fold
    pi = permutation_importance(
        hgb, x_hgb.iloc[va], y.iloc[va], scoring="roc_auc", n_repeats=5, random_state=7
    )
    imp = pd.Series(pi.importances_mean, index=x_hgb.columns).sort_values(ascending=False)
    print("\n=== HGB permutation importance (ROC-AUC drop, last fold val) ===")
    print(imp.round(4).to_string())

    out = df[["user_id", "campaign_id", "corroborated", "cycle_year"]].copy()
    out["oof_lr"], out["oof_hgb"] = oof["lr"], oof["hgb"]
    out.to_parquet(HERE / "datasets" / "oof_predictions.parquet", index=False)
    (HERE / "results.json").write_text(
        json.dumps(
            {
                "pooled": results,
                "winner": winner,
                "ref_threshold_maxf1": t_star,
                "n": len(df),
                "positive_rate": float(y.mean()),
            },
            indent=2,
        )
    )
    print("\nsaved oof_predictions.parquet + results.json")


if __name__ == "__main__":
    main()
