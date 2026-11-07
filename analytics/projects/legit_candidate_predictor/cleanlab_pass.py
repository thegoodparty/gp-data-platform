"""Confident-learning pass (cleanlab) over the DATA-2225 HGB OOF predictions.

Label = corroboration, not ground-truth legitimacy. corroborated=1 is
trustworthy; corroborated=0 mixes noise signups with real candidates that
BallotReady under-covers. Only label=0 issues are meaningful findings.

Writes datasets/cleanlab_mislabeled_negatives.parquet. Read-only otherwise.
"""

from pathlib import Path

import numpy as np
import pandas as pd
from cleanlab.filter import find_label_issues

PROJ = Path(__file__).resolve().parent
DS = PROJ / "datasets"

FREE_DOMAINS = {
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "aol.com",
    "outlook.com",
    "icloud.com",
    "msn.com",
    "live.com",
    "comcast.net",
    "ymail.com",
    "protonmail.com",
    "proton.me",
    "att.net",
    "sbcglobal.net",
    "me.com",
    "mac.com",
    "verizon.net",
    "bellsouth.net",
    "cox.net",
    "charter.net",
    "earthlink.net",
    "mail.com",
    "gmx.com",
    "zoho.com",
    "yandex.com",
    "rocketmail.com",
    "hotmail.co.uk",
    "yahoo.co.uk",
    "googlemail.com",
}


def email_domain_class(domain: object) -> str:
    if not isinstance(domain, str) or not domain.strip():
        return "missing"
    d = domain.strip().lower()
    if d == "goodparty.org":
        return "goodparty.org"
    if d in FREE_DOMAINS:
        return "free"
    return "custom"


def main() -> None:
    oof = pd.read_parquet(DS / "oof_sentinel_fix.parquet")
    ws = pd.read_parquet(
        DS / "working_set.parquet",
        columns=[
            "campaign_id",
            "campaign_state",
            "election_level",
            "br_normalized_position_type",
            "email_domain",
            "filing_deadline",
            "n_br_candidacies_total",
        ],
    )

    # --- Sanity: count stability -------------------------------------------
    n = len(oof)
    pos_rate = oof["corroborated"].mean()
    print(f"n = {n} (expected 20480)  positive rate = {pos_rate:.4f} (expected 0.5937)")
    assert n == 20480, f"unexpected n={n}"
    assert abs(pos_rate - 0.5937) < 0.0005, f"unexpected positive rate {pos_rate}"
    assert oof["campaign_id"].is_unique and ws["campaign_id"].is_unique

    df = oof.merge(ws, on="campaign_id", how="left", validate="1:1")
    assert len(df) == n

    labels = df["corroborated"].astype(int).to_numpy()
    p = df["oof_hgb"].to_numpy(dtype=float)
    pred_probs = np.column_stack([1.0 - p, p])

    # --- 1a. cleanlab find_label_issues on the full pool --------------------
    issues = find_label_issues(labels=labels, pred_probs=pred_probs, filter_by="prune_by_noise_rate")
    df["cleanlab_issue"] = issues

    n_issue_0 = int((issues & (labels == 0)).sum())
    n_issue_1 = int((issues & (labels == 1)).sum())
    print(
        f"cleanlab issues: label=0 flagged = {n_issue_0}; "
        f"label=1 flagged = {n_issue_1} (sanity line only; label=1 is trustworthy)"
    )

    # --- 1b. High-confidence list via 90%-precision threshold ---------------
    # Smallest oof_hgb threshold t such that precision on label=1 among rows
    # with oof_hgb >= t is >= 0.90, computed on the full OOF pool.
    order = np.argsort(-p)
    sorted_labels = labels[order]
    sorted_p = p[order]
    cum_tp = np.cumsum(sorted_labels)
    cum_n = np.arange(1, n + 1)
    precision = cum_tp / cum_n
    ok = np.where(precision >= 0.90)[0]
    assert ok.size, "no threshold reaches 90% precision"
    k = ok.max()  # deepest cut still at >=90% precision
    thr = sorted_p[k]
    print(
        f"90%-precision threshold on oof_hgb: {thr:.4f} " f"(top {k + 1} rows, precision {precision[k]:.4f})"
    )
    df["high_conf"] = (df["oof_hgb"] >= thr) & (df["corroborated"] == 0)

    neg = df[df["corroborated"] == 0].copy()
    n_neg = len(neg)
    n_hc = int(neg["high_conf"].sum())
    n_cl = int(neg["cleanlab_issue"].sum())
    n_both = int((neg["cleanlab_issue"] & neg["high_conf"]).sum())
    n_union = int((neg["cleanlab_issue"] | neg["high_conf"]).sum())
    print(f"\nlabel=0 pool: {n_neg}")
    print(f"  cleanlab_issue: {n_cl}")
    print(f"  high_conf (oof_hgb >= {thr:.4f}): {n_hc}")
    print(f"  overlap (both): {n_both}   union: {n_union}")

    # --- 2. Save output parquet --------------------------------------------
    out = neg.assign(
        filing_deadline_null=neg["filing_deadline"].isna(),
        n_br_candidacies_total_null=neg["n_br_candidacies_total"].isna(),
    )[
        [
            "campaign_id",
            "user_id",
            "oof_hgb",
            "cleanlab_issue",
            "high_conf",
            "campaign_state",
            "election_level",
            "br_normalized_position_type",
            "email_domain",
            "filing_deadline_null",
            "n_br_candidacies_total_null",
        ]
    ]
    out_path = DS / "cleanlab_mislabeled_negatives.parquet"
    out.to_parquet(out_path, index=False)
    print(f"\nwrote {out_path} ({len(out)} rows)")

    # --- 3. Characterize flagged vs rest of label=0 -------------------------
    neg["email_class"] = neg["email_domain"].map(email_domain_class)
    neg["no_roster"] = neg["n_br_candidacies_total"].isna()
    neg["no_deadline"] = neg["filing_deadline"].isna()
    neg["no_position"] = neg["br_normalized_position_type"].isna()
    flagged = neg[neg["cleanlab_issue"]]
    rest = neg[~neg["cleanlab_issue"]]

    def dist(col: str, top: int | None = None) -> pd.DataFrame:
        f = flagged[col].fillna("(null)").value_counts(normalize=True)
        r = rest[col].fillna("(null)").value_counts(normalize=True)
        fc = flagged[col].fillna("(null)").value_counts()
        t = pd.DataFrame(
            {
                "flagged_n": fc,
                "flagged_pct": (f * 100).round(1),
                "rest_pct": (r * 100).round(1),
            }
        ).fillna(0)
        t = t.sort_values("flagged_n", ascending=False)
        return t.head(top) if top else t

    pd.set_option("display.width", 120)
    print("\n=== flagged (cleanlab, label=0) vs rest of label=0 ===")
    print("\n-- campaign_state (top 10 by flagged n) --")
    print(dist("campaign_state", top=10))
    print("\n-- election_level --")
    print(dist("election_level"))
    print("\n-- email domain class --")
    print(dist("email_class"))

    print("\n-- BR-coverage flags (share True) --")
    cov = pd.DataFrame(
        {
            "flagged_pct": [flagged[c].mean() * 100 for c in ("no_roster", "no_deadline", "no_position")],
            "rest_pct": [rest[c].mean() * 100 for c in ("no_roster", "no_deadline", "no_position")],
        },
        index=["no_roster", "no_deadline", "no_position"],
    ).round(1)
    print(cov)

    # Explicit overlap with the zero-roster population
    zero_roster_all = int(df["n_br_candidacies_total"].isna().sum())
    zero_roster_neg = int(neg["no_roster"].sum())
    print(
        f"\nzero-roster population: {zero_roster_all} rows overall "
        f"({zero_roster_neg} within label=0; expected ~4496 users)"
    )
    for name, sub in (("cleanlab_issue", flagged), ("high_conf", neg[neg["high_conf"]])):
        inz = int(sub["no_roster"].sum())
        pct_of_flagged = 100 * inz / len(sub) if len(sub) else float("nan")
        pct_of_zero = 100 * inz / zero_roster_neg if zero_roster_neg else float("nan")
        print(
            f"  {name}: {inz}/{len(sub)} flagged are zero-roster "
            f"({pct_of_flagged:.1f}% of flagged; {pct_of_zero:.1f}% of label=0 zero-roster)"
        )
    base = 100 * zero_roster_neg / n_neg
    print(f"  baseline: {base:.1f}% of all label=0 are zero-roster")

    # Same characterization for the high-conf list, briefly
    hc = neg[neg["high_conf"]]
    print("\n-- high_conf set: BR-coverage flag shares --")
    print(pd.Series({c: round(hc[c].mean() * 100, 1) for c in ("no_roster", "no_deadline", "no_position")}))
    print("\n-- high_conf email class --")
    print(hc["email_class"].value_counts())


if __name__ == "__main__":
    main()
