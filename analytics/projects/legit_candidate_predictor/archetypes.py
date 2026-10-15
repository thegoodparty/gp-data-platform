"""DATA-2238 Area 1: anti-pattern archetypes in the uncorroborated pool.

Two halves, because the pool has two kinds of ignorance in it.

Part A, contradiction anatomy: WHY a real-looking person fails a roster check. Built from the
verifier notes, so it only covers rows that have notes, which is the sweep wave (adversarially
selected high-confidence cleanlab flags). It describes that slice and nothing wider. The point is
not the counts, it is the split: a large share of contradictions are cases where the person is real
and OUR RECORD points at the wrong race. That is data-quality debt, not a noise signup, and the two
call for opposite responses.

Part B, pool anti-patterns: a-priori flags evaluated across the whole negative class, with verified
realness attached wherever the design sample supports an estimate. These are the flags that can be
computed for every row including the ones no verifier could ever reach, which is what makes them
usable in intake triage.

Per the 2026-08-06 rule, name-plausibility flags are a-priori assets and are never label-pruned.
Nothing here selects a flag on its observed label rate; the rates are reported, not optimised.

Run (from analytics/):
  uv run --with pyarrow python projects/legit_candidate_predictor/archetypes.py
  uv run --with pyarrow python projects/legit_candidate_predictor/archetypes.py --show-unclassified
  # --ledger pulls in harness, which imports sklearn, and mirrors the row to MLflow
  uv run --with pyarrow --with scikit-learn --with mlflow-skinny python \
      projects/legit_candidate_predictor/archetypes.py --ledger
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

DATASETS = HERE / "datasets"
WS_FILE = DATASETS / "working_set_v2.parquet"
WS_VERSION = "b04cdd5ac4b1"
VERDICTS = DATASETS / "web_verify_verdicts_snapshot.parquet"
NAME_FEATURES = DATASETS / "name_features.parquet"
REAL = ("CONFIRMED", "LIKELY")

# Ordered most-specific first: the first pattern that matches wins, so the generic
# "absent from a complete roster" rule can sit at the end as the catch-all it is.
ARCHETYPE_RULES: list[tuple[str, str]] = [
    ("record_garble", r"garbl|first\s*=\s*last|transpos|concatenat|name variant|misspell|likely a garble"),
    (
        "relative_attribution",
        r"\b(relative|spouse|husband|wife|daughter|son|father|mother|sibling|brother|sister)\b",
    ),
    (
        "filed_then_removed",
        r"disqualif|withdrew|withdrawn|removed from the ballot|dropped out|"
        r"eliminated in the .{0,20}primary",
    ),
    (
        "wrong_office_same_cycle",
        r"ran for .{0,60}?\bnot\b|,\s*not (city council|school board|mayor|the )|"
        r"a different office|different office|instead ran for",
    ),
    # Deliberately narrow. "Zip 28451=Leland NC" is the verifiers' standard preamble for
    # establishing which jurisdiction a zip belongs to, not a finding that the jurisdiction is
    # wrong, and matching on it inflated this class nine-fold. Explicit contrast words only.
    (
        "wrong_jurisdiction",
        r"different (county|state|city|town)|cross-state|another state|\bWISCONSIN\b|" r"\bin a different\b",
    ),
    # The verifiers write "Real <Name> ..." when the candidacy they found belongs to someone
    # else. That is a match failure, not proof our signup ran, so it counts as no candidacy found.
    ("namesake_or_wrong_person", r"\bReal [A-Z]|\bunrelated\b|only nationwide match"),
    (
        "seat_not_up_or_declined",
        r"did not (seek|file|run)|declined|seat wasn.t up|term runs to|"
        r"\b20(19|21|22|23|24)\b cycle|not (up|on the ballot) (this|that) cycle|"
        r"prior cycle|earlier cycle",
    ),
    (
        "sitting_officeholder_not_candidate",
        r"\bsits on\b|sitting\b.{0,40}(council|board|commission|trustee|member)|"
        r"is an? (sitting |current )?(councilman|councilwoman|council member|board member|"
        r"incumbent)\b|resident commenter",
    ),
    (
        "race_not_held_or_write_in",
        r"write-?in|no filed candidates|no .{0,40}race that cycle|race was not held|"
        r"unopposed and cancel|no such race|does not exist|appointed .{0,20}vacancy|"
        r"applied .{0,20}for an appointed",
    ),
    (
        "absent_from_complete_roster",
        r"\bno\b|not among|not on (it|the ballot|that|official|roster)|omit|absent|"
        r"do(es)? not include|in neither|exclude|not listed|not a candidate|"
        r"full .{0,30}roster|complete .{0,30}(roster|list|ballot)",
    ),
]

# Which archetypes mean the person is real and the RECORD is wrong, versus the person being real
# but not a candidate this cycle, versus no sign of a candidacy at all. This is the split that
# decides what to do about each group.
MECHANISM = {
    "record_garble": "our_record_wrong",
    "wrong_office_same_cycle": "our_record_wrong",
    "wrong_jurisdiction": "our_record_wrong",
    "namesake_or_wrong_person": "no_candidacy_found",
    "filed_then_removed": "candidacy_attempted",
    "relative_attribution": "real_person_no_candidacy",
    "seat_not_up_or_declined": "real_person_no_candidacy",
    "sitting_officeholder_not_candidate": "real_person_no_candidacy",
    "race_not_held_or_write_in": "structurally_uncorroborable",
    "absent_from_complete_roster": "no_candidacy_found",
    "unclassified": "unclassified",
}


def classify(note: str) -> str:
    if not isinstance(note, str) or not note.strip():
        return "unclassified"
    for name, pattern in ARCHETYPE_RULES:
        if re.search(pattern, note, flags=re.IGNORECASE):
            return name
    return "unclassified"


def contradiction_anatomy(v: pd.DataFrame, show_unclassified: bool) -> pd.DataFrame:
    c = v[v["verdict"] == "CONTRADICTED"].copy()
    noted = c[c["notes"].notna() & c["notes"].astype(str).str.strip().ne("")].copy()
    print(
        f"CONTRADICTED rows: {len(c)}; with verifier notes: {len(noted)} "
        f"(waves: {sorted(noted['wave'].unique())})"
    )
    print(
        "The notes come from the sweep wave, so this anatomy describes the flagged slice, "
        "not the negative class.\n"
    )

    noted["archetype"] = noted["notes"].map(classify)
    noted["mechanism"] = noted["archetype"].map(MECHANISM)

    t = (
        noted.groupby(["mechanism", "archetype"])
        .size()
        .rename("n")
        .reset_index()
        .sort_values(["mechanism", "n"], ascending=[True, False])
    )
    t["share"] = (t["n"] / len(noted)).round(3)
    print("=== contradiction anatomy (why a real-looking person fails a roster check) ===")
    print(t.to_string(index=False))

    by_mech = noted.groupby("mechanism").size().rename("n")
    print("\n--- rolled up ---")
    print((by_mech / len(noted)).round(3).sort_values(ascending=False).to_string())
    wrong = by_mech.get("our_record_wrong", 0)
    print(
        f"\n{wrong} of {len(noted)} contradictions ({wrong / len(noted):.0%}) are cases where the "
        f"person is real and our record points at the wrong race. Those are ER and intake "
        f"defects, recoverable without any new data source."
    )

    if show_unclassified:
        u = noted[noted["archetype"] == "unclassified"]
        print(f"\n--- {len(u)} unclassified notes (extend the rules, do not widen the catch-all) ---")
        for n in u["notes"]:
            print(f"  | {n}")
    return noted


def pool_anti_patterns(v: pd.DataFrame) -> int:
    ws = pd.read_parquet(WS_FILE)
    names = pd.read_parquet(NAME_FEATURES)
    df = ws.merge(names, on="user_id", how="left", validate="many_to_one")
    assert len(df) == len(ws), "name-feature merge fanned out"

    signup = pd.to_datetime(df["signup_date"])
    deadline = pd.to_datetime(df["filing_deadline"])
    dom = df["email_domain"].fillna("")

    # True anti-patterns: a row carrying one is evidence AGAINST a real candidacy.
    flags = pd.DataFrame(
        {
            "internal_email": dom.eq("goodparty.org"),
            "name_implausible": (
                df[
                    [
                        "name_keyboard_run",
                        "name_placeholder_token",
                        "name_fictional",
                        "name_tripled_char",
                        "name_fn_eq_ln",
                    ]
                ]
                .fillna(0)
                .astype(bool)
                .any(axis=1)
            ),
            "name_both_unknown": df["name_both_unknown"].fillna(0).astype(bool),
        }
    )
    flags["any_anti_pattern"] = flags.any(axis=1)
    # Context flags, reported separately because they are NOT anti-patterns. Signing up after the
    # filing deadline runs the other way entirely (people who have already filed sign up late), and
    # no_br_position is the known coverage hole, a property of BR's roster rather than the signup.
    # Mixing either into an "any flag" bucket swamps the rare true anti-patterns and inverts it.
    flags["ctx_signed_up_after_deadline"] = (signup > deadline).fillna(False)
    flags["ctx_no_br_position"] = df["br_normalized_position_type"].isna() | df[
        "br_normalized_position_type"
    ].astype(str).eq("missing")

    y = df["corroborated"].astype(int)
    neg = y.eq(0)

    rows = []
    for col in flags.columns:
        f = flags[col]
        rows.append(
            {
                "flag": col,
                "n_pool": int(f.sum()),
                "share_of_negatives": float(f[neg].mean()),
                "share_of_positives": float(f[~neg].mean()),
                "corroboration_rate": float(y[f].mean()) if f.any() else float("nan"),
            }
        )
    t = pd.DataFrame(rows)
    t["lift_vs_base"] = (t["corroboration_rate"] / y.mean()).round(2)
    print("\n\n=== a-priori anti-pattern flags across the working set ===")
    print(t.round(4).to_string(index=False))
    print(f"base corroboration rate: {y.mean():.4f}")

    # Verified realness per flag, where the design sample can carry an estimate.
    design = v[v["wave"].isin(("stratified", "mopup")) & ~v["verdict"].eq("NOT_FOUND_BUDGET")].copy()
    design["is_real"] = design["verdict"].isin(REAL)
    keyed = df[["campaign_id"]].join(flags)
    d = design.merge(keyed, on="campaign_id", how="left", validate="one_to_one")

    print("\n=== verified realness by flag (design sample only; n<10 suppressed) ===")
    out = []
    for col in flags.columns:
        g = d[d[col].fillna(False)]
        out.append(
            {
                "flag": col,
                "n_verified": len(g),
                "p_real": round(g["is_real"].mean(), 3) if len(g) >= 10 else None,
            }
        )
    print(pd.DataFrame(out).to_string(index=False))

    # The load-bearing number: how much of the negative class do the true anti-patterns explain?
    neg_anti = int(flags.loc[neg, "any_anti_pattern"].sum())
    print(
        f"\nnegative rows carrying a true anti-pattern: {neg_anti} of {int(neg.sum()):,} "
        f"({neg_anti / neg.sum():.2%})"
    )
    print(
        "The PU estimate puts about 44% of the negative class at real. Anti-patterns cannot "
        "account for the other side of that: spam and gibberish are real archetypes but they are "
        "a rounding error at this scale. The uncorroborated class is overwhelmingly real people, "
        "either miscorded or interested-but-not-running."
    )
    return neg_anti


def main(show_unclassified: bool, ledger: bool) -> None:
    v = pd.read_parquet(VERDICTS)
    noted = contradiction_anatomy(v, show_unclassified)
    anti = pool_anti_patterns(v)

    out = noted[["campaign_id", "user_id", "wave", "verdict", "archetype", "mechanism", "notes"]]
    out.to_parquet(DATASETS / "contradiction_archetypes.parquet", index=False)
    print(f"\nsaved datasets/contradiction_archetypes.parquet ({len(out)} classified rows)")

    if ledger:
        import harness

        share = noted["mechanism"].value_counts(normalize=True)
        harness.append_ledger(
            "contradiction archetypes + pool anti-patterns",
            "other",
            "none",
            {
                "n_classified": int(len(noted)),
                "share_our_record_wrong": round(float(share.get("our_record_wrong", 0)), 4),
                "share_no_candidacy_found": round(float(share.get("no_candidacy_found", 0)), 4),
                "share_real_person_no_candidacy": round(float(share.get("real_person_no_candidacy", 0)), 4),
                "share_unclassified": round(float(share.get("unclassified", 0)), 4),
                "negatives_with_anti_pattern": int(anti),
            },
            notes="DATA-2238. Anatomy is coded from verifier notes, which exist only for the "
            "sweep wave, so the shares describe the flagged slice. The codebook was "
            "developed against this corpus; full coverage is not evidence of generality. "
            "Anti-pattern flag counts are class-wide and are the load-bearing number: "
            "gibberish and internal-email signups cannot account for the negative class.",
            area="area1",
            ver=WS_VERSION,
        )
        print("ledger row appended")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--show-unclassified", action="store_true")
    ap.add_argument("--ledger", action="store_true")
    a = ap.parse_args()
    main(a.show_unclassified, a.ledger)
