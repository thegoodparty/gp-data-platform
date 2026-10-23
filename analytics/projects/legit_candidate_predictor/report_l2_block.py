"""DATA-2239 L2/haystaq FULL-BLOCK screen: readable intake report.

Rewrites intake/l2_block_features.md as human-readable markdown — real tables,
percentages formatted as percentages — replacing the generic write_intake() dump for
this family (Tristan, 2026-08-13: the to_string code blocks were unreadable at 791
columns). The decisions half below the marker is preserved verbatim, same convention
as intake.py.

Adds the complete exclusion audit the generic intake lacks:
  - every proxy-screen drop with its exact protected-class posterior
    (P(class | level) vs base), so individual columns can be argued back in;
  - the 0.25-0.30 near-misses that stayed;
  - full rule-exclusion lists (protected, PII, geo, meta) in collapsible sections;
  - kept columns closest to the proxy line.

Rerunnable any time; run AFTER build_l2_block.py (it reads the parquet + screen CSVs +
the cached screening sample for exact posteriors).

Run:  cd analytics && uv run --with pyarrow python \
        projects/legit_candidate_predictor/report_l2_block.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from intake import DECISIONS_MARKER, binary_flags, sentinel_scan  # noqa: E402
from screen_l2_proxy import TARGETS, bin_column  # noqa: E402

OUT = HERE / "intake" / "l2_block_features.md"
DROP_AT = 0.30


def pct(x: float, digits: int = 1) -> str:
    return "—" if pd.isna(x) else f"{100 * x:.{digits}f}%"


def md_table(df: pd.DataFrame) -> list[str]:
    cols = list(df.columns)
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for _, r in df.iterrows():
        lines.append("| " + " | ".join("" if pd.isna(v) else str(v) for v in r) + " |")
    return lines


def details(summary: str, names: list[str]) -> list[str]:
    body = ", ".join(f"`{n}`" for n in names)
    return ["<details><summary>" + summary + "</summary>", "", body, "", "</details>"]


def exact_posteriors(sample: pd.DataFrame, screen: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """For each column's recorded worst (target, class, level), the exact posterior."""
    tgt, base = {}, {}
    for tname, tcol in TARGETS.items():
        t = sample[tcol].astype("object").str.strip().replace("", np.nan)
        vc = t.value_counts(normalize=True)
        t = t.where(t.isin(vc[vc >= 0.01].index))
        tgt[tname] = t
        base[tname] = t.value_counts(normalize=True)

    # match the screen's numeric coercion so deciles reproduce
    df = sample.copy()
    for c in columns:
        if df[c].dtype == object:
            cleaned = df[c].astype(str).str.strip().str.replace(r"[%$,]", "", regex=True)
            conv = pd.to_numeric(cleaned.where(df[c].notna()), errors="coerce")
            orig_n = int(df[c].replace("", np.nan).notna().sum())
            if orig_n and conv.notna().sum() >= 0.98 * orig_n:
                df[c] = conv

    val = screen[screen.kind == "value"]
    rows = []
    for c in columns:
        sub = val[val.column == c]
        if not len(sub):
            continue
        r = sub.loc[sub["shift"].idxmax()]
        t = tgt[r.target]
        lev = bin_column(df[c])[t.notna()]
        tv = t[t.notna()]
        in_level = lev == r.level
        post = float((tv[in_level] == r["class"]).mean()) if in_level.any() else np.nan
        rows.append(
            {
                "column": c.replace("l2b_", ""),
                "screen target": r.target,
                "class": r["class"],
                "worst level": r.level,
                "n at level": f"{int(r.level_n):,}",
                "P(class) base": pct(base[r.target].get(r["class"], np.nan)),
                "P(class \\| level)": pct(post),
                "posterior shift": f"{r['shift']:.2f}",
                "_shift": r["shift"],
            }
        )
    out = pd.DataFrame(rows).sort_values("_shift", ascending=False).drop(columns="_shift")
    return out


if __name__ == "__main__":
    disp = pd.read_csv(HERE / "datasets" / "l2_block_dispositions.csv")
    cand = pd.read_csv(HERE / "datasets" / "l2_block_candidates.csv")
    screen = pd.read_csv(HERE / "datasets" / "l2_proxy_screen.csv")
    full = pd.read_parquet(HERE / "datasets" / "l2_block_features.parquet")
    sample = pd.read_parquet(HERE / "datasets" / "_l2_proxy_sample.parquet")
    ws = pd.read_parquet(HERE / "datasets" / "working_set.parquet")

    kept = cand[~cand.proxy_dropped]
    dropped = cand[cand.proxy_dropped]
    val = screen[screen.kind == "value"]
    strength = val.groupby("column")["shift"].max()

    matched = full[full.l2b_matched == 1]
    ws_users = set(ws.user_id)
    ws_matched = ws.user_id.isin(set(matched.user_id))

    doc: list[str] = []
    doc += [
        "# Intake review: `l2_block_features` (L2/haystaq full block)",
        "",
        "- **Source**: `goodparty_data_catalog.dbt.int__l2_nationwide_uniform_w_haystaq` " "(1,396 columns)",
        "- **Join**: `user_id` → `l2_match_bridge.parquet` (frozen 2026-08-12) → "
        "`LALVOTERID`; the bridge stays the only artifact carrying the voter id",
        f"- **Grain**: one row per `user_id`, unique ({len(full):,} rows, " f"{full.shape[1] - 1} features)",
        "- **Pass class**: B with the current-snapshot caveat (as BR position). "
        "Year-indexed vote history is cell-gated to years strictly before signup; "
        "registration / move / FEC are date-gated exactly as `build_l2_match.py`. "
        "Reserve candidates: the registration / vote-history block (claims).",
        "- **Profile**: `profiles/l2_block_features.md` · **Sample**: "
        "`datasets/l2_block_features_sample.csv` (~100 stratified rows)",
        "- Regenerate this file with `report_l2_block.py` (decisions half preserved).",
        "",
        "## 1. Triage funnel",
        "",
    ]
    n_drop = disp.disposition.value_counts()
    funnel = pd.DataFrame(
        [
            ("catalog columns", 1396, ""),
            (
                "− district / jurisdiction geography",
                -int(n_drop.get("drop_geo", 0)),
                "high-cardinality strings; state is already in the model",
            ),
            (
                "− PII",
                -int(n_drop.get("drop_pii", 0)),
                "names, raw dates, addresses, phones, device ids, lat/lng",
            ),
            (
                "− protected classes (decided 2026-08-13)",
                -int(n_drop.get("drop_protected", 0)),
                "gender / ethnicity / religion / language blocks, both tiers",
            ),
            (
                "− disability (decided at the 2026-08-13 gate)",
                -int(n_drop.get("drop_protected_pending", 0)),
                "`Disabled_In_HH`, `AccessibilityHandicapFlag` — a-priori drop confirmed",
            ),
            ("− identifiers / vendor meta", -int(n_drop.get("drop_id_meta", 0)), ""),
            ("− vendor-reserved, unknown semantics", -int(n_drop.get("drop_unknown", 0)), ""),
            ("− free text", -int(n_drop.get("drop_freetext", 0)), ""),
            ("= mechanical-triage survivors", int((disp.disposition == "keep").sum()), ""),
            ("− dead / constant in the screening sample", -16, "listed in §5"),
            (
                "− high-cardinality strings",
                -2,
                "`Occupation_of_Person` (252 levels), moved-voter perf variant (79)",
            ),
            ("− protected-class proxies at shift ≥ 0.30", -len(dropped), "full audit in §2"),
            ("= candidates pulled", len(cand) - len(dropped), ""),
            (
                "− all-NaN after PIT gating",
                -4,
                "`House_Plants`, `Primary_2026`, `OtherElection_2026`, `PRI_BLT_2026`",
            ),
            ("+ derived at build", 2, "`l2b_matched`, `l2b_fec_donor_pre_signup`"),
            ("= **features in the artifact**", full.shape[1] - 1, ""),
        ],
        columns=["stage", "n", "note"],
    )
    doc += md_table(funnel)

    # ---- 2. proxy drops with exact posteriors (the cherry-pick table)
    doc += [
        "",
        f"## 2. Proxy-screen exclusions ({len(dropped)} columns) — the cherry-pick table",
        "",
        "Label-free screen on a 109,568-row nationwide L2 hash sample: a column drops "
        f"when any of its values (deciles for numerics, levels with n ≥ 200) shifts the "
        f"posterior of any protected class by ≥ {DROP_AT:.2f} from base. Each row below "
        "is that column's WORST offense — `P(class | level)` against the population "
        "base rate — so any column here can be individually argued back in. Thresholds: "
        "0.30 drops 150, 0.25 would drop 272, 0.20 would drop 494 "
        "(threshold 0.30 confirmed at the 2026-08-13 gate).",
        "",
    ]
    drop_tab = exact_posteriors(sample, screen, dropped.column_name.tolist())
    doc += md_table(drop_tab)

    near = strength[(strength >= 0.25) & (strength < DROP_AT)]
    doc += [
        "",
        f"### Near-misses that STAYED (shift 0.25–0.30, {len(near)} columns)",
        "",
        "The columns closest to the line, same format — the counterpart list if the "
        "threshold ever tightens.",
        "",
    ]
    near_tab = exact_posteriors(sample, screen, list(near.index))
    doc += md_table(near_tab)

    # ---- 3. rule exclusions
    doc += ["", "## 3. Rule exclusions (a priori, label-free)", ""]
    prot = disp[disp.disposition.isin(["drop_protected", "drop_protected_pending"])]
    doc += [
        f"**Protected classes ({len(prot)} columns, decided; includes the two "
        "disability columns confirmed at the gate):**",
        "",
    ]
    doc += md_table(prot[["column_name", "reason"]].rename(columns={"column_name": "column"}))
    for label, key in [
        ("district / jurisdiction geography", "drop_geo"),
        ("PII", "drop_pii"),
        ("identifiers / vendor meta", "drop_id_meta"),
        ("vendor-reserved", "drop_unknown"),
        ("free text", "drop_freetext"),
    ]:
        names = disp[disp.disposition == key].column_name.tolist()
        doc += ["", *details(f"{label} — {len(names)} columns (click to expand)", names)]

    # ---- 4. kept overview
    doc += ["", "## 4. What survived, by block", ""]

    def block_of(c: str) -> str:
        if c.startswith("hs_"):
            return "haystaq scores (hs_)"
        if c.startswith("hf_"):
            return "haystaq flags (hf_)"
        if c.startswith("ConsumerData"):
            return "consumer data"
        if c.startswith("PRI_BLT"):
            return "primary ballot party by year"
        if any(
            c.startswith(p)
            for p in ("General_", "Primary_", "AnyElection", "OtherElection", "PresidentialPrimary")
        ):
            return "vote history by year"
        if c.startswith("FECDonors"):
            return "FEC donations (date-gated)"
        return "voter core (registration, party, phones, household)"

    ov = (
        kept.assign(block=kept.column_name.map(block_of))
        .groupby(["block", "time_discipline"])
        .size()
        .reset_index(name="n")
    )
    doc += md_table(
        ov.pivot_table(
            index="block", columns="time_discipline", values="n", fill_value=0, aggfunc="sum"
        ).reset_index()
    )
    closest = kept.dropna(subset=["proxy_strength"]).nlargest(20, "proxy_strength")
    doc += ["", "**Kept columns closest to the proxy line (top 20 by residual " "association):**", ""]
    doc += md_table(
        pd.DataFrame(
            {
                "column": closest.column_name,
                "max posterior shift": closest.proxy_strength.map(lambda v: f"{v:.2f}"),
                "missingness shift": closest.missingness_shift.map(
                    lambda v: "—" if pd.isna(v) else f"{v:.2f}"
                ),
            }
        )
    )
    doc += [
        "",
        "Missingness-pattern shifts are reported, not auto-dropped "
        "(`AddressDistricts_Change_Changed_CD/County` missingness shifts the gender "
        "posterior by 0.40 — a coverage pattern, watched, not a value proxy).",
        "",
    ]

    # ---- 5. data-driven drops
    dead16 = sorted(set(disp[disp.disposition == "keep"].column_name) - set(cand.column_name))
    doc += [
        "## 5. Dead / high-cardinality (dropped on data)",
        "",
        *details(f"dead or constant in the screening sample — {len(dead16)} columns", dead16),
        "",
    ]

    # ---- 6. mechanical checks, readable
    doc += [
        "## 6. Mechanical checks",
        "",
        "### Reach and missingness",
        "",
        f"- Population spine: {len(full):,} users; matched "
        f"{len(matched):,} ({pct(full.l2b_matched.mean())}).",
        f"- Working set: {int(ws_matched.sum()):,} of {len(ws):,} rows reached "
        f"({pct(ws_matched.mean())}) — the accepted 56% ceiling.",
        "- Missingness is dominated by ONE cause: the match gate. Unmatched users "
        "are NaN across the entire block by construction. Within MATCHED users the "
        "excess-missing columns are:",
        "",
    ]
    vc = [c for c in matched.columns if c not in ("user_id", "l2b_matched")]
    wm = matched[vc].isna().mean().sort_values(ascending=False)
    wm = wm[wm > 0.20].head(15)
    doc += md_table(
        pd.DataFrame(
            {
                "column": wm.index,
                "missing within matched": [pct(v) for v in wm.to_numpy()],
                "why": ["date-gate or vendor fill (see null worksheet in decisions §1)"] * len(wm),
            }
        )
    )

    sent = sentinel_scan(full, "user_id")
    doc += ["", "### Sentinel zeros (3 flagged; DECIDED at the 2026-08-13 gate: " "keep as real zeros)", ""]
    if len(sent):
        doc += md_table(
            pd.DataFrame(
                {
                    "column": sent.column,
                    "zero share": [pct(v) for v in sent.share],
                    "null share (separate unknown level)": [pct(v) for v in sent.null_share],
                    "call": ["real zero — vendor carries unknown as NULL"] * len(sent),
                }
            )
        )

    flags = binary_flags(full, "user_id")
    rare = flags[~flags.verdict.str.startswith("usable")] if len(flags) else flags
    doc += [
        "",
        f"### Too rare to learn ({len(rare)} flags)",
        "",
        "Kept in the artifact (the full-block experiment uses no pre-selection); "
        "a tree simply cannot split on them. None warrant rule routing.",
        "",
    ]
    if len(rare):
        doc += md_table(
            pd.DataFrame(
                {
                    "flag": rare.flag,
                    "minority class n (working set)": rare.minority,
                }
            )
        )

    doc += [
        "",
        "### Redundancy and cross-family",
        "",
        "- Within-block: ~980 pairs at |r| ≥ 0.95, overwhelmingly haystaq "
        "support/oppose mirror pairs and ideology twins. Deliberately NOT resolved "
        "here — the correlation-cluster map (`datasets/l2_block_clusters.csv`) is the "
        "resolution artifact: any kept subset takes one representative per cluster.",
        "- Cross-family vs the accepted easy-match family: 19 pairs at exactly "
        "r = ±1.000 (`l2b_Voters_Age` ↔ `l2_age_at_signup`, FEC fields, the 11-column "
        "haystaq shortlist...). That is a REPLICATION CHECK passing, not a conflict: "
        "the block supersedes `l2_features` in experiments; `l2_features` stays the "
        "accepted 25-column artifact; neither is in the baseline.",
        "- Hygiene: no null keys, no infs, no padded strings, no duplicate columns.",
        "",
        "### Era gates",
        "",
        "The only era structure is the match gate itself plus the deliberate PIT gates "
        "(vote-history years, registration, FEC). L2 snapshot acquisition is "
        "independent of signup date, so fill does not step by signup year beyond those.",
        "",
    ]

    # match-rate by signup year (the one era table worth reading)
    j = ws[["user_id", "signup_date"]].copy()
    j["year"] = pd.to_datetime(j.signup_date).dt.year
    j["matched"] = j.user_id.isin(set(matched.user_id))
    era = j.groupby("year").agg(n=("user_id", "size"), match_rate=("matched", "mean"))
    doc += md_table(
        pd.DataFrame(
            {
                "signup year": era.index,
                "n": [f"{v:,}" for v in era.n],
                "match rate": [pct(v) for v in era.match_rate],
            }
        )
    )

    tail_default = "\n".join(
        [
            "",
            "",
            "### 1. Null semantics",
            "",
            "Settled structurally (2026-08-13 gate):",
            "",
            "| class | encoding | columns |",
            "|---|---|---|",
            "| unmatched user | NaN across the block, `l2b_matched` = 0 | all |",
            "| matched, flag absent | genuine 0 (vendor writes presence only) "
            "| vote history, consumer Y-flags |",
            "| matched, no primary ballot | `NONE` category | `PRI_BLT_*` |",
            "| gated by PIT | NaN (own-year votes, post-signup reg/FEC/moves) "
            "| year- and date-gated columns |",
            "| sentinel zeros (3 cols) | real zeros, NULL carries unknown | see §6 |",
            "| vendor did not model / fill | NaN | hs_/hf_/consumer remainder |",
            "",
            "### 2. Cleanup",
            "",
            "Numeric-in-string parsing (%/$/comma stripped) at build; single-level Y flags "
            "to 1/0; everything else left as delivered.",
            "",
            "### 3. Derived variables",
            "",
            "None at this stage by design (raw-measurement policy 2026-08-12); "
            "cluster representatives and any ratios wait for the stage-7 pass.",
            "",
            "### 4. Exclusions",
            "",
            "See §§2-3 and 5 above (complete lists with association levels).",
            "",
            "### 5. Leakage and coverage-proxy interrogation",
            "",
            "- `l2b_matched` partly encodes name-matchability (name-commonness channel) — "
            "read with the standing caveat; ambiguity counts stay in the bridge.",
            "- Vendor fill (consumer data, haystaq coverage) describes data-broker reach, "
            "not the person; missingness-shift column in §4 watches the protected side.",
            "- No column is written because a campaign happened; the PIT gates remove the "
            "channels that could be (own-year votes, post-signup registration/donations).",
            "",
            "### 6. Covariates and confounders",
            "",
            "State (match universe), signup era (via lead time), and district size move "
            "with much of the block; the full-block experiment on frozen folds is the "
            "controlled read.",
            "",
            "### 7. Open questions",
            "",
            "- Cherry-pick candidates from §2 (columns to argue back in despite the screen)?",
            "- Univariate map + cluster representatives (l2_block_univariate.csv / "
            "l2_block_clusters.csv) — fold-internal, lands with the experiment.",
            "",
        ]
    )

    preserved = ""
    if OUT.exists() and DECISIONS_MARKER in OUT.read_text():
        preserved = OUT.read_text().split(DECISIONS_MARKER, 1)[1]
    # the generic write_intake skeleton (empty decision tables) is not worth preserving;
    # detect it by its unfilled first row
    if "|  |  |  |" in preserved or not preserved.strip():
        preserved = tail_default
    OUT.write_text("\n".join(doc) + "\n\n" + DECISIONS_MARKER + preserved)
    print(f"wrote {OUT}  ({OUT.stat().st_size / 1024:.0f} KB)")
    print(f"proxy table rows: {len(drop_tab)}   near-miss rows: {len(near_tab)}")
