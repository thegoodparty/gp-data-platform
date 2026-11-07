"""Repeatable intake review for every new data source: the mechanical half, automated.

Pairs with `profile.py`. A profile tells you what is in a dataset; an intake review decides
what to do about it. Both run at the end of every `build_*.py`, and the intake markdown is the
per-source sidebar: null semantics, cleanup, derived variables, exclusions, covariates.

The split is deliberate. This module computes the checks that can be computed and leaves the
judgment calls as headings to fill in, because the judgment is the point. What it automates is
the set of traps this project has actually fallen into:

  sentinel conflation   a zero that means "unknown" read as a real zero. This cost 0.005 AUC on
                        n_br_candidacies_by_deadline: 4,455 rows carried 0 meaning "no deadline
                        known", conflated with 8,536 genuine zeros. Any numeric column whose
                        zeros dominate its distribution gets flagged here.
  era gates             a column that only exists after some instrumentation date, so fill rate
                        is a proxy for signup era rather than for the thing itself. Bit us twice
                        (election_level, Amplitude geo). Fill-by-year table catches it.
  coverage proxies      a feature that encodes "did our pipeline know about this row" rather
                        than a property of the row. Cannot be automated, so it is a required
                        heading with the question stated explicitly.
  dead columns          constant or near-constant, high-cardinality free text, exact duplicates
                        of a column already present.

Usage at the end of a build script:

    from intake import write_intake
    write_intake(feats, "br_position_features", key="br_position_id",
                 source="stg_airbyte_source__ballotready_api_position",
                 join="br_position_id -> working_set.br_position_id")
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
INTAKE = HERE / "intake"
WORKING_SET = HERE / "datasets" / "working_set.parquet"

SENTINEL_VALUES = (0, -1, -999, 9999)

# Every built feature family, for the cross-family covariance check (DATA-2239 scope amendment,
# 2026-08-12). Append a row when a new family's build script lands. The check joins each family
# through the working set, so families keyed on either user_id or br_position_id both work.
FAMILY_REGISTRY: list[tuple[str, str]] = [
    ("br_position_features.parquet", "br_position_id"),
    ("incumbency_features.parquet", "user_id"),
    ("name_features.parquet", "user_id"),
    ("geo_features.parquet", "user_id"),
    ("gp_user_details_features.parquet", "user_id"),
    ("l2_features.parquet", "user_id"),
    ("l2_block_features.parquet", "user_id"),
    ("amplitude_family_scaffold.parquet", "user_id"),
]

# Working-set columns that must never appear in a feature-vs-feature correlation: the label
# (correlating candidate features against it is selection on the outcome) and join keys.
WS_NEVER_CORRELATE = {
    "corroborated",
    "user_id",
    "campaign_id",
    "gp_candidacy_id",
    "gp_candidate_id",
    "br_position_id",
}

# Everything above this heading is regenerated on every run; everything below it is human-written
# and preserved. Do not change the string without migrating the existing intake files.
DECISIONS_MARKER = "## Decisions (fill these in; they are the point of the review)"


def _fmt(df: pd.DataFrame) -> str:
    return "```\n" + df.to_string() + "\n```" if len(df) else "_none_"


def grain_check(df: pd.DataFrame, key: str) -> tuple[str, pd.DataFrame]:
    dupes = int(df[key].duplicated().sum())
    exact_dupe_rows = int(df.duplicated().sum())
    lines = pd.DataFrame(
        [
            ("rows", f"{len(df):,}"),
            ("columns", str(df.shape[1])),
            (f"distinct {key}", f"{df[key].nunique():,}"),
            (f"duplicate {key}", f"{dupes:,}"),
            ("exact duplicate rows", f"{exact_dupe_rows:,}"),
            ("key is unique", "YES" if dupes == 0 else "NO — resolve before joining"),
        ],
        columns=["check", "value"],
    )
    verdict = "unique" if dupes == 0 else "NOT UNIQUE"
    return verdict, lines


def _is_binary(s: pd.Series) -> bool:
    """True for a 0/1 indicator. A zero there is a genuine false, never a sentinel."""
    vals = set(pd.unique(s.dropna()))
    return bool(vals) and vals <= {0, 1}


def sentinel_scan(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Numeric columns where a magic value dominates. The zeros-vs-unknown trap.

    Binary indicators are skipped: their zeros are false by construction, and flagging them
    buried the real signal in v0 of this check (12 flags, 11 of them meaningless).
    """
    rows = []
    for c in df.select_dtypes(include="number").columns:
        if c == key or _is_binary(df[c]):
            continue
        s = df[c]
        for v in SENTINEL_VALUES:
            share = float((s == v).mean())
            if share >= 0.10:
                rows.append(
                    {
                        "column": c,
                        "value": v,
                        "share": round(share, 4),
                        "n": int((s == v).sum()),
                        "null_share": round(float(s.isna().mean()), 4),
                        "distinct_others": int(s[s != v].nunique()),
                        "question": f"does {v} mean {v}, or does it mean unknown?",
                    }
                )
    return pd.DataFrame(rows)


MIN_POSITIVES_FOR_A_SPLIT = 30


def dead_columns(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Constant, near-constant, or unusable-as-is columns. Binary flags are judged separately."""
    rows = []
    for c in df.columns:
        if c == key or _is_binary(df[c]):
            continue
        s = df[c]
        nun = s.nunique(dropna=True)
        top_share = float(s.value_counts(normalize=True, dropna=True).iloc[0]) if nun else 1.0
        reason = None
        if nun <= 1:
            reason = "constant"
        elif top_share >= 0.99:
            reason = f"near-constant ({top_share:.1%} one value)"
        elif s.dtype == object and nun > 0.5 * len(s):
            reason = "free text / near-unique — needs encoding or exclusion"
        if reason:
            rows.append({"column": c, "n_unique": nun, "reason": reason})
    return pd.DataFrame(rows)


def binary_flags(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Positive counts for 0/1 flags, with the learnability verdict.

    Rare flags are not defective — they are supposed to be rare. But a gradient-boosted tree
    cannot split on a handful of positives, which is exactly why the name-plausibility family
    scored a null as features while working as a rule. This table makes that call up front
    instead of after an experiment.

    Counted on the rows that actually reach the working set where that is knowable: a flag with
    plenty of positives across a 60k side table can still have four in the modelled population,
    and it is the latter that decides whether a split exists.
    """
    sub = df
    scope = "side table"
    if WORKING_SET.exists():
        ws = pd.read_parquet(WORKING_SET)
        if key in ws.columns:
            sub = df[df[key].isin(set(ws[key].dropna()))]
            scope = "working set"
    if not len(sub):
        sub, scope = df, "side table (no working-set overlap)"

    rows = []
    for c in df.columns:
        if c == key or not _is_binary(df[c]):
            continue
        vals = sub[c].dropna().astype(float)
        pos = int(vals.sum())
        neg = int(len(vals) - pos)
        # The MINORITY class is what a split needs. A flag that is 99.8% true is exactly as
        # unlearnable as one that is 0.2% true, and v1 of this check only caught the latter.
        minority = min(pos, neg)
        rows.append(
            {
                "flag": c,
                f"positives_in_{scope.replace(' ', '_')}": pos,
                "negatives": neg,
                "minority": minority,
                "share": round(pos / max(len(vals), 1), 4),
                "verdict": (
                    "usable as a feature"
                    if minority >= MIN_POSITIVES_FOR_A_SPLIT
                    else f"minority class <{MIN_POSITIVES_FOR_A_SPLIT} — rule, not feature"
                ),
            }
        )
    out = pd.DataFrame(rows)
    return out.sort_values("minority") if len(out) else out


def missingness(df: pd.DataFrame, key: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Per-column missingness, and which columns go missing together.

    Two rates, because they answer different questions. Within-table missingness says how complete
    the source is. Within-population missingness says what the model will actually see, and the two
    diverge whenever the join itself is partial: a column that is 0% missing in a side table
    covering 88% of rows is 12% missing to the model.

    Co-missingness groups matter for null semantics: columns that vanish together usually share one
    cause (one absent parent record, one instrumentation era), so they get one decision, not five.
    """
    cols = [c for c in df.columns if c != key]
    within = df[cols].isna().mean().rename("missing_in_table")

    pop = pd.Series(np.nan, index=cols, name="missing_in_population")
    reach = np.nan
    if WORKING_SET.exists():
        ws = pd.read_parquet(WORKING_SET)
        if key in ws.columns:
            j = ws[[key]].merge(df, on=key, how="left")
            pop = j[cols].isna().mean().rename("missing_in_population")
            reach = float(j[cols[0]].notna().mean()) if cols else np.nan

    tab = pd.concat([within, pop], axis=1).round(4).sort_values("missing_in_table", ascending=False)
    tab.index.name = "column"
    if not np.isnan(reach):
        tab.attrs["join_reach"] = reach

    # Columns whose missing masks are identical share a cause.
    groups: dict[str, list[str]] = {}
    for c in cols:
        sig = df[c].isna().to_numpy().tobytes()
        groups.setdefault(sig, []).append(c)
    shared = pd.DataFrame(
        [
            {
                "n_columns": len(v),
                "missing_share": round(float(df[v[0]].isna().mean()), 4),
                "columns": ", ".join(v),
            }
            for v in groups.values()
            if len(v) > 1 and df[v[0]].isna().any()
        ]
    )
    if len(shared):
        shared = shared.sort_values("missing_share", ascending=False)
    return tab, shared


def pending_decisions(df: pd.DataFrame, key: str, name: str = "") -> list[dict]:
    """The ambiguous calls this dataset raises, as structured items to put to a human.

    The mechanical checks can find these but cannot settle them, so the build script surfaces them
    for a decision instead of silently picking a default. Interactive by design: silent defaults are
    how a coverage proxy gets into a model.
    """
    items: list[dict] = []
    for r in sentinel_scan(df, key).itertuples():
        items.append(
            {
                "kind": "null_semantics",
                "column": r.column,
                "question": f"`{r.column}` is {r.value} for {r.share:.1%} of rows "
                f"({r.n:,}) with {r.null_share:.1%} null. Is {r.value} a real value or a "
                f"stand-in for unknown?",
                "options": [
                    "real value, keep as is",
                    "unknown, NaN it plus an indicator",
                    "both are present and need separating",
                ],
            }
        )
    flags = binary_flags(df, key)
    for r in flags[~flags["verdict"].str.startswith("usable")].itertuples() if len(flags) else []:
        items.append(
            {
                "kind": "rule_or_feature",
                "column": r.flag,
                "question": f"`{r.flag}` has a minority class of {r.minority}. Too small for a tree "
                f"split. Route as a rule, keep as a feature anyway, or drop?",
                "options": ["rule", "keep as feature", "drop"],
            }
        )
    for r in redundancy(df, key).itertuples():
        items.append(
            {
                "kind": "redundancy",
                "column": f"{r.a} / {r.b}",
                "question": f"`{r.a}` and `{r.b}` correlate at {r.abs_corr:.3f}. Which one survives?",
                "options": [r.a, r.b, "keep both"],
            }
        )
    if name:
        xfam = cross_family_covariance(df, key, name)
        for r in xfam[xfam["verdict"].str.startswith("DECIDE")].itertuples() if len(xfam) else []:
            items.append(
                {
                    "kind": "cross_family_redundancy",
                    "column": f"{r.new_column} / {r.existing_column}",
                    "question": f"`{r.new_column}` correlates at {r.corr:+.3f} with "
                    f"`{r.existing_column}` (family: {r.family}). Same concept from two "
                    f"sources — which survives?",
                    "options": [r.new_column, r.existing_column, "keep both"],
                }
            )
    return items


def redundancy(df: pd.DataFrame, key: str, thresh: float = 0.95) -> pd.DataFrame:
    num = df.select_dtypes(include="number").drop(columns=[key], errors="ignore")
    if num.shape[1] < 2:
        return pd.DataFrame()
    corr = num.corr(numeric_only=True).abs()
    rows = []
    cols = corr.columns
    for i, a in enumerate(cols):
        for b in cols[i + 1 :]:
            r = corr.loc[a, b]
            if pd.notna(r) and r >= thresh:
                rows.append({"a": a, "b": b, "abs_corr": round(float(r), 4)})
    return pd.DataFrame(rows).sort_values("abs_corr", ascending=False) if rows else pd.DataFrame()


def population_reach(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """How much of the modelled population this table actually reaches."""
    if not WORKING_SET.exists():
        return pd.DataFrame()
    ws = pd.read_parquet(WORKING_SET)
    if key not in ws.columns:
        return pd.DataFrame([{"note": f"{key} not in working_set; reach not computable here"}])
    left = ws[[key]].copy()
    reached = left[key].isin(set(df[key].dropna()))
    return pd.DataFrame(
        [
            {
                "working_set_rows": len(ws),
                f"distinct {key} in working set": int(ws[key].nunique()),
                "rows reached": int(reached.sum()),
                "reach": f"{reached.mean():.1%}",
            }
        ]
    )


def fill_by_era(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Fill rate by signup year. Catches instrumentation-era gates masquerading as signal."""
    if not WORKING_SET.exists():
        return pd.DataFrame()
    ws = pd.read_parquet(WORKING_SET)
    if key not in ws.columns or "signup_date" not in ws.columns:
        return pd.DataFrame()
    j = ws[[key, "signup_date"]].merge(df, on=key, how="left")
    j["year"] = pd.to_datetime(j["signup_date"]).dt.year
    cols = [c for c in df.columns if c != key]
    out = j.groupby("year")[cols].apply(lambda g: g.notna().mean().round(3))
    out.insert(0, "n", j.groupby("year").size())
    return out


def null_worksheet(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Pre-filled null-semantics and imputation worksheet, one row per column with missingness.

    Mechanical half of the DATA-2239 per-source requirement: the null share the model actually
    sees, which co-missingness group the column belongs to (one group, one decision), and whether
    the fill rate steps across years (era gate suspicion). The two right-hand cells stay TBD for
    the human: class (not applicable / unknown / genuine zero / right-censored) and imputation
    (NaN-native is the HGB default, but every column still declares it).
    """
    miss, _ = missingness(df, key)
    cols = [c for c in df.columns if c != key]

    group_of: dict[str, int] = {}
    sigs: dict[bytes, int] = {}
    for c in cols:
        sig = df[c].isna().to_numpy().tobytes()
        group_of[c] = sigs.setdefault(sig, len(sigs) + 1)

    era = fill_by_era(df, key)
    stepped: set[str] = set()
    if len(era):
        fills = era.drop(columns=["n"], errors="ignore")
        big_years = era["n"] >= 100  # tiny cohorts produce noise steps, not era gates
        f = fills[big_years]
        if len(f) >= 2:
            stepped = set(f.columns[(f.diff().abs() > 0.15).any()])

    rows = []
    for c in cols:
        pop_miss = miss.loc[c, "missing_in_population"]
        tab_miss = miss.loc[c, "missing_in_table"]
        if (pd.isna(pop_miss) or pop_miss == 0) and tab_miss == 0:
            continue
        rows.append(
            {
                "column": c,
                "missing_in_population": pop_miss,
                "comiss_group": group_of[c],
                "era_step": "YES" if c in stepped else "",
                "class": "TBD",
                "imputation": "TBD",
            }
        )
    return pd.DataFrame(rows)


def cross_family_covariance(
    df: pd.DataFrame, key: str, name: str, report: float = 0.70, decide: float = 0.95
) -> pd.DataFrame:
    """Correlate this family's numeric columns against every OTHER built family plus the
    working set's own numerics, joined through the working set.

    The within-source redundancy check cannot see a twin living in another family — knowRun vs
    has_password at 0.956 was only caught because both happened to sit in one artifact. The label
    and join keys are excluded by construction (WS_NEVER_CORRELATE).
    """
    if not WORKING_SET.exists():
        return pd.DataFrame()
    ws = pd.read_parquet(WORKING_SET)
    if key not in ws.columns:
        return pd.DataFrame()

    frame_ = ws.merge(df, on=key, how="left", suffixes=("", "__dup"))
    new_cols = [c for c in df.columns if c != key and pd.api.types.is_numeric_dtype(df[c])]
    new_cols = [c for c in new_cols if c in frame_.columns]

    others: list[tuple[str, str]] = []  # (column, family label)
    for col in ws.columns:
        if col not in WS_NEVER_CORRELATE and pd.api.types.is_numeric_dtype(ws[col]):
            others.append((col, "working_set"))
    for fname, fkey in FAMILY_REGISTRY:
        if Path(fname).stem == name or not (HERE / "datasets" / fname).exists():
            continue
        if fkey not in ws.columns:
            continue
        fam = pd.read_parquet(HERE / "datasets" / fname)
        if fkey not in fam.columns:
            continue
        fam_cols = [c for c in fam.columns if c != fkey and pd.api.types.is_numeric_dtype(fam[c])]
        # A column name already present would land twice and silently self-correlate.
        fam_cols = [c for c in fam_cols if c not in frame_.columns]
        if not fam_cols:
            continue
        frame_ = frame_.merge(fam[[fkey, *fam_cols]], on=fkey, how="left")
        others += [(c, Path(fname).stem) for c in fam_cols]

    rows = []
    for c in new_cols:
        a = pd.to_numeric(frame_[c], errors="coerce")
        if a.notna().sum() < 100 or a.nunique(dropna=True) < 2:
            continue
        for other, family in others:
            b = pd.to_numeric(frame_[other], errors="coerce")
            if b.notna().sum() < 100 or b.nunique(dropna=True) < 2:
                continue
            r = a.corr(b)
            if pd.notna(r) and abs(r) >= report:
                rows.append(
                    {
                        "new_column": c,
                        "existing_column": other,
                        "family": family,
                        "corr": round(float(r), 4),
                        "verdict": ("DECIDE — same concept twice" if abs(r) >= decide else "note"),
                    }
                )
    out = pd.DataFrame(rows)
    return out.sort_values("corr", key=abs, ascending=False) if len(out) else out


def hygiene_checks(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Standard hygiene: the boring defects that break joins and models silently."""
    rows = []
    rows.append(
        {
            "check": "null keys",
            "value": int(df[key].isna().sum()),
            "verdict": "ok" if df[key].notna().all() else "FIX — rows unjoinable",
        }
    )
    num = df.select_dtypes(include="number")
    n_inf = int(np.isinf(num.to_numpy(dtype=float, na_value=np.nan)).sum()) if len(num.columns) else 0
    rows.append(
        {
            "check": "inf values in numerics",
            "value": n_inf,
            "verdict": "ok" if n_inf == 0 else "FIX — NaN or cap them",
        }
    )
    padded = [
        c
        for c in df.select_dtypes(include="object").columns
        if df[c].dropna().astype(str).str.match(r"^\s|\s$").any()
    ]
    rows.append(
        {
            "check": "whitespace-padded strings",
            "value": ", ".join(padded) or 0,
            "verdict": "ok" if not padded else "FIX — strip before joining",
        }
    )
    dupes = df.columns[df.columns.duplicated()].tolist()
    rows.append(
        {
            "check": "duplicate column names",
            "value": ", ".join(dupes) or 0,
            "verdict": "ok" if not dupes else "FIX",
        }
    )
    return pd.DataFrame(rows)


def write_intake(
    df: pd.DataFrame,
    name: str,
    *,
    key: str,
    source: str = "",
    join: str = "",
    pit_class: str = "TBD",
    note: str = "",
) -> Path:
    """Run the mechanical checks, write `intake/<name>.md` with the decisions left to fill."""
    verdict, grain = grain_check(df, key)
    sent = sentinel_scan(df, key)
    dead = dead_columns(df, key)
    flags = binary_flags(df, key)
    miss, shared = missingness(df, key)
    redun = redundancy(df, key)
    reach = population_reach(df, key)
    era = fill_by_era(df, key)
    nulls = null_worksheet(df, key)
    xfam = cross_family_covariance(df, key, name)
    hyg = hygiene_checks(df, key)

    doc = [
        f"# Intake review: `{name}`",
        "",
        f"- **Source**: {source or 'TBD'}",
        f"- **Join**: {join or 'TBD'}",
        f"- **Grain**: one row per `{key}` ({verdict})",
        f"- **Pass class**: {pit_class}  (B = knowable at or before signup and immutable; A = otherwise)",
        f"- **Profile**: `profiles/{name}.md`",
    ]
    if note:
        doc += ["", note]

    doc += [
        "",
        "## Mechanical checks (automated, regenerated with the build script)",
        "",
        "### Grain and duplicates",
        "",
        _fmt(grain),
    ]
    doc += ["", "### Population reach", "", _fmt(reach)]
    doc += [
        "",
        "### Missingness — within this table vs within the population the model sees",
        "",
        _fmt(miss),
        "",
        "The two columns diverge whenever the join is partial: a column that is 0% missing in a "
        "side table reaching 88% of rows is 12% missing to the model. Decide null semantics "
        "against the second column, not the first.",
        "",
        "#### Columns that go missing together (one cause, one decision)",
        "",
        _fmt(shared),
    ]
    doc += [
        "",
        "### Sentinel scan — numeric columns where one magic value dominates",
        "",
        _fmt(sent),
        "",
        'Every row above is a decision, not a finding. A zero that means "unknown" must '
        "become NaN plus an explicit indicator; leaving it as zero is how "
        "`n_br_candidacies_by_deadline` leaked 0.005 AUC of coverage-proxy signal.",
    ]
    doc += ["", "### Dead or unusable columns", "", _fmt(dead)]
    doc += [
        "",
        "### Binary flags — positives and learnability",
        "",
        _fmt(flags),
        "",
        f"A flag whose MINORITY class is under {MIN_POSITIVES_FOR_A_SPLIT} cannot be split on by "
        "a tree no matter how clean its signal is — 99.8% true is as unlearnable as 0.2% true. "
        "Route those as rules and say so here, rather than discovering it from a null experiment.",
    ]
    doc += ["", "### Redundancy (abs corr >= 0.95)", "", _fmt(redun)]
    doc += [
        "",
        "### Fill rate by signup year — era-gate check",
        "",
        _fmt(era),
        "",
        "If a column's fill rate steps at a year boundary, it is partly an instrumentation "
        "marker. Decide whether the feature is gated, era-interacted, or dropped.",
    ]
    doc += [
        "",
        "### Null-semantics and imputation worksheet (DATA-2239 per-source requirement)",
        "",
        _fmt(nulls),
        "",
        "One row per column with missingness the model will see. Copy each row's settled "
        "class and imputation into decision section 1; columns sharing a comiss_group share "
        "one cause and one decision. NaN-native (HGB routes missing values) is the default "
        "imputation but must still be declared per column. era_step=YES means the fill rate "
        "moves >15pp between adjacent years: suspect an instrumentation gate, not behavior.",
    ]
    doc += [
        "",
        "### Cross-family covariance (|r| >= 0.70 vs working set + every built family)",
        "",
        _fmt(xfam),
        "",
        "The within-source redundancy check cannot see a twin living in another family. "
        "knowRun vs has_password (0.956, 2026-08-12) was caught by luck because both sat in "
        "one artifact; this check makes it mechanical. Rows marked DECIDE are one concept "
        "from two sources: exactly one survives.",
    ]
    doc += ["", "### Hygiene", "", _fmt(hyg)]

    doc += [
        "",
        DECISIONS_MARKER,
        "",
        "### 1. Null semantics",
        "",
        "For each column with missing values, which is it?",
        "",
        "- **not applicable** — the concept does not exist for this row (encode as its own category)",
        "- **unknown** — the concept exists, we do not know it (NaN plus an indicator)",
        "- **genuine zero / false** — the value really is zero or false",
        "- **not yet arrived** — right-censored; the value may land later (this is a PIT problem, "
        "not a null problem)",
        "",
        "| column | class | encoding | why |",
        "|---|---|---|---|",
        "|  |  |  |  |",
        "",
        "### 2. Cleanup",
        "",
        "Type coercions, unit normalisation, string trimming, outlier handling, and what is "
        "deliberately left dirty.",
        "",
        "### 3. Derived variables",
        "",
        "What we compute from the raw columns and the reasoning for each. Ratios and "
        "cohort-normalised versions belong here.",
        "",
        "| derived | formula | rationale |",
        "|---|---|---|",
        "|  |  |  |",
        "",
        "### 4. Exclusions",
        "",
        "Columns deliberately not used, each with a reason. Reasons that recur in this project: "
        "post-signup mutable, label-adjacent, coverage proxy, duplicate of an existing feature, "
        "equity-sensitive.",
        "",
        "| column | reason |",
        "|---|---|",
        "|  |  |",
        "",
        "### 5. Leakage and coverage-proxy interrogation",
        "",
        "Required question for every new source: **does this column describe the row, or does it "
        "describe whether our pipeline knew about the row?** If the latter, it predicts the label "
        "through the label's own construction and must not enter pass B.",
        "",
        "Second required question: could this value have been written or updated *because* the "
        "outcome happened? If yes it is not pass-B legal regardless of how it looks.",
        "",
        "### 6. Covariates and confounders",
        "",
        "What else moves with these features, and what must be conditioned on or reported "
        "alongside. State, office level, signup era, and Pro status are the usual suspects here; "
        "Pro gating in particular has already flipped one interpretation in this project.",
        "",
        "### 7. Open questions",
        "",
        "- ",
        "",
    ]

    INTAKE.mkdir(exist_ok=True)
    path = INTAKE / f"{name}.md"

    # Never overwrite decisions a human has written. The mechanical half above the marker is
    # regenerated every run; everything from the marker down is preserved verbatim once it
    # differs from the freshly generated skeleton.
    preserved = ""
    if path.exists() and DECISIONS_MARKER in path.read_text():
        preserved = path.read_text().split(DECISIONS_MARKER, 1)[1]
    mechanical = "\n".join(doc).split(DECISIONS_MARKER, 1)[0]
    tail = preserved if preserved.strip() else "\n".join(doc).split(DECISIONS_MARKER, 1)[1]
    path.write_text(mechanical + DECISIONS_MARKER + tail)
    if preserved.strip():
        print("(existing decision sections preserved; only the mechanical checks were refreshed)")

    too_rare = flags[~flags["verdict"].str.startswith("usable")] if len(flags) else flags
    print(f"\n=== intake review: {name} ===")
    print(
        f"grain: {verdict}   sentinel flags: {len(sent)}   dead cols: {len(dead)}   "
        f"redundant pairs: {len(redun)}   too-rare flags: {len(too_rare)}   "
        f"cross-family pairs: {len(xfam)}   hygiene failures: "
        f"{int((hyg['verdict'] != 'ok').sum())}"
    )
    if len(sent):
        print("SENTINEL SCAN — decide each of these before using the column:")
        print(sent.to_string(index=False))
    if len(too_rare):
        print("TOO RARE TO LEARN — route as rules, not features:")
        print(too_rare.to_string(index=False))
    state = "decisions preserved" if preserved.strip() else "decision sections need filling in"
    print(f"wrote -> {path.relative_to(HERE)}  ({state})")

    n_xfam_decide = int(xfam["verdict"].str.startswith("DECIDE").sum()) if len(xfam) else 0
    if n_xfam_decide:
        print("CROSS-FAMILY — same concept in two sources, one must survive:")
        print(xfam[xfam["verdict"].str.startswith("DECIDE")].to_string(index=False))
    bad_hyg = hyg[hyg["verdict"] != "ok"]
    if len(bad_hyg):
        print("HYGIENE FAILURES:")
        print(bad_hyg.to_string(index=False))

    pend = pending_decisions(df, key, name)
    if pend:
        print(
            f"\n{len(pend)} decision(s) need a human — put these to the analyst before shipping "
            "the family:"
        )
        for i, q in enumerate(pend, 1):
            print(f"  {i}. [{q['kind']}] {q['question']}")
    return path
