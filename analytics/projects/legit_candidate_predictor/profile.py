"""Data profile for build scripts: the notebook's `eda_summary` table, importable.

DELIBERATE TWIN (Tristan, 2026-08-06). The notebook keeps its own inline copy of `eda_summary`
so it stays editable there, and `build_notebook.py` now emits that cell so regeneration no
longer deletes it. This module is the copy that build scripts import. Both were aligned to the
same output (column order via `reindex`, `index.name = "column"`); if you change one, change the
other, and prefer changing the notebook first since that is the one being read by a human.

Same output shape as the notebook version:

  all columns      dtype, count, missing_%
  numeric only     mean, std, min, 25%, 50%, 75%, max, skew
  categorical only n_unique, top_N_values

`write_profile()` additionally drops a markdown file under `profiles/` so a new dataset can be
eyeballed without opening Python.

PII: `mask_pii=True` (the default) reports count and missing_% for name / email / phone /
address columns but never their top values, so profiles stay safe to commit and to paste.

Usage from a build script:

    from profile import write_profile
    write_profile(feats, "name_features")

Usage from the notebook:

    from profile import eda_summary
    display(eda_summary(df))
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
PROFILES = HERE / "profiles"

PII_PATTERN = re.compile(
    r"(first_name|last_name|full_name|_name$|^name$|email|phone|address|street|lalvoterid)",
    re.I,
)


def eda_summary(df: pd.DataFrame, cat_top_n: int = 5, mask_pii: bool = True) -> pd.DataFrame:
    """One summary row per column: dtype, fill, numeric distribution or category frequencies.

    Cells that do not apply to a column's type are left as NaN, so the numeric and
    categorical blocks read as one table.
    """
    numeric_cols = df.select_dtypes(include="number").columns
    cat_cols = df.select_dtypes(include=["object", "category", "bool"]).columns
    top_col = f"top_{cat_top_n}_values"

    rows: dict[str, dict] = {}

    for col in numeric_cols:
        s = df[col]
        rows[col] = {
            "dtype": str(s.dtype),
            "count": s.notna().sum(),
            "missing_%": round(s.isna().mean() * 100, 2),
            "mean": round(s.mean(), 3),
            "std": round(s.std(), 3),
            "min": round(s.min(), 3),
            "25%": round(s.quantile(0.25), 3),
            "50%": round(s.quantile(0.5), 3),
            "75%": round(s.quantile(0.75), 3),
            "max": round(s.max(), 3),
            "skew": round(s.skew(), 3),
        }

    for col in cat_cols:
        s = df[col]
        row = {
            "dtype": str(s.dtype),
            "count": s.notna().sum(),
            "missing_%": round(s.isna().mean() * 100, 2),
            "n_unique": s.nunique(dropna=True),
        }
        if mask_pii and PII_PATTERN.search(str(col)):
            row[top_col] = "<masked: PII>"
        else:
            top = s.value_counts(dropna=True).head(cat_top_n)
            row[top_col] = ", ".join(f"{idx} ({cnt})" for idx, cnt in top.items())
        rows[col] = row

    column_order = [
        "dtype",
        "count",
        "missing_%",
        "mean",
        "std",
        "min",
        "25%",
        "50%",
        "75%",
        "max",
        "skew",
        "n_unique",
        top_col,
    ]
    summary = pd.DataFrame(rows).T.reindex(columns=column_order)
    summary.index.name = "column"
    return summary


def write_profile(
    df: pd.DataFrame, name: str, *, cat_top_n: int = 5, mask_pii: bool = True, note: str = ""
) -> Path:
    """Print the profile and write it to `profiles/<name>.md`. Returns the path."""
    summary = eda_summary(df, cat_top_n=cat_top_n, mask_pii=mask_pii)
    PROFILES.mkdir(exist_ok=True)
    path = PROFILES / f"{name}.md"

    header = [f"# Data profile: `{name}`", ""]
    header.append(f"{len(df):,} rows x {df.shape[1]} columns.")
    if note:
        header.append("")
        header.append(note)
    header += [
        "",
        "Regenerate with the build script that produced the dataset. Numeric and",
        "categorical columns share one table; blanks mean the statistic does not apply",
        "to that column's type. PII columns show fill only, never top values.",
        "",
        "```",
    ]
    body = summary.to_string()
    path.write_text("\n".join(header) + "\n" + body + "\n```\n")

    print(f"\n=== profile: {name} ({len(df):,} rows x {df.shape[1]} cols) ===")
    print(body)
    print(f"\nwrote -> {path.relative_to(HERE)}")
    return path
