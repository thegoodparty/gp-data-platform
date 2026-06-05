# scripts/audit_low_confidence.py
"""
Audit low-confidence matches: find the most ambiguous pairs (closest to 0.5
match probability) to help identify false positives and understand model
uncertainty.

Usage:
    uv run python -m scripts.cli audit low-confidence --entity-type candidacy_stage --results-dir results/candidacy_stage/
"""

from pathlib import Path

import pandas as pd

from scripts.entity_config import EntityConfig


def run_low_confidence(
    pairwise_df: pd.DataFrame,
    results_dir: Path,
    config: EntityConfig,
    sample_n: int = 20,
) -> pd.DataFrame:
    """Find the N most ambiguous pairs (closest to 0.5) and write audit CSV."""
    df = pairwise_df.copy()
    print(f"Total pairwise predictions: {len(df):,}")

    if len(df) == 0:
        print("No predictions to audit.")
        return pd.DataFrame()

    # Rank by distance from 0.5 — the most ambiguous pairs come first
    df["_ambiguity"] = (df["match_probability"] - 0.5).abs()
    sampled = df.nsmallest(sample_n, "_ambiguity")
    sampled = sampled.sort_values("match_probability")

    prob_range = sampled["match_probability"]
    print(
        f"Most ambiguous {len(sampled)} pairs: "
        f"probability range [{prob_range.min():.4f}, {prob_range.max():.4f}]"
    )

    # Build a side-by-side comparison output
    rows = []
    for _, pair in sampled.iterrows():
        row = {
            "match_probability": round(pair["match_probability"], 4),
            "match_weight": round(pair["match_weight"], 2),
            "distance_from_0.5": round(pair["_ambiguity"], 4),
        }
        # Add side-by-side display columns
        for col in config.audit_display_columns:
            l_col = f"{col}_l"
            r_col = f"{col}_r"
            if l_col in pair.index:
                row[l_col] = pair[l_col]
                row[r_col] = pair[r_col]

        # Add gamma values for interpretability
        for col in config.audit_gamma_columns:
            if col in pair.index:
                row[col] = int(pair[col]) if pd.notna(pair[col]) else None

        rows.append(row)

    out_df = pd.DataFrame(rows)
    out_path = results_dir / "audit_low_confidence.csv"
    out_df.to_csv(out_path, index=False)
    print(f"\n{len(out_df)} most ambiguous pairs → {out_path}")

    # Print a quick breakdown of gamma patterns
    print("\n── Gamma Pattern Summary (sampled pairs) ──")
    for col in config.audit_gamma_columns:
        if col in out_df.columns:
            counts = out_df[col].value_counts().sort_index()
            dist = ", ".join(f"γ={int(k)}:{int(v)}" for k, v in counts.items())
            print(f"  {col}: {dist}")

    return out_df
