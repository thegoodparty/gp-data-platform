"""
Audit low-confidence matches: find the most ambiguous pairs (closest to 0.5
match probability) to help identify false positives and understand model
uncertainty.

Intended to be used during exploratory work and while adding a new provider to
an existing entity.

Usage:
    cd entity_resolution
    uv run python scripts/audit_low_confidence.py --results-dir results/
    uv run python scripts/audit_low_confidence.py --results-dir results/ --sample 30
"""

from pathlib import Path

import click
import pandas as pd

# Comparison columns whose gamma values we surface for interpretation
GAMMA_COLS = [
    "gamma_last_name",
    "gamma_first_name",
    "gamma_party",
    "gamma_email",
    "gamma_phone",
    "gamma_state",
    "gamma_election_date",
    "gamma_official_office_name",
    "gamma_district_identifier",
]

# Human-readable columns to display side-by-side
DISPLAY_COLS = [
    "source_name",
    "unique_id",
    "first_name",
    "last_name",
    "party",
    "email",
    "phone",
    "state",
    "election_date",
    "official_office_name",
    "district_identifier",
    "candidate_office",
    "br_race_id",
]


def run_low_confidence(
    pairwise_df: pd.DataFrame,
    results_dir: Path,
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
        for col in DISPLAY_COLS:
            l_col = f"{col}_l"
            r_col = f"{col}_r"
            if l_col in pair.index:
                row[l_col] = pair[l_col]
                row[r_col] = pair[r_col]

        # Add gamma values for interpretability
        for col in GAMMA_COLS:
            if col in pair.index:
                row[col] = int(pair[col]) if pd.notna(pair[col]) else None

        rows.append(row)

    out_df = pd.DataFrame(rows)
    out_path = results_dir / "audit_low_confidence.csv"
    out_df.to_csv(out_path, index=False)
    print(f"\n{len(out_df)} most ambiguous pairs → {out_path}")

    # Print a quick breakdown of gamma patterns
    print("\n── Gamma Pattern Summary (sampled pairs) ──")
    for col in GAMMA_COLS:
        if col in out_df.columns:
            counts = out_df[col].value_counts().sort_index()
            dist = ", ".join(f"γ={int(k)}:{int(v)}" for k, v in counts.items())
            print(f"  {col}: {dist}")

    return out_df


@click.command()
@click.option(
    "--results-dir",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Directory containing match results.",
)
@click.option(
    "--sample",
    "sample_n",
    default=20,
    type=int,
    help="Number of most-ambiguous pairs to return.",
)
def main(results_dir: Path, sample_n: int) -> None:
    """Find the most ambiguous matches for manual review."""
    pairwise_path = results_dir / "pairwise_predictions.csv"
    if not pairwise_path.exists():
        raise FileNotFoundError(f"Expected pairwise_predictions.csv in {results_dir}")

    df = pd.read_csv(pairwise_path)
    run_low_confidence(df, results_dir, sample_n)


if __name__ == "__main__":
    main()
