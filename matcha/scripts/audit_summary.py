"""
Audit summary: overall match statistics for an entity resolution run.

Usage:
    uv run python -m scripts.cli audit summary --results-dir results/
"""

from pathlib import Path

import pandas as pd


def run_summary(
    input_df: pd.DataFrame,
    pairwise_df: pd.DataFrame,
    clustered_df: pd.DataFrame,
    results_dir: Path,
) -> None:
    """Print match statistics and write a summary CSV (from DataFrames)."""
    providers = sorted(input_df["source_name"].unique())
    print(f"Providers in input: {providers}")

    # ── Input record counts + coverage ──
    cluster_sizes = clustered_df.groupby("cluster_id").size()
    multi_cluster_ids = cluster_sizes[cluster_sizes > 1].index
    matched_ids = set(
        clustered_df.loc[
            clustered_df["cluster_id"].isin(multi_cluster_ids), "unique_id"
        ]
    )

    print("\n── Input Records ──")
    summary_rows = []
    for p in providers:
        provider_ids = set(input_df[input_df["source_name"] == p]["unique_id"])
        matched = provider_ids & matched_ids
        print(f"  {p}: {len(provider_ids):,}")
        summary_rows.append(
            {
                "provider": p,
                "input_records": len(provider_ids),
                "matched_records": len(matched),
                "match_rate": len(matched) / len(provider_ids) if provider_ids else 0,
            }
        )
    print(f"  Total: {len(input_df):,}")

    # ── Coverage: % of each provider's records that appear in a multi-record cluster ──
    print("\n── Match Coverage ──")
    for row in summary_rows:
        p = row["provider"]
        print(
            f"  {p}: {row['matched_records']:,}/{row['input_records']:,} "
            f"({row['match_rate'] * 100:.1f}%)"
        )

    # ── Cluster size distribution ──
    print("\n── Cluster Sizes ──")
    for size, count in sorted(cluster_sizes.value_counts().items()):
        label = "singletons (unmatched)" if size == 1 else f"size {size}"
        print(f"  {label}: {count:,} clusters")

    # ── Match rate by provider pair ──
    print("\n── Pairwise Matches by Provider Pair ──")
    for (src_l, src_r), group in sorted(
        pairwise_df.groupby(["source_name_l", "source_name_r"])
    ):
        pair = f"{src_l} × {src_r}"
        print(f"  {pair}: {len(group):,} pairs")
        probs = group["match_probability"]
        print(f"    probability: mean={probs.mean():.3f}, median={probs.median():.3f}")
        high = (probs >= 0.95).sum()
        mid = ((probs >= 0.5) & (probs < 0.95)).sum()
        low = (probs < 0.5).sum()
        print(
            f"    high (≥0.95): {high:,}  |  mid (0.5–0.95): {mid:,}  |  low (<0.5): {low:,}"
        )

    # ── Cross-source vs within-source clusters ──
    print("\n── Cluster Types ──")
    cluster_providers = clustered_df.groupby("cluster_id")["source_name"].nunique()
    multi_mask = cluster_sizes > 1
    cross_source = (cluster_providers[multi_mask] > 1).sum()
    within_source = (cluster_providers[multi_mask] == 1).sum()
    print(f"  Cross-source matched clusters: {cross_source:,}")
    if within_source > 0:
        print(f"  WARNING: Within-source duplicate clusters: {within_source:,}")

    # ── Write summary CSV ──
    summary_out = results_dir / "audit_summary.csv"
    pd.DataFrame(summary_rows).to_csv(summary_out, index=False)
    print(f"\nSummary written to {summary_out}")
