# scripts/audit_false_negatives.py
"""
Audit false negatives: find input records that didn't match but plausibly should have.

Usage:
    uv run python -m scripts.cli audit false-negatives --entity-type candidacy_stage --results-dir results/candidacy_stage/
"""

from pathlib import Path

import pandas as pd
from rapidfuzz.distance import JaroWinkler

from scripts.entity_config import EntityConfig


def _name_similar(a: str | None, b: str | None, threshold: float = 0.88) -> bool:
    """Check if two names are similar using JW similarity or exact match."""
    if pd.isna(a) or pd.isna(b):
        return False
    a, b = str(a).lower().strip(), str(b).lower().strip()
    if a == b:
        return True
    return JaroWinkler.similarity(a, b) >= threshold


def _canonicalize_pair_key(id_l: str, id_r: str) -> tuple[str, str]:
    """Return a sorted pair key for orientation-independent lookup."""
    return (min(id_l, id_r), max(id_l, id_r))


def _classify_pair(
    id_l: str,
    id_r: str,
    pairwise_keys: set[tuple[str, str]],
    filtered_keys: set[tuple[str, str]] | None,
) -> str:
    """Classify a pair into one of three states."""
    key = _canonicalize_pair_key(id_l, id_r)
    if key in pairwise_keys:
        return "generated_and_kept"
    if filtered_keys is not None and key in filtered_keys:
        return "generated_but_filtered"
    return "never_generated"


def _load_filtered_keys(results_dir: Path) -> set[tuple[str, str]] | None:
    """Load canonicalized pair keys from filtered_pairs.csv, or None if missing."""
    filtered_path = results_dir / "filtered_pairs.csv"
    if not filtered_path.exists():
        print(
            "WARNING: filtered_pairs.csv not found — cannot distinguish filtered "
            "pairs from blocking misses. Rerun the match pipeline for full "
            "3-state classification."
        )
        return None
    filtered_df = pd.read_csv(filtered_path, dtype=str)
    return {
        _canonicalize_pair_key(row["unique_id_l"], row["unique_id_r"])
        for _, row in filtered_df.iterrows()
    }


def run_false_negatives(
    input_df: pd.DataFrame,
    pairwise_df: pd.DataFrame,
    clustered_df: pd.DataFrame,
    results_dir: Path,
    config: EntityConfig,
    sample_n: int = 20,
) -> pd.DataFrame:
    """Find suspicious non-matches and write audit CSV."""
    # Find singletons — records in clusters of size 1 (unmatched)
    cluster_sizes = clustered_df.groupby("cluster_id").size()
    singleton_clusters = cluster_sizes[cluster_sizes == 1].index
    singletons = clustered_df[clustered_df["cluster_id"].isin(singleton_clusters)]
    print(f"Total clustered records: {len(clustered_df):,}")
    print(f"Singleton (unmatched) records: {len(singletons):,}")

    providers = sorted(input_df["source_name"].unique())
    print(f"Providers: {providers}")

    # Build canonicalized pair key sets for 3-state classification
    pairwise_keys = set()
    if len(pairwise_df) > 0 and "unique_id_l" in pairwise_df.columns:
        pairwise_keys = {
            _canonicalize_pair_key(row["unique_id_l"], row["unique_id_r"])
            for _, row in pairwise_df[["unique_id_l", "unique_id_r"]].iterrows()
        }

    filtered_keys = _load_filtered_keys(results_dir)

    # Pre-group input_df for O(1) candidate lookups
    group_cols = config.false_negative_group_cols
    candidate_groups = {k: v for k, v in input_df.groupby(group_cols)}

    # For each singleton, look for plausible matches in other providers
    suspicious_pairs = []
    singleton_sample = singletons.sample(
        n=min(sample_n * 5, len(singletons)), random_state=42
    )

    for _, singleton in singleton_sample.iterrows():
        s_provider = singleton["source_name"]
        s_last = singleton.get("last_name")

        if pd.isna(s_last):
            continue

        try:
            lookup_key_parts = []
            for col in group_cols:
                val = singleton.get(col)
                if pd.isna(val):
                    break
                lookup_key_parts.append(val)
            else:
                for other_provider in providers:
                    if other_provider == s_provider:
                        continue

                    other_key = tuple(
                        other_provider if col == "source_name" else part
                        for col, part in zip(group_cols, lookup_key_parts)
                    )
                    candidates = candidate_groups.get(other_key)
                    if candidates is None:
                        continue

                    for _, cand in candidates.iterrows():
                        if not _name_similar(s_last, cand.get("last_name")):
                            continue

                        first_ok = _name_similar(
                            singleton.get("first_name"),
                            cand.get("first_name"),
                            threshold=0.85,
                        )
                        email_ok = (
                            singleton.get("email")
                            and cand.get("email")
                            and singleton["email"] == cand["email"]
                        )
                        phone_ok = (
                            singleton.get("phone")
                            and cand.get("phone")
                            and singleton["phone"] == cand["phone"]
                        )

                        if not (first_ok or email_ok or phone_ok):
                            continue

                        pair_status = _classify_pair(
                            singleton["unique_id"],
                            cand["unique_id"],
                            pairwise_keys,
                            filtered_keys,
                        )

                        row = {"pair_status": pair_status}
                        for col in config.audit_display_columns:
                            row[f"{col}_l"] = singleton.get(col)
                            row[f"{col}_r"] = cand.get(col)
                        suspicious_pairs.append(row)
        except (KeyError, TypeError):
            continue

        if len(suspicious_pairs) >= sample_n:
            break

    out_df = pd.DataFrame(suspicious_pairs[:sample_n])

    if len(out_df) == 0:
        print("\nNo suspicious non-matches found in sampled singletons.")
        return out_df

    out_path = results_dir / "audit_false_negatives.csv"
    out_df.to_csv(out_path, index=False)

    # Diagnostics
    if "pair_status" in out_df.columns:
        status_counts = out_df["pair_status"].value_counts()
        print(f"\nSuspicious non-matches found: {len(out_df)}")
        for status, count in status_counts.items():
            print(f"  {status}: {count}")
    print(f"\nWritten to {out_path}")

    return out_df
