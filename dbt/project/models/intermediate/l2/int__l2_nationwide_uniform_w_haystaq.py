import re
from functools import reduce
from operator import or_

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import max as spark_max
from pyspark.sql.types import StringType


def _parse_state_allowlist(raw: str | None) -> set[str] | None:
    if raw is None:
        return None
    normalized = raw.strip().upper()
    if not normalized:
        return None
    parts = re.split(r"[,\s]+", normalized)
    allowlist = {p for p in parts if p}
    return allowlist or None


def _apply_state_allowlist(df: DataFrame, allowlist: set[str] | None) -> DataFrame:
    if allowlist is None:
        return df
    return df.filter(col("state_postal_code").isin(sorted(allowlist)))


def _seed_assignment_updates(
    uniform_df: DataFrame, this_df: DataFrame, assignments_df: DataFrame
) -> DataFrame:
    """LALVOTERIDs carrying a seeded district value this table has not caught up to.

    l2_manual_district_assignments reaches voters through a coalesce in
    int__l2_nationwide_uniform, a view, so editing the seed moves no voter's
    loaded_at and the loaded_at thresholds never revisit those rows. Without this
    leg an assignment is stranded until the state's next L2 delivery, and this
    table cannot be full refreshed to recover. int__l2_district_aggregations
    unions the seed into its candidate set for the same reason.

    Matching on the district value rather than the seed's sparse
    county/city/precinct tuple keeps that match in one place; re-emitting a voter
    L2 already agreed with is a no-op.
    """
    assignments = assignments_df.select("state", "l2_district_type", "l2_district_name").distinct().collect()
    if not assignments:
        return uniform_df.select("LALVOTERID").limit(0)

    assigned = uniform_df.filter(
        reduce(
            or_,
            [
                (col("state_postal_code") == row["state"])
                & (col(row["l2_district_type"]) == row["l2_district_name"])
                for row in assignments
            ],
        )
    )

    district_columns = sorted({row["l2_district_type"] for row in assignments})
    stored_columns = {c.lower() for c in this_df.columns}
    if not {c.lower() for c in district_columns}.issubset(stored_columns):
        # A district type this table has never carried, so every match is behind.
        return assigned.select("LALVOTERID")

    # Both sides are already confined to the seeded states, and the table clusters
    # on state_postal_code, so the comparison prunes to those files.
    stored = this_df.filter(col("state_postal_code").isin(sorted({row["state"] for row in assignments})))

    # subtract compares the whole tuple null-safely, so a run where the seed has
    # already landed everywhere emits nothing and the merge stays a no-op.
    return (
        assigned.select("LALVOTERID", *district_columns)
        .subtract(stored.select("LALVOTERID", *district_columns))
        .select("LALVOTERID")
    )


def model(dbt, session: SparkSession) -> DataFrame:
    """
    Join nationwide L2 uniform data to nationwide Haystaq flags + scores on LALVOTERID.
    """
    dbt.config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="LALVOTERID",
        on_schema_change="append_new_columns",
        # The prod table carries deliberate hierarchical liquid clustering
        # (delta.liquid.hierarchicalClusteringColumns = voters_active,
        # state_postal_code, set 2026-07-28 for serving-read performance).
        # auto_liquid_cluster would issue ALTER TABLE CLUSTER BY AUTO, which
        # Delta rejects against that property and fails the whole merge.
        auto_liquid_cluster=False,
        # A full refresh would rebuild without the retired vendor columns this
        # table has accumulated (append_new_columns never drops), which the
        # agent voter views still project by name, and would also wipe the
        # clustering property above. Pin it off until the retired-column
        # cleanup retargets the views first; that cleanup flips this
        # deliberately.
        full_refresh=False,
        tags=[
            "intermediate",
            "l2",
            "nationwide_uniform",
            "uniform",
            "nationwide_haystaq",
            "haystaq",
        ],
    )

    state_allowlist = _parse_state_allowlist(dbt.config.meta_get("l2_state_allowlist"))

    uniform_df: DataFrame = dbt.ref("int__l2_nationwide_uniform").withColumn(
        "LALVOTERID", col("LALVOTERID").cast(StringType())
    )
    flags_df: DataFrame = dbt.ref("int__l2_nationwide_haystaq_flags").withColumn(
        "LALVOTERID", col("LALVOTERID").cast(StringType())
    )
    scores_df: DataFrame = dbt.ref("int__l2_nationwide_haystaq_scores").withColumn(
        "LALVOTERID", col("LALVOTERID").cast(StringType())
    )
    assignments_df: DataFrame = dbt.ref("l2_manual_district_assignments")

    uniform_df = _apply_state_allowlist(uniform_df, state_allowlist)
    flags_df = _apply_state_allowlist(flags_df, state_allowlist)
    scores_df = _apply_state_allowlist(scores_df, state_allowlist)

    hf_columns = [c for c in flags_df.columns if c.startswith("hf_")]
    hs_columns = [c for c in scores_df.columns if c.startswith("hs_")]

    flags_selected = flags_df.select(
        col("LALVOTERID"),
        col("loaded_at").alias("haystaq_flags_loaded_at"),
        *[col(c) for c in hf_columns],
    )
    scores_selected = scores_df.select(
        col("LALVOTERID"),
        col("loaded_at").alias("haystaq_scores_loaded_at"),
        *[col(c) for c in hs_columns],
    )

    if dbt.is_incremental:
        this_df = session.table(f"{dbt.this}")
        this_df = _apply_state_allowlist(this_df, state_allowlist)

        thresholds = this_df.groupBy("state_postal_code").agg(
            spark_max("loaded_at").alias("max_uniform_loaded_at"),
            spark_max("haystaq_flags_loaded_at").alias("max_flags_loaded_at"),
            spark_max("haystaq_scores_loaded_at").alias("max_scores_loaded_at"),
        )

        uniform_updates = (
            uniform_df.join(thresholds, on="state_postal_code", how="left")
            .filter(col("max_uniform_loaded_at").isNull() | (col("loaded_at") > col("max_uniform_loaded_at")))
            .select("LALVOTERID")
        )
        flags_updates = (
            flags_df.join(thresholds, on="state_postal_code", how="left")
            .filter(col("max_flags_loaded_at").isNull() | (col("loaded_at") > col("max_flags_loaded_at")))
            .select("LALVOTERID")
        )
        scores_updates = (
            scores_df.join(thresholds, on="state_postal_code", how="left")
            .filter(col("max_scores_loaded_at").isNull() | (col("loaded_at") > col("max_scores_loaded_at")))
            .select("LALVOTERID")
        )

        seed_updates = _seed_assignment_updates(uniform_df, this_df, assignments_df)

        changed_ids = (
            uniform_updates.union(flags_updates).union(scores_updates).union(seed_updates).distinct()
        )

        if not changed_ids.take(1):
            return (
                uniform_df.limit(0)
                .join(flags_selected.limit(0), on="LALVOTERID", how="left")
                .join(scores_selected.limit(0), on="LALVOTERID", how="left")
            )

        uniform_df = uniform_df.join(changed_ids, on="LALVOTERID", how="inner")
        flags_selected = flags_selected.join(changed_ids, on="LALVOTERID", how="inner")
        scores_selected = scores_selected.join(changed_ids, on="LALVOTERID", how="inner")

    return uniform_df.join(flags_selected, on="LALVOTERID", how="left").join(
        scores_selected, on="LALVOTERID", how="left"
    )
