import re
from typing import Optional, Set

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import max as spark_max
from pyspark.sql.types import StringType


def _parse_state_allowlist(raw: Optional[str]) -> Optional[Set[str]]:
    if raw is None:
        return None
    normalized = raw.strip().upper()
    if not normalized:
        return None
    parts = re.split(r"[,\s]+", normalized)
    allowlist = {p for p in parts if p}
    return allowlist or None


def _apply_state_allowlist(df: DataFrame, allowlist: Optional[Set[str]]) -> DataFrame:
    if allowlist is None:
        return df
    return df.filter(col("state_postal_code").isin(sorted(allowlist)))


def model(dbt, session: SparkSession) -> DataFrame:
    """
    Join nationwide L2 uniform data to nationwide Haystaq flags + scores on LALVOTERID.
    """
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="LALVOTERID",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=True,
        tags=[
            "intermediate",
            "l2",
            "nationwide_uniform",
            "uniform",
            "nationwide_haystaq",
            "haystaq",
        ],
    )

    state_allowlist = _parse_state_allowlist(dbt.config.get("l2_state_allowlist"))

    uniform_df: DataFrame = dbt.ref("int__l2_nationwide_uniform").withColumn(
        "LALVOTERID", col("LALVOTERID").cast(StringType())
    )
    flags_df: DataFrame = dbt.ref("int__l2_nationwide_haystaq_flags").withColumn(
        "LALVOTERID", col("LALVOTERID").cast(StringType())
    )
    scores_df: DataFrame = dbt.ref("int__l2_nationwide_haystaq_scores").withColumn(
        "LALVOTERID", col("LALVOTERID").cast(StringType())
    )

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
            .filter(
                col("max_uniform_loaded_at").isNull()
                | (col("loaded_at") > col("max_uniform_loaded_at"))
            )
            .select("LALVOTERID")
        )
        flags_updates = (
            flags_df.join(thresholds, on="state_postal_code", how="left")
            .filter(
                col("max_flags_loaded_at").isNull()
                | (col("loaded_at") > col("max_flags_loaded_at"))
            )
            .select("LALVOTERID")
        )
        scores_updates = (
            scores_df.join(thresholds, on="state_postal_code", how="left")
            .filter(
                col("max_scores_loaded_at").isNull()
                | (col("loaded_at") > col("max_scores_loaded_at"))
            )
            .select("LALVOTERID")
        )

        changed_ids = (
            uniform_updates.union(flags_updates).union(scores_updates).distinct()
        )

        if not changed_ids.take(1):
            return (
                uniform_df.limit(0)
                .join(flags_selected.limit(0), on="LALVOTERID", how="left")
                .join(scores_selected.limit(0), on="LALVOTERID", how="left")
            )

        uniform_df = uniform_df.join(changed_ids, on="LALVOTERID", how="inner")
        flags_selected = flags_selected.join(changed_ids, on="LALVOTERID", how="inner")
        scores_selected = scores_selected.join(
            changed_ids, on="LALVOTERID", how="inner"
        )

    return uniform_df.join(flags_selected, on="LALVOTERID", how="left").join(
        scores_selected, on="LALVOTERID", how="left"
    )
