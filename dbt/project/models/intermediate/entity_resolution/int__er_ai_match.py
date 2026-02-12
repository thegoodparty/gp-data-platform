import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    expr,
    lit,
    udf,
)
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window


def cosine_similarity_udf():
    """UDF to compute cosine similarity between two float arrays."""

    @udf(returnType=FloatType())
    def cosine_sim(vec_a, vec_b):
        if vec_a is None or vec_b is None:
            return None
        a = np.array(vec_a, dtype=np.float32)
        b = np.array(vec_b, dtype=np.float32)
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return float(np.dot(a, b) / (norm_a * norm_b))

    return cosine_sim


def model(dbt, session: SparkSession) -> DataFrame:
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="table",
        auto_liquid_cluster=True,
        tags=["intermediate", "entity_resolution"],
    )

    # Load inputs
    embeddings: DataFrame = dbt.ref("int__er_embedding_store")
    deterministic_matches: DataFrame = dbt.ref("int__er_deterministic_match")
    splink_matches: DataFrame = dbt.ref("int__er_splink_match")
    features: DataFrame = dbt.ref("int__er_match_features_union")

    # Collect already-matched record IDs from both prior passes
    matched_ts = (
        deterministic_matches.select("ts_record_id")
        .union(splink_matches.select("ts_record_id"))
        .distinct()
    )
    matched_br = (
        deterministic_matches.select("br_record_id")
        .union(splink_matches.select("br_record_id"))
        .distinct()
    )

    # Filter embeddings to unmatched records only
    ts_embeddings = (
        embeddings.filter(col("source_system") == "techspeed")
        .join(
            matched_ts,
            embeddings["record_id"] == matched_ts["ts_record_id"],
            "left_anti",
        )
        .alias("ts")
    )
    br_embeddings = (
        embeddings.filter(col("source_system") == "ballotready")
        .join(
            matched_br,
            embeddings["record_id"] == matched_br["br_record_id"],
            "left_anti",
        )
        .alias("br")
    )

    if ts_embeddings.count() == 0 or br_embeddings.count() == 0:
        return session.createDataFrame(
            [],
            schema="ts_record_id string, br_record_id string, "
            "ai_similarity_score double, decided_by string",
        )

    # Get state_abbr and election_year from features for blocking
    ts_features = features.filter(col("source_system") == "techspeed").select(
        col("record_id").alias("ts_feat_id"),
        col("state_abbr").alias("ts_state"),
        col("election_year").alias("ts_year"),
    )
    br_features = features.filter(col("source_system") == "ballotready").select(
        col("record_id").alias("br_feat_id"),
        col("state_abbr").alias("br_state"),
        col("election_year").alias("br_year"),
    )

    # Join features onto embeddings for blocking
    ts_with_feats = ts_embeddings.join(
        ts_features, col("ts.record_id") == col("ts_feat_id"), "inner"
    ).select(
        col("ts.record_id").alias("ts_record_id"),
        col("ts.embedding").alias("ts_embedding"),
        col("ts_state"),
        col("ts_year"),
    )

    br_with_feats = br_embeddings.join(
        br_features, col("br.record_id") == col("br_feat_id"), "inner"
    ).select(
        col("br.record_id").alias("br_record_id"),
        col("br.embedding").alias("br_embedding"),
        col("br_state"),
        col("br_year"),
    )

    # Block on state + election_year to limit cross-join
    candidates = ts_with_feats.join(
        br_with_feats,
        (col("ts_state") == col("br_state")) & (col("ts_year") == col("br_year")),
        "inner",
    )

    # Compute cosine similarity
    cosine_sim = cosine_similarity_udf()
    scored = candidates.withColumn(
        "ai_similarity_score",
        cosine_sim(col("ts_embedding"), col("br_embedding")).cast("double"),
    )

    # Filter by threshold
    matches = scored.filter(col("ai_similarity_score") >= 0.90)

    if matches.count() == 0:
        return session.createDataFrame(
            [],
            schema="ts_record_id string, br_record_id string, "
            "ai_similarity_score double, decided_by string",
        )

    # 1:1 enforcement: keep highest similarity per side
    ts_window = Window.partitionBy("ts_record_id").orderBy(
        col("ai_similarity_score").desc()
    )
    br_window = Window.partitionBy("br_record_id").orderBy(
        col("ai_similarity_score").desc()
    )

    matches = matches.withColumn("ts_rank", expr("row_number()").over(ts_window))
    matches = matches.filter(col("ts_rank") == 1).drop("ts_rank")

    matches = matches.withColumn("br_rank", expr("row_number()").over(br_window))
    matches = matches.filter(col("br_rank") == 1).drop("br_rank")

    return matches.withColumn("decided_by", lit("ai_embedding")).select(
        "ts_record_id",
        "br_record_id",
        "ai_similarity_score",
        "decided_by",
    )
