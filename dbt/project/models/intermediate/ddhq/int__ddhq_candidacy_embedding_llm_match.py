import logging
from ast import List

import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, DoubleType, FloatType

# Configure logging for the module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create console handler if not exists
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


@udf(returnType=ArrayType(DoubleType()))
def normalize_vector(vec: List) -> List:
    """Normalize a vector to unit length (L2 norm). Input and output are lists of floats."""
    vec_array = np.array(vec)
    norm = np.linalg.norm(vec_array).tolist()
    if norm == 0:
        return np.zeros_like(vec_array).tolist()  # Return zero vector if norm is zero
    return (vec_array / norm).tolist()


@udf(returnType=FloatType())
def cosine_similarity(vec1: List, vec2: List) -> float:
    """Compute dot product between two normalized vectors (cosine similarity)."""
    vec1_array = np.array(vec1)
    vec2_array = np.array(vec2)
    return float(
        np.dot(vec1_array, vec2_array)
        / (np.linalg.norm(vec1_array) * np.linalg.norm(vec2_array))
    )


def model(dbt, session: SparkSession) -> DataFrame:
    """
    Main dbt model function that processes HubSpot and DDHQ data for candidate matching.

    This model:
    1. Loads HubSpot candidacy data with embeddings
    2. Loads DDHQ election results data with embeddings
    3. Uses Spark LSH for approximate vector similarity search
    4. Converts cosine similarity to confidence scores
    5. Ranks matches and selects the best ones
    6. Returns matched candidates with confidence scores
    """
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        unique_key=["gp_candidacy_id", "election_date", "election_type"],
        incremental_strategy="merge",
        on_schema_change="append_new_columns",
        tags=["intermediate", "ddhq", "candidacy", "embedding", "llm", "match"],
    )

    # Get configuration values
    confidence_threshold = int(dbt.config.get("confidence_threshold", 70))
    max_matches_per_candidate = int(dbt.config.get("max_matches_per_candidate", 10))
    similarity_threshold = float(
        dbt.config.get("similarity_threshold", 0.632)
    )  # LSH threshold for cosine sim > 0.8

    logger.info(
        f"Starting DDHQ candidacy matching with confidence_threshold={confidence_threshold}, max_matches_per_candidate={max_matches_per_candidate}, similarity_threshold={similarity_threshold}"
    )

    # Load source data
    hubspot_data: DataFrame = dbt.ref("int__general_candidacy_embeddings_for_ddhq")
    ddhq_data: DataFrame = dbt.ref("int__ddhq_election_results_embeddings")

    # Downsample during dev
    hubspot_data = hubspot_data.limit(100)
    ddhq_data = ddhq_data.limit(1000)

    # Select HubSpot columns for processing
    hubspot_clean = hubspot_data.select(
        col("gp_candidacy_id").alias("hubspot_gp_candidacy_id"),
        col("candidacy_id").alias("hubspot_candidacy_id"),
        col("first_name").alias("hubspot_first_name"),
        col("last_name").alias("hubspot_last_name"),
        col("state").alias("hubspot_state"),
        col("city").alias("hubspot_city"),
        col("candidate_office").alias("hubspot_candidate_office"),
        col("official_office_name").alias("hubspot_official_office_name"),
        col("party_affiliation").alias("hubspot_party_affiliation"),
        col("election_date").alias("hubspot_election_date"),
        col("election_type").alias("hubspot_election_type"),
        col("is_uncontested").alias("hubspot_is_uncontested"),
        col("name_race_embedding").alias("hubspot_embedding"),
    )

    # # Select DDHQ columns for processing
    # ddhq_clean = ddhq_data.select(
    #     col("candidate").alias("ddhq_candidate"),
    #     col("race_name").alias("ddhq_race_name"),
    #     col("candidate_party").alias("ddhq_candidate_party"),
    #     col("is_winner").alias("ddhq_is_winner"),
    #     col("race_id").alias("ddhq_race_id"),
    #     col("candidate_id").alias("ddhq_candidate_id"),
    #     col("election_type").alias("ddhq_election_type"),
    #     col("date").alias("ddhq_date"),
    #     col("is_uncontested").alias("ddhq_is_uncontested"),
    #     col("name_race_embedding").alias("ddhq_embedding"),
    # )

    # Normalize vectors in both tables
    hubspot_normalized = hubspot_clean.withColumn(
        "hubspot_embedding_normalized", normalize_vector(col("hubspot_embedding"))
    ).select(
        col("hubspot_gp_candidacy_id"),
        col("hubspot_embedding"),
        col("hubspot_embedding_normalized"),
    )

    hubspot_normalized = hubspot_normalized.withColumn(
        "hubspot_embedding_cosine_similarity",
        cosine_similarity(
            col("hubspot_embedding"), col("hubspot_embedding_normalized")
        ),
    ).select(
        col("hubspot_gp_candidacy_id"),
        col("hubspot_embedding"),
        col("hubspot_embedding_normalized"),
        col("hubspot_embedding_cosine_similarity"),
    )

    # ddhq_normalized = ddhq_clean.withColumn(
    #     "ddhq_embedding_normalized",
    #     normalize_vector(col("ddhq_embedding"))
    # ).select(ddhq_clean["ddhq_race_id"], ddhq_clean["ddhq_embedding"], "ddhq_embedding_normalized")

    return hubspot_normalized
    # # Select and rename columns for clarity
    # logger.info("Selecting and renaming columns")
    # result_columns = filtered_matches.select(
    #     col("datasetA.hubspot_gp_candidacy_id").alias("hubspot_gp_candidacy_id"),
    #     col("datasetA.hubspot_candidacy_id").alias("hubspot_candidacy_id"),
    #     col("datasetA.hubspot_first_name").alias("hubspot_first_name"),
    #     col("datasetA.hubspot_last_name").alias("hubspot_last_name"),
    #     col("datasetA.hubspot_full_name").alias("hubspot_full_name"),
    #     col("datasetA.hubspot_state").alias("hubspot_state"),
    #     col("datasetA.hubspot_city").alias("hubspot_city"),
    #     col("datasetA.hubspot_candidate_office").alias("hubspot_candidate_office"),
    #     col("datasetA.hubspot_official_office_name").alias("hubspot_official_office_name"),
    #     col("datasetA.hubspot_party_affiliation").alias("hubspot_party_affiliation"),
    #     col("datasetA.hubspot_election_date").alias("hubspot_election_date"),
    #     col("datasetA.hubspot_election_type").alias("hubspot_election_type"),
    #     col("datasetA.hubspot_is_uncontested").alias("hubspot_is_uncontested"),
    #     col("datasetB.ddhq_candidate").alias("ddhq_candidate"),
    #     col("datasetB.ddhq_race_name").alias("ddhq_race_name"),
    #     col("datasetB.ddhq_candidate_party").alias("ddhq_candidate_party"),
    #     col("datasetB.ddhq_is_winner").alias("ddhq_is_winner"),
    #     col("datasetB.ddhq_race_id").alias("ddhq_race_id"),
    #     col("datasetB.ddhq_candidate_id").alias("ddhq_candidate_id"),
    #     col("datasetB.ddhq_election_type").alias("ddhq_election_type"),
    #     col("datasetB.ddhq_date").alias("ddhq_date"),
    #     col("datasetB.ddhq_is_uncontested").alias("ddhq_is_uncontested"),
    #     col("similarity_score").alias("match_similarity"),
    #     col("confidence_score").alias("llm_confidence"),
    #     lit("LSH similarity").alias("llm_reasoning")
    # )

    # # Rank matches for each HubSpot candidate
    # logger.info("Ranking matches per candidate")
    # window_spec = Window.partitionBy("hubspot_gp_candidacy_id").orderBy(col("llm_confidence").desc())
    # ranked_matches = result_columns.withColumn(
    #     "match_rank",
    #     row_number().over(window_spec)
    # )

    # # Select top matches per candidate
    # top_matches = ranked_matches.filter(
    #     col("match_rank") <= max_matches_per_candidate
    # )

    # # Add match indicators
    # final_results = top_matches.withColumn(
    #     "has_match",
    #     when(col("match_rank") == 1, True).otherwise(False)
    # ).withColumn(
    #     "is_best_match",
    #     when(col("match_rank") == 1, True).otherwise(False)
    # ).withColumn(
    #     "llm_best_match",
    #     col("match_rank")
    # ).withColumn(
    #     "ddhq_matched_index",
    #     col("match_rank")
    # )

    # # Select final columns in the expected order
    # final_results = final_results.select(
    #     col("hubspot_gp_candidacy_id"),
    #     col("hubspot_candidacy_id"),
    #     col("hubspot_first_name"),
    #     col("hubspot_last_name"),
    #     col("hubspot_full_name"),
    #     col("hubspot_state"),
    #     col("hubspot_city"),
    #     col("hubspot_candidate_office"),
    #     col("hubspot_official_office_name"),
    #     col("hubspot_party_affiliation"),
    #     col("hubspot_election_date"),
    #     col("hubspot_election_type"),
    #     col("hubspot_is_uncontested"),
    #     col("llm_best_match"),
    #     col("llm_confidence"),
    #     col("llm_reasoning"),
    #     lit("").alias("top_10_candidates"),  # Placeholder for compatibility
    #     col("has_match"),
    #     col("ddhq_matched_index"),
    #     col("ddhq_candidate"),
    #     col("ddhq_race_name"),
    #     col("ddhq_candidate_party"),
    #     col("ddhq_is_winner"),
    #     col("ddhq_race_id"),
    #     col("ddhq_candidate_id"),
    #     col("ddhq_election_type"),
    #     col("ddhq_date"),
    #     col("ddhq_is_uncontested"),
    #     col("match_similarity")
    # )

    # logger.info(f"Generated {final_results.count()} matches using LSH approximate similarity search")

    # return final_results
