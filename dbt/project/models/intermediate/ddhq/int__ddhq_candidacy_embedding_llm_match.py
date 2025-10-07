import logging
from ast import List

import faiss
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
def _normalize_vector(vec: List) -> List:
    """Normalize a vector to unit length (L2 norm). Input and output are lists of floats."""
    vec_array = np.array(vec)
    norm = np.linalg.norm(vec_array).tolist()
    if norm == 0:
        return np.zeros_like(vec_array, dtype=np.float32).tolist()
    return (vec_array / norm).astype(np.float32).tolist()


@udf(returnType=FloatType())
def _dot_product(vec1: List, vec2: List) -> float:
    """Compute dot product between two normalized vectors (cosine similarity)."""
    vec1_array = np.array(vec1)
    vec2_array = np.array(vec2)
    return float(np.dot(vec1_array, vec2_array))


@udf(returnType=FloatType())
def _cosine_similarity(vec1: list, vec2: list) -> float:
    """Compute cosine similarity between two vectors."""
    if not vec1 or not vec2 or len(vec1) != len(vec2):
        return 0.0
    vec1_array = np.array(vec1, dtype=np.float32)
    vec2_array = np.array(vec2, dtype=np.float32)
    norm1 = np.linalg.norm(vec1_array)
    norm2 = np.linalg.norm(vec2_array)
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return float(np.dot(vec1_array, vec2_array) / (norm1 * norm2))


def _faiss_similarity_search(
    table_a: DataFrame,  # HubSpot (database vectors)
    table_b: DataFrame,  # DDHQ (query vectors)
    vector_col_a: str = "hubspot_embedding_normalized",
    vector_col_b: str = "ddhq_embedding_normalized",
    id_col_a: str = "hubspot_name_race",
    id_col_b: str = "ddhq_name_race",
    original_vector_col_a: str = "hubspot_embedding",  # If you want to return originals
    original_vector_col_b: str = "ddhq_embedding",
    use_approximate: bool = True,  # Toggle exact vs. approx
    nlist: int = 245,  # For approx: ~sqrt(N)
    nprobe: int = 10,  # For approx: start with 10-20
) -> DataFrame:
    """
    Perform similarity search using FAISS for cosine similarity on normalized vectors.
    Returns the closest vector_a for each vector_b.
    """
    # Collect HubSpot data (database) to driver
    a_rows = table_a.select(id_col_a, vector_col_a, original_vector_col_a).collect()
    a_ids = [row[id_col_a] for row in a_rows]
    a_vectors = np.array([row[vector_col_a] for row in a_rows], dtype=np.float32)
    a_original_vectors = [
        row[original_vector_col_a] for row in a_rows
    ]  # List for later

    # Collect DDHQ data (queries) to driver
    b_rows = table_b.select(id_col_b, vector_col_b, original_vector_col_b).collect()
    b_ids = [row[id_col_b] for row in b_rows]
    b_vectors = np.array([row[vector_col_b] for row in b_rows], dtype=np.float32)
    b_original_vectors = [
        row[original_vector_col_b] for row in b_rows
    ]  # List for later

    # Ensure normalization (idempotent if already normalized)
    faiss.normalize_L2(a_vectors)
    faiss.normalize_L2(b_vectors)

    d = a_vectors.shape[1]  # Dimension (~3000)

    if use_approximate:
        # Approximate: IVFFlat for faster search
        quantizer = faiss.IndexFlatIP(d)
        index = faiss.IndexIVFFlat(quantizer, d, nlist, faiss.METRIC_INNER_PRODUCT)
        index.train(a_vectors)  # Train on database vectors
        index.add(a_vectors)
        index.nprobe = nprobe  # Tune for accuracy vs. speed
    else:
        # Exact: FlatIP for brute-force
        index = faiss.IndexFlatIP(d)
        index.add(a_vectors)

    # Search for top-1 nearest neighbor (k=1)
    k = 1
    distances, indexes = index.search(b_vectors, k)  # D: similarities, I: indices

    # Assemble results
    results = []
    for i in range(len(b_ids)):
        match_idx = indexes[i][0]
        similarity = float(distances[i][0])
        results.append(
            {
                "id_a": a_ids[match_idx],
                "vector_a": a_original_vectors[match_idx],  # Original HubSpot vector
                "id_b": b_ids[i],
                "vector_b": b_original_vectors[i],  # Original DDHQ vector
                "similarity": similarity,
            }
        )

    # Convert back to Spark DataFrame
    result_df = table_a.sparkSession.createDataFrame(
        results
    )  # Use session from params if needed

    return result_df


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
    hubspot_data = hubspot_data.limit(10000)
    ddhq_data = ddhq_data.limit(10000)

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
        col("name_race").alias("hubspot_name_race"),
        col("name_race_embedding").alias("hubspot_embedding"),
    )

    # Select DDHQ columns for processing
    ddhq_clean = ddhq_data.select(
        col("candidate").alias("ddhq_candidate"),
        col("race_name").alias("ddhq_race_name"),
        col("candidate_party").alias("ddhq_candidate_party"),
        col("is_winner").alias("ddhq_is_winner"),
        col("race_id").alias("ddhq_race_id"),
        col("candidate_id").alias("ddhq_candidate_id"),
        col("election_type").alias("ddhq_election_type"),
        col("date").alias("ddhq_date"),
        col("is_uncontested").alias("ddhq_is_uncontested"),
        col("name_race_embedding").alias("ddhq_embedding"),
        col("name_race").alias("ddhq_name_race"),
    )

    # Normalize vectors in both tables
    hubspot_normalized = hubspot_clean.withColumn(
        "hubspot_embedding_normalized", _normalize_vector(col("hubspot_embedding"))
    ).select(
        col("hubspot_gp_candidacy_id"),
        col("hubspot_embedding"),
        col("hubspot_embedding_normalized"),
        col("hubspot_name_race"),
    )
    ddhq_normalized = ddhq_clean.withColumn(
        "ddhq_embedding_normalized", _normalize_vector(col("ddhq_embedding"))
    ).select(
        col("ddhq_candidate"),
        col("ddhq_race_id"),
        col("ddhq_embedding"),
        col("ddhq_embedding_normalized"),
        col("ddhq_name_race"),
    )

    similarity_results = _faiss_similarity_search(
        table_a=hubspot_normalized,
        table_b=ddhq_normalized,
        vector_col_a="hubspot_embedding_normalized",
        vector_col_b="ddhq_embedding_normalized",
        id_col_a="hubspot_name_race",
        id_col_b="ddhq_name_race",
        original_vector_col_a="hubspot_embedding",
        original_vector_col_b="ddhq_embedding",
        use_approximate=True,
        nlist=245,
        nprobe=10,
    )

    return similarity_results

    # hubspot_normalized = hubspot_normalized.withColumn(
    #     "hubspot_embedding_cosine_similarity",
    #     _cosine_similarity(
    #         col("hubspot_embedding"), col("hubspot_embedding_normalized")
    #     ),
    # ).select(
    #     col("hubspot_gp_candidacy_id"),
    #     col("hubspot_embedding"),
    #     col("hubspot_embedding_normalized"),
    #     col("hubspot_embedding_cosine_similarity"),
    # )
    # return hubspot_normalized

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
