from typing import Callable

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from thefuzz import fuzz, process

# Configuration parameters
DEFAULT_FUZZY_THRESHOLD: int = 85
DEFAULT_TOP_N_MATCHES: int = 1


def get_fuzzy_match_schema(
    candidate_code_field_name: str = "candidate_code",
) -> StructType:
    """
    Returns the schema for fuzzy matching results.

    Args:
        candidate_code_field_name: Name of the candidate code field (e.g. 'br_candidate_code', 'techspeed_candidate_code')
    """
    return StructType(
        [
            StructField(candidate_code_field_name, StringType(), True),
            StructField("fuzzy_matched_hubspot_candidate_code", StringType(), True),
            StructField("fuzzy_match_score", IntegerType(), True),
            StructField("fuzzy_match_rank", IntegerType(), True),
            StructField("fuzzy_matched_hubspot_contact_id", StringType(), True),
            StructField("fuzzy_matched_first_name", StringType(), True),
            StructField("fuzzy_matched_last_name", StringType(), True),
            StructField("fuzzy_matched_state", StringType(), True),
            StructField("fuzzy_matched_office_type", StringType(), True),
        ]
    )


def create_fuzzy_matching_udf(
    hubspot_df: pd.DataFrame,
    candidate_code_field_name: str = "candidate_code",
    hubspot_code_field_name: str = "hs_candidate_code",
    threshold: int = DEFAULT_FUZZY_THRESHOLD,
    top_n: int = DEFAULT_TOP_N_MATCHES,
) -> Callable:
    """
    Creates a pandas UDF for fuzzy matching candidate codes against HubSpot data.

    Args:
        hubspot_df: DataFrame with HubSpot candidates including codes and contact details
        candidate_code_field_name: Name of the candidate code field in the results
        hubspot_code_field_name: Name of the HubSpot code field to match against
        threshold: Minimum similarity score (default: 85)
        top_n: Number of top matches to return per candidate (default: 1)

    Returns:
        Pandas UDF function that can be used in PySpark DataFrame operations
    """
    # Validate inputs
    assert top_n == 1, "Currently only top_n=1 is supported"
    assert (
        hubspot_code_field_name in hubspot_df.columns
    ), f"Column {hubspot_code_field_name} not found in HubSpot DataFrame"

    # Prepare HubSpot data for fuzzy matching
    hubspot_codes = (
        hubspot_df[hubspot_df[hubspot_code_field_name].notna()][hubspot_code_field_name]
        .unique()
        .tolist()
    )

    # Create lookup dictionary for HubSpot data
    hubspot_lookup = (
        hubspot_df.dropna(subset=[hubspot_code_field_name])
        .drop_duplicates(subset=[hubspot_code_field_name])
        .set_index(hubspot_code_field_name)
        .to_dict("index")
    )

    # Get the schema for this specific field name
    schema = get_fuzzy_match_schema(candidate_code_field_name)

    @pandas_udf(returnType=schema)
    def fuzzy_match_udf(candidate_codes: pd.Series) -> pd.DataFrame:
        """
        Performs fuzzy matching for a series of candidate codes.

        Args:
            candidate_codes: Series of candidate codes to match against HubSpot

        Returns:
            DataFrame with fuzzy match results
        """
        fuzzy_results = []

        for candidate_code in candidate_codes:
            if pd.isna(candidate_code):
                # Handle null candidate codes
                fuzzy_results.append(
                    _create_null_result(candidate_code, candidate_code_field_name)
                )
                continue

            # Find fuzzy matches
            matches = process.extract(
                candidate_code, hubspot_codes, scorer=fuzz.ratio, limit=top_n
            )

            # Process matches above threshold
            match_found = False
            for rank, (matched_code, score) in enumerate(matches, 1):
                if score >= threshold:
                    hubspot_data = hubspot_lookup[matched_code]
                    fuzzy_results.append(
                        _create_match_result(
                            candidate_code,
                            matched_code,
                            score,
                            rank,
                            hubspot_data,
                            candidate_code_field_name,
                        )
                    )
                    match_found = True
                    break

            # If no match found above threshold, add null entry
            if not match_found:
                fuzzy_results.append(
                    _create_null_result(candidate_code, candidate_code_field_name)
                )

        return pd.DataFrame(fuzzy_results)

    return fuzzy_match_udf


def _create_match_result(
    candidate_code: str,
    matched_code: str,
    score: int,
    rank: int,
    hubspot_data: dict,
    candidate_code_field_name: str,
) -> dict:
    """Creates a dictionary for a successful fuzzy match result."""
    return {
        candidate_code_field_name: candidate_code,
        "fuzzy_matched_hubspot_candidate_code": matched_code,
        "fuzzy_match_score": score,
        "fuzzy_match_rank": rank,
        "fuzzy_matched_hubspot_contact_id": hubspot_data.get("id"),
        "fuzzy_matched_first_name": hubspot_data.get("properties_firstname"),
        "fuzzy_matched_last_name": hubspot_data.get("properties_lastname"),
        "fuzzy_matched_state": hubspot_data.get("properties_state"),
        "fuzzy_matched_office_type": hubspot_data.get("properties_office_type"),
    }


def _create_null_result(candidate_code: str, candidate_code_field_name: str) -> dict:
    """Creates a dictionary for a null/no-match fuzzy result."""
    return {
        candidate_code_field_name: candidate_code,
        "fuzzy_matched_hubspot_candidate_code": None,
        "fuzzy_match_score": None,
        "fuzzy_match_rank": None,
        "fuzzy_matched_hubspot_contact_id": None,
        "fuzzy_matched_first_name": None,
        "fuzzy_matched_last_name": None,
        "fuzzy_matched_state": None,
        "fuzzy_matched_office_type": None,
    }


def add_match_type_column(
    df, match_indicator_col: str = "fuzzy_matched_hubspot_candidate_code"
):
    """
    Adds a match_type column to the DataFrame based on whether fuzzy matches were found.

    Args:
        df: PySpark DataFrame with fuzzy match results
        match_indicator_col: Column name to use for determining match type

    Returns:
        DataFrame with added match_type column
    """
    from pyspark.sql.functions import col, when

    return df.withColumn(
        "match_type",
        when(col(match_indicator_col).isNotNull(), "fuzzy").otherwise("none"),
    )


def model(dbt, session: SparkSession) -> DataFrame:
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="br_candidate_code",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=True,
        tags=["intermediate", "ballotready", "hubspot", "fuzzy_deduped"],
        create_notebook=True,
    )

    # Get the cleaned BallotReady candidates (filtered for HubSpot upload)
    br_with_contest: DataFrame = dbt.ref("int__ballotready_final_candidacies")
    hubspot_candidate_codes: DataFrame = dbt.ref("int__hubspot_candidacy_codes")

    # Handle incremental logic: Only process new or updated records based on candidacy_updated_at
    if dbt.is_incremental:
        existing_table = session.table(f"{dbt.this}")
        max_updated_at_row = existing_table.agg(
            {"candidacy_updated_at": "max"}
        ).collect()[0]
        max_updated_at = max_updated_at_row[0] if max_updated_at_row else None

        if max_updated_at:
            br_with_contest = br_with_contest.filter(
                col("candidacy_updated_at") > max_updated_at
            )

    # Get HubSpot candidate codes as pandas for fuzzy matching
    hubspot_candidate_codes_pd = hubspot_candidate_codes.toPandas()

    # Create the fuzzy matching UDF using our shared utility
    udf_perform_fuzzy_matching = create_fuzzy_matching_udf(
        hubspot_df=hubspot_candidate_codes_pd,
        candidate_code_field_name="br_candidate_code",
        hubspot_code_field_name="hs_candidate_code",
        threshold=85,
        top_n=1,
    )

    # Apply fuzzy matching to all BR candidates
    fuzzy_results = br_with_contest.withColumn(
        "fuzzy_results", udf_perform_fuzzy_matching(col("br_candidate_code"))
    )

    # Expand the struct fields from fuzzy_results - following TechSpeed pattern
    base_columns = [c for c in fuzzy_results.columns if c != "fuzzy_results"]

    fuzzy_results = fuzzy_results.select(
        *[col(c) for c in base_columns],
        col("fuzzy_results.br_candidate_code").alias("fuzzy_results_br_candidate_code"),
        col("fuzzy_results.fuzzy_matched_hubspot_candidate_code").alias(
            "fuzzy_matched_hubspot_candidate_code"
        ),
        col("fuzzy_results.fuzzy_match_score").alias("fuzzy_match_score"),
        col("fuzzy_results.fuzzy_match_rank").alias("fuzzy_match_rank"),
        col("fuzzy_results.fuzzy_matched_hubspot_contact_id").alias(
            "fuzzy_matched_hubspot_contact_id"
        ),
        col("fuzzy_results.fuzzy_matched_first_name").alias("fuzzy_matched_first_name"),
        col("fuzzy_results.fuzzy_matched_last_name").alias("fuzzy_matched_last_name"),
        col("fuzzy_results.fuzzy_matched_state").alias("fuzzy_matched_state"),
        col("fuzzy_results.fuzzy_matched_office_type").alias(
            "fuzzy_matched_office_type"
        ),
    )

    # Add match type column using shared utility
    fuzzy_results = add_match_type_column(fuzzy_results)

    return fuzzy_results
