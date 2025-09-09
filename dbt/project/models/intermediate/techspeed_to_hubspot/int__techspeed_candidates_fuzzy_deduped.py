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

FUZZ_MATCH_SCHEMA = StructType(
    [
        StructField("techspeed_candidate_code", StringType(), True),
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

# Configuration parameters
FUZZY_THRESHOLD: int = 85  # Default threshold for fuzzy matching
TOP_N_MATCHES: int = 1  # Default number of matches to return per candidate.


def wrap_perform_fuzzy_matching(
    hubspot_df: pd.DataFrame,
    threshold: int = FUZZY_THRESHOLD,
    top_n: int = TOP_N_MATCHES,
) -> Callable:
    """
    Wraps the perform_fuzzy_matching function in a callable that can be used in a pandas UDF.
    The wrapper is necessary to pass in threshold and top_n to the UDF.
    """

    @pandas_udf(returnType=FUZZ_MATCH_SCHEMA)
    def perform_fuzzy_matching(techspeed_codes: pd.Series) -> pd.DataFrame:
        """
        Perform fuzzy matching between BR candidate codes and HubSpot candidate codes

        Args:
            techspeed_df: DataFrame with TechSpeed candidates
            hubspot_df: DataFrame with HubSpot candidates
            threshold: Minimum similarity score (default: 85)
            top_n: Number of top matches to return per candidate (default: 1)

        Returns:
            DataFrame with fuzzy match results
        """
        # TODO: unneeded in practice, but possibly useful later: add support for multiple matches
        # this would rqeuire the output schema to hold all the results in a list of dictionaries.
        # the corresponding pyspark schema would be an array of structs.
        assert top_n == 1, "top_n must be 1"

        # Prepare HubSpot data for fuzzy matching
        hubspot_codes = (
            hubspot_df[hubspot_df["hubspot_candidate_code"].notna()][
                "hubspot_candidate_code"
            ]
            .unique()
            .tolist()
        )

        # Create lookup dictionary for HubSpot data
        hubspot_lookup = (
            hubspot_df.dropna(subset=["hubspot_candidate_code"])
            .drop_duplicates(subset=["hubspot_candidate_code"])
            .set_index("hubspot_candidate_code")
            .to_dict("index")
        )

        fuzzy_results = []

        # Process each BR candidate with progress bar
        # for _, row in techspeed_codes.iterrows():
        #     techspeed_code = row.get("techspeed_candidate_code")
        for techspeed_code in techspeed_codes:

            if pd.isna(techspeed_code):
                continue

            # Find fuzzy matches
            matches = process.extract(
                techspeed_code, hubspot_codes, scorer=fuzz.ratio, limit=top_n
            )

            # Filter by threshold and add to results
            for rank, (matched_code, score) in enumerate(matches, 1):
                if score >= threshold:
                    hubspot_data = hubspot_lookup[matched_code]

                    fuzzy_results.append(
                        {
                            "techspeed_candidate_code": techspeed_code,
                            "fuzzy_matched_hubspot_candidate_code": matched_code,
                            "fuzzy_match_score": score,
                            "fuzzy_match_rank": rank,
                            "fuzzy_matched_hubspot_contact_id": hubspot_data.get("id"),
                            "fuzzy_matched_first_name": hubspot_data.get(
                                "properties_firstname"
                            ),
                            "fuzzy_matched_last_name": hubspot_data.get(
                                "properties_lastname"
                            ),
                            "fuzzy_matched_state": hubspot_data.get("properties_state"),
                            "fuzzy_matched_office_type": hubspot_data.get(
                                "properties_office_type"
                            ),
                        }
                    )
                else:
                    fuzzy_results.append(
                        {
                            "techspeed_candidate_code": techspeed_code,
                            "fuzzy_matched_hubspot_candidate_code": None,
                            "fuzzy_match_score": None,
                            "fuzzy_match_rank": None,
                            "fuzzy_matched_hubspot_contact_id": None,
                            "fuzzy_matched_first_name": None,
                            "fuzzy_matched_last_name": None,
                            "fuzzy_matched_state": None,
                            "fuzzy_matched_office_type": None,
                        }
                    )

        return pd.DataFrame(fuzzy_results)

    return perform_fuzzy_matching


def model(dbt, session: SparkSession) -> DataFrame:
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="techspeed_candidate_code",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=True,
        tags=["intermediate", "techspeed", "hubspot", "fuzzy_deduped"],
    )

    techspeed_candidates_w_hubspot: DataFrame = dbt.ref(
        "int__techspeed_candidates_w_hubspot"
    )
    hubspot_candidate_codes: DataFrame = dbt.ref("int__hubspot_candidate_codes")

    # Handle incremental logic: Only process new or updated records based on _airbyte_extracted_at
    if dbt.is_incremental:
        existing_table = session.table(f"{dbt.this}")
        max_extracted_at_row = existing_table.agg(
            {"_airbyte_extracted_at": "max"}
        ).collect()[0]
        max_extracted_at = max_extracted_at_row[0] if max_extracted_at_row else None

        if max_extracted_at:
            techspeed_candidates_w_hubspot = techspeed_candidates_w_hubspot.filter(
                col("_airbyte_extracted_at") > max_extracted_at
            )

    hubspot_candidate_codes = hubspot_candidate_codes.toPandas()

    udf_perform_fuzzy_matching = wrap_perform_fuzzy_matching(
        hubspot_candidate_codes, threshold=FUZZY_THRESHOLD, top_n=TOP_N_MATCHES
    )
    fuzzy_results = techspeed_candidates_w_hubspot.withColumn(
        "fuzzy_results", udf_perform_fuzzy_matching(col("techspeed_candidate_code"))
    )

    # Select all original columns except fuzzy_results, and expand the struct fields from fuzzy_result
    # (Assume all columns from techspeed_candidates_w_hubspot are needed, except fuzzy_results)
    base_columns = [c for c in fuzzy_results.columns if c != "fuzzy_results"]

    fuzzy_results = fuzzy_results.select(
        *[col(c) for c in base_columns],
        col("fuzzy_results.techspeed_candidate_code").alias(
            "fuzzy_results_techspeed_candidate_code"
        ),
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

    return fuzzy_results
