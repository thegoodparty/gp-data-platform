import sys
import os

# Add the utils directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'utils'))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from fuzzy_matching_utils import create_fuzzy_matching_udf, add_match_type_column


def model(dbt, session: SparkSession) -> DataFrame:
    dbt.config(
        submission_method="serverless_cluster",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="br_candidate_code", 
        on_schema_change="append_new_columns",
        auto_liquid_cluster=True,
        tags=["intermediate", "ballotready", "hubspot", "fuzzy_deduped"],
        create_notebook=True
    )

    # Get the cleaned BallotReady candidates (filtered for HubSpot upload)
    br_with_contest: DataFrame = dbt.ref("br_with_contest")
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
        top_n=1
    )
    
    # Apply fuzzy matching to all BR candidates
    fuzzy_results = br_with_contest.withColumn(
        "fuzzy_results", 
        udf_perform_fuzzy_matching(col("br_candidate_code"))
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