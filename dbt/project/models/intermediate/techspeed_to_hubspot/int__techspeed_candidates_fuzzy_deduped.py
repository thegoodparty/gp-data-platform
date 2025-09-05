import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from thefuzz import fuzz, process

THIS_TABLE_SCHEMA = StructType(
    [
        StructField("candidacy_id_source", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("candidate_type", StringType(), True),
        StructField("email", StringType(), True),
        StructField("techspeed_candidate_code", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("candidate_id_tier", StringType(), True),
        StructField("party", StringType(), True),
        StructField("website_url", StringType(), True),
        StructField("linkedin_url", StringType(), True),
        StructField("instagram_handle", StringType(), True),
        StructField("twitter_handle", StringType(), True),
        StructField("facebook_url", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("street_address", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("district", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("official_office_name", StringType(), True),
        StructField("candidate_office", StringType(), True),
        StructField("office_type", StringType(), True),
        StructField("office_level", StringType(), True),
        StructField("filing_deadline", StringType(), True),
        StructField("primary_election_date", StringType(), True),
        StructField("general_election_date", StringType(), True),
        StructField("election_date", StringType(), True),
        StructField("election_type", StringType(), True),
        StructField("uncontested", StringType(), True),
        StructField("number_of_candidates", StringType(), True),
        StructField("number_of_seats_available", StringType(), True),
        StructField("open_seat", StringType(), True),
        StructField("partisan", StringType(), True),
        StructField("population", StringType(), True),
        StructField("ballotready_race_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("contact_owner", StringType(), True),
        StructField("owner_name", StringType(), True),
        StructField("uploaded", StringType(), True),
        StructField("_airbyte_extracted_at", TimestampType(), True),
        StructField("_ab_source_file_url", StringType(), True),
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
TOP_N_MATCHES: int = 1  # Default number of matches to return per candidate


def wrap_perform_fuzzy_matching(
    hubspot_df: pd.DataFrame,
    threshold: int = FUZZY_THRESHOLD,
    top_n: int = TOP_N_MATCHES,
) -> pd.DataFrame:
    """
    Wraps the perform_fuzzy_matching function in a callable that can be used in a pandas UDF.
    The wrapper is necessary to pass in threshold and top_n to the UDF.
    """

    @pandas_udf(returnType=THIS_TABLE_SCHEMA)
    def perform_fuzzy_matching(techspeed_df: pd.DataFrame) -> pd.DataFrame:
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
        # Prepare HubSpot data for fuzzy matching
        hubspot_codes = (
            hubspot_df[hubspot_df["hs_candidate_code"].notna()]["hs_candidate_code"]
            .unique()
            .tolist()
        )

        # Create lookup dictionary for HubSpot data
        hubspot_lookup = (
            hubspot_df.dropna(subset=["hs_candidate_code"])
            .drop_duplicates(subset=["hs_candidate_code"])
            .set_index("hs_candidate_code")
            .to_dict("index")
        )

        fuzzy_results = []

        # Process each BR candidate with progress bar
        for _, row in techspeed_df.iterrows():
            techspeed_code = row.get("techspeed_candidate_code")

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
                            "candidacy_id": row.get("candidacy_id"),
                            "techspeed_candidate_code": techspeed_code,
                            "fuzzy_matched_hs_candidate_code": matched_code,
                            "fuzzy_match_score": score,
                            "fuzzy_match_rank": rank,
                            "fuzzy_matched_hs_contact_id": hubspot_data.get("id"),
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

        return pd.DataFrame(fuzzy_results)

    return perform_fuzzy_matching


def model(dbt, session: SparkSession) -> DataFrame:
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="merge",
        unique_id="techspeed_candidate_code",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=True,
        tags=["intermediate", "techspeed", "hubspot", "fuzzy_deduped"],
    )

    # TODO: handle incremental

    techspeed_candidates_w_hubspot: DataFrame = dbt.ref(
        "int__techspeed_candidates_w_hubspot"
    )
    hubspot_candidate_codes: DataFrame = dbt.ref("int__hubspot_candidate_codes")

    # during dev, downsample
    techspeed_candidates_w_hubspot = techspeed_candidates_w_hubspot.sample(frac=0.1)
    hubspot_candidate_codes = hubspot_candidate_codes.sample(frac=0.1).toPandas()

    udf_perform_fuzzy_matching = wrap_perform_fuzzy_matching(
        hubspot_candidate_codes, threshold=FUZZY_THRESHOLD, top_n=TOP_N_MATCHES
    )
    fuzzy_results = udf_perform_fuzzy_matching(techspeed_candidates_w_hubspot)

    return fuzzy_results
