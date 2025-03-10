import logging
import random
import time
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List

import pandas as pd
import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, lit, pandas_udf
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _base64_encode_id(candidacy_id: str) -> str:
    """
    Encodes a candidacy ID into the format required by CivicEngine API.

    Args:
        candidacy_id: The raw candidacy ID to encode.

    Returns:
        The base64-encoded ID string with the proper prefix.
    """
    id_prefix = "gid://ballot-factory/Candidacy/"
    prefixed_id = f"{id_prefix}{candidacy_id}"
    encoded_bytes: bytes = b64encode(prefixed_id.encode("utf-8"))
    encoded_id: str = encoded_bytes.decode("utf-8")
    return encoded_id


def _get_stances_batch(
    candidacy_ids: List[str],
    ce_api_token: str,
    base_sleep: float = 0.1,
    jitter_factor: float = 0.1,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Queries the CivicEngine GraphQL API to get stance IDs for multiple candidacies. Uses the 'nodes' GraphQL query for efficient batch processing.

    Args:
        candidacy_ids: List of candidacy IDs to get the stances for
        ce_api_token: Authentication token for the CivicEngine API
        base_sleep: Base number of seconds to sleep after making an API call
        jitter_factor: Random factor to apply to sleep time (0.5 means ±50% of base_sleep)
        timeout: Timeout in seconds for the API request

    Returns:
        List of stance dictionaries containing stance data and metadata for all provided candidacies

    Raises:
        ValueError: If the API response is invalid or unexpected
    """
    url = "https://bpi.civicengine.com/graphql"

    # Process candidacy IDs in batches
    all_stances = []

    # Encode all candidacy IDs
    encoded_ids = [_base64_encode_id(str(cid)) for cid in candidacy_ids]

    # Construct the payload with the nodes query
    payload = {
        "query": """
        query GetCandidacyStancesBatch($ids: [ID!]!) {
            nodes(ids: $ids) {
                ... on Candidacy {
                    id
                    databaseId
                    stances {
                        databaseId
                        id
                        issue {
                            databaseId
                            id
                        }
                        locale
                        referenceUrl
                        statement
                    }
                }
            }
        }
        """,
        "variables": {"ids": encoded_ids},
    }

    # Add headers with authentication
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {ce_api_token}",
    }

    try:
        logging.debug(f"Sending request for {len(candidacy_ids)} candidacies")
        response = requests.post(url, json=payload, headers=headers, timeout=timeout)

        # Calculate sleep time with jitter to avoid synchronized API calls
        jitter = random.uniform(-jitter_factor, jitter_factor) * base_sleep
        sleep_time = max(0.05, base_sleep + jitter)  # Ensure minimum sleep of 0.05s
        time.sleep(sleep_time)

        response.raise_for_status()
        data = response.json()

        # If the response is empty or has an unexpected structure, log and handle the error
        if not data or not isinstance(data, dict) or "data" not in data:
            raise ValueError("Invalid response from CivicEngine API")

        # Safely navigate through the response, handling None values at any level
        data_dict = data.get("data") or {}
        nodes_list = data_dict.get("nodes") or []

        for node in nodes_list:
            if not node:
                continue

            candidacy_db_id = node.get("databaseId")
            encoded_candidacy_id = node.get("id")

            if not candidacy_db_id:
                continue

            stances = node.get("stances", [])
            if not stances:
                continue

            # Add candidacy_id and encoded_candidacy_id to each stance
            for stance in stances:
                stance["candidacy_id"] = int(candidacy_db_id)
                stance["encoded_candidacy_id"] = encoded_candidacy_id
                all_stances.append(stance)

        logging.debug(
            f"Retrieved {len(all_stances)} stances for {len(candidacy_ids)} candidacies"
        )
        return all_stances

    except (KeyError, TypeError) as e:
        logging.warning(f"Error processing stances for candidacies batch: {str(e)}")
        return []
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for candidacies batch: {str(e)}")
        return []


# Define the schema for stances array - used by both the pandas UDF and for empty arrays
stance_schema = ArrayType(
    StructType(
        [
            StructField(name="databaseId", dataType=IntegerType(), nullable=False),
            StructField("id", StringType(), False),
            StructField(
                "issue",
                StructType(
                    [
                        StructField("databaseId", IntegerType(), True),
                        StructField("id", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField("locale", StringType(), True),
            StructField("referenceUrl", StringType(), True),
            StructField("statement", StringType(), True),
            StructField("candidacy_id", IntegerType(), True),
            StructField("encoded_candidacy_id", StringType(), True),
        ]
    )
)


def _get_candidacy_stances_token(ce_api_token: str) -> Callable:
    """Wraps the token in a pandas UDF for proper order of operations."""

    @pandas_udf(returnType=stance_schema)
    def get_candidacy_stances(candidacy_ids: pd.Series) -> pd.Series:
        """
        Pandas UDF that processes batches of candidacy IDs and returns their stances.

        This function is distributed by Spark to different workers, allowing parallel
        processing of candidacy data. Inside each worker, candidacies are further
        batched for efficient API calls.

        Args:
            candidacy_ids: Series of candidacy IDs to process

        Returns:
            Series of stance arrays for each candidacy ID
        """
        if not ce_api_token:
            raise ValueError("Missing required environment variable: CE_API_TOKEN")

        # Create a map to store stances by candidacy ID
        stances_by_candidacy: Dict[int, List[Any]] = {}

        # Get unique candidacy IDs to avoid duplicate API calls
        unique_candidacy_ids = candidacy_ids.unique().tolist()

        # Set batch size for API calls
        batch_size = 100

        # Process candidacies in batches
        for i in range(0, len(unique_candidacy_ids), batch_size):
            batch = unique_candidacy_ids[i : i + batch_size]
            batch_size_info = f"Batch {i//batch_size + 1}/{(len(unique_candidacy_ids) + batch_size - 1)//batch_size}, size: {len(batch)}"
            logging.debug(f"Processing {batch_size_info}")

            try:
                batch_stances = _get_stances_batch(batch, ce_api_token)

                # Organize stances by candidacy_id
                for stance in batch_stances:
                    cid = stance["candidacy_id"]
                    if cid not in stances_by_candidacy:
                        stances_by_candidacy[cid] = []
                    stances_by_candidacy[cid].append(stance)
            except Exception as e:
                logging.error(f"Error processing batch {i//batch_size}: {str(e)}")

        # Create result series mapping each candidacy ID to its stances array
        result = pd.Series(
            [stances_by_candidacy.get(int(cid), []) for cid in candidacy_ids]
        )
        return result

    return get_candidacy_stances


def model(dbt, session) -> DataFrame:
    """
    `dbt` Python model to retrieve and process stance data from the CivicEngine API using pandas UDFs.

    This model:
    1. Gets candidate IDs from the candidacies staging table
    2. For incremental runs, filters to only include recently updated records
    3. Fetches stance data for multiple candidacies in batches from the CivicEngine API using pandas UDF
    4. Processes and structures the response data
    5. Adds metadata including created_at and updated_at timestamps

    Args:
        dbt: The dbt context object
        session: The Spark session

    Returns:
        DataFrame containing processed stance data
    """
    # Configure the model
    dbt.config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="candidacy_id",
        on_schema_change="fail",
        tags=["ballotready", "stance", "api", "pandas_udf"],
    )

    # Get API token from environment variables
    ce_api_token = dbt.config.get("ce_api_token")
    if not ce_api_token:
        raise ValueError("Missing required config parameter: ce_api_token")

    # Get candidacies
    candidacies: DataFrame = dbt.ref(
        "stg_airbyte_source__ballotready_s3_candidacies_v3"
    )

    # Validate source data
    if candidacies.count() == 0:
        logging.warning("No candidacies found in source table")
        # Return empty DataFrame with correct schema
        empty_df = session.createDataFrame(
            [],
            StructType(
                [
                    StructField("candidacy_id", IntegerType(), False),
                    StructField("stances", stance_schema, True),
                    StructField("created_at", TimestampType(), False),
                    StructField("updated_at", TimestampType(), False),
                ]
            ),
        )
        return empty_df

    # Get distinct candidacy IDs
    ids_from_candidacies = candidacies.select("candidacy_id").distinct().collect()
    ids_from_candidacies = [row.candidacy_id for row in ids_from_candidacies]
    logging.info(f"INFO: Found {len(ids_from_candidacies)} distinct candidacy IDs")

    # Handle incremental loading
    if dbt.is_incremental:
        logging.info("INFO: Running in incremental mode")
        existing_table = session.table(f"{dbt.this}")
        existing_timestamps = existing_table.select(
            "candidacy_id", "created_at"
        ).distinct()

        # Get maximum updated_at from existing table
        max_updated_at_row = existing_table.agg({"updated_at": "max"}).collect()[0]
        max_updated_at = max_updated_at_row[0] if max_updated_at_row else None

        if max_updated_at:
            # Filter source to only process records updated since last run
            candidacies = candidacies.filter(
                candidacies["candidacy_updated_at"] >= max_updated_at
            )
            logging.info(
                f"INFO: Filtered to candidacies updated since {max_updated_at}"
            )
        else:
            # Fallback to 30-day window if no max_updated_at found
            thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            candidacies = candidacies.filter(
                candidacies["candidacy_updated_at"] >= thirty_days_ago
            )
            logging.info(
                f"INFO: No max updated_at found. Filtered to candidacies updated since {thirty_days_ago}"
            )
    else:
        logging.info("INFO: Running in full refresh mode")

    # If no records to process after filtering, return early
    if candidacies.count() == 0 and dbt.is_incremental:
        logging.info("INFO: No new or updated candidacies to process")
        # Return empty DataFrame with correct schema
        empty_df = session.createDataFrame(
            [],
            StructType(
                [
                    StructField("candidacy_id", IntegerType(), False),
                    StructField("stances", stance_schema, True),
                    StructField("created_at", TimestampType(), False),
                    StructField("updated_at", TimestampType(), False),
                ]
            ),
        )
        return empty_df

    # For development/testing purposes (commented out by default)
    # candidacies = candidacies.sample(False, 0.1).limit(1000)

    # Process candidacies using the pandas UDF for parallel processing
    logging.info("INFO: Starting parallel processing of candidacies using pandas UDF")

    # Create a DataFrame with just candidacy_ids for processing
    stance = candidacies.select(
        col("candidacy_id").cast("integer").alias("candidacy_id")
    )

    # Apply the pandas UDF to get stances for each candidacy
    get_candidacy_stances = _get_candidacy_stances_token(ce_api_token)
    stance = stance.withColumn("stances", get_candidacy_stances("candidacy_id"))
    logging.info(f"INFO: Processed {stance.count()} candidacies with pandas UDF")

    # Add timestamp metadata
    current_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    if dbt.is_incremental:
        # Prepare a lookup DataFrame with existing candidacy_ids and their original created_at values
        existing_created_at_lookup = existing_timestamps

        # Left join with this lookup to preserve original created_at values for existing records
        stance = stance.join(existing_created_at_lookup, on="candidacy_id", how="left")

        # Use coalesce to keep original created_at for existing records, and set current time for new ones
        stance = stance.withColumn(
            "created_at",
            coalesce(col("created_at"), lit(current_time_utc).cast(TimestampType())),
        )
    else:
        # For initial load, set created_at for all records
        stance = stance.withColumn(
            "created_at", lit(current_time_utc).cast(TimestampType())
        )

    stance = stance.withColumn(
        "updated_at", lit(current_time_utc).cast(TimestampType())
    )

    # Count and log the final row count
    row_count = stance.count()
    logging.info(f"INFO: Completed processing with {row_count} rows")

    # Return the final DataFrame
    return stance
