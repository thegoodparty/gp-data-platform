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
    Encodes a candidacy ID into a base64 string for use with the GraphQL API.

    Args:
        candidacy_id: The ID of the candidacy

    Returns:
        A base64 encoded string
    """
    id_prefix = "gid://ballot-factory/Candidacy/"
    prefixed_id = f"{id_prefix}{candidacy_id}"
    encoded_bytes: bytes = b64encode(prefixed_id.encode("utf-8"))
    encoded_id: str = encoded_bytes.decode("utf-8")
    return encoded_id


def _get_endorsements_batch(
    candidacy_ids: List[str],
    ce_api_token: str,
    base_sleep: float = 0.1,
    jitter_factor: float = 0.1,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Fetches endorsements for a batch of candidacy IDs from the CivicEngine GraphQL API.

    Args:
        candidacy_ids: List of candidacy IDs to fetch
        ce_api_token: API token for CivicEngine
        base_sleep: Base sleep time for backoff
        jitter_factor: Jitter factor for backoff
        timeout: Timeout for the API call

    Returns:
        List of dictionaries containing endorsement data
    """
    url = "https://bpi.civicengine.com/graphql"
    if not candidacy_ids:
        return []

    # Encode candidacy IDs for the GraphQL query
    encoded_ids = [_base64_encode_id(str(cid)) for cid in candidacy_ids]

    # Process candidacy IDs in batches
    all_endorsements = []

    # Build the GraphQL query
    query = """
    query GetCandidacyEndorsements($ids: [ID!]!) {
        nodes(ids: $ids) {
            ... on Candidacy {
                id
                databaseId
                endorsements {
                    databaseId
                    id
                    createdAt
                    endorser
                    recommendation
                    status
                    updatedAt
                    organization {
                        databaseId
                        id
                    }
                }
            }
        }
    }
    """
    variables = {"ids": encoded_ids}
    payload = {"query": query, "variables": variables}

    # API request setup
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

            endorsements = node.get("endorsements", [])
            if not endorsements:
                continue

            # process each endorsement
            for endorsement in endorsements:
                # Add candidacy_id and encoded_candidacy_id to each stance
                endorsement["candidacy_id"] = int(candidacy_db_id)
                endorsement["encoded_candidacy_id"] = encoded_candidacy_id

                # handle timestamps with timezone for pandas -> arrow -> spark
                endorsement["createdAt"] = pd.to_datetime(endorsement["createdAt"])
                endorsement["updatedAt"] = pd.to_datetime(endorsement["updatedAt"])
                all_endorsements.append(endorsement)

        logging.debug(
            f"Retrieved {len(all_endorsements)} endorsements for {len(candidacy_ids)} candidacies"
        )
        return all_endorsements

    except (KeyError, TypeError) as e:
        logging.warning(f"Error processing stances for candidacies batch: {str(e)}")
        raise ValueError(f"Failed to process stances: {str(e)}")
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for candidacies batch: {str(e)}")
        raise RuntimeError(f"API request failed: {str(e)}")


# Schema for endorsement data
endorsement_schema = ArrayType(
    StructType(
        [
            StructField("databaseId", IntegerType()),
            StructField("id", StringType()),
            StructField("createdAt", TimestampType()),
            StructField("endorser", StringType()),
            StructField("recommendation", StringType()),
            StructField("status", StringType()),
            StructField("updatedAt", TimestampType()),
            StructField(
                "organization",
                StructType(
                    [
                        StructField("databaseId", IntegerType()),
                        StructField("id", StringType()),
                    ]
                ),
            ),
            StructField("candidacy_id", IntegerType()),
            StructField("encoded_candidacy_id", StringType()),
        ]
    )
)

"""
Sample output:
[
    {
        "databaseId": 622474,
        "id": "Z2lkOi8vYmFsbG90LWZhY3RvcnkvRW5kb3JzZW1lbnQvNjIyNDc0",
        "createdAt": "2024-10-23T17:32:09Z",
        "endorser": "VoteVets",
        "recommendation": "PRO",
        "status": "ACTIVE",
        "updatedAt": "2024-10-23T17:32:09Z",
        "organization": {
            "databaseId": 2227,
            "id": "Z2lkOi8vYmFsbG90LWZhY3RvcnkvT3JnYW5pemF0aW9uLzIyMjc="
        },
        "candidacy_id": 851869,
        "encoded_candidacy_id": "Z2lkOi8vYmFsbG90LWZhY3RvcnkvQ2FuZGlkYWN5Lzg1MTg2OQ=="
    },
    ...
]
"""


def _get_candidacy_endorsements_token(ce_api_token: str) -> Callable:
    """
    Creates a pandas UDF that fetches endorsements for candidacy IDs.

    Args:
        ce_api_token: API token for CivicEngine

    Returns:
        A pandas UDF function
    """

    @pandas_udf(returnType=endorsement_schema)
    def get_candidacy_endorsements(candidacy_ids: pd.Series) -> pd.Series:
        """
        Pandas UDF to process candidacy IDs in batches and fetch their endorsements.

        Args:
            candidacy_ids: Series of candidacy IDs

        Returns:
            Series of endorsement data
        """
        # Create a map to store endorsements by candidacy ID
        endorsements_by_candidacy: Dict[int, List[Any]] = {}
        candidacy_ids = candidacy_ids.tolist()

        # Set batch size for API calls
        batch_size = 100

        # Process candidacies in batches
        for i in range(0, len(candidacy_ids), batch_size):
            batch = candidacy_ids[i : i + batch_size]
            batch_size_info = f"Batch {i//batch_size + 1}/{(len(candidacy_ids) + batch_size - 1)//batch_size}, size: {len(batch)}"
            logging.debug(f"Processing {batch_size_info}")

            try:
                batch_response = _get_endorsements_batch(batch, ce_api_token)

                # Organize endorsements by candidacy_id
                for endorsement in batch_response:
                    cid = endorsement["candidacy_id"]
                    if cid not in endorsements_by_candidacy:
                        endorsements_by_candidacy[cid] = []
                    endorsements_by_candidacy[cid].append(endorsement)

            except Exception as e:
                logging.error(f"Error processing batch {i//batch_size}: {str(e)}")

        # Create result series mapping each candidacy ID to its endorsements array
        result = pd.Series(
            [endorsements_by_candidacy.get(int(cid), []) for cid in candidacy_ids]
        )
        return result

    return get_candidacy_endorsements


def model(dbt, session) -> DataFrame:
    """
    Main dbt model function to process candidacy endorsements.

    Args:
        dbt: dbt context
        session: Spark session

    Returns:
        DataFrame containing candidacy endorsements
    """
    # Configure the model
    dbt.config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="candidacy_id",
        on_schema_change="fail",
        tags=["ballotready", "endorsement", "api", "pandas_udf"],
    )

    # Configure logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Get API token from environment variable
    ce_api_token = dbt.config.get("ce_api_token")
    if not ce_api_token:
        raise ValueError("CE_API_TOKEN environment variable must be set")

    # Get references to required models
    candidacies: DataFrame = dbt.ref(
        "stg_airbyte_source__ballotready_s3_candidacies_v3"
    )

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

    # If no records to process after filtering, return empty DataFrame with schema
    if candidacies.count() == 0 and dbt.is_incremental:
        logging.info("INFO: No new or updated candidacies to process")
        # Create empty DataFrame with the expected schema
        schema = StructType(
            [
                StructField("candidacy_id", IntegerType()),
                StructField("endorsements", endorsement_schema),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        )
        return session.createDataFrame([], schema)

    # For development/testing purposes (commented out by default)
    # candidacies = candidacies.sample(False, 0.1).limit(1000)

    # Process candidacies using the pandas UDF for parallel processing
    logging.info("INFO: Starting parallel processing of candidacies using pandas UDF")

    # Create a DataFrame with just candidacy_ids for processing
    endorsement = candidacies.select(
        col("candidacy_id").cast("integer").alias("candidacy_id")
    )

    # Apply the pandas UDF to get endorsements for each candidacy
    get_candidacy_endorsements = _get_candidacy_endorsements_token(ce_api_token)
    endorsement = endorsement.withColumn(
        "endorsements", get_candidacy_endorsements("candidacy_id")
    )

    # Add timestamp metadata
    current_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    if dbt.is_incremental:
        # Prepare a lookup DataFrame with existing candidacy_ids and their original created_at values
        existing_created_at_lookup = existing_timestamps

        # Left join with this lookup to preserve original created_at values for existing records
        endorsement = endorsement.join(
            existing_created_at_lookup, on="candidacy_id", how="left"
        )

        # Use coalesce to keep original created_at for existing records, and set current time for new ones
        endorsement = endorsement.withColumn(
            "created_at",
            coalesce(col("created_at"), lit(current_time_utc).cast(TimestampType())),
        )
    else:
        # For full refresh, set created_at to current time for all records
        endorsement = endorsement.withColumn(
            "created_at", lit(current_time_utc).cast(TimestampType())
        )

    # Set updated_at to current time for all records
    endorsement = endorsement.withColumn(
        "updated_at", lit(current_time_utc).cast(TimestampType())
    )

    # Count and log the final row count
    row_count = endorsement.count()
    logging.info(f"INFO: Completed processing with {row_count} rows")

    return endorsement
