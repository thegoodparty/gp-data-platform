import logging
import random
import time
from base64 import b64encode
from typing import Any, Callable, Dict, List

import pandas as pd
import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def _base64_encode_id(geofence_id: str) -> str:
    """
    Encodes a geofence ID into the format required by CivicEngine API.

    Args:
        geofence_id: The raw geofence ID to encode.

    Returns:
        The base64-encoded ID string with the proper prefix.
    """
    id_prefix = "gid://ballot-factory/Position/"
    prefixed_id = f"{id_prefix}{geofence_id}"
    encoded_bytes: bytes = b64encode(prefixed_id.encode("utf-8"))
    encoded_id: str = encoded_bytes.decode("utf-8")
    return encoded_id


def _get_normalized_positions_batch(
    position_ids: List[int],
    ce_api_token: str,
    base_sleep: float = 0.1,
    jitter_factor: float = 0.1,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Fetches normalized positions for a batch of position IDs from the CivicEngine API.

    Args:
        position_ids: List of position IDs to fetch normalized positions for
        ce_api_token: Authentication token for the CivicEngine API
        base_sleep: Base number of seconds to sleep after making an API call
        jitter_factor: Random factor to apply to sleep time (0.5 means ±50% of base_sleep)
        timeout: Timeout in seconds for the API request
    """
    url = "https://bpi.civicengine.com/graphql"

    # Encode all position IDs
    encoded_ids = [_base64_encode_id(str(position_id)) for position_id in position_ids]

    # Construct the payload with the nodes query
    payload = {
        "query": """
        query GetPositionsBatch($ids: [ID!]!) {
            nodes(ids: $ids) {
                ... on Position {
                    databaseId
                    normalizedPosition {
                        databaseId
                        description
                        id
                        issues {
                            databaseId
                            id
                        }
                        mtfcc
                        name
                    }
                }
            }
        """,
        "variables": {"ids": encoded_ids},
    }

    # Send the request to the API
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {ce_api_token}",
    }

    try:
        logging.debug(f"Sending request for {len(encoded_ids)} positions")
        response = requests.post(url, json=payload, headers=headers, timeout=timeout)

        # Calculate sleep time with jitter to avoid synchronized API calls
        jitter = random.uniform(-jitter_factor, jitter_factor) * base_sleep
        sleep_time = max(0.05, base_sleep + jitter)  # Ensure minimum sleep of 0.05s
        time.sleep(sleep_time)

        response.raise_for_status()

        # Parse the response
        """
        """
        data = response.json()
        positions: List[Dict[str, Any]] = data.get("data", {}).get("nodes", [])
        return positions

    except (KeyError, TypeError) as e:
        logging.error(f"Error processing positions batch: {str(e)}")
        raise ValueError(f"Failed to parse position data from API response: {str(e)}")


normalized_position_schema = StructType(
    [
        StructField("databaseId", IntegerType(), nullable=False),
        StructField("description", StringType(), True),
        StructField("id", StringType(), nullable=False),
        StructField(
            "issues",
            ArrayType(
                StructType(
                    [
                        StructField("databaseId", IntegerType(), False),
                        StructField("id", StringType(), False),
                    ]
                ),
                False,
            ),
        ),
        StructField("mtfcc", StringType(), True),
        StructField("name", StringType(), False),
    ]
)


def _get_normalized_position_token(ce_api_token: str) -> Callable:
    """
    Wraps the token in a pandas UDF for proper order of operations.

    Args:
        ce_api_token: API token for CivicEngine

    Returns:
        A pandas UDF function
    """

    @pandas_udf(returnType=StringType())
    # @pandas_udf(returnType=normalized_position_schema)
    def _get_normalized_position(position_ids: pd.Series) -> pd.Series:
        """
        Pandas UDF that processes batches of position IDs and returns their normalized positions.
        """
        if not ce_api_token:
            raise ValueError("Missing required environment variable: CE_API_TOKEN")

        # Create a map to store stances by candidacy ID
        normalized_positions_by_position_id: Dict[int, Dict[str, Any] | None] = {}

        # Set batch size for API calls
        batch_size = 100

        # Process position IDs in batches
        for i in range(0, len(position_ids), batch_size):
            batch = position_ids[i : i + batch_size]
            batch_size_info = f"Batch {i//batch_size + 1}/{(len(position_ids) + batch_size - 1)//batch_size}, size: {len(batch)}"
            logging.debug(f"Processing {batch_size_info}")

            try:
                batch_normalized_positions = _get_normalized_positions_batch(
                    batch, ce_api_token
                )
                """
                sample data:
                """

                for normalized_position in batch_normalized_positions:
                    position_id = normalized_position["databaseId"]
                    normalized_positions_by_position_id[position_id] = (
                        normalized_position["normalizedPosition"]
                    )

            except Exception as e:
                logging.error(f"Error processing batch {i}: {e}")
                raise e

        result = pd.Series(
            [
                normalized_positions_by_position_id.get(position_id, {})
                for position_id in position_ids
            ]
        )
        return result

    return _get_normalized_position


def model(dbt, session) -> DataFrame:
    """
    `dbt` Python model to normalize position data from the CivicEngine API.
    """
    dbt.config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        on_schema_change="fail",
        tags=["ballotready", "normalized_position", "api", "pandas_udf"],
    )

    # Get API token from environment variables
    ce_api_token = dbt.config.get("ce_api_token")
    if not ce_api_token:
        raise ValueError("Missing required config parameter: ce_api_token")

    # Get position data
    positions: DataFrame = dbt.ref("stg_airbyte_source__ballotready_api_position")

    # Filter if incremental by updated_at
    if dbt.is_incremental:
        # Get the max updated_at from the existing table
        existing_table = session.table(f"{dbt.this}")
        max_updated_at_row = existing_table.agg({"updated_at": "max"}).collect()[0]
        max_updated_at = max_updated_at_row[0] if max_updated_at_row else None

        if max_updated_at:
            positions = positions.filter(positions.updated_at > max_updated_at)
            logging.info(f"INFO: Filtered to positions updated since {max_updated_at}")
        else:
            logging.info("INFO: No max updated_at found. Processing all positions.")

    # deduplicate and only get the position ids
    positions = positions.select("database_id").dropDuplicates(["database_id"])

    # Validate source data
    if positions.count() == 0:
        logging.warning("No positions found in source table")
        empty_df = session.createDataFrame([], normalized_position_schema)
        return empty_df

    # For development/testing purposes (commented out by default)
    positions = positions.sample(False, 0.1).limit(1000)

    get_normalized_position = _get_normalized_position_token(ce_api_token)
    normalized_positions = positions.withColumn(
        "normalized_position", get_normalized_position(positions["database_id"])
    )

    return normalized_positions
