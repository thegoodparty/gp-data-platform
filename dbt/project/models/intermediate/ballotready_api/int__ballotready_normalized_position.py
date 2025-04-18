import logging
import random
import time
from base64 import b64encode
from typing import Any, Callable, Dict, List

import pandas as pd
import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, current_timestamp, pandas_udf
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _base64_encode_id(geofence_id: str) -> str:
    """
    Encodes a geofence ID into the format required by CivicEngine API.

    Args:
        geofence_id: The raw geofence ID to encode.

    Returns:
        The base64-encoded ID string with the proper prefix.
    """
    id_prefix = "gid://ballot-factory/NormalizedPosition/"
    prefixed_id = f"{id_prefix}{geofence_id}"
    encoded_bytes: bytes = b64encode(prefixed_id.encode("utf-8"))
    encoded_id: str = encoded_bytes.decode("utf-8")
    return encoded_id


@pandas_udf(returnType=StringType())
def _base64encode_id_udf(position_ids: pd.Series) -> pd.Series:
    return position_ids.astype(str).apply(_base64_encode_id)


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
            query GetNormalizedPositionsBatch($ids: [ID!]!) {
                nodes(ids: $ids) {
                    ... on NormalizedPosition {
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
        data = response.json()
        positions: List[Dict[str, Any]] = data.get("data", {}).get("nodes", [])
        return positions

    except (KeyError, TypeError) as e:
        logging.error(f"Error processing positions batch: {str(e)}")
        raise ValueError(f"Failed to parse position data from API response: {str(e)}")


normalized_position_schema = StructType(
    [
        StructField("databaseId", IntegerType()),
        StructField("description", StringType()),
        StructField("id", StringType()),
        StructField(
            "issues",
            ArrayType(
                StructType(
                    [
                        StructField("databaseId", IntegerType()),
                        StructField("id", StringType()),
                    ]
                ),
            ),
        ),
        StructField("mtfcc", StringType()),
        StructField("name", StringType()),
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

    @pandas_udf(returnType=normalized_position_schema)
    def _get_normalized_position(normalized_position_ids: pd.Series) -> pd.DataFrame:
        """
        Pandas UDF that processes batches of Normalized Position IDs and returns their normalized positions.
        """
        if not ce_api_token:
            raise ValueError("Missing required environment variable: CE_API_TOKEN")

        # Create a map to store stances by candidacy ID
        normalized_positions_by_database_id: Dict[int, Dict[str, Any] | None] = {}

        # Set batch size for API calls
        batch_size = 100

        # Process position IDs in batches
        for i in range(0, len(normalized_position_ids), batch_size):
            batch = normalized_position_ids[i : i + batch_size]
            batch_size_info = f"Batch {i//batch_size + 1}/{(len(normalized_position_ids) + batch_size - 1)//batch_size}, size: {len(batch)}"
            logging.debug(f"Processing {batch_size_info}")

            try:
                batch_normalized_positions = _get_normalized_positions_batch(
                    batch, ce_api_token
                )
                """
                sample data:
                [
                    {
                        "databaseId": 1520,
                        "description": "The City Legislature is the municipality's governing body, responsible for voting on ordinances and policies, and often is in charge of hiring a city manager.",
                        "id": "Z2lkOi8vYmFsbG90LWZhY3RvcnkvTm9ybWFsaXplZFBvc2l0aW9uLzE1MjA=",
                        "mtfcc": null,
                        "name": "City Legislature",
                        "issues": [{
                            "databaseId": 4, "id": "Z2lkOi8vYmFsbG90LWZhY3RvcnkvSXNzdWUvNA=="},
                            ...]
                    },
                    ...
                ]
                """

                for normalized_position in batch_normalized_positions:
                    if normalized_position:
                        normalized_position_id = normalized_position["databaseId"]
                        normalized_positions_by_database_id[normalized_position_id] = (
                            normalized_position
                        )

            except Exception as e:
                logging.error(f"Error processing batch {i}: {e}")
                raise e

        # Create a list of dictionaries for each normalized_position in order of input
        result_data: List[Dict[str, Any]] = []
        for database_id in normalized_position_ids:
            try:
                normalized_position = normalized_positions_by_database_id.get(database_id, {})  # type: ignore
                if normalized_position:
                    result_data.append(
                        {
                            "databaseId": normalized_position.get("databaseId"),
                            "description": normalized_position["description"],
                            "id": normalized_position["id"],
                            "mtfcc": normalized_position["mtfcc"],
                            "name": normalized_position["name"],
                            "issues": normalized_position["issues"],
                        }
                    )
                else:
                    result_data.append(
                        {
                            "databaseId": -1,
                            "description": None,
                            "id": None,
                            "mtfcc": None,
                            "name": None,
                            "issues": None,
                        }
                    )
            except Exception as e:
                logging.error(f"Error processing position {database_id}: {e}")
                raise e

        result_data = pd.DataFrame(result_data)
        return result_data

    return _get_normalized_position


def model(dbt, session) -> DataFrame:
    """
    `dbt` Python model to normalize position data from the CivicEngine API.
    """
    dbt.config(
        submission_method="all_purpose_cluster",  # required for .cache()
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",  # required for .cache()
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

        # For development/testing purposes (commented out by default)
        # max_updated_at = "2023-03-15 00:00:00"

        if max_updated_at:
            positions = positions.filter(positions.updated_at > max_updated_at)
            logging.info(f"INFO: Filtered to positions updated since {max_updated_at}")
        else:
            logging.info("INFO: No max updated_at found. Processing all positions.")

    # deduplicate and only get the position ids
    normalized_positions = (
        positions.select("normalized_position.databaseId")
        .dropDuplicates(["databaseId"])
        .withColumnRenamed("databaseId", "database_id")
    )

    # Trigger a cache to ensure filters above are applied before making API calls
    normalized_positions.cache()
    normalized_positions_count = normalized_positions.count()

    # Validate source data
    if normalized_positions_count == 0:
        logging.warning("No positions found in source table")
        empty_df: DataFrame = session.createDataFrame(
            [],
            normalized_position_schema.add(
                StructField("created_at", TimestampType())
            ).add(StructField("updated_at", TimestampType())),
        )
        empty_df = empty_df.withColumnRenamed("databaseId", "database_id")
        return empty_df

    # For development/testing purposes (commented out by default)
    # normalized_positions = normalized_positions.sample(False, 0.1).limit(5)

    get_normalized_position = _get_normalized_position_token(ce_api_token)
    normalized_positions = normalized_positions.withColumn(
        "normalized_position_data", get_normalized_position(col("database_id"))
    )

    # Explode the normalized_position column into individual fields
    normalized_positions = normalized_positions.selectExpr(
        "normalized_position_data.databaseId as database_id",
        "normalized_position_data.description as description",
        "normalized_position_data.id as id",
        "normalized_position_data.issues as issues",
        "normalized_position_data.mtfcc as mtfcc",
        "normalized_position_data.name as name",
    )

    # Add created_at and updated_at timestamps
    # If this is an incremental run, we need to preserve created_at for existing records
    if dbt.is_incremental:
        existing_table = session.table(f"{dbt.this}")

        # Join with existing table to get created_at for existing records
        normalized_positions = normalized_positions.join(
            existing_table.select("id", "created_at"),
            normalized_positions.id == existing_table.id,
            "left",
        ).drop(existing_table.id)

        # For new records (where created_at is null), set current timestamp
        normalized_positions = normalized_positions.withColumn(
            "created_at", coalesce(col("created_at"), current_timestamp())
        )
    else:
        # For initial load, all records get current timestamp
        normalized_positions = normalized_positions.withColumn(
            "created_at", current_timestamp()
        )

    # deduplicate
    normalized_positions = normalized_positions.dropDuplicates(["id"])

    # updated_at is always set to current timestamp
    normalized_positions = normalized_positions.withColumn(
        "updated_at", current_timestamp()
    )

    # Drop rows with negative databaseId values, where -1 was a placeholder for failed records
    # Trigger a cache to ensure preceding transformations are applied before the filter
    normalized_positions.cache()
    normalized_positions_count = normalized_positions.count()
    normalized_positions = normalized_positions.filter(col("database_id") >= 0)

    return normalized_positions
