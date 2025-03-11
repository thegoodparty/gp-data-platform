import logging
import random
import time
from base64 import b64encode
from typing import Any, Callable, Dict, List

import pandas as pd
import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import (
    DateType,
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
    id_prefix = "gid://ballot-factory/Geofence/"
    prefixed_id = f"{id_prefix}{geofence_id}"
    encoded_bytes: bytes = b64encode(prefixed_id.encode("utf-8"))
    encoded_id: str = encoded_bytes.decode("utf-8")
    return encoded_id


def _get_geofences_batch(
    geofence_ids: List[str],
    ce_api_token: str,
    base_sleep: float = 0.1,
    jitter_factor: float = 0.1,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Fetches geofences for a batch of geo IDs using the CivicEngine API.

    Args:
        geofence_ids: List of geofence IDs to fetch geofences for
        ce_api_token: Authentication token for the CivicEngine API
        base_sleep: Base number of seconds to sleep after making an API call
        jitter_factor: Random factor to apply to sleep time (0.5 means ±50% of base_sleep)
        timeout: Timeout in seconds for the API request

    Returns:
        List of geofences for the given geo IDs
    """
    url = "https://bpi.civicengine.com/graphql"

    # Encode all geo IDs
    encoded_ids = [_base64_encode_id(str(geofence_id)) for geofence_id in geofence_ids]

    # Construct the payload with the nodes query
    payload = {
        "query": """
        query GetGeofencesBatch($ids: [ID!]!) {
            nodes(ids: $ids) {
                ... on Geofence {
                    createdAt
                    databaseId
                    geoId
                    id
                    mtfcc
                    updatedAt
                    validFrom
                    validTo
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
        logging.debug(f"Sending request for {len(encoded_ids)} geofences")
        response = requests.post(url, json=payload, headers=headers, timeout=timeout)

        # Calculate sleep time with jitter to avoid synchronized API calls
        jitter = random.uniform(-jitter_factor, jitter_factor) * base_sleep
        sleep_time = max(0.05, base_sleep + jitter)  # Ensure minimum sleep of 0.05s
        time.sleep(sleep_time)

        response.raise_for_status()

        # Parse the response
        """
        sample response:
        {
            "data": {
                "nodes": [
                    {
                        "createdAt": "2016-09-14T18:40:31Z",
                        "databaseId": 3622,
                        "geoId": null,
                        "id": "Z2lkOi8vYmFsbG90LWZhY3RvcnkvR2VvZmVuY2UvMzYyMg==",
                        "mtfcc": null,
                        "updatedAt": "2016-09-14T18:40:31Z",
                        "validFrom": "2016-09-14",
                        "validTo": null
                    },
                    ...
                ]
            }
        }
        """
        data = response.json()
        geofences: List[Dict[str, Any]] = data.get("data", {}).get("nodes", [])
        return geofences

    except (KeyError, TypeError) as e:
        logging.error(f"Error processing geofences batch: {str(e)}")
        raise ValueError(f"Failed to parse geofence data from API response: {str(e)}")
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for geofences batch: {str(e)}")
        raise RuntimeError(f"Failed to fetch geofence data from API: {str(e)}")


def _get_geofence_token(ce_api_token: str) -> Callable:
    """Wraps the token in a pandas UDF for proper order of operations."""

    @pandas_udf(returnType=geofence_schema)
    def get_geofence(geofence_ids: pd.Series) -> pd.DataFrame:
        """
        Pandas UDF that processes batches of geofence IDs and returns their geofences.

        This function is distributed by Spark to different workers, allowing parallel
        processing of geo data. Inside each worker, geo IDs are further
        batched for efficient API calls.

        Args:
            geofence_ids: Series of geofence IDs to process
        """
        if not ce_api_token:
            raise ValueError("Missing required environment variable: CE_API_TOKEN")

        # Create a map to store stances by candidacy ID
        geofences_by_geofence_id: Dict[int, Dict[str, Any] | None] = {}

        # Set batch size for API calls
        batch_size = 100

        # Process geo IDs in batches
        for i in range(0, len(geofence_ids), batch_size):
            batch = geofence_ids[i : i + batch_size]
            batch_size_info = f"Batch {i//batch_size + 1}/{(len(geofence_ids) + batch_size - 1)//batch_size}, size: {len(batch)}"
            logging.debug(f"Processing {batch_size_info}")

            try:
                batch_geofences = _get_geofences_batch(batch, ce_api_token)
                """
                sample data:
                [
                    {
                        "createdAt": "2024-12-18T16:10:44Z",
                        "databaseId": 1355244,
                        "geoId": "38079",
                        "id": "Z2lkOi8vYmFsbG90LWZhY3RvcnkvR2VvZmVuY2UvMTM1NTI0NA==",
                        "mtfcc": "G4020",
                        "updatedAt": "2025-02-14T21:20:57Z",
                        "validFrom": "2024-01-01",
                        "validTo": null,
                    },
                    ...
                ]
                """
                # Organize geofences by geofence_id
                for geofence in batch_geofences:
                    geofence_id = int(geofence["databaseId"])
                    geofences_by_geofence_id[geofence_id] = geofence
            except Exception as e:
                logging.error(f"Error processing batch {i//batch_size}: {str(e)}")

        # Create a list of dictionaries for each geofence in order of input
        result_data: List[Dict[str, Any]] = []
        for geofence_id in geofence_ids:
            geofence: Dict[str, Any] = geofences_by_geofence_id.get(int(geofence_id), {})  # type: ignore
            if geofence:
                result_data.append(
                    {
                        "createdAt": pd.to_datetime(geofence["createdAt"]),
                        "databaseId": geofence["databaseId"],
                        "geoId": geofence["geoId"],
                        "id": geofence["id"],
                        "mtfcc": geofence["mtfcc"],
                        "updatedAt": pd.to_datetime(geofence["updatedAt"]),
                        "validFrom": (
                            pd.to_datetime(geofence["validFrom"]).date()
                            if geofence["validFrom"]
                            else None
                        ),
                        "validTo": (
                            pd.to_datetime(geofence["validTo"]).date()
                            if geofence["validTo"]
                            else None
                        ),
                    }
                )
            else:
                # Raise error for missing geofences since they are required
                raise ValueError(
                    f"No geofence data found for geofence_id: {geofence_id}"
                )

        # Convert to DataFrame with the correct schema
        result_df = pd.DataFrame(result_data)

        # Ensure correct types and non-nullable fields
        result_df["createdAt"] = result_df["createdAt"].astype("datetime64[ns]")
        result_df["databaseId"] = result_df["databaseId"].astype("int32")
        result_df["id"] = result_df["id"].astype("string")
        result_df["updatedAt"] = result_df["updatedAt"].astype("datetime64[ns]")

        return result_df

    return get_geofence


geofence_schema = StructType(
    [
        StructField(name="createdAt", dataType=TimestampType(), nullable=True),  # f
        StructField("databaseId", IntegerType(), True),  # f
        StructField("geoId", StringType(), True),
        StructField("id", StringType(), True),  # f
        StructField("mtfcc", StringType(), True),
        StructField("updatedAt", TimestampType(), True),  # f
        StructField("validFrom", DateType(), True),
        StructField("validTo", DateType(), True),
    ]
)


def model(dbt, session) -> DataFrame:
    # Configure the model
    dbt.config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="candidacy_id",
        on_schema_change="fail",
        tags=["ballotready", "geofence", "api", "pandas_udf"],
    )

    # get API token from environment variables
    ce_api_token = dbt.config.get("ce_api_token")
    if not ce_api_token:
        raise ValueError("Missing required config parameter: ce_api_token")

    # get all candidacy and position data
    candidacy_df: DataFrame = dbt.ref(
        "stg_airbyte_source__ballotready_s3_candidacies_v3"
    )

    # handle incremental loading
    if dbt.is_incremental:
        logging.info("INFO: Running in incremental mode")
        existing_table = session.table(f"{dbt.this}")
        existing_timestamps = existing_table.select(
            "geofence_id", "created_at"
        ).distinct()

        # get the latest updated_at date
        latest_updated_at = existing_timestamps.agg({"updated_at": "max"}).collect()[0][
            0
        ]

        # get all unique geo id after the latest updated_at date
        geofence = candidacy_df.select("geofence_id", "candidacy_updated_at").filter(
            col("candidacy_updated_at") > latest_updated_at
        )
    else:
        geofence = candidacy_df.select("geofence_id")

    # if geofence_id is empty, return empty DataFrame
    if geofence.count() == 0:
        logging.info("INFO: No new or updated geofence ids to process")
        return session.createDataFrame([], geofence_schema)

    # For development/testing purposes (commented out by default)
    # geofence = geofence.sample(False, 0.1).limit(1000)

    # get geofence data from API
    get_geofence = _get_geofence_token(ce_api_token)

    # First get the geofence data as a struct
    geofence = geofence.withColumn("geofence_data", get_geofence(col("geofence_id")))

    # Then extract each field into its own column
    result = geofence.select(
        col("geofence_data.createdAt").alias("createdAt"),
        col("geofence_data.databaseId").alias("databaseId"),
        col("geofence_data.geoId").alias("geoId"),
        col("geofence_data.id").alias("id"),
        col("geofence_data.mtfcc").alias("mtfcc"),
        col("geofence_data.updatedAt").alias("updatedAt"),
        col("geofence_data.validFrom").alias("validFrom"),
        col("geofence_data.validTo").alias("validTo"),
    )
    return result
