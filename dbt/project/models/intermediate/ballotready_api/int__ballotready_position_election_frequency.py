import logging
import random
import time
from base64 import b64encode
from typing import Any, Callable, Dict, List

import pandas as pd
import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, pandas_udf
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _base64_encode_id(database_id: int) -> str:
    """
    UDF to encode the database_id to a base64 string.
    """
    id_prefix = "gid://ballot-factory/PositionElectionFrequency/"
    prefixed_id = f"{id_prefix}{database_id}"
    encoded_bytes: bytes = b64encode(prefixed_id.encode("utf-8"))
    encoded_id: str = encoded_bytes.decode("utf-8")
    return encoded_id


@pandas_udf(returnType=StringType())
def _base64encode_id_udf(database_ids: pd.Series) -> pd.Series:
    return database_ids.astype(str).apply(_base64_encode_id)


def _get_position_election_frequency_batch(
    position_database_ids: List[int],
    ce_api_token: str,
    base_sleep: float = 0.1,
    jitter_factor: float = 0.1,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Fetches position election frequency data for a batch of position database IDs from the CivicEngine API.

    Args:
        position_database_ids: List of position database IDs to fetch data for
        ce_api_token: API token for CivicEngine
        base_sleep: Base number of seconds to sleep after making an API call
        jitter_factor: Random factor to apply to sleep time (0.5 means ±50% of base_sleep)
        timeout: Timeout in seconds for the API request
    """
    url = "https://bpi.civicengine.com/graphql"

    # Encode all position IDs
    encoded_ids = [
        _base64_encode_id(position_database_id)
        for position_database_id in position_database_ids
    ]

    # Construct the payload with the nodes query
    payload = {
        "query": """
            query GetPositionElectionFrequencyBatch($ids: [ID!]!) {
                nodes(ids: $ids) {
                    ... on PositionElectionFrequency {
                        databaseId
                        frequency
                        id
                        referenceYear
                        seats
                        validFrom
                        validTo
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
        logging.debug(f"Sending request for {len(position_database_ids)} positions")
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

        # Extract the nodes from the response
        nodes = data.get("data", {}).get("nodes", [])

        return nodes

    except Exception as e:
        logging.error(f"Error fetching position election frequency data: {str(e)}")
        raise e


position_election_frequency_schema = StructType(
    [
        StructField("databaseId", IntegerType(), True),
        StructField("frequency", ArrayType(IntegerType()), True),
        StructField("id", StringType(), True),
        StructField("referenceYear", IntegerType(), True),
        StructField("seats", ArrayType(IntegerType()), True),
        StructField("validFrom", TimestampType(), True),
        StructField("validTo", TimestampType(), True),
    ]
)


def _get_position_election_frequency_token(ce_api_token: str) -> Callable:
    """
    Wraps the token in a pandas UDF for proper order of operations.

    Args:
        ce_api_token: API token for CivicEngine

    Returns:
        A pandas UDF function
    """

    @pandas_udf(returnType=position_election_frequency_schema)
    def _get_position_election_frequency(
        position_database_ids: pd.Series,
    ) -> pd.DataFrame:
        """
        Pandas UDF that processes batches of Position Database IDs and returns their position election frequency data.
        """
        if not ce_api_token:
            raise ValueError("Missing required environment variable: CE_API_TOKEN")

        # Create a map to store stances by candidacy ID
        position_election_frequency_by_database_id: Dict[int, Dict[str, Any] | None] = (
            {}
        )

        # Set batch size for API calls
        batch_size = 100

        # Process position IDs in batches
        for i in range(0, len(position_database_ids), batch_size):
            batch = position_database_ids[i : i + batch_size]
            batch_size_info = f"Batch {i//batch_size + 1}/{(len(position_database_ids) + batch_size - 1)//batch_size}, size: {len(batch)}"
            logging.debug(f"Processing {batch_size_info}")

            try:
                batch_position_election_frequency = (
                    _get_position_election_frequency_batch(batch, ce_api_token)
                )
            except Exception as e:
                logging.error(f"Error processing batch {i}: {e}")
                raise e

            for position_election_frequency in batch_position_election_frequency:
                if position_election_frequency:
                    position_election_frequency_id = position_election_frequency[
                        "databaseId"
                    ]
                    position_election_frequency_by_database_id[
                        position_election_frequency_id
                    ] = position_election_frequency

                    # handle timestamp columns
                    position_election_frequency["validFrom"] = pd.to_datetime(
                        position_election_frequency["validFrom"]
                    )
                    position_election_frequency["validTo"] = pd.to_datetime(
                        position_election_frequency["validTo"]
                    )

        # order according to position_database_ids and handle missing values
        empty_dictionary = {
            "databaseId": -1,
            "frequency": None,
            "id": None,
            "referenceYear": None,
            "seats": None,
            "validFrom": None,
            "validTo": None,
        }

        position_election_frequency_by_database_id = [
            position_election_frequency_by_database_id.get(
                position_database_id, empty_dictionary
            )
            for position_database_id in position_database_ids
        ]  # type: ignore

        return pd.DataFrame(position_election_frequency_by_database_id)

    return _get_position_election_frequency


def model(dbt, session) -> DataFrame:
    """
    `dbt` Python model to retrieve and process position election frequency data from the CivicEngine API.
    See https://developers.civicengine.com/docs/api/graphql/reference/objects/position-election-frequency
    """
    dbt.config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        on_schema_change="fail",
        tags=["ballotready", "position_election_frequency", "api", "pandas_udf"],
    )

    # Get API token from environment variables
    ce_api_token = dbt.config.get("ce_api_token")
    if not ce_api_token:
        raise ValueError("Missing required config parameter: ce_api_token")

    # Get position election frequency data
    position: DataFrame = dbt.ref("stg_airbyte_source__ballotready_api_position")

    # get the position election frequency data
    position_election_frequency = (
        position.select("election_frequencies")
        .withColumn("election_frequency", explode("election_frequencies"))
        .select(col("election_frequency.databaseId").alias("database_id"))
    )
    position_election_frequency = position_election_frequency.select(
        "database_id"
    ).dropDuplicates(["database_id"])

    # Filter if incremental by updated_at
    if dbt.is_incremental:

        # Get existing database_ids from the incremental table
        existing_records = session.table(f"{dbt.this}")

        # Filter out database_ids that already exist in the table
        position_election_frequency = position_election_frequency.join(
            existing_records.select("database_id"),
            on="database_id",
            how="left_anti",  # Only keep rows that don't match (equivalent to NOT IN)
        )

        logging.info(
            f"Found {position_election_frequency.count()} new position election frequencies to process"
        )

    # Validate source data
    if position_election_frequency.count() == 0:
        logging.warning("No positions found in source table")
        empty_df: DataFrame = session.createDataFrame(
            [],
            StructType(
                [
                    StructField("database_id", IntegerType(), True),
                    StructField("frequency", ArrayType(IntegerType()), True),
                    StructField("id", StringType(), True),
                    StructField("reference_year", IntegerType(), True),
                    StructField("seats", ArrayType(IntegerType()), True),
                    StructField("valid_from", TimestampType(), True),
                    StructField("valid_to", TimestampType(), True),
                ]
            ),
        )
        return empty_df

    # for development/testing purposes (commented out by default)
    # position_election_frequency = position_election_frequency.sample(False, 0.1).limit(
    # 200
    # )

    get_position_election_frequency = _get_position_election_frequency_token(
        ce_api_token
    )
    position_election_frequency = position_election_frequency.withColumn(
        "api_data", get_position_election_frequency(col("database_id"))
    )

    position_election_frequency = position_election_frequency.select(
        col("api_data.databaseId").alias("database_id"),
        col("api_data.frequency").alias("frequency"),
        col("api_data.id").alias("id"),
        col("api_data.referenceYear").alias("reference_year"),
        col("api_data.seats").alias("seats"),
        col("api_data.validFrom").alias("valid_from"),
        col("api_data.validTo").alias("valid_to"),
    )

    # Filter out placeholder records with negative database_id values which were placeholders
    # in pandas UDF
    position_election_frequency = position_election_frequency.filter(
        col("database_id") >= 0
    )
    return position_election_frequency
