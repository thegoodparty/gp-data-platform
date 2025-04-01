import logging
import random
import time
from typing import Any, Callable, Dict, List

import pandas as pd
import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, pandas_udf, substring
from pyspark.sql.types import (
    ArrayType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

RACE_SCHEMA = ArrayType(
    StructType(
        [
            StructField("zip_code", IntegerType(), True),
            StructField(name="databaseId", dataType=IntegerType(), nullable=True),
            StructField("id", StringType(), True),
            StructField(
                "election",
                StructType(
                    [
                        StructField("databaseId", IntegerType(), True),
                        StructField("id", StringType(), True),
                        StructField("electionDay", DateType(), True),
                    ]
                ),
                True,
            ),
            StructField(
                "position",
                StructType(
                    [
                        StructField("databaseId", IntegerType(), True),
                        StructField("id", StringType(), True),
                        StructField("level", StringType(), True),
                    ]
                ),
                True,
            ),
        ]
    )
)


def _get_races_by_zip_batch(
    zip_codes: List[int],
    ce_api_token: str,
    base_sleep: float = 0.01,
    jitter_factor: float = 0.01,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Fetches races for a given zip code.
    """
    url = "https://bpi.civicengine.com/graphql"

    """
    query Mtfcc {
        mtfcc {
            nodes {
                databaseId
                id
                # createdAt
                mtfcc
                name
                # updatedAt
            }
        }
    }
    """
    query = """
    query GetRacesByZip($zipCodes: [ID!]!) {
        races(
            location: {
                zip: $zipCodes
            }
        ) {
            id
            databaseId
            election {
                id
                databaseId
                electionDay
            }
            position {
                id
                databaseId
                level
            }
        }
    """
    variables = {"zipCodes": zip_codes}
    payload = {"query": query, "variables": variables}

    headers = {
        "Authorization": f"Bearer {ce_api_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    # Make the API request with retry logic
    max_retries = 5
    for _attempt in range(max_retries):
        try:
            response = requests.post(
                url, headers=headers, json=payload, timeout=timeout
            )
            races = response.json().get("data", {}).get("races", [])
            for race in races:
                race["zip_code"] = zip_codes[0]
            return races
        except Exception as e:
            logging.error(f"Error fetching races for zip codes {zip_codes}: {e}")
            time.sleep(base_sleep + random.uniform(0, jitter_factor))

    raise Exception(
        f"Failed to fetch races for zip codes {zip_codes} after {max_retries} attempts"
    )


def _get_races_by_zip_token(ce_api_token: str) -> Callable:
    """
    Creates a pandas UDF that fetches races for a given zip code.

    Args:
        ce_api_token: API token for CivicEngine

    Returns:
        A pandas UDF function
    """

    @pandas_udf(returnType=RACE_SCHEMA)
    def _get_races_by_zip(zip_code: pd.Series) -> pd.Series:
        """
        Pandas UDF to process zip codes in batches and fetch their races.

        Args:
            zip_code: Series of zip codes

        Returns:
            Series of races
        """
        # Adjust batch size based on API limits
        # Note that it may be required to send a single zip code at a time to maintain
        # zip code order as it's not returned in the API response, and it's 1:many here
        batch_size = 1
        races_by_zip: Dict[int, List[Dict[str, Any]]] = {}

        # process in batches
        for i in range(0, len(zip_code), batch_size):
            batch = zip_code.iloc[i : i + batch_size].tolist()
            batch_response = _get_races_by_zip_batch(batch, ce_api_token)

            for race in batch_response:
                # TODO: fix the type error here
                races_by_zip[race["zip_code"]] = race  # type: ignore

    return _get_races_by_zip


def model(dbt, session) -> DataFrame:
    dbt.config(
        materialized="incremental",
        unique_key="zip_code",
        on_schema_change="fail",
        tags=["ballotready", "races", "zip", "pandas_udf"],
    )

    # configure logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Get API token from environment variable
    ce_api_token = dbt.config.get("ce_api_token")
    if not ce_api_token:
        raise ValueError("CE_API_TOKEN environment variable must be set")

    # get references to required models
    zip_codes: DataFrame = dbt.ref("us_zip_codes")

    # take just the first 5 digits of the integer zip code
    zip_codes = zip_codes.withColumn(
        "zip_code",
        substring(col("zip_code").cast(StringType()), 1, 5).cast(IntegerType()),
    )

    if dbt.is_incremental:
        this_table: DataFrame = dbt.this

        # check for any new zip codes in the zip_codes table
        new_zip_codes = zip_codes.select("zip_code").subtract(
            this_table.select("zip_code")
        )
        if new_zip_codes.count() > 0:
            return session.createDataFrame([], this_table.schema)

    # For development/testing purposes (commented out by default)
    # candidacies = candidacies.sample(False, 0.1).limit(1000)

    # apply the pandas UDF to get races for each zip code
    get_races_by_zip = _get_races_by_zip_token(ce_api_token)
    races_by_zip = this_table.withColumn("races", get_races_by_zip("zip_code"))

    return races_by_zip
