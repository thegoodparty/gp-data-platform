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
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _base64_encode_id(filing_period_id: int) -> str:
    """Base64 encodes the filing period id"""
    id_prefix = "gid://ballot-factory/FilingPeriod/"
    prefixed_id = f"{id_prefix}{filing_period_id}"
    encoded_bytes: bytes = b64encode(prefixed_id.encode("utf-8"))
    encoded_id: str = encoded_bytes.decode("utf-8")
    return encoded_id


def _get_filing_periods_batch(
    filing_period_ids: List[int],
    ce_api_token: str,
    base_sleep: float = 0.05,
    jitter_factor: float = 0.05,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """Fetches filing periods for a batch of filing period IDs using the CivicEngine API."""
    url = "https://bpi.civicengine.com/graphql"

    # Encode all filing period IDs
    encoded_ids = [
        _base64_encode_id(filing_period_id) for filing_period_id in filing_period_ids
    ]

    # Construct the payload with the nodes query
    payload = {
        "query": """
        query GetFilingPeriodsBatch($ids: [ID!]!) {
            nodes(ids: $ids) {
                ... on FilingPeriod {
                    createdAt
                    databaseId
                    endOn
                    id
                    notes
                    startOn
                    type
                    updatedAt
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
        logging.debug(f"Sending request for {len(encoded_ids)} filing periods")
        response = requests.post(url, json=payload, headers=headers, timeout=timeout)

        # Calculate sleep time with jitter to avoid synchronized API calls
        jitter = random.uniform(-jitter_factor, jitter_factor) * base_sleep
        sleep_time = max(0.05, base_sleep + jitter)  # Ensure minimum sleep of 0.05s
        time.sleep(sleep_time)

        response.raise_for_status()

        data = response.json()
        filing_periods: List[Dict[str, Any]] = data.get("data", {}).get("nodes", [])
        return filing_periods

    except (KeyError, TypeError) as e:
        logging.error(f"Error processing filing periods batch: {str(e)}")
        raise ValueError(
            f"Failed to parse filing period data from API response: {str(e)}"
        )
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for filing periods batch: {str(e)}")
        raise RuntimeError(f"Failed to fetch filing period data from API: {str(e)}")


filing_period_schema = StructType(
    [
        StructField("createdAt", TimestampType(), True),
        StructField("databaseId", IntegerType(), True),
        StructField("endOn", DateType(), True),
        StructField("id", StringType(), True),
        StructField("notes", StringType(), True),
        StructField("startOn", DateType(), True),
        StructField("type", StringType(), True),
        StructField("updatedAt", TimestampType(), True),
    ]
)


def _get_filing_period_token(ce_api_token: str) -> Callable:
    """Wraps the get_filing_period function in a callable that can be used in a pandas UDF"""

    @pandas_udf(filing_period_schema)
    def get_filing_period(filing_period_ids: pd.Series) -> pd.DataFrame:
        """
        Pandas UDF that processes batches of filing period IDs and returns their filing periods.

        This function is distributed by Spark to different workers, allowing parallel
        processing of filing period data. Inside each worker, filing period IDs are further
        batched for efficient API calls.
        """
        if not ce_api_token:
            raise ValueError("Missing required environment variable: CE_API_TOKEN")

        filing_periods_by_filing_period_id: Dict[int, Dict[str, Any] | None] = {}

        batch_size = 200

        for i in range(0, len(filing_period_ids), batch_size):
            batch = filing_period_ids[i : i + batch_size]
            batch_size_info = f"Batch {i//batch_size + 1}/{(len(filing_period_ids) + batch_size - 1)//batch_size}, size: {len(batch)}"
            logging.debug(f"Processing {batch_size_info}")

            try:
                batch_filing_periods = _get_filing_periods_batch(batch, ce_api_token)
                # process and organize filing periods by filing period id
                for filing_period in batch_filing_periods:
                    # process ints and timestamps
                    filing_period["databaseId"] = int(filing_period["databaseId"])
                    filing_period["createdAt"] = pd.Timestamp(
                        filing_period["createdAt"]
                    )
                    filing_period["endOn"] = pd.Timestamp(filing_period["endOn"])
                    filing_period["startOn"] = pd.Timestamp(filing_period["startOn"])
                    filing_period["updatedAt"] = pd.Timestamp(
                        filing_period["updatedAt"]
                    )

                    # organize by database id
                    filing_period_id = filing_period["databaseId"]
                    filing_periods_by_filing_period_id[filing_period_id] = filing_period
            except Exception as e:
                logging.error(f"Error processing batch {i//batch_size}: {str(e)}")

        # create a list of dictionaries for each filing period in order of input
        result_data: List[Dict[str, Any]] = []
        for filing_period_id in filing_period_ids:
            filing_period = filing_periods_by_filing_period_id.get(
                int(filing_period_id), {}
            )  # type: ignore
            if filing_period:
                result_data.append(filing_period)
            else:
                result_data.append(
                    {
                        "createdAt": None,
                        "databaseId": -1,  # Use -1 directly for failed records
                        "endOn": None,
                        "id": None,
                        "notes": None,
                        "startOn": None,
                        "type": None,
                        "updatedAt": None,
                    }
                )

        return pd.DataFrame(result_data)

    return get_filing_period


def model(dbt, session) -> DataFrame:
    dbt.config(
        submission_method="all_purpose_cluster",  # required for .cache()
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",  # required for .cache()
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="database_id",
        on_schema_change="fail",
        tags=["ballotready", "filing_period", "api", "pandas_udf"],
    )

    # get API token from Databricks secrets
    dbt_env = dbt.config.get("dbt_environment")
    ce_api_token = dbutils.secrets.get(  # type: ignore[name-defined]
        scope=f"dbt-secrets-{dbt_env}", key="civic-engine-api-token"
    )
    if not ce_api_token:
        raise ValueError("Missing required secret: civic-engine-api-token")

    # get unique filing period ids from race
    race: DataFrame = dbt.ref("stg_airbyte_source__ballotready_api_race")

    if dbt.is_incremental:
        logging.info("INFO: Running in incremental mode")
        existing_table = session.table(f"{dbt.this}")
        existing_table = existing_table.select("updated_at")

        # get the latest updated_at date from the existing table
        latest_updated_at = existing_table.agg({"updated_at": "max"}).collect()[0][0]

        # filter the race dataframe to only include rows with an updated_at date after the latest updated_at date
        race = race.filter(col("updated_at") > latest_updated_at)

    # explode the filing_periods column, deduplicate, rename to database_id and drop nulls
    filing_periods: DataFrame = race.select("filing_periods").withColumn(
        "filing_periods", explode("filing_periods")
    )
    filing_periods = (
        filing_periods.select("filing_periods.databaseId").distinct()
    ).withColumnRenamed("databaseId", "database_id")
    filing_periods = filing_periods.filter(col("database_id").isNotNull())

    # Trigger a cache to ensure these transformations are applied. This is important for incremental models to avoid unnecessary API calls
    filing_periods.cache()
    filing_periods_count = filing_periods.count()
    logging.info(f"Found {filing_periods_count} new filing periods to process")

    # if filing_periods is empty, return an empty dataframe
    if filing_periods.count() == 0:
        logging.info("INFO: No new or updated filing periods to process")
        return session.createDataFrame(
            [],
            StructType(
                [
                    StructField("created_at", TimestampType(), True),
                    StructField("database_id", IntegerType(), True),
                    StructField("end_on", DateType(), True),
                    StructField("id", StringType(), True),
                    StructField("notes", StringType(), True),
                    StructField("start_on", DateType(), True),
                    StructField("type", StringType(), True),
                    StructField("updated_at", TimestampType(), True),
                ]
            ),
        )

    # get filing period data from API. This is a long operation,
    # downsample with `.sample(False, 0.1).limit(10000)` if needed
    get_filing_period = _get_filing_period_token(ce_api_token)

    # First get the filing period data as a struct, then extract each field into its own column
    filing_periods = filing_periods.withColumn(
        "filing_period_data", get_filing_period(col("database_id"))
    )
    result = filing_periods.select(
        col("filing_period_data.createdAt").alias("created_at"),
        col("filing_period_data.databaseId").alias("database_id"),
        col("filing_period_data.endOn").alias("end_on"),
        col("filing_period_data.id").alias("id"),
        col("filing_period_data.notes").alias("notes"),
        col("filing_period_data.startOn").alias("start_on"),
        col("filing_period_data.type").alias("type"),
        col("filing_period_data.updatedAt").alias("updated_at"),
    )

    # Drop rows with database_id -1, which is a placeholder for failed records
    # Trigger a cache to ensure these transformations are applied before the filter
    result.cache()
    result.count()
    result = result.filter(col("database_id") != -1)
    result = result.filter(col("database_id").isNotNull())
    result = result.filter(col("id").isNotNull())
    return result
