import logging
import random
import time
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any, Dict, List

import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
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


def _get_stance(
    candidacy_id: str,
    ce_api_token: str,
    base_sleep: float = 0.1,
    jitter_factor: float = 0.1,
) -> List[Dict[str, Any]]:
    """
    Queries the CivicEngine GraphQL API to get stance IDs for a given candidacy.

    Args:
        candidacy_id: The candidacy ID to get the stance for
        ce_api_token: Authentication token for the CivicEngine API
        base_sleep: Base number of seconds to sleep after making an API call
        jitter_factor: Random factor to apply to sleep time (0.5 means ±50% of base_sleep)

    Returns:
        List of stance dictionaries containing stance data and metadata

    Raises:
        ValueError: If the API response is invalid or unexpected
    """
    url = "https://bpi.civicengine.com/graphql"
    encoded_candidacy_id = _base64_encode_id(candidacy_id)

    # Construct the payload
    payload = {
        "query": """
        query GetCandidacyStances($id: ID!) {
            node(id: $id) {
                ... on Candidacy {
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
        "variables": {"id": encoded_candidacy_id},
    }

    # Add headers with authentication
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {ce_api_token}",
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)

        # Calculate sleep time with jitter to avoid synchronized API calls
        jitter = random.uniform(-jitter_factor, jitter_factor) * base_sleep
        sleep_time = max(0.05, base_sleep + jitter)  # Ensure minimum sleep of 0.05s
        time.sleep(sleep_time)
        response.raise_for_status()
        data = response.json()

        # If the response is empty or has an unexpected structure, log and handle the error
        if not data or not isinstance(data, dict) or "data" not in data:
            raise ValueError("Invalid response from CivicEngine API")

        stances = data.get("data", {}).get("node", {}).get("stances", [])
        if not stances:
            return []

        # Add candidacy_id and encoded_candidacy_id to each stance
        for stance in stances:
            stance["candidacy_id"] = int(candidacy_id)
            stance["encoded_candidacy_id"] = encoded_candidacy_id
        return stances

    except (KeyError, TypeError) as e:
        logging.warning(
            f"Error processing stance for candidacy {candidacy_id}: {str(e)}"
        )
        return []
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for candidacy {candidacy_id}: {str(e)}")
        return []


def model(dbt, session) -> DataFrame:
    """
    `dbt` Python model to retrieve and process stance data from the CivicEngine API.

    This model:
    1. Gets candidate IDs from the candidacies staging table
    2. For incremental runs, filters to only include recently updated records
    3. Fetches stance data for each candidacy from the CivicEngine API
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
        # Add retry configuration for resilience
        on_schema_change="fail",
        # Add additional tags for documentation
        tags=["ballotready", "stance", "api"],
    )

    # Get API token from environment variables
    ce_api_token = dbt.config.get("ce_api_token")
    if not ce_api_token:
        raise ValueError("Missing required config parameter: ce_api_token")

    # Get candidacies and ids
    candidacies: DataFrame = dbt.ref(
        "stg_airbyte_source__ballotready_s3_candidacies_v3"
    )

    # Validate source data
    if candidacies.count() == 0:
        logging.warning("No candidacies found in source table")

    ids_from_candidacies = candidacies.select("candidacy_id").distinct().collect()
    ids_from_candidacies = [row.candidacy_id for row in ids_from_candidacies]
    logging.info(f"INFO: Found {len(ids_from_candidacies)} distinct candidacy IDs")

    # Handle incremental loading
    if dbt.is_incremental:
        logging.info("INFO: Running in incremental mode")
        # Get existing candidacy ids in this table
        existing_candidacy_ids = session.sql(
            f"select distinct(candidacy_id) from {dbt.this}"
        ).collect()[0]

        # Only include records updated in the last 30 days
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        candidacies = candidacies.filter(candidacies["updated_at"] >= thirty_days_ago)
        logging.info(f"INFO: Filtered to candidacies updated since {thirty_days_ago}")
    else:
        logging.info("INFO: Running in full refresh mode")
        existing_candidacy_ids = []

    # Get ids from candidacies that are not in the current table
    candidacy_ids_to_get = [
        candidacy_id
        for candidacy_id in ids_from_candidacies
        if candidacy_id not in existing_candidacy_ids
    ]
    logging.info(f"INFO: Need to fetch {len(candidacy_ids_to_get)} new candidacy IDs")

    # Filter candidacies to only include ids that need to be fetched
    candidacies = candidacies.filter(
        candidacies["candidacy_id"].isin(candidacy_ids_to_get)
    )
    # Dev: take a random sample of 10 rows from stance
    # candidacies = candidacies.sample(False, 0.1).limit(100)

    # Define the return type for the UDF to ensure proper data structure
    stance_schema = ArrayType(
        StructType(
            [
                StructField(name="databaseId", dataType=IntegerType(), nullable=False),
                StructField("id", StringType()),
                StructField(
                    "issue",
                    StructType(
                        [
                            StructField("databaseId", IntegerType()),
                            StructField("id", StringType()),
                        ]
                    ),
                ),
                StructField("locale", StringType()),
                StructField("referenceUrl", StringType()),
                StructField("statement", StringType()),
                StructField("candidacy_id", IntegerType()),
                StructField("encoded_candidacy_id", StringType()),
            ]
        )
    )

    # Create UDF with sleep parameter
    _get_stance_udf = udf(
        f=partial(
            _get_stance, ce_api_token=ce_api_token
        ),  # Adjust base_sleep and jitter_factor as needed
        returnType=stance_schema,
    )

    # Apply the UDF to get stance data for each candidacy
    stance = candidacies.withColumn("stances", _get_stance_udf("candidacy_id"))
    stance = stance.select("candidacy_id", "stances")

    # Add timestamp metadata
    current_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    if dbt.is_incremental:
        # For existing records, keep their original created_at
        # For new records, set to current time
        stance = stance.withColumn(
            "created_at",
            lit(current_time_utc).where(
                ~stance["candidacy_id"].isin(existing_candidacy_ids)
            ),
        )
    else:
        # For initial load, set created_at for all records
        stance = stance.withColumn("created_at", lit(current_time_utc))

    stance = stance.withColumn("updated_at", lit(current_time_utc))

    logging.info(f"INFO: Completed processing with {stance.count()} rows")
    return stance
