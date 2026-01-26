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


def _base64_encode_id(br_candidacy_id: str) -> str:
    """
    Encodes a candidacy ID into the format required by CivicEngine API.

    Args:
        br_candidacy_id: The raw candidacy ID to encode.

    Returns:
        The base64-encoded ID string with the proper prefix.
    """
    id_prefix = "gid://ballot-factory/Candidacy/"
    prefixed_id = f"{id_prefix}{br_candidacy_id}"
    encoded_bytes: bytes = b64encode(prefixed_id.encode("utf-8"))
    encoded_id: str = encoded_bytes.decode("utf-8")
    return encoded_id


def _get_parties_batch(
    br_candidacy_ids: List[str],
    ce_api_token: str,
    base_sleep: float = 0.1,
    jitter_factor: float = 0.1,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Fetches parties for a batch of candidacy IDs from the CivicEngine GraphQL API.

    Args:
        br_candidacy_ids: List of candidacy IDs to fetch
        ce_api_token: API token for CivicEngine
        base_sleep: Base sleep time for backoff
        jitter_factor: Jitter factor for backoff
        timeout: Timeout for the API call

    Returns:
        List of dictionaries containing party data
    """
    url = "https://bpi.civicengine.com/graphql"
    if not br_candidacy_ids:
        return []
    all_parties = []

    # Encode candidacy IDs for the GraphQL query
    encoded_ids = [_base64_encode_id(str(cid)) for cid in br_candidacy_ids]

    # Build the GraphQL query
    query = """
    query GetCandidacyParties($ids: [ID!]!) {
      nodes(ids: $ids) {
        ... on Candidacy {
          id
          databaseId
          parties {
            createdAt
            databaseId
            id
            name
            shortName
            updatedAt
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

    # Make the API request with retry logic
    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=timeout,
            )

            if response.status_code == 200:
                data = response.json()

                # Safely navigate through the response, handling None values at any level
                data_dict = data.get("data") or {}
                nodes_list = data_dict.get("nodes", [])

                for node in nodes_list:
                    if not node:
                        continue

                    candidacy_db_id = node.get("databaseId")
                    encoded_br_candidacy_id = node.get("id")

                    if not candidacy_db_id:
                        continue

                    parties = node.get("parties", [])
                    if not parties:
                        continue

                    # process each party
                    for party in parties:
                        # Add br_candidacy_id and encoded_br_candidacy_id to each party
                        party["br_candidacy_id"] = int(candidacy_db_id)
                        party["encoded_br_candidacy_id"] = encoded_br_candidacy_id

                        # handle timestamps with timezone for pandas -> arrow -> spark
                        party["createdAt"] = pd.to_datetime(party["createdAt"])
                        party["updatedAt"] = pd.to_datetime(party["updatedAt"])
                        all_parties.append(party)

                    logging.debug(
                        f"Retrieved {len(all_parties)} endorsements for {len(br_candidacy_ids)} candidacies"
                    )
                return all_parties

            # Handle API rate limiting
            if response.status_code == 429:
                sleep_time = base_sleep * (2**attempt) + random.random() * jitter_factor
                logging.warning(
                    f"Rate limited. Retrying in {sleep_time:.2f} seconds..."
                )
                time.sleep(sleep_time)
                continue

            # Handle other errors
            logging.error(
                f"API request failed with status code {response.status_code}: {response.text}"
            )
            return []

        except requests.exceptions.RequestException as e:
            sleep_time = base_sleep * (2**attempt) + random.random() * jitter_factor
            logging.warning(
                f"Request error: {e}. Retrying in {sleep_time:.2f} seconds..."
            )
            time.sleep(sleep_time)

    logging.error(f"Failed to fetch parties after {max_retries} attempts.")
    return []


# Schema for party data
party_schema = ArrayType(
    StructType(
        [
            StructField(name="createdAt", dataType=TimestampType(), nullable=False),
            StructField("databaseId", IntegerType(), False),
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("shortName", StringType(), False),
            StructField("updatedAt", TimestampType(), False),
        ]
    )
)


def _get_candidacy_parties_token(ce_api_token: str) -> Callable:
    """
    Creates a pandas UDF that fetches parties for candidacy IDs.

    Args:
        ce_api_token: API token for CivicEngine

    Returns:
        A pandas UDF function
    """

    @pandas_udf(returnType=party_schema)
    # @pandas_udf(returnType=StringType())
    def get_candidacy_parties(br_candidacy_ids: pd.Series) -> pd.Series:
        """
        Pandas UDF to process candidacy IDs in batches and fetch their parties.

        Args:
            br_candidacy_ids: Series of candidacy IDs

        Returns:
            Series of party data
        """
        batch_size = 50  # Adjust batch size based on API limits
        parties_by_candidacy: Dict[int, List[Dict[str, Any]]] = {}

        # Process in batches
        for i in range(0, len(br_candidacy_ids), batch_size):
            batch = br_candidacy_ids.iloc[i : i + batch_size].tolist()
            batch_response = _get_parties_batch(batch, ce_api_token)

            # Organize parties by br_candidacy_id
            for party in batch_response:
                cid = party["br_candidacy_id"]
                if cid not in parties_by_candidacy:
                    parties_by_candidacy[cid] = []
                parties_by_candidacy[cid].append(party)

        # Create result series mapping each candidacy ID to its endorsements array
        result = pd.Series(
            [parties_by_candidacy.get(int(cid), []) for cid in br_candidacy_ids]
        )
        return pd.Series(result)

    return get_candidacy_parties


def model(dbt, session) -> DataFrame:
    """
    Main dbt model function to process candidacy parties.

    Args:
        dbt: dbt context
        session: Spark session

    Returns:
        DataFrame containing candidacy parties
    """
    # Configure the model
    dbt.config(
        submission_method="all_purpose_cluster",  # required for .cache()
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",  # required for .cache()
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="br_candidacy_id",
        on_schema_change="fail",
        tags=["ballotready", "party", "api", "pandas_udf"],
    )

    # Configure logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Get API token from Databricks secrets
    dbt_env = dbt.config.get("dbt_environment")
    ce_api_token = dbutils.secrets.get(  # type: ignore[name-defined]
        scope=f"dbt-secrets-{dbt_env}", key="civic-engine-api-token"
    )
    if not ce_api_token:
        raise ValueError("Missing required secret: civic-engine-api-token")

    # Get references to required models
    candidacies: DataFrame = dbt.ref(
        "stg_airbyte_source__ballotready_s3_candidacies_v3"
    )

    # Check for incremental run
    if dbt.is_incremental:
        logging.info("INFO: Running in incremental mode")
        existing_table = session.table(f"{dbt.this}")
        existing_timestamps = existing_table.select(
            "br_candidacy_id", "created_at"
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

    # Trigger a cache to ensure filters above are applied before making API calls
    candidacies.cache()
    candidacies_count = candidacies.count()

    # If no records to process after filtering, return early
    if candidacies_count == 0 and dbt.is_incremental:
        logging.info("INFO: No new or updated candidacies to process")
        # Return empty DataFrame with correct schema
        schema = StructType(
            [
                StructField("br_candidacy_id", IntegerType()),
                StructField("parties", party_schema),
                StructField("created_at", TimestampType()),
                StructField("updated_at", TimestampType()),
            ]
        )
        return session.createDataFrame([], schema)

    # For development/testing purposes (commented out by default)
    # candidacies = candidacies.sample(False, 0.1).limit(1000)

    # Process candidacies using the pandas UDF for parallel processing
    logging.info("INFO: Starting parallel processing of candidacies using pandas UDF")

    # Create a DataFrame with just br_candidacy_ids for processing
    party = candidacies.select(
        col("br_candidacy_id").cast("integer").alias("br_candidacy_id")
    )

    # Apply the pandas UDF to get parties for each candidacy
    get_candidacy_parties = _get_candidacy_parties_token(ce_api_token)
    party = party.withColumn("parties", get_candidacy_parties("br_candidacy_id"))
    logging.info(f"INFO: Processed {party.count()} candidacies with pandas UDF")

    # Add timestamp metadata
    current_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    if dbt.is_incremental:
        # Prepare a lookup DataFrame with existing br_candidacy_ids and their original created_at values
        existing_created_at_lookup = existing_timestamps

        # Left join with this lookup to preserve original created_at values for existing records
        party = party.join(existing_created_at_lookup, on="br_candidacy_id", how="left")

        # Use coalesce to keep original created_at for existing records, and set current time for new ones
        party = party.withColumn(
            "created_at",
            coalesce(col("created_at"), lit(current_time_utc).cast(TimestampType())),
        )
    else:
        # For full refresh, set created_at to current time for all records
        party = party.withColumn(
            "created_at", lit(current_time_utc).cast(TimestampType())
        )

    # Set updated_at to current time for all records
    party = party.withColumn("updated_at", lit(current_time_utc).cast(TimestampType()))

    # Count and log the final row count
    row_count = party.count()
    logging.info(f"INFO: Completed processing with {row_count} rows")

    return party
