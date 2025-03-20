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
    ArrayType,
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CANDIDACY_SCHEMA = StructType(
    [
        StructField("candidate_person_database_id", IntegerType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("database_id", IntegerType(), True),
        StructField("election_database_id", IntegerType(), True),
        StructField(
            "endorsements",
            ArrayType(
                [
                    StructField("databaseId", IntegerType(), True),
                    StructField("id", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("id", StringType(), True),
        StructField("is_certified", BooleanType(), True),
        StructField("is_hidden", BooleanType(), True),
        StructField(
            "parties",
            ArrayType(
                [
                    StructField("databaseId", IntegerType(), True),
                    StructField("id", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("position_database_id", IntegerType(), True),
        StructField("race_database_id", IntegerType(), True),
        StructField("result", StringType(), True),
        StructField(
            "stances",
            ArrayType(
                [
                    StructField("databaseId", IntegerType(), True),
                    StructField("id", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("updated_at", TimestampType(), True),
        StructField("withdrawn", BooleanType(), True),
    ]
)


def _base64_encode_id(candidacy_id: int) -> str:
    """Base64 encodes the candidacy id"""
    id_prefix = "gid://ballot-factory/Candidacy/"
    prefixed_id = f"{id_prefix}{candidacy_id}"
    encoded_bytes: bytes = b64encode(prefixed_id.encode("utf-8"))
    encoded_id: str = encoded_bytes.decode("utf-8")
    return encoded_id


def _get_candidacy_batch(
    candidacy_ids: List[int],
    ce_api_token: str,
    base_sleep: float = 0.1,
    jitter_factor: float = 0.1,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Fetches candidacies from the BallotReady API in batches.

    This function handles pagination and rate limiting to efficiently retrieve
    candidacy data for a list of IDs.
    """
    url = "https://api.ballotready.org/graphql"

    encoded_ids = [_base64_encode_id(candidacy_id) for candidacy_id in candidacy_ids]

    # Construct the payload with the nodes query
    payload = {
        "query": """
        query GetCandidaciesBatch($ids: [ID!]!) {
            nodes(ids: $ids) {
                ... on Candidacy {
                    Person {
                        databaseId
                    }
                    createdAt
                    databaseId
                    election {
                        databaseId
                    }
                    endorsements {
                        databaseId
                        id
                    }
                    id
                    isCertified
                    isHidden
                    parties {
                        databaseId
                        id
                    }
                    position {
                        databaseId
                    }
                    race {
                        databaseId
                    }
                    result
                    stances {
                        databaseId
                        id
                    }
                    updatedAt
                    withdrawn
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
        logging.debug(f"Sending request for {len(encoded_ids)} candidacies")
        response = requests.post(url, json=payload, headers=headers)

        # Calculate sleep time with jitter to avoid synchronized API calls
        jitter = random.uniform(-jitter_factor, jitter_factor) * base_sleep
        sleep_time = max(0.05, base_sleep + jitter)  # Ensure minimum sleep of 0.05s
        time.sleep(sleep_time)

        response.raise_for_status()
        data = response.json()
        candidacies: List[Dict[str, Any]] = data["data"]["nodes"]

        # process each entry (rename, handle timestamps)
        for candidacy in candidacies:
            candidacy["candidate_person_database_id"] = int(
                candidacy["Person"]["databaseId"]
            )
            candidacy["created_at"] = pd.to_datetime(candidacy["createdAt"])
            candidacy["database_id"] = int(candidacy["databaseId"])
            candidacy["election_database_id"] = int(candidacy["election"]["databaseId"])
            candidacy["is_certified"] = candidacy["isCertified"]
            candidacy["is_hidden"] = candidacy["isHidden"]
            candidacy["position_database_id"] = int(candidacy["position"]["databaseId"])
            candidacy["race_database_id"] = int(candidacy["race"]["databaseId"])
            candidacy["updated_at"] = pd.to_datetime(candidacy["updatedAt"])
        return candidacies

    except (KeyError, TypeError) as e:
        logging.error(f"Error processing candidacy batch: {str(e)}")
        raise ValueError(f"Failed to parse candidacy data from API response: {str(e)}")
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for candidacy batch: {str(e)}")
        raise RuntimeError(f"Failed to fetch candidacy data from API: {str(e)}")


def _get_candidacy_token(ce_api_token: str) -> Callable:
    """
    Wraps the get_candidacy function in a callable that can be used in a pandas UDF.
    The wrapper is necessary to pass in ce_api_token to the UDF.
    """

    @pandas_udf(CANDIDACY_SCHEMA)
    def get_candidacy(candidacy_ids: pd.Series) -> pd.DataFrame:
        """
        Pandas UDF that processes batches of candidacy IDs and returns their candidacies.

        This function is distributed by Spark to different workers, allowing parallel
        processing of candidacy data. Inside each worker, candidacy IDs are further
        batched for efficient API calls.
        """
        if not ce_api_token:
            raise ValueError("Missing required environment variable: CE_API_TOKEN")

        candidacies_by_candidacy_id: Dict[int, Dict[str, Any] | None] = {}

        batch_size = 100

        for i in range(0, len(candidacy_ids), batch_size):
            batch = candidacy_ids[i : i + batch_size]
            batch_size_info = f"Batch {i//batch_size + 1}/{(len(candidacy_ids) + batch_size - 1)//batch_size}, size: {len(batch)}"
            logging.debug(f"Processing {batch_size_info}")

            try:
                batch_candidacies = _get_candidacy_batch(batch, ce_api_token)
                # process and organize candidacies by candidacy id
                for candidacy in batch_candidacies:
                    candidacies_by_candidacy_id[candidacy["databaseId"]] = candidacy

            except Exception as e:
                logging.error(f"Error processing candidacy batch: {e}")
                raise e

        # create a list of dictionaries for each candidacy in order of input
        result_data: List[Dict[str, Any]] = []
        for candidacy_id in candidacy_ids:
            candidacy = candidacies_by_candidacy_id.get(int(candidacy_id), {})  # type: ignore
            if candidacy:
                result_data.append(candidacy)
            else:
                result_data.append(
                    {
                        "candidate_person_database_id": None,
                        "created_at": None,
                        "database_id": -1,
                        "election_database_id": None,
                        "endorsements": None,
                        "id": None,
                        "is_certified": None,
                        "is_hidden": None,
                        "parties": None,
                        "position_database_id": None,
                        "race_database_id": None,
                        "result": None,
                        "stances": None,
                        "updated_at": None,
                        "withdrawn": None,
                    }
                )
        return pd.DataFrame(result_data)

    return get_candidacy


def model(dbt, session) -> DataFrame:
    # configure the model
    dbt.config(
        materialized="table",
        incremental_strategy="merge",
        unique_key="database_id",
        on_schema_change="fail",
        tags=["ballotready", "candidacies", "api", "pandas_udf"],
    )

    # get api token from environment variables
    ce_api_token = dbt.config.get("ce_api_token")
    if not ce_api_token:
        raise ValueError("Missing required config parameter: ce_api_token")

    # get candidacies
    candidacies_s3: DataFrame = dbt.ref(
        "stg_airbyte_source__ballotready_s3_candidacy_v3"
    )

    if dbt.is_incremental:
        existing_table = session.table(f"{dbt.this}")
        max_updated_at_row = existing_table.agg({"updated_at": "max"}).collect()[0]
        max_updated_at = max_updated_at_row[0] if max_updated_at_row else None

        if max_updated_at:
            candidacies_s3 = candidacies_s3.filter(
                candidacies_s3["updated_at"] >= max_updated_at
            )

    # get distinct candidacy IDs
    ids_from_candidacies = candidacies_s3.select("candidacy_id").distinct().collect()
    ids_from_candidacies = [row.candidacy_id for row in ids_from_candidacies]
    logging.info(f"INFO: Found {len(ids_from_candidacies)} distinct candidacy IDs")

    if len(ids_from_candidacies) == 0:
        logging.info("INFO: No new or updated candidacies to process")
        return session.createDataFrame([], CANDIDACY_SCHEMA)

    # downsample in dev for quicker testing
    if dbt.get_config("dbt_environment") != "prod":
        candidacies_s3 = candidacies_s3.sample(False, 0.1).limit(1000)

    # get candidacy data from API
    get_candidacy = _get_candidacy_token(ce_api_token)
    candidacies = candidacies_s3.withColumn(
        "candidacy", get_candidacy(col("candidacy_id"))
    )

    candidacies = candidacies.select(
        col("candidacy.candidate_person_database_id").alias(
            "candidate_person_database_id"
        ),
        col("candidacy.created_at").alias("created_at"),
        col("candidacy.database_id").alias("database_id"),
        col("candidacy.election_database_id").alias("election_database_id"),
        col("candidacy.endorsements").alias("endorsements"),
        col("candidacy.id").alias("id"),
        col("candidacy.is_certified").alias("is_certified"),
        col("candidacy.is_hidden").alias("is_hidden"),
        col("candidacy.parties").alias("parties"),
        col("candidacy.position_database_id").alias("position_database_id"),
        col("candidacy.race_database_id").alias("race_database_id"),
        col("candidacy.result").alias("result"),
        col("candidacy.stances").alias("stances"),
        col("candidacy.updated_at").alias("updated_at"),
        col("candidacy.withdrawn").alias("withdrawn"),
    )

    # remove cases where database_id is -1 which was a placeholder
    candidacies = candidacies.filter(candidacies["database_id"] != -1)
    return candidacies
