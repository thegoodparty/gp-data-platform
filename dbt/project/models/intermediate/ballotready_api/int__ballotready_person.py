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
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PERSON_BR_SCHEMA = StructType(
    [
        StructField(name="bioText", dataType=StringType(), nullable=False),
        StructField(
            "candidacies",
            ArrayType(
                StructType(
                    [
                        StructField("databaseId", StringType(), False),
                        StructField("id", StringType(), False),
                    ]
                ),
                False,
            ),
            False,
        ),
        StructField("createdAt", TimestampType(), False),
        StructField("databaseId", IntegerType(), False),
        StructField(
            "degrees",
            ArrayType(
                StructType(
                    [
                        StructField("databaseId", StringType(), False),
                        StructField("id", StringType(), False),
                    ]
                ),
                False,
            ),
            False,
        ),
        StructField(
            "experiences",
            ArrayType(
                StructType(
                    [
                        StructField("databaseId", StringType(), False),
                        StructField("id", StringType(), False),
                    ]
                ),
                False,
            ),
            False,
        ),
        StructField("firstName", StringType(), False),
        StructField("fullName", StringType(), False),
        StructField("id", StringType(), False),
        StructField(
            "images",
            ArrayType(
                StructType(
                    [
                        StructField("type", StringType(), False),
                        StructField("url", StringType(), False),
                    ]
                ),
                False,
            ),
            False,
        ),
        StructField("lastName", StringType(), False),
        StructField("middleName", StringType(), False),
        StructField("nickname", StringType(), False),
        StructField(
            "officeHolders",
            ArrayType(
                StructType(
                    [
                        StructField("databaseId", StringType(), False),
                        StructField("id", StringType(), False),
                    ]
                )
            ),
        ),
        StructField("slug", StringType(), False),
        StructField("suffix", StringType(), False),
        StructField("updatedAt", TimestampType(), False),
        StructField(
            "urls",
            ArrayType(
                StructType(
                    [
                        StructField("databaseId", IntegerType(), False),
                        StructField("id", StringType(), False),
                    ]
                ),
                False,
            ),
            False,
        ),
    ]
)


def _base64_encode_id(person_id: int) -> str:
    """Base64 encodes the person id"""
    id_prefix = "gid://ballot-factory/Person/"
    prefixed_id = f"{id_prefix}{person_id}"
    encoded_bytes: bytes = b64encode(prefixed_id.encode("utf-8"))
    encoded_id: str = encoded_bytes.decode("utf-8")
    return encoded_id


@pandas_udf(StringType())
def _base64_encode_id_udf(person_id: pd.Series) -> pd.Series:
    return pd.Series([_base64_encode_id(x) for x in person_id])


def _get_person_batch(
    person_ids: List[int],
    ce_api_token: str,
    base_sleep: float = 0.1,
    jitter_factor: float = 0.1,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Fetches persons from the BallotReady API in batches.

    This function handles pagination and rate limiting to efficiently retrieve
    person data for a list of IDs.
    """
    url = "https://bpi.civicengine.com/graphql"

    encoded_ids = [_base64_encode_id(person_id) for person_id in person_ids]

    # Construct the payload with the nodes query
    # TODO: complete payload query
    payload = {
        "query": """
        query GetPersonsBatch($ids: [ID!]!) {
            nodes(ids: $ids) {
                ... on Person {
                    bioText
                    candidacies {
                        databaseId
                        id
                    }
                    createdAt
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
        logging.debug(f"Sending request for {len(encoded_ids)} persons")
        response = requests.post(url, json=payload, headers=headers, timeout=timeout)

        # Calculate sleep time with jitter to avoid synchronized API calls
        jitter = random.uniform(-jitter_factor, jitter_factor) * base_sleep
        sleep_time = max(0.05, base_sleep + jitter)  # Ensure minimum sleep of 0.05s
        time.sleep(sleep_time)

        # Parse the response
        response_data = response.json()
        logging.debug(f"Received response for {len(encoded_ids)} persons")
        persons: List[Dict[str, Any]] = response_data["data"]["nodes"]

        # process each entry (rename, handle timestamps)
        # TODO: complete renaming and data types to match schema
        for person in persons:
            if person:
                person["databaseId"] = int(person["databaseId"])
                person["createdAt"] = pd.to_datetime(person["createdAt"])
                person["database_id"] = int(person["databaseId"])
        return persons

    except (KeyError, TypeError) as e:
        logging.error(f"Error processing person batch: {str(e)}")
        raise ValueError(f"Failed to parse person data from API response: {str(e)}")


def _get_person_token(ce_api_token: str) -> Callable:
    """
    Wraps the get_person function in a callable that can be used in a pandas UDF.
    The wrapper is necessary to pass in ce_api_token to the UDF.
    """

    @pandas_udf(StringType())
    # @pandas_udf(PERSON_BR_SCHEMA)
    def get_person(person_ids: pd.Series) -> pd.DataFrame:
        """
        Pandas UDF that processes batches of person IDs and returns their persons.
        """
        if not ce_api_token:
            raise ValueError("Missing required environment variable: CE_API_TOKEN")

        persons_by_person_id: Dict[int, Dict[str, Any] | None] = {}

        batch_size = 100

        for i in range(0, len(person_ids), batch_size):
            batch = person_ids[i : i + batch_size]
            batch_size_info = f"Batch {i//batch_size + 1}/{(len(person_ids) + batch_size - 1)//batch_size}, size: {len(batch)}"
            logging.debug(f"Processing {batch_size_info}")

            try:
                batch_persons = _get_person_batch(batch, ce_api_token)
                # process and organize persons by person id
                for person in batch_persons:
                    if person:
                        persons_by_person_id[person["databaseId"]] = person

            except Exception as e:
                logging.error(f"Error processing person batch: {e}")
                raise e

        # create a list of dictionaries for each person in order of input
        # TODO: handle missing persons with dictionary of key list
        # persons_list = [persons_by_person_id.get(id)for id in person_ids]
        # return pd.DataFrame(persons_list)

        from json import dumps

        persons_list = [dumps(persons_by_person_id.get(id)) for id in person_ids]
        return persons_list

    return get_person


def model(dbt, session) -> DataFrame:
    # configure the model
    dbt.config(
        submission_method="all_purpose_cluster",  # required for .cache()
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",  # required for .cache()
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="database_id",
        tags=["intermediate", "ballotready", "person", "api", "pandas_udf"],
    )

    # get api token from environment variables
    ce_api_token = dbt.config.get("ce_api_token")
    if not ce_api_token:
        raise ValueError("Missing required config parameter: ce_api_token")

    # get persons
    candidacy: DataFrame = dbt.ref("int__ballotready_candidacy")

    if dbt.is_incremental:
        existing_table = session.table(f"{dbt.this}")
        max_updated_at_row = existing_table.agg({"updated_at": "max"}).collect()[0]
        max_updated_at = max_updated_at_row[0] if max_updated_at_row else None

        if max_updated_at:
            candidacy = candidacy.filter(candidacy["updated_at"] >= max_updated_at)

    # get distinct person IDs
    person_ids = candidacy.select("candidate_database_id").distinct()

    # Trigger a cache to ensure these transformations are applied before the filter
    # if candidacy_id is empty, return empty DataFrame
    person_ids.cache()
    person_ids_count = person_ids.count()

    if person_ids_count == 0:
        logging.info("INFO: No new or updated persons to process")
        empty_df = session.createDataFrame([], PERSON_BR_SCHEMA)
        # TODO: rename columns to match expected output
        return empty_df

    # get person data from API
    _get_person = _get_person_token(ce_api_token)
    person = person_ids.withColumn("person", _get_person(col("person_database_id")))

    return person
