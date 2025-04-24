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
        StructField(name="bioText", dataType=StringType(), nullable=True),
        StructField(
            "candidacies",
            ArrayType(
                StructType(
                    [
                        StructField("databaseId", IntegerType(), True),
                        StructField("id", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            "contacts",
            ArrayType(
                StructType(
                    [
                        StructField("email", StringType(), True),
                        StructField("fax", StringType(), True),
                        StructField("phone", StringType(), True),
                        StructField("type", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("createdAt", TimestampType(), True),
        StructField("databaseId", IntegerType(), True),
        StructField(
            "degrees",
            ArrayType(
                StructType(
                    [
                        StructField("databaseId", IntegerType(), True),
                        StructField("degree", StringType(), True),
                        StructField("gradYear", IntegerType(), True),
                        StructField("id", StringType(), True),
                        StructField("major", StringType(), True),
                        StructField("school", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            "experiences",
            ArrayType(
                StructType(
                    [
                        StructField("databaseId", IntegerType(), True),
                        StructField("end", StringType(), True),
                        StructField("id", StringType(), True),
                        StructField("organization", StringType(), True),
                        StructField("start", StringType(), True),
                        StructField("title", StringType(), True),
                        StructField("type", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("firstName", StringType(), True),
        StructField("fullName", StringType(), True),
        StructField("id", StringType(), True),
        StructField(
            "images",
            ArrayType(
                StructType(
                    [
                        StructField("type", StringType(), True),
                        StructField("url", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("lastName", StringType(), True),
        StructField("middleName", StringType(), True),
        StructField("nickname", StringType(), True),
        StructField(
            "officeHolders",
            ArrayType(
                StructType(
                    [
                        StructField("databaseId", IntegerType(), True),
                        StructField("id", StringType(), True),
                    ]
                )
            ),
        ),
        StructField("slug", StringType(), True),
        StructField("suffix", StringType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField(
            "urls",
            ArrayType(
                StructType(
                    [
                        StructField("databaseId", IntegerType(), True),
                        StructField("id", StringType(), True),
                        StructField("type", StringType(), True),
                        StructField("url", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
    ]
)


def _base64_encode_id(person_id: int) -> str:
    """Base64 encodes the person id"""
    id_prefix = "gid://ballot-factory/Candidate/"
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
    timeout: int = 60,
) -> List[Dict[str, Any]]:
    """
    Fetches persons from the BallotReady API in batches.

    This function handles pagination and rate limiting to efficiently retrieve
    person data for a list of IDs.
    """
    url = "https://bpi.civicengine.com/graphql"

    encoded_ids = [_base64_encode_id(person_id) for person_id in person_ids]

    # Construct the payload with the nodes query
    payload = {
        "query": """
        query GetPersonsBatch($ids: [ID!]!) {
            nodes(ids: $ids) {
                ... on Person {
                    bioText
                    candidacies(includeUncertified: true) {
                        databaseId
                        id
                    }
                    contacts {
                        email
                        fax
                        phone
                        type
                    }
                    createdAt
                    databaseId
                    degrees {
                        databaseId
                        degree
                        gradYear
                        id
                        major
                        school
                    }
                    experiences {
                        databaseId
                        end
                        id
                        organization
                        start
                        title
                        type
                    }
                    firstName
                    fullName
                    id
                    images {
                        type
                        url
                    }
                    lastName
                    middleName
                    nickname
                    officeHolders {
                        nodes {
                            databaseId
                            id
                        }
                    }
                    slug
                    suffix
                    updatedAt
                    urls {
                        databaseId
                        id
                        type
                        url
                    }
                }
            }
        }
        """,
        "variables": {"ids": encoded_ids},
    }

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

        # handle necessary transformations to match schema
        for person in persons:
            if person:
                person["createdAt"] = pd.to_datetime(person["createdAt"])
                person["officeHolders"] = person["officeHolders"]["nodes"]
                person["updatedAt"] = pd.to_datetime(person["updatedAt"])
        return persons

    except (KeyError, TypeError) as e:
        logging.error(f"Error processing person batch: {str(e)}")
        raise ValueError(f"Failed to parse person data from API response: {str(e)}")


def _get_person_token(ce_api_token: str) -> Callable:
    """
    Wraps the get_person function in a callable that can be used in a pandas UDF.
    The wrapper is necessary to pass in ce_api_token to the UDF.
    """

    @pandas_udf(PERSON_BR_SCHEMA)
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
                if i == 0:
                    print(persons_by_person_id)

            except Exception as e:
                logging.error(f"Error processing person batch: {e}")
                raise e

        empty_person: Dict[str, Any] = {}
        for field in PERSON_BR_SCHEMA:
            if isinstance(field.dataType, ArrayType):
                empty_person[field.name] = []
            elif isinstance(field.dataType, StructType):
                empty_person[field.name] = {}
            else:
                empty_person[field.name] = None
        persons_list = [persons_by_person_id.get(id, empty_person) for id in person_ids]
        return pd.DataFrame(persons_list)

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
    # if person_ids is empty, return empty DataFrame
    person_ids.cache()
    person_ids_count = person_ids.count()
    if person_ids_count == 0:
        logging.info("INFO: No new or updated persons to process")
        empty_df = session.createDataFrame([], PERSON_BR_SCHEMA)
        empty_df = empty_df.select(
            col("bioText").alias("bio_text"),
            col("candidacies"),
            col("contacts"),
            col("createdAt").alias("created_at"),
            col("databaseId").alias("database_id"),
            col("degrees"),
            col("experiences"),
            col("firstName").alias("first_name"),
            col("fullName").alias("full_name"),
            col("id"),
            col("images"),
            col("lastName").alias("last_name"),
            col("middleName").alias("middle_name"),
            col("nickname"),
            col("officeHolders").alias("office_holders"),
            col("slug"),
            col("suffix"),
            col("updatedAt").alias("updated_at"),
            col("urls"),
        )
        return empty_df

    # TODO: test at higher limit
    # TODO: remove limit
    # filter for 1000 samples (100 is 3 minutes, 1k is 12 minutes, 10k is )
    person_ids = person_ids.limit(10000)
    display(person_ids)  # type: ignore

    # get person data from API
    _get_person = _get_person_token(ce_api_token)
    person = person_ids.withColumn("person", _get_person(col("candidate_database_id")))

    # Explode the person column and extract fields
    person = person.select(
        col("person.bioText").alias("bio_text"),
        col("person.candidacies").alias("candidacies"),
        col("person.contacts").alias("contacts"),
        col("person.createdAt").alias("created_at"),
        col("person.databaseId").alias("database_id"),
        col("person.degrees").alias("degrees"),
        col("person.experiences").alias("experiences"),
        col("person.firstName").alias("first_name"),
        col("person.fullName").alias("full_name"),
        col("person.id").alias("id"),
        col("person.images").alias("images"),
        col("person.lastName").alias("last_name"),
        col("person.middleName").alias("middle_name"),
        col("person.nickname").alias("nickname"),
        col("person.officeHolders").alias("office_holders"),
        col("person.slug").alias("slug"),
        col("person.suffix").alias("suffix"),
        col("person.updatedAt").alias("updated_at"),
        col("person.urls").alias("urls"),
    )

    # Trigger a cache to ensure these transformations are applied before the filter
    person.cache()
    person.count()
    person = person.filter(col("id").isNotNull()).filter(col("database_id").isNotNull())
    return person
