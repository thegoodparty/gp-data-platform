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
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

OFFICE_HOLDER_BR_SCHEMA = StructType(
    [
        StructField(
            name="addresses",
            dataType=ArrayType(
                StructType(
                    [
                        StructField("addressLine1", StringType(), True),
                        StructField("addressLine2", StringType(), True),
                        StructField("city", StringType(), True),
                        StructField("country", StringType(), True),
                        StructField("createdAt", TimestampType(), True),
                        StructField("databaseId", IntegerType(), True),
                        StructField("id", StringType(), True),
                        StructField("state", StringType(), True),
                        StructField("updatedAt", TimestampType(), True),
                        StructField("zip", StringType(), True),
                    ]
                )
            ),
            nullable=True,
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
                )
            ),
            True,
        ),
        StructField("createdAt", TimestampType(), True),
        StructField("databaseId", IntegerType(), True),
        StructField("endAt", DateType(), True),
        StructField("id", StringType(), True),
        StructField("isAppointed", BooleanType(), True),
        StructField("isCurrent", BooleanType(), True),
        StructField("isOffCycle", BooleanType(), True),
        StructField("isVacant", BooleanType(), True),
        StructField("officeTitle", StringType(), True),
        StructField(
            "parties",
            ArrayType(
                StructType(
                    [
                        StructField("databaseId", IntegerType(), True),
                        StructField("id", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField(
            "person",
            StructType(
                [
                    StructField("databaseId", IntegerType(), True),
                    StructField("id", StringType(), True),
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
                ]
            ),
            True,
        ),
        StructField("specificity", StringType(), True),
        StructField("startAt", DateType(), True),
        StructField("totalYearsInOffice", IntegerType(), True),
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
                )
            ),
            True,
        ),
    ]
)


def _base64_encode_id(office_holder_id: int) -> str:
    """
    Encodes an office holder ID to a base64 string.
    """
    id_prefix = "gid://ballot-factory/OfficeHolder/"
    prefixed_id = f"{id_prefix}{office_holder_id}"
    encoded_bytes: bytes = b64encode(prefixed_id.encode("utf-8"))
    encoded_id: str = encoded_bytes.decode("utf-8")
    return encoded_id


def _get_office_holder_batch(
    office_holder_ids: List[int],
    ce_api_token: str,
    base_sleep: float = 0.1,
    jitter_factor: float = 0.1,
    timeout: int = 60,
) -> List[Dict[str, Any]]:
    """
    Fetches office holders from the BallotReady API in batches.
    """
    url = "https://bpi.civicengine.com/graphql"

    encoded_ids = [
        _base64_encode_id(office_holder_id) for office_holder_id in office_holder_ids
    ]

    # Construct the payload with the nodes query
    payload = {
        "query": """
        query GetOfficeHoldersBatch($ids: [ID!]!) {
            nodes(ids: $ids) {
                ... on OfficeHolder {
                    addresses {
                        addressLine1
                        addressLine2
                        city
                        country
                        createdAt
                        databaseId
                        id
                        state
                        updatedAt
                        zip
                    }
                    contacts {
                        email
                        fax
                        phone
                        type
                    }
                    createdAt
                    databaseId
                    endAt
                    id
                    isAppointed
                    isCurrent
                    isOffCycle
                    isVacant
                    officeTitle
                    parties {
                        databaseId
                        id
                    }
                    person {
                        databaseId
                        id
                    }
                    position {
                        databaseId
                        id
                    }
                    specificity
                    startAt
                    totalYearsInOffice
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
        """
    }

    # Add the ids to the payload
    payload["variables"] = {"ids": encoded_ids}  # type: ignore

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {ce_api_token}",
    }

    try:
        logging.debug(f"Sending request for {len(encoded_ids)} office holders")
        response = requests.post(url, json=payload, headers=headers, timeout=timeout)

        # Calculate sleep time with jitter to avoid synchronized API calls
        jitter = random.uniform(-jitter_factor, jitter_factor) * base_sleep
        sleep_time = max(0.05, base_sleep + jitter)  # Ensure minimum sleep of 0.05s
        time.sleep(sleep_time)

        logging.debug(f"Received response for {len(encoded_ids)} office holders")
        response_data = response.json()
        office_holders: List[Dict[str, Any]] = response_data["data"]["nodes"]

        # handle necessary transformations to match schema
        for office_holder in office_holders:
            if office_holder:
                office_holder["createdAt"] = pd.to_datetime(office_holder["createdAt"])
                office_holder["endAt"] = pd.to_datetime(office_holder["endAt"])
                office_holder["startAt"] = pd.to_datetime(office_holder["startAt"])
                office_holder["updatedAt"] = pd.to_datetime(office_holder["updatedAt"])
        return office_holders

    except (KeyError, TypeError) as e:
        logging.error(f"Error processing office holder batch: {e}")
        raise ValueError(
            f"Failed to parse office holder data from API response: {str(e)}"
        )


def _get_office_holder_token(ce_api_token: str) -> Callable:
    """
    Wraps the get_office_holder function in a callable that can be used in a pandas UDF.
    The wrapper is necessary to pass in ce_api_token to the UDF.
    """

    @pandas_udf(OFFICE_HOLDER_BR_SCHEMA)
    def get_office_holder(office_holder_ids: pd.Series) -> pd.DataFrame:
        """
        Pandas UDF that processes batches of office holder IDs and returns their office holders.
        """
        if not ce_api_token:
            raise ValueError("Missing required environment variable: CE_API_TOKEN")

        office_holders_by_database_id: Dict[int, Dict[str, Any] | None] = {}

        batch_size = 100

        for i in range(0, len(office_holder_ids), batch_size):
            batch = office_holder_ids[i : i + batch_size]
            batch_size_info = f"Batch {i//batch_size + 1}/{(len(office_holder_ids) + batch_size - 1)//batch_size}, size: {len(batch)}"
            logging.debug(f"Processing {batch_size_info}")

            try:
                batch_office_holders = _get_office_holder_batch(batch, ce_api_token)
                # process and organize office holders by database id
                for office_holder in batch_office_holders:
                    if office_holder:
                        office_holders_by_database_id[office_holder["databaseId"]] = (
                            office_holder
                        )

            except Exception as e:
                logging.error(f"Error processing office holder batch: {e}")
                raise e

        empty_office_holder: Dict[str, Any] = {}
        for field in OFFICE_HOLDER_BR_SCHEMA:
            if isinstance(field.dataType, ArrayType):
                empty_office_holder[field.name] = []
            elif isinstance(field.dataType, StructType):
                empty_office_holder[field.name] = {}
            else:
                empty_office_holder[field.name] = None
        office_holders_list = [
            office_holders_by_database_id.get(id, empty_office_holder)
            for id in office_holder_ids
        ]
        return pd.DataFrame(office_holders_list)

    return get_office_holder


def model(dbt, session) -> DataFrame:
    # configure the model
    dbt.config(
        submission_method="all_purpose_cluster",  # required for .cache()
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",  # required for .cache()
        materialized="incremental",
        database_id="int__ballotready_office_holder",
        auto_liquid_cluster=True,
        tags=["intermediate", "ballotready", "office_holder", "api", "pandas_udf"],
    )

    # get api token from environment variables
    ce_api_token = dbt.config.get("ce_api_token")
    if not ce_api_token:
        raise ValueError("Missing required config parameter: ce_api_token")

    # get office holders
    person: DataFrame = dbt.ref("int__ballotready_person")

    if dbt.is_incremental:
        # get the max updated_at from the existing table
        existing_table = session.table(f"{dbt.this}")
        max_updated_at_row = existing_table.agg({"updated_at": "max"}).collect()[0]
        max_updated_at = max_updated_at_row[0] if max_updated_at_row else None

        if max_updated_at:
            person = person.filter(col("updated_at") > max_updated_at)

    # get distinct office holder IDs
    office_holder_ids = person.select(col("office_holders")).filter(
        col("office_holders").isNotNull()
    )
    office_holder_ids = (
        office_holder_ids.explode("office_holders")
        .select(col("office_holders.databaseId").alias("database_id"))
        .distinct()
    )

    # Trigger a cache to ensure these transformations are applied before the filter
    office_holder_ids.cache()
    office_holder_ids_count = office_holder_ids.count()
    if office_holder_ids_count == 0:
        logging.info("INFO: No new or updated office holders to process")
        empty_df = session.createDataFrame([], OFFICE_HOLDER_BR_SCHEMA)
        empty_df = empty_df.select(
            col("addresses"),
            col("contacts"),
            col("createdAt").alias("created_at"),
            col("databaseId").alias("database_id"),
            col("endAt").alias("end_at"),
            col("id"),
            col("isAppointed").alias("is_appointed"),
            col("isCurrent").alias("is_current"),
            col("isOffCycle").alias("is_off_cycle"),
            col("isVacant").alias("is_vacant"),
            col("officeTitle").alias("office_title"),
            col("parties"),
            col("person"),
            col("position"),
            col("specificity"),
            col("startAt").alias("start_at"),
            col("totalYearsInOffice").alias("total_years_in_office"),
            col("updatedAt").alias("updated_at"),
            col("urls"),
        )
        return empty_df

    # get office holder data from API
    _get_office_holder = _get_office_holder_token(ce_api_token)
    office_holder = office_holder_ids.withColumn(
        "office_holder", _get_office_holder(col("database_id"))
    )

    # Explode the office holder column and extract fields
    office_holder = office_holder.select(
        col("office_holder.addresses").alias("addresses"),
        col("office_holder.contacts").alias("contacts"),
        col("office_holder.createdAt").alias("created_at"),
        col("office_holder.databaseId").alias("database_id"),
        col("office_holder.endAt").alias("end_at"),
        col("office_holder.id").alias("id"),
        col("office_holder.isAppointed").alias("is_appointed"),
        col("office_holder.isCurrent").alias("is_current"),
        col("office_holder.isOffCycle").alias("is_off_cycle"),
        col("office_holder.isVacant").alias("is_vacant"),
        col("office_holder.officeTitle").alias("office_title"),
        col("office_holder.parties").alias("parties"),
        col("office_holder.person").alias("person"),
        col("office_holder.position").alias("position"),
        col("office_holder.specificity").alias("specificity"),
        col("office_holder.startAt").alias("start_at"),
        col("office_holder.totalYearsInOffice").alias("total_years_in_office"),
        col("office_holder.updatedAt").alias("updated_at"),
        col("office_holder.urls").alias("urls"),
    )

    # Trigger a cache to ensure these transformations are applied before the filter
    office_holder.cache()
    office_holder.count()
    office_holder = office_holder.filter(col("id").isNotNull()).filter(
        col("database_id").isNotNull()
    )
    return office_holder
