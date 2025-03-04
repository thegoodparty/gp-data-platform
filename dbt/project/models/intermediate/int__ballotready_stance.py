from base64 import b64encode
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Dict, List

import pandas as pd
import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import ArrayType, IntegerType, StructField, StructType


def _base64_encode_id(candidacy_id: str) -> str:
    """
    Encodes a UTF-8 string into a base64-encoded string.

    Args:
        input_string: The string to encode.

    Returns:
        The base64-encoded string.
    """
    id_prefix = "gid://ballot-factory/Candidacy/"
    prefixed_id = f"{id_prefix}{candidacy_id}"
    encoded_bytes: bytes = b64encode(prefixed_id.encode("utf-8"))
    encoded_id: str = encoded_bytes.decode("utf-8")
    return encoded_id


def _get_stance(candidacy_id: str, ce_api_token: str) -> List[Dict[str, int]]:
    """
    Queries the CivicEngine GraphQL API to get stance IDs for a given candidacy.

    Args:
        candidacy_id: the candidacy id to get the stance for

    Returns:
        DataFrame containing stance IDs
    """
    # GraphQL endpoint
    url = "https://bpi.civicengine.com/graphql"
    print(f"candidacy_id is type: {type(candidacy_id)}")

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

    # Make the request
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    data = response.json()

    # empty_stances = []
    empty_stances = [{"databaseId": 24601}]
    # Extract all stance data and convert to DataFrame
    try:
        stances = data["data"]["node"]["stances"]

        # if no stances, return empty dataframe
        if not stances:
            return empty_stances
            # return json.dumps(empty_stances)

        # Add candidacy_id and encoded_candidacy_id to each stance
        new_stances = []
        for stance in stances:
            stance["candidacy_id"] = candidacy_id
            stance["encoded_candidacy_id"] = encoded_candidacy_id
            new_stances.append({"databaseId": stance["databaseId"]})
        # return json.dumps(new_stances)
        return new_stances
        # return json.dumps(stances)

    except (KeyError, TypeError):
        return empty_stances


def model(dbt, session) -> pd.DataFrame:
    dbt.config(
        materialized="incremental", incremental_strategy="merge", unique_key="id"
    )

    ce_api_token = dbt.config.get("ce_api_token")

    # get candidacies and ids
    candidacies: DataFrame = dbt.ref(
        "stg_airbyte_source__ballotready_s3_candidacies_v3"
    )
    ids_from_candidacies = candidacies.select("candidacy_id").distinct().collect()
    ids_from_candidacies = [row.candidacy_id for row in ids_from_candidacies]

    if dbt.is_incremental:
        # get existing candidacy ids in this table
        existing_candidacy_ids = session.sql(
            f"select distinct(candidacy_id) from {dbt.this}"
        ).collect()[0]

        # only include records updated in the last 30 days
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        candidacies = candidacies.filter(candidacies["updated_at"] >= thirty_days_ago)

    else:
        existing_candidacy_ids = []

    # get ids from candidacies that are not in the current table
    candidacy_ids_to_get = [
        candidacy_id
        for candidacy_id in ids_from_candidacies
        if candidacy_id not in existing_candidacy_ids
    ]

    # filter candidacies to only include ids that need to be fetched
    candidacies = candidacies.filter(
        candidacies["candidacy_id"].isin(candidacy_ids_to_get)
    )

    # for development take a 1% sample and limit to 10
    candidacies = candidacies.sample(False, 0.01).limit(20)

    # wrapper for _get_stance here since it requires `dbt` as an argument
    _get_stance_udf = udf(
        f=partial(_get_stance, ce_api_token=ce_api_token),
        # returnType=StringType(),
        returnType=ArrayType(
            StructType(
                [
                    StructField(
                        name="databaseId", dataType=IntegerType(), nullable=False
                    ),
                    #             StructField("id", StringType()),
                    # #             StructField("issue", StringType()),
                    #             StructField("locale", StringType()),
                    #             StructField("referenceUrl", StringType()),
                    #             StructField("statement", StringType()),
                    # StructField("candidacy_id", IntegerType()),
                    #             StructField("encoded_candidacy_id", StringType()),
                ]
            )
        ),
    )

    """
    {"databaseId": integer, "id": "string=", "issue": {"databaseId": integer, "id": "string"}, "locale": "string", "referenceUrl": "string", "statement": "string", "candidacy_id": integer (or string?), "encoded_candidacy_id": "string"},
    """

    # Get stance. note that stance does not have an updated_at field so there is no need to use the incremental strategy. This will be a full data refresh everytime since we need to get all stances for all candidacies in dataframe.
    stance = candidacies.withColumn("stances", _get_stance_udf("candidacy_id"))
    stance = stance.select("candidacy_id", "stances")

    # Add created_at column - only for new records
    # Get current timestamp in UTC for created_at and updated_at
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

    # Show the DataFrame with the new columns
    display(stance)  # type: ignore

    return stance
