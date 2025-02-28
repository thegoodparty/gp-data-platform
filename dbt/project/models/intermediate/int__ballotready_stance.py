import hashlib
import os
from base64 import b64encode
from datetime import datetime

import pandas as pd
import requests
from pyspark.sql import DataFrame


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


def _get_stance_from_candidacies(row: pd.Series) -> pd.DataFrame:
    """
    Queries the CivicEngine GraphQL API to get stance IDs for a given candidacy.

    Args:
        encoded_candidacy_id: The base64 encoded candidacy ID

    Returns:
        DataFrame containing stance IDs
    """
    encoded_candidacy_id = _base64_encode_id(row["candidacy_id"])

    # GraphQL endpoint
    url = "https://bpi.civicengine.com/graphql"

    # Construct the payload
    payload = {
        "query": """
        query GetCandidacyStances($id: ID!) {
            node(id: $id) {
                ... on Candidacy {
                    stances {
                        nodes {
                            databaseId
                            id
                            issue
                            locale
                            referenceUrl
                            statement
                        }
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
        "Authorization": f"Bearer {os.environ['DBT_ENV_SECRET_CIVICENGINE_API_TOKEN']}",
    }

    # Make the request
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()

    # Parse response
    data = response.json()

    # Extract all stance data and convert to DataFrame
    try:
        stances = data["data"]["node"]["stances"]["nodes"]

        # if no stances, return empty dataframe
        if not stances:
            return pd.DataFrame(
                columns=[
                    "databaseId",
                    "id",
                    "issue",
                    "locale",
                    "referenceUrl",
                    "statement",
                    "candidacy_id",
                    "encoded_candidacy_id",
                ]
            )

        # Add candidacy_id and encoded_candidacy_id to each stance
        for stance in stances:
            stance["candidacy_id"] = row["candidacy_id"]
            stance["encoded_candidacy_id"] = encoded_candidacy_id
        return pd.DataFrame(stances)

    except (KeyError, TypeError):
        # Return empty DataFrame if no stances found
        return pd.DataFrame(
            columns=[
                "databaseId",
                "id",
                "issue",
                "locale",
                "referenceUrl",
                "statement",
                "candidacy_id",
                "encoded_candidacy_id",
            ]
        )


def _generate_hash(row):
    # Combine relevant columns with null handling
    columns_to_hash = ["stance_id", "candidacy_id"]
    combined_value = ""

    for col in columns_to_hash:
        val = row.get(col, None)
        if val is None:
            combined_value += "_this_used_to_be_null_"
        else:
            combined_value += str(val)

    # Generate MD5 hash
    return hashlib.md5(combined_value.encode()).hexdigest()


def model(dbt, session):
    dbt.config(
        materialized="incremental", incremental_strategy="merge", unique_key="id"
    )

    # get candidacies and ids
    candidacies: DataFrame = dbt.ref(
        "stg_airbyte_source__ballotready_s3_candidacies_v3"
    )
    ids_from_candidacies = candidacies.select("candidacy_id").distinct().collect()
    ids_from_candidacies = [row.candidacy_id for row in ids_from_candidacies]

    if dbt.is_incremental:
        existing_candidacy_ids = session.sql(
            f"select distinct(candidacy_id) from {dbt.this}"
        ).collect()[0]
    else:
        existing_candidacy_ids = []

    # get ids from candidacies that are not in the current table
    ids_to_get = [
        candidacy_id
        for candidacy_id in ids_from_candidacies
        if candidacy_id not in existing_candidacy_ids
    ]

    # filter candidacies to only include ids that need to be fetched
    candidacies = candidacies.filter(candidacies["candidacy_id"].isin(ids_to_get))

    # Fet stance. note that stance does not have an updated_at field so there is no need to use the incremental strategy. This will be a full data refresh everytime since we need to
    stance = candidacies.apply(_get_stance_from_candidacies, axis=1)

    # Convert the result to a DataFrame
    stance = session.createDataFrame(stance)

    # rename id -> stance_id and generate a surrogate key with dbt macros
    stance = stance.rename(columns={"id": "stance_id"})
    stance["id"] = stance.apply(_generate_hash, axis=1)

    # Add a timestamp for when this record was processed
    stance["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # convert to spark
    stance = stance.to_spark()
    return stance
