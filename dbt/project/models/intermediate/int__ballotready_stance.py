import hashlib
from base64 import b64encode
from datetime import datetime
from functools import partial

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


def _get_stance(candidacy_id: str, dbt) -> pd.Series:
    """
    Queries the CivicEngine GraphQL API to get stance IDs for a given candidacy.

    Args:
        candidacy_id: the candidacy id to get the stance for

    Returns:
        DataFrame containing stance IDs
    """
    # GraphQL endpoint
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
        "Authorization": f"Bearer {dbt.config.get('ce_api_token')}",
    }

    # Make the request
    response = requests.post(url, json=payload, headers=headers)
    print(response)
    response.raise_for_status()
    data = response.json()
    print(data)

    # Extract all stance data and convert to DataFrame
    try:
        stances = data["data"]["node"]["stances"]

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
            stance["candidacy_id"] = candidacy_id
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


def model(dbt, session) -> pd.DataFrame:
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

    # for development
    candidacies = candidacies.limit(3)

    # Fet stance. note that stance does not have an updated_at field so there is no need to use the incremental strategy. This will be a full data refresh everytime since we need to
    candidacies_pd = candidacies.toPandas()
    stance = candidacies_pd["candidacy_id"].apply(partial(_get_stance, dbt=dbt))

    stance = session.createDataFrame(stance)

    # Convert the resulting pd.Series to a DataFrame
    stance = pd.DataFrame(stance)

    # rename id -> stance_id and generate a surrogate key with dbt macros
    stance = stance.rename(columns={"id": "stance_id"})
    stance["id"] = stance.apply(_generate_hash, axis=1)

    # Add a timestamp for when this record was processed
    stance["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return stance


# This part is user provided model code
# you will need to copy the next section to run the code
