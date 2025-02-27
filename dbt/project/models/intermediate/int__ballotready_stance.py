import os
from base64 import b64encode

import pandas as pd
import requests


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


def _get_stance_ids(encoded_candidacy_id: str) -> pd.DataFrame:
    """
    Queries the CivicEngine GraphQL API to get stance IDs for a given candidacy.

    Args:
        encoded_candidacy_id: The base64 encoded candidacy ID

    Returns:
        DataFrame containing stance IDs
    """
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
                            id
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

    # Extract stance IDs and convert to DataFrame
    try:
        stances = data["data"]["node"]["stances"]["nodes"]
        stance_ids = [stance["id"] for stance in stances]
        return pd.DataFrame({"stance_id": stance_ids})
    except (KeyError, TypeError):
        # Return empty DataFrame if no stances found
        return pd.DataFrame(columns=["stance_id"])


def model(dbt, session):
    # configure the model
    dbt.config(materialized="incremental")

    # Get upstream data using ref
    candidacies = dbt.ref("stg_airbyte_source__ballotready_s3_candidacies_v3")
    candidacies = candidacies.to_pandas_on_spark()

    # transform candidacy ids
    candidacies["encoded_candidacy_id"] = candidacies["candidacy_id"].apply(
        _base64_encode_id
    )

    # get stances
    # stances = candidacies["encoded_candidacy_id"].apply(get_stance_ids)

    # stance = stance.to_spark()
    # return stance
