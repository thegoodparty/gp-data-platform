"""Extraction helpers for the extract_ballotready DAG.

Pulls BallotReady (CivicEngine) GraphQL objects by id and lands the raw node
payloads in Databricks. Every entity is addressed the same way, through
`nodes(ids:)` over base64 global ids, so one client and one registry cover all
of them.
"""

import logging
from base64 import b64encode
from collections.abc import Iterator
from typing import Any

logger = logging.getLogger("airflow.task")

CIVIC_ENGINE_GRAPHQL_URL = "https://bpi.civicengine.com/graphql"
_NODE_ID_PREFIX = "gid://ballot-factory"


def encode_node_id(node_type: str, node_id: int) -> str:
    """Encode an integer BallotReady id as its base64 GraphQL global id."""
    return b64encode(f"{_NODE_ID_PREFIX}/{node_type}/{node_id}".encode()).decode("utf-8")


def chunked(seq: list[Any], size: int) -> Iterator[list[Any]]:
    """Yield successive `size`-length chunks of `seq`."""
    if size < 1:
        raise ValueError(f"chunk size must be >= 1, got {size}")
    for i in range(0, len(seq), size):
        yield seq[i : i + size]
