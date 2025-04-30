# TODO: uncomment fields `expandedText` and `parentIssue` throughout file: API call and schema
import logging
import random
import time
from base64 import b64encode
from typing import Any, Callable, Dict, List

import pandas as pd
import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, pandas_udf
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

ISSUE_BR_SCHEMA = StructType(
    [
        StructField(name="databaseId", dataType=IntegerType(), nullable=True),
        # StructField("expandedText", StringType(), True),
        StructField("id", StringType(), True),
        StructField("key", StringType(), True),
        StructField("name", StringType(), True),
        # StructField(
        #     "parentIssue",
        #     StructType(
        #         [
        #             StructField("databaseId", IntegerType(), True),
        #             StructField("id", StringType(), True),
        #         ]
        #     ),
        #     True,
        # ),
        StructField("pluginEnabled", BooleanType(), True),
        StructField("responseType", StringType(), True),
        StructField("rowOrder", IntegerType(), True),
    ]
)


def _base64_encode_id(issue_id: int) -> str:
    """Base64 encodes the issue id"""
    id_prefix = "gid://ballot-factory/Issue/"
    prefixed_id = f"{id_prefix}{issue_id}"
    encoded_bytes: bytes = b64encode(prefixed_id.encode("utf-8"))
    encoded_id: str = encoded_bytes.decode("utf-8")
    return encoded_id


@pandas_udf(StringType())
def _base64_encode_id_udf(issue_id: pd.Series) -> pd.Series:
    return pd.Series([_base64_encode_id(x) for x in issue_id])


def _get_issue_batch(
    issue_ids: List[int],
    ce_api_token: str,
    base_sleep: float = 0.1,
    jitter_factor: float = 0.1,
    timeout: int = 60,
) -> List[Dict[str, Any]]:
    """
    Fetches issues from the BallotReady API in batches.

    This function handles pagination and rate limiting to efficiently retrieve
    issue data for a list of IDs.
    """
    url = "https://bpi.civicengine.com/graphql"

    encoded_ids = [_base64_encode_id(issue_id) for issue_id in issue_ids]

    payload = {
        "query": """
        query GetIssuesBatch($ids: [ID!]!) {
            nodes(ids: $ids) {
                ... on Issue {
                    databaseId
                    # expandedText
                    id
                    key
                    name
                    # parentIssue {
                    #     databaseId
                    #     id
                    # }
                    pluginEnabled
                    responseType
                    rowOrder
                }
            }
        }
        """,
        "variables": {"ids": encoded_ids},
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {ce_api_token}",
        "Accept": "application/json",
    }

    try:
        logging.debug(f"Sending request for {len(encoded_ids)} issues")
        response = requests.post(url, json=payload, headers=headers, timeout=timeout)

        # Calculate sleep time with jitter to avoid synchronized API calls
        jitter = random.uniform(-jitter_factor, jitter_factor) * base_sleep
        sleep_time = max(0.05, base_sleep + jitter)  # Ensure minimum sleep of 0.05s
        time.sleep(sleep_time)

        # Parse the response
        response_data = response.json()
        logging.debug(f"Received response for {len(encoded_ids)} issues")
        issues: List[Dict[str, Any]] = response_data["data"]["nodes"]
        return issues

    except (KeyError, TypeError) as e:
        logging.error(f"Error processing issue batch: {str(e)}")
        raise ValueError(f"Failed to parse issue data from API response: {str(e)}")


def _get_issue_token(ce_api_token: str) -> Callable:

    @pandas_udf(ISSUE_BR_SCHEMA)
    def get_issue(issue_database_id: pd.Series) -> pd.DataFrame:
        """
        Pandas UDF that processes batches of issue IDs and returns their issues.
        """
        if not ce_api_token:
            raise ValueError("Missing required environment variable: CE_API_TOKEN")

        issues_by_issue_db_id: Dict[int, Dict[str, Any] | None] = {}

        batch_size = 100

        for i in range(0, len(issue_database_id), batch_size):
            batch = issue_database_id[i : i + batch_size]
            batch_size_info = f"Batch {i//batch_size + 1}/{(len(issue_database_id) + batch_size - 1)//batch_size}, size: {len(batch)}"
            logging.debug(f"Processing {batch_size_info}")

            try:
                batch_issues = _get_issue_batch(batch, ce_api_token)
                # process and organize issues by issue database id
                for issue in batch_issues:
                    if issue:
                        issues_by_issue_db_id[issue["databaseId"]] = issue

            except Exception as e:
                logging.error(f"Error processing issue batch: {e}")
                raise e

        # create empty issue dict for missing issue ids. order according to input id list
        empty_issue: Dict[str, Any] = {}
        for field in ISSUE_BR_SCHEMA:
            if isinstance(field.dataType, ArrayType):
                empty_issue[field.name] = []
            elif isinstance(field.dataType, StructType):
                empty_issue[field.name] = {}
            else:
                empty_issue[field.name] = None
        issues_list = [
            issues_by_issue_db_id.get(id, empty_issue) for id in issue_database_id
        ]
        return pd.DataFrame(issues_list)

    return get_issue


def model(dbt, session) -> DataFrame:
    # configure the model
    dbt.config(
        submission_method="all_purpose_cluster",  # required for .cache()
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",  # required for .cache()
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="database_id",
        tags=["intermediate", "ballotready", "issue", "api", "pandas_udf"],
    )

    # get api token from environment variables
    ce_api_token = dbt.config.get("ce_api_token")
    if not ce_api_token:
        raise ValueError("Missing required config parameter: ce_api_token")

    # get unique issue ids from stances
    stances: DataFrame = dbt.ref("int__ballotready_stance")

    # First, explode the stances array to get individual stance records then extract the issue database IDs
    exploded_stances = stances.select("stances").withColumn(
        "stance", explode("stances")
    )
    issue_database_ids = exploded_stances.select(
        col("stance.issue.databaseId").alias("database_id")
    ).distinct()

    # for incremental, only pick up issue id's that don't already exist in the table
    if dbt.is_incremental:
        existing_table: DataFrame = session.table(f"{dbt.this}")
        existing_issue_database_ids = existing_table.select("database_id").distinct()
        issue_database_ids = issue_database_ids.join(
            other=existing_issue_database_ids,
            on="database_id",
            how="left_anti",
        )

    # trigger a cache to ensure transformations are applied before the filter
    issue_database_ids.cache()
    issue_database_ids_count = issue_database_ids.count()
    if issue_database_ids_count == 0:
        logging.info("INFO: No new or updated issues to process")
        empty_df = session.createDataFrame([], ISSUE_BR_SCHEMA)
        empty_df = empty_df.select(
            col("databaseId").alias("database_id"),
            # col("expandedText").alias("expanded_text"),
            col("id"),
            col("key"),
            col("name"),
            # col("parentIssue").alias("parent_issue"),
            col("pluginEnabled").alias("plugin_enabled"),
            col("responseType").alias("response_type"),
            col("rowOrder").alias("row_order"),
        )
        return empty_df

    # get issue data from API
    _get_issue = _get_issue_token(ce_api_token)
    issue = issue_database_ids.withColumn("issue", _get_issue(col("database_id")))

    issue = issue.select(
        col("issue.databaseId").alias("database_id"),
        # col("issue.expandedText").alias("expanded_text"),
        col("issue.id").alias("id"),
        col("issue.key").alias("key"),
        col("issue.name").alias("name"),
        # col("issue.parentIssue").alias("parent_issue"),
        col("issue.pluginEnabled").alias("plugin_enabled"),
        col("issue.responseType").alias("response_type"),
        col("issue.rowOrder").alias("row_order"),
    )

    # Trigger a cache to ensure these transformations are applied before the filter
    issue.cache()
    issue.count()
    issue = issue.filter(col("id").isNotNull()).filter(col("database_id").isNotNull())
    return issue
