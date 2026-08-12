import logging
import random
import time
from base64 import b64encode
from collections.abc import Callable
from typing import Any

import pandas as pd
import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, pandas_udf
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _base64_encode_id(filing_period_id: int) -> str:
    """Base64 encodes the filing period id"""
    id_prefix = "gid://ballot-factory/FilingPeriod/"
    prefixed_id = f"{id_prefix}{filing_period_id}"
    encoded_bytes: bytes = b64encode(prefixed_id.encode("utf-8"))
    encoded_id: str = encoded_bytes.decode("utf-8")
    return encoded_id


_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


def _get_filing_periods_batch(
    filing_period_ids: list[int],
    ce_api_token: str,
    base_sleep: float = 0.05,
    jitter_factor: float = 0.05,
    timeout: int = 30,
    max_attempts: int = 3,
) -> list[dict[str, Any]]:
    """Fetches filing periods for a batch of filing period IDs using the CivicEngine API.

    Nodes that carry no filing period are dropped here rather than handed back to the
    caller: `nodes(ids:)` answers with null for an id it cannot resolve and with an empty
    object for a node of some other type, and either one used to abort the whole batch.
    """
    url = "https://bpi.civicengine.com/graphql"

    # Encode all filing period IDs
    encoded_ids = [_base64_encode_id(filing_period_id) for filing_period_id in filing_period_ids]

    # Construct the payload with the nodes query
    payload = {
        "query": """
        query GetFilingPeriodsBatch($ids: [ID!]!) {
            nodes(ids: $ids) {
                ... on FilingPeriod {
                    createdAt
                    databaseId
                    endOn
                    id
                    notes
                    startOn
                    type
                    updatedAt
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

    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            logging.debug(f"Sending request for {len(encoded_ids)} filing periods")
            response = requests.post(url, json=payload, headers=headers, timeout=timeout)

            # Calculate sleep time with jitter to avoid synchronized API calls
            jitter = random.uniform(-jitter_factor, jitter_factor) * base_sleep
            sleep_time = max(0.05, base_sleep + jitter)  # Ensure minimum sleep of 0.05s
            time.sleep(sleep_time)

            if response.status_code in _RETRYABLE_STATUS_CODES and attempt < max_attempts:
                logging.warning(f"Status {response.status_code} on attempt {attempt}, retrying batch")
                time.sleep(_backoff_seconds(attempt))
                continue

            response.raise_for_status()

            data = response.json() or {}
            if data.get("errors"):
                logging.warning(f"GraphQL errors for filing periods batch: {data['errors']}")
            nodes: list[dict[str, Any] | None] = (data.get("data") or {}).get("nodes") or []
            return [node for node in nodes if node]

        except requests.exceptions.HTTPError as e:
            # A retryable status only reaches raise_for_status on the final attempt;
            # anything else (401, 403, 404) will not improve by asking again.
            status_code = e.response.status_code if e.response is not None else None
            if status_code not in _RETRYABLE_STATUS_CODES:
                logging.error(f"Non-retryable status {status_code} for filing periods batch: {e!s}")
                raise RuntimeError(f"Failed to fetch filing period data from API: {e!s}") from e
            last_error = e
            break

        except requests.exceptions.RequestException as e:
            last_error = e
            if attempt == max_attempts:
                break
            logging.warning(f"Request failed on attempt {attempt}, retrying batch: {e!s}")
            time.sleep(_backoff_seconds(attempt))

    logging.error(
        f"API request failed for filing periods batch after {max_attempts} attempts: {last_error!s}"
    )
    raise RuntimeError(f"Failed to fetch filing period data from API: {last_error!s}") from last_error


def _backoff_seconds(attempt: int, cap: float = 8.0) -> float:
    """Exponential backoff with jitter so retrying workers don't resynchronize."""
    return min(cap, 0.5 * 2 ** (attempt - 1)) + random.uniform(0, 0.25)


filing_period_schema = StructType(
    [
        StructField("createdAt", TimestampType(), True),
        StructField("databaseId", IntegerType(), True),
        StructField("endOn", DateType(), True),
        StructField("id", StringType(), True),
        StructField("notes", StringType(), True),
        StructField("startOn", DateType(), True),
        StructField("type", StringType(), True),
        StructField("updatedAt", TimestampType(), True),
    ]
)


_FILING_PERIOD_FIELDS = (
    "createdAt",
    "databaseId",
    "endOn",
    "id",
    "notes",
    "startOn",
    "type",
    "updatedAt",
)
_FILING_PERIOD_TIMESTAMP_FIELDS = ("createdAt", "endOn", "startOn", "updatedAt")


def _normalize_filing_period(node: dict[str, Any]) -> dict[str, Any] | None:
    """Coerces one API node into `filing_period_schema` field order, or None if unusable.

    Keys are emitted in schema order because the pandas UDF's return frame is matched to
    the struct positionally.
    """
    if node.get("databaseId") is None or node.get("id") is None:
        return None

    record = {field: node.get(field) for field in _FILING_PERIOD_FIELDS}
    try:
        record["databaseId"] = int(record["databaseId"])
    except (TypeError, ValueError):
        return None
    for field in _FILING_PERIOD_TIMESTAMP_FIELDS:
        record[field] = pd.Timestamp(record[field]) if record[field] is not None else pd.NaT
    return record


def _get_filing_period_token(ce_api_token: str) -> Callable:
    """Wraps the get_filing_period function in a callable that can be used in a pandas UDF"""

    @pandas_udf(filing_period_schema)
    def get_filing_period(filing_period_ids: pd.Series) -> pd.DataFrame:
        """
        Pandas UDF that processes batches of filing period IDs and returns their filing periods.

        This function is distributed by Spark to different workers, allowing parallel
        processing of filing period data. Inside each worker, filing period IDs are further
        batched for efficient API calls.
        """
        if not ce_api_token:
            raise ValueError("Missing required environment variable: CE_API_TOKEN")

        filing_periods_by_filing_period_id: dict[int, dict[str, Any] | None] = {}

        batch_size = 200
        unusable_nodes = 0
        failed_batches = 0

        for i in range(0, len(filing_period_ids), batch_size):
            batch = filing_period_ids[i : i + batch_size]
            batch_size_info = f"Batch {i//batch_size + 1}/{(len(filing_period_ids) + batch_size - 1)//batch_size}, size: {len(batch)}"
            logging.debug(f"Processing {batch_size_info}")

            try:
                batch_filing_periods = _get_filing_periods_batch(batch, ce_api_token)
            except Exception as e:
                failed_batches += 1
                logging.error(f"Error fetching batch {i//batch_size}: {e!s}")
                continue

            # Normalize per record: one unusable node must not cost the rest of its batch
            for node in batch_filing_periods:
                filing_period = _normalize_filing_period(node)
                if filing_period is None:
                    unusable_nodes += 1
                    continue
                filing_periods_by_filing_period_id[filing_period["databaseId"]] = filing_period

        logging.info(
            f"Filing periods requested: {len(filing_period_ids)}, "
            f"resolved: {len(filing_periods_by_filing_period_id)}, "
            f"unusable nodes: {unusable_nodes}, failed batches: {failed_batches}"
        )

        # create a list of dictionaries for each filing period in order of input
        result_data: list[dict[str, Any]] = []
        for filing_period_id in filing_period_ids:
            filing_period = filing_periods_by_filing_period_id.get(int(filing_period_id), {})  # type: ignore
            if filing_period:
                result_data.append(filing_period)
            else:
                result_data.append(
                    {
                        "createdAt": None,
                        "databaseId": -1,  # Use -1 directly for failed records
                        "endOn": None,
                        "id": None,
                        "notes": None,
                        "startOn": None,
                        "type": None,
                        "updatedAt": None,
                    }
                )

        return pd.DataFrame(result_data)

    return get_filing_period


def _referenced_filing_period_ids(race: DataFrame) -> DataFrame:
    """Distinct non-null filing period ids referenced by the given races."""
    return (
        race.select(explode("filing_periods").alias("filing_period"))
        .select(col("filing_period.databaseId").alias("database_id"))
        .filter(col("database_id").isNotNull())
        .distinct()
    )


def model(dbt, session) -> DataFrame:
    dbt.config(
        submission_method="all_purpose_cluster",  # required for .cache()
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",  # required for .cache()
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="database_id",
        on_schema_change="fail",
        tags=["ballotready", "filing_period", "api", "pandas_udf"],
    )

    # get API token from Databricks secrets
    dbt_env = dbt.config.meta_get("dbt_environment")
    ce_api_token = dbutils.secrets.get(  # type: ignore[name-defined]
        scope=f"dbt-secrets-{dbt_env}", key="civic-engine-api-token"
    )
    if not ce_api_token:
        raise ValueError("Missing required secret: civic-engine-api-token")

    # a run fetches at most this many ids, so no single run monopolizes the cluster
    max_ids_per_run = int(dbt.config.meta_get("max_ids_per_run", 100_000))

    # get unique filing period ids from race
    race: DataFrame = dbt.ref("stg_airbyte_source__ballotready_api_race")
    referenced_ids = _referenced_filing_period_ids(race)

    if dbt.is_incremental:
        logging.info("INFO: Running in incremental mode")
        existing_table = session.table(f"{dbt.this}")
        latest_updated_at = existing_table.agg({"updated_at": "max"}).collect()[0][0]

        # Leading edge: ids referenced by recently updated races. The watermark is a
        # filing period timestamp compared against a race timestamp, so it skips races
        # whose own updates predate the newest filing period edit we already stored.
        leading_edge_ids = (
            _referenced_filing_period_ids(race.filter(col("updated_at") > latest_updated_at))
            if latest_updated_at is not None
            else session.createDataFrame([], StructType([StructField("database_id", IntegerType(), True)]))
        )
        leading_edge_ids.cache()
        leading_edge_count = leading_edge_ids.count()

        # Backlog: ids that races reference but that never landed here, whether a race
        # was skipped by the watermark above or its batch failed. The leading edge is
        # subtracted too, so the budget below buys ids this run was not already going to
        # fetch. Newest ids first, since those carry the deadlines for current cycles.
        backlog_ids = referenced_ids.join(
            existing_table.select("database_id"), on="database_id", how="left_anti"
        ).join(leading_edge_ids, on="database_id", how="left_anti")
        headroom = max(max_ids_per_run - leading_edge_count, 0)
        if headroom == 0:
            logging.warning(
                f"Leading edge of {leading_edge_count} ids fills max_ids_per_run "
                f"({max_ids_per_run}); the backlog makes no progress this run"
            )
        logging.info(f"Leading edge: {leading_edge_count} ids, backlog budget this run: {headroom} ids")

        # Both sides are distinct and the anti-join above makes them disjoint, so the
        # union needs no further dedup.
        filing_periods: DataFrame = leading_edge_ids.unionByName(
            backlog_ids.orderBy(col("database_id").desc()).limit(headroom)
        )
    else:
        filing_periods = referenced_ids.orderBy(col("database_id").desc()).limit(max_ids_per_run)

    # Trigger a cache to ensure these transformations are applied. This is important for incremental models to avoid unnecessary API calls
    filing_periods.cache()
    filing_periods_count = filing_periods.count()
    logging.info(f"Found {filing_periods_count} new filing periods to process")

    # if filing_periods is empty, return an empty dataframe
    if filing_periods_count == 0:
        logging.info("INFO: No new or updated filing periods to process")
        return session.createDataFrame(
            [],
            StructType(
                [
                    StructField("created_at", TimestampType(), True),
                    StructField("database_id", IntegerType(), True),
                    StructField("end_on", DateType(), True),
                    StructField("id", StringType(), True),
                    StructField("notes", StringType(), True),
                    StructField("start_on", DateType(), True),
                    StructField("type", StringType(), True),
                    StructField("updated_at", TimestampType(), True),
                ]
            ),
        )

    # get filing period data from API. This is a long operation,
    # downsample with `.sample(False, 0.1).limit(10000)` if needed
    get_filing_period = _get_filing_period_token(ce_api_token)

    # First get the filing period data as a struct, then extract each field into its own column
    filing_periods = filing_periods.withColumn("filing_period_data", get_filing_period(col("database_id")))
    result = filing_periods.select(
        col("filing_period_data.createdAt").alias("created_at"),
        col("filing_period_data.databaseId").alias("database_id"),
        col("filing_period_data.endOn").alias("end_on"),
        col("filing_period_data.id").alias("id"),
        col("filing_period_data.notes").alias("notes"),
        col("filing_period_data.startOn").alias("start_on"),
        col("filing_period_data.type").alias("type"),
        col("filing_period_data.updatedAt").alias("updated_at"),
    )

    # Drop rows with database_id -1, which is a placeholder for failed records
    # Trigger a cache to ensure these transformations are applied before the filter
    result.cache()
    attempted_count = result.count()
    result = result.filter(col("database_id") != -1)
    result = result.filter(col("database_id").isNotNull())
    result = result.filter(col("id").isNotNull())

    # Unresolved ids stay absent from this table, so the next run's backlog picks them up
    resolved_count = result.count()
    logging.info(
        f"Filing period ids attempted: {attempted_count}, resolved: {resolved_count}, "
        f"unresolved: {attempted_count - resolved_count}"
    )
    return result
