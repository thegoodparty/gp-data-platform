"""
## L2 Expired Voters Ingestion DAG

This DAG downloads expired L2 LALVOTERID files from L2's SFTP server and
stages them to a Databricks table for downstream dbt modelling.

### Pipeline Steps:
1. Query the `l2_expired_voters_loads` metadata table for already-processed
   files (idempotency check — only fully loaded files are considered processed)
2. Download new expired voter files from L2 SFTP, parse **all** LALVOTERIDs,
   and write them to Databricks (a completion record is written to the loads
   table only after all batch inserts succeed)

### Configuration:

**Connections** (set in Astro Environment Manager):
- `l2_sftp` (SFTP) — L2 SFTP server credentials
- `databricks` / `databricks_dev` (Generic) — host, login (OAuth client_id),
  password (OAuth client_secret), extras: `{"http_path": "/sql/1.0/warehouses/..."}`

**Variables** (set in Astro Environment Manager):
- `l2_sftp_expired_dir` — SFTP directory for expired voter files
- `l2_sftp_expired_file_pattern` — regex pattern for matching files
- `databricks_conn_id` — connection ID for Databricks
  (e.g., `databricks_dev` in dev, `databricks` in prod)
- `databricks_source_schema` — schema where Airflow stages ingested data for dbt
  visibility (e.g., `airflow_source` in prod, `airflow_source_dev` in dev)
"""

import logging
import os
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

from include.custom_functions.databricks_utils import (
    _validate_lalvoterids,
    get_databricks_connection,
    get_processed_files,
    stage_expired_voter_ids,
)
from include.custom_functions.l2_sftp import (
    create_sftp_connection,
)
from include.custom_functions.l2_sftp import (
    download_expired_voter_files as download_files,
)
from include.custom_functions.l2_sftp import (
    parse_expired_voter_ids,
)
from pendulum import datetime, duration

from airflow.sdk import BaseHook, Variable, dag, task

t_log = logging.getLogger("airflow.task")

DATABRICKS_CATALOG = "goodparty_data_catalog"


@dag(
    start_date=datetime(2025, 6, 7),
    schedule=duration(weeks=2),
    max_consecutive_failed_dag_runs=5,
    max_active_runs=1,
    doc_md=__doc__,
    catchup=False,
    default_args={
        "owner": "Data Engineering Team",
        "retries": 3,
        "retry_delay": duration(seconds=30),
    },
    tags=["l2", "expired_voters", "ingestion"],
    is_paused_upon_creation=True,
)
def l2_expired_voters():

    @task
    def fetch_processed_files() -> List[str]:
        """
        Query the loads metadata table for files that have been fully loaded
        so the ingest task can skip them (idempotency).
        """
        db_conn_id = Variable.get("databricks_conn_id")
        db_conn = BaseHook.get_connection(db_conn_id)
        schema = Variable.get("databricks_source_schema")

        connection = get_databricks_connection(
            host=db_conn.host,
            http_path=db_conn.extra_dejson.get("http_path", ""),
            client_id=db_conn.login,
            client_secret=db_conn.password,
        )
        try:
            processed = get_processed_files(
                connection=connection, catalog=DATABRICKS_CATALOG, schema=schema
            )
        finally:
            connection.close()

        return list(processed)

    @task
    def ingest_expired_voter_files(
        processed_files: List[str],
    ) -> Dict[str, Any]:
        """
        Connect to L2 SFTP, download expired voter files, parse LALVOTERIDs,
        and write them to Databricks.

        Skips files that have already been processed (listed in processed_files).

        Returns lightweight metadata (no LALVOTERID list in XCom):
            - count: number of expired LALVOTERIDs written
            - source_files: list of source file names
            - rows_staged: number of rows written to table
        """
        from airflow.sdk import get_current_context

        sftp_conn = BaseHook.get_connection("l2_sftp")
        expired_dir = Variable.get("l2_sftp_expired_dir")
        file_pattern = Variable.get("l2_sftp_expired_file_pattern")
        already_processed = set(processed_files)

        transport = None
        sftp_client = None
        try:
            transport, sftp_client = create_sftp_connection(
                host=sftp_conn.host,
                port=sftp_conn.port or 22,
                username=sftp_conn.login,
                password=sftp_conn.password,
            )

            with TemporaryDirectory(prefix="l2_expired_") as temp_dir:
                file_timestamps: Dict[str, str] = {}
                extracted_paths = download_files(
                    sftp_client=sftp_client,
                    remote_dir=expired_dir,
                    file_pattern=file_pattern,
                    local_dir=temp_dir,
                    file_timestamps=file_timestamps,
                )

                if not extracted_paths:
                    t_log.info("No expired voter files found on SFTP.")
                    return {
                        "count": 0,
                        "source_files": [],
                        "rows_staged": 0,
                    }

                # Build composite "filename|mtime" keys for idempotency so
                # republished files with the same name are re-processed.
                def _file_key(path: str) -> str:
                    basename = os.path.basename(path)
                    mtime = file_timestamps.get(basename, "")
                    return f"{basename}|{mtime}"

                # Filter out already-processed files
                new_paths = [
                    p for p in extracted_paths if _file_key(p) not in already_processed
                ]
                if not new_paths:
                    skipped = [_file_key(p) for p in extracted_paths]
                    t_log.info(
                        f"All {len(extracted_paths)} file(s) already processed, "
                        f"skipping: {skipped}"
                    )
                    return {
                        "count": 0,
                        "source_files": [],
                        "rows_staged": 0,
                    }

                if len(new_paths) < len(extracted_paths):
                    skipped = [
                        _file_key(p)
                        for p in extracted_paths
                        if _file_key(p) in already_processed
                    ]
                    t_log.info(f"Skipping already-processed files: {skipped}")

                lalvoterids = parse_expired_voter_ids(new_paths)
                _validate_lalvoterids(lalvoterids)
                source_files = [os.path.basename(p) for p in new_paths]
                # Keep only timestamps for new (non-skipped) files
                new_timestamps = {
                    os.path.basename(p): file_timestamps.get(os.path.basename(p), "")
                    for p in new_paths
                }

        finally:
            if sftp_client is not None:
                sftp_client.close()
            if transport is not None:
                transport.close()

        t_log.info(
            f"Ingested {len(lalvoterids)} expired LALVOTERIDs "
            f"from {len(source_files)} file(s): {source_files}"
        )

        # Write to Databricks
        context = get_current_context()
        dag_run_id = context["dag_run"].run_id

        db_conn_id = Variable.get("databricks_conn_id")
        db_conn = BaseHook.get_connection(db_conn_id)
        source_schema = Variable.get("databricks_source_schema")

        connection = get_databricks_connection(
            host=db_conn.host,
            http_path=db_conn.extra_dejson.get("http_path", ""),
            client_id=db_conn.login,
            client_secret=db_conn.password,
        )
        try:
            rows_staged = stage_expired_voter_ids(
                connection=connection,
                catalog=DATABRICKS_CATALOG,
                schema=source_schema,
                lalvoterids=lalvoterids,
                source_files=source_files,
                file_timestamps=new_timestamps,
                dag_run_id=dag_run_id,
            )
        finally:
            connection.close()

        return {
            "count": len(lalvoterids),
            "source_files": source_files,
            "rows_staged": rows_staged,
        }

    # ------------------------------------ #
    # Calling tasks + Setting dependencies #
    # ------------------------------------ #

    processed_files = fetch_processed_files()
    ingest_expired_voter_files(processed_files)


# Instantiate the DAG
l2_expired_voters()
