"""
## L2 Expired Voters Removal DAG

This DAG downloads expired L2 LALVOTERID files from L2's SFTP server and removes
the corresponding voter records from downstream systems (Databricks and People-API
PostgreSQL).

### Pipeline Steps:
1. Query staging table for already-processed files (idempotency check)
2. Download new expired voter files from L2 SFTP and parse **all** LALVOTERIDs
3. Stage **all** expired LALVOTERIDs to `airflow_source.l2_expired_voters` for dbt visibility
4. Apply state allowlist filter, then delete from Databricks `int__l2_nationwide_uniform`
   and `m_people_api__voter`
5. Apply state allowlist filter, then delete from People-API PostgreSQL

Steps 3–5 run in parallel after step 2 completes.
The state allowlist is only applied to deletions (steps 4–5), not to staging (step 3).

### Configuration:

**Connections** (set in Astro Environment Manager):
- `l2_sftp` (SFTP) — L2 SFTP server credentials
- `databricks` / `databricks_dev` (Generic) — host, login (OAuth client_id),
  password (OAuth client_secret), extras: `{"http_path": "/sql/1.0/warehouses/..."}`
- `people_api_db` (Postgres) — host, port, login, password;
  extras: `{"database": "...", "schema": "..."}`

**Variables** (set in Astro Environment Manager):
- `l2_sftp_expired_dir` — SFTP directory for expired voter files
- `l2_sftp_expired_file_pattern` — regex pattern for matching files
- `databricks_voter_schema` — Databricks schema where voter models live
  (e.g., `dbt` in prod, `dbt_hugh` in dev)
- `databricks_l2_table` — (optional, default: `int__l2_nationwide_uniform`)
- `databricks_conn_id` — (optional, default: `databricks`) connection ID for Databricks;
  set to `databricks_dev` in dev, `databricks` in prod
- `l2_state_allowlist` — (optional) comma-separated state codes to filter
  deletions by (e.g., `NC,WY` for dev). Only affects delete tasks — all IDs
  are still staged for auditability. Empty or unset = delete all states.
- `databricks_source_schema` — schema where Airflow stages ingested data for dbt
  visibility (e.g., `airflow_source` in prod, `airflow_source_dev` in dev)
"""

import logging
import os
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

from include.custom_functions.databricks_utils import (
    delete_from_databricks_table,
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
    filter_by_state_allowlist,
    parse_expired_voter_ids,
)
from include.custom_functions.postgres_utils import (
    connect_db,
    delete_expired_voters,
)
from pendulum import datetime, duration

from airflow.sdk import BaseHook, Variable, dag, task

t_log = logging.getLogger("airflow.task")

DATABRICKS_CATALOG = "goodparty_data_catalog"


@dag(
    start_date=datetime(2025, 6, 1),
    schedule=None,  # manual trigger until SFTP file details are confirmed
    max_consecutive_failed_dag_runs=5,
    max_active_runs=1,
    doc_md=__doc__,
    default_args={
        "owner": "Data Engineering Team",
        "retries": 3,
        "retry_delay": duration(seconds=30),
    },
    tags=["l2", "expired_voters", "deletion"],
    is_paused_upon_creation=True,
)
def l2_remove_expired_voters():

    @task
    def fetch_processed_files() -> List[str]:
        """
        Query the staging table for files that have already been processed
        so the ingest task can skip them (idempotency).
        """
        db_conn_id = Variable.get("databricks_conn_id", default="databricks")
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
        Connect to L2 SFTP, download expired voter files, and parse LALVOTERIDs.
        Skips files that have already been processed (listed in processed_files).

        Returns a dict with keys:
            - lalvoterids: list of expired LALVOTERID strings
            - source_files: list of source file names
            - file_timestamps: dict mapping basename to SFTP mtime (ISO 8601)
        """
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
                        "lalvoterids": [],
                        "source_files": [],
                        "file_timestamps": {},
                    }

                # Filter out already-processed files
                new_paths = [
                    p
                    for p in extracted_paths
                    if os.path.basename(p) not in already_processed
                ]
                if not new_paths:
                    skipped = [os.path.basename(p) for p in extracted_paths]
                    t_log.info(
                        f"All {len(extracted_paths)} file(s) already processed, "
                        f"skipping: {skipped}"
                    )
                    return {
                        "lalvoterids": [],
                        "source_files": [],
                        "file_timestamps": {},
                    }

                if len(new_paths) < len(extracted_paths):
                    skipped = [
                        os.path.basename(p)
                        for p in extracted_paths
                        if os.path.basename(p) in already_processed
                    ]
                    t_log.info(f"Skipping already-processed files: {skipped}")

                lalvoterids = parse_expired_voter_ids(new_paths)
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
        return {
            "lalvoterids": lalvoterids,
            "source_files": source_files,
            "file_timestamps": new_timestamps,
        }

    @task
    def stage_to_databricks(ingest_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Write ingested expired LALVOTERIDs to a staging table in the airflow_source
        schema so they are visible in dbt for auditability.
        """
        from airflow.sdk import get_current_context

        lalvoterids = ingest_result["lalvoterids"]
        if not lalvoterids:
            t_log.info("No LALVOTERIDs to stage.")
            return {"rows_staged": 0}

        db_conn_id = Variable.get("databricks_conn_id", default="databricks")
        db_conn = BaseHook.get_connection(db_conn_id)
        schema = Variable.get("databricks_source_schema")

        t_log.info(
            f"Staging {len(lalvoterids)} expired voters to "
            f"{DATABRICKS_CATALOG}.{schema}.l2_expired_voters"
        )

        context = get_current_context()
        dag_run_id = context["dag_run"].run_id

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
                schema=schema,
                lalvoterids=lalvoterids,
                source_files=ingest_result["source_files"],
                file_timestamps=ingest_result.get("file_timestamps", {}),
                dag_run_id=dag_run_id,
            )
        finally:
            connection.close()

        return {"rows_staged": rows_staged}

    @task
    def delete_from_databricks(ingest_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Delete expired LALVOTERIDs from Databricks voter tables:
          - int__l2_nationwide_uniform (source intermediate table)
          - m_people_api__voter (downstream mart — incremental, won't auto-delete)

        Applies the state allowlist filter so only matching states are deleted.
        """
        lalvoterids = ingest_result["lalvoterids"]
        if not lalvoterids:
            t_log.info("No LALVOTERIDs to delete from Databricks.")
            return {"l2_rows_deleted": 0, "voter_mart_rows_deleted": 0}

        state_allowlist = Variable.get("l2_state_allowlist", default="")
        lalvoterids = filter_by_state_allowlist(lalvoterids, state_allowlist)
        if not lalvoterids:
            t_log.info("No LALVOTERIDs remain after state allowlist filter.")
            return {"l2_rows_deleted": 0, "voter_mart_rows_deleted": 0}

        db_conn_id = Variable.get("databricks_conn_id", default="databricks")
        db_conn = BaseHook.get_connection(db_conn_id)
        schema = Variable.get("databricks_voter_schema")
        l2_table = Variable.get(
            "databricks_l2_table", default="int__l2_nationwide_uniform"
        )

        t_log.info(
            f"Deleting {len(lalvoterids)} expired voters from "
            f"{DATABRICKS_CATALOG}.{schema}.{l2_table} "
            f"and {DATABRICKS_CATALOG}.{schema}.m_people_api__voter"
        )

        connection = get_databricks_connection(
            host=db_conn.host,
            http_path=db_conn.extra_dejson.get("http_path", ""),
            client_id=db_conn.login,
            client_secret=db_conn.password,
        )
        try:
            l2_rows_deleted = delete_from_databricks_table(
                connection=connection,
                catalog=DATABRICKS_CATALOG,
                schema=schema,
                table=l2_table,
                column="LALVOTERID",
                values=lalvoterids,
            )
            voter_mart_rows_deleted = delete_from_databricks_table(
                connection=connection,
                catalog=DATABRICKS_CATALOG,
                schema=schema,
                table="m_people_api__voter",
                column="LALVOTERID",
                values=lalvoterids,
            )
        finally:
            connection.close()

        return {
            "l2_rows_deleted": l2_rows_deleted,
            "voter_mart_rows_deleted": voter_mart_rows_deleted,
        }

    @task
    def delete_from_people_api(ingest_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Delete expired LALVOTERIDs from People-API PostgreSQL.
        Deletes DistrictVoter rows first (FK dependency), then Voter rows.
        Applies the state allowlist filter so only matching states are deleted.
        """
        lalvoterids = ingest_result["lalvoterids"]
        if not lalvoterids:
            t_log.info("No LALVOTERIDs to delete from People-API.")
            return {"district_voter_deleted": 0, "voter_deleted": 0}

        state_allowlist = Variable.get("l2_state_allowlist", default="")
        lalvoterids = filter_by_state_allowlist(lalvoterids, state_allowlist)
        if not lalvoterids:
            t_log.info("No LALVOTERIDs remain after state allowlist filter.")
            return {"district_voter_deleted": 0, "voter_deleted": 0}

        pg_conn = BaseHook.get_connection("people_api_db")
        extras = pg_conn.extra_dejson
        db_host = pg_conn.host
        db_port = pg_conn.port or 5432
        db_user = pg_conn.login
        db_password = pg_conn.password
        db_name = extras.get("database", pg_conn.schema)
        db_schema = extras.get("schema", "public")

        t_log.info(
            f"Deleting {len(lalvoterids)} expired voters from People-API "
            f"({db_host}/{db_name}, schema={db_schema})"
        )

        conn = connect_db(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
        )
        try:
            result = delete_expired_voters(
                conn=conn,
                schema=db_schema,
                lalvoterids=lalvoterids,
            )
        finally:
            conn.close()

        return result

    # ------------------------------------ #
    # Calling tasks + Setting dependencies #
    # ------------------------------------ #

    processed_files = fetch_processed_files()
    ingest_result = ingest_expired_voter_files(processed_files)
    # Stage + deletions run in parallel after ingestion completes
    stage_to_databricks(ingest_result)
    delete_from_databricks(ingest_result)
    delete_from_people_api(ingest_result)


# Instantiate the DAG
l2_remove_expired_voters()
