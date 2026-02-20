"""
## L2 Expired Voters Removal DAG

This DAG downloads expired L2 LALVOTERID files from L2's SFTP server, removes
the corresponding voter records from the Databricks `int__l2_nationwide_uniform`
table, then triggers a dbt Cloud full-refresh to propagate changes downstream.

### Pipeline Steps:
1. Query staging table for already-processed files (idempotency check)
2. Download new expired voter files from L2 SFTP, parse **all** LALVOTERIDs,
   and stage them to Databricks with `status='pending'`
3. Read staged IDs, apply state allowlist filter, then delete from Databricks
   `int__l2_nationwide_uniform`
4. Verify deletions succeeded (fails DAG if any IDs remain)
5. Mark staged rows as `status='completed'` (only after verification passes)
6. Trigger dbt Cloud full-refresh of downstream models

Marking complete (step 5) runs only after verification passes, so files
are considered processed only when deletions are confirmed. If the DAG
fails mid-delete, pending rows are cleaned up on retry (idempotent).

### Configuration:

**Connections** (set in Astro Environment Manager):
- `l2_sftp` (SFTP) — L2 SFTP server credentials
- `databricks` / `databricks_dev` (Generic) — host, login (OAuth client_id),
  password (OAuth client_secret), extras: `{"http_path": "/sql/1.0/warehouses/..."}`
- `dbt_cloud` (dbt Cloud) — API token, account ID

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
- `dbt_cloud_job_id` — integer ID of the dbt Cloud job to trigger for
  full-refresh of downstream models
"""

import logging
import os
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

from include.custom_functions.databricks_utils import (
    _validate_lalvoterids,
    count_in_databricks_table,
    delete_from_databricks_table,
    get_databricks_connection,
    get_processed_files,
    get_staged_voter_ids,
    mark_staging_complete,
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
from pendulum import datetime, duration

from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
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
    tags=["l2", "expired_voters", "deletion"],
    is_paused_upon_creation=True,
)
def l2_remove_expired_voters():

    @task
    def fetch_processed_files() -> List[str]:
        """
        Query the staging table for files that have already been processed
        (status='completed') so the ingest task can skip them (idempotency).
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
        Connect to L2 SFTP, download expired voter files, parse LALVOTERIDs,
        and stage them to Databricks with status='pending'.

        Skips files that have already been processed (listed in processed_files).

        Returns lightweight metadata (no LALVOTERID list in XCom):
            - count: number of expired LALVOTERIDs staged
            - source_files: list of source file names
            - rows_staged: number of rows written to staging table
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

        # Stage to Databricks with status='pending'
        context = get_current_context()
        dag_run_id = context["dag_run"].run_id

        db_conn_id = Variable.get("databricks_conn_id", default="databricks")
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

    @task
    def delete_from_databricks(ingest_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Delete expired LALVOTERIDs from Databricks `int__l2_nationwide_uniform`.

        Reads IDs from the staging table (avoids large XCom payloads).
        Applies the state allowlist filter so only matching states are deleted.
        """
        from airflow.sdk import get_current_context

        if not ingest_result["count"]:
            t_log.info("No LALVOTERIDs to delete from Databricks.")
            return {"l2_rows_deleted": 0}

        context = get_current_context()
        dag_run_id = context["dag_run"].run_id

        db_conn_id = Variable.get("databricks_conn_id", default="databricks")
        db_conn = BaseHook.get_connection(db_conn_id)
        source_schema = Variable.get("databricks_source_schema")
        voter_schema = Variable.get("databricks_voter_schema")
        l2_table = Variable.get(
            "databricks_l2_table", default="int__l2_nationwide_uniform"
        )

        connection = get_databricks_connection(
            host=db_conn.host,
            http_path=db_conn.extra_dejson.get("http_path", ""),
            client_id=db_conn.login,
            client_secret=db_conn.password,
        )
        try:
            lalvoterids = get_staged_voter_ids(
                connection=connection,
                catalog=DATABRICKS_CATALOG,
                schema=source_schema,
                dag_run_id=dag_run_id,
            )

            state_allowlist = Variable.get("l2_state_allowlist", default="")
            lalvoterids = filter_by_state_allowlist(lalvoterids, state_allowlist)
            if not lalvoterids:
                t_log.info("No LALVOTERIDs remain after state allowlist filter.")
                return {"l2_rows_deleted": 0}

            t_log.info(
                f"Deleting {len(lalvoterids)} expired voters from "
                f"{DATABRICKS_CATALOG}.{voter_schema}.{l2_table}"
            )

            l2_rows_deleted = delete_from_databricks_table(
                connection=connection,
                catalog=DATABRICKS_CATALOG,
                schema=voter_schema,
                table=l2_table,
                column="LALVOTERID",
                values=lalvoterids,
            )
        finally:
            connection.close()

        return {"l2_rows_deleted": l2_rows_deleted}

    @task
    def verify_deletions(
        ingest_result: Dict[str, Any],
        databricks_result: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Verify that all expired LALVOTERIDs were successfully removed from
        `int__l2_nationwide_uniform`. Raises an error if any remain.

        Reads IDs from the Databricks staging table (avoids large XCom payloads).
        Accepts databricks_result as input to ensure this task runs only after
        the delete task completes.
        """
        from airflow.sdk import get_current_context

        if not ingest_result["count"]:
            t_log.info("No LALVOTERIDs to verify — skipping.")
            return {"status": "skipped", "reason": "no_ids"}

        context = get_current_context()
        dag_run_id = context["dag_run"].run_id

        # Read staged IDs from Databricks
        db_conn_id = Variable.get("databricks_conn_id", default="databricks")
        db_conn = BaseHook.get_connection(db_conn_id)
        source_schema = Variable.get("databricks_source_schema")

        connection = get_databricks_connection(
            host=db_conn.host,
            http_path=db_conn.extra_dejson.get("http_path", ""),
            client_id=db_conn.login,
            client_secret=db_conn.password,
        )
        try:
            lalvoterids = get_staged_voter_ids(
                connection=connection,
                catalog=DATABRICKS_CATALOG,
                schema=source_schema,
                dag_run_id=dag_run_id,
            )

            state_allowlist = Variable.get("l2_state_allowlist", default="")
            filtered_ids = filter_by_state_allowlist(lalvoterids, state_allowlist)
            if not filtered_ids:
                t_log.info(
                    "No LALVOTERIDs remain after state allowlist filter — skipping."
                )
                return {"status": "skipped", "reason": "no_ids_after_filter"}

            voter_schema = Variable.get("databricks_voter_schema")
            l2_table = Variable.get(
                "databricks_l2_table", default="int__l2_nationwide_uniform"
            )

            l2_remaining = count_in_databricks_table(
                connection=connection,
                catalog=DATABRICKS_CATALOG,
                schema=voter_schema,
                table=l2_table,
                column="LALVOTERID",
                values=filtered_ids,
            )
        finally:
            connection.close()

        t_log.info(
            f"Verification results for {len(filtered_ids)} LALVOTERIDs:\n"
            f"  {l2_table}: {l2_remaining} remaining"
        )

        if l2_remaining > 0:
            raise RuntimeError(
                f"Deletion verification failed — "
                f"{l2_table}: {l2_remaining} rows still present"
            )

        t_log.info("All expired LALVOTERIDs verified removed from downstream tables.")
        return {
            "status": "verified",
            "ids_checked": len(filtered_ids),
            "l2_remaining": l2_remaining,
        }

    @task
    def mark_staged_complete(
        verify_result: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Mark staged rows as 'completed' after verification passes.

        Only completed rows are considered by the idempotency check in
        fetch_processed_files, so files are only marked as processed once
        deletions are confirmed.

        Scoped to the same state-allowlist-filtered IDs used by the delete
        and verify tasks, so IDs excluded by the allowlist stay 'pending'
        and can be retried when the allowlist changes.

        Accepts verify_result as input to enforce dependency ordering.
        """
        from airflow.sdk import get_current_context

        context = get_current_context()
        dag_run_id = context["dag_run"].run_id

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
            lalvoterids = get_staged_voter_ids(
                connection=connection,
                catalog=DATABRICKS_CATALOG,
                schema=schema,
                dag_run_id=dag_run_id,
            )

            state_allowlist = Variable.get("l2_state_allowlist", default="")
            filtered_ids = filter_by_state_allowlist(lalvoterids, state_allowlist)

            updated = mark_staging_complete(
                connection=connection,
                catalog=DATABRICKS_CATALOG,
                schema=schema,
                dag_run_id=dag_run_id,
                lalvoterids=filtered_ids,
            )
        finally:
            connection.close()

        return {"rows_marked_complete": updated}

    # ------------------------------------ #
    # Calling tasks + Setting dependencies #
    # ------------------------------------ #

    processed_files = fetch_processed_files()
    ingest_result = ingest_expired_voter_files(processed_files)
    db_result = delete_from_databricks(ingest_result)
    verify_result = verify_deletions(ingest_result, db_result)
    mark_result = mark_staged_complete(verify_result)

    dbt_cloud_job_id = Variable.get("dbt_cloud_job_id")
    trigger_dbt = DbtCloudRunJobOperator(
        task_id="trigger_dbt_full_refresh",
        dbt_cloud_conn_id="dbt_cloud",
        job_id=int(dbt_cloud_job_id),
        steps_override=[
            "dbt run --select int__l2_nationwide_uniform+ "
            "--exclude int__l2_nationwide_uniform --full-refresh"
        ],
        wait_for_termination=True,
        deferrable=True,
        check_interval=120,
        timeout=43200,  # 12 hours
    )
    mark_result >> trigger_dbt


# Instantiate the DAG
l2_remove_expired_voters()
