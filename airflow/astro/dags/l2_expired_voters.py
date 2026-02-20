"""
## L2 Expired Voters Removal DAG

This DAG downloads expired L2 LALVOTERID files from L2's SFTP server and removes
the corresponding voter records from downstream systems (Databricks and People-API
PostgreSQL).

### Pipeline Steps:
1. Query staging table for already-processed files (idempotency check)
2. Download new expired voter files from L2 SFTP, parse **all** LALVOTERIDs,
   and stage them to Databricks with `status='pending'`
3. Read staged IDs, apply state allowlist filter, then delete from Databricks
   `int__l2_nationwide_uniform` and `m_people_api__voter` (parallel)
4. Read staged IDs, apply state allowlist filter, then delete from People-API
   PostgreSQL (parallel with step 3)
5. Verify all deletions succeeded (fails DAG if any IDs remain)
6. Mark staged rows as `status='completed'` (only after verification passes)

Steps 3–4 run in parallel. Marking complete (step 6) runs only after
verification passes, so files are considered processed only when deletions
are confirmed. If the DAG fails mid-delete, pending rows are cleaned up
on retry (idempotent).

### Configuration:

**Connections** (set in Astro Environment Manager):
- `l2_sftp` (SFTP) — L2 SFTP server credentials
- `databricks` / `databricks_dev` (Generic) — host, login (OAuth client_id),
  password (OAuth client_secret), extras: `{"http_path": "/sql/1.0/warehouses/..."}`
- `people_api_db` (Postgres) — host, port, login, password,
  schema (= database name, e.g. `people_prod`)
- `gp_bastion_host` (SSH) — SSH bastion/jump host for tunneling to RDS;
  host, port (default 22), login (SSH username), private key;
  set NO_HOST_KEY_CHECK=True

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
- `people_api_schema` — SQL schema where Voter/DistrictVoter tables live
  (e.g. `green`)
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
from include.custom_functions.postgres_utils import (
    connect_db,
    delete_expired_voters,
    execute_query,
)
from pendulum import datetime, duration

from airflow.sdk import BaseHook, Variable, dag, task

t_log = logging.getLogger("airflow.task")

DATABRICKS_CATALOG = "goodparty_data_catalog"


@dag(
    start_date=datetime(2025, 6, 1),
    schedule="@weekly",
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
        Delete expired LALVOTERIDs from Databricks voter tables:
          - int__l2_nationwide_uniform (source intermediate table)
          - m_people_api__voter (downstream mart — incremental, won't auto-delete)

        Reads IDs from the staging table (avoids large XCom payloads).
        Applies the state allowlist filter so only matching states are deleted.
        """
        from airflow.sdk import get_current_context

        if not ingest_result["count"]:
            t_log.info("No LALVOTERIDs to delete from Databricks.")
            return {"l2_rows_deleted": 0, "voter_mart_rows_deleted": 0}

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
                return {"l2_rows_deleted": 0, "voter_mart_rows_deleted": 0}

            t_log.info(
                f"Deleting {len(lalvoterids)} expired voters from "
                f"{DATABRICKS_CATALOG}.{voter_schema}.{l2_table} "
                f"and {DATABRICKS_CATALOG}.{voter_schema}.m_people_api__voter"
            )

            l2_rows_deleted = delete_from_databricks_table(
                connection=connection,
                catalog=DATABRICKS_CATALOG,
                schema=voter_schema,
                table=l2_table,
                column="LALVOTERID",
                values=lalvoterids,
            )
            voter_mart_rows_deleted = delete_from_databricks_table(
                connection=connection,
                catalog=DATABRICKS_CATALOG,
                schema=voter_schema,
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

        Reads IDs from the Databricks staging table (avoids large XCom payloads).
        Applies the state allowlist filter so only matching states are deleted.
        """
        from airflow.sdk import get_current_context

        if not ingest_result["count"]:
            t_log.info("No LALVOTERIDs to delete from People-API.")
            return {"district_voter_deleted": 0, "voter_deleted": 0}

        context = get_current_context()
        dag_run_id = context["dag_run"].run_id

        # Read staged IDs from Databricks
        db_conn_id = Variable.get("databricks_conn_id", default="databricks")
        db_conn = BaseHook.get_connection(db_conn_id)
        source_schema = Variable.get("databricks_source_schema")

        db_connection = get_databricks_connection(
            host=db_conn.host,
            http_path=db_conn.extra_dejson.get("http_path", ""),
            client_id=db_conn.login,
            client_secret=db_conn.password,
        )
        try:
            lalvoterids = get_staged_voter_ids(
                connection=db_connection,
                catalog=DATABRICKS_CATALOG,
                schema=source_schema,
                dag_run_id=dag_run_id,
            )
        finally:
            db_connection.close()

        state_allowlist = Variable.get("l2_state_allowlist", default="")
        lalvoterids = filter_by_state_allowlist(lalvoterids, state_allowlist)
        if not lalvoterids:
            t_log.info("No LALVOTERIDs remain after state allowlist filter.")
            return {"district_voter_deleted": 0, "voter_deleted": 0}

        # Delete from PostgreSQL (via SSH tunnel to reach private VPC)
        from airflow.providers.ssh.hooks.ssh import SSHHook

        pg_conn = BaseHook.get_connection("people_api_db")
        db_host = pg_conn.host
        db_port = pg_conn.port or 5432
        db_user = pg_conn.login
        db_password = pg_conn.password
        db_name = pg_conn.schema  # Airflow Postgres convention: schema = DB name
        db_schema = Variable.get("people_api_schema")

        t_log.info(
            f"Deleting {len(lalvoterids)} expired voters from People-API "
            f"({db_host}/{db_name}, schema={db_schema})"
        )

        ssh_hook = SSHHook(ssh_conn_id="gp_bastion_host")
        tunnel = ssh_hook.get_tunnel(remote_port=db_port, remote_host=db_host)
        tunnel.start()
        try:
            conn = connect_db(
                host="127.0.0.1",
                port=tunnel.local_bind_port,
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
        finally:
            tunnel.stop()

        return result

    @task
    def verify_deletions(
        ingest_result: Dict[str, Any],
        databricks_result: Dict[str, Any],
        people_api_result: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Verify that all expired LALVOTERIDs were successfully removed from
        downstream tables. Raises an error if any remain.

        Reads IDs from the Databricks staging table (avoids large XCom payloads).
        Accepts databricks_result and people_api_result as inputs to ensure
        this task runs only after both delete tasks complete.
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

            # --- Verify Databricks ---
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
            voter_mart_remaining = count_in_databricks_table(
                connection=connection,
                catalog=DATABRICKS_CATALOG,
                schema=voter_schema,
                table="m_people_api__voter",
                column="LALVOTERID",
                values=filtered_ids,
            )
        finally:
            connection.close()

        # --- Verify People-API PostgreSQL (via SSH tunnel) ---
        from airflow.providers.ssh.hooks.ssh import SSHHook

        pg_conn = BaseHook.get_connection("people_api_db")
        db_host = pg_conn.host
        db_port = pg_conn.port or 5432
        db_user = pg_conn.login
        db_password = pg_conn.password
        db_name = pg_conn.schema  # Airflow Postgres convention: schema = DB name
        db_schema = Variable.get("people_api_schema")

        ssh_hook = SSHHook(ssh_conn_id="gp_bastion_host")
        tunnel = ssh_hook.get_tunnel(remote_port=db_port, remote_host=db_host)
        tunnel.start()
        try:
            conn = connect_db(
                host="127.0.0.1",
                port=tunnel.local_bind_port,
                user=db_user,
                password=db_password,
                database=db_name,
            )
            try:
                result = execute_query(
                    conn,
                    f'SELECT COUNT(*) FROM {db_schema}."Voter" '
                    f'WHERE "LALVOTERID" = ANY(%s)',
                    (filtered_ids,),
                    return_results=True,
                )
                pg_remaining = result[0][0] if result else 0
            finally:
                conn.close()
        finally:
            tunnel.stop()

        # --- Report ---
        t_log.info(
            f"Verification results for {len(filtered_ids)} LALVOTERIDs:\n"
            f"  {l2_table}: {l2_remaining} remaining\n"
            f"  m_people_api__voter: {voter_mart_remaining} remaining\n"
            f"  People-API Voter: {pg_remaining} remaining"
        )

        failures = []
        if l2_remaining > 0:
            failures.append(f"{l2_table}: {l2_remaining} rows still present")
        if voter_mart_remaining > 0:
            failures.append(
                f"m_people_api__voter: {voter_mart_remaining} rows still present"
            )
        if pg_remaining > 0:
            failures.append(f"People-API Voter: {pg_remaining} rows still present")

        if failures:
            raise RuntimeError(f"Deletion verification failed — {'; '.join(failures)}")

        t_log.info("All expired LALVOTERIDs verified removed from downstream tables.")
        return {
            "status": "verified",
            "ids_checked": len(filtered_ids),
            "l2_remaining": l2_remaining,
            "voter_mart_remaining": voter_mart_remaining,
            "pg_remaining": pg_remaining,
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
    # Deletions run in parallel after ingestion + staging completes
    db_result = delete_from_databricks(ingest_result)
    pa_result = delete_from_people_api(ingest_result)
    # Verify after both deletes complete
    verify_result = verify_deletions(ingest_result, db_result, pa_result)
    # Mark staged rows as completed only after verification passes
    mark_staged_complete(verify_result)


# Instantiate the DAG
l2_remove_expired_voters()
