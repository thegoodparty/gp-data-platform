"""
## L2 Expired Voters Removal DAG

This DAG ingests expired L2 LALVOTERID files from L2's SFTP server and removes
the corresponding voter records from downstream systems (Databricks and People-API
PostgreSQL).

### Pipeline Steps:
1. Download expired voter files from L2 SFTP, parse LALVOTERIDs, and archive to S3
2. Delete expired rows from Databricks `int__l2_nationwide_uniform`
3. Delete expired rows from People-API PostgreSQL (`DistrictVoter` then `Voter`)

### Configuration:
All settings are managed via Airflow Variables. Set these before running:
- **SFTP**: `l2_sftp_host`, `l2_sftp_port`, `l2_sftp_user`, `l2_sftp_password`,
  `l2_sftp_expired_dir`, `l2_sftp_expired_file_pattern`
- **S3**: `l2_s3_bucket`, `l2_s3_access_key`, `l2_s3_secret_key`, `l2_s3_expired_prefix`
- **Databricks**: `databricks_host`, `databricks_http_path`, `databricks_access_token`,
  `databricks_catalog`, `databricks_schema`, `databricks_l2_table`
- **People-API DB**: `people_db_host`, `people_db_port`, `people_db_user`,
  `people_db_password`, `people_db_name`, `people_db_schema`
"""

import logging
import os
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

import boto3
from include.custom_functions.databricks_utils import (
    delete_from_databricks_table,
    get_databricks_connection,
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
from include.custom_functions.postgres_utils import (
    connect_db,
    delete_expired_voters,
)
from pendulum import datetime, duration

from airflow.models import Variable
from airflow.sdk import dag, task

t_log = logging.getLogger("airflow.task")


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
    def ingest_expired_voter_files() -> Dict[str, Any]:
        """
        Connect to L2 SFTP, download expired voter files, parse LALVOTERIDs,
        and archive raw files to S3 for audit trail.

        Returns a dict with keys:
            - lalvoterids: list of expired LALVOTERID strings
            - source_files: list of source file names
            - s3_keys: list of S3 keys where files were archived
        """
        # SFTP config
        sftp_host = Variable.get("l2_sftp_host")
        sftp_port = int(Variable.get("l2_sftp_port"))
        sftp_user = Variable.get("l2_sftp_user")
        sftp_password = Variable.get("l2_sftp_password")
        expired_dir = Variable.get("l2_sftp_expired_dir")
        file_pattern = Variable.get("l2_sftp_expired_file_pattern")

        # S3 config
        s3_bucket = Variable.get("l2_s3_bucket")
        s3_access_key = Variable.get("l2_s3_access_key")
        s3_secret_key = Variable.get("l2_s3_secret_key")
        s3_prefix = Variable.get("l2_s3_expired_prefix")

        transport = None
        sftp_client = None
        try:
            transport, sftp_client = create_sftp_connection(
                host=sftp_host,
                port=sftp_port,
                username=sftp_user,
                password=sftp_password,
            )

            with TemporaryDirectory(prefix="l2_expired_") as temp_dir:
                # Download and extract files from SFTP
                extracted_paths = download_files(
                    sftp_client=sftp_client,
                    remote_dir=expired_dir,
                    file_pattern=file_pattern,
                    local_dir=temp_dir,
                )

                if not extracted_paths:
                    t_log.info("No expired voter files found on SFTP.")
                    return {"lalvoterids": [], "source_files": [], "s3_keys": []}

                # Parse LALVOTERID values
                lalvoterids = parse_expired_voter_ids(extracted_paths)
                source_files = [os.path.basename(p) for p in extracted_paths]

                # Archive to S3
                s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=s3_access_key,
                    aws_secret_access_key=s3_secret_key,
                )
                s3_keys: List[str] = []
                for local_path in extracted_paths:
                    filename = os.path.basename(local_path)
                    s3_key = f"{s3_prefix}{filename}"
                    s3_client.upload_file(
                        Filename=local_path, Bucket=s3_bucket, Key=s3_key
                    )
                    s3_keys.append(s3_key)
                    t_log.info(f"Archived {filename} to s3://{s3_bucket}/{s3_key}")

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
            "s3_keys": s3_keys,
        }

    @task
    def delete_from_databricks(ingest_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Delete expired LALVOTERIDs from the Databricks int__l2_nationwide_uniform table.
        """
        lalvoterids = ingest_result["lalvoterids"]
        if not lalvoterids:
            t_log.info("No LALVOTERIDs to delete from Databricks.")
            return {"rows_deleted": 0}

        db_host = Variable.get("databricks_host")
        db_http_path = Variable.get("databricks_http_path")
        db_token = Variable.get("databricks_access_token")
        catalog = Variable.get(
            "databricks_catalog", default_var="goodparty_data_catalog"
        )
        schema = Variable.get("databricks_schema")
        table = Variable.get(
            "databricks_l2_table", default_var="int__l2_nationwide_uniform"
        )

        t_log.info(
            f"Deleting {len(lalvoterids)} expired voters from "
            f"{catalog}.{schema}.{table}"
        )

        connection = get_databricks_connection(
            host=db_host, http_path=db_http_path, access_token=db_token
        )
        try:
            rows_deleted = delete_from_databricks_table(
                connection=connection,
                catalog=catalog,
                schema=schema,
                table=table,
                column="LALVOTERID",
                values=lalvoterids,
            )
        finally:
            connection.close()

        return {"rows_deleted": rows_deleted}

    @task
    def delete_from_people_api(ingest_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Delete expired LALVOTERIDs from People-API PostgreSQL.
        Deletes DistrictVoter rows first (FK dependency), then Voter rows.
        """
        lalvoterids = ingest_result["lalvoterids"]
        if not lalvoterids:
            t_log.info("No LALVOTERIDs to delete from People-API.")
            return {"district_voter_deleted": 0, "voter_deleted": 0}

        db_host = Variable.get("people_db_host")
        db_port = int(Variable.get("people_db_port"))
        db_user = Variable.get("people_db_user")
        db_password = Variable.get("people_db_password")
        db_name = Variable.get("people_db_name")
        db_schema = Variable.get("people_db_schema")

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

    ingest_result = ingest_expired_voter_files()
    # Deletions run in parallel after ingestion + S3 archive completes
    delete_from_databricks(ingest_result)
    delete_from_people_api(ingest_result)


# Instantiate the DAG
l2_remove_expired_voters()
