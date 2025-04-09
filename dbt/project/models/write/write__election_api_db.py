import base64
import importlib.util
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, concat_ws, lit, when

# install paramiko if not installed on the serverless databricks host
if importlib.util.find_spec("paramiko") is None:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "paramiko"])
from paramiko import AutoAddPolicy, SSHClient

# install psycopg2 if not installed
if importlib.util.find_spec("psycopg2") is None:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])

import psycopg2


# Function to transform array columns to PostgreSQL compatible format
def _prepare_df_for_postgres(df):
    """
    Transforms array columns in a DataFrame to PostgreSQL compatible format.

    Examples:
        - Input array column: ["apple", "banana", "cherry"]
            Output: "{apple,banana,cherry}"

        - Input array column: [1, 2, 3]
            Output: "{1,2,3}"

        - Input array column: None or []
            Output: NULL
    """
    # Copy the DataFrame to avoid changing the original
    transformed_df = df

    # Get the schema to identify array columns
    schema = df.schema
    for field in schema:
        # Check if the field is an array type
        if "array" in str(field.dataType).lower():
            # Convert array to PostgreSQL array format {val1,val2,val3}
            transformed_df = transformed_df.withColumn(
                field.name, concat_ws(",", col(field.name)).cast("string")
            ).withColumn(
                field.name,
                when(
                    col(field.name).isNotNull(),
                    concat(lit("{"), col(field.name), lit("}")),
                ),
            )

    return transformed_df


def model(dbt, session) -> DataFrame:
    """
    This model loads data for the mart that services the election api.
    The tables are written to the postgres database directly from spark
    since Airbyte does not support reads from databricks.
    """
    # configure the data model
    dbt.config(
        materialized="incremental",
        incremental_strategy="append",
        unique_key="id",
        on_schema_change="fail",
        tags=["ballotready", "election_api", "write", "postgres"],
        # packages = ["paramiko"]
    )

    # get db and ssh tunnel config
    db_host = dbt.config.get("election_db_host")
    db_port = int(dbt.config.get("election_db_port"))
    db_user = dbt.config.get("election_db_user")
    db_pw = dbt.config.get("election_db_pw")
    db_name = dbt.config.get("election_db_name")
    db_schema = dbt.config.get("election_db_schema")
    ssh_host = dbt.config.get("ssh_host")
    ssh_port = int(dbt.config.get("ssh_port"))
    ssh_username = dbt.config.get("ssh_user")
    ssh_pk_1 = dbt.config.get("ssh_pk_1")
    ssh_pk_2 = dbt.config.get("ssh_pk_2")
    ssh_pk_3 = dbt.config.get("ssh_pk_3")
    ssh_pk = ssh_pk_1 + ssh_pk_2 + ssh_pk_3

    # Temporary file paths
    ssh_key_file_path = None
    temp_csv_dir = None
    client = None
    sftp = None

    try:
        # get the data to write
        place_df: DataFrame = dbt.ref("m_election_api__place")
        race_df: DataFrame = dbt.ref("m_election_api__race")

        # TODO: separate place_df having `plarent_id` and without. May require separate loading to satisfy
        # foreign key constraints.

        # Create a temporary directory for CSV files
        temp_csv_dir = tempfile.mkdtemp()
        place_csv_path = f"{temp_csv_dir}/place.csv"
        race_csv_path = f"{temp_csv_dir}/race.csv"

        # Prepare DataFrames for PostgreSQL compatible format
        place_df_prepared = _prepare_df_for_postgres(place_df)
        race_df_prepared = _prepare_df_for_postgres(race_df)

        # Export to CSV
        logging.info(f"Writing place data to CSV at {place_csv_path}")
        place_df_prepared.write.format("csv").option("header", "true").mode(
            "overwrite"
        ).save(place_csv_path)

        logging.info(f"Writing race data to CSV at {race_csv_path}")
        race_df_prepared.write.format("csv").option("header", "true").mode(
            "overwrite"
        ).save(race_csv_path)

        logging.info("Data exported to CSV files")

        # Setup SSH connection with key
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=".pem", mode="w"
        ) as ssh_key_file:
            decoded_bytes = base64.b64decode(ssh_pk)
            decoded_string = decoded_bytes.decode("utf-8").strip()
            ssh_key_file.write(decoded_string)
            ssh_key_file_path = ssh_key_file.name

        # Use the key file for SSH connection
        client = SSHClient()
        client.set_missing_host_key_policy(
            AutoAddPolicy()
        )  # Automatically add unknown host keys
        client.connect(
            hostname=ssh_host,
            port=ssh_port,
            username=ssh_username,
            key_filename=ssh_key_file_path,
        )
        logging.info("Connected to bastion host")

        # Create an SSH tunnel for PostgreSQL connection
        # Forward a local port to the PostgreSQL server through the SSH tunnel
        local_port = 24601  # Local port to forward
        postgres_server = (db_host, db_port)
        transport = client.get_transport()

        # Create a channel for direct tcpip
        local_address = ("localhost", local_port)
        dest_address = (db_host, db_port)

        # Start the channel
        channel = transport.open_channel("direct-tcpip", dest_address, local_address)  # type: ignore

        # Test if the channel is active
        if not channel.active:
            raise Exception("Failed to establish SSH tunnel")

        logging.info(
            f"SSH tunnel established to PostgreSQL server at {db_host}:{db_port}"
        )

        # Create a PostgreSQL connection through the SSH tunnel
        pg_conn = psycopg2.connect(
            host="localhost",
            port=local_port,
            user=db_user,
            password=db_pw,
            database=db_name,
        )
        pg_conn.autocommit = True
        cursor = pg_conn.cursor()

        # Truncate existing tables
        logging.info(f"Truncating tables in schema {db_schema}")
        cursor.execute(f"TRUNCATE TABLE {db_schema}.place;")
        cursor.execute(f"TRUNCATE TABLE {db_schema}.race;")

        # Load data from CSV files directly
        with open(place_csv_path, "r") as f:
            next(f)  # Skip header
            cursor.copy_expert(f"COPY {db_schema}.place FROM STDIN WITH CSV HEADER", f)

        with open(race_csv_path, "r") as f:
            next(f)  # Skip header
            cursor.copy_expert(f"COPY {db_schema}.race FROM STDIN WITH CSV HEADER", f)

        cursor.close()
        pg_conn.close()

        logging.info("Successfully imported data into PostgreSQL")

        # log the loading information including the number of rows loaded
        columns = ["id", "table_name", "number_of_rows", "loaded_at"]
        data = [
            (
                str(uuid.uuid4()),
                "m_election_api__place",
                place_df.count(),
                datetime.now(),
            ),
            (
                str(uuid.uuid4()),
                "m_election_api__race",
                race_df.count(),
                datetime.now(),
            ),
        ]
        load_log_df = session.createDataFrame(data, columns)
    except Exception as e:
        logging.error(f"Error: {e}")
        raise e
    # force clean up
    finally:
        if "pg_conn" in locals() and pg_conn:
            pg_conn.close()
        if "channel" in locals() and channel:
            channel.close()
        if "transport" in locals() and transport:
            transport.close()
        if sftp:
            sftp.close()
        if client:
            client.close()
        if ssh_key_file_path and os.path.exists(ssh_key_file_path):
            os.remove(ssh_key_file_path)
        if temp_csv_dir and os.path.exists(temp_csv_dir):
            shutil.rmtree(temp_csv_dir)

    return load_log_df
