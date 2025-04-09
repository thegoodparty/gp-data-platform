import base64
import importlib.util
import logging
import os
import socket
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
    since Airbyte does not support reads from databricks. We use JDBC
    through an SSH tunnel.
    """
    # configure the data model
    dbt.config(
        materialized="incremental",
        connection="databricks-dedicated-cluster",
        incremental_strategy="append",
        unique_key="id",
        on_schema_change="fail",
        tags=["ballotready", "election_api", "write", "postgres"],
        # TODO: once serverless supports library installations on jobs, uncomment the following line
        # packages=["paramiko"],
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

    # Temporary file path for SSH key
    ssh_key_file_path = None
    ssh_client = None
    local_port = None
    channel = None
    transport = None

    try:
        # get the data to write
        place_df: DataFrame = dbt.ref("m_election_api__place")
        race_df: DataFrame = dbt.ref("m_election_api__race")

        # TODO: separate place_df having `plarent_id` and without. May require separate loading to satisfy
        # foreign key constraints.

        # Prepare DataFrames for PostgreSQL compatible format
        # place_df_prepared = _prepare_df_for_postgres(place_df)
        # race_df_prepared = _prepare_df_for_postgres(race_df)

        # Setup SSH connection with key
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=".pem", mode="w"
        ) as ssh_key_file:
            decoded_bytes = base64.b64decode(ssh_pk)
            decoded_string = decoded_bytes.decode("utf-8").strip()
            ssh_key_file.write(decoded_string)
            ssh_key_file_path = ssh_key_file.name

        # Use the key file for SSH connection
        ssh_client = SSHClient()
        ssh_client.set_missing_host_key_policy(
            AutoAddPolicy()
        )  # Automatically add unknown host keys
        ssh_client.connect(
            hostname=ssh_host,
            port=ssh_port,
            username=ssh_username,
            key_filename=ssh_key_file_path,
        )
        logging.info("Connected to bastion host")

        # Set up port forwarding
        local_port = 24629  # Local port to forward
        transport = ssh_client.get_transport()
        transport.request_port_forward("localhost", local_port)  # type: ignore
        tunnel_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tunnel_socket.bind(("localhost", local_port))
        tunnel_socket.listen(1)

        # Create a channel for direct tcpip
        local_address = ("localhost", local_port)
        dest_address = (db_host, db_port)
        # Log connection details for debugging
        logging.info(
            f"Attempting to connect to PostgreSQL at {db_host}:{db_port} through bastion host {ssh_host}"
        )
        try:
            # Start the channel
            channel = transport.open_channel(  # type: ignore
                kind="direct-tcpip",
                dest_addr=dest_address,
                src_addr=local_address,
                timeout=10,
            )  # type: ignore
        except Exception as e:
            logging.error(f"Failed to establish channel: {e}")
            logging.error(
                f"Connection details: SSH host={ssh_host}, DB host={db_host}, DB port={db_port}"
            )
            raise Exception(f"Failed to establish SSH tunnel to database: {e}") from e

        # Test if the channel is active
        if not channel.active:
            raise Exception("Failed to establish SSH tunnel")

        logging.info(
            f"SSH tunnel established to PostgreSQL server at {db_host}:{db_port}"
        )

        # JDBC connection properties
        jdbc_url = f"jdbc:postgresql://localhost:{local_port}/{db_name}"
        properties = {
            "user": db_user,
            "password": db_pw,
            "driver": "org.postgresql.Driver",
        }

        # Write the DataFrames to PostgreSQL using JDBC
        logging.info("Writing place data to PostgreSQL via JDBC")
        place_df.write.format("jdbc").option("url", jdbc_url).option(
            "dbtable", f"{db_schema}.place"
        ).option("user", db_user).option("password", db_pw).option(
            "driver", "org.postgresql.Driver"
        ).mode(
            "append"
        ).save()

        logging.info("Writing race data to PostgreSQL via JDBC")
        race_df.write.format("jdbc").option("url", jdbc_url).option(
            "dbtable", f"{db_schema}.race"
        ).option("user", db_user).option("password", db_pw).option(
            "driver", "org.postgresql.Driver"
        ).mode(
            "append"
        ).save()

        logging.info("Successfully imported data into PostgreSQL via JDBC")

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
        if "channel" in locals() and channel:
            channel.close()
        if "transport" in locals() and transport:
            transport.close()
        if ssh_client:
            ssh_client.close()
        if ssh_key_file_path and os.path.exists(ssh_key_file_path):
            os.remove(ssh_key_file_path)

    return load_log_df
