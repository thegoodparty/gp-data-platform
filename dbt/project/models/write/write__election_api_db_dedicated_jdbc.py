import base64
import importlib.util
import logging
import os
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime

import psycopg2
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, concat_ws, lit, when

# install required packages if not installed on the databricks host
if importlib.util.find_spec("sshtunnel") is None:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "sshtunnel"])
from sshtunnel import SSHTunnelForwarder


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
    This model loads data for the mart that services the election API. The tables are written
    to the postgres database directly from spark since Airbyte does not support reads from
    databricks. We use JDBC through an SSH tunnel, which requires a dedicated compute instance
    rather than the default serverless.
    """
    # configure the data model
    dbt.config(
        submission_method="all_purpose_cluster",  # required to write with jdbc
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",  # required to write with jdbc
        materialized="incremental",
        incremental_strategy="append",
        unique_key="id",
        on_schema_change="fail",
        tags=["ballotready", "election_api", "write", "postgres"],
        # TODO: once serverless supports library installations on jobs, uncomment the following line
        # packages=["paramiko", "sshtunnel"],
    )

    # Add barrier execution mode to ensure operations happen on the driver
    session.conf.set("spark.databricks.barrier.mode", "true")

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

    # fix the forwarded port
    local_port = 8090
    # local_port = 5432

    # Temporary file path for SSH key
    ssh_key_file_path = None
    tunnel = None

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
            # decoded_string = decoded_bytes.decode("utf-8").strip()
            decoded_string = decoded_bytes.decode("utf-8")
            ssh_key_file.write(decoded_string)
            ssh_key_file_path = ssh_key_file.name

        logging.info(
            f"Attempting to connect to PostgreSQL at {db_host}:{db_port} through bastion host {ssh_host}"
        )

        # Create SSH tunnel with explicit IP address instead of 'localhost'
        local_host = "localhost"
        # local_host = "127.0.0.1"
        tunnel = SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_username,
            ssh_pkey=ssh_key_file_path,
            remote_bind_address=(db_host, db_port),
            local_bind_address=(local_host, local_port),
            # local_bind_address=(local_host, 0),
            set_keepalive=10,  # Keep the connection alive
            mute_exceptions=False,  # Show exceptions
        )

        # Start the tunnel
        tunnel.start()

        # Test if the tunnel is active
        if not tunnel.is_active:
            logging.error("SSH tunnel failed to start")
            raise Exception("Failed to establish SSH tunnel connection")

        # Display detailed tunnel information for debugging
        logging.info(f"SSH tunnel active: {tunnel.is_active}")
        logging.info(f"Local bind port: {tunnel.local_bind_port}")
        logging.info(f"Local bind host: {tunnel.local_bind_host}")

        # Verify tunnel works by checking connectivity to a public endpoint
        try:
            import requests

            response = requests.get("https://api.ipify.org", timeout=5)
            if response.status_code == 200:
                logging.info(
                    f"Tunnel connectivity verified. Public IP: {response.text}"
                )
            else:
                logging.warning(
                    f"Tunnel connectivity check returned status code: {response.status_code}"
                )
        except Exception as e:
            logging.warning(f"Tunnel connectivity check failed: {str(e)}")

        logging.info("SSH tunnel started successfully")

        # Get the dynamically allocated local port
        assert tunnel.local_bind_port == local_port
        # local_port = tunnel.local_bind_port

        logging.info(f"SSH tunnel established with local port: {local_port}")
        jdbc_url = f"jdbc:postgresql://{local_host}:{local_port}/{db_name}"
        logging.info(f"JDBC URL: {jdbc_url}")

        # Test connection to PostgreSQL database
        logging.info(
            f"Attempting to connect to PostgreSQL through tunnel at {local_host}:{local_port}"
        )

        # ensure the connection is made from the driver node
        from pyspark.sql.functions import spark_partition_id

        if spark_partition_id() == 0:
            # Connect to PostgreSQL through the tunnel
            conn = psycopg2.connect(
                host=local_host,  # Use explicit IP instead of localhost
                port=local_port,  # This is forwarded to the remote database
                user=db_user,
                password=db_pw,
                database=db_name,
                connect_timeout=30,  # Increase connection timeout
            )

            # Test the connection
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()

        if result and result[0] == 1:
            logging.info(
                "Successfully connected to the database through the SSH tunnel!"
            )

            # Close cursor and connection
            cursor.close()
            conn.close()
        else:
            logging.warning("Database connection test returned unexpected result")

            # Close cursor and connection
            cursor.close()
            conn.close()

        # JDBC connection properties using the explicit IP
        jdbc_url = f"jdbc:postgresql://{local_host}:{local_port}/{db_name}"
        # properties = {
        #     "user": db_user,
        #     "password": db_pw,
        #     "driver": "org.postgresql.Driver",
        #     "url": jdbc_url,
        #     "dbtable": f"{db_schema}.place",
        # }

        # Write the DataFrames to PostgreSQL using JDBC
        logging.info("Writing place data to PostgreSQL via JDBC")

        # Write place data directly using JDBC
        # Use coalesce to ensure all writes go through the SSH tunnel on the driver node
        place_df.coalesce(1).write.format("jdbc").option("url", jdbc_url).option(
            "dbtable", f"{db_schema}.place"
        ).option("user", db_user).option("password", db_pw).option(
            "driver", "org.postgresql.Driver"
        ).mode(
            "overwrite"
        ).save()

        logging.info("Writing race data to PostgreSQL via JDBC")
        # Use coalesce to ensure all writes go through the SSH tunnel on the driver node
        race_df.coalesce(1).write.format("jdbc").option("url", jdbc_url).option(
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
    finally:
        # Clean up resources
        if tunnel is not None and tunnel.is_active:
            try:
                tunnel.stop()
                logging.info("SSH tunnel stopped successfully")
            except Exception as e:
                logging.error(f"Error stopping SSH tunnel: {e}")
        if ssh_key_file_path and os.path.exists(ssh_key_file_path):
            os.remove(ssh_key_file_path)
            logging.info("SSH key file removed")

    return load_log_df
