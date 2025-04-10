import base64
import importlib.util
import logging
import os
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime

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
            decoded_string = decoded_bytes.decode("utf-8").strip()
            ssh_key_file.write(decoded_string)
            ssh_key_file_path = ssh_key_file.name

        logging.info(
            f"Attempting to connect to PostgreSQL at {db_host}:{db_port} through bastion host {ssh_host}"
        )

        # Create SSH tunnel with automatic local port allocation
        tunnel = SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_username,
            ssh_pkey=ssh_key_file_path,
            remote_bind_address=(db_host, db_port),
            local_bind_address=("localhost", 0),  # Dynamically allocate a local port
        )

        # Start the tunnel
        tunnel.start()

        # Test if the tunnel is active
        if not tunnel.is_active:
            logging.error("SSH tunnel failed to start")
            raise Exception("Failed to establish SSH tunnel connection")

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
        local_port = tunnel.local_bind_port

        logging.info(f"SSH tunnel established with local port: {local_port}")
        jdbc_url = f"jdbc:postgresql://localhost:{local_port}/{db_name}"
        logging.info(f"JDBC URL: {jdbc_url}")

        # Test connection before writing
        try:
            test_df = (
                session.read.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", "(SELECT 1) AS test")
                .option("user", db_user)
                .option("password", db_pw)
                .option("driver", "org.postgresql.Driver")
                .load()
            )

            test_count = test_df.count()
            logging.info(f"Connection test successful: {test_count} rows")
        except Exception as e:
            logging.error(f"Connection test failed: {str(e)}")
            logging.error(f"Exception type: {type(e).__name__}")
            raise

        # JDBC connection properties using the dynamically allocated port
        jdbc_url = f"jdbc:postgresql://localhost:{local_port}/{db_name}"
        properties = {
            "user": db_user,
            "password": db_pw,
            "driver": "org.postgresql.Driver",
            "url": jdbc_url,
            "dbtable": f"{db_schema}.place",
        }

        # Write the DataFrames to PostgreSQL using JDBC
        logging.info("Writing place data to PostgreSQL via JDBC")
        place_df.write.format("jdbc").options(**properties).mode("overwrite").save()

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
    finally:
        # Clean up resources
        if tunnel is not None and tunnel.is_active:
            tunnel.stop()
        if ssh_key_file_path and os.path.exists(ssh_key_file_path):
            os.remove(ssh_key_file_path)

    return load_log_df
