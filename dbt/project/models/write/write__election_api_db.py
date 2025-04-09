import base64
import importlib.util
import logging
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime

from pyspark.sql import DataFrame

# Check if sshtunnel is installed, install if not
# TODO: replace with paramiko if sshtunnel is not working
# if importlib.util.find_spec("sshtunnel") is None:
#     subprocess.check_call([sys.executable, "-m", "pip", "install", "sshtunnel==0.4.0"])
if importlib.util.find_spec("paramiko") is None:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "paramiko"])
from paramiko import SSHClient

# from sshtunnel import SSHTunnelForwarder


# def _create_ssh_tunnel(
#     ssh_host: str,
#     ssh_port: int,
#     ssh_username: str,
#     ssh_key_file_path: str,
#     remote_host: str,
#     remote_port: int,
#     local_port: int,
# ) -> SSHTunnelForwarder:
#     logging.info(
#         f"Creating SSH tunnel with {ssh_host}:{ssh_port} to {remote_host}:{remote_port} on port {local_port}"
#     )
#     tunnel = SSHTunnelForwarder(
#         ssh_address_or_host=(ssh_host, ssh_port),
#         ssh_username=ssh_username,
#         ssh_pkey=ssh_key_file_path,
#         remote_bind_address=(remote_host, remote_port),
#         # local_bind_address=("localhost", local_port),
#     )
#     tunnel.start()
#     return tunnel


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
        # packages = ["sshtunnel == 0.4.0"]
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

    try:

        # store the ssh key in a temp file and delete after establishing the tunnel
        with tempfile.NamedTemporaryFile(delete=True, suffix=".pem") as ssh_key_file:
            decoded_bytes = base64.b64decode(ssh_pk)
            decoded_string = decoded_bytes.decode("utf-8")
            ssh_key_file.write(decoded_string.encode())
            ssh_key_file_path = ssh_key_file.name

            # Use the key file for SSH connection
            client = SSHClient()
            client.load_system_host_keys()
            client.connect(
                hostname=ssh_host,
                port=ssh_port,
                username=ssh_username,
                key_filename=ssh_key_file_path,
            )

        #     # create the ssh tunnel
        #     tunnel = _create_ssh_tunnel(
        #         ssh_host=ssh_host,
        #         ssh_port=ssh_port,
        #         ssh_username=ssh_username,
        #         ssh_key_file_path=ssh_key_file_path,
        #         remote_host=db_host,
        #         remote_port=db_port,
        #         local_port=5432,
        #     )

        # get the data to write
        place_df: DataFrame = dbt.ref("m_election_api__place")
        race_df: DataFrame = dbt.ref("m_election_api__race")

        # TODO: add candidate_df: DataFrame = dbt.ref("m_election_api__candidate")

        # write the data to the database
        # TODO: if loading is slow and/or tables grow, replace overwrite with incremental
        place_df.write.format("jdbc").mode("overwrite").option(
            "url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
        ).option("dbtable", f"{db_schema}.place").option("user", db_user).option(
            "password", db_pw
        ).save()

        race_df.write.format("jdbc").mode("overwrite").option(
            "url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
        ).option("dbtable", f"{db_schema}.race").option("user", db_user).option(
            "password", db_pw
        ).save()

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
        # if "tunnel" in locals():
        #     tunnel.stop()
        client.close()

    return load_log_df
