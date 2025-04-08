import logging
import uuid
from datetime import datetime

from pyspark.sql import DataFrame
from sshtunnel import SSHTunnelForwarder


def _create_ssh_tunnel(
    ssh_host: str,
    ssh_port: int,
    ssh_username: str,
    ssh_password: str,
    remote_host: str,
    remote_port: int,
    local_port: int,
) -> SSHTunnelForwarder:
    tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_username,
        ssh_password=ssh_password,
        remote_bind_address=(remote_host, remote_port),
        local_bind_address=("localhost", local_port),
    )
    tunnel.start()
    return tunnel


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
    )

    place_table_name = "m_election_api__place"
    race_table_name = "m_election_api__race"

    # get db and ssh tunnel config
    db_host = dbt.config.get("election_api_db_host")
    db_port = dbt.config.get("election_api_db_port")
    db_user = dbt.config.get("election_api_db_user")
    db_password = dbt.config.get("election_api_db_password")
    db_name = dbt.config.get("election_api_db_name")
    db_schema = dbt.config.get("election_api_db_schema")
    ssh_host = dbt.config.get("election_api_db_ssh_host")
    ssh_port = dbt.config.get("election_api_db_ssh_port")
    ssh_username = dbt.config.get("election_api_db_ssh_username")
    ssh_password = dbt.config.get("election_api_db_ssh_password")

    try:
        # create the ssh tunnel
        tunnel = _create_ssh_tunnel(
            ssh_host, ssh_port, ssh_username, ssh_password, db_host, db_port, 5432
        )

        # get the data to write
        place_df: DataFrame = dbt.ref(place_table_name)
        race_df: DataFrame = dbt.ref(race_table_name)

        # TODO: add candidate_df: DataFrame = dbt.ref("m_election_api__candidate")

        # write the data to the database
        place_df.write.format("jdbc").mode("append").option(
            "url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
        ).option("dbtable", f"{db_schema}.{place_table_name}").option(
            "user", db_user
        ).option(
            "password", db_password
        ).save()
        race_df.write.format("jdbc").mode("append").option(
            "url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
        ).option("dbtable", f"{db_schema}.{race_table_name}").option(
            "user", db_user
        ).option(
            "password", db_password
        ).save()

        # TODO: log the loading information including the number of rows loaded
        # and the time it took to load the data
        # Create a sample DataFrame (replace with your actual DataFrame)

        columns = ["id", "table_name", "number_of_rows", "loaded_at"]
        data = [
            (
                str(uuid.uuid4()),
                place_table_name,
                place_df.count(),
                datetime.now(),
            ),
            (
                str(uuid.uuid4()),
                race_table_name,
                race_df.count(),
                datetime.now(),
            ),
        ]
        df = session.createDataFrame(data, columns)
    except Exception as e:
        logging.error(f"Error: {e}")
        raise e
    finally:
        tunnel.stop()

    return df
