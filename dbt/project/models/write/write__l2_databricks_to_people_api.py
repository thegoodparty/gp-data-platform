import logging

# from datetime import datetime
from typing import Any, List, Tuple

import psycopg2
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _execute_sql_query(
    query: str,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    return_results: bool = False,
) -> List[Tuple[Any, ...]]:
    """
    Execute a SQL query and return the results. Not that the results should be None if no results are returned.
    """
    try:
        conn = psycopg2.connect(
            dbname=database, user=user, password=password, host=host, port=port
        )
        cursor = conn.cursor()
        cursor.execute(query)
        if return_results:
            results = cursor.fetchall() if cursor.rowcount > 0 else []
        else:
            results = []
        conn.commit()
    except Exception as e:
        logging.error(f"Error executing query: {query}")
        logging.error(f"Error: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

    return results


def model(dbt, session: SparkSession) -> DataFrame:
    """
    This model loads data from Databricks to the people api database.
    It is used to load the data from the Databricks tables to the people api database.
    """
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="append",
        unique_key="id",
        on_schema_change="fail",
        tags=["l2", "databricks", "people_api", "load"],
    )

    # get dbt configs
    staging_schema = dbt.config.get("staging_schema")
    db_host = dbt.config.get("people_db_host")
    db_port = int(dbt.config.get("people_db_port"))
    db_name = dbt.config.get("people_db_name")
    # db_schema = dbt.config.get("people_db_schema")
    db_user = dbt.config.get("people_db_user")
    db_pw = dbt.config.get("people_db_pw")
    dbt_env_name = dbt.config.get("dbt_environment")

    l2_nationwide_uniform: DataFrame = dbt.ref("int__l2_nationwide_uniform")

    # downsample for non-prod environment with three least populous states
    # TODO: downsample based on on dbt cloud account. current env vars listed in docs are not available
    # see https://docs.getdbt.com/docs/build/environment-variables#special-environment-variables
    if dbt_env_name != "prod":
        l2_nationwide_uniform = l2_nationwide_uniform.filter(
            col("state_postal_code").isin(["WY", "ND", "VT"])
        )

    # handle incremental loading
    # TODO: compare against the max loaded_at of "this" table
    if dbt.is_incremental:
        max_loaded_at = l2_nationwide_uniform.agg(max("loaded_at")).collect()[0][0]
        l2_nationwide_uniform = l2_nationwide_uniform.filter(
            col("loaded_at") > max_loaded_at
        )

    # count (forces cache and preceding filters) the dataframe and enforce filter before for-loop filtering
    l2_nationwide_uniform.count()

    # initialize list to capture metadata about data loads
    # load_details: List[Dict[str, Any]] = []
    # TODO: add metadata about the load to the load_details list

    # Create a staging schema if it doesn't exist
    _execute_sql_query(
        f"CREATE SCHEMA IF NOT EXISTS {staging_schema};",
        db_host,
        db_port,
        db_user,
        db_pw,
        db_name,
    )

    # TODO:
    # * load to the staging schema
    # * copy over to the 'public' schema, may require some casting
    simple_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("loaded_at", TimestampType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    empty_df = l2_nationwide_uniform.sparkSession.createDataFrame([], simple_schema)
    return empty_df
