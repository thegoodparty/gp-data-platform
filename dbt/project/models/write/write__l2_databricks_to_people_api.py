# import logging
# from datetime import datetime
# from typing import Any, Dict, List, Tuple

# import psycopg2
from pyspark.sql import DataFrame, SparkSession

# from pyspark.sql.functions import col

# from pyspark.sql.types import (
#     IntegerType,
#     StringType,
#     StructField,
#     StructType,
#     TimestampType,
# )


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
    # staging_schema = dbt.config.get("staging_schema")
    # db_host = dbt.config.get("people_db_host")
    # db_port = int(dbt.config.get("people_db_port"))
    # db_name = dbt.config.get("people_db_name")
    # db_schema = dbt.config.get("people_db_schema")
    # db_user = dbt.config.get("people_db_user")
    # db_pw = dbt.config.get("people_db_pw")
    # dbt_env_name = dbt.config.get("dbt_environment")

    # set databricks temporary volume path based on dbt cloud environment name
    # TODO: use volume path based on dbt cloud account. current env vars listed in docs are not available
    # see https://docs.getdbt.com/docs/build/environment-variables#special-environment-variables
    # if dbt_env_name == "dev":
    #     vol_prefix = "dbt_hugh"
    # elif dbt_env_name == "prod":
    #     vol_prefix = "dbt"
    # else:
    #     raise ValueError(
    #         f"Invalid `vol_prefix` handling of dbt environment name: {dbt_env_name}"
    #     )
    # databricks_volume_directory = (
    #     f"/Volumes/goodparty_data_catalog/{vol_prefix}/object_storage/l2_temp"
    # )

    # # get latest files loaded to databricks, filtering for uniform files
    # loaded_to_databricks: DataFrame = dbt.ref("load__l2_s3_to_databricks").filter(
    #     col("source_file_type") == "uniform"
    # )
