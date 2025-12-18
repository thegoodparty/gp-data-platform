from datetime import datetime
from typing import Any, Dict, List, Literal
from uuid import uuid4

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lower, row_number, when
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window


def _filter_latest_loaded_files(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "source_file_type",
        when(
            lower(col("source_file_name")).like("%_haystaqdnaflags_%.tab"),
            "haystaq_dna_flags",
        )
        .when(
            lower(col("source_file_name")).like("%_haystaqdnascores_%.tab"),
            "haystaq_dna_scores",
        )
        .otherwise(None),
    )

    window_spec = Window.partitionBy("source_file_type").orderBy(
        col("loaded_at").desc()
    )
    return (
        df.withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )


def _extract_table_name(source_file_type: str, state_id: str) -> str:
    return f"l2_s3_{state_id.lower()}_{source_file_type}"


def model(dbt, session: SparkSession) -> DataFrame:
    """
    This model loads Haystaq data from S3 to Databricks.
    """
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="append",
        unique_key="id",
        on_schema_change="fail",
        tags=["l2", "haystaq", "s3", "databricks", "load"],
    )

    dbt_env_name = dbt.config.get("dbt_environment")
    s3_bucket = dbt.config.get("l2_s3_bucket")
    databricks_schema_override = dbt.config.get("l2_haystaq_databricks_schema")

    if databricks_schema_override:
        databricks_schema = databricks_schema_override
    else:
        if dbt_env_name == "dev":
            databricks_schema = "dbt_hugh_source"
        elif dbt_env_name == "prod":
            databricks_schema = "dbt_source"
        else:
            raise ValueError(f"Invalid `dbt_env_name`: {dbt_env_name}")

    s3_files_loaded: DataFrame = dbt.ref("load__l2_haystaq_sftp_to_s3")
    state_list = [
        row.state_id for row in s3_files_loaded.select("state_id").distinct().collect()
    ]

    load_id = str(uuid4())
    load_details: List[Dict[str, Any]] = []

    for state_id in state_list:
        state_s3_files = s3_files_loaded.filter(col("state_id") == state_id)
        latest_s3_files = _filter_latest_loaded_files(state_s3_files)
        latest_s3_files = latest_s3_files.filter(col("source_file_type").isNotNull())

        # incremental handling: only load if new or newer
        if dbt.is_incremental:
            this_table = session.table(f"{dbt.this}")
            this_table_state_files = this_table.filter(col("state_id") == state_id)
            this_table_latest_files = _filter_latest_loaded_files(
                this_table_state_files
            )
            this_table_latest_files_names = [
                file.source_file_name
                for file in this_table_latest_files.toLocalIterator()
            ]

            files_to_load_list: List[
                Dict[
                    Literal["source_file_name", "source_file_type", "s3_state_prefix"],
                    str,
                ]
            ] = []

            for s3_file in latest_s3_files.toLocalIterator():
                if s3_file.source_file_name in this_table_latest_files_names:
                    prior = (
                        this_table_latest_files.filter(
                            col("source_file_name") == s3_file.source_file_name
                        )
                        .first()
                        .loaded_at
                    )
                    if s3_file.loaded_at > prior:
                        files_to_load_list.append(
                            {
                                "source_file_name": s3_file.source_file_name,
                                "source_file_type": s3_file.source_file_type,
                                "s3_state_prefix": s3_file.s3_state_prefix,
                            }
                        )
                else:
                    files_to_load_list.append(
                        {
                            "source_file_name": s3_file.source_file_name,
                            "source_file_type": s3_file.source_file_type,
                            "s3_state_prefix": s3_file.s3_state_prefix,
                        }
                    )

            files_to_load: DataFrame = session.createDataFrame(
                data=files_to_load_list,
                schema=StructType(
                    [
                        StructField("source_file_name", StringType(), True),
                        StructField("source_file_type", StringType(), True),
                        StructField("s3_state_prefix", StringType(), True),
                    ]
                ),
            )
        else:
            files_to_load = latest_s3_files.select(
                "source_file_name", "source_file_type", "s3_state_prefix"
            )

        for file in files_to_load.toLocalIterator():
            source_file_name = file.source_file_name
            source_file_type = file.source_file_type
            table_name = _extract_table_name(source_file_type, state_id)

            s3_path = f"s3a://{s3_bucket}/{file.s3_state_prefix}{source_file_name}"
            data_df = session.read.options(delimiter="\t").csv(
                path=s3_path,
                header=True,
                inferSchema=True,
            )
            data_df = data_df.withColumn("loaded_at", current_timestamp())

            table_path = f"goodparty_data_catalog.{databricks_schema}.{table_name}"
            data_df.write.mode("overwrite").option("overwriteSchema", "true").option(
                "clusterByAuto", "true"
            ).format("delta").saveAsTable(table_path)

            load_details.append(
                {
                    "id": str(uuid4()),
                    "load_id": load_id,
                    "loaded_at": datetime.now(),
                    "state_id": state_id,
                    "source_s3_path": s3_path,
                    "source_file_name": source_file_name,
                    "source_file_type": source_file_type,
                    "table_name": table_name,
                    "table_path": table_path,
                }
            )

    # normalize loaded_at to timestamp via schema
    load_details_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("load_id", StringType(), True),
            StructField("loaded_at", TimestampType(), True),
            StructField("state_id", StringType(), True),
            StructField("source_s3_path", StringType(), True),
            StructField("source_file_name", StringType(), True),
            StructField("source_file_type", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("table_path", StringType(), True),
        ]
    )
    return session.createDataFrame(load_details, load_details_schema)
