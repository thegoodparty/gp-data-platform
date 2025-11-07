from datetime import datetime

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RESPONSE_DATA_SCHEMA = StructType(
    [
        StructField("status", StringType(), True),
        StructField("run_id", StringType(), True),
        StructField(
            "s3_output",
            StructType(
                [
                    StructField("bucket", StringType(), True),
                    StructField("prefix", StringType(), True),
                    StructField(
                        "files",
                        StructType(
                            [
                                StructField("parquet", StringType(), True),
                                StructField("tsv", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField("task_arn", StringType(), True),
        StructField("estimated_completion", TimestampType(), True),
        StructField(
            "config",
            StructType(
                [
                    StructField("hubspot_table", StringType(), True),
                    StructField("ddhq_table", StringType(), True),
                    StructField("embedding_batch_size", IntegerType(), True),
                    StructField("embedding_max_workers", IntegerType(), True),
                    StructField("matching_batch_size", IntegerType(), True),
                    StructField("matching_max_workers", IntegerType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "polling",
            StructType(
                [
                    StructField("method", StringType(), True),
                    StructField("interval", StringType(), True),
                    StructField("timeout", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)


def _create_election_match_request(
    gp_ai_url: str,
    gp_ai_api_key: str,
    schema: str,
    candidacy_table_name: str,
    election_results_table_name: str,
) -> dict:
    """
    Creates and executes an API request to start the election match process.

    Args:
        gp_ai_url: The base URL for the GP AI API
        gp_ai_api_key: The API key for authentication
        schema: The database schema name
        candidacy_table_name: The name of the candidacy table
        election_results_table_name: The name of the election results table

    Returns:
        The JSON response data from the API:

    sample response:
        {
            "status": "STARTED",
            "run_id": "20251106_181323",
            "s3_output": {
                "bucket": "ddhq-matcher-output-prod",
                "prefix": "output/20251106_181323",
                "files": {
                "parquet": "s3://ddhq-matcher-output-prod/output/20251106_181323/matches.parquet",
                "tsv": "s3://ddhq-matcher-output-prod/output/20251106_181323/matches.tsv"
                }
            },
            "task_arn": "arn:aws:ecs:us-west-2:333022194791:task/ddhq-matcher-prod/90737e7487ac43bd810d3bd7e77bc462",
            "estimated_completion": "2025-11-06T18:43:24.908326Z",
            "config": {
                "hubspot_table": "dbt.m_general__candidacy",
                "ddhq_table": "dbt.stg_airbyte_source__ddhq_gdrive_election_results",
                "embedding_batch_size": 100,
                "embedding_max_workers": 80,
                "matching_batch_size": 1000,
                "matching_max_workers": 2000
            },
            "polling": {
                "method": "S3 HEAD request",
                "interval": "20 minutes",
                "timeout": "2 days"
            }
        }
    }

    """
    url = f"{gp_ai_url}/match/hubspot-ddhq"
    headers = {
        "x-api-key": gp_ai_api_key,
        "Content-Type": "application/json",
    }
    payload = {
        "hubspot_table": f"{schema}.{candidacy_table_name}",
        "ddhq_table": f"{schema}.{election_results_table_name}",
        "embedding_batch_size": 100,
        "embedding_max_workers": 80,
        "matching_batch_size": 1000,
        "matching_max_workers": 2000,
    }

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_data = response.json()
    response_data["estimated_completion"] = datetime.strptime(
        response_data["estimated_completion"], "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    return response_data


def model(dbt, session: SparkSession) -> DataFrame:
    """
    This model starts the election match process for GP AI. It does not write the output of the AI job back to the data warehouse.
    """
    # configure the data model
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="run_id",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=True,
        tags=["intermediate", "gp_ai", "election_match"],
    )

    # get dbt configs
    # dbt_cloud_env_type = dbt.config.get("dbt_cloud_env_type")  # may don't need this, since will write to dev anyways
    # maybe just use dbt.schema
    gp_ai_api_key = dbt.config.get("gp_ai_api_key")
    gp_ai_url = dbt.config.get("gp_ai_url")

    # make the following references to maintain lineage
    candidacy_table_name = "m_general__candidacy"
    election_results_table_name = "stg_airbyte_source__ddhq_gdrive_election_results"
    candidacy_table = dbt.ref(
        "m_general__candidacy"
    )  # dbt ref only supports string literals
    election_results_table = dbt.ref(
        "stg_airbyte_source__ddhq_gdrive_election_results"
    )  # dbt ref only supports string literals

    # if incremental, check if the most recent run is done after latest updates to candidacy and election_results table
    if dbt.is_incremental:
        # get the max updated_at from source tables
        max_updated_at_candidacy = candidacy_table.agg({"updated_at": "max"}).collect()[
            0
        ][0]
        max_updated_at_election_results = election_results_table.agg(
            {"updated_at": "max"}
        ).collect()[0][0]
        max_updated_source = max(
            max_updated_at_candidacy, max_updated_at_election_results
        )

        # get the max run_id from this table
        this_table: DataFrame = session.table(f"{dbt.this}")
        max_run_id = this_table.agg({"run_id": "max"}).collect()[0][0]
        max_updated_target = (
            datetime.strptime(max_run_id, "%Y%m%d_%H%M%S") if max_run_id else None
        )

        if max_updated_target is None or max_updated_source > max_updated_target:
            # start the election match process
            response_data = _create_election_match_request(
                gp_ai_url=gp_ai_url,
                gp_ai_api_key=gp_ai_api_key,
                schema=dbt.this.schema,
                candidacy_table_name=candidacy_table_name,
                election_results_table_name=election_results_table_name,
            )
            response_data_df = session.createDataFrame(
                [response_data], RESPONSE_DATA_SCHEMA
            )
            return response_data_df
        else:
            # no need to start the election match process
            return session.createDataFrame([], RESPONSE_DATA_SCHEMA)

    # make API request to start election match
    response_data = _create_election_match_request(
        gp_ai_url=gp_ai_url,
        gp_ai_api_key=gp_ai_api_key,
        schema=dbt.this.schema,
        candidacy_table_name=candidacy_table_name,
        election_results_table_name=election_results_table_name,
    )

    response_data_df = session.createDataFrame([response_data], RESPONSE_DATA_SCHEMA)
    return response_data_df
