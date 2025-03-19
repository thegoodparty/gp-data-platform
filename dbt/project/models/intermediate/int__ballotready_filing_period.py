from pyspark.sql import DataFrame


def model(dbt, session) -> DataFrame:
    dbt.config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        on_schema_change="fail",
        tags=["ballotready", "filing_period", "api", "pandas_udf"],
    )

    # get API token from environment variables
    ce_api_token = dbt.config.get("ce_api_token")
    if not ce_api_token:
        raise ValueError("Missing required config parameter: ce_api_token")

    # get unique filing period ids from race
