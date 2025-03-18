import logging

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

position_election_frequency_schema = StructType(
    [
        StructField("databaseId", IntegerType(), True),
        StructField("frequency", ArrayType(IntegerType()), True),
        StructField("id", StringType(), True),
        StructField("referenceYear", IntegerType(), True),
        StructField("seats", ArrayType(IntegerType()), True),
        StructField("valid_from", TimestampType(), True),
        StructField("valid_to", TimestampType(), True),
    ]
)


def model(dbt, session) -> DataFrame:
    """
    `dbt` Python model to retrieve and process position election frequency data from the CivicEngine API.
    See https://developers.civicengine.com/docs/api/graphql/reference/objects/position-election-frequency
    """
    dbt.config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        on_schema_change="fail",
        tags=["ballotready", "position_election_frequency", "api", "pandas_udf"],
    )

    # Get API token from environment variables
    ce_api_token = dbt.config.get("ce_api_token")
    if not ce_api_token:
        raise ValueError("Missing required config parameter: ce_api_token")

    # Get position election frequency data
    position: DataFrame = dbt.ref("stg_airbyte_source__ballotready_api_position")

    # Filter if incremental by updated_at
    if dbt.is_incremental:
        # Get the max updated_at from the existing table
        existing_table = session.table(f"{dbt.this}")
        max_updated_at_row = existing_table.agg(
            {"position_updated_at": "max"}
        ).collect()[0]
        max_updated_at = max_updated_at_row[0] if max_updated_at_row else None

        # For development/testing purposes (commented out by default)
        # max_updated_at = "2023-03-15 00:00:00"

        if max_updated_at:
            position = position.filter(position.updated_at > max_updated_at)
            logging.info(f"INFO: Filtered to positions updated since {max_updated_at}")
        else:
            logging.info("INFO: No max updated_at found. Processing all positions.")

    # deduplicate and only get the position ids
    position_election_frequency = (
        position.select("database_id")
        .dropDuplicates(["database_id"])
        .withColumnRenamed("database_id", "position_database_id")
    )

    # Validate source data
    if position_election_frequency.count() == 0:
        logging.warning("No positions found in source table")
        empty_df: DataFrame = session.createDataFrame(
            [],
            StructType(
                [
                    StructField("position_database_id", IntegerType(), True),
                    StructField("database_id", IntegerType(), True),
                    StructField("frequency", ArrayType(IntegerType()), True),
                    StructField("id", StringType(), True),
                    StructField("reference_year", IntegerType(), True),
                    StructField("seats", ArrayType(IntegerType()), True),
                    StructField("valid_from", TimestampType(), True),
                    StructField("valid_to", TimestampType(), True),
                    StructField("position_created_at", TimestampType(), True),
                    StructField("position_updated_at", TimestampType(), True),
                ]
            ),
        )
        return empty_df
