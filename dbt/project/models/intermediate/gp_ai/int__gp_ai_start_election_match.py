from pyspark.sql import DataFrame, SparkSession


def model(dbt, session: SparkSession) -> DataFrame:
    """
    This model starts the election match process for GP AI.
    """
    # configure the data model
    dbt.config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=True,
        tags=["intermediate", "gp_ai", "election_match"],
    )

    # get the data to start the election match process
    return dbt.ref("int__gp_ai_start_election_match")
