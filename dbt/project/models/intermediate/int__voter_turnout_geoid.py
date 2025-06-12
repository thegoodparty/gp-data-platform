from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession


def model(dbt, session: SparkSession) -> DataFrame:
    dbt.config(
        submission_method="all_purpose_cluster",  # required for .cache()
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",  # required for .cache()
        materialized="incremental",
        incremental_strategy="merge",
        # unique_key="database_id",
        on_schema_change="fail",
        tags=["voter_turnout", "geoid", "l2"],
    )

    # load fips codes
    # fips_codes = dbt.ref("fips_codes")

    # load voter turnout
    # voter_turnout = dbt.ref("stg_sandbox_source__turnout_projections_placeholder")

    # join fips codes to voter turnout

    # TODO: add pseudocode and logic here
    return
