"""
This model creates the district voter table in the mart layer using the voter table.
It transforms voter district data into a normalized district voter relationship table.
This python data model is significantly faster than the sql data model (stored as .sql_backup).
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, StructField, StructType

# Define the schema for the district voter table
THIS_SCHEMA = StructType(
    [
        StructField(name="voter_id", dataType=StringType(), nullable=False),
        StructField("district_id", StringType(), False),
        StructField("type", StringType(), False),
        StructField("name", StringType(), False),
        StructField("state", StringType(), False),
        StructField("created_at", StringType(), False),
        StructField("updated_at", StringType(), False),
    ]
)

# District types with no per-voter column to unpivot. DistrictStats derives statewide
# associations straight from Voter.state, so emitting them here as well would add a row
# per voter per state and double count every statewide district.
NON_VOTER_DISTRICT_TYPES = {"state", "country"}


def _district_columns(voter_df: DataFrame, district_df: DataFrame) -> list[str]:
    """
    The district columns to unpivot, derived from the district table.

    This list was hardcoded, a third hand-maintained copy of the L2 district types
    alongside the macro and the voter mart. It had drifted in both directions: seven
    office-bearing types had District rows with no column here, so they could never
    receive a DistrictVoter row, and nineteen columns here matched no district at all.
    The join is an exact match that returns nothing rather than erroring, so the drift
    only ever surfaced as districts silently reporting zero voters.

    Intersecting the two lists removes the copy rather than correcting it, so the
    types this model unpivots cannot fall behind the districts it joins to again.
    """
    district_types = {
        row["type"].lower()
        for row in district_df.select("type").distinct().collect()
        if row["type"] is not None
    }
    return [
        column
        for column in voter_df.columns
        if column.lower() in district_types and column.lower() not in NON_VOTER_DISTRICT_TYPES
    ]


def model(dbt, session: SparkSession) -> DataFrame:
    """
    This model creates the district voter table in the mart layer using the voter table.
    It transforms voter district data into a normalized district voter relationship table.
    """
    # Configure the data model
    dbt.config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["voter_id", "type"],
        on_schema_change="append_new_columns",
        liquid_clustered_by=["voter_id", "type", "updated_at"],
        tags=["mart", "people_api", "district_voter"],
    )

    # Get the voter data
    voter_df: DataFrame = dbt.ref("m_people_api__voter")

    # Apply incremental logic
    if dbt.is_incremental:
        this_df: DataFrame = session.table(f"{dbt.this}")
        max_updated_at = this_df.agg({"updated_at": "max"}).collect()[0][0]
        voter_df = voter_df.filter(col("updated_at") > max_updated_at)

    # check if count is 0, exit early
    voter_df_count = voter_df.count()
    if voter_df_count == 0:
        return session.createDataFrame(data=[], schema=THIS_SCHEMA)

    # Get the district table for joining
    district__mart_df: DataFrame = dbt.ref("m_people_api__district")

    district_columns = _district_columns(voter_df, district__mart_df)

    # Create district voter records by unpivoting the district columns
    district_voter_records = []

    for column in district_columns:
        # Create a DataFrame for this district column
        district_df = (
            voter_df.select(
                col("id"),
                col("created_at"),
                col("updated_at"),
                col("State"),
                col(column).alias("district_value"),
            )
            .filter(col(column).isNotNull())
            .withColumn("type", lit(column))
            .withColumn("name", col("district_value").cast(StringType()))
            .withColumn("state", col("State"))
            .select(
                col("id").alias("voter_id"),
                col("type"),
                col("name"),
                col("state"),
                col("created_at"),
                col("updated_at"),
            )
        )
        district_voter_records.append(district_df)

    # Union all district voter records
    if district_voter_records:
        districts_from_voters = district_voter_records[0]
        for df in district_voter_records[1:]:
            districts_from_voters = districts_from_voters.union(df)
    else:
        # Create empty DataFrame with proper schema if no records
        schema = THIS_SCHEMA
        districts_from_voters = session.createDataFrame([], schema)

    # Join with district table to get district_id
    result_df = (
        districts_from_voters.join(
            district__mart_df.select(col("id").alias("district_id"), col("state"), col("type"), col("name")),
            on=["state", "type", "name"],
            how="left",
        )
        .filter(col("district_id").isNotNull())
        .select(
            col("voter_id"),
            col("district_id"),
            col("type"),
            col("name"),
            col("state"),
            col("created_at"),
            col("updated_at"),
        )
    )

    return result_df
