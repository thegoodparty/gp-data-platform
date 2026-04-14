from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    array_distinct,
    array_union,
    col,
    collect_list,
    concat,
    count,
    length,
    lit,
    size,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import (
    when,
)
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

THIS_TABLE_SCHEMA = StructType(
    [
        StructField(name="zip_code", dataType=StringType(), nullable=False),
        StructField(name="state_postal_code", dataType=StringType(), nullable=False),
        StructField(name="district_type", dataType=StringType(), nullable=False),
        StructField(
            name="district_names", dataType=ArrayType(StringType()), nullable=False
        ),
        StructField(name="loaded_at", dataType=TimestampType(), nullable=False),
    ]
)


# Minimum percentage of voters in a zip code that must be assigned to a district
# for that district to be included. Filters out noise from tiny geographic overlaps.
# See: https://goodpartyorg.slack.com/archives/C090662DLRM/p1776185747062729
MIN_VOTER_PCT = 0.001  # 0.1%


def model(dbt, session: SparkSession) -> DataFrame:
    """
    This model reads in the L2 UNIFORM data and unions them together.
    This is done in pyspark since in SparkSQL, there are some column data type mismatches which cannot
    be uncovered from SQL logs.
    """
    # configure the data model
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["zip_code", "state_postal_code", "district_type"],
        on_schema_change="fail",
        auto_liquid_cluster=True,
        tags=["intermediate", "l2", "zip_code", "districts"],
    )

    l2_uniform_data: DataFrame = dbt.ref("int__l2_nationwide_uniform")

    # get the max loaded for this table and compare against latest changes to l2 uniform data
    existing_table = None
    if dbt.is_incremental:
        existing_table = session.table(f"{dbt.this}")
        max_loaded_at = existing_table.agg({"loaded_at": "max"}).collect()[0][0]

        # Handle case where table is empty (max_loaded_at is None)
        if max_loaded_at is not None:
            l2_uniform_data = l2_uniform_data.filter(col("loaded_at") > max_loaded_at)

        if l2_uniform_data.count() == 0:
            return session.createDataFrame(
                data=[],
                schema=THIS_TABLE_SCHEMA,
            )

    # Create a list of DataFrames for each district type
    district_dataframes = []
    district_type_from_columns = (
        dbt.ref("int__model_prediction_voter_turnout")
        .select(col("district_type"))
        .distinct()
        .collect()
    )
    district_type_from_columns = [
        district_type.district_type for district_type in district_type_from_columns
    ]

    # remove "Country" as a district since it doesn't match any of the other district types
    district_type_from_columns = [
        district_type
        for district_type in district_type_from_columns
        if district_type not in ["Country", "State"]
    ]

    for district_type in district_type_from_columns:
        # Get data for this district type from residence
        district_df = (
            l2_uniform_data.select(
                col("state_postal_code"),
                col("Residence_Addresses_Zip").alias("zip_code"),
                col(district_type).alias("district_name"),
            )
            .filter(col("Residence_Addresses_Zip").isNotNull())
            .filter(col("district_name").isNotNull())
        )

        # if zipcode is 4 characters, pad with a 0
        district_df = district_df.withColumn(
            "zip_code",
            when(
                length(col("zip_code")) == 4, concat(lit("0"), col("zip_code"))
            ).otherwise(col("zip_code")),
        )

        # Count voters per (zip, district_name) and filter out districts where
        # fewer than MIN_VOTER_PCT of the zip's voters are assigned there.
        district_df = district_df.groupBy(
            "state_postal_code", "zip_code", "district_name"
        ).agg(count("*").alias("voter_count"))

        zip_window = Window.partitionBy("zip_code", "state_postal_code")
        district_df = (
            district_df.withColumn(
                "total_voters", spark_sum("voter_count").over(zip_window)
            )
            .filter(col("voter_count") / col("total_voters") >= MIN_VOTER_PCT)
            .drop("voter_count", "total_voters")
        )

        district_df = district_df.withColumn("district_type", lit(district_type))

        district_dataframes.append(district_df)

    # Union all district DataFrames together
    if district_dataframes:
        zip_code_to_l2_district = district_dataframes[0]
        for df in district_dataframes[1:]:
            zip_code_to_l2_district = zip_code_to_l2_district.union(df)
    else:
        # If no district dataframes, create empty DataFrame with correct schema
        zip_code_to_l2_district = session.createDataFrame(
            data=[],
            schema=THIS_TABLE_SCHEMA,
        )

    # Group by zip_code, state_postal_code, district_type and collect district_names into arrays
    # drop rows which have `State` for district_type
    zip_code_to_l2_district = zip_code_to_l2_district.filter(
        col("district_type") != "State"
    )

    final_result = (
        zip_code_to_l2_district.groupBy(
            "zip_code",
            "state_postal_code",
            "district_type",
        )
        .agg(collect_list("district_name").alias("district_names"))
        .withColumn("loaded_at", lit(datetime.now()))
    )

    # drop rows which have no district names or the size of the array is 0
    final_result = final_result.filter(col("district_names").isNotNull())
    final_result = final_result.filter(size(col("district_names")) > 0)

    # On incremental runs, merge new district_names with existing ones so that
    # the upsert doesn't overwrite previously captured districts.
    if existing_table is not None:
        final_result = (
            final_result.alias("new")
            .join(
                existing_table.select(
                    "zip_code",
                    "state_postal_code",
                    "district_type",
                    col("district_names").alias("existing_district_names"),
                ),
                on=["zip_code", "state_postal_code", "district_type"],
                how="left",
            )
            .withColumn(
                "district_names",
                when(
                    col("existing_district_names").isNotNull(),
                    array_distinct(
                        array_union(
                            col("district_names"), col("existing_district_names")
                        )
                    ),
                ).otherwise(col("district_names")),
            )
            .drop("existing_district_names")
        )

    return final_result
