from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, collect_list, concat, length, lit, size, when
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

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
    if dbt.is_incremental:
        existing_table = session.table(f"{dbt.this}")
        max_loaded_at = existing_table.agg({"loaded_at": "max"}).collect()[0][0]
        l2_uniform_data = l2_uniform_data.filter(col("loaded_at") > max_loaded_at)

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
        if district_type != "Country"
    ]

    for district_type in district_type_from_columns:
        # Get data for this district type from both residence and mailing addresses
        district_df = (
            l2_uniform_data.select(
                col("state_postal_code"),
                col("Residence_Addresses_Zip").alias("zip_code"),
                col(district_type).alias("district_name"),
            )
            .filter(col("Residence_Addresses_Zip").isNotNull())
            .filter(col("district_name").isNotNull())
            .distinct()
            .withColumn("district_type", lit(district_type))
        )

        # if zipcode is 4 characters, pad with a 0
        district_df = district_df.withColumn(
            "zip_code",
            when(
                length(col("zip_code")) == 4, concat(lit("0"), col("zip_code"))
            ).otherwise(col("zip_code")),
        )

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

    return final_result
