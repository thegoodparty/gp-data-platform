from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    col,
    concat,
    count,
    length,
    lit,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import (
    when,
)
from pyspark.sql.types import (
    LongType,
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
        StructField(name="district_name", dataType=StringType(), nullable=False),
        StructField(name="voters_in_zip_district", dataType=LongType(), nullable=False),
        StructField(name="voters_in_zip", dataType=LongType(), nullable=False),
        StructField(name="loaded_at", dataType=TimestampType(), nullable=False),
    ]
)


def model(dbt, session: SparkSession) -> DataFrame:
    """
    Maps each (zip_code, state, district_type, district_name) to L2 voter
    counts: voters in that intersection and voters in the zip overall.

    No threshold is applied — downstream consumers (election-api) decide
    the cutoff. Output is flat (one row per intersection); upstream callers
    that want an array of names should aggregate themselves.
    """
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=[
            "zip_code",
            "state_postal_code",
            "district_type",
            "district_name",
        ],
        on_schema_change="fail",
        auto_liquid_cluster=True,
        tags=["intermediate", "l2", "zip_code", "districts"],
    )

    l2_uniform_data: DataFrame = dbt.ref("int__l2_nationwide_uniform")

    # On incremental runs, identify zip codes with new voter records, then
    # recompute those zips against the full source population so voters_in_zip
    # is accurate (not skewed by batch size).
    if dbt.is_incremental:
        existing_table = session.table(f"{dbt.this}")
        max_loaded_at = existing_table.agg({"loaded_at": "max"}).collect()[0][0]

        if max_loaded_at is not None:
            new_records = l2_uniform_data.filter(col("loaded_at") > max_loaded_at)
        else:
            new_records = l2_uniform_data

        if new_records.count() == 0:
            return session.createDataFrame(
                data=[],
                schema=THIS_TABLE_SCHEMA,
            )

        affected_zips = (
            new_records.select("Residence_Addresses_Zip", "state_postal_code")
            .filter(col("Residence_Addresses_Zip").isNotNull())
            .distinct()
        )
        l2_uniform_data = l2_uniform_data.join(
            affected_zips,
            on=["Residence_Addresses_Zip", "state_postal_code"],
            how="inner",
        )

    district_type_from_columns = (
        dbt.ref("int__model_prediction_voter_turnout")
        .select(col("district_type"))
        .distinct()
        .collect()
    )
    district_type_from_columns = [
        district_type.district_type for district_type in district_type_from_columns
    ]

    # Country/State are too coarse to be useful for office picking
    district_type_from_columns = [
        district_type
        for district_type in district_type_from_columns
        if district_type not in ["Country", "State"]
    ]

    zip_window = Window.partitionBy("zip_code", "state_postal_code")

    district_dataframes = []
    for district_type in district_type_from_columns:
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

        district_df = (
            district_df.groupBy("state_postal_code", "zip_code", "district_name")
            .agg(count("*").alias("voters_in_zip_district"))
            .withColumn(
                "voters_in_zip",
                spark_sum("voters_in_zip_district").over(zip_window),
            )
            .withColumn("district_type", lit(district_type))
        )

        district_dataframes.append(district_df)

    if district_dataframes:
        zip_code_to_l2_district = district_dataframes[0]
        for df in district_dataframes[1:]:
            zip_code_to_l2_district = zip_code_to_l2_district.union(df)
    else:
        return session.createDataFrame(data=[], schema=THIS_TABLE_SCHEMA)

    final_result = zip_code_to_l2_district.select(
        col("zip_code"),
        col("state_postal_code"),
        col("district_type"),
        col("district_name"),
        col("voters_in_zip_district").cast(LongType()).alias("voters_in_zip_district"),
        col("voters_in_zip").cast(LongType()).alias("voters_in_zip"),
    ).withColumn("loaded_at", lit(datetime.now()))

    return final_result
