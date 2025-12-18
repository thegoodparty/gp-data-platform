"""
This model creates district statistics by aggregating voter demographic data per district.
It computes bucket distributions for age, homeowner status, education, presence of children,
and estimated income range for each district.

Output schema matches:
    - district_id: String (primary key)
    - computed_at: DateTime
    - total_constituents: Int
    - total_constituents_with_cell_phone: Int
    - buckets: Struct containing:
        - age: Array of Bucket structs
        - homeowner: Array of Bucket structs
        - education: Array of Bucket structs
        - presenceOfChildren: Array of Bucket structs
        - estimatedIncomeRange: Array of Bucket structs

    Where Bucket is a Struct with:
        - label: String
        - count: Long
        - percent: Double

See: https://github.com/thegoodparty/people-api/tree/develop/prisma/schema/DistrictStats.prisma
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Define the Bucket schema: { label, count, percent }
BUCKET_SCHEMA = StructType(
    [
        StructField("label", StringType(), False),
        StructField("count", LongType(), False),
        StructField("percent", DoubleType(), False),
    ]
)

# Define the Buckets schema containing all demographic categories
BUCKETS_SCHEMA = StructType(
    [
        StructField("age", ArrayType(BUCKET_SCHEMA), True),
        StructField("homeowner", ArrayType(BUCKET_SCHEMA), True),
        StructField("education", ArrayType(BUCKET_SCHEMA), True),
        StructField("presenceOfChildren", ArrayType(BUCKET_SCHEMA), True),
        StructField("estimatedIncomeRange", ArrayType(BUCKET_SCHEMA), True),
    ]
)

# Output schema for the model
OUTPUT_SCHEMA = StructType(
    [
        StructField("district_id", StringType(), False),
        StructField("computed_at", TimestampType(), False),
        StructField("total_constituents", LongType(), False),
        StructField("total_constituents_with_cell_phone", LongType(), False),
        StructField("buckets", BUCKETS_SCHEMA, False),
    ]
)


def _get_age_bucket_label(age_int_col):
    """Create a column expression that maps age to bucket labels."""
    return (
        F.when((F.col(age_int_col) >= 18) & (F.col(age_int_col) <= 24), F.lit("18-24"))
        .when((F.col(age_int_col) >= 25) & (F.col(age_int_col) <= 34), F.lit("25-34"))
        .when((F.col(age_int_col) >= 35) & (F.col(age_int_col) <= 44), F.lit("35-44"))
        .when((F.col(age_int_col) >= 45) & (F.col(age_int_col) <= 54), F.lit("45-54"))
        .when((F.col(age_int_col) >= 55) & (F.col(age_int_col) <= 64), F.lit("55-64"))
        .when((F.col(age_int_col) >= 65) & (F.col(age_int_col) <= 74), F.lit("65-74"))
        .when(F.col(age_int_col) >= 75, F.lit("75+"))
        .otherwise(F.lit("Unknown"))
    )


def _aggregate_buckets(
    df: DataFrame, district_col: str, category_col: str
) -> DataFrame:
    """
    Aggregate counts per category for each district.
    Returns DataFrame with district_id, label, count columns.
    """
    return (
        df.groupBy(district_col, category_col)
        .agg(F.count("*").alias("count"))
        .withColumnRenamed(category_col, "label")
        .withColumnRenamed(district_col, "district_id")
    )


def _buckets_to_array(df: DataFrame, bucket_name: str) -> DataFrame:
    """
    Convert bucket counts to array of structs per district.
    Input: DataFrame with district_id, label, count, total_constituents
    Output: DataFrame with district_id, {bucket_name} (array of bucket structs)
    """
    # Calculate percent and create bucket struct
    df_with_percent = df.withColumn(
        "percent", F.round((F.col("count") / F.col("total_constituents")) * 100, 2)
    ).withColumn("label", F.coalesce(F.col("label"), F.lit("Unknown")))

    # Create struct and collect as array per district
    return (
        df_with_percent.withColumn(
            "bucket_entry",
            F.struct(
                F.col("label").alias("label"),
                F.col("count").alias("count"),
                F.col("percent").alias("percent"),
            ),
        )
        .groupBy("district_id")
        .agg(F.sort_array(F.collect_list("bucket_entry"), asc=False).alias(bucket_name))
    )


def model(dbt, session: SparkSession) -> DataFrame:
    """
    Create district statistics by aggregating voter demographic data per district.
    Computes bucket distributions for various demographic categories using efficient
    Spark aggregations.
    """
    # Configure the data model
    dbt.config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="district_id",
        on_schema_change="fail",
        tags=["mart", "people_api", "district_stats"],
    )

    # Get source dataframes
    voter_df: DataFrame = dbt.ref("m_people_api__voter")
    districtvoter_df: DataFrame = dbt.ref("m_people_api__districtvoter")

    # Apply incremental logic - only process districts that have updated voters
    if dbt.is_incremental:
        this_df: DataFrame = session.table(f"{dbt.this}")
        max_computed_at = this_df.agg({"computed_at": "max"}).collect()[0][0]
        if max_computed_at:
            # Get districts with updated voters
            updated_voter_ids = (
                voter_df.filter(F.col("updated_at") > max_computed_at)
                .select("id")
                .distinct()
            )
            # Get distinct district IDs for updated voters
            updated_district_ids = (
                districtvoter_df.join(
                    updated_voter_ids,
                    districtvoter_df.voter_id == updated_voter_ids.id,
                    "inner",
                )
                .select("district_id")
                .distinct()
            )
            # Filter to only include updated districts
            districtvoter_df = districtvoter_df.join(
                updated_district_ids,
                on="district_id",
                how="inner",
            )

    # Join districtvoter with voter data to get demographics per district
    voters_with_districts = districtvoter_df.join(
        voter_df.select(
            F.col("id").alias("voter_id"),
            F.col("Age_Int"),
            F.col("Homeowner_Probability_Model"),
            F.col("Education_Of_Person"),
            F.col("Presence_Of_Children"),
            F.col("Estimated_Income_Amount"),
            F.col("VoterTelephones_CellPhoneFormatted"),
        ),
        on="voter_id",
        how="inner",
    )

    # Check if there's any data to process
    if voters_with_districts.limit(1).count() == 0:
        return session.createDataFrame([], OUTPUT_SCHEMA)

    # Add age bucket label column
    voters_with_buckets = voters_with_districts.withColumn(
        "age_bucket", _get_age_bucket_label("Age_Int")
    )

    # Compute total constituents per district
    district_totals = voters_with_buckets.groupBy("district_id").agg(
        F.count("*").alias("total_constituents"),
        F.sum(
            F.when(
                (F.col("VoterTelephones_CellPhoneFormatted").isNotNull())
                & (F.col("VoterTelephones_CellPhoneFormatted") != ""),
                1,
            ).otherwise(0)
        ).alias("total_constituents_with_cell_phone"),
    )

    # Compute age buckets
    age_counts = _aggregate_buckets(voters_with_buckets, "district_id", "age_bucket")
    age_counts_with_total = age_counts.join(
        district_totals.select("district_id", "total_constituents"),
        on="district_id",
        how="inner",
    )
    age_buckets_df = _buckets_to_array(age_counts_with_total, "age")

    # Compute homeowner buckets
    homeowner_counts = _aggregate_buckets(
        voters_with_buckets, "district_id", "Homeowner_Probability_Model"
    )
    homeowner_counts_with_total = homeowner_counts.join(
        district_totals.select("district_id", "total_constituents"),
        on="district_id",
        how="inner",
    )
    homeowner_buckets_df = _buckets_to_array(homeowner_counts_with_total, "homeowner")

    # Compute education buckets
    education_counts = _aggregate_buckets(
        voters_with_buckets, "district_id", "Education_Of_Person"
    )
    education_counts_with_total = education_counts.join(
        district_totals.select("district_id", "total_constituents"),
        on="district_id",
        how="inner",
    )
    education_buckets_df = _buckets_to_array(education_counts_with_total, "education")

    # Compute presence of children buckets
    children_counts = _aggregate_buckets(
        voters_with_buckets, "district_id", "Presence_Of_Children"
    )
    children_counts_with_total = children_counts.join(
        district_totals.select("district_id", "total_constituents"),
        on="district_id",
        how="inner",
    )
    children_buckets_df = _buckets_to_array(
        children_counts_with_total, "presenceOfChildren"
    )

    # Compute income buckets
    income_counts = _aggregate_buckets(
        voters_with_buckets, "district_id", "Estimated_Income_Amount"
    )
    income_counts_with_total = income_counts.join(
        district_totals.select("district_id", "total_constituents"),
        on="district_id",
        how="inner",
    )
    income_buckets_df = _buckets_to_array(
        income_counts_with_total, "estimatedIncomeRange"
    )

    # Join all bucket dataframes together
    result_df = (
        district_totals.join(age_buckets_df, on="district_id", how="left")
        .join(homeowner_buckets_df, on="district_id", how="left")
        .join(education_buckets_df, on="district_id", how="left")
        .join(children_buckets_df, on="district_id", how="left")
        .join(income_buckets_df, on="district_id", how="left")
    )

    # Create the final buckets struct
    final_df = (
        result_df.withColumn(
            "buckets",
            F.struct(
                F.col("age").alias("age"),
                F.col("homeowner").alias("homeowner"),
                F.col("education").alias("education"),
                F.col("presenceOfChildren").alias("presenceOfChildren"),
                F.col("estimatedIncomeRange").alias("estimatedIncomeRange"),
            ),
        )
        .withColumn("computed_at", F.current_timestamp())
        .select(
            "district_id",
            "computed_at",
            "total_constituents",
            "total_constituents_with_cell_phone",
            "buckets",
        )
    )

    return final_df
