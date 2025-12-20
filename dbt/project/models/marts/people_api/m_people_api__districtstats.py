"""
This model creates district statistics by aggregating voter demographic data per district.
It computes bucket distributions for age, homeowner status, education, presence of children,
and estimated income range for each district. It takes about 850 s for a full build

Output schema matches:
    - district_id: String (primary key)
    - updated_at: DateTime
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
        StructField("updated_at", TimestampType(), False),
        StructField("total_constituents", LongType(), False),
        StructField("total_constituents_with_cell_phone", LongType(), False),
        StructField("buckets", BUCKETS_SCHEMA, False),
    ]
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


def _get_age_bucket_label(age_int_col):
    """Create a column expression that maps age to bucket labels."""
    return (
        F.when((F.col(age_int_col) >= 18) & (F.col(age_int_col) <= 25), F.lit("18-25"))
        .when((F.col(age_int_col) >= 26) & (F.col(age_int_col) <= 35), F.lit("26-35"))
        .when((F.col(age_int_col) >= 36) & (F.col(age_int_col) <= 50), F.lit("36-50"))
        .when((F.col(age_int_col) >= 51), F.lit("51+"))
        .otherwise(F.lit("Unknown"))
    )


def _get_income_bucket_label(income_int_col):
    """Create a column expression that maps estimated income (integer) to bucket labels."""
    return (
        F.when(
            (F.col(income_int_col) >= 1000) & (F.col(income_int_col) < 15000),
            F.lit("1k–15k"),
        )
        .when(
            (F.col(income_int_col) >= 15000) & (F.col(income_int_col) < 25000),
            F.lit("15k–25k"),
        )
        .when(
            (F.col(income_int_col) >= 25000) & (F.col(income_int_col) < 35000),
            F.lit("25k–35k"),
        )
        .when(
            (F.col(income_int_col) >= 35000) & (F.col(income_int_col) < 50000),
            F.lit("35k–50k"),
        )
        .when(
            (F.col(income_int_col) >= 50000) & (F.col(income_int_col) < 75000),
            F.lit("50k–75k"),
        )
        .when(
            (F.col(income_int_col) >= 75000) & (F.col(income_int_col) < 100000),
            F.lit("75k–100k"),
        )
        .when(
            (F.col(income_int_col) >= 100000) & (F.col(income_int_col) < 125000),
            F.lit("100k–125k"),
        )
        .when(
            (F.col(income_int_col) >= 125000) & (F.col(income_int_col) < 150000),
            F.lit("125k–150k"),
        )
        .when(
            (F.col(income_int_col) >= 150000) & (F.col(income_int_col) < 175000),
            F.lit("150k–175k"),
        )
        .when(
            (F.col(income_int_col) >= 175000) & (F.col(income_int_col) < 200000),
            F.lit("175k–200k"),
        )
        .when(
            (F.col(income_int_col) >= 200000) & (F.col(income_int_col) < 250000),
            F.lit("200k–250k"),
        )
        .when(F.col(income_int_col) >= 250000, F.lit("250k+"))
        .otherwise(F.lit("Unknown"))
    )


def _process_age_buckets(
    voters_with_buckets: DataFrame, district_totals: DataFrame
) -> DataFrame:
    """Process age bucket aggregation."""
    age_counts = _aggregate_buckets(voters_with_buckets, "district_id", "age_bucket")
    age_counts_with_total = age_counts.join(
        district_totals.select("district_id", "total_constituents"),
        on="district_id",
        how="inner",
    )
    return _buckets_to_array(age_counts_with_total, "age")


def _process_homeowner_buckets(
    voters_with_buckets: DataFrame, district_totals: DataFrame
) -> DataFrame:
    """
    Process homeowner bucket aggregation with label mapping:
    "Home Owner" / "Probable Home Owner" -> "Yes", "Renter" -> "No", else -> "Unknown"
    """
    homeowner_counts = _aggregate_buckets(
        voters_with_buckets, "district_id", "Homeowner_Probability_Model"
    )
    # Map labels to simplified categories
    homeowner_counts = homeowner_counts.withColumn(
        "label",
        F.when(F.col("label").isin("Home Owner", "Probable Home Owner"), F.lit("Yes"))
        .when(F.col("label") == "Renter", F.lit("No"))
        .otherwise(F.lit("Unknown")),
    )
    # Re-aggregate to combine mapped labels
    homeowner_counts = homeowner_counts.groupBy("district_id", "label").agg(
        F.sum("count").alias("count")
    )
    homeowner_counts_with_total = homeowner_counts.join(
        district_totals.select("district_id", "total_constituents"),
        on="district_id",
        how="inner",
    )
    return _buckets_to_array(homeowner_counts_with_total, "homeowner")


def _process_education_buckets(
    voters_with_buckets: DataFrame, district_totals: DataFrame
) -> DataFrame:
    """
    Process education bucket aggregation with label mapping:
    - "Did Not Complete High School Likely" -> "None"
    - "Unknown" -> "Unknown"
    - "Attended Vocational/Technical School Likely" -> "Technical School"
    - "Completed Graduate School Likely" -> "Graduate Degree"
    - "Completed College Likely" -> "College Degree"
    - "Completed High School Likely" -> "High School Diploma"
    - "Attended But Did Not Complete College Likely" -> "Some College"
    - else -> "Unknown"
    """
    education_counts = _aggregate_buckets(
        voters_with_buckets, "district_id", "Education_Of_Person"
    )
    # Map labels to human-readable categories
    education_counts = education_counts.withColumn(
        "label",
        F.when(F.col("label") == "Did Not Complete High School Likely", F.lit("None"))
        .when(F.col("label") == "Unknown", F.lit("Unknown"))
        .when(
            F.col("label") == "Attended Vocational/Technical School Likely",
            F.lit("Technical School"),
        )
        .when(
            F.col("label") == "Completed Graduate School Likely",
            F.lit("Graduate Degree"),
        )
        .when(F.col("label") == "Completed College Likely", F.lit("College Degree"))
        .when(
            F.col("label") == "Completed High School Likely",
            F.lit("High School Diploma"),
        )
        .when(
            F.col("label") == "Attended But Did Not Complete College Likely",
            F.lit("Some College"),
        )
        .otherwise(F.lit("Unknown")),
    )
    # Re-aggregate to combine mapped labels
    education_counts = education_counts.groupBy("district_id", "label").agg(
        F.sum("count").alias("count")
    )
    education_counts_with_total = education_counts.join(
        district_totals.select("district_id", "total_constituents"),
        on="district_id",
        how="inner",
    )
    return _buckets_to_array(education_counts_with_total, "education")


def _process_presence_of_children_buckets(
    voters_with_buckets: DataFrame, district_totals: DataFrame
) -> DataFrame:
    """
    Process presence of children bucket aggregation with label mapping:
    "Y" -> "Yes", "N" -> "No", else -> "Unknown"
    """
    children_counts = _aggregate_buckets(
        voters_with_buckets, "district_id", "Presence_Of_Children"
    )
    # Map labels to human-readable categories
    children_counts = children_counts.withColumn(
        "label",
        F.when(F.col("label") == "Y", F.lit("Yes"))
        .when(F.col("label") == "N", F.lit("No"))
        .otherwise(F.lit("Unknown")),
    )
    # Re-aggregate to combine mapped labels
    children_counts = children_counts.groupBy("district_id", "label").agg(
        F.sum("count").alias("count")
    )
    children_counts_with_total = children_counts.join(
        district_totals.select("district_id", "total_constituents"),
        on="district_id",
        how="inner",
    )
    return _buckets_to_array(children_counts_with_total, "presenceOfChildren")


def _process_income_buckets(
    voters_with_buckets: DataFrame, district_totals: DataFrame
) -> DataFrame:
    """Process estimated income range bucket aggregation."""
    income_counts = _aggregate_buckets(
        voters_with_buckets, "district_id", "income_bucket"
    )
    income_counts_with_total = income_counts.join(
        district_totals.select("district_id", "total_constituents"),
        on="district_id",
        how="inner",
    )
    return _buckets_to_array(income_counts_with_total, "estimatedIncomeRange")


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
        max_updated_at = this_df.agg({"updated_at": "max"}).collect()[0][0]
        if max_updated_at:
            # Get districts with updated voters
            updated_voter_ids = (
                voter_df.filter(F.col("updated_at") > max_updated_at)
                .select("id")
                .distinct()
            )
            # Get distinct district IDs for updated voters
            updated_district_ids = (
                districtvoter_df.join(
                    other=updated_voter_ids,
                    on=districtvoter_df.voter_id == updated_voter_ids.id,
                    how="inner",
                )
                .select("district_id")
                .distinct()
            )
            # Filter to only include updated districts
            districtvoter_df = districtvoter_df.join(
                other=updated_district_ids,
                on="district_id",
                how="inner",
            )

    # Join districtvoter with voter data to get demographics per district
    # Select only needed columns from districtvoter_df to avoid column name conflicts
    voters_with_districts = districtvoter_df.select("voter_id", "district_id").join(
        voter_df.select(
            F.col("id").alias("voter_id"),
            F.col("Age_Int"),
            F.col("Homeowner_Probability_Model"),
            F.col("Education_Of_Person"),
            F.col("Presence_Of_Children"),
            F.col("Estimated_Income_Amount_Int"),
            F.col("VoterTelephones_CellPhoneFormatted"),
            F.col("updated_at"),
        ),
        on="voter_id",
        how="inner",
    )

    # Check if there's any data to process
    if voters_with_districts.limit(1).count() == 0:
        return session.createDataFrame([], OUTPUT_SCHEMA)

    # Add age and income bucket label columns
    voters_with_buckets = voters_with_districts.withColumn(
        "age_bucket", _get_age_bucket_label("Age_Int")
    ).withColumn(
        "income_bucket", _get_income_bucket_label("Estimated_Income_Amount_Int")
    )

    # Compute total constituents per district and max updated_at from voters
    district_totals = voters_with_buckets.groupBy("district_id").agg(
        F.count("*").alias("total_constituents"),
        F.sum(
            F.when(
                (F.col("VoterTelephones_CellPhoneFormatted").isNotNull())
                & (F.col("VoterTelephones_CellPhoneFormatted") != ""),
                1,
            ).otherwise(0)
        ).alias("total_constituents_with_cell_phone"),
        F.max(F.col("updated_at")).alias("updated_at"),
    )

    # Compute all bucket aggregations using helper functions
    age_buckets_df = _process_age_buckets(voters_with_buckets, district_totals)
    homeowner_buckets_df = _process_homeowner_buckets(
        voters_with_buckets, district_totals
    )
    education_buckets_df = _process_education_buckets(
        voters_with_buckets, district_totals
    )
    children_buckets_df = _process_presence_of_children_buckets(
        voters_with_buckets, district_totals
    )
    income_buckets_df = _process_income_buckets(voters_with_buckets, district_totals)

    # Join all bucket dataframes together
    result_df = (
        district_totals.join(age_buckets_df, on="district_id", how="left")
        .join(homeowner_buckets_df, on="district_id", how="left")
        .join(education_buckets_df, on="district_id", how="left")
        .join(children_buckets_df, on="district_id", how="left")
        .join(income_buckets_df, on="district_id", how="left")
    )

    # Create the final buckets struct
    final_df = result_df.withColumn(
        "buckets",
        F.struct(
            F.col("age").alias("age"),
            F.col("homeowner").alias("homeowner"),
            F.col("education").alias("education"),
            F.col("presenceOfChildren").alias("presenceOfChildren"),
            F.col("estimatedIncomeRange").alias("estimatedIncomeRange"),
        ),
    ).select(
        "district_id",
        "updated_at",
        "total_constituents",
        "total_constituents_with_cell_phone",
        "buckets",
    )

    return final_df
