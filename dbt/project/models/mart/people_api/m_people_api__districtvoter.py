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
        StructField("type", StringType(), False),
        StructField("name", StringType(), False),
        StructField("state", StringType(), False),
        StructField("created_at", StringType(), False),
        StructField("updated_at", StringType(), False),
    ]
)

# Define the district columns to process
DISTRICT_COLUMNS = [
    "AddressDistricts_Change_Changed_CD",
    "AddressDistricts_Change_Changed_County",
    "AddressDistricts_Change_Changed_HD",
    "AddressDistricts_Change_Changed_LD",
    "AddressDistricts_Change_Changed_SD",
    "Airport_District",
    "Annexation_District",
    "Aquatic_Center_District",
    "Aquatic_District",
    "Assessment_District",
    "Bay_Area_Rapid_Transit",
    "Board_of_Education_District",
    "Board_of_Education_SubDistrict",
    "Bonds_District",
    "Borough",
    "Borough_Ward",
    "Career_Center",
    "Cemetery_District",
    "Central_Committee_District",
    "Chemical_Control_District",
    "Committee_Super_District",
    "City",
    "City_Council_Commissioner_District",
    "City_Mayoral_District",
    "City_School_District",
    "City_Ward",
    "Coast_Water_District",
    "College_Board_District",
    "Communications_District",
    "Community_College_At_Large",
    "Community_College_Commissioner_District",
    "Community_College_SubDistrict",
    "Community_Council_District",
    "Community_Council_SubDistrict",
    "Community_Facilities_District",
    "Community_Facilities_SubDistrict",
    "Community_Hospital_District",
    "Community_Planning_Area",
    "Community_Service_District",
    "Community_Service_SubDistrict",
    "Congressional_Township",
    "Conservation_District",
    "Conservation_SubDistrict",
    "Consolidated_Water_District",
    "Control_Zone_District",
    "Corrections_District",
    "County",
    "County_Board_of_Education_District",
    "County_Board_of_Education_SubDistrict",
    "County_Community_College_District",
    "County_Commissioner_District",
    "County_Fire_District",
    "County_Hospital_District",
    "County_Legislative_District",
    "County_Library_District",
    "County_Memorial_District",
    "County_Paramedic_District",
    "County_Service_Area",
    "County_Service_Area_SubDistrict",
    "County_Sewer_District",
    "County_Superintendent_of_Schools_District",
    "County_Supervisorial_District",
    "County_Unified_School_District",
    "County_Water_District",
    "County_Water_Landowner_District",
    "County_Water_SubDistrict",
    "Democratic_Convention_Member",
    "Democratic_Zone",
    "Designated_Market_Area_DMA",
    "District_Attorney",
    "Drainage_District",
    "Education_Commission_District",
    "Educational_Service_District",
    "Educational_Service_Subdistrict",
    "Election_Commissioner_District",
    "Elementary_School_District",
    "Elementary_School_SubDistrict",
    "Emergency_Communication_911_District",
    "Emergency_Communication_911_SubDistrict",
    "Enterprise_Zone_District",
    "EXT_District",
    "Exempted_Village_School_District",
    "Facilities_Improvement_District",
    "FIPS",
    "Fire_District",
    "Fire_Maintenance_District",
    "Fire_Protection_District",
    "Fire_Protection_SubDistrict",
    "Fire_Protection_Tax_Measure_District",
    "Fire_Service_Area_District",
    "Fire_SubDistrict",
    "Flood_Control_Zone",
    "Forest_Preserve",
    "Garbage_District",
    "Geological_Hazard_Abatement_District",
    "Hamlet_Community_Area",
    "Health_District",
    "High_School_District",
    "High_School_SubDistrict",
    "Hospital_SubDistrict",
    "Improvement_Landowner_District",
    "Independent_Fire_District",
    "Irrigation_District",
    "Irrigation_SubDistrict",
    "Island",
    "Judicial_Appellate_District",
    "Judicial_Circuit_Court_District",
    "Judicial_County_Board_of_Review_District",
    "Judicial_County_Court_District",
    "Judicial_District",
    "Judicial_District_Court_District",
    "Judicial_Family_Court_District",
    "Judicial_Jury_District",
    "Judicial_Juvenile_Court_District",
    "Judicial_Magistrate_Division",
    "Judicial_Municipal_Court_District",
    "Judicial_Sub_Circuit_District",
    "Judicial_Superior_Court_District",
    "Judicial_Supreme_Court_District",
    "Landscaping_and_Lighting_Assessment_District",
    "Land_Commission",
    "Law_Enforcement_District",
    "Learning_Community_Coordinating_Council_District",
    "Levee_District",
    "Levee_Reconstruction_Assesment_District",
    "Library_District",
    "Library_Services_District",
    "Library_SubDistrict",
    "Lighting_District",
    "Local_Hospital_District",
    "Local_Park_District",
    "Maintenance_District",
    "Master_Plan_District",
    "Memorial_District",
    "Metro_Service_District",
    "Metro_Service_Subdistrict",
    "Metro_Transit_District",
    "Metropolitan_Water_District",
    "Middle_School_District",
    "Mosquito_Abatement_District",
    "Mountain_Water_District",
    "Multi_township_Assessor",
    "Municipal_Advisory_Council_District",
    "Municipal_Utility_District",
    "Municipal_Utility_SubDistrict",
    "Municipal_Water_District",
    "Municipal_Water_SubDistrict",
    "Museum_District",
    "Northeast_Soil_and_Water_District",
    "Open_Space_District",
    "Open_Space_SubDistrict",
    "Other",
    "Paramedic_District",
    "Park_Commissioner_District",
    "Park_District",
    "Park_SubDistrict",
    "Planning_Area_District",
    "Police_District",
    "Port_District",
    "Port_SubDistrict",
    "Power_District",
    "Precinct",
    "Proposed_City",
    "Proposed_City_Commissioner_District",
    "Proposed_Community_College",
    "Proposed_District",
    "Proposed_Elementary_School_District",
    "Proposed_Fire_District",
    "Proposed_Unified_School_District",
    "Public_Airport_District",
    "Public_Regulation_Commission",
    "Public_Service_Commission_District",
    "Public_Utility_District",
    "Public_Utility_SubDistrict",
    "Rapid_Transit_District",
    "Rapid_Transit_SubDistrict",
    "Reclamation_District",
    "Recreation_District",
    "Recreational_SubDistrict",
    "Regional_Office_of_Education_District",
    "Republican_Area",
    "Republican_Convention_Member",
    "Resort_Improvement_District",
    "Resource_Conservation_District",
    "River_Water_District",
    "Road_Maintenance_District",
    "Rural_Service_District",
    "Sanitary_District",
    "Sanitary_SubDistrict",
    "School_Board_District",
    "School_District",
    "School_District_Vocational",
    "School_Facilities_Improvement_District",
    "School_Subdistrict",
    "Service_Area_District",
    "Sewer_District",
    "Sewer_Maintenance_District",
    "Sewer_SubDistrict",
    "Snow_Removal_District",
    "Special_Reporting_District",
    "Special_Tax_District",
    "State_House_District",
    "State_Legislative_District",
    "State_Senate_District",
    "Storm_Water_District",
    "Street_Lighting_District",
    "Superintendent_of_Schools_District",
    "TV_Translator_District",
    "Town_Council",
    "Town_District",
    "Town_Ward",
    "Township",
    "Township_Ward",
    "Transit_District",
    "Transit_SubDistrict",
    "TriCity_Service_District",
    "US_Congressional_District",
    "Unified_School_District",
    "Unified_School_SubDistrict",
    "Unincorporated_District",
    "Unincorporated_Park_District",
    "Unprotected_Fire_District",
    "Ute_Creek_Soil_District",
    "Vector_Control_District",
    "Village",
    "Village_Ward",
    "Vote_By_Mail_Area",
    "Wastewater_District",
    "Water_Agency",
    "Water_Agency_SubDistrict",
    "Water_Conservation_District",
    "Water_Conservation_SubDistrict",
    "Water_Control_Water_Conservation",
    "Water_Control_Water_Conservation_SubDistrict",
    "Water_District",
    "Water_Public_Utility_District",
    "Water_Public_Utility_Subdistrict",
    "Water_Replacement_District",
    "Water_Replacement_SubDistrict",
    "Water_SubDistrict",
    "Weed_District",
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

    # Downsample for dev/testing
    district_columns = DISTRICT_COLUMNS  # [:150]
    # (10, 83s + 84s)
    # (40, 183 + 87)
    # (80, 230s + 77s)
    # (150, 335 + 79)
    # (242, 386 + 81)

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

    # Get the district table for joining
    district__mart_df: DataFrame = dbt.ref("m_people_api__district")

    # Join with district table to get district_id
    result_df = (
        districts_from_voters.join(
            district__mart_df.select(
                col("id").alias("district_id"), col("state"), col("type"), col("name")
            ),
            on=["state", "type", "name"],
            how="left",
        )
        .filter(col("district_id").isNotNull())
        .select(
            col("voter_id"),
            col("district_id"),
            col("type"),
            col("created_at"),
            col("updated_at"),
        )
    )

    return result_df
