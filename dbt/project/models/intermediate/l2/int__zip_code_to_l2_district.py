from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, lit

DISTRICT_TYPE_FROM_COLUMNS = [
    "County",
    "Voters_FIPS",
    "County_Commissioner_District",
    "County_Supervisorial_District",
    "Precinct",
    "US_Congressional_District",
    "State_Senate_District",
    "State_House_District",
    "State_Legislative_District",
    "Borough",
    "Borough_Ward",
    "City",
    "City_Council_Commissioner_District",
    "City_Mayoral_District",
    "City_Ward",
    "Hamlet_Community_Area",
    "Proposed_City",
    "Proposed_City_Commissioner_District",
    "Town_Council",
    "Town_District",
    "Town_Ward",
    "Township",
    "Township_Ward",
    "Village",
    "Village_Ward",
    "Judicial_Appellate_District",
    "Judicial_Chancery_Court",
    "Judicial_Circuit_Court_District",
    "Judicial_County_Board_of_Review_District",
    "Judicial_County_Court_District",
    "Judicial_District",
    "Judicial_District_Court_District",
    "Judicial_Family_Court_District",
    "Judicial_Jury_District",
    "Judicial_Justice_of_the_Peace",
    "Judicial_Juvenile_Court_District",
    "Judicial_Magistrate_Division",
    "Judicial_Municipal_Court_District",
    "Judicial_Sub_Circuit_District",
    "Judicial_Superior_Court_District",
    "Judicial_Supreme_Court_District",
    "City_School_District",
    "College_Board_District",
    "Community_College",
    "Community_College_At_Large",
    "Community_College_Commissioner_District",
    "Community_College_SubDistrict",
    "County_Board_of_Education_District",
    "County_Board_of_Education_SubDistrict",
    "County_Community_College_District",
    "County_Superintendent_of_Schools_District",
    "County_Unified_School_District",
    "Education_Commission_District",
    "Educational_Service_District",
    "Educational_Service_Subdistrict",
    "Elementary_School_District",
    "Elementary_School_SubDistrict",
    "Exempted_Village_School_District",
    "High_School_District",
    "High_School_SubDistrict",
    "Proposed_Community_College",
    "Proposed_Elementary_School_District",
    "Proposed_Unified_School_District",
    "Regional_Office_of_Education_District",
    "School_Board_District",
    "School_District",
    "School_District_Vocational",
    "School_Facilities_Improvement_District",
    "School_Subdistrict",
    "Superintendent_of_Schools_District",
    "Unified_School_District",
    "Unified_School_SubDistrict",
    "2024_Proposed_Congressional_District",
    "2024_Proposed_State_Senate_District",
    "2024_Proposed_State_House_District",
    "2024_Proposed_State_Legislative_District",
    "2001_US_Congressional_District",
    "2001_State_House_District",
    "2001_State_Legislative_District",
    "2001_State_Senate_District",
    "2010_US_Congressional_District",
    "2010_State_House_District",
    "2010_State_Legislative_District",
    "2010_State_Senate_District",
    "4H_Livestock_District",
    "Airport_District",
    "Annexation_District",
    "Aquatic_Center_District",
    "Aquatic_District",
    "Assessment_District",
    "Bay_Area_Rapid_Transit",
    "Board_of_Education_District",
    "Board_of_Education_SubDistrict",
    "Bonds_District",
    "Career_Center",
    "Cemetery_District",
    "Central_Committee_District",
    "Chemical_Control_District",
    "Coast_Water_District",
    "Committee_Super_District",
    "Communications_District",
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
    "County_Fire_District",
    "County_Hospital_District",
    "County_Legislative_District",
    "County_Library_District",
    "County_Memorial_District",
    "County_Paramedic_District",
    "County_Service_Area",
    "County_Service_Area_SubDistrict",
    "County_Sewer_District",
    "County_Water_District",
    "County_Water_Landowner_District",
    "County_Water_SubDistrict",
    "Democratic_Convention_Member",
    "Democratic_Zone",
    "District_Attorney",
    "Drainage_District",
    "Election_Commissioner_District",
    "Emergency_Communication_911_District",
    "Emergency_Communication_911_SubDistrict",
    "Enterprise_Zone_District",
    "EXT_District",
    "Facilities_Improvement_District",
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
    "Health_District",
    "Hospital_District",
    "Hospital_SubDistrict",
    "Improvement_Landowner_District",
    "Independent_Fire_District",
    "Irrigation_District",
    "Irrigation_SubDistrict",
    "Island",
    "Land_Commission",
    "Landscaping_and_Lighting_Assessment_District",
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
    "Proposed_District",
    "Proposed_Fire_District",
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
    "Republican_Area",
    "Republican_Convention_Member",
    "Resort_Improvement_District",
    "Resource_Conservation_District",
    "River_Water_District",
    "Road_Maintenance_District",
    "Rural_Service_District",
    "Sanitary_District",
    "Sanitary_SubDistrict",
    "Service_Area_District",
    "Sewer_District",
    "Sewer_Maintenance_District",
    "Sewer_SubDistrict",
    "Snow_Removal_District",
    "Soil_and_Water_District",
    "Soil_and_Water_District_At_Large",
    "Special_Reporting_District",
    "Special_Tax_District",
    "State_Board_of_Equalization",
    "Storm_Water_District",
    "Street_Lighting_District",
    "Transit_District",
    "Transit_SubDistrict",
    "TriCity_Service_District",
    "TV_Translator_District",
    "Unincorporated_District",
    "Unincorporated_Park_District",
    "Unprotected_Fire_District",
    "Ute_Creek_Soil_District",
    "Vector_Control_District",
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
        unique_key="zip_code",
        on_schema_change="fail",
        auto_liquid_cluster=True,
        tags=["intermediate", "l2", "zip_code", "districts"],
    )

    # TODO: handle incremental runs
    # get the max loaded_at for the incremental run
    # if dbt.is_incremental:

    l2_uniform_data: DataFrame = dbt.ref("int__l2_nationwide_uniform")

    # during dev, test on a subset of the states
    state_list = ["AL", "AK", "AZ", "AR"]
    l2_uniform_data = l2_uniform_data.filter(col("state_postal_code").isin(state_list))

    # get the distinct zip codes from the l2_uniform_data
    distinct_zip_codes: DataFrame = (
        l2_uniform_data.select(col("Residence_Addresses_Zip").alias("zip_code"))
        .distinct()
        .union(
            l2_uniform_data.select(
                col("Mailing_Addresses_Zip").alias("zip_code")
            ).distinct()
        )
    )

    # for each zip code, for each district type (column), get the unique set of values for that column
    for zip_code in distinct_zip_codes.collect():
        for district_type in DISTRICT_TYPE_FROM_COLUMNS:

            # get the unique set of values for that column
            unique_values: DataFrame = (
                l2_uniform_data.filter(
                    (col("Residence_Addresses_Zip") == zip_code.zip_code)
                    | (col("Mailing_Addresses_Zip") == zip_code.zip_code)
                )
                .select(col(district_type).alias("district"), col("state_postal_code"))
                .distinct()
            )

            # add column for zip code to be used as join key
            unique_values = unique_values.withColumn("zip_code", lit(zip_code.zip_code))

            # join the unique values to the zip code
            zip_code_to_district: DataFrame = zip_code.join(
                unique_values, on="zip_code", how="left"
            )
            zip_code_to_district = zip_code_to_district.withColumn(
                "district_type", lit(district_type)
            )

            # union the zip code to the district
            distinct_zip_codes = distinct_zip_codes.union(zip_code_to_district)

    return distinct_zip_codes
