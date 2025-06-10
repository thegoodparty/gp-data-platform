import logging
from datetime import datetime
from typing import Any, Dict, List, Tuple

import psycopg2
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

VOTER_COLUMN_LIST = [
    "LALVOTERID",
    "Voters_Active",
    "Voters_StateVoterID",
    "Voters_CountyVoterID",
    "VoterTelephones_LandlineFormatted",
    "VoterTelephones_LandlineConfidenceCode",
    "VoterTelephones_CellPhoneFormatted",
    "VoterTelephones_CellConfidenceCode",
    "Voters_FirstName",
    "Voters_MiddleName",
    "Voters_LastName",
    "Voters_NameSuffix",
    "Residence_Addresses_AddressLine",
    "Residence_Addresses_ExtraAddressLine",
    "Residence_Addresses_City",
    "Residence_Addresses_State",
    "Residence_Addresses_Zip",
    "Residence_Addresses_ZipPlus4",
    "Residence_Addresses_DPBC",
    "Residence_Addresses_CheckDigit",
    "Residence_Addresses_HouseNumber",
    "Residence_Addresses_PrefixDirection",
    "Residence_Addresses_StreetName",
    "Residence_Addresses_Designator",
    "Residence_Addresses_SuffixDirection",
    "Residence_Addresses_ApartmentNum",
    "Residence_Addresses_ApartmentType",
    "Residence_Addresses_CassErrStatCode",
    "Voters_SequenceZigZag",
    "Voters_SequenceOddEven",
    "Residence_Addresses_Latitude",
    "Residence_Addresses_Longitude",
    "Residence_Addresses_GeoHash",
    "Residence_Addresses_LatLongAccuracy",
    "Residence_HHParties_Description",
    "Mailing_Addresses_AddressLine",
    "Mailing_Addresses_ExtraAddressLine",
    "Mailing_Addresses_City",
    "Mailing_Addresses_State",
    "Mailing_Addresses_Zip",
    "Mailing_Addresses_ZipPlus4",
    "Mailing_Addresses_DPBC",
    "Mailing_Addresses_CheckDigit",
    "Mailing_Addresses_HouseNumber",
    "Mailing_Addresses_PrefixDirection",
    "Mailing_Addresses_StreetName",
    "Mailing_Addresses_Designator",
    "Mailing_Addresses_SuffixDirection",
    "Mailing_Addresses_ApartmentNum",
    "Mailing_Addresses_ApartmentType",
    "Mailing_Addresses_CassErrStatCode",
    "Mailing_Families_FamilyID",
    "Mailing_Families_HHCount",
    "Mailing_HHGender_Description",
    "Mailing_HHParties_Description",
    "Voters_Age",
    "Voters_Gender",
    "DateConfidence_Description",
    "Parties_Description",
    "VoterParties_Change_Changed_Party",
    "Ethnic_Description",
    "EthnicGroups_EthnicGroup1Desc",
    "CountyEthnic_LALEthnicCode",
    "CountyEthnic_Description",
    "Religions_Description",
    "Voters_CalculatedRegDate",
    "Voters_OfficialRegDate",
    "Voters_PlaceOfBirth",
    "Languages_Description",
    "AbsenteeTypes_Description",
    "MilitaryStatus_Description",
    "MaritalStatus_Description",
    "Voters_MovedFrom_State",
    "Voters_MovedFrom_Date",
    "Voters_MovedFrom_Party_Description",
    "Voters_VotingPerformanceEvenYearGeneral",
    "Voters_VotingPerformanceEvenYearPrimary",
    "Voters_VotingPerformanceEvenYearGeneralAndPrimary",
    "Voters_VotingPerformanceMinorElection",
    "General_2026",
    "Primary_2026",
    "OtherElection_2026",
    "AnyElection_2025",
    "General_2024",
    "Primary_2024",
    "PresidentialPrimary_2024",
    "OtherElection_2024",
    "AnyElection_2023",
    "General_2022",
    "Primary_2022",
    "OtherElection_2022",
    "AnyElection_2021",
    "General_2020",
    "Primary_2020",
    "PresidentialPrimary_2020",
    "OtherElection_2020",
    "AnyElection_2019",
    "General_2018",
    "Primary_2018",
    "OtherElection_2018",
    "AnyElection_2017",
    "General_2016",
    "Primary_2016",
    "PresidentialPrimary_2016",
    "OtherElection_2016",
    "US_Congressional_District",
    "AddressDistricts_Change_Changed_CD",
    "State_Senate_District",
    "AddressDistricts_Change_Changed_SD",
    "State_House_District",
    "AddressDistricts_Change_Changed_HD",
    "State_Legislative_District",
    "AddressDistricts_Change_Changed_LD",
    "County",
    "Voters_FIPS",
    "AddressDistricts_Change_Changed_County",
    "Precinct",
    "County_Legislative_District",
    "City",
    "City_Council_Commissioner_District",
    "County_Commissioner_District",
    "County_Supervisorial_District",
    "City_Mayoral_District",
    "Town_District",
    "Town_Council",
    "Village",
    "Township",
    "Borough",
    "Hamlet_Community_Area",
    "City_Ward",
    "Town_Ward",
    "Township_Ward",
    "Village_Ward",
    "Borough_Ward",
    "Board_of_Education_District",
    "Board_of_Education_SubDistrict",
    "City_School_District",
    "College_Board_District",
    "Community_College_Commissioner_District",
    "Community_College_SubDistrict",
    "County_Board_of_Education_District",
    "County_Board_of_Education_SubDistrict",
    "County_Community_College_District",
    "County_Superintendent_of_Schools_District",
    "County_Unified_School_District",
    "District_Attorney",
    "Education_Commission_District",
    "Educational_Service_District",
    "Election_Commissioner_District",
    "Elementary_School_District",
    "Elementary_School_SubDistrict",
    "Exempted_Village_School_District",
    "High_School_District",
    "High_School_SubDistrict",
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
    "Judicial_Sub_Circuit_District",
    "Judicial_Superior_Court_District",
    "Judicial_Supreme_Court_District",
    "Middle_School_District",
    "Municipal_Court_District",
    "Proposed_City_Commissioner_District",
    "Proposed_Elementary_School_District",
    "Proposed_Unified_School_District",
    "Regional_Office_of_Education_District",
    "School_Board_District",
    "School_District",
    "School_District_Vocational",
    "School_Facilities_Improvement_District",
    "School_Subdistrict",
    "Service_Area_District",
    "Superintendent_of_Schools_District",
    "Unified_School_District",
    "Unified_School_SubDistrict",
    "Coast_Water_District",
    "Consolidated_Water_District",
    "County_Water_District",
    "County_Water_Landowner_District",
    "County_Water_SubDistrict",
    "Metropolitan_Water_District",
    "Mountain_Water_District",
    "Municipal_Water_District",
    "Municipal_Water_SubDistrict",
    "River_Water_District",
    "Water_Agency",
    "Water_Agency_SubDistrict",
    "Water_Conservation_District",
    "Water_Conservation_SubDistrict",
    "Water_Control__Water_Conservation",
    "Water_Control__Water_Conservation_SubDistrict",
    "Water_District",
    "Water_Public_Utility_District",
    "Water_Public_Utility_Subdistrict",
    "Water_Replacement_District",
    "Water_Replacement_SubDistrict",
    "Water_SubDistrict",
    "County_Fire_District",
    "Fire_District",
    "Fire_Maintenance_District",
    "Fire_Protection_District",
    "Fire_Protection_SubDistrict",
    "Fire_Protection_Tax_Measure_District",
    "Fire_Service_Area_District",
    "Fire_SubDistrict",
    "Independent_Fire_District",
    "Proposed_Fire_District",
    "Unprotected_Fire_District",
    "Bay_Area_Rapid_Transit",
    "Metro_Transit_District",
    "Rapid_Transit_District",
    "Rapid_Transit_SubDistrict",
    "Transit_District",
    "Transit_SubDistrict",
    "Community_Service_District",
    "Community_Service_SubDistrict",
    "County_Service_Area",
    "County_Service_Area_SubDistrict",
    "TriCity_Service_District",
    "Library_Services_District",
    "Airport_District",
    "Annexation_District",
    "Aquatic_Center_District",
    "Aquatic_District",
    "Assessment_District",
    "Bonds_District",
    "Career_Center",
    "Cemetery_District",
    "Central_Committee_District",
    "Chemical_Control_District",
    "Committee_Super_District",
    "Communications_District",
    "Community_College_At_Large",
    "Community_Council_District",
    "Community_Council_SubDistrict",
    "Community_Facilities_District",
    "Community_Facilities_SubDistrict",
    "Community_Hospital_District",
    "Community_Planning_Area",
    "Congressional_Township",
    "Conservation_District",
    "Conservation_SubDistrict",
    "Control_Zone_District",
    "Corrections_District",
    "County_Hospital_District",
    "County_Library_District",
    "County_Memorial_District",
    "County_Paramedic_District",
    "County_Sewer_District",
    "Democratic_Convention_Member",
    "Democratic_Zone",
    "Designated_Market_Area_DMA",
    "Drainage_District",
    "Educational_Service_Subdistrict",
    "Emergency_Communication_911_District",
    "Emergency_Communication_911_SubDistrict",
    "Enterprise_Zone_District",
    "EXT_District",
    "Facilities_Improvement_District",
    "Flood_Control_Zone",
    "Forest_Preserve",
    "Garbage_District",
    "Geological_Hazard_Abatement_District",
    "Health_District",
    "Hospital_SubDistrict",
    "Improvement_Landowner_District",
    "Irrigation_District",
    "Irrigation_SubDistrict",
    "Island",
    "Land_Commission",
    "Landscaping_And_Lighting_Assessment_Distric",
    "Law_Enforcement_District",
    "Learning_Community_Coordinating_Council_District",
    "Levee_District",
    "Levee_Reconstruction_Assesment_District",
    "Library_District",
    "Library_SubDistrict",
    "Lighting_District",
    "Local_Hospital_District",
    "Local_Park_District",
    "Maintenance_District",
    "Master_Plan_District",
    "Memorial_District",
    "Metro_Service_District",
    "Metro_Service_Subdistrict",
    "Mosquito_Abatement_District",
    "Multi_township_Assessor",
    "Municipal_Advisory_Council_District",
    "Municipal_Utility_District",
    "Municipal_Utility_SubDistrict",
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
    "Proposed_City",
    "Proposed_Community_College",
    "Proposed_District",
    "Public_Airport_District",
    "Public_Regulation_Commission",
    "Public_Service_Commission_District",
    "Public_Utility_District",
    "Public_Utility_SubDistrict",
    "Reclamation_District",
    "Recreation_District",
    "Recreational_SubDistrict",
    "Republican_Area",
    "Republican_Convention_Member",
    "Resort_Improvement_District",
    "Resource_Conservation_District",
    "Road_Maintenance_District",
    "Rural_Service_District",
    "Sanitary_District",
    "Sanitary_SubDistrict",
    "Sewer_District",
    "Sewer_Maintenance_District",
    "Sewer_SubDistrict",
    "Snow_Removal_District",
    "Soil_And_Water_District",
    "Soil_And_Water_District_At_Large",
    "Special_Reporting_District",
    "Special_Tax_District",
    "Storm_Water_District",
    "Street_Lighting_District",
    "TV_Translator_District",
    "Unincorporated_District",
    "Unincorporated_Park_District",
    "Ute_Creek_Soil_District",
    "Vector_Control_District",
    "Vote_By_Mail_Area",
    "Wastewater_District",
    "Weed_District",
]

# some columns have since been removed from L2. Use NULLs as a placeholder for existing schema
REMOVED_COLUMNS = [
    "Residence_Addresses_GeoHash",
    "Mailing_Families_HHCount",
    "Mailing_HHGender_Description",
    "Mailing_HHParties_Description",
    "DateConfidence_Description",
    "Religions_Description",
    "Languages_Description",
    "Landscaping_And_Lighting_Assessment_Distric",
    "MilitaryStatus_Description",
    "MaritalStatus_Description",
    "Municipal_Court_District",
    "Water_Control__Water_Conservation",
    "Water_Control__Water_Conservation_SubDistrict",
    "Soil_And_Water_District",
    "Soil_And_Water_District_At_Large",
]

# most columns are text, some are integer
INTEGER_COLUMNS = [
    "Residence_Addresses_CheckDigit",
    "Residence_Addresses_PrefixDirection",
    "Residence_Addresses_SuffixDirection",
    "Mailing_Addresses_CheckDigit",
    "Mailing_Addresses_PrefixDirection",
    "Mailing_Addresses_SuffixDirection",
]

# protect case with double quotes for building SQL strings
PROTECTED_VOTER_COLUMN_LIST = [f'"{col}"' for col in VOTER_COLUMN_LIST]
PROTECTED_REMOVED_COLUMNS = [f'"{col}"' for col in REMOVED_COLUMNS]
PROTECTED_INTEGER_COLUMNS = [f'"{col}"' for col in INTEGER_COLUMNS]
PROTECTED_NONREMOVED_NONINTEGER_COLUMNS = [
    f'"{col}"'
    for col in VOTER_COLUMN_LIST
    if col not in REMOVED_COLUMNS + INTEGER_COLUMNS
]


# build upsert query
voter_column_list_str = ",".join(PROTECTED_VOTER_COLUMN_LIST)
nonremoved_noninteger_column_list_str = ",".join(
    PROTECTED_NONREMOVED_NONINTEGER_COLUMNS
)
removed_column_null_list_str = "NULL AS " + ", NULL AS ".join(PROTECTED_REMOVED_COLUMNS)
integer_column_list_str = "::INT, ".join(PROTECTED_INTEGER_COLUMNS) + "::INT"
update_columns_list_str = " " + ", ".join(
    [
        f"{col} = EXCLUDED.{col}"
        for col in PROTECTED_VOTER_COLUMN_LIST
        if col != "LALVOTERID"
    ]
)

UPSERT_QUERY = (
    """
    INSERT INTO {db_schema}."{table_name}" (
    """
    + voter_column_list_str
    + """
    )
    SELECT
        "LALVOTERID",
        "Voters_Active",
        "Voters_StateVoterID",
        "Voters_CountyVoterID",
        "VoterTelephones_LandlineFormatted",
        "VoterTelephones_LandlineConfidenceCode",
        "VoterTelephones_CellPhoneFormatted",
        "VoterTelephones_CellConfidenceCode",
        "Voters_FirstName",
        "Voters_MiddleName",
        "Voters_LastName",
        "Voters_NameSuffix",
        "Residence_Addresses_AddressLine",
        "Residence_Addresses_ExtraAddressLine",
        "Residence_Addresses_City",
        "Residence_Addresses_State",
        "Residence_Addresses_Zip",
        "Residence_Addresses_ZipPlus4",
        "Residence_Addresses_DPBC",
        "Residence_Addresses_CheckDigit"::INT,
        "Residence_Addresses_HouseNumber",
        "Residence_Addresses_PrefixDirection"::INT,
        "Residence_Addresses_StreetName",
        "Residence_Addresses_Designator",
        "Residence_Addresses_SuffixDirection"::INT,
        "Residence_Addresses_ApartmentNum",
        "Residence_Addresses_ApartmentType",
        "Residence_Addresses_CassErrStatCode",
        "Voters_SequenceZigZag",
        "Voters_SequenceOddEven",
        "Residence_Addresses_Latitude",
        "Residence_Addresses_Longitude",
        NULL AS "Residence_Addresses_GeoHash",
        "Residence_Addresses_LatLongAccuracy",
        "Residence_HHParties_Description",
        "Mailing_Addresses_AddressLine",
        "Mailing_Addresses_ExtraAddressLine",
        "Mailing_Addresses_City",
        "Mailing_Addresses_State",
        "Mailing_Addresses_Zip",
        "Mailing_Addresses_ZipPlus4",
        "Mailing_Addresses_DPBC",
        "Mailing_Addresses_CheckDigit"::INT,
        "Mailing_Addresses_HouseNumber",
        "Mailing_Addresses_PrefixDirection"::INT,
        "Mailing_Addresses_StreetName",
        "Mailing_Addresses_Designator",
        "Mailing_Addresses_SuffixDirection"::INT,
        "Mailing_Addresses_ApartmentNum",
        "Mailing_Addresses_ApartmentType",
        "Mailing_Addresses_CassErrStatCode",
        "Mailing_Families_FamilyID",
        NULL AS "Mailing_Families_HHCount",
        NULL AS "Mailing_HHGender_Description",
        NULL AS "Mailing_HHParties_Description",
        "Voters_Age",
        "Voters_Gender",
        NULL AS "DateConfidence_Description",
        "Parties_Description",
        "VoterParties_Change_Changed_Party",
        "Ethnic_Description",
        "EthnicGroups_EthnicGroup1Desc",
        "CountyEthnic_LALEthnicCode",
        "CountyEthnic_Description",
        NULL AS "Religions_Description",
        "Voters_CalculatedRegDate"::DATE,
        "Voters_OfficialRegDate",
        "Voters_PlaceOfBirth",
        NULL AS "Languages_Description",
        "AbsenteeTypes_Description",
        NULL AS "MilitaryStatus_Description",
        NULL AS "MaritalStatus_Description",
        "Voters_MovedFrom_State",
        "Voters_MovedFrom_Date"::DATE,
        "Voters_MovedFrom_Party_Description",
        "Voters_VotingPerformanceEvenYearGeneral",
        "Voters_VotingPerformanceEvenYearPrimary",
        "Voters_VotingPerformanceEvenYearGeneralAndPrimary",
        "Voters_VotingPerformanceMinorElection",
        "General_2026",
        "Primary_2026",
        "OtherElection_2026",
        "AnyElection_2025",
        "General_2024",
        "Primary_2024",
        "PresidentialPrimary_2024",
        "OtherElection_2024",
        "AnyElection_2023",
        "General_2022",
        "Primary_2022",
        "OtherElection_2022",
        "AnyElection_2021",
        "General_2020",
        "Primary_2020",
        "PresidentialPrimary_2020",
        "OtherElection_2020",
        "AnyElection_2019",
        "General_2018",
        "Primary_2018",
        "OtherElection_2018",
        "AnyElection_2017",
        "General_2016",
        "Primary_2016",
        "PresidentialPrimary_2016",
        "OtherElection_2016",
        "US_Congressional_District",
        "AddressDistricts_Change_Changed_CD",
        "State_Senate_District",
        "AddressDistricts_Change_Changed_SD",
        "State_House_District",
        "AddressDistricts_Change_Changed_HD",
        "State_Legislative_District",
        "AddressDistricts_Change_Changed_LD",
        "County",
        "Voters_FIPS",
        "AddressDistricts_Change_Changed_County",
        "Precinct",
        "County_Legislative_District",
        "City",
        "City_Council_Commissioner_District",
        "County_Commissioner_District",
        "County_Supervisorial_District",
        "City_Mayoral_District",
        "Town_District",
        "Town_Council",
        "Village",
        "Township",
        "Borough",
        "Hamlet_Community_Area",
        "City_Ward",
        "Town_Ward",
        "Township_Ward",
        "Village_Ward",
        "Borough_Ward",
        "Board_of_Education_District",
        "Board_of_Education_SubDistrict",
        "City_School_District",
        "College_Board_District",
        "Community_College_Commissioner_District",
        "Community_College_SubDistrict",
        "County_Board_of_Education_District",
        "County_Board_of_Education_SubDistrict",
        "County_Community_College_District",
        "County_Superintendent_of_Schools_District",
        "County_Unified_School_District",
        "District_Attorney",
        "Education_Commission_District",
        "Educational_Service_District",
        "Election_Commissioner_District",
        "Elementary_School_District",
        "Elementary_School_SubDistrict",
        "Exempted_Village_School_District",
        "High_School_District",
        "High_School_SubDistrict",
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
        "Judicial_Sub_Circuit_District",
        "Judicial_Superior_Court_District",
        "Judicial_Supreme_Court_District",
        "Middle_School_District",
        NULL AS "Municipal_Court_District",
        "Proposed_City_Commissioner_District",
        "Proposed_Elementary_School_District",
        "Proposed_Unified_School_District",
        "Regional_Office_of_Education_District",
        "School_Board_District",
        "School_District",
        "School_District_Vocational",
        "School_Facilities_Improvement_District",
        "School_Subdistrict",
        "Service_Area_District",
        "Superintendent_of_Schools_District",
        "Unified_School_District",
        "Unified_School_SubDistrict",
        "Coast_Water_District",
        "Consolidated_Water_District",
        "County_Water_District",
        "County_Water_Landowner_District",
        "County_Water_SubDistrict",
        "Metropolitan_Water_District",
        "Mountain_Water_District",
        "Municipal_Water_District",
        "Municipal_Water_SubDistrict",
        "River_Water_District",
        "Water_Agency",
        "Water_Agency_SubDistrict",
        "Water_Conservation_District",
        "Water_Conservation_SubDistrict",
        NULL AS "Water_Control__Water_Conservation",
        NULL AS "Water_Control__Water_Conservation_SubDistrict",
        "Water_District",
        "Water_Public_Utility_District",
        "Water_Public_Utility_Subdistrict",
        "Water_Replacement_District",
        "Water_Replacement_SubDistrict",
        "Water_SubDistrict",
        "County_Fire_District",
        "Fire_District",
        "Fire_Maintenance_District",
        "Fire_Protection_District",
        "Fire_Protection_SubDistrict",
        "Fire_Protection_Tax_Measure_District",
        "Fire_Service_Area_District",
        "Fire_SubDistrict",
        "Independent_Fire_District",
        "Proposed_Fire_District",
        "Unprotected_Fire_District",
        "Bay_Area_Rapid_Transit",
        "Metro_Transit_District",
        "Rapid_Transit_District",
        "Rapid_Transit_SubDistrict",
        "Transit_District",
        "Transit_SubDistrict",
        "Community_Service_District",
        "Community_Service_SubDistrict",
        "County_Service_Area",
        "County_Service_Area_SubDistrict",
        "TriCity_Service_District",
        "Library_Services_District",
        "Airport_District",
        "Annexation_District",
        "Aquatic_Center_District",
        "Aquatic_District",
        "Assessment_District",
        "Bonds_District",
        "Career_Center",
        "Cemetery_District",
        "Central_Committee_District",
        "Chemical_Control_District",
        "Committee_Super_District",
        "Communications_District",
        "Community_College_At_Large",
        "Community_Council_District",
        "Community_Council_SubDistrict",
        "Community_Facilities_District",
        "Community_Facilities_SubDistrict",
        "Community_Hospital_District",
        "Community_Planning_Area",
        "Congressional_Township",
        "Conservation_District",
        "Conservation_SubDistrict",
        "Control_Zone_District",
        "Corrections_District",
        "County_Hospital_District",
        "County_Library_District",
        "County_Memorial_District",
        "County_Paramedic_District",
        "County_Sewer_District",
        "Democratic_Convention_Member",
        "Democratic_Zone",
        "Designated_Market_Area_DMA",
        "Drainage_District",
        "Educational_Service_Subdistrict",
        "Emergency_Communication_911_District",
        "Emergency_Communication_911_SubDistrict",
        "Enterprise_Zone_District",
        "EXT_District",
        "Facilities_Improvement_District",
        "Flood_Control_Zone",
        "Forest_Preserve",
        "Garbage_District",
        "Geological_Hazard_Abatement_District",
        "Health_District",
        "Hospital_SubDistrict",
        "Improvement_Landowner_District",
        "Irrigation_District",
        "Irrigation_SubDistrict",
        "Island",
        "Land_Commission",
        NULL AS "Landscaping_And_Lighting_Assessment_Distric",
        "Law_Enforcement_District",
        "Learning_Community_Coordinating_Council_District",
        "Levee_District",
        "Levee_Reconstruction_Assesment_District",
        "Library_District",
        "Library_SubDistrict",
        "Lighting_District",
        "Local_Hospital_District",
        "Local_Park_District",
        "Maintenance_District",
        "Master_Plan_District",
        "Memorial_District",
        "Metro_Service_District",
        "Metro_Service_Subdistrict",
        "Mosquito_Abatement_District",
        "Multi_township_Assessor",
        "Municipal_Advisory_Council_District",
        "Municipal_Utility_District",
        "Municipal_Utility_SubDistrict",
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
        "Proposed_City",
        "Proposed_Community_College",
        "Proposed_District",
        "Public_Airport_District",
        "Public_Regulation_Commission",
        "Public_Service_Commission_District",
        "Public_Utility_District",
        "Public_Utility_SubDistrict",
        "Reclamation_District",
        "Recreation_District",
        "Recreational_SubDistrict",
        "Republican_Area",
        "Republican_Convention_Member",
        "Resort_Improvement_District",
        "Resource_Conservation_District",
        "Road_Maintenance_District",
        "Rural_Service_District",
        "Sanitary_District",
        "Sanitary_SubDistrict",
        "Sewer_District",
        "Sewer_Maintenance_District",
        "Sewer_SubDistrict",
        "Snow_Removal_District",
        NULL AS "Soil_And_Water_District",
        NULL AS "Soil_And_Water_District_At_Large",
        "Special_Reporting_District",
        "Special_Tax_District",
        "Storm_Water_District",
        "Street_Lighting_District",
        "TV_Translator_District",
        "Unincorporated_District",
        "Unincorporated_Park_District",
        "Ute_Creek_Soil_District",
        "Vector_Control_District",
        "Vote_By_Mail_Area",
        "Wastewater_District",
        "Weed_District"
    FROM {staging_schema}."{table_name}"
    ON CONFLICT ("LALVOTERID") DO UPDATE SET
        "Voters_Active" = EXCLUDED."Voters_Active",
        "Voters_StateVoterID" = EXCLUDED."Voters_StateVoterID",
        "Voters_CountyVoterID" = EXCLUDED."Voters_CountyVoterID",
        "VoterTelephones_LandlineFormatted" = EXCLUDED."VoterTelephones_LandlineFormatted",
        "VoterTelephones_LandlineConfidenceCode" = EXCLUDED."VoterTelephones_LandlineConfidenceCode",
        "VoterTelephones_CellPhoneFormatted" = EXCLUDED."VoterTelephones_CellPhoneFormatted",
        "VoterTelephones_CellConfidenceCode" = EXCLUDED."VoterTelephones_CellConfidenceCode",
        "Voters_FirstName" = EXCLUDED."Voters_FirstName",
        "Voters_MiddleName" = EXCLUDED."Voters_MiddleName",
        "Voters_LastName" = EXCLUDED."Voters_LastName",
        "Voters_NameSuffix" = EXCLUDED."Voters_NameSuffix",
        "Residence_Addresses_AddressLine" = EXCLUDED."Residence_Addresses_AddressLine",
        "Residence_Addresses_ExtraAddressLine" = EXCLUDED."Residence_Addresses_ExtraAddressLine",
        "Residence_Addresses_City" = EXCLUDED."Residence_Addresses_City",
        "Residence_Addresses_State" = EXCLUDED."Residence_Addresses_State",
        "Residence_Addresses_Zip" = EXCLUDED."Residence_Addresses_Zip",
        "Residence_Addresses_ZipPlus4" = EXCLUDED."Residence_Addresses_ZipPlus4",
        "Residence_Addresses_DPBC" = EXCLUDED."Residence_Addresses_DPBC",
        "Residence_Addresses_CheckDigit" = EXCLUDED."Residence_Addresses_CheckDigit",
        "Residence_Addresses_HouseNumber" = EXCLUDED."Residence_Addresses_HouseNumber",
        "Residence_Addresses_PrefixDirection" = EXCLUDED."Residence_Addresses_PrefixDirection",
        "Residence_Addresses_StreetName" = EXCLUDED."Residence_Addresses_StreetName",
        "Residence_Addresses_Designator" = EXCLUDED."Residence_Addresses_Designator",
        "Residence_Addresses_SuffixDirection" = EXCLUDED."Residence_Addresses_SuffixDirection",
        "Residence_Addresses_ApartmentNum" = EXCLUDED."Residence_Addresses_ApartmentNum",
        "Residence_Addresses_ApartmentType" = EXCLUDED."Residence_Addresses_ApartmentType",
        "Residence_Addresses_CassErrStatCode" = EXCLUDED."Residence_Addresses_CassErrStatCode",
        "Voters_SequenceZigZag" = EXCLUDED."Voters_SequenceZigZag",
        "Voters_SequenceOddEven" = EXCLUDED."Voters_SequenceOddEven",
        "Residence_Addresses_Latitude" = EXCLUDED."Residence_Addresses_Latitude",
        "Residence_Addresses_Longitude" = EXCLUDED."Residence_Addresses_Longitude",
        "Residence_Addresses_LatLongAccuracy" = EXCLUDED."Residence_Addresses_LatLongAccuracy",
        "Residence_HHParties_Description" = EXCLUDED."Residence_HHParties_Description",
        "Mailing_Addresses_AddressLine" = EXCLUDED."Mailing_Addresses_AddressLine",
        "Mailing_Addresses_ExtraAddressLine" = EXCLUDED."Mailing_Addresses_ExtraAddressLine",
        "Mailing_Addresses_City" = EXCLUDED."Mailing_Addresses_City",
        "Mailing_Addresses_State" = EXCLUDED."Mailing_Addresses_State",
        "Mailing_Addresses_Zip" = EXCLUDED."Mailing_Addresses_Zip",
        "Mailing_Addresses_ZipPlus4" = EXCLUDED."Mailing_Addresses_ZipPlus4",
        "Mailing_Addresses_DPBC" = EXCLUDED."Mailing_Addresses_DPBC",
        "Mailing_Addresses_CheckDigit" = EXCLUDED."Mailing_Addresses_CheckDigit",
        "Mailing_Addresses_HouseNumber" = EXCLUDED."Mailing_Addresses_HouseNumber",
        "Mailing_Addresses_PrefixDirection" = EXCLUDED."Mailing_Addresses_PrefixDirection",
        "Mailing_Addresses_StreetName" = EXCLUDED."Mailing_Addresses_StreetName",
        "Mailing_Addresses_Designator" = EXCLUDED."Mailing_Addresses_Designator",
        "Mailing_Addresses_SuffixDirection" = EXCLUDED."Mailing_Addresses_SuffixDirection",
        "Mailing_Addresses_ApartmentNum" = EXCLUDED."Mailing_Addresses_ApartmentNum",
        "Mailing_Addresses_ApartmentType" = EXCLUDED."Mailing_Addresses_ApartmentType",
        "Mailing_Addresses_CassErrStatCode" = EXCLUDED."Mailing_Addresses_CassErrStatCode",
        "Mailing_Families_FamilyID" = EXCLUDED."Mailing_Families_FamilyID",
        "Mailing_Families_HHCount" = EXCLUDED."Mailing_Families_HHCount",
        "Mailing_HHGender_Description" = EXCLUDED."Mailing_HHGender_Description",
        "Mailing_HHParties_Description" = EXCLUDED."Mailing_HHParties_Description",
        "Voters_Age" = EXCLUDED."Voters_Age",
        "Voters_MovedFrom_State" = EXCLUDED."Voters_MovedFrom_State",
        "Voters_MovedFrom_Date" = EXCLUDED."Voters_MovedFrom_Date",
        "Voters_MovedFrom_Party_Description" = EXCLUDED."Voters_MovedFrom_Party_Description",
        "Voters_VotingPerformanceEvenYearGeneral" = EXCLUDED."Voters_VotingPerformanceEvenYearGeneral",
        "Voters_VotingPerformanceEvenYearPrimary" = EXCLUDED."Voters_VotingPerformanceEvenYearPrimary",
        "Voters_VotingPerformanceEvenYearGeneralAndPrimary" = EXCLUDED."Voters_VotingPerformanceEvenYearGeneralAndPrimary",
        "Voters_VotingPerformanceMinorElection" = EXCLUDED."Voters_VotingPerformanceMinorElection"
    """
    # + update_columns_list_str  # TODO: add this back in if we increase compute resources
)


def _execute_sql_query(
    query: str,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    return_results: bool = False,
) -> List[Tuple[Any, ...]]:
    """
    Execute a SQL query and return the results. Not that the results should be None if no results are returned.
    """
    try:
        conn = psycopg2.connect(
            dbname=database, user=user, password=password, host=host, port=port
        )
        cursor = conn.cursor()
        cursor.execute(query)
        if return_results:
            results = cursor.fetchall() if cursor.rowcount > 0 else []
        else:
            results = []
        conn.commit()
    except Exception as e:
        logging.error(f"Error executing query: {query}")
        logging.error(f"Error: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

    return results


def _load_data_to_postgres(
    df: DataFrame,
    state_id: str,
    source_file_name: str,
    upsert_query: str,
    db_host: str,
    db_port: int,
    db_user: str,
    db_pw: str,
    db_name: str,
    staging_schema: str,
    db_schema: str,
) -> int:
    """
    Load a DataFrame to PostgreSQL via JDBC and execute an upsert query.

    Args:
        df: DataFrame to load
        state_id: State ID
        source_file_name: Name of the source file
        upsert_query: SQL query to execute for upserting data from staging to the target table
        db_host: Database host
        db_port: Database port
        db_user: Database user
        db_pw: Database password
        db_name: Database name
        staging_schema: Schema for staging tables
        db_schema: Target schema for the final tables

    Returns:
        Number of rows loaded
    """
    table_name = f"Voter{state_id.upper()}"
    logging.info(f"Writing {table_name} data to PostgreSQL via JDBC")

    # Construct JDBC URL
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

    # make a wake up call to the database
    _execute_sql_query(
        'SELECT * FROM public."VoterFile" LIMIT 1;',
        db_host,
        db_port,
        db_user,
        db_pw,
        db_name,
        return_results=False,
    )

    df.write.format("jdbc").option("url", jdbc_url).option(
        "dbtable", f'{staging_schema}."{table_name}"'
    ).option("user", db_user).option("password", db_pw).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "overwrite"
    ).save()

    # turn off synchronous_commit for the large upsert query in the session
    _execute_sql_query(
        "SET synchronous_commit = off; SET work_mem = '128MB'; SET max_parallel_workers_per_gather = 8;",
        db_host,
        db_port,
        db_user,
        db_pw,
        db_name,
    )

    # Execute the upsert query to the destination table
    upsert_query_w_config = (
        "SET synchronous_commit = off; "
        "SET work_mem = '128MB'; "
        "SET max_parallel_workers_per_gather = 8; "
        + upsert_query.format(
            db_schema=db_schema, staging_schema=staging_schema, table_name=table_name
        )
        + ";"
    )
    _execute_sql_query(
        upsert_query_w_config,
        db_host,
        db_port,
        db_user,
        db_pw,
        db_name,
    )

    # turn synchronous_commit back on
    _execute_sql_query(
        "SET synchronous_commit = on;",
        db_host,
        db_port,
        db_user,
        db_pw,
        db_name,
    )

    # number of rows loaded
    num_rows_loaded = df.count()

    # load the source file name to the destination table
    _execute_sql_query(
        f"""
        INSERT INTO {db_schema}."VoterFile" (
            "Filename",
            "State",
            "Lines",
            "Loaded",
            "updatedAt"
        )
        VALUES (
            '{source_file_name}',
            '{state_id}',
            {num_rows_loaded},
            true,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT ("Filename") DO UPDATE SET
            "Lines" = EXCLUDED."Lines",
            "Loaded" = EXCLUDED."Loaded",
            "updatedAt" = CURRENT_TIMESTAMP
        """,
        db_host,
        db_port,
        db_user,
        db_pw,
        db_name,
    )

    return num_rows_loaded


def model(dbt, session: SparkSession) -> DataFrame:
    """
    This model loads data from Databricks to the voter db in gp-api.
    It is used to load the data from the Databricks tables to the gp-api database.
    """
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="append",
        unique_key="id",
        on_schema_change="fail",
        tags=["l2", "databricks", "gp-api", "load"],
    )

    # get dbt configs
    staging_schema = dbt.config.get("staging_schema")
    db_host = dbt.config.get("voter_db_host")
    db_port = int(dbt.config.get("voter_db_port"))
    db_user = dbt.config.get("voter_db_user")
    db_pw = dbt.config.get("voter_db_pw")
    db_name = dbt.config.get("voter_db_name")
    db_schema = dbt.config.get("voter_db_schema")

    # get latest files loaded to databricks, filtering for uniform files
    loaded_to_databricks = dbt.ref("load__l2_s3_to_databricks").filter(
        col("source_file_type") == "uniform"
    )
    state_list = [
        row.state_id
        for row in loaded_to_databricks.select("state_id").distinct().collect()
    ]

    # TODO: test in a subset of states, eventually read all 50 + DC
    state_list = [
        "AK",
        "AL",
        "AR",
        "AZ",
        "CO",
        "CT",
        "DC",
        "LA",
        "MN",
        "NJ",
        "VA",
        "WY",
    ]

    # initialize list to capture metadata about data loads
    load_details: List[Dict[str, Any]] = []

    # Create a staging schema if it doesn't exist
    _execute_sql_query(
        f"CREATE SCHEMA IF NOT EXISTS {staging_schema};",
        db_host,
        db_port,
        db_user,
        db_pw,
        db_name,
    )

    for state_id in state_list:
        state_files_loaded = loaded_to_databricks.filter(col("state_id") == state_id)

        # order by loaded_at descending and take the first row
        latest_file = state_files_loaded.orderBy(col("loaded_at").desc()).first()

        # check if it's already loaded or not. If so, skip.
        query = f"""
            SELECT COUNT(*) FROM {db_schema}."VoterFile"
            WHERE "Filename" = '{latest_file.source_file_name}'
        """
        results = _execute_sql_query(
            query, db_host, db_port, db_user, db_pw, db_name, return_results=True
        )
        if results[0][0] > 0:
            continue

        # write to destination postgres table
        voter_column_list = [
            col for col in VOTER_COLUMN_LIST if col not in REMOVED_COLUMNS
        ]

        df = session.read.table(latest_file.table_path).select(voter_column_list)

        # safe_cast the integer columns
        for column in INTEGER_COLUMNS:
            df = df.withColumn(column, col(column).try_cast("int"))

        num_rows_loaded = _load_data_to_postgres(
            df=df,
            state_id=state_id,
            source_file_name=latest_file.source_file_name,
            upsert_query=UPSERT_QUERY,
            db_host=db_host,
            db_port=db_port,
            db_user=db_user,
            db_pw=db_pw,
            db_name=db_name,
            staging_schema=staging_schema,
            db_schema=db_schema,
        )

        # add to load_details
        load_details.append(
            {
                "id": latest_file.id,
                "load_id": latest_file.load_id,
                "loaded_at": datetime.now(),
                "state_id": state_id,
                "source_s3_path": latest_file.source_s3_path,
                "source_file_name": latest_file.source_file_name,
                "source_file_type": latest_file.source_file_type,
                "table_name": f"Voter{state_id.upper()}",
                "num_rows_loaded": num_rows_loaded,
            }
        )

    load_details_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("load_id", StringType(), True),
            StructField("loaded_at", TimestampType(), True),
            StructField("state_id", StringType(), True),
            StructField("source_s3_path", StringType(), True),
            StructField("source_file_name", StringType(), True),
            StructField("source_file_type", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("num_rows_loaded", IntegerType(), True),
        ]
    )
    load_details_df = session.createDataFrame(load_details, load_details_schema)
    return load_details_df
