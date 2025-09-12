import logging
from datetime import datetime
from typing import Any, Dict, List, Tuple
from uuid import uuid4

import psycopg2
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, max
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

VOTER_COLUMN_LIST: list = [
    "id",
    "LALVOTERID",
    "State",
    "VoterTelephones_LandlineFormatted",
    "VoterTelephones_LandlineConfidenceCode",
    "VoterTelephones_CellPhoneFormatted",
    "VoterTelephones_CellConfidenceCode",
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
    "DateConfidence_Description",
    "Parties_Description",
    "VoterParties_Change_Changed_Party",
    "Ethnic_Description",
    "EthnicGroups_EthnicGroup1Desc",
    "CountyEthnic_LALEthnicCode",
    "CountyEthnic_Description",
    "Religions_Description",
    "Languages_Description",
    "AbsenteeTypes_Description",
    "MilitaryStatus_Description",
    "MaritalStatus_Description",
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
    "Active",
    "Age",
    "Age_Int",
    "CalculatedRegDate",
    "CountyVoterID",
    "FIPS",
    "FirstName",
    "Gender",
    "LastName",
    "MiddleName",
    "MovedFrom_Date",
    "MovedFrom_Party_Description",
    "MovedFrom_State",
    "NameSuffix",
    "OfficialRegDate",
    "PlaceOfBirth",
    "SequenceOddEven",
    "SequenceZigZag",
    "StateVoterID",
    "VotingPerformanceEvenYearGeneral",
    "VotingPerformanceEvenYearGeneralAndPrimary",
    "VotingPerformanceEvenYearPrimary",
    "VotingPerformanceMinorElection",
    "created_at",
    "updated_at",
]

# protect case with double quotes for building SQL strings
protected_voter_column_list = [f'"{col}"' for col in VOTER_COLUMN_LIST]
voter_column_list_str = ",".join(
    [col for col in protected_voter_column_list if col != '"id"']
)
update_set_query = [
    f"{col} = EXCLUDED.{col}"
    for col in protected_voter_column_list
    if col != '"LALVOTERID"'
]
update_set_query_str = ", ".join(update_set_query)

# Note that the value list under `INSERT` and `SELECT` must be in the same order
UPSERT_QUERY = (
    """
    INSERT INTO {db_schema}."{table_name}" (
        "id",
    """
    + voter_column_list_str
    + """
    )
    SELECT
        "id"::uuid,
    """
    + voter_column_list_str
    + """
    FROM {staging_schema}."{staging_table_name}"
    ON CONFLICT ("LALVOTERID") DO UPDATE SET
    """
    + update_set_query_str
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
    table_name = "Voter"
    staging_table_name = f"Voter_{state_id.upper()}"
    logging.info(f"Writing {state_id} data to PostgreSQL via JDBC")

    # Construct JDBC URL
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

    # make a wake up call to the database
    _execute_sql_query(
        'SELECT * FROM public."Voter" LIMIT 1;',
        db_host,
        db_port,
        db_user,
        db_pw,
        db_name,
        return_results=False,
    )

    df.write.format("jdbc").option("url", jdbc_url).option(
        "dbtable", f'{staging_schema}."{staging_table_name}"'
    ).option("user", db_user).option("password", db_pw).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "overwrite"
    ).save()

    # turn off synchronous_commit, increase working memory parallelization for the large upsert query in the session
    upsert_query_w_config = (
        "SET synchronous_commit = off; "
        "SET work_mem = '128MB'; "
        "SET max_parallel_workers_per_gather = 8; "
        + upsert_query.format(
            db_schema=db_schema,
            staging_schema=staging_schema,
            table_name=table_name,
            staging_table_name=staging_table_name,
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

    return df.count()


def model(dbt, session: SparkSession) -> DataFrame:
    """
    This model loads data from Databricks to the people api database.
    It is used to load the data from the Databricks tables to the people api database.
    """
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="append",
        unique_key="id",
        on_schema_change="fail",
        tags=["l2", "databricks", "people_api", "load"],
    )

    # get dbt configs
    staging_schema = dbt.config.get("staging_schema")
    db_host = dbt.config.get("people_db_host")
    db_port = int(dbt.config.get("people_db_port"))
    db_name = dbt.config.get("people_db_name")
    db_schema = dbt.config.get("people_db_schema")
    db_user = dbt.config.get("people_db_user")
    db_pw = dbt.config.get("people_db_pw")
    dbt_env_name = dbt.config.get("dbt_environment")

    voter_table: DataFrame = dbt.ref("m_people_api__voter")

    # downsample for non-prod environment with three least populous states
    # TODO: downsample based on on dbt cloud account. current env vars listed in docs are not available
    # see https://docs.getdbt.com/docs/build/environment-variables#special-environment-variables
    filter_list = ["WY"]  # , "ND", "VT"]
    if dbt_env_name != "prod":
        voter_table = voter_table.filter(col("State").isin(filter_list))

    # count (forces cache and preceding filters) the dataframe and enforce filter before for-loop filtering
    voter_table.count()

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

    # get the list of states to load
    state_list: List[str] = [
        row.State for row in voter_table.select("State").distinct().collect()
    ]

    # load data state by state since job may time out
    load_id = str(uuid4())
    if dbt.is_incremental:
        this_table = session.table(f"{dbt.this}")

    for state_id in state_list:
        state_df = voter_table.filter(col("State") == state_id)

        # handle incremental loading
        if dbt.is_incremental:
            max_loaded_at = (
                this_table.filter(col("State") == state_id)
                .agg(max("loaded_at"))
                .collect()[0][0]
            )
            if max_loaded_at:
                state_df = state_df.filter(col("loaded_at") > max_loaded_at)

        num_rows_loaded = _load_data_to_postgres(
            df=state_df,
            state_id=state_id,
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
                "id": str(uuid4()),
                "load_id": load_id,
                "loaded_at": datetime.now(),
                "state_id": state_id,
                "num_rows_loaded": num_rows_loaded,
            }
        )

    load_details_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("load_id", StringType(), True),
            StructField("loaded_at", TimestampType(), True),
            StructField("state_id", StringType(), True),
            StructField("num_rows_loaded", IntegerType(), True),
        ]
    )
    load_details_df = session.createDataFrame(load_details, load_details_schema)
    return load_details_df
