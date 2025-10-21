import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import psycopg2
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import asc, col, count
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

VOTER_COLUMN_LIST: list = [
    # Core Voter Information
    "id",
    "LALVOTERID",
    "State",
    "AbsenteeTypes_Description",
    "Active",
    "Age",
    "Age_Int",
    "FirstName",
    "MiddleName",
    "LastName",
    "Gender",
    # Demographics
    "Business_Owner",
    "CalculatedRegDate",
    "CountyEthnic_Description",
    "CountyEthnic_LALEthnicCode",
    "CountyVoterID",
    "Education_Of_Person",
    "Estimated_Income_Amount",
    "Estimated_Income_Amount_Int",
    "EthnicGroups_EthnicGroup1Desc",
    "Ethnic_Description",
    "Homeowner_Probability_Model",
    "Language_Code",
    "Mailing_Addresses_AddressLine",
    "Mailing_Addresses_ApartmentNum",
    "Mailing_Addresses_ApartmentType",
    "Mailing_Addresses_CassErrStatCode",
    "Mailing_Addresses_CheckDigit",
    "Mailing_Addresses_City",
    "Mailing_Addresses_Designator",
    "Mailing_Addresses_DPBC",
    "Mailing_Addresses_ExtraAddressLine",
    "Mailing_Addresses_HouseNumber",
    "Mailing_Addresses_PrefixDirection",
    "Mailing_Addresses_State",
    "Mailing_Addresses_StreetName",
    "Mailing_Addresses_SuffixDirection",
    "Mailing_Addresses_Zip",
    "Mailing_Addresses_ZipPlus4",
    "Mailing_Families_FamilyID",
    # tbl_updated.`Mailing_HHGender_Description`,
    "Marital_Status",
    "MovedFrom_Date",
    "MovedFrom_Party_Description",
    "MovedFrom_State",
    "NameSuffix",
    "OfficialRegDate",
    "Parties_Description",
    "PlaceOfBirth",
    "Presence_Of_Children",
    "Residence_Addresses_AddressLine",
    "Residence_Addresses_ApartmentNum",
    "Residence_Addresses_ApartmentType",
    "Residence_Addresses_CassErrStatCode",
    "Residence_Addresses_CheckDigit",
    "Residence_Addresses_City",
    "Residence_Addresses_Designator",
    "Residence_Addresses_DPBC",
    "Residence_Addresses_ExtraAddressLine",
    "Residence_Addresses_HouseNumber",
    "Residence_Addresses_LatLongAccuracy",
    "Residence_Addresses_Latitude",
    "Residence_Addresses_Longitude",
    "Residence_Addresses_PrefixDirection",
    "Residence_Addresses_State",
    "Residence_Addresses_StreetName",
    "Residence_Addresses_SuffixDirection",
    "Residence_Addresses_Zip",
    "Residence_Addresses_ZipPlus4",
    "Residence_HHParties_Description",
    "Registered_Voter",
    "SequenceOddEven",
    "SequenceZigZag",
    "StateVoterID",
    "Veteran_Status",
    "VoterParties_Change_Changed_Party",
    "Voter_Status",
    "Voter_Status_UpdatedAt",
    "VoterTelephones_CellConfidenceCode",
    "VoterTelephones_CellPhoneFormatted",
    "VoterTelephones_LandlineConfidenceCode",
    "VoterTelephones_LandlineFormatted",
    # Voter Turnout
    "AnyElection_2017",
    "AnyElection_2019",
    "AnyElection_2021",
    "AnyElection_2023",
    "AnyElection_2025",
    "General_2016",
    "General_2018",
    "General_2020",
    "General_2022",
    "General_2024",
    "General_2026",
    "OtherElection_2016",
    "OtherElection_2018",
    "OtherElection_2020",
    "OtherElection_2022",
    "OtherElection_2024",
    "OtherElection_2026",
    "PresidentialPrimary_2016",
    "PresidentialPrimary_2020",
    "PresidentialPrimary_2024",
    "Primary_2016",
    "Primary_2018",
    "Primary_2020",
    "Primary_2022",
    "Primary_2024",
    "Primary_2026",
    "VotingPerformanceEvenYearGeneral",
    "VotingPerformanceEvenYearGeneralAndPrimary",
    "VotingPerformanceEvenYearPrimary",
    "VotingPerformanceMinorElection",
    # Districts
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
VOTER_UPSERT_QUERY = (
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

DISTRICT_UPSERT_QUERY = """
    INSERT INTO {db_schema}."District" (
        id,
        created_at,
        updated_at,
        type,
        name,
        state
    )
    SELECT
        id::uuid,
        created_at,
        updated_at,
        type,
        name,
        state::\"USState\"
    FROM {staging_schema}."District"
    WHERE state not in ('US') -- ignore US federal district (presidental election)
    ON CONFLICT (id) DO UPDATE SET
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        type = EXCLUDED.type,
        name = EXCLUDED.name,
        state = EXCLUDED.state
"""

DISTRICT_VOTER_UPSERT_QUERY = """
    INSERT INTO {db_schema}."DistrictVoter" (
        voter_id,
        district_id,
        created_at,
        updated_at
    )
    SELECT
        voter_id::uuid,
        district_id::uuid,
        created_at,
        updated_at
    FROM {staging_schema}."DistrictVoter"
    ON CONFLICT (voter_id, district_id) DO UPDATE SET
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at
"""


def _connect_db(
    db_name: str,
    db_user: str,
    db_pw: str,
    db_host: str,
    db_port: int,
) -> psycopg2.extensions.connection:
    """Connect to the PostgreSQL database."""
    try:
        conn: psycopg2.extensions.connection = psycopg2.connect(
            dbname=db_name, user=db_user, password=db_pw, host=db_host, port=db_port
        )
        return conn
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        raise e


def _execute_sql_query(
    query: str,
    conn: psycopg2.extensions.connection,
    return_results: bool = False,
) -> List[Tuple[Any, ...]]:
    """
    Execute a SQL query and return the results. Not that the results should be None if no results are returned.
    """
    cursor = None
    try:
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
        if cursor:
            cursor.close()

    return results


def _load_data_to_postgres(
    df: DataFrame,
    table_name: str,
    upsert_query: str,
    db_host: str,
    db_port: int,
    db_user: str,
    db_pw: str,
    db_name: str,
    staging_schema: str,
    db_schema: str,
    state_id: Optional[str] = None,
) -> int:
    """
    Load a DataFrame to PostgreSQL via JDBC and execute an upsert query.

    Args:
        df: DataFrame to load
        table_name: Name of the target table
        upsert_query: SQL query to execute for upserting data from staging to the target table
        db_host: Database host
        db_port: Database port
        db_user: Database user
        db_pw: Database password
        db_name: Database name
        staging_schema: Schema for staging tables
        db_schema: Target schema for the final tables
        state_id: Optional state ID for state-specific staging table naming

    Returns:
        Number of rows loaded
    """
    # Determine staging table name
    if state_id:
        staging_table_name = f"{table_name}_{state_id.upper()}"
        logging.info(f"Writing {state_id} data to PostgreSQL via JDBC")
    else:
        staging_table_name = table_name
        logging.info(f"Writing {table_name} data to PostgreSQL via JDBC")

    # Construct JDBC URL
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

    # connect to the database
    conn = None
    try:
        conn = _connect_db(
            db_name=db_name,
            db_user=db_user,
            db_pw=db_pw,
            db_host=db_host,
            db_port=db_port,
        )

        # make a wake up call to the database
        _execute_sql_query(
            f'SELECT * FROM {db_schema}."{table_name}" LIMIT 1;',
            conn=conn,
            return_results=False,
        )

        df.write.format("jdbc").option("url", jdbc_url).option(
            "dbtable", f'{staging_schema}."{staging_table_name}"'
        ).option("user", db_user).option("password", db_pw).option(
            "driver", "org.postgresql.Driver"
        ).mode(
            "overwrite"
        ).save()

        # Build upsert query with configuration
        config_parts = [
            "SET synchronous_commit = off;",
            "SET work_mem = '128MB';",
            "SET maintenance_work_mem = '2GB';",
            "SET max_parallel_workers_per_gather = 8;",
        ]

        # Format the upsert query based on whether we have state-specific parameters
        if state_id:
            formatted_upsert_query = upsert_query.format(
                db_schema=db_schema,
                staging_schema=staging_schema,
                table_name=table_name,
                staging_table_name=staging_table_name,
            )
        else:
            formatted_upsert_query = upsert_query.format(
                db_schema=db_schema,
                staging_schema=staging_schema,
            )

        upsert_query_w_config = (
            " ".join(config_parts) + " " + formatted_upsert_query + ";"
        )

        _execute_sql_query(
            query=upsert_query_w_config,
            conn=conn,
            return_results=False,
        )

        # turn synchronous_commit back on
        _execute_sql_query(
            query="SET synchronous_commit = on;",
            conn=conn,
            return_results=False,
        )

        return df.count()
    except Exception as e:
        logging.error(f"Error in _load_data_to_postgres: {e}")
        raise e
    finally:
        # ensure connection is always closed
        if conn:
            conn.close()


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
        enable=True,
        tags=["l2", "databricks", "people_api", "write", "weekly"],
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
    district_table: DataFrame = dbt.ref("m_people_api__district")
    district_voter_table: DataFrame = dbt.ref("m_people_api__districtvoter")

    # downsample for non-prod environment with three least populous states
    # TODO: downsample based on on dbt cloud account. current env vars listed in docs are not available
    # see https://docs.getdbt.com/docs/build/environment-variables#special-environment-variables
    filter_list = ["WY", "ND", "VT", "DC", "AK", "SD"]  # 17.6 MM rows in DistrictVoter
    if dbt_env_name != "prod":
        voter_table = voter_table.filter(col("State").isin(filter_list))
        district_voter_table = district_voter_table.filter(
            col("state").isin(filter_list)
        )

    # initialize list to capture metadata about data loads
    load_id = str(uuid4())
    load_details: List[Dict[str, Any]] = []

    # count (forces cache and preceding filters) the dataframe and enforce filter before for-loop filtering
    voter_table.count()

    # Create a staging schema if it doesn't exist
    conn = None
    try:
        conn = _connect_db(
            db_name=db_name,
            db_user=db_user,
            db_pw=db_pw,
            db_host=db_host,
            db_port=db_port,
        )
        _execute_sql_query(
            f"CREATE SCHEMA IF NOT EXISTS {staging_schema};",
            conn=conn,
            return_results=False,
        )
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        raise e
    finally:
        # ensure connection is closed after creating staging schema
        if conn:
            conn.close()

    # get the list of states to load, ordered by number of rows per state (ascending)
    state_counts = (
        voter_table.groupBy("State")
        .agg(count("*").alias("row_count"))
        .orderBy(asc("row_count"))
        .select("State")
        .collect()
    )
    state_list: List[str] = [row.State for row in state_counts]

    # load data state by state since job may time out
    for state_id in state_list:
        state_df = voter_table.filter(col("State") == state_id)

        # check for the latest updated_at for the state in the destination db
        query = f"""
            SELECT MAX(updated_at) FROM {db_schema}."Voter"
            WHERE "State" = '{state_id}'
        """
        conn = _connect_db(
            db_name=db_name,
            db_user=db_user,
            db_pw=db_pw,
            db_host=db_host,
            db_port=db_port,
        )
        max_updated_at_voter = _execute_sql_query(query, conn, return_results=True)[0][
            0
        ]
        conn.close()
        if max_updated_at_voter:
            # postgres rounds down microseconds, so add 2 seconds as a safe buffer
            max_updated_at = max_updated_at_voter + timedelta(seconds=2)  # type: ignore
            state_df = state_df.filter(
                col("updated_at") > max_updated_at
            )  # TODO: add back in after testing

            # force filter to be applied
            row_count = state_df.count()
            if row_count == 0:
                logging.info(f"No rows to load for {state_id}")
                continue  # TODO: add back in after some testing
            state_df.cache()

        num_rows_loaded = _load_data_to_postgres(
            df=state_df,
            table_name="Voter",
            upsert_query=VOTER_UPSERT_QUERY,
            db_host=db_host,
            db_port=db_port,
            db_user=db_user,
            db_pw=db_pw,
            db_name=db_name,
            staging_schema=staging_schema,
            db_schema=db_schema,
            state_id=state_id,
        )

        # add to load_details
        load_details.append(
            {
                "id": str(uuid4()),
                "load_id": load_id,
                "loaded_at": datetime.now(),
                "table": "Voter",
                "state_id": state_id,
                "num_rows_loaded": num_rows_loaded,
            }
        )

    # Get the max updated_at value from the existing data
    jdbc_props = {
        "url": f"jdbc:postgresql://{db_host}:{db_port}/{db_name}",
        "user": db_user,
        "password": db_pw,
        "driver": "org.postgresql.Driver",
    }

    # Process and load non-state-specific tables using a single loop
    table_configs = [
        ("District", district_table, DISTRICT_UPSERT_QUERY),
        ("DistrictVoter", district_voter_table, DISTRICT_VOTER_UPSERT_QUERY),
    ]

    for table_name, df, upsert_query in table_configs:
        # Get the max updated_at value from the existing data
        query = (
            f'SELECT MAX(updated_at) AS max_updated_at FROM {db_schema}."{table_name}"'
        )
        max_updated_at_df = (
            session.read.format("jdbc")
            .options(**jdbc_props)
            .option("query", query)
            .load()
        )

        # Handle empty table case
        try:
            max_updated_at = max_updated_at_df.collect()[0]["max_updated_at"]
        except IndexError:
            # Table is empty, set max_updated_at to None
            max_updated_at = None

        # Filter DataFrame if max_updated_at exists
        if max_updated_at:
            df = df.filter(df.updated_at > max_updated_at)

        # Load data to PostgreSQL
        num_rows_loaded = _load_data_to_postgres(
            df=df,
            table_name=table_name,
            upsert_query=upsert_query,
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
                "table": table_name,
                "state_id": None,
                "num_rows_loaded": num_rows_loaded,
            }
        )

    load_details_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("load_id", StringType(), True),
            StructField("loaded_at", TimestampType(), True),
            StructField("table", StringType(), True),
            StructField("state_id", StringType(), True),
            StructField("num_rows_loaded", IntegerType(), True),
        ]
    )
    load_details_df = session.createDataFrame(load_details, load_details_schema)
    return load_details_df
