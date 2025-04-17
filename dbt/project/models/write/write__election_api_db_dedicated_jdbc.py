import logging
import uuid
from datetime import datetime

import psycopg2
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, date_add, date_sub

PLACE_UPSERT_QUERY = """
    INSERT INTO {db_schema}."Place" (
        id,
        created_at,
        updated_at,
        br_database_id,
        name,
        slug,
        geoid,
        mtfcc,
        state,
        city_largest,
        county_name,
        population,
        density,
        income_household_median,
        unemployment_rate,
        home_value,
        parent_id
    )
    SELECT
        id::uuid,
        created_at,
        updated_at,
        br_database_id,
        name,
        slug,
        geoid,
        mtfcc,
        state,
        city_largest,
        county_name,
        CASE WHEN population = '' THEN NULL ELSE population::integer END,
        CASE WHEN density = '' THEN NULL ELSE density::real END,
        CASE WHEN income_household_median = '' THEN NULL ELSE income_household_median::integer END,
        CASE WHEN unemployment_rate = '' THEN NULL ELSE unemployment_rate::real END,
        CASE WHEN home_value = '' THEN NULL ELSE home_value::integer END,
        parent_id::uuid
    FROM {staging_schema}."Place"
    ON CONFLICT (id) DO UPDATE SET
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        br_database_id = EXCLUDED.br_database_id,
        name = EXCLUDED.name,
        slug = EXCLUDED.slug,
        geoid = EXCLUDED.geoid,
        mtfcc = EXCLUDED.mtfcc,
        state = EXCLUDED.state,
        city_largest = EXCLUDED.city_largest,
        county_name = EXCLUDED.county_name,
        population = EXCLUDED.population,
        density = EXCLUDED.density,
        income_household_median = EXCLUDED.income_household_median,
        unemployment_rate = EXCLUDED.unemployment_rate,
        home_value = EXCLUDED.home_value,
        parent_id = EXCLUDED.parent_id
"""

RACE_UPSERT_QUERY = """
    INSERT INTO {db_schema}."Race" (
        id,
        created_at,
        updated_at,
        br_hash_id,
        br_database_id,
        election_date,
        state,
        position_level,
        normalized_position_name,
        position_description,
        filing_office_address,
        filing_phone_number,
        paperwork_instructions,
        filing_requirements,
        is_runoff,
        is_primary,
        partisan_type,
        filing_date_start,
        filing_date_end,
        employment_type,
        eligibility_requirements,
        salary,
        sub_area_name,
        sub_area_value,
        frequency,
        place_id,
        slug,
        position_names
    )
    SELECT
        id::uuid,
        created_at,
        updated_at,
        br_hash_id,
        br_database_id,
        election_date,
        state,
        position_level::\"PositionLevel\",
        normalized_position_name,
        position_description,
        filing_office_address,
        filing_phone_number,
        paperwork_instructions,
        filing_requirements,
        is_runoff,
        is_primary,
        partisan_type,
        filing_date_start,
        filing_date_end,
        employment_type,
        eligibility_requirements,
        salary,
        sub_area_name,
        sub_area_value,
        frequency,
        place_id::uuid,
        slug,
        position_names
    FROM {staging_schema}."Race"
    ON CONFLICT (id) DO UPDATE SET
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        br_hash_id = EXCLUDED.br_hash_id,
        br_database_id = EXCLUDED.br_database_id,
        election_date = EXCLUDED.election_date,
        state = EXCLUDED.state,
        position_level = EXCLUDED.position_level,
        normalized_position_name = EXCLUDED.normalized_position_name,
        position_description = EXCLUDED.position_description,
        filing_office_address = EXCLUDED.filing_office_address,
        filing_phone_number = EXCLUDED.filing_phone_number,
        paperwork_instructions = EXCLUDED.paperwork_instructions,
        filing_requirements = EXCLUDED.filing_requirements,
        is_runoff = EXCLUDED.is_runoff,
        is_primary = EXCLUDED.is_primary,
        partisan_type = EXCLUDED.partisan_type,
        filing_date_start = EXCLUDED.filing_date_start,
        filing_date_end = EXCLUDED.filing_date_end,
        employment_type = EXCLUDED.employment_type,
        eligibility_requirements = EXCLUDED.eligibility_requirements,
        salary = EXCLUDED.salary,
        sub_area_name = EXCLUDED.sub_area_name,
        sub_area_value = EXCLUDED.sub_area_value,
        frequency = EXCLUDED.frequency,
        place_id = EXCLUDED.place_id,
        slug = EXCLUDED.slug,
        position_names = EXCLUDED.position_names
"""


def _execute_sql_query(
    query: str, host: str, port: int, user: str, password: str, database: str
) -> None:

    try:
        conn = psycopg2.connect(
            dbname=database, user=user, password=password, host=host, port=port
        )
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        logging.error(f"Error executing query: {query}")
        logging.error(f"Error: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()


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
) -> int:
    """
    Load a DataFrame to PostgreSQL via JDBC and execute an upsert query.

    Args:
        df: DataFrame to load
        table_name: Name of the table
        upsert_query: SQL query to execute for upserting data from staging to the target table
        db_host: Database host
        db_port: Database port
        db_user: Database user
        db_pw: Database password
        db_name: Database name
        staging_schema: Schema for staging tables
        db_schema: Target schema for the final tables
    """
    logging.info(f"Writing {table_name} data to PostgreSQL via JDBC")

    # Construct JDBC URL
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

    # Write data directly using JDBC
    df.write.format("jdbc").option("url", jdbc_url).option(
        "dbtable", f'{staging_schema}."{table_name}"'
    ).option("user", db_user).option("password", db_pw).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "overwrite"
    ).save()

    # Execute the upsert query
    _execute_sql_query(
        upsert_query.format(db_schema=db_schema, staging_schema=staging_schema),
        db_host,
        db_port,
        db_user,
        db_pw,
        db_name,
    )

    return df.count()


def model(dbt, session) -> DataFrame:
    """
    This model loads data for the mart that services the election API. The tables are written
    to the postgres database directly from spark since Airbyte does not support reads from
    databricks. We use JDBC drivers, which requires a dedicated compute instance rather than the default serverless.
    """
    # configure the data model
    dbt.config(
        submission_method="all_purpose_cluster",  # required to write with jdbc
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",  # required to write with jdbc
        materialized="incremental",
        incremental_strategy="append",
        unique_key="id",
        on_schema_change="fail",
        tags=["ballotready", "election_api", "write", "postgres"],
    )

    # get db configs
    staging_schema = dbt.config.get("staging_schema")
    dbt_environment = dbt.config.get("dbt_environment")
    db_host = dbt.config.get("election_db_host")
    db_port = int(dbt.config.get("election_db_port"))
    db_user = dbt.config.get("election_db_user")
    db_pw = dbt.config.get("election_db_pw")
    db_name = dbt.config.get("election_db_name")
    db_schema = dbt.config.get("election_db_schema")

    # TODO: disable prod loads until dev testing is complete and prod db is deployed
    if dbt_environment == "prod":
        logging.info("Skipping load for prod environment")
        # Create an empty DataFrame with the same schema
        columns = ["id", "table_name", "number_of_rows", "loaded_at"]
        return session.createDataFrame([], schema=columns)

    # get the data to write
    place_df: DataFrame = dbt.ref("m_election_api__place")
    race_df: DataFrame = dbt.ref("m_election_api__race")

    # Implement incremental logic if this is an incremental run
    if dbt.is_incremental:
        # Get the max updated_at value from the existing data
        jdbc_props = {
            "url": f"jdbc:postgresql://{db_host}:{db_port}/{db_name}",
            "user": db_user,
            "password": db_pw,
            "driver": "org.postgresql.Driver",
        }

        # Get the latest timestamp for places
        place_query = (
            f'SELECT MAX(updated_at) AS max_updated_at FROM {db_schema}."Place"'
        )
        place_max_df = (
            session.read.format("jdbc")
            .options(**jdbc_props)
            .option("query", place_query)
            .load()
        )
        place_max_updated_at = place_max_df.collect()[0]["max_updated_at"]

        # Get the latest timestamp for races
        race_query = f'SELECT MAX(updated_at) AS max_updated_at FROM {db_schema}."Race"'
        race_max_df = (
            session.read.format("jdbc")
            .options(**jdbc_props)
            .option("query", race_query)
            .load()
        )
        race_max_updated_at = race_max_df.collect()[0]["max_updated_at"]

        # Filter dataframes to only include new or updated records
        if place_max_updated_at:
            logging.info(
                f"Filtering place data for records updated after {place_max_updated_at}"
            )
            place_df = place_df.filter(place_df.updated_at > place_max_updated_at)

        if race_max_updated_at:
            logging.info(
                f"Filtering race data for records updated after {race_max_updated_at}"
            )
            race_df = race_df.filter(race_df.updated_at > race_max_updated_at)

        logging.info(
            f"Incremental load: {place_df.count()} place records and {race_df.count()} race records to process"
        )

    # Create a staging schema if it doesn't exist
    _execute_sql_query(
        f"CREATE SCHEMA IF NOT EXISTS {staging_schema};",
        db_host,
        db_port,
        db_user,
        db_pw,
        db_name,
    )

    # Load Place and Race data using the utility function
    place_count = _load_data_to_postgres(
        df=place_df,
        table_name="Place",
        upsert_query=PLACE_UPSERT_QUERY,
        db_host=db_host,
        db_port=db_port,
        db_user=db_user,
        db_pw=db_pw,
        db_name=db_name,
        staging_schema=staging_schema,
        db_schema=db_schema,
    )

    # for race, we need to drop rows that have `election_date` more than 1 day ago, and more than 2 years from now
    race_df = race_df.filter(
        (race_df.election_date > date_sub(current_date(), 1))
        & (race_df.election_date < date_add(current_date(), 2 * 365))
    )

    race_count = _load_data_to_postgres(
        df=race_df,
        table_name="Race",
        upsert_query=RACE_UPSERT_QUERY,
        db_host=db_host,
        db_port=db_port,
        db_user=db_user,
        db_pw=db_pw,
        db_name=db_name,
        staging_schema=staging_schema,
        db_schema=db_schema,
    )

    # for the race db, drop rows that have `election_date` more than 1 day ago
    _execute_sql_query(
        f"DELETE FROM {db_schema}.\"Race\" WHERE election_date < current_date - interval '1 day'",
        db_host,
        db_port,
        db_user,
        db_pw,
        db_name,
    )

    # log the loading information including the number of rows loaded
    columns = ["id", "table_name", "number_of_rows", "loaded_at"]
    data = [
        (
            str(uuid.uuid4()),
            "m_election_api__place",
            place_count,
            datetime.now(),
        ),
        (
            str(uuid.uuid4()),
            "m_election_api__race",
            race_count,
            datetime.now(),
        ),
    ]
    load_log_df = session.createDataFrame(data, columns)

    return load_log_df
