import logging
import uuid
from datetime import datetime
from typing import Dict

import psycopg2
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, date_add, date_sub
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CANDIDACY_UPSERT_QUERY = """
    INSERT INTO {db_schema}."Candidacy" (
        id,
        created_at,
        updated_at,
        br_database_id,
        slug,
        first_name,
        last_name,
        party,
        place_name,
        state,
        image,
        about,
        urls,
        election_frequency,
        salary,
        normalized_position_name,
        position_name,
        position_description,
        race_id
    )
    SELECT
        id::uuid,
        created_at,
        updated_at,
        br_database_id,
        slug,
        first_name,
        last_name,
        party,
        place_name,
        state,
        image,
        about,
        urls,
        election_frequency,
        salary,
        normalized_position_name,
        position_name,
        position_description,
        race_id::uuid
    FROM {staging_schema}."Candidacy"
    ON CONFLICT (id) DO UPDATE SET
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        br_database_id = EXCLUDED.br_database_id,
        slug = EXCLUDED.slug,
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        party = EXCLUDED.party,
        place_name = EXCLUDED.place_name,
        state = EXCLUDED.state,
        image = EXCLUDED.image,
        about = EXCLUDED.about,
        urls = EXCLUDED.urls,
        election_frequency = EXCLUDED.election_frequency,
        salary = EXCLUDED.salary,
        normalized_position_name = EXCLUDED.normalized_position_name,
        position_name = EXCLUDED.position_name,
        position_description = EXCLUDED.position_description,
        race_id = EXCLUDED.race_id
"""

DISTRICT_UPSERT_QUERY = """
    INSERT INTO {db_schema}."District" (
        id,
        created_at,
        updated_at,
        state,
        l2_district_type,
        l2_district_name
    )
    SELECT
        id::uuid,
        created_at,
        updated_at,
        state,
        l2_district_type,
        l2_district_name
    from {staging_schema}."District"
    ON CONFLICT (id) DO UPDATE SET
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        state = EXCLUDED.state,
        l2_district_type = EXCLUDED.l2_district_type,
        l2_district_name = EXCLUDED.l2_district_name
    """

ISSUE_UPSERT_QUERY = """
    INSERT INTO {db_schema}."Issue" (
        id,
        created_at,
        updated_at,
        br_database_id,
        expanded_text,
        key,
        name,
        parent_id
    )
    SELECT
        id::uuid,
        created_at,
        updated_at,
        br_database_id,
        expanded_text,
        key,
        name,
        parent_id::uuid
    FROM {staging_schema}."Issue"
    ON CONFLICT (id) DO UPDATE SET
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        br_database_id = EXCLUDED.br_database_id,
        expanded_text = EXCLUDED.expanded_text,
        key = EXCLUDED.key,
        name = EXCLUDED.name,
        parent_id = EXCLUDED.parent_id
"""

PROJECTED_TURNOUT_UPSERT_QUERY = """
    INSERT INTO {db_schema}."Projected_Turnout" (
        id,
        created_at,
        updated_at,
        election_year,
        election_code,
        projected_turnout,
        inference_at,
        model_version,
        district_id
    )
    SELECT
        id::uuid,
        created_at,
        updated_at,
        election_year,
        election_code::\"ElectionCode\",
        projected_turnout,
        inference_at,
        model_version,
        district_id
    from {staging_schema}."Projected_Turnout"
    ON CONFLICT (id) DO UPDATE SET
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        election_year = EXCLUDED.election_year,
        election_code = EXCLUDED.election_code,
        projected_turnout = EXCLUDED.projected_turnout,
        inference_at = EXCLUDED.inference_at,
        model_version = EXCLUDED.model_version,
        district_id = EXCLUDED.district_id
    """

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
        position_geoid,
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
        position_geoid::text,
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
        position_geoid = EXCLUDED.position_geoid,
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

STANCE_UPSERT_QUERY = """
    INSERT INTO {db_schema}."Stance" (
        id,
        created_at,
        updated_at,
        br_database_id,
        stance_locale,
        stance_reference_url,
        stance_statement,
        issue_id,
        candidacy_id
    )
    SELECT
        id::uuid,
        created_at,
        updated_at,
        br_database_id,
        stance_locale,
        stance_reference_url,
        stance_statement,
        issue_id::uuid,
        candidacy_id::uuid
    FROM {staging_schema}."Stance"
    ON CONFLICT (id) DO UPDATE SET
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        br_database_id = EXCLUDED.br_database_id,
        stance_locale = EXCLUDED.stance_locale,
        stance_reference_url = EXCLUDED.stance_reference_url,
        stance_statement = EXCLUDED.stance_statement,
        issue_id = EXCLUDED.issue_id,
        candidacy_id = EXCLUDED.candidacy_id
"""

WRITE_TABLE_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("number_of_rows", IntegerType(), False),
        StructField("loaded_at", TimestampType(), False),
    ]
)


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

    # dedup on slugs if they have different ids:
    if table_name in ["Candidacy", "Place"]:
        dedup_query = f"""
        with
            dupes as (
                select tbl_pub.id as pub_id, tbl_stg.id as stg_id
                from {db_schema}."{table_name}" as tbl_pub
                left join {staging_schema}."{table_name}" as tbl_stg
                    on tbl_pub.slug = tbl_stg.slug
                    and tbl_pub.id != tbl_stg.id::uuid
            )
            delete from {db_schema}."{table_name}"
            where id in (select pub_id from dupes where stg_id is not null)
        """
        _execute_sql_query(
            dedup_query,
            db_host,
            db_port,
            db_user,
            db_pw,
            db_name,
        )

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
    db_host = dbt.config.get("election_db_host")
    db_port = int(dbt.config.get("election_db_port"))
    db_user = dbt.config.get("election_db_user")
    db_pw = dbt.config.get("election_db_pw")
    db_name = dbt.config.get("election_db_name")
    db_schema = dbt.config.get("election_db_schema")

    # get the data to write
    candidacy_df: DataFrame = dbt.ref("m_election_api__candidacy")
    issue_df: DataFrame = dbt.ref("m_election_api__issue")
    place_df: DataFrame = dbt.ref("m_election_api__place")
    race_df: DataFrame = dbt.ref("m_election_api__race")
    stance_df: DataFrame = dbt.ref("m_election_api__stance")
    district_df: DataFrame = dbt.ref("m_election_api__district")
    projected_turnout_df: DataFrame = dbt.ref("m_election_api__projected_turnout")

    # filter the race dataframe to only include races that are within 1 day of the current date and 2 years from the current date
    race_df = race_df.filter(
        (race_df.election_date > date_sub(current_date(), 1))
        & (race_df.election_date < date_add(current_date(), 2 * 365))
    )

    # Implement incremental logic if this is an incremental run
    if dbt.is_incremental:
        # Get the max updated_at value from the existing data
        jdbc_props = {
            "url": f"jdbc:postgresql://{db_host}:{db_port}/{db_name}",
            "user": db_user,
            "password": db_pw,
            "driver": "org.postgresql.Driver",
        }

        # get the latest timestamp for each table and filter the dataframes to only include new or updated records
        max_updated_at = {}
        to_load = zip(
            [
                "Candidacy",
                "Issue",
                "Place",
                "Race",
                "Stance",
                "District",
                "Projected_Turnout",
            ],
            [
                candidacy_df,
                issue_df,
                place_df,
                race_df,
                stance_df,
                district_df,
                projected_turnout_df,
            ],
        )
        for table, df in to_load:
            query = (
                f'SELECT MAX(updated_at) AS max_updated_at FROM {db_schema}."{table}"'
            )
            max_updated_at_df = (
                session.read.format("jdbc")
                .options(**jdbc_props)
                .option("query", query)
                .load()
            )
            max_updated_at[table] = max_updated_at_df.collect()[0]["max_updated_at"]

            if max_updated_at[table]:
                df = df.filter(df.updated_at > max_updated_at[table])

    # Create a staging schema if it doesn't exist
    _execute_sql_query(
        f"CREATE SCHEMA IF NOT EXISTS {staging_schema};",
        db_host,
        db_port,
        db_user,
        db_pw,
        db_name,
    )

    # Load tables to postgres. The ordering of the tables is important to satisfy
    # foreign key constraints.
    table_load_counts: Dict[str, int] = {}
    for table_name, df, upsert_query in zip(
        [
            "Place",
            "Race",
            "Candidacy",
            "Issue",
            "Stance",
            "District",
            "Projected_Turnout",
        ],
        [
            place_df,
            race_df,
            candidacy_df,
            issue_df,
            stance_df,
            district_df,
            projected_turnout_df,
        ],
        [
            PLACE_UPSERT_QUERY,
            RACE_UPSERT_QUERY,
            CANDIDACY_UPSERT_QUERY,
            ISSUE_UPSERT_QUERY,
            STANCE_UPSERT_QUERY,
            DISTRICT_UPSERT_QUERY,
            PROJECTED_TURNOUT_UPSERT_QUERY,
        ],
    ):
        table_load_counts[table_name] = _load_data_to_postgres(
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

    # for the race db, drop rows that have `election_date` more than 1 day ago
    _execute_sql_query(
        f"DELETE FROM {db_schema}.\"Race\" WHERE election_date < current_date - interval '1 day'",
        db_host,
        db_port,
        db_user,
        db_pw,
        db_name,
    )

    data = []
    for table_name, count in table_load_counts.items():
        data.append(
            (
                str(uuid.uuid4()),
                f"m_election_api___{table_name}".lower(),
                count,
                datetime.now(),
            )
        )
    load_log_df = session.createDataFrame(data, schema=WRITE_TABLE_SCHEMA)

    return load_log_df
