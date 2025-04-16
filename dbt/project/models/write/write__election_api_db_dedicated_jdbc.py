import logging
import uuid
from datetime import datetime

import psycopg2
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, concat_ws, lit, when

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
):

    try:
        conn = psycopg2.connect(
            dbname=database, user=user, password=password, host=host, port=port
        )
        cursor = conn.cursor()
        # cursor.execute("CREATE SCHEMA IF NOT EXISTS databricks_staging;")
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        logging.error(f"Error executing query: {query}")
        logging.error(f"Error: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()


# Function to transform array columns to PostgreSQL compatible format
def _prepare_df_for_postgres(df):
    """
    Transforms array columns in a DataFrame to PostgreSQL compatible format.

    Examples:
        - Input array column: ["apple", "banana", "cherry"]
            Output: "{apple,banana,cherry}"

        - Input array column: [1, 2, 3]
            Output: "{1,2,3}"

        - Input array column: None or []
            Output: NULL
    """
    # Copy the DataFrame to avoid changing the original
    transformed_df = df

    # Get the schema to identify array columns
    schema = df.schema
    for field in schema:
        # Check if the field is an array type
        if "array" in str(field.dataType).lower():
            # Convert array to PostgreSQL array format {val1,val2,val3}
            transformed_df = transformed_df.withColumn(
                field.name, concat_ws(",", col(field.name)).cast("string")
            ).withColumn(
                field.name,
                when(
                    col(field.name).isNotNull(),
                    concat(lit("{"), col(field.name), lit("}")),
                ),
            )

    return transformed_df


def model(dbt, session) -> DataFrame:
    """
    This model loads data for the mart that services the election API. The tables are written
    to the postgres database directly from spark since Airbyte does not support reads from
    databricks. We use JDBC through an SSH tunnel, which requires a dedicated compute instance
    rather than the default serverless.
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
        # TODO: once serverless supports library installations on jobs, uncomment the following line
        # packages=["paramiko", "sshtunnel"],
        # enabled=False,
    )

    # Add barrier execution mode to ensure operations happen on the driver
    session.conf.set("spark.databricks.barrier.mode", "true")

    # get db and ssh tunnel config
    db_host = dbt.config.get("election_db_host")
    db_port = int(dbt.config.get("election_db_port"))
    db_user = dbt.config.get("election_db_user")
    db_pw = dbt.config.get("election_db_pw")
    db_name = dbt.config.get("election_db_name")
    db_schema = dbt.config.get("election_db_schema")
    # TODO: remove ssh tunnel config, delete variables from dbt env and write.yaml
    # ssh_host = dbt.config.get("ssh_host")  # type: ignore
    # ssh_port = int(dbt.config.get("ssh_port"))  # type: ignore
    # ssh_username = dbt.config.get("ssh_user")  # type: ignore
    # ssh_pk_1 = dbt.config.get("ssh_pk_1")  # type: ignore
    # ssh_pk_2 = dbt.config.get("ssh_pk_2")  # type: ignore
    # ssh_pk_3 = dbt.config.get("ssh_pk_3")  # type: ignore

    try:
        # get the data to write
        place_df: DataFrame = dbt.ref("m_election_api__place")
        race_df: DataFrame = dbt.ref("m_election_api__race")

        # TODO: separate place_df having `plarent_id` and without. May require separate loading to satisfy
        # foreign key constraints.

        # Prepare DataFrames for PostgreSQL compatible format
        # place_df_prepared = _prepare_df_for_postgres(place_df)
        # race_df_prepared = _prepare_df_for_postgres(race_df)

        # JDBC connection properties using the explicit IP
        jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

        # Write the DataFrames to PostgreSQL using JDBC
        logging.info("Writing place data to PostgreSQL via JDBC")

        # Write place data directly using JDBC
        # First, create the schema if it doesn't exist
        # Create a temporary DataFrame with a SQL query to create the schema
        staging_schema = "databricks_staging"  # TODO: pass this in as an environment variable from the dbt project
        _execute_sql_query(
            f"CREATE SCHEMA IF NOT EXISTS {staging_schema};",
            db_host,
            db_port,
            db_user,
            db_pw,
            db_name,
        )
        place_df.write.format("jdbc").option("url", jdbc_url).option(
            "dbtable", f'{staging_schema}."Place"'
        ).option("user", db_user).option("password", db_pw).option(
            "driver", "org.postgresql.Driver"
        ).mode(
            "overwrite"
        ).save()

        # execute the `Place`` upsert query
        _execute_sql_query(
            PLACE_UPSERT_QUERY.format(
                db_schema=db_schema, staging_schema=staging_schema
            ),
            db_host,
            db_port,
            db_user,
            db_pw,
            db_name,
        )

        logging.info("Writing race data to PostgreSQL via JDBC")
        race_df.write.format("jdbc").option("url", jdbc_url).option(
            "dbtable", f"{staging_schema}.Race"
        ).option("user", db_user).option("password", db_pw).option(
            "driver", "org.postgresql.Driver"
        ).mode(
            "overwrite"
        ).save()

        # execute the `Race` upsert query
        _execute_sql_query(
            RACE_UPSERT_QUERY.format(
                db_schema=db_schema, staging_schema=staging_schema
            ),
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
                place_df.count(),
                datetime.now(),
            ),
            (
                str(uuid.uuid4()),
                "m_election_api__race",
                race_df.count(),
                datetime.now(),
            ),
        ]
        load_log_df = session.createDataFrame(data, columns)

    except Exception as e:
        logging.error(f"Error: {e}")
        raise e
    # finally:
    # Clean up resources

    # delete the staging schema
    # _execute_sql_query(f"DROP SCHEMA IF EXISTS {staging_schema} CASCADE;", db_host, db_port, db_user, db_pw, db_name)

    # if tunnel is not None and tunnel.is_active:
    #     try:
    #         tunnel.stop()
    #         logging.info("SSH tunnel stopped successfully")
    #     except Exception as e:
    #         logging.error(f"Error stopping SSH tunnel: {e}")
    # if ssh_key_file_path and os.path.exists(ssh_key_file_path):
    #     os.remove(ssh_key_file_path)
    #     logging.info("SSH key file removed")

    return load_log_df
