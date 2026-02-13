import logging
from typing import Any, List, Tuple

import psycopg2
import psycopg2.extensions

logger = logging.getLogger("airflow.task")


def connect_db(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
) -> psycopg2.extensions.connection:
    """
    Connect to a PostgreSQL database.

    Mirrors the pattern from dbt/project/models/write/write__people_api_db.py.
    """
    try:
        conn = psycopg2.connect(
            dbname=database, user=user, password=password, host=host, port=port
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise


def execute_query(
    conn: psycopg2.extensions.connection,
    query: str,
    params: Any = None,
    return_results: bool = False,
) -> List[Tuple[Any, ...]]:
    """
    Execute a SQL query with optional parameters and return results.

    Mirrors the pattern from dbt/project/models/write/write__people_api_db.py.
    """
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        if return_results:
            results = cursor.fetchall() if cursor.rowcount > 0 else []
        else:
            results = []
        conn.commit()
    except Exception as e:
        logger.error(f"Error executing query: {query}")
        logger.error(f"Error: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

    return results


def delete_expired_voters(
    conn: psycopg2.extensions.connection,
    schema: str,
    lalvoterids: List[str],
    batch_size: int = 1000,
) -> dict:
    """
    Delete expired voters from People-API PostgreSQL in the correct order:
      1. DistrictVoter (references Voter via voter_id FK)
      2. Voter (by LALVOTERID)

    Processes in batches to avoid query size limits. Uses parameterized queries
    for safety.

    Returns:
        {"district_voter_deleted": int, "voter_deleted": int}
    """
    total_district_voter_deleted = 0
    total_voter_deleted = 0

    for i in range(0, len(lalvoterids), batch_size):
        batch = lalvoterids[i : i + batch_size]
        logger.info(
            f"Processing batch {i // batch_size + 1} "
            f"({len(batch)} LALVOTERIDs, {i + 1}-{i + len(batch)} of {len(lalvoterids)})"
        )

        # Step 1: Delete from DistrictVoter using voter_id subquery
        dv_query = f"""
            DELETE FROM {schema}."DistrictVoter"
            WHERE voter_id IN (
                SELECT id FROM {schema}."Voter"
                WHERE "LALVOTERID" = ANY(%s)
            )
        """
        cursor = conn.cursor()
        try:
            cursor.execute(dv_query, (batch,))
            dv_count = cursor.rowcount
            total_district_voter_deleted += dv_count
            logger.info(f"  Deleted {dv_count} DistrictVoter rows")
        finally:
            cursor.close()

        # Step 2: Delete from Voter
        v_query = f"""
            DELETE FROM {schema}."Voter"
            WHERE "LALVOTERID" = ANY(%s)
        """
        cursor = conn.cursor()
        try:
            cursor.execute(v_query, (batch,))
            v_count = cursor.rowcount
            total_voter_deleted += v_count
            logger.info(f"  Deleted {v_count} Voter rows")
        finally:
            cursor.close()

        conn.commit()

    logger.info(
        f"Total deleted: {total_district_voter_deleted} DistrictVoter rows, "
        f"{total_voter_deleted} Voter rows"
    )
    return {
        "district_voter_deleted": total_district_voter_deleted,
        "voter_deleted": total_voter_deleted,
    }
