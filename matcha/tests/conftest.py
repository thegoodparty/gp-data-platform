"""Shared fixtures for the test suite."""

import os
import uuid
from pathlib import Path

import pandas as pd
import pytest

DUMMY_CSV = Path(__file__).parent / "dummy_data.csv"
DUMMY_CSV_EO = Path(__file__).parent / "dummy_data_elected.csv"

# Databricks catalog used for ephemeral test schemas.
# Override via DATABRICKS_TEST_CATALOG env var.
_TEST_CATALOG = os.environ.get("DATABRICKS_TEST_CATALOG", "goodparty_data_catalog")


@pytest.fixture(scope="session")
def databricks_schema():
    """Create an ephemeral Databricks schema for the test session, then drop it.

    Yields the fully-qualified ``catalog.schema`` prefix that tests can use
    to build table FQNs.

    Requires a live Databricks connection (DATABRICKS_HTTP_PATH must be set).
    """
    if not os.environ.get("DATABRICKS_HTTP_PATH"):
        pytest.skip("DATABRICKS_HTTP_PATH not set")

    from scripts.databricks_io import get_connection

    schema_name = f"_test_{uuid.uuid4().hex[:8]}"
    fq_schema = f"{_TEST_CATALOG}.{schema_name}"

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"CREATE SCHEMA `{_TEST_CATALOG}`.`{schema_name}`")
        print(f"\nCreated test schema: {fq_schema}")
    finally:
        cursor.close()
        conn.close()

    yield fq_schema

    # Teardown: drop the entire schema and all tables within it
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"DROP SCHEMA `{_TEST_CATALOG}`.`{schema_name}` CASCADE")
        print(f"\nDropped test schema: {fq_schema}")
    finally:
        cursor.close()
        conn.close()


@pytest.fixture(scope="session")
def databricks_input_table(databricks_schema):
    """Upload the dummy CSV as a Databricks table and return its FQN."""
    from scripts.databricks_io import write_table

    fqn = f"{databricks_schema}.input_prematch"
    df = pd.read_csv(DUMMY_CSV, dtype=str)
    write_table(df, fqn, overwrite=True)
    return fqn


@pytest.fixture()
def databricks_tables(databricks_schema, databricks_input_table, tmp_path):
    """Provide a dict of FQNs and a local output dir for a single test run.

    Each test gets unique output table names (via a short UUID suffix) so
    tests don't collide if run in parallel.
    """
    suffix = uuid.uuid4().hex[:6]
    return {
        "input_fqn": databricks_input_table,
        "output_cluster_fqn": f"{databricks_schema}.clustered_{suffix}",
        "output_pairwise_fqn": f"{databricks_schema}.pairwise_{suffix}",
        "output_dir": tmp_path,
    }


@pytest.fixture(scope="session")
def databricks_input_table_eo(databricks_schema):
    """Upload the elected officials dummy CSV as a Databricks table."""
    from scripts.databricks_io import write_table

    fqn = f"{databricks_schema}.input_prematch_eo"
    df = pd.read_csv(DUMMY_CSV_EO, dtype=str)
    write_table(df, fqn, overwrite=True)
    return fqn


@pytest.fixture()
def databricks_tables_eo(databricks_schema, databricks_input_table_eo, tmp_path):
    """EO-specific test table FQNs."""
    suffix = uuid.uuid4().hex[:6]
    return {
        "input_fqn": databricks_input_table_eo,
        "output_cluster_fqn": f"{databricks_schema}.clustered_eo_{suffix}",
        "output_pairwise_fqn": f"{databricks_schema}.pairwise_eo_{suffix}",
        "output_dir": tmp_path,
    }
