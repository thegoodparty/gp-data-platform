"""Shared fixtures for entity resolution tests."""

import os
import sys
import uuid
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

_DUMMY_DATA_CSV = Path(__file__).resolve().parent / "dummy_data.csv"


def _build_dummy_data() -> pd.DataFrame:
    """Load ~18 dummy records across 2 sources with 3 known match pairs."""
    df = pd.read_csv(_DUMMY_DATA_CSV, dtype=str)
    df = df.replace({"": None})
    return df


@pytest.fixture(scope="module")
def databricks_tables(tmp_path_factory):
    """Upload dummy data to Databricks, yield table names, tear down after."""
    test_schema = os.environ.get("DATABRICKS_TEST_SCHEMA")
    if not test_schema:
        pytest.skip("DATABRICKS_TEST_SCHEMA not set")

    from databricks_io import get_connection, write_table

    suffix = uuid.uuid4().hex[:8]
    input_fqn = f"{test_schema}.er_test_input_{suffix}"
    output_fqn = f"{test_schema}.er_test_output_{suffix}"
    pairwise_fqn = f"{test_schema}.er_test_pairwise_{suffix}"

    input_df = _build_dummy_data()
    write_table(input_df, input_fqn, overwrite=True)

    output_dir = tmp_path_factory.mktemp("er_integration")

    yield {
        "input_fqn": input_fqn,
        "output_fqn": output_fqn,
        "pairwise_fqn": pairwise_fqn,
        "output_dir": output_dir,
        "input_df": input_df,
    }

    # Teardown: drop all test tables
    try:
        conn = get_connection()
        cursor = conn.cursor()
        for fqn in [input_fqn, output_fqn, pairwise_fqn]:
            try:
                parts = fqn.split(".")
                cursor.execute(
                    f"DROP TABLE IF EXISTS `{parts[0]}`.`{parts[1]}`.`{parts[2]}`"
                )
            except Exception as e:
                print(f"Warning: failed to drop {fqn}: {e}")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Warning: teardown connection failed: {e}")
