"""Unit tests for the sync_election_api DAG declarations.

The declaration-consistency test guards the cross-group FK wiring: a
MartSync whose fkeys and parents disagree would break the staging FK build
in production. Mocks airflow/databricks/etc. so the file collects without
the Astro runtime installed.
"""

import sys
from unittest.mock import MagicMock, patch

# Captured before the stubbing below poisons sys.modules, so the array
# adaptation test exercises the real adapter regardless of collection order.
from psycopg2.extensions import adapt as psycopg2_adapt

# Stub external modules so the DAG file can be imported in any environment.
_STUBS = (
    "airflow",
    "airflow.decorators",
    "airflow.sdk",
    "databricks",
    "databricks.sql",
    "databricks.sql.client",
    "databricks.sdk",
    "databricks.sdk.core",
    "paramiko",
    "pendulum",
    "sshtunnel",
    "psycopg2",
    "psycopg2.extras",
)
for _mod in _STUBS:
    sys.modules[_mod] = MagicMock()

from dags.sync_election_api import TABLES, _apply_overlap_override  # noqa: E402
from include.custom_functions.election_api_utils import QualityGate  # noqa: E402


def test_parents_match_fkey_references():
    """Every non-self FK must have its referenced table's group in `parents`
    (and vice versa): a missing edge can build a staging FK before its
    referenced staging table is loaded; a stale edge points at a group that
    no longer exists."""
    table_to_group = {t.spec.target_table: t.group_id for t in TABLES}
    for t in TABLES:
        referenced_groups = {
            table_to_group[fk.ref_table] for fk in t.spec.fkeys if fk.ref_table != t.spec.target_table
        }
        assert referenced_groups == set(t.parents), t.group_id


def test_overlap_override_leaves_undeclared_floors_alone():
    """The override exists for dev rehearsals against a live table of another
    id vintage. It must not add a re-key check to a table whose ids
    legitimately re-mint (ZipToPosition, Projected_Turnout)."""
    with patch("dags.sync_election_api.Variable.get", return_value="0.5"):
        assert _apply_overlap_override(QualityGate(cold_start_floor=1)).min_id_overlap is None
        declared = QualityGate(cold_start_floor=1, min_id_overlap=0.9)
        assert _apply_overlap_override(declared).min_id_overlap == 0.5


def test_psycopg2_adapts_python_lists_to_postgres_arrays():
    """Array round-trips (Race frequency int[], Candidacy urls text[]): the
    arrow-backed connector returns numpy arrays, the loader normalizes them
    to Python lists, and those lists must adapt to ARRAY literals."""
    assert psycopg2_adapt([1, 2]).getquoted() == b"ARRAY[1,2]"
    assert psycopg2_adapt(["a", "b"]).getquoted() == b"ARRAY['a','b']"
