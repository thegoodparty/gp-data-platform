"""Shared task wiring for the election-api mart-to-Postgres sync DAGs.

`sync_election_api` and `sync_election_api_density` differ only in their table
set and schedule; the build -> load -> index -> gate -> swap lifecycle is the
same, so it lives here and each DAG file is a `TABLES` declaration plus a call
to `wire_sync_dag`. The lifecycle itself is documented in
`dags/sync_election_api.py`; the SQL is in `election_api_utils`.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass

from airflow.sdk import Variable, task, task_group
from include.custom_functions.election_api_utils import (
    QualityGate,
    TableSyncSpec,
    apply_ddl,
    bulk_insert_from_databricks,
    create_staging_table,
    drop_old_tables,
    run_quality_checks,
    staging_columns,
    swap_staging_into_target,
)
from include.custom_functions.postgres_utils import get_postgres_via_ssh

t_log = logging.getLogger("airflow.task")

PG_CONN_ID = "election_api_db"
# Loads run on their own worker queue at low concurrency; see the DAG docstring.
LOAD_QUEUE = "election-api-sync"


def open_pg():
    """Open a Postgres connection — tunneled in cloud, direct on VPN locally.

    The bastion connection id comes from the `election_api_bastion_conn_id`
    Airflow Variable; an empty value (or unset) bypasses the tunnel.
    """
    bastion = Variable.get("election_api_bastion_conn_id", default="gp_bastion_host")
    return get_postgres_via_ssh(
        bastion_conn_id=bastion or None,
        pg_conn_id=PG_CONN_ID,
    )


@dataclass(frozen=True)
class MartSync:
    """One mart-to-table sync: everything the task-group factory needs."""

    group_id: str
    spec: TableSyncSpec
    source_model: str
    gate: QualityGate
    partition_column: str | None = None
    # Extra per-table checks run after the generic gate: (conn, spec, loaded).
    extra_checks: Callable[..., None] | None = None
    # group_ids of the tables this table's staging FKs reference; wired as
    # parent.build_indexes_and_fk >> this.build_indexes_and_fk (the FK add
    # needs the referenced staging table loaded with its PK in place).
    parents: tuple[str, ...] = ()


def build_sync_group(table: MartSync) -> dict:
    """One build->load->index->gate task group for a table's staging copy.

    Returns handles to the tasks that participate in cross-group wiring.
    """
    spec = table.spec
    handles: dict = {}

    @task_group(group_id=table.group_id)
    def group():
        @task
        def build_staging() -> None:
            with open_pg() as conn:
                create_staging_table(conn, spec)

        @task(queue=LOAD_QUEUE)
        def load_staging() -> int:
            catalog = Variable.get("databricks_catalog")
            schema = Variable.get("election_api_source_schema", default="dbt")
            with open_pg() as conn:
                # The live table's own columns drive the load: dbt must publish
                # every one of them, with matching names and types.
                columns = staging_columns(conn, spec)
                col_list = ", ".join(f"`{c}`" for c in columns)
                query = f"SELECT {col_list} " f"FROM `{catalog}`.`{schema}`.`{table.source_model}`"
                return bulk_insert_from_databricks(
                    conn,
                    spec,
                    source_query=query,
                    target_columns=columns,
                    partition_column=table.partition_column,
                )

        @task
        def build_indexes_and_fk() -> None:
            with open_pg() as conn:
                apply_ddl(conn, spec.constraint_ddl())

        @task
        def quality_checks(loaded_count: int) -> None:
            with open_pg() as conn:
                run_quality_checks(conn, spec, table.gate, loaded_count)
                if table.extra_checks:
                    table.extra_checks(conn, spec, loaded_count)

        s = build_staging()
        loaded = load_staging()
        idx = build_indexes_and_fk()
        qc = quality_checks(loaded)
        s >> loaded >> idx >> qc
        handles["build_indexes_and_fk"] = idx
        handles["quality_checks"] = qc

    group()
    return handles


def wire_sync_dag(tables: tuple[MartSync, ...], swap_gate_variable: str) -> None:
    """Build every table's group, then gate one set-wise swap behind them all.

    Call from inside a `@dag` body. `swap_gate_variable` is the DAG's own
    rehearsal switch: anything but "true" leaves staging built and unswapped.
    """
    handles = {table.group_id: build_sync_group(table) for table in tables}
    # Self-references need no edge: the PK lands in the same transaction,
    # before the FK.
    for table in tables:
        for parent in table.parents:
            handles[parent]["build_indexes_and_fk"] >> handles[table.group_id]["build_indexes_and_fk"]

    @task.short_circuit
    def cutover_enabled() -> bool:
        enabled = Variable.get(swap_gate_variable, default="false").strip().lower() == "true"
        if not enabled:
            t_log.info("Swap disabled (rehearsal mode); staging left for parity checks")
        return enabled

    @task
    def swap() -> None:
        with open_pg() as conn:
            swap_staging_into_target(conn, [table.spec for table in tables])

    @task
    def drop_old() -> None:
        with open_pg() as conn:
            drop_old_tables(conn, [table.spec for table in tables])

    gate = cutover_enabled()
    for table in tables:
        handles[table.group_id]["quality_checks"] >> gate
    gate >> swap() >> drop_old()
