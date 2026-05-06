"""
## Sync election-api marts from Databricks to PostgreSQL

Builds and atomically swaps election-api Postgres tables from their dbt marts
in Databricks. Each table is its own task group following the same lifecycle:

1. **build_staging** — drop & recreate `staging."<Table>_new"` LIKE the live
   target (no indexes — fast bulk-insert).
2. **load_staging** — stream the source mart from Databricks into staging.
3. **build_indexes_and_fk** — add PK, indexes, and FK constraints.
4. **quality_checks** — gate the swap on row-count / coverage floors.
5. **swap** — atomic rename swap (`_new` -> live, live -> `_old`) with all
   indexes and constraints renamed to canonical Prisma names.
6. **drop_old** — drop the renamed-aside `_old` table.

The shared lifecycle lives in
`include/custom_functions/election_api_utils.py`. Each task group below is
a thin per-table wrapper that supplies the source query, transform, and
constraint DDL.

This is the prototype for migrating the legacy
`dbt/project/models/write/write__election_api_db.py` (which writes 8 election
tables to Postgres via JDBC from a Databricks cluster) onto Airflow.

### Connections (set in Astro Environment Manager):
- `databricks` / `databricks_dev` (Generic) — Databricks OAuth M2M.
- `gp_bastion_host` (SSH) — bastion for tunneling to PostgreSQL.
- `election_api_db` (Postgres) — election-api database credentials.

### Variables (set in Astro Environment Manager):
- `databricks_conn_id` — selects Databricks connection
  (e.g., `databricks_dev` in dev, `databricks` in prod).
- `databricks_catalog` — Databricks catalog name (e.g., `goodparty_data_catalog`).
- `election_api_bastion_conn_id` (optional) — SSH bastion to tunnel through.
  Defaults to `gp_bastion_host`. Set to an empty string for local dev on VPN
  where the Postgres host is reachable directly.

The source schema is hardcoded to `dbt` (not `databricks_dbt_schema`, which
points at `dbt_staging` for in-flight dbt build artifacts). The election-api
sync reads the production-quality version of the marts in both dev and prod.
"""

import logging
import uuid
from datetime import datetime, timezone

from include.custom_functions.election_api_utils import (
    TableSyncSpec,
    apply_ddl,
    bulk_insert_from_databricks,
    create_staging_table,
    drop_old_table,
    swap_staging_into_target,
)
from include.custom_functions.postgres_utils import get_postgres_via_ssh
from pendulum import datetime as pendulum_datetime
from pendulum import duration

from airflow.sdk import Variable, dag, task, task_group

t_log = logging.getLogger("airflow.task")

PG_CONN_ID = "election_api_db"
DATABRICKS_SCHEMA = "dbt"  # canonical mart location (not dbt_staging)


def _open_pg():
    """Open a Postgres connection — tunneled in cloud, direct on VPN locally.

    The bastion connection id comes from the `election_api_bastion_conn_id`
    Airflow Variable; an empty value (or unset) bypasses the tunnel.
    """
    bastion = Variable.get(
        "election_api_bastion_conn_id", default_var="gp_bastion_host"
    )
    return get_postgres_via_ssh(
        bastion_conn_id=bastion or None,
        pg_conn_id=PG_CONN_ID,
    )


# ---------------------------------------------------------------------------
# ZipToPosition
# ---------------------------------------------------------------------------

ZTP = TableSyncSpec(
    target_table="ZipToPosition",
    indexes=(
        "ZipToPosition_zip_code_idx",
        "ZipToPosition_position_id_idx",
        "ZipToPosition_zip_code_position_id_election_date_key",
    ),
    fkeys=("ZipToPosition_position_id_fkey",),
)

ZTP_SOURCE_COLUMNS = [
    "position_id",
    "name",
    "br_database_id",
    "zip_code",
    "election_year",
    "election_date",
    "display_office_level",
    "office_type",
    "state",
    "district",
]
ZTP_TARGET_COLUMNS = ["id", "updated_at", *ZTP_SOURCE_COLUMNS]

# Stable namespace so uuid5 produces the same id across runs.
_UUID_NAMESPACE = uuid.UUID("0a3f9b2c-7d1e-4c5a-9e8d-1f7e6a4c2b30")


def _ztp_transform_row(row: tuple) -> tuple:
    (
        position_id,
        name,
        br_database_id,
        zip_code,
        election_year,
        election_date,
        display_office_level,
        office_type,
        state,
        district,
    ) = row
    row_id = uuid.uuid5(
        _UUID_NAMESPACE,
        f"{zip_code}|{position_id}|{election_date}",
    )
    return (
        str(row_id),
        datetime.now(timezone.utc),
        position_id,
        name,
        br_database_id,
        zip_code,
        election_year,
        election_date,
        display_office_level,
        office_type,
        state,
        district,
    )


# ---------------------------------------------------------------------------
# DistrictTopIssue
# ---------------------------------------------------------------------------

DTI = TableSyncSpec(
    target_table="DistrictTopIssue",
    indexes=("DistrictTopIssue_district_id_issue_key",),
    fkeys=("DistrictTopIssue_district_id_fkey",),
)

# Postgres dropped the denormalized l2_* columns; we read only the seven
# columns that map 1:1 onto the live shape.
DTI_COLUMNS = [
    "id",
    "updated_at",
    "district_id",
    "issue",
    "issue_label",
    "score",
    "issue_rank",
]


def _dti_constraint_ddl() -> list[str]:
    sn, nt = DTI.staging_schema, DTI.new_table
    target_schema = DTI.target_schema
    return [
        (
            f'ALTER TABLE "{sn}"."{nt}" '
            f'ADD CONSTRAINT "{DTI.stage_name(DTI.pk_name)}" PRIMARY KEY (id)'
        ),
        (
            f"CREATE UNIQUE INDEX "
            f'"{DTI.stage_name("DistrictTopIssue_district_id_issue_key")}" '
            f'ON "{sn}"."{nt}" (district_id, issue)'
        ),
        (
            f'ALTER TABLE "{sn}"."{nt}" '
            f'ADD CONSTRAINT "{DTI.stage_name("DistrictTopIssue_district_id_fkey")}" '
            f"FOREIGN KEY (district_id) "
            f'REFERENCES "{target_schema}"."District"(id) '
            f"ON UPDATE CASCADE ON DELETE RESTRICT"
        ),
    ]


def _ztp_constraint_ddl() -> list[str]:
    sn, nt = ZTP.staging_schema, ZTP.new_table
    target_schema = ZTP.target_schema
    return [
        (
            f'ALTER TABLE "{sn}"."{nt}" '
            f'ADD CONSTRAINT "{ZTP.stage_name(ZTP.pk_name)}" PRIMARY KEY (id)'
        ),
        (
            f'CREATE INDEX "{ZTP.stage_name("ZipToPosition_zip_code_idx")}" '
            f'ON "{sn}"."{nt}" (zip_code)'
        ),
        (
            f'CREATE INDEX "{ZTP.stage_name("ZipToPosition_position_id_idx")}" '
            f'ON "{sn}"."{nt}" (position_id)'
        ),
        (
            f"CREATE UNIQUE INDEX "
            f'"{ZTP.stage_name("ZipToPosition_zip_code_position_id_election_date_key")}" '
            f'ON "{sn}"."{nt}" '
            f"(zip_code, position_id, election_date) NULLS NOT DISTINCT"
        ),
        (
            f'ALTER TABLE "{sn}"."{nt}" '
            f'ADD CONSTRAINT "{ZTP.stage_name("ZipToPosition_position_id_fkey")}" '
            f"FOREIGN KEY (position_id) "
            f'REFERENCES "{target_schema}"."Position"(id) '
            f"ON UPDATE CASCADE ON DELETE RESTRICT"
        ),
    ]


@dag(
    start_date=pendulum_datetime(2026, 5, 5, tz="UTC"),
    schedule="@daily",
    max_consecutive_failed_dag_runs=5,
    max_active_runs=1,
    doc_md=__doc__,
    catchup=False,
    default_args={
        "owner": "Data Engineering Team",
        "retries": 3,
        "retry_delay": duration(seconds=30),
    },
    tags=["election_api", "postgres"],
    is_paused_upon_creation=True,
)
def sync_election_api():

    @task_group(group_id="zip_to_position")
    def zip_to_position():

        @task
        def build_staging() -> None:
            with _open_pg() as conn:
                create_staging_table(conn, ZTP)

        @task
        def load_staging() -> int:
            catalog = Variable.get("databricks_catalog")
            col_list = ", ".join(ZTP_SOURCE_COLUMNS)
            # TEMP (DATA-1867): prod mart has 9 dupe (zip_code, position_id,
            # election_date) triples and stale row counts pending a
            # full-refresh rebuild. Dedupe + LIMIT for fast end-to-end
            # iteration. Revert to the plain SELECT after prod is rebuilt
            # and the dbt unique test passes.
            query = (
                f"SELECT {col_list} "
                f"FROM `{catalog}`.`{DATABRICKS_SCHEMA}`."
                f"`m_election_api__zip_to_position` "
                f"QUALIFY row_number() OVER ("
                f"  PARTITION BY zip_code, position_id, election_date "
                f"  ORDER BY display_office_level"
                f") = 1 "
                f"LIMIT 5000"
            )
            with _open_pg() as conn:
                return bulk_insert_from_databricks(
                    conn,
                    ZTP,
                    source_query=query,
                    target_columns=ZTP_TARGET_COLUMNS,
                    transform_row=_ztp_transform_row,
                )

        @task
        def build_indexes_and_fk() -> None:
            with _open_pg() as conn:
                apply_ddl(conn, _ztp_constraint_ddl())

        @task
        def quality_checks(loaded_count: int) -> None:
            # TEMP (DATA-1867): floors lowered for the LIMIT 5000 dev sample.
            # Restore to >= 1_000 / >= 30 once we're loading the full mart.
            if loaded_count < 100:
                raise ValueError(
                    f"Only {loaded_count} rows loaded (<100) — refusing to swap"
                )
            with _open_pg() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(
                        f"SELECT COUNT(DISTINCT state) "
                        f'FROM "{ZTP.staging_schema}"."{ZTP.new_table}"'
                    )
                    distinct_states = cur.fetchone()[0]
                    if distinct_states < 1:
                        raise ValueError(
                            f"Only {distinct_states} distinct states "
                            f"— refusing to swap"
                        )
                    cur.execute(
                        f"SELECT MIN(election_date), MAX(election_date) "
                        f'FROM "{ZTP.staging_schema}"."{ZTP.new_table}"'
                    )
                    min_date, max_date = cur.fetchone()
                    t_log.info(
                        "Quality checks passed: %d rows, %d states, %s..%s",
                        loaded_count,
                        distinct_states,
                        min_date,
                        max_date,
                    )
                finally:
                    cur.close()

        @task
        def swap() -> None:
            with _open_pg() as conn:
                swap_staging_into_target(conn, ZTP)

        @task
        def drop_old() -> None:
            with _open_pg() as conn:
                drop_old_table(conn, ZTP)

        s = build_staging()
        loaded = load_staging()
        idx = build_indexes_and_fk()
        qc = quality_checks(loaded)
        sw = swap()
        do = drop_old()
        s >> loaded >> idx >> qc >> sw >> do

    @task_group(group_id="district_top_issues")
    def district_top_issues():

        @task
        def build_staging() -> None:
            with _open_pg() as conn:
                create_staging_table(conn, DTI)

        @task
        def load_staging() -> int:
            catalog = Variable.get("databricks_catalog")
            col_list = ", ".join(DTI_COLUMNS)
            query = (
                f"SELECT {col_list} "
                f"FROM `{catalog}`.`{DATABRICKS_SCHEMA}`."
                f"`m_election_api__district_top_issues`"
            )
            with _open_pg() as conn:
                return bulk_insert_from_databricks(
                    conn,
                    DTI,
                    source_query=query,
                    target_columns=DTI_COLUMNS,
                )

        @task
        def build_indexes_and_fk() -> None:
            with _open_pg() as conn:
                apply_ddl(conn, _dti_constraint_ddl())

        @task
        def quality_checks(loaded_count: int) -> None:
            if loaded_count < 1_000:
                raise ValueError(
                    f"Only {loaded_count} rows loaded (<1000) — refusing to swap"
                )
            with _open_pg() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(
                        f"SELECT COUNT(DISTINCT issue) "
                        f'FROM "{DTI.staging_schema}"."{DTI.new_table}"'
                    )
                    distinct_issues = cur.fetchone()[0]
                    if distinct_issues < 10:
                        raise ValueError(
                            f"Only {distinct_issues} distinct issues "
                            f"(<10) — refusing to swap"
                        )
                    cur.execute(
                        f"SELECT MIN(issue_rank), MAX(issue_rank) "
                        f'FROM "{DTI.staging_schema}"."{DTI.new_table}"'
                    )
                    min_rank, max_rank = cur.fetchone()
                    t_log.info(
                        "Quality checks passed: %d rows, %d distinct issues, "
                        "issue_rank range %s..%s",
                        loaded_count,
                        distinct_issues,
                        min_rank,
                        max_rank,
                    )
                finally:
                    cur.close()

        @task
        def swap() -> None:
            with _open_pg() as conn:
                swap_staging_into_target(conn, DTI)

        @task
        def drop_old() -> None:
            with _open_pg() as conn:
                drop_old_table(conn, DTI)

        s = build_staging()
        loaded = load_staging()
        idx = build_indexes_and_fk()
        qc = quality_checks(loaded)
        sw = swap()
        do = drop_old()
        s >> loaded >> idx >> qc >> sw >> do

    zip_to_position()
    district_top_issues()


sync_election_api()
