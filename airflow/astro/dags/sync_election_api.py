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
    bastion = Variable.get("election_api_bastion_conn_id", default="gp_bastion_host")
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
        "ZipToPosition_zip_code_pct_districtzip_to_zip_idx",
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
    "voters_in_zip",
    "voters_in_zip_district",
    "pct_districtzip_to_zip",
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
        voters_in_zip,
        voters_in_zip_district,
        pct_districtzip_to_zip,
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
        voters_in_zip,
        voters_in_zip_district,
        pct_districtzip_to_zip,
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

# DATA-1903 phase (a) only: the v3 mart now scores 68 issues per district
# (up from 27), so taking the mart's top-10 by overall issue_rank would
# surface the 41 newly-introduced issues (Aliens, Border Wall, federal-only
# noise) ahead of the locally-actionable issues the API serves today. To
# preserve pre-DATA-1903 API behavior while the election-api team's Prisma
# migration is in flight, the load_staging query filters the mart to this
# legacy 27-issue universe, recomputes issue_rank within the filtered set,
# and takes top-10. Drop V2_LEGACY_ISSUES and the legacy_top_10 CTE in
# phase (c) once the four jurisdictional flag columns are live in Postgres
# and the API can do its own filter-then-rerank by flag.
V2_LEGACY_ISSUES = (
    "hs_abortion_pro_choice",
    "hs_affordable_housing_gov_has_role",
    "hs_casino_support",
    "hs_charter_schools_support",
    "hs_civil_liberties_support",
    "hs_climate_change_believer",
    "hs_death_penalty_support",
    "hs_dei_support",
    "hs_econ_anxiety_very_worried",
    "hs_gun_control_support",
    "hs_immigration_undesirable",
    "hs_income_inequality_serious",
    "hs_marijuana_legal_support",
    "hs_medicaid_expansion_support",
    "hs_medicare_for_all_support",
    "hs_min_wage_15_increase_support",
    "hs_pipeline_fracking_support",
    "hs_police_trust_yes",
    "hs_public_transit_support",
    "hs_regulations_too_harsh",
    "hs_same_sex_marriage_support",
    "hs_school_choice_support",
    "hs_school_funding_more",
    "hs_tax_cuts_support",
    "hs_trump_tariffs_support",
    "hs_unions_beneficial",
    "hs_violent_crime_very_worried",
)


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
            f"CREATE INDEX "
            f'"{ZTP.stage_name("ZipToPosition_zip_code_pct_districtzip_to_zip_idx")}" '
            f'ON "{sn}"."{nt}" (zip_code, pct_districtzip_to_zip)'
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
            query = (
                f"SELECT {col_list} "
                f"FROM `{catalog}`.`{DATABRICKS_SCHEMA}`."
                f"`m_election_api__zip_to_position`"
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
            if loaded_count < 1_000:
                raise ValueError(
                    f"Only {loaded_count} rows loaded (<1000) — refusing to swap"
                )
            with _open_pg() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(
                        f"SELECT COUNT(DISTINCT state) "
                        f'FROM "{ZTP.staging_schema}"."{ZTP.new_table}"'
                    )
                    distinct_states = cur.fetchone()[0]
                    if distinct_states < 30:
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
            # TEMP (DATA-1903 phase a): the v3 mart ranks all 68 issues per
            # district. Postgres still has the pre-DATA-1903 7-column shape
            # and the API has no jurisdictional-flag filter yet. To preserve
            # current API behavior, filter the mart to the 27 legacy issues,
            # recompute issue_rank within that subset, and take top-10. The
            # entire legacy_top_10 CTE and V2_LEGACY_ISSUES are removed in
            # phase (c) once Patrick's Prisma migration adds the four
            # jurisdictional flag columns to DistrictTopIssue and DTI_COLUMNS
            # is extended to sync them.
            legacy_in_clause = ", ".join(f"'{i}'" for i in V2_LEGACY_ISSUES)
            # `new_rank` (not `issue_rank`) inside the CTE — the source mart
            # has its own `issue_rank` column, and Databricks resolves a
            # QUALIFY clause against the source column instead of the SELECT
            # alias when they share a name, which would silently filter to
            # the mart's 1..68 ranks (~3 legacy issues per district), not
            # the recomputed 1..N within the filtered subset. Outer SELECT
            # renames `new_rank` back to `issue_rank` to match DTI_COLUMNS.
            query = (
                f"WITH legacy_top_10 AS ("
                f"  SELECT"
                f"    id, updated_at, district_id, issue, issue_label, score,"
                f"    row_number() OVER ("
                f"      PARTITION BY district_id "
                f"      ORDER BY score DESC, issue ASC"
                f"    ) AS new_rank"
                f"  FROM `{catalog}`.`{DATABRICKS_SCHEMA}`."
                f"`m_election_api__district_top_issues`"
                f"  WHERE issue IN ({legacy_in_clause})"
                f"  QUALIFY new_rank <= 10"
                f") "
                f"SELECT"
                f"  id, updated_at, district_id, issue, issue_label, score,"
                f"  new_rank AS issue_rank "
                f"FROM legacy_top_10"
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
