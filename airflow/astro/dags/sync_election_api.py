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

# Eleven columns: seven from the pre-DATA-1903 shape plus the four
# jurisdictional flags added by election-api PR #164.
DTI_COLUMNS = [
    "id",
    "updated_at",
    "district_id",
    "issue",
    "issue_label",
    "score",
    "is_local",
    "is_regional",
    "is_state",
    "is_federal",
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
            # Shape-aware: compare staging to the prior live table so the
            # thresholds auto-track the table's natural size as it grows.
            # Catches catastrophic regressions (e.g., a partial Databricks
            # load that drops most rows) without hardcoding numbers that
            # will drift as the seed grows or shrinks.
            with _open_pg() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(
                        f"SELECT "
                        f"COUNT(DISTINCT issue), "
                        f"MIN(issue_rank), MAX(issue_rank), "
                        f"COUNT(*) FILTER ("
                        f"  WHERE is_local IS NULL OR is_regional IS NULL"
                        f"  OR is_state IS NULL OR is_federal IS NULL) "
                        f'FROM "{DTI.staging_schema}"."{DTI.new_table}"'
                    )
                    distinct_issues, min_rank, max_rank, null_flag_rows = cur.fetchone()

                    # Prior live state (may be absent on a true cold start).
                    # Both identifiers must be double-quoted in the regclass
                    # argument — Postgres folds unquoted mixed-case to lowercase,
                    # so `public.DistrictTopIssue` would resolve to
                    # `public.districttopissue` and always return NULL.
                    cur.execute(
                        "SELECT to_regclass(%s)",
                        (f'"{DTI.target_schema}"."{DTI.target_table}"',),
                    )
                    prior_exists = cur.fetchone()[0] is not None
                    if prior_exists:
                        cur.execute(
                            f"SELECT COUNT(*), COUNT(DISTINCT issue) "
                            f'FROM "{DTI.target_schema}"."{DTI.target_table}"'
                        )
                        prior_count, prior_issues = cur.fetchone()
                    else:
                        prior_count, prior_issues = 0, 0

                    # Row count: refuse on a >50% drop vs prior live;
                    # absolute floor only used on cold start. High-ratio
                    # syncs (>3x prior) log a warning but do not refuse —
                    # the DATA-1903 phase-c cutover is intentionally ~6.8x
                    # (970K legacy top-10 rows -> 6.6M full-mart rows), so
                    # blocking on growth would self-DOS the cutover. After
                    # cutover the ratio stabilizes near 1.0; future >3x
                    # excursions are worth on-call attention.
                    if prior_count > 0:
                        ratio = loaded_count / prior_count
                        if ratio < 0.5:
                            raise ValueError(
                                f"Loaded {loaded_count} rows, prior live had "
                                f"{prior_count} (ratio {ratio:.2f}) "
                                f"— refusing to swap"
                            )
                        if ratio > 3:
                            t_log.warning(
                                "High-ratio sync: loaded %d rows, prior live "
                                "had %d (ratio %.2f). Sometimes intentional "
                                "(e.g., DATA-1903 phase-c cutover) — sometimes "
                                "a JOIN fan-out or duplication bug. Verify "
                                "the source mart's grain before trusting.",
                                loaded_count,
                                prior_count,
                                ratio,
                            )
                    elif loaded_count < 100_000:
                        raise ValueError(
                            f"Cold-start load of {loaded_count} rows is "
                            f"implausibly small — refusing to swap"
                        )

                    # Distinct issues: stay at-or-above prior. Allows growth
                    # (e.g., seed adds new issues) and catches regression.
                    if distinct_issues < prior_issues:
                        raise ValueError(
                            f"Staging has {distinct_issues} distinct issues, "
                            f"prior live had {prior_issues} — refusing to swap"
                        )

                    # Flag integrity: every row should have all four flags
                    # populated. The mart's LEFT JOIN to haystaq_issue_tags
                    # only emits NULLs on drift, which the dbt-side tests
                    # catch — this check is a belt-and-suspenders gate.
                    if null_flag_rows > 0:
                        raise ValueError(
                            f"{null_flag_rows} staging rows have a NULL "
                            f"jurisdictional flag — refusing to swap"
                        )

                    t_log.info(
                        "Quality checks passed: %d rows (prior %d), "
                        "%d distinct issues (prior %d), "
                        "issue_rank %s..%s, all flags populated",
                        loaded_count,
                        prior_count,
                        distinct_issues,
                        prior_issues,
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
