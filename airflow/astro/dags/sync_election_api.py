"""
## Sync m_election_api__zip_to_position to election-api PostgreSQL

Reads the dbt mart from Databricks and refreshes the
`public."ZipToPosition"` table in the election-api Postgres database
using a build-and-swap pattern:

1. **build_staging_table** — drop & recreate `staging."ZipToPosition_new"`
   with `LIKE public."ZipToPosition" INCLUDING DEFAULTS` (no indexes).
2. **load_staging_from_databricks** — stream the mart into staging in 5k-row
   batches; synthesize `id` (deterministic UUIDv5 over
   `zip_code|position_id|election_date`) and `updated_at` (now).
3. **build_indexes_and_fk** — add PK, the three indexes, and the FK to
   `Position` on the staging table.
4. **quality_checks** — gate the swap on row-count and state-coverage floors.
5. **swap** — atomic rename-swap inside a single Postgres transaction:
   `public."ZipToPosition"` → `public."ZipToPosition_old"` and
   `staging."ZipToPosition_new"` → `public."ZipToPosition"`. Indexes and the
   FK constraint are renamed back to their canonical names so the live shape
   matches the Prisma migration exactly.
6. **drop_old** — drop `public."ZipToPosition_old"` after the swap commits.

This is the prototype for migrating the legacy
`dbt/project/models/write/write__election_api_db.py` (which writes 8 election
tables to Postgres via JDBC from a Databricks cluster) onto Airflow. See the
PR description for the follow-up generalization plan.

### Connections (set in Astro Environment Manager):
- `databricks` / `databricks_dev` (Generic) — Databricks OAuth M2M.
- `gp_bastion_host` (SSH) — bastion for tunneling to PostgreSQL.
- `election_api_db` (Postgres) — election-api database credentials. **NEW** —
  must be created per-deployment before this DAG can run.

### Variables (set in Astro Environment Manager):
- `databricks_conn_id` — selects Databricks connection
  (e.g., `databricks_dev` in dev, `databricks` in prod).
- `databricks_catalog` — Databricks catalog name (e.g., `goodparty_data_catalog`).

The source schema is hardcoded to `dbt` (not `databricks_dbt_schema`, which
points at `dbt_staging` for in-flight dbt build artifacts). The election-api
sync reads the production-quality version of the mart in both dev and prod.
"""

import logging
import uuid
from datetime import datetime, timezone

import psycopg2.extras
from include.custom_functions.databricks_utils import read_databricks_table
from include.custom_functions.postgres_utils import get_postgres_via_ssh
from pendulum import datetime as pendulum_datetime
from pendulum import duration

from airflow.sdk import Variable, dag, task

t_log = logging.getLogger("airflow.task")

PG_CONN_ID = "election_api_db"
TARGET_SCHEMA = "public"
STAGING_SCHEMA = "staging"
TARGET_TABLE = "ZipToPosition"
NEW_TABLE = "ZipToPosition_new"
OLD_TABLE = "ZipToPosition_old"

SOURCE_COLUMNS = [
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
TARGET_COLUMNS = ["id", "updated_at", *SOURCE_COLUMNS]

# Stable namespace so uuid5 produces the same id across runs. Arbitrary fixed
# value — only its stability matters (changing it would re-key every row).
_UUID_NAMESPACE = uuid.UUID("0a3f9b2c-7d1e-4c5a-9e8d-1f7e6a4c2b30")


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

    @task
    def build_staging_table() -> None:
        """Drop and recreate staging.ZipToPosition_new without indexes."""
        with get_postgres_via_ssh(pg_conn_id=PG_CONN_ID) as conn:
            cur = conn.cursor()
            try:
                cur.execute(f'DROP TABLE IF EXISTS "{STAGING_SCHEMA}"."{NEW_TABLE}"')
                cur.execute(
                    f'CREATE TABLE "{STAGING_SCHEMA}"."{NEW_TABLE}" '
                    f'(LIKE "{TARGET_SCHEMA}"."{TARGET_TABLE}" INCLUDING DEFAULTS)'
                )
                conn.commit()
                t_log.info(
                    "Created %s.%s (LIKE %s.%s INCLUDING DEFAULTS)",
                    STAGING_SCHEMA,
                    NEW_TABLE,
                    TARGET_SCHEMA,
                    TARGET_TABLE,
                )
            finally:
                cur.close()

    @task
    def load_staging_from_databricks() -> int:
        """Stream the mart from Databricks into staging.ZipToPosition_new."""
        catalog = Variable.get("databricks_catalog")
        # Read from `dbt`, not the `databricks_dbt_schema` variable (which
        # points at `dbt_staging` for in-flight dbt build artifacts).
        databricks_schema = "dbt"
        batch_size = 5000

        col_list = ", ".join(SOURCE_COLUMNS)
        query = (
            f"SELECT {col_list} "
            f"FROM `{catalog}`.`{databricks_schema}`.`m_election_api__zip_to_position`"
        )
        t_log.info("Reading from Databricks: %s", query)
        _col_names, batches = read_databricks_table(query, batch_size=batch_size)

        target_col_list = ", ".join(f'"{c}"' for c in TARGET_COLUMNS)
        insert_sql = (
            f'INSERT INTO "{STAGING_SCHEMA}"."{NEW_TABLE}" '
            f"({target_col_list}) VALUES %s"
        )
        now = datetime.now(timezone.utc)

        total = 0
        try:
            with get_postgres_via_ssh(pg_conn_id=PG_CONN_ID) as conn:
                cur = conn.cursor()
                try:
                    for batch in batches:
                        rows = []
                        for row in batch:
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
                            rows.append(
                                (
                                    str(row_id),
                                    now,
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
                            )
                        if not rows:
                            continue
                        psycopg2.extras.execute_values(
                            cur, insert_sql, rows, page_size=batch_size
                        )
                        total += len(rows)
                        if total % 50_000 < batch_size:
                            t_log.info("Inserted %d rows so far", total)
                    conn.commit()
                finally:
                    cur.close()
        finally:
            batches.close()

        t_log.info("Loaded %d rows into %s.%s", total, STAGING_SCHEMA, NEW_TABLE)
        return total

    @task
    def build_indexes_and_fk() -> None:
        """Add PK, indexes, and FK to Position on the staging table."""
        statements = [
            (
                f'ALTER TABLE "{STAGING_SCHEMA}"."{NEW_TABLE}" '
                f'ADD CONSTRAINT "{NEW_TABLE}_pkey" PRIMARY KEY (id)'
            ),
            (
                f'CREATE INDEX "{NEW_TABLE}_zip_code_idx" '
                f'ON "{STAGING_SCHEMA}"."{NEW_TABLE}" (zip_code)'
            ),
            (
                f'CREATE INDEX "{NEW_TABLE}_position_id_idx" '
                f'ON "{STAGING_SCHEMA}"."{NEW_TABLE}" (position_id)'
            ),
            (
                f'CREATE UNIQUE INDEX "{NEW_TABLE}_unique" '
                f'ON "{STAGING_SCHEMA}"."{NEW_TABLE}" '
                f"(zip_code, position_id, election_date) NULLS NOT DISTINCT"
            ),
            (
                f'ALTER TABLE "{STAGING_SCHEMA}"."{NEW_TABLE}" '
                f'ADD CONSTRAINT "{NEW_TABLE}_position_id_fkey" '
                f"FOREIGN KEY (position_id) "
                f'REFERENCES "{TARGET_SCHEMA}"."Position"(id) '
                f"ON UPDATE CASCADE ON DELETE RESTRICT"
            ),
        ]
        with get_postgres_via_ssh(pg_conn_id=PG_CONN_ID) as conn:
            cur = conn.cursor()
            try:
                for stmt in statements:
                    cur.execute(stmt)
                conn.commit()
                t_log.info("Built indexes + FK on %s.%s", STAGING_SCHEMA, NEW_TABLE)
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()

    @task
    def quality_checks(loaded_count: int) -> None:
        """Gate the swap on row-count and coverage floors."""
        if loaded_count < 1_000:
            raise ValueError(
                f"Only {loaded_count} rows loaded (<1000) — refusing to swap"
            )

        with get_postgres_via_ssh(pg_conn_id=PG_CONN_ID) as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    f"SELECT COUNT(DISTINCT state) "
                    f'FROM "{STAGING_SCHEMA}"."{NEW_TABLE}"'
                )
                distinct_states = cur.fetchone()[0]
                if distinct_states < 30:
                    raise ValueError(
                        f"Only {distinct_states} distinct states "
                        f"(<30) — refusing to swap"
                    )

                cur.execute(
                    f"SELECT MIN(election_date), MAX(election_date) "
                    f'FROM "{STAGING_SCHEMA}"."{NEW_TABLE}"'
                )
                min_date, max_date = cur.fetchone()
                t_log.info(
                    "Quality checks passed: %d rows, %d distinct states, "
                    "election_date range %s..%s",
                    loaded_count,
                    distinct_states,
                    min_date,
                    max_date,
                )
            finally:
                cur.close()

    @task
    def swap() -> None:
        """Atomic rename-swap of the new table into public."""
        with get_postgres_via_ssh(pg_conn_id=PG_CONN_ID) as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT 1 FROM pg_tables "
                    "WHERE schemaname = %s AND tablename = %s",
                    (TARGET_SCHEMA, TARGET_TABLE),
                )
                target_exists = cur.fetchone() is not None

                statements = []
                if target_exists:
                    statements += [
                        f'ALTER TABLE "{TARGET_SCHEMA}"."{TARGET_TABLE}" '
                        f'RENAME TO "{OLD_TABLE}"',
                        f'ALTER INDEX "{TARGET_SCHEMA}"."{TARGET_TABLE}_pkey" '
                        f'RENAME TO "{OLD_TABLE}_pkey"',
                        f'ALTER INDEX "{TARGET_SCHEMA}"."{TARGET_TABLE}_zip_code_idx" '
                        f'RENAME TO "{OLD_TABLE}_zip_code_idx"',
                        f'ALTER INDEX "{TARGET_SCHEMA}"."{TARGET_TABLE}_position_id_idx" '
                        f'RENAME TO "{OLD_TABLE}_position_id_idx"',
                        f'ALTER INDEX "{TARGET_SCHEMA}".'
                        f'"{TARGET_TABLE}_zip_code_position_id_election_date_key" '
                        f'RENAME TO "{OLD_TABLE}_unique"',
                        f'ALTER TABLE "{TARGET_SCHEMA}"."{OLD_TABLE}" '
                        f'RENAME CONSTRAINT "{TARGET_TABLE}_position_id_fkey" '
                        f'TO "{OLD_TABLE}_position_id_fkey"',
                    ]
                statements += [
                    f'ALTER TABLE "{STAGING_SCHEMA}"."{NEW_TABLE}" '
                    f'SET SCHEMA "{TARGET_SCHEMA}"',
                    f'ALTER TABLE "{TARGET_SCHEMA}"."{NEW_TABLE}" '
                    f'RENAME TO "{TARGET_TABLE}"',
                    f'ALTER INDEX "{TARGET_SCHEMA}"."{NEW_TABLE}_pkey" '
                    f'RENAME TO "{TARGET_TABLE}_pkey"',
                    f'ALTER INDEX "{TARGET_SCHEMA}"."{NEW_TABLE}_zip_code_idx" '
                    f'RENAME TO "{TARGET_TABLE}_zip_code_idx"',
                    f'ALTER INDEX "{TARGET_SCHEMA}"."{NEW_TABLE}_position_id_idx" '
                    f'RENAME TO "{TARGET_TABLE}_position_id_idx"',
                    f'ALTER INDEX "{TARGET_SCHEMA}"."{NEW_TABLE}_unique" '
                    f"RENAME TO "
                    f'"{TARGET_TABLE}_zip_code_position_id_election_date_key"',
                    f'ALTER TABLE "{TARGET_SCHEMA}"."{TARGET_TABLE}" '
                    f'RENAME CONSTRAINT "{NEW_TABLE}_position_id_fkey" '
                    f'TO "{TARGET_TABLE}_position_id_fkey"',
                ]

                for stmt in statements:
                    cur.execute(stmt)
                conn.commit()
                t_log.info("Swap complete (target_existed=%s)", target_exists)
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()

    @task
    def drop_old() -> None:
        """Drop the renamed-aside old table after the swap."""
        with get_postgres_via_ssh(pg_conn_id=PG_CONN_ID) as conn:
            cur = conn.cursor()
            try:
                cur.execute(f'DROP TABLE IF EXISTS "{TARGET_SCHEMA}"."{OLD_TABLE}"')
                conn.commit()
                t_log.info("Dropped %s.%s if it existed", TARGET_SCHEMA, OLD_TABLE)
            finally:
                cur.close()

    staging = build_staging_table()
    loaded = load_staging_from_databricks()
    indexed = build_indexes_and_fk()
    checked = quality_checks(loaded)
    swapped = swap()
    dropped = drop_old()

    staging >> loaded >> indexed >> checked >> swapped >> dropped


sync_election_api()
