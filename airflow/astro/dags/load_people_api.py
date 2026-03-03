"""
## Load People-API Tables to PostgreSQL

Reads dbt models from Databricks and upserts rows into the people-api
PostgreSQL database via an SSH tunnel through the bastion host.

### Tables Loaded:
1. **load_districts** — `m_people_api__district` → `"District"`
   (parent table, must run first due to foreign key constraints)
2. **load_district_stats** — `m_people_api__districtstats` → `"DistrictStats"`
   (has FK to District)

### Connections (set in Astro Environment Manager):
- `databricks` / `databricks_dev` (Generic) — Databricks OAuth M2M credentials
- `gp_bastion_host` (SSH) — bastion host for tunneling to PostgreSQL
- `people_api_db` (Postgres) — people-api database credentials

### Variables (set in Astro Environment Manager):
- `databricks_conn_id` — selects Databricks connection
  (e.g., `databricks_dev` in dev, `databricks` in prod)
- `databricks_dbt_schema` — Databricks schema where dbt models live
  (e.g., `dbt` in prod, `dbt_staging` in dev)
- `people_api_schema` — PostgreSQL schema name for people-api tables
"""

import json
import logging

from include.custom_functions.postgres_utils import (
    get_postgres_via_ssh,
    read_databricks_table,
    upsert_rows,
)
from pendulum import datetime, duration
from psycopg2.extras import Json

from airflow.sdk import Variable, dag, task

t_log = logging.getLogger("airflow.task")

DATABRICKS_CATALOG = "goodparty_data_catalog"

# The dbt SQL model defines struct fields as lowercase, but the API expects
# camelCase. Remap the two affected keys on the way into PostgreSQL.
_BUCKET_KEY_MAP = {
    "presenceofchildren": "presenceOfChildren",
    "estimatedincomerange": "estimatedIncomeRange",
}

DISTRICT_COLUMNS = [
    "id",
    "created_at",
    "updated_at",
    "type",
    "name",
    "state",
]

DISTRICT_STATS_COLUMNS = [
    "district_id",
    "updated_at",
    "total_constituents",
    "total_constituents_with_cell_phone",
    "buckets",
]


@dag(
    start_date=datetime(2026, 3, 2),
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
    tags=["people_api", "postgres"],
    is_paused_upon_creation=True,
)
def load_people_api():

    @task
    def load_districts():
        """Read District from Databricks and upsert into PostgreSQL.

        Loads all districts except federal-level (state='US').
        Must complete before DistrictStats due to foreign key constraint.
        """
        schema = Variable.get("databricks_dbt_schema", default="dbt")
        batch_size = 5000

        query = (
            f"SELECT id, created_at, updated_at, type, name, state "
            f"FROM `{DATABRICKS_CATALOG}`.`{schema}`.`m_people_api__district` "
            f"WHERE state != 'US'"
        )

        t_log.info("Reading from Databricks: %s", query)
        _col_names, row_iter = read_databricks_table(query, fetch_size=batch_size)

        pg_schema = Variable.get("people_api_schema")

        with get_postgres_via_ssh() as conn:
            total = 0
            batch = []
            for row in row_iter:
                batch.append(tuple(row))
                if len(batch) >= batch_size:
                    total += upsert_rows(
                        conn=conn,
                        schema=pg_schema,
                        table="District",
                        columns=DISTRICT_COLUMNS,
                        conflict_columns=["id"],
                        rows=batch,
                    )
                    batch = []

            if batch:
                total += upsert_rows(
                    conn=conn,
                    schema=pg_schema,
                    table="District",
                    columns=DISTRICT_COLUMNS,
                    conflict_columns=["id"],
                    rows=batch,
                )

        t_log.info("Upserted %d District rows to PostgreSQL", total)

    @task
    def load_district_stats():
        """Read DistrictStats from Databricks and upsert into PostgreSQL.

        Streams rows in batches to stay within the Astro worker memory limit.
        """
        schema = Variable.get("databricks_dbt_schema", default="dbt")
        batch_size = 5000

        query = (
            f"SELECT district_id, updated_at, total_constituents, "
            f"total_constituents_with_cell_phone, to_json(buckets) AS buckets "
            f"FROM `{DATABRICKS_CATALOG}`.`{schema}`.`m_people_api__districtstats`"
        )

        t_log.info("Reading from Databricks: %s", query)
        _col_names, row_iter = read_databricks_table(query, fetch_size=batch_size)

        pg_schema = Variable.get("people_api_schema")

        with get_postgres_via_ssh() as conn:
            total = 0
            batch = []
            for row in row_iter:
                batch.append(
                    (
                        row[0],  # district_id
                        row[1],  # updated_at
                        row[2],  # total_constituents
                        row[3],  # total_constituents_with_cell_phone
                        Json(  # buckets — parse and fix camelCase keys
                            {
                                _BUCKET_KEY_MAP.get(k, k): v
                                for k, v in json.loads(row[4]).items()
                            }
                        ),
                    )
                )
                if len(batch) >= batch_size:
                    total += upsert_rows(
                        conn=conn,
                        schema=pg_schema,
                        table="DistrictStats",
                        columns=DISTRICT_STATS_COLUMNS,
                        conflict_columns=["district_id"],
                        rows=batch,
                    )
                    batch = []

            if batch:
                total += upsert_rows(
                    conn=conn,
                    schema=pg_schema,
                    table="DistrictStats",
                    columns=DISTRICT_STATS_COLUMNS,
                    conflict_columns=["district_id"],
                    rows=batch,
                )

        t_log.info("Upserted %d DistrictStats rows to PostgreSQL", total)

    load_districts() >> load_district_stats()


load_people_api()
