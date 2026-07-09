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
`dbt/project/models/write/write__election_api_db.py` (which writes the other
election tables to Postgres via JDBC from a Databricks cluster) onto Airflow.
Projected_Turnout is the first table cut over from that writer: upsert-by-id
delivery can never delete, so model-version supersessions and L2 coverage
drift stranded stale rows in the API (DATA-2015). The swap replaces the
table wholesale each run, so the API always matches the Databricks mart.

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

### Deploy model:
- `main` → `astro-prod`. `astro-dev`'s branch mapping is set manually in the
  Astro Cloud UI's Git Deploys settings. Astro's webhook fires on push events
  to the mapped branch, so a branch-mapping change alone does not redeploy —
  a subsequent push to the new branch (or a manual redeploy via the Astro UI)
  is what triggers the sync.
- The election-api Postgres schema is owned by the election-api repo. Prisma
  migrations apply when election-api is deployed to the corresponding env,
  not on PR merge alone. Check status via the `_prisma_migrations` table on
  the target Postgres before kicking a sync that depends on new columns.
"""

import logging
import uuid
from datetime import UTC, datetime

from airflow.sdk import Variable, dag, task, task_group
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

t_log = logging.getLogger("airflow.task")

PG_CONN_ID = "election_api_db"
DATABRICKS_SCHEMA = "dbt"  # canonical mart location (not dbt_staging)

# Memory note: load_staging streams a mart into Postgres, but
# bulk_insert_from_databricks reads one partition at a time over a single
# connection, so each task's peak memory is bounded to ~one partition (tens of
# MB on top of the worker's shared base). Under the Astro executor, tasks run as
# subprocesses on shared worker nodes sized via the deployment's worker queue
# (see astro.tf), so no per-task pod override (executor_config) is needed —
# pod_override is a Kubernetes-executor feature and is ignored here anyway.


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
        datetime.now(UTC),
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
        (f'ALTER TABLE "{sn}"."{nt}" ' f'ADD CONSTRAINT "{DTI.stage_name(DTI.pk_name)}" PRIMARY KEY (id)'),
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
        (f'ALTER TABLE "{sn}"."{nt}" ' f'ADD CONSTRAINT "{ZTP.stage_name(ZTP.pk_name)}" PRIMARY KEY (id)'),
        (f'CREATE INDEX "{ZTP.stage_name("ZipToPosition_zip_code_idx")}" ' f'ON "{sn}"."{nt}" (zip_code)'),
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


# ---------------------------------------------------------------------------
# Elected_Office_Support
# ---------------------------------------------------------------------------

# elected_office_id is the gp-api elected_office instance; it is not an enforced
# FK (elected_office lives in gp-api, not the Election API), so the table has only
# a primary key — no secondary indexes or foreign keys.
EOS = TableSyncSpec(target_table="Elected_Office_Support")

# The mart emits these columns directly, so rows pass through with no transform
# (source order must match this list).
EOS_COLUMNS = [
    "elected_office_id",
    "support_constituents",
    "total_constituents",
    "created_at",
    "updated_at",
]


def _eos_constraint_ddl() -> list[str]:
    sn, nt = EOS.staging_schema, EOS.new_table
    return [
        (
            f'ALTER TABLE "{sn}"."{nt}" '
            f'ADD CONSTRAINT "{EOS.stage_name(EOS.pk_name)}" PRIMARY KEY (elected_office_id)'
        ),
    ]


# ---------------------------------------------------------------------------
# Projected_Turnout
# ---------------------------------------------------------------------------

# First table cut over from the legacy dbt writer (DATA-2015): the writer
# upserts by id and never deletes, so model-version supersessions and L2
# coverage drift stranded stale rows. Swapping the whole table each run keeps
# the API equal to the mart with no supersession bookkeeping.
PT = TableSyncSpec(
    target_table="Projected_Turnout",
    indexes=("Projected_Turnout_district_id_election_year_idx",),
    fkeys=("Projected_Turnout_district_id_fkey",),
)

# The mart emits these columns directly, so rows pass through with no transform
# (source order must match this list; it mirrors the legacy dbt writer's column
# mapping for this table). election_code arrives as the enum label text
# (General | LocalOrMunicipal | ConsolidatedGeneral | Primary); the staging
# table is a LIKE-clone of the live table, so the column keeps the
# "ElectionCode" enum type and Postgres rejects any unknown label at insert.
PT_COLUMNS = [
    "id",
    "created_at",
    "updated_at",
    "election_year",
    "election_code",
    "projected_turnout",
    "inference_at",
    "model_version",
    "district_id",
]


def _pt_constraint_ddl() -> list[str]:
    sn, nt = PT.staging_schema, PT.new_table
    target_schema = PT.target_schema
    return [
        (f'ALTER TABLE "{sn}"."{nt}" ' f'ADD CONSTRAINT "{PT.stage_name(PT.pk_name)}" PRIMARY KEY (id)'),
        (
            f"CREATE INDEX "
            f'"{PT.stage_name("Projected_Turnout_district_id_election_year_idx")}" '
            f'ON "{sn}"."{nt}" (district_id, election_year)'
        ),
        (
            f'ALTER TABLE "{sn}"."{nt}" '
            f'ADD CONSTRAINT "{PT.stage_name("Projected_Turnout_district_id_fkey")}" '
            f"FOREIGN KEY (district_id) "
            f'REFERENCES "{target_schema}"."District"(id) '
            f"ON UPDATE CASCADE ON DELETE RESTRICT"
        ),
    ]


def _pt_quality_gate(loaded_count: int, dup_keys: int, prior_key_count: int, null_keys: int) -> None:
    """Decide whether staged Projected_Turnout data may swap into place.

    Pure decision logic (unit-tested); the quality_checks task gathers the
    inputs from Postgres. Raises ValueError (task fails, no swap) when:

    - ``null_keys`` > 0 — staged rows with a NULL district_id, election_year,
      or election_code. Belt-and-braces over the staging table's inherited
      NOT NULLs (the LIKE-clone copies them, so such a row should already
      have failed the load): the gate carries its own proof rather than
      relying on an inherited constraint.
    - ``dup_keys`` > 0 — duplicate (district_id, election_year, election_code)
      keys in staging. Must be zero: the election-api consumer does not
      disambiguate model_version, so a duplicate key would make it serve an
      arbitrary row. This is the invariant the swap delivery exists to
      guarantee (the legacy upsert writer could not).
    - loaded rows fall below half of ``prior_key_count``, the prior live
      table's DISTINCT natural-key count (0 on a true cold start). The
      baseline is keys, not raw rows: a live table last written by the legacy
      upsert path can be bloated with superseded model_version duplicates,
      while staging is deduped, so keys-vs-keys lets a legitimate dedupe
      cutover pass while still refusing a coverage collapse.
    - cold start (no prior table) with an implausibly small load.
    """
    if null_keys > 0:
        raise ValueError(
            f"{null_keys} staging rows have a NULL district_id, "
            f"election_year, or election_code — refusing to swap"
        )
    if dup_keys > 0:
        raise ValueError(
            f"{dup_keys} duplicate (district_id, election_year, "
            f"election_code) keys in staging — refusing to swap"
        )
    if prior_key_count > 0:
        ratio = loaded_count / prior_key_count
        if ratio < 0.5:
            raise ValueError(
                f"Loaded {loaded_count} rows, prior live had "
                f"{prior_key_count} distinct (district_id, "
                f"election_year, election_code) keys "
                f"(ratio {ratio:.2f}) — refusing to swap"
            )
    elif loaded_count < 100_000:
        raise ValueError(f"Cold-start load of {loaded_count} rows is implausibly small — refusing to swap")


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
                    # Statewide coverage (DATA-1986) added ~260k rows; read one
                    # state at a time so the worker's peak memory stays bounded
                    # as the mart grows, instead of buffering the full result.
                    partition_column="state",
                )

        @task
        def build_indexes_and_fk() -> None:
            with _open_pg() as conn:
                apply_ddl(conn, _ztp_constraint_ddl())

        @task
        def quality_checks(loaded_count: int) -> None:
            if loaded_count < 1_000:
                raise ValueError(f"Only {loaded_count} rows loaded (<1000) — refusing to swap")
            with _open_pg() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(
                        f"SELECT COUNT(DISTINCT state) " f'FROM "{ZTP.staging_schema}"."{ZTP.new_table}"'
                    )
                    distinct_states = cur.fetchone()[0]
                    if distinct_states < 30:
                        raise ValueError(f"Only {distinct_states} distinct states " f"— refusing to swap")
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
                    # ~5.1M rows; this load runs concurrently with
                    # zip_to_position on the same worker, so read one issue at a
                    # time (~68 partitions, <=~96k rows each) to keep the
                    # combined peak memory bounded and avoid OOM (the zip-only
                    # partition in #493 left this load unbounded).
                    partition_column="issue",
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

    @task_group(group_id="elected_official_support")
    def elected_official_support():
        @task
        def build_staging() -> None:
            with _open_pg() as conn:
                create_staging_table(conn, EOS)

        @task
        def load_staging() -> int:
            catalog = Variable.get("databricks_catalog")
            col_list = ", ".join(EOS_COLUMNS)
            query = (
                f"SELECT {col_list} "
                f"FROM `{catalog}`.`{DATABRICKS_SCHEMA}`."
                f"`m_election_api__elected_official_support`"
            )
            # ~1.1k rows; no partitioning needed (one small result fits easily).
            with _open_pg() as conn:
                return bulk_insert_from_databricks(
                    conn,
                    EOS,
                    source_query=query,
                    target_columns=EOS_COLUMNS,
                )

        @task
        def build_indexes_and_fk() -> None:
            with _open_pg() as conn:
                apply_ddl(conn, _eos_constraint_ddl())

        @task
        def quality_checks(loaded_count: int) -> None:
            # Coverage is intentionally low: the support score needs an election
            # vote tally, which exists for only a small share of offices (see the
            # model docs / DATA-2000 PR). A flat floor can't catch a regression
            # that halves a ~1.1k-row table (a ~550-row load would clear 500), so
            # gate on the ratio to the prior live table (same pattern as the
            # DistrictTopIssue block); the absolute floor is the cold-start
            # fallback only.
            with _open_pg() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(
                        f"SELECT COUNT(*), COUNT(DISTINCT elected_office_id), "
                        f"COUNT(*) FILTER (WHERE elected_office_id IS NULL) "
                        f'FROM "{EOS.staging_schema}"."{EOS.new_table}"'
                    )
                    n, distinct_pos, null_pos = cur.fetchone()

                    # Prior live state (absent on a true cold start). Both
                    # identifiers must be double-quoted in the regclass argument
                    # so Postgres does not fold the mixed-case name to lowercase.
                    cur.execute(
                        "SELECT to_regclass(%s)",
                        (f'"{EOS.target_schema}"."{EOS.target_table}"',),
                    )
                    prior_exists = cur.fetchone()[0] is not None
                    if prior_exists:
                        cur.execute(f'SELECT COUNT(*) FROM "{EOS.target_schema}"."{EOS.target_table}"')
                        prior_count = cur.fetchone()[0]
                    else:
                        prior_count = 0

                    if prior_count > 0:
                        ratio = loaded_count / prior_count
                        if ratio < 0.5:
                            raise ValueError(
                                f"Loaded {loaded_count} rows, prior live had "
                                f"{prior_count} (ratio {ratio:.2f}) — refusing to swap"
                            )
                    elif loaded_count < 500:
                        raise ValueError(f"Cold-start load of {loaded_count} rows (<500) — refusing to swap")

                    if null_pos > 0:
                        raise ValueError(
                            f"{null_pos} staging rows have a NULL elected_office_id — refusing to swap"
                        )
                    if distinct_pos != n:
                        raise ValueError(
                            f"elected_office_id not unique ({distinct_pos} distinct of {n}) — refusing to swap"
                        )
                    t_log.info(
                        "Quality checks passed: %d rows (prior %d), elected_office_id unique and non-null",
                        n,
                        prior_count,
                    )
                finally:
                    cur.close()

        @task
        def swap() -> None:
            with _open_pg() as conn:
                swap_staging_into_target(conn, EOS)

        @task
        def drop_old() -> None:
            with _open_pg() as conn:
                drop_old_table(conn, EOS)

        s = build_staging()
        loaded = load_staging()
        idx = build_indexes_and_fk()
        qc = quality_checks(loaded)
        sw = swap()
        do = drop_old()
        s >> loaded >> idx >> qc >> sw >> do

    @task_group(group_id="projected_turnout")
    def projected_turnout():
        @task
        def build_staging() -> None:
            with _open_pg() as conn:
                create_staging_table(conn, PT)

        @task
        def load_staging() -> int:
            catalog = Variable.get("databricks_catalog")
            col_list = ", ".join(PT_COLUMNS)
            query = (
                f"SELECT {col_list} "
                f"FROM `{catalog}`.`{DATABRICKS_SCHEMA}`."
                f"`m_election_api__projected_turnout`"
            )
            with _open_pg() as conn:
                return bulk_insert_from_databricks(
                    conn,
                    PT,
                    source_query=query,
                    target_columns=PT_COLUMNS,
                    # ~800k rows; this load runs concurrently with the other
                    # groups on the same worker, so read one election year at a
                    # time (a handful of partitions) to keep the combined peak
                    # memory bounded.
                    partition_column="election_year",
                )

        @task
        def build_indexes_and_fk() -> None:
            with _open_pg() as conn:
                apply_ddl(conn, _pt_constraint_ddl())

        @task
        def quality_checks(loaded_count: int) -> None:
            # Gate decisions (and their rationale) live in _pt_quality_gate,
            # which is pure and unit-tested; this task only gathers the
            # inputs from Postgres.
            with _open_pg() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(
                        f"SELECT COUNT(*) FROM ("
                        f"SELECT district_id, election_year, election_code "
                        f'FROM "{PT.staging_schema}"."{PT.new_table}" '
                        f"GROUP BY district_id, election_year, election_code "
                        f"HAVING COUNT(*) > 1"
                        f") AS dupe_keys"
                    )
                    dup_keys = cur.fetchone()[0]

                    # NULL keys: belt-and-braces over the staging table's
                    # inherited NOT NULLs — the gate proves it directly.
                    cur.execute(
                        f"SELECT COUNT(*) "
                        f'FROM "{PT.staging_schema}"."{PT.new_table}" '
                        f"WHERE district_id IS NULL "
                        f"OR election_year IS NULL "
                        f"OR election_code IS NULL"
                    )
                    null_keys = cur.fetchone()[0]

                    # Prior live state (absent on a true cold start). Both
                    # identifiers must be double-quoted in the regclass argument
                    # so Postgres does not fold the mixed-case name to lowercase.
                    cur.execute(
                        "SELECT to_regclass(%s)",
                        (f'"{PT.target_schema}"."{PT.target_table}"',),
                    )
                    prior_exists = cur.fetchone()[0] is not None
                    if prior_exists:
                        cur.execute(
                            f"SELECT COUNT(*) FROM ("
                            f"SELECT DISTINCT district_id, election_year, election_code "
                            f'FROM "{PT.target_schema}"."{PT.target_table}"'
                            f") AS prior_keys"
                        )
                        prior_key_count = cur.fetchone()[0]
                    else:
                        prior_key_count = 0
                finally:
                    cur.close()

            _pt_quality_gate(loaded_count, dup_keys, prior_key_count, null_keys)

            t_log.info(
                "Quality checks passed: %d rows (prior %d distinct keys), "
                "no duplicate and no NULL (district, election) keys",
                loaded_count,
                prior_key_count,
            )

        @task
        def swap() -> None:
            with _open_pg() as conn:
                swap_staging_into_target(conn, PT)

        @task
        def drop_old() -> None:
            with _open_pg() as conn:
                drop_old_table(conn, PT)

        s = build_staging()
        loaded = load_staging()
        idx = build_indexes_and_fk()
        qc = quality_checks(loaded)
        sw = swap()
        do = drop_old()
        s >> loaded >> idx >> qc >> sw >> do

    # The pipelines run in parallel; under the Kubernetes executor each task
    # is its own pod, and each load_staging reads one partition at a time, so
    # they don't contend for memory.
    zip_to_position()
    district_top_issues()
    elected_official_support()
    projected_turnout()


sync_election_api()
