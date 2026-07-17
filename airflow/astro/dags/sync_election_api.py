"""
## Sync election-api marts from Databricks to PostgreSQL

Builds and atomically swaps election-api Postgres tables from their dbt marts
in Databricks. Every table is declared as a `SyncTableConfig`; the shared
lifecycle is:

1. **build_staging** — drop & recreate `staging."<Table>_new"` LIKE the live
   target (no indexes — fast bulk-insert).
2. **load_staging** — stream the source mart from Databricks into staging.
3. **build_indexes_and_fk** — add PK, indexes, and FK constraints.
4. **quality_checks** — gate the swap on row-count / coverage floors.
5. **swap** — atomic rename swap (`_new` -> live, live -> `_old`) with all
   indexes and constraints renamed to canonical Prisma names. A leftover
   `_old` from a run that crashed before drop_old is pre-dropped in the same
   transaction, so a crashed run never wedges subsequent ones.
6. **drop_old** — drop the renamed-aside `_old` table.

Independent tables get steps 5-6 from the `_sync_table_group` factory. Person
and OfficeHolder cannot swap independently: FK constraints bind to the
referenced table's OID, so an FK pointing at a swapped table would follow the
renamed-aside `_old` copy. The `person_spine` group instead stages both
tables and builds their FKs staged-to-staged, in dependency order:
`OfficeHolder_new.person_id` references `staging."Person_new"` directly (so
its constraint travels with the pair through the rename swap and validates
the staged data exactly), and both tables swap in ONE transaction. Only
`Candidacy.person_id -> Person` — owned by a table outside the group — still
needs rewiring: the swap transaction drops it first, renames both tables,
nulls Candidacy rows whose person left the new set (its ON DELETE SET NULL
semantics), and re-creates it against the fresh Person.

The shared lifecycle lives in
`include/custom_functions/election_api_utils.py`.

This DAG is the migration target for the legacy
`dbt/project/models/write/write__election_api_db.py` (which writes the other
election tables to Postgres via JDBC from a Databricks cluster).
Projected_Turnout was the first table cut over: upsert-by-id delivery can
never delete, so model-version supersessions and L2 coverage drift stranded
stale rows in the API (DATA-2015). Person and OfficeHolder (DATA-2118) are
delivered here from the start.

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

### Failure alerting (Astro-side, one-time setup):
Nothing in this repo sends failure notifications — there is no
`on_failure_callback`/notifier wiring, no Slack provider, and no SMTP
config. Alerting for this DAG is configured in the Astro UI (Workspace →
Alerts): create a DAG Failure alert scoped to `sync_election_api` on the
prod deployment and attach the team's Slack communication channel. Without
it, the only failure signal is `max_consecutive_failed_dag_runs=5`
eventually pausing the DAG, which freezes all synced tables silently.

### Deploy model:
- `main` → `astro-prod`. `astro-dev`'s branch mapping is set manually in the
  Astro Cloud UI's Git Deploys settings. Astro's webhook fires on push events
  to the mapped branch, so a branch-mapping change alone does not redeploy —
  a subsequent push to the new branch (or a manual redeploy via the Astro UI)
  is what triggers the sync.
- The election-api Postgres schema is owned by the election-api package in
  the omni repo. Prisma migrations apply when election-api is deployed to the
  corresponding env, not on PR merge alone. Check status via the
  `_prisma_migrations` table on the target Postgres before kicking a sync
  that depends on new columns/tables (Person and OfficeHolder need
  `20260708163759_add_person_officeholder`).
"""

import json
import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from airflow.sdk import Variable, dag, task, task_group
from include.custom_functions.election_api_utils import (
    TableSyncSpec,
    apply_ddl,
    bulk_insert_from_databricks,
    create_staging_table,
    drop_old_table,
    swap_group_staging_into_target,
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


def _prior_live_rowcount(conn, spec: TableSyncSpec) -> int | None:
    """Row count of the live target table, or None on a true cold start.

    Both identifiers must be double-quoted in the regclass argument — Postgres
    folds unquoted mixed-case to lowercase, so `public.DistrictTopIssue` would
    resolve to `public.districttopissue` and always return NULL.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT to_regclass(%s)",
            (f'"{spec.target_schema}"."{spec.target_table}"',),
        )
        if cur.fetchone()[0] is None:
            return None
        cur.execute(f'SELECT COUNT(*) FROM "{spec.target_schema}"."{spec.target_table}"')
        return cur.fetchone()[0]
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Table-group factory
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SyncTableConfig:
    """One synced table: where it comes from, how it loads, how it's gated."""

    group_id: str
    spec: TableSyncSpec
    source_table: str
    # Databricks SELECT order; positional with target_columns/transform_row.
    source_columns: tuple[str, ...]
    # Staging-named constraint/index DDL applied after the load.
    constraint_ddl: Callable[[], list[str]]
    # Opens its own Postgres connection; raises to block the swap.
    quality_checks: Callable[[int], None]
    target_columns: tuple[str, ...] | None = None  # defaults to source_columns
    transform_row: Callable[[tuple], tuple] | None = None
    partition_column: str | None = None


def _staged_load(cfg: SyncTableConfig) -> dict:
    """Steps 1-4 (build/load/index/quality); returns the task nodes by name
    so callers can chain the whole pipeline (via `quality_checks`) or add
    cross-table dependencies (e.g. an FK build that needs another staged
    table's PK in place first, via `build_indexes_and_fk`)."""

    @task
    def build_staging() -> None:
        with _open_pg() as conn:
            create_staging_table(conn, cfg.spec)

    @task
    def load_staging() -> int:
        catalog = Variable.get("databricks_catalog")
        col_list = ", ".join(cfg.source_columns)
        query = f"SELECT {col_list} " f"FROM `{catalog}`.`{DATABRICKS_SCHEMA}`.`{cfg.source_table}`"
        with _open_pg() as conn:
            return bulk_insert_from_databricks(
                conn,
                cfg.spec,
                source_query=query,
                target_columns=list(cfg.target_columns or cfg.source_columns),
                transform_row=cfg.transform_row,
                partition_column=cfg.partition_column,
            )

    @task
    def build_indexes_and_fk() -> None:
        with _open_pg() as conn:
            apply_ddl(conn, cfg.constraint_ddl())

    @task
    def quality_checks(loaded_count: int) -> None:
        cfg.quality_checks(loaded_count)

    loaded = load_staging()
    indexes = build_indexes_and_fk()
    qc = quality_checks(loaded)
    build_staging() >> loaded >> indexes >> qc
    return {"build_indexes_and_fk": indexes, "quality_checks": qc}


def _sync_table_group(cfg: SyncTableConfig):
    """Full lifecycle for a table with no FK ties to other swapped tables."""

    @task_group(group_id=cfg.group_id)
    def group():
        @task
        def swap() -> None:
            with _open_pg() as conn:
                swap_staging_into_target(conn, cfg.spec)

        @task
        def drop_old() -> None:
            with _open_pg() as conn:
                drop_old_table(conn, cfg.spec)

        _staged_load(cfg)["quality_checks"] >> swap() >> drop_old()

    return group()


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


def _ztp_quality_checks(loaded_count: int) -> None:
    if loaded_count < 1_000:
        raise ValueError(f"Only {loaded_count} rows loaded (<1000) — refusing to swap")
    with _open_pg() as conn:
        cur = conn.cursor()
        try:
            cur.execute(f"SELECT COUNT(DISTINCT state) " f'FROM "{ZTP.staging_schema}"."{ZTP.new_table}"')
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


ZTP_CONFIG = SyncTableConfig(
    group_id="zip_to_position",
    spec=ZTP,
    source_table="m_election_api__zip_to_position",
    source_columns=tuple(ZTP_SOURCE_COLUMNS),
    target_columns=tuple(ZTP_TARGET_COLUMNS),
    transform_row=_ztp_transform_row,
    # Statewide coverage (DATA-1986) added ~260k rows; read one state at a
    # time so the worker's peak memory stays bounded as the mart grows,
    # instead of buffering the full result.
    partition_column="state",
    constraint_ddl=_ztp_constraint_ddl,
    quality_checks=_ztp_quality_checks,
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


def _dti_quality_checks(loaded_count: int) -> None:
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

            prior_count = _prior_live_rowcount(conn, DTI)
            if prior_count is not None:
                cur.execute(
                    f"SELECT COUNT(DISTINCT issue) " f'FROM "{DTI.target_schema}"."{DTI.target_table}"'
                )
                prior_issues = cur.fetchone()[0]
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
                    f"Cold-start load of {loaded_count} rows is " f"implausibly small — refusing to swap"
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
                    f"{null_flag_rows} staging rows have a NULL " f"jurisdictional flag — refusing to swap"
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


DTI_CONFIG = SyncTableConfig(
    group_id="district_top_issues",
    spec=DTI,
    source_table="m_election_api__district_top_issues",
    source_columns=tuple(DTI_COLUMNS),
    # ~5.1M rows; this load runs concurrently with zip_to_position on the same
    # worker, so read one issue at a time (~68 partitions, <=~96k rows each)
    # to keep the combined peak memory bounded and avoid OOM (the zip-only
    # partition in #493 left this load unbounded).
    partition_column="issue",
    constraint_ddl=_dti_constraint_ddl,
    quality_checks=_dti_quality_checks,
)


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


def _eos_quality_checks(loaded_count: int) -> None:
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

            prior_count = _prior_live_rowcount(conn, EOS) or 0

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
                raise ValueError(f"{null_pos} staging rows have a NULL elected_office_id — refusing to swap")
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


EOS_CONFIG = SyncTableConfig(
    group_id="elected_official_support",
    spec=EOS,
    source_table="m_election_api__elected_official_support",
    source_columns=tuple(EOS_COLUMNS),
    # ~1.1k rows; no partitioning needed (one small result fits easily).
    constraint_ddl=_eos_constraint_ddl,
    quality_checks=_eos_quality_checks,
)


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


def _pt_quality_checks(loaded_count: int) -> None:
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

            # Prior live state (absent on a true cold start). Keys, not raw
            # rows — see _pt_quality_gate.
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


PT_CONFIG = SyncTableConfig(
    group_id="projected_turnout",
    spec=PT,
    source_table="m_election_api__projected_turnout",
    source_columns=tuple(PT_COLUMNS),
    # ~800k rows; this load runs concurrently with the other groups on the
    # same worker, so read one election year at a time (a handful of
    # partitions) to keep the combined peak memory bounded.
    partition_column="election_year",
    constraint_ddl=_pt_constraint_ddl,
    quality_checks=_pt_quality_checks,
)


# ---------------------------------------------------------------------------
# Person + OfficeHolder (grouped swap — other tables hold FKs to Person)
# ---------------------------------------------------------------------------

PERSON = TableSyncSpec(
    target_table="Person",
    indexes=("Person_slug_idx",),
)

PERSON_SOURCE_COLUMNS = [
    "id",
    "br_person_id",
    "slug",
    "first_name",
    "middle_name",
    "last_name",
    "nickname",
    "suffix",
    "full_name",
    "bio_text",
    "headshot_url",
    "website_url",
    "linkedin_url",
    "facebook_url",
    "twitter_url",
    "instagram_url",
    "email",
    "phone",
    "degrees",
    "experiences",
    "state",
]
# created_at is filled by the staging table's inherited column default;
# updated_at has no default (Prisma @updatedAt), so the transform stamps it.
PERSON_TARGET_COLUMNS = [*PERSON_SOURCE_COLUMNS, "updated_at"]


def _person_transform_row(row: tuple) -> tuple:
    # degrees/experiences arrive as JSON strings and land in JSONB as-is.
    return (*row, datetime.now(UTC))


def _person_constraint_ddl() -> list[str]:
    sn, nt = PERSON.staging_schema, PERSON.new_table
    return [
        (
            f'ALTER TABLE "{sn}"."{nt}" '
            f'ADD CONSTRAINT "{PERSON.stage_name(PERSON.pk_name)}" PRIMARY KEY (id)'
        ),
        (f'CREATE INDEX "{PERSON.stage_name("Person_slug_idx")}" ' f'ON "{sn}"."{nt}" (slug)'),
    ]


def _person_quality_gate(loaded_count: int, null_slugs: int, prior_count: int) -> None:
    """Decide whether staged Person data may swap into place.

    Pure decision logic (unit-tested); the quality_checks task gathers the
    inputs from Postgres. Duplicate/NULL ids already failed the staged PK
    build, so the gate checks the remaining invariants: `slug` present (NOT
    NULL in the API) and no coverage collapse vs the prior live table.
    """
    if null_slugs > 0:
        raise ValueError(f"{null_slugs} staging rows have a NULL slug — refusing to swap")
    if prior_count > 0:
        ratio = loaded_count / prior_count
        if ratio < 0.5:
            raise ValueError(
                f"Loaded {loaded_count} rows, prior live had "
                f"{prior_count} (ratio {ratio:.2f}) — refusing to swap"
            )
    elif loaded_count < 100_000:
        raise ValueError(f"Cold-start load of {loaded_count} rows is implausibly small — refusing to swap")


def _person_quality_checks(loaded_count: int) -> None:
    with _open_pg() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT COUNT(*) FILTER (WHERE slug IS NULL) "
                f'FROM "{PERSON.staging_schema}"."{PERSON.new_table}"'
            )
            null_slugs = cur.fetchone()[0]
            prior_count = _prior_live_rowcount(conn, PERSON) or 0
        finally:
            cur.close()

    _person_quality_gate(loaded_count, null_slugs, prior_count)
    t_log.info("Quality checks passed: %d rows (prior %d), slugs present", loaded_count, prior_count)


PERSON_CONFIG = SyncTableConfig(
    group_id="person",
    spec=PERSON,
    source_table="m_election_api__person",
    source_columns=tuple(PERSON_SOURCE_COLUMNS),
    target_columns=tuple(PERSON_TARGET_COLUMNS),
    transform_row=_person_transform_row,
    # ~500k rows; read one state at a time to bound peak memory.
    partition_column="state",
    constraint_ddl=_person_constraint_ddl,
    quality_checks=_person_quality_checks,
)


OFFICE_HOLDER = TableSyncSpec(
    target_table="OfficeHolder",
    indexes=(
        "OfficeHolder_person_id_idx",
        "OfficeHolder_position_id_idx",
    ),
    # person_id references staging."Person_new" when built; the constraint is
    # OID-bound, so it follows the pair through the rename swap and comes out
    # pointing at the fresh live Person.
    fkeys=(
        "OfficeHolder_person_id_fkey",
        "OfficeHolder_position_id_fkey",
    ),
)

OFFICE_HOLDER_SOURCE_COLUMNS = [
    "id",
    "br_office_holder_id",
    "position_name",
    "normalized_position_name",
    "office_title",
    "party_names",
    "start_at",
    "end_at",
    "term_date_specificity",
    "is_current",
    "is_appointed",
    "is_vacant",
    "number_of_seats",
    "next_election_date",
    "mailing_address_line_1",
    "mailing_address_line_2",
    "mailing_city",
    "mailing_state",
    "mailing_zip",
    "office_phone",
    "office_email",
    "website_url",
    "linkedin_url",
    "facebook_url",
    "twitter_url",
    "instagram_url",
    "sub_area_name",
    "sub_area_value",
    "state",
    "geo_id",
    "mtfcc",
    "person_id",
    "position_id",
]
OFFICE_HOLDER_TARGET_COLUMNS = [*OFFICE_HOLDER_SOURCE_COLUMNS, "updated_at"]

_PARTY_NAMES_IDX = OFFICE_HOLDER_SOURCE_COLUMNS.index("party_names")


def _office_holder_transform_row(row: tuple) -> tuple:
    # party_names arrives as a JSON string (the mart emits to_json because the
    # Databricks reader doesn't round-trip complex types); decode to a Python
    # list so psycopg2 adapts it to the text[] column.
    values = list(row)
    if values[_PARTY_NAMES_IDX] is not None:
        values[_PARTY_NAMES_IDX] = json.loads(values[_PARTY_NAMES_IDX])
    return (*values, datetime.now(UTC))


def _office_holder_constraint_ddl() -> list[str]:
    # Runs after person's build_indexes_and_fk (needs the staged Person PK):
    # the person_id FK references the STAGED Person table, validating the two
    # loads against each other exactly — any office holder without a staged
    # person fails here, loudly, before the swap. The position FK references
    # the live Position (delivered by the legacy dbt writer on its own
    # cadence), so positions the API doesn't know yet are nulled first —
    # matching the constraint's ON DELETE SET NULL semantics.
    sn, nt = OFFICE_HOLDER.staging_schema, OFFICE_HOLDER.new_table
    return [
        (
            f'UPDATE "{sn}"."{nt}" AS oh SET position_id = NULL '
            f"WHERE oh.position_id IS NOT NULL "
            f"AND NOT EXISTS ("
            f'SELECT 1 FROM "{OFFICE_HOLDER.target_schema}"."Position" AS p '
            f"WHERE p.id = oh.position_id)"
        ),
        (
            f'ALTER TABLE "{sn}"."{nt}" '
            f'ADD CONSTRAINT "{OFFICE_HOLDER.stage_name(OFFICE_HOLDER.pk_name)}" PRIMARY KEY (id)'
        ),
        (
            f'CREATE INDEX "{OFFICE_HOLDER.stage_name("OfficeHolder_person_id_idx")}" '
            f'ON "{sn}"."{nt}" (person_id)'
        ),
        (
            f'CREATE INDEX "{OFFICE_HOLDER.stage_name("OfficeHolder_position_id_idx")}" '
            f'ON "{sn}"."{nt}" (position_id)'
        ),
        (
            f'ALTER TABLE "{sn}"."{nt}" '
            f'ADD CONSTRAINT "{OFFICE_HOLDER.stage_name("OfficeHolder_person_id_fkey")}" '
            f"FOREIGN KEY (person_id) "
            f'REFERENCES "{PERSON.staging_schema}"."{PERSON.new_table}"(id) '
            f"ON DELETE CASCADE ON UPDATE CASCADE"
        ),
        (
            f'ALTER TABLE "{sn}"."{nt}" '
            f'ADD CONSTRAINT "{OFFICE_HOLDER.stage_name("OfficeHolder_position_id_fkey")}" '
            f"FOREIGN KEY (position_id) "
            f'REFERENCES "{OFFICE_HOLDER.target_schema}"."Position"(id) '
            f"ON DELETE SET NULL ON UPDATE CASCADE"
        ),
    ]


def _office_holder_quality_gate(loaded_count: int, null_person_ids: int, prior_count: int) -> None:
    """Decide whether staged OfficeHolder data may swap into place.

    Pure decision logic (unit-tested). `person_id` NULLs must be zero: the
    column is NOT NULL in the API and the mart inner-joins the person mart,
    so any NULL means the mart contract broke.
    """
    if null_person_ids > 0:
        raise ValueError(f"{null_person_ids} staging rows have a NULL person_id — refusing to swap")
    if prior_count > 0:
        ratio = loaded_count / prior_count
        if ratio < 0.5:
            raise ValueError(
                f"Loaded {loaded_count} rows, prior live had "
                f"{prior_count} (ratio {ratio:.2f}) — refusing to swap"
            )
    elif loaded_count < 100_000:
        raise ValueError(f"Cold-start load of {loaded_count} rows is implausibly small — refusing to swap")


def _office_holder_quality_checks(loaded_count: int) -> None:
    with _open_pg() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT COUNT(*) FILTER (WHERE person_id IS NULL) "
                f'FROM "{OFFICE_HOLDER.staging_schema}"."{OFFICE_HOLDER.new_table}"'
            )
            null_person_ids = cur.fetchone()[0]
            prior_count = _prior_live_rowcount(conn, OFFICE_HOLDER) or 0
        finally:
            cur.close()

    _office_holder_quality_gate(loaded_count, null_person_ids, prior_count)
    t_log.info(
        "Quality checks passed: %d rows (prior %d), person_id present",
        loaded_count,
        prior_count,
    )


OFFICE_HOLDER_CONFIG = SyncTableConfig(
    group_id="office_holder",
    spec=OFFICE_HOLDER,
    source_table="m_election_api__office_holder",
    source_columns=tuple(OFFICE_HOLDER_SOURCE_COLUMNS),
    target_columns=tuple(OFFICE_HOLDER_TARGET_COLUMNS),
    transform_row=_office_holder_transform_row,
    # ~520k rows; read one state at a time to bound peak memory.
    partition_column="state",
    constraint_ddl=_office_holder_constraint_ddl,
    quality_checks=_office_holder_quality_checks,
)


def _person_spine_pre_swap_ddl() -> list[str]:
    """Run inside the group-swap transaction, before any rename.

    Candidacy is outside the swap group but holds an FK to Person; the
    constraint binds to the live table's OID and would follow it to
    `Person_old`, wedging drop_old. Drop it here (IF EXISTS covers cold start
    and crash recovery) and re-create it in the post-swap DDL. The staged
    OfficeHolder FKs need no such handling — they reference the staged Person
    (swapped in the same transaction) and the stable live Position.
    """
    return [
        'ALTER TABLE "public"."Candidacy" ' 'DROP CONSTRAINT IF EXISTS "Candidacy_person_id_fkey"',
    ]


def _person_spine_post_swap_ddl() -> list[str]:
    """Run inside the group-swap transaction, after both tables are renamed.

    Candidacy rows whose person is absent from the new Person set are nulled
    (the constraint's ON DELETE SET NULL semantics; the legacy dbt writer
    back-fills person_id once persons arrive), then the FK is re-created
    against the fresh Person — validating as it goes, so a violation rolls
    the whole swap back.
    """
    return [
        (
            'UPDATE "public"."Candidacy" AS c SET person_id = NULL '
            "WHERE c.person_id IS NOT NULL "
            "AND NOT EXISTS ("
            'SELECT 1 FROM "public"."Person" AS p WHERE p.id = c.person_id)'
        ),
        (
            'ALTER TABLE "public"."Candidacy" '
            'ADD CONSTRAINT "Candidacy_person_id_fkey" '
            'FOREIGN KEY (person_id) REFERENCES "public"."Person"(id) '
            "ON DELETE SET NULL ON UPDATE CASCADE"
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
    # The pipelines run in parallel; under the Astro executor each task is its
    # own subprocess, and each load_staging reads one partition at a time, so
    # they don't contend for memory.
    for config in (ZTP_CONFIG, DTI_CONFIG, EOS_CONFIG, PT_CONFIG):
        _sync_table_group(config)

    @task_group(group_id="person_spine")
    def person_spine():
        @task_group(group_id=PERSON_CONFIG.group_id)
        def person_stage():
            return _staged_load(PERSON_CONFIG)

        @task_group(group_id=OFFICE_HOLDER_CONFIG.group_id)
        def office_holder_stage():
            return _staged_load(OFFICE_HOLDER_CONFIG)

        @task
        def swap() -> None:
            with _open_pg() as conn:
                swap_group_staging_into_target(
                    conn,
                    [PERSON, OFFICE_HOLDER],  # parent-first
                    pre_swap_ddl=_person_spine_pre_swap_ddl(),
                    post_swap_ddl=_person_spine_post_swap_ddl(),
                )

        @task
        def drop_old() -> None:
            with _open_pg() as conn:
                # Child first: OfficeHolder_old's FK references Person_old.
                drop_old_table(conn, OFFICE_HOLDER)
                drop_old_table(conn, PERSON)

        person_nodes = person_stage()
        office_holder_nodes = office_holder_stage()
        # The staged OfficeHolder person_id FK references staging."Person_new",
        # so it can only build (and validate) after person's staged PK exists.
        person_nodes["build_indexes_and_fk"] >> office_holder_nodes["build_indexes_and_fk"]

        sw = swap()
        [person_nodes["quality_checks"], office_holder_nodes["quality_checks"]] >> sw >> drop_old()

    person_spine()


sync_election_api()
