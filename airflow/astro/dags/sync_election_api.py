"""
## Sync election-api marts from Databricks to PostgreSQL

Builds a complete staging copy of every election-api Postgres table from its
dbt mart in Databricks, then swaps the entire set into place in one atomic
transaction. Each table is one task group with the same build lifecycle:

1. **build_staging** — drop & recreate `staging."<Table>_new"` LIKE the live
   target (no indexes — fast bulk-insert).
2. **load_staging** — stream the source mart from Databricks into staging.
   Runs on the `election-api-sync` worker queue (see Worker queue below).
   The column list is the live table's own columns, so the mart must publish
   every one of them, with matching names and types — there is no per-table
   column mapping, row transform, or exclusion list in this DAG. A live column
   the mart lacks fails the load; a new live column whose data the mart already
   publishes starts flowing automatically. Adding a column therefore lands
   mart-side first, then in Postgres.
3. **build_indexes_and_fk** — add PK, indexes, and FK constraints, generated
   from the table's declarative spec. Cross-table FKs reference the sibling
   STAGING table, so the staging set is self-contained and validates against
   its own vintage.
4. **quality_checks** — gate the swap on generic count / id-overlap /
   NULL-probe floors plus optional per-table extras.

Once every table's quality gate is green, a single **swap** task renames the
whole set in atomically (live -> `_old`, `_new` -> live, all indexes and
constraints renamed to canonical Prisma names; constraints follow their
tables through the renames, so the fresh vintage's FKs arrive pointing at
fresh siblings and the old vintage's leave with it). No child rows are ever
mutated and the API only ever sees one complete, referentially consistent
vintage. **drop_old** then drops the renamed-aside set. Leftover `_old`
tables from a crashed run are pre-dropped inside the swap transaction, so a
crashed run never wedges subsequent ones.

The shared lifecycle lives in
`include/custom_functions/election_api_utils.py`; each table below is a
declarative `MartSync` entry (spec, mart, gate, FK parents) consumed by a
single task-group factory. The only cross-group ordering is
`parent.build_indexes_and_fk >> child.build_indexes_and_fk`: a staging FK
needs the referenced staging table loaded with its PK in place. Loads run in
parallel.

The swap is gated behind the `election_api_swap_enabled` Variable (rehearsal
mode unless it is exactly "true"): every night while disabled is a full
dress rehearsal — every staging table built, loaded, indexed, and gated;
only the swap is withheld. This DAG is the only writer to these tables, so
rehearsal freezes ALL of them: keep the rehearsal window short.

### The voter-density tables are here, not in a DAG of their own

`District_Voter_Density` (~59.3M rows) and its `_Meta` sibling are the two
largest tables in the set, and their marts rebuild `monthly` in dbt while this
DAG runs nightly — so most nights re-copy identical rows. They are still here
rather than on their own monthly schedule because that is the only way their
foreign key to `District` can exist at all: an FK from outside this set does
not survive the swap. `District` is renamed to `District_old`, which leaves any
outside FK pointing at the stale vintage, and `drop_old` then removes the
constraint with only a NOTICE — rows intact, constraint gone, orphans
insertable. Inside the set the FK references the staging sibling and rides the
renames with it, so it is enforced continuously.

The cost is a nightly full re-copy of data that only changes monthly, which
adds tens of minutes to the run. `ForeignKey(on_missing_parent="skip")` on both
is what keeps that safe: their marts lag District's, so a district an L2 rename
dropped is a stale row and is pruned, not a failure that would take the whole
set down.

`people-api-loader` also loads these two marts, into the people-api cluster.
That copy is what the app reads today and goes away with people-api itself;
this one is its replacement, not a second live source.

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
- `election_api_swap_enabled` — cutover switch for the set-wise swap.
  Anything but "true" is rehearsal mode (no table is swapped).
- `election_api_source_schema` (optional) — Databricks schema holding the
  marts. Defaults to `dbt`, the canonical production-quality build, in both
  dev and prod (deliberately not `databricks_dbt_schema`, which points at
  `dbt_staging` for in-flight artifacts). Override on a dev deployment to
  test unmerged mart changes end to end from a development schema.

### Worker queue:
`load_staging` runs on the `election-api-sync` queue, declared per environment
in gp-terraform-dataplatform's `locals.tf`. Without it those tasks are never
picked up and fail with no logs. A load task peaks around 400 MB
(~250 MB of imports plus one `batch_size` fetch), so the queue is A5 at
concurrency 2 — five on one 2 GiB A5 is what OOM-killed these tasks. Keep the
concurrency low rather than widening it; Astro scales workers out from the
queued-task count, so low concurrency gives the DAG more cores, not fewer.

### Deploy model:
Branch-to-deployment mapping lives in Astro's Git Deploys settings; see
`airflow/astro/README.md`.

The election-api Postgres schema is owned by the election-api repo. Prisma
migrations apply when election-api is deployed to the corresponding env, not
on PR merge alone. Check `_prisma_migrations` on the target Postgres before
kicking a sync that depends on new columns.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass

from airflow.sdk import Variable, dag, task, task_group
from include.custom_functions.election_api_utils import (
    ForeignKey,
    Index,
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
from pendulum import datetime as pendulum_datetime
from pendulum import duration

t_log = logging.getLogger("airflow.task")

PG_CONN_ID = "election_api_db"
SWAP_GATE_VARIABLE = "election_api_swap_enabled"
# Loads run on their own worker queue at low concurrency; see the docstring.
LOAD_QUEUE = "election-api-sync"


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


# ---------------------------------------------------------------------------
# Per-table extra quality checks (beyond the generic gate)
# ---------------------------------------------------------------------------


# Mirrors `voter_density_k` in dbt/project/dbt_project.yml, the value the marts
# suppress at. Duplicated because the DAG has no dbt context; a change there
# without a change here fails the swap closed, the right direction for a
# privacy floor.
VOTER_DENSITY_K = 10


def _dvd_extra_checks(conn, spec: TableSyncSpec, loaded_count: int) -> None:
    """Refuse to publish a cell holding fewer than K voters.

    The generic gate cannot see this: dropping the suppression filter upstream
    raises the row count, so the ratio check reads it as a healthy load.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            f'SELECT COUNT(*) FROM "{spec.staging_schema}"."{spec.new_table}" '
            f"WHERE voter_count < {VOTER_DENSITY_K}"
        )
        under_k = cur.fetchone()[0]
    finally:
        cur.close()
    if under_k > 0:
        raise ValueError(f"{under_k} cells below K={VOTER_DENSITY_K} in staging — refusing to swap")


def _ztp_extra_checks(conn, spec: TableSyncSpec, loaded_count: int) -> None:
    """Statewide coverage: a partial load can pass the count ratio while
    silently dropping whole states."""
    cur = conn.cursor()
    try:
        cur.execute(f'SELECT COUNT(DISTINCT state) FROM "{spec.staging_schema}"."{spec.new_table}"')
        distinct_states = cur.fetchone()[0]
    finally:
        cur.close()
    if distinct_states < 30:
        raise ValueError(f"Only {distinct_states} distinct states — refusing to swap")


def _pt_extra_checks(conn, spec: TableSyncSpec, loaded_count: int) -> None:
    """The election-api consumer does not disambiguate model_version, so a
    duplicate (district_id, election_year, election_code) key would make it
    serve an arbitrary row — the invariant the swap delivery exists to
    guarantee (the legacy upsert writer could not)."""
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT COUNT(*) FROM ("
            f"SELECT district_id, election_year, election_code "
            f'FROM "{spec.staging_schema}"."{spec.new_table}" '
            f"GROUP BY district_id, election_year, election_code "
            f"HAVING COUNT(*) > 1"
            f") AS dupe_keys"
        )
        dup_keys = cur.fetchone()[0]
    finally:
        cur.close()
    if dup_keys > 0:
        raise ValueError(
            f"{dup_keys} duplicate (district_id, election_year, "
            f"election_code) keys in staging — refusing to swap"
        )


# ---------------------------------------------------------------------------
# Table declarations
# ---------------------------------------------------------------------------

# Staged-vs-live id overlap floor for the Prisma-graph tables, whose minted
# ids are deterministic and may be held by external consumers. 0.90 leaves
# headroom for the first enabled run, which prunes rows the legacy writer
# could never delete.
_GRAPH_ID_OVERLAP = 0.90

TABLES: tuple[MartSync, ...] = (
    MartSync(
        group_id="place",
        spec=TableSyncSpec(
            target_table="Place",
            indexes=(
                Index("Place_slug_key", "(slug)", unique=True),
                Index("Place_geoid_key", "(geoid)", unique=True),
            ),
            fkeys=(ForeignKey("Place_parent_id_fkey", "parent_id", "Place"),),
        ),
        source_model="m_election_api__place",
        gate=QualityGate(cold_start_floor=40_000, min_id_overlap=_GRAPH_ID_OVERLAP),
    ),
    MartSync(
        group_id="district",
        spec=TableSyncSpec(
            target_table="District",
            indexes=(
                Index(
                    "District_state_l2_district_type_l2_district_name_key",
                    "(state, l2_district_type, l2_district_name)",
                    unique=True,
                ),
            ),
        ),
        source_model="m_election_api__district",
        gate=QualityGate(cold_start_floor=100_000, min_id_overlap=_GRAPH_ID_OVERLAP),
        partition_column="state",
    ),
    MartSync(
        group_id="issue",
        spec=TableSyncSpec(
            target_table="Issue",
            fkeys=(ForeignKey("Issue_parent_id_fkey", "parent_id", "Issue"),),
        ),
        source_model="m_election_api__issue",
        # The BR issue taxonomy is ~20 rows; the ratio gate does the real work.
        gate=QualityGate(cold_start_floor=10, min_id_overlap=_GRAPH_ID_OVERLAP),
    ),
    MartSync(
        group_id="person",
        spec=TableSyncSpec(
            target_table="Person",
            indexes=(
                Index("Person_slug_idx", "(slug)"),
                Index("Person_gp_api_user_id_idx", "(gp_api_user_id)"),
            ),
        ),
        source_model="m_election_api__person",
        partition_column="state",
        gate=QualityGate(cold_start_floor=200_000, min_id_overlap=_GRAPH_ID_OVERLAP),
    ),
    MartSync(
        group_id="position",
        spec=TableSyncSpec(
            target_table="Position",
            indexes=(Index("Position_br_position_id_key", "(br_position_id)", unique=True),),
            fkeys=(ForeignKey("Position_district_id_fkey", "district_id", "District"),),
        ),
        source_model="m_election_api__position",
        partition_column="state",
        gate=QualityGate(cold_start_floor=200_000, min_id_overlap=_GRAPH_ID_OVERLAP),
        parents=("district",),
    ),
    MartSync(
        group_id="race",
        spec=TableSyncSpec(
            target_table="Race",
            indexes=(
                Index("Race_br_hash_id_idx", "(br_hash_id)"),
                Index("Race_place_id_idx", "(place_id)"),
                Index("Race_position_id_idx", "(position_id)"),
                Index("Race_slug_idx", "(slug)"),
            ),
            fkeys=(
                ForeignKey("Race_place_id_fkey", "place_id", "Place"),
                ForeignKey("Race_position_id_fkey", "position_id", "Position"),
            ),
        ),
        source_model="m_election_api__race",
        # ~1M rows; one state at a time keeps each server-side result small.
        partition_column="state",
        gate=QualityGate(cold_start_floor=100_000, min_id_overlap=_GRAPH_ID_OVERLAP),
        parents=("place", "position"),
    ),
    MartSync(
        group_id="candidacy",
        spec=TableSyncSpec(
            target_table="Candidacy",
            indexes=(
                Index("Candidacy_slug_key", "(slug)", unique=True),
                Index("Candidacy_person_id_idx", "(person_id)"),
                Index("Candidacy_race_id_idx", "(race_id)"),
            ),
            fkeys=(
                ForeignKey("Candidacy_person_id_fkey", "person_id", "Person"),
                ForeignKey("Candidacy_race_id_fkey", "race_id", "Race"),
            ),
        ),
        source_model="m_election_api__candidacy",
        partition_column="state",
        gate=QualityGate(cold_start_floor=150_000, min_id_overlap=_GRAPH_ID_OVERLAP),
        parents=("person", "race"),
    ),
    MartSync(
        group_id="office_holder",
        spec=TableSyncSpec(
            target_table="OfficeHolder",
            indexes=(
                Index("OfficeHolder_person_id_idx", "(person_id)"),
                Index("OfficeHolder_position_id_idx", "(position_id)"),
            ),
            fkeys=(
                ForeignKey("OfficeHolder_person_id_fkey", "person_id", "Person", on_delete="CASCADE"),
                ForeignKey("OfficeHolder_position_id_fkey", "position_id", "Position"),
            ),
        ),
        source_model="m_election_api__office_holder",
        partition_column="state",
        gate=QualityGate(cold_start_floor=100_000, min_id_overlap=_GRAPH_ID_OVERLAP),
        parents=("person", "position"),
    ),
    MartSync(
        group_id="stance",
        spec=TableSyncSpec(
            target_table="Stance",
            indexes=(
                Index("Stance_candidacy_id_idx", "(candidacy_id)"),
                Index("Stance_issue_id_idx", "(issue_id)"),
            ),
            fkeys=(
                ForeignKey("Stance_issue_id_fkey", "issue_id", "Issue", on_delete="RESTRICT"),
                ForeignKey("Stance_candidacy_id_fkey", "candidacy_id", "Candidacy"),
            ),
        ),
        source_model="m_election_api__stance",
        partition_column="issue_id",
        gate=QualityGate(cold_start_floor=50_000, min_id_overlap=_GRAPH_ID_OVERLAP),
        parents=("issue", "candidacy"),
    ),
    MartSync(
        group_id="zip_to_position",
        spec=TableSyncSpec(
            target_table="ZipToPosition",
            indexes=(
                Index("ZipToPosition_zip_code_idx", "(zip_code)"),
                Index("ZipToPosition_position_id_idx", "(position_id)"),
                Index(
                    "ZipToPosition_zip_code_pct_districtzip_to_zip_idx",
                    "(zip_code, pct_districtzip_to_zip)",
                ),
                Index(
                    "ZipToPosition_zip_code_position_id_election_date_key",
                    "(zip_code, position_id, election_date) NULLS NOT DISTINCT",
                    unique=True,
                ),
            ),
            fkeys=(
                ForeignKey(
                    "ZipToPosition_position_id_fkey",
                    "position_id",
                    "Position",
                    on_delete="RESTRICT",
                ),
            ),
        ),
        source_model="m_election_api__zip_to_position",
        # Statewide coverage added ~260k rows; read one state at a time so each
        # server-side result set stays small as the mart grows.
        partition_column="state",
        # No id-overlap floor: nothing references ZipToPosition ids (PK only;
        # the API reads by zip/position), and this change re-mints them (the
        # mart now derives them from the natural key).
        gate=QualityGate(cold_start_floor=1_000),
        extra_checks=_ztp_extra_checks,
        parents=("position",),
    ),
    MartSync(
        group_id="district_top_issues",
        spec=TableSyncSpec(
            target_table="DistrictTopIssue",
            indexes=(Index("DistrictTopIssue_district_id_issue_key", "(district_id, issue)", unique=True),),
            fkeys=(
                ForeignKey(
                    "DistrictTopIssue_district_id_fkey",
                    "district_id",
                    "District",
                    on_delete="RESTRICT",
                ),
            ),
        ),
        source_model="m_election_api__district_top_issues",
        # ~5.1M rows; read one issue at a time (~68 partitions) so no single
        # server-side result set holds the whole mart.
        partition_column="issue",
        gate=QualityGate(
            cold_start_floor=100_000,
            # The mart's LEFT JOIN to haystaq_issue_tags only emits NULL flags
            # on drift; belt-and-suspenders over the dbt-side tests.
            not_null_columns=("is_local", "is_regional", "is_state", "is_federal"),
        ),
        parents=("district",),
    ),
    MartSync(
        group_id="elected_official_support",
        spec=TableSyncSpec(
            target_table="Elected_Office_Support",
            # elected_office_id is the gp-api elected_office instance; it is
            # not an enforced FK (elected_office lives in gp-api, not the
            # Election API), so the table has only a primary key.
            pk_columns=("elected_office_id",),
        ),
        source_model="m_election_api__elected_official_support",
        # ~1.1k rows; coverage is intentionally low (the support score needs
        # an election vote tally). The ratio gate does the real work; the PK
        # added in build_indexes enforces elected_office_id unique + non-null.
        gate=QualityGate(cold_start_floor=500),
    ),
    MartSync(
        group_id="projected_turnout",
        spec=TableSyncSpec(
            target_table="Projected_Turnout",
            indexes=(
                Index(
                    "Projected_Turnout_district_id_election_year_idx",
                    "(district_id, election_year)",
                ),
            ),
            fkeys=(
                ForeignKey(
                    "Projected_Turnout_district_id_fkey",
                    "district_id",
                    "District",
                    on_delete="RESTRICT",
                ),
            ),
        ),
        source_model="m_election_api__projected_turnout",
        partition_column="election_year",
        # No id-overlap floor: rows legitimately re-key on model-version
        # supersessions.
        gate=QualityGate(
            cold_start_floor=100_000,
            not_null_columns=("district_id", "election_year", "election_code"),
        ),
        extra_checks=_pt_extra_checks,
        parents=("district",),
    ),
    MartSync(
        group_id="district_voter_density",
        spec=TableSyncSpec(
            # Prisma model DistrictVoterDensity, @@map'd to this table name.
            target_table="District_Voter_Density",
            # The mart grain.
            pk_columns=("district_id", "resolution", "h3_index"),
            indexes=(
                Index(
                    "District_Voter_Density_district_id_resolution_idx",
                    "(district_id, resolution)",
                ),
            ),
            fkeys=(
                ForeignKey(
                    "District_Voter_Density_district_id_fkey",
                    "district_id",
                    "District",
                    on_delete="RESTRICT",
                    # These marts rebuild monthly while District rebuilds
                    # nightly, so a district an L2 rename dropped is a stale
                    # row, not a bad one. Failing here would take the whole
                    # swap set down, not just density.
                    on_missing_parent="skip",
                ),
            ),
        ),
        source_model="m_people_api__district_voter_density",
        # ~59.3M rows, the largest table in the set by an order of magnitude;
        # read one state at a time so no single server-side result set holds
        # the whole mart. Unpartitioned is what OOM-kills these tasks.
        partition_column="state",
        # No id-overlap floor: the key is composite and natural, with no minted
        # id an external consumer could hold.
        gate=QualityGate(
            cold_start_floor=40_000_000,
            not_null_columns=("lat", "lng", "voter_count"),
        ),
        extra_checks=_dvd_extra_checks,
        parents=("district",),
    ),
    MartSync(
        group_id="district_voter_density_meta",
        spec=TableSyncSpec(
            # Prisma model DistrictVoterDensityMeta, @@map'd to this table name.
            target_table="District_Voter_Density_Meta",
            # One row per district per published resolution.
            pk_columns=("district_id", "resolution"),
            fkeys=(
                ForeignKey(
                    "District_Voter_Density_Meta_district_id_fkey",
                    "district_id",
                    "District",
                    on_delete="RESTRICT",
                    on_missing_parent="skip",
                ),
            ),
        ),
        source_model="m_people_api__district_voter_density_meta",
        # ~512k rows across four resolutions; small enough to read in one pass.
        gate=QualityGate(
            cold_start_floor=400_000,
            # coverage drives the app's resolution choice; a NULL there hides
            # the map for that district.
            not_null_columns=("coverage", "total_voters"),
        ),
        parents=("district",),
    ),
)


# ---------------------------------------------------------------------------
# Task-group factory
# ---------------------------------------------------------------------------


def _build_group(table: MartSync) -> dict:
    """One build->load->index->gate task group for a table's staging copy.

    Returns handles to the tasks that participate in cross-group wiring.
    """
    spec = table.spec
    handles: dict = {}

    @task_group(group_id=table.group_id)
    def group():
        @task
        def build_staging() -> None:
            with _open_pg() as conn:
                create_staging_table(conn, spec)

        @task(queue=LOAD_QUEUE)
        def load_staging() -> int:
            catalog = Variable.get("databricks_catalog")
            schema = Variable.get("election_api_source_schema", default="dbt")
            with _open_pg() as conn:
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
            with _open_pg() as conn:
                apply_ddl(conn, spec.constraint_ddl())

        @task
        def quality_checks(loaded_count: int) -> None:
            with _open_pg() as conn:
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


@dag(
    start_date=pendulum_datetime(2026, 5, 5, tz="UTC"),
    schedule="@daily",
    max_consecutive_failed_dag_runs=5,
    max_active_runs=1,
    doc_md=__doc__,
    catchup=False,
    default_args={
        "owner": "Data Engineering Team",
        # Two attempts: a step that fails twice is not a transient blip.
        "retries": 1,
        "retry_delay": duration(seconds=30),
    },
    tags=["election_api", "postgres"],
    is_paused_upon_creation=True,
)
def sync_election_api():
    handles = {table.group_id: _build_group(table) for table in TABLES}
    # Self-references need no edge: the PK lands in the same transaction,
    # before the FK.
    for table in TABLES:
        for parent in table.parents:
            handles[parent]["build_indexes_and_fk"] >> handles[table.group_id]["build_indexes_and_fk"]

    @task.short_circuit
    def cutover_enabled() -> bool:
        enabled = Variable.get(SWAP_GATE_VARIABLE, default="false").strip().lower() == "true"
        if not enabled:
            t_log.info("Swap disabled (rehearsal mode); staging left for parity checks")
        return enabled

    @task
    def swap() -> None:
        with _open_pg() as conn:
            swap_staging_into_target(conn, [table.spec for table in TABLES])

    @task
    def drop_old() -> None:
        with _open_pg() as conn:
            drop_old_tables(conn, [table.spec for table in TABLES])

    gate = cutover_enabled()
    for table in TABLES:
        handles[table.group_id]["quality_checks"] >> gate
    gate >> swap() >> drop_old()


sync_election_api()
