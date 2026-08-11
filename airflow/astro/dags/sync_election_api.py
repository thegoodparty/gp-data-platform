"""
## Sync election-api marts from Databricks to PostgreSQL

Builds and atomically swaps every election-api Postgres table from its dbt
mart in Databricks. Each table is one task group with the same lifecycle:

1. **build_staging** — drop & recreate `staging."<Table>_new"` LIKE the live
   target (no indexes — fast bulk-insert).
2. **load_staging** — stream the source mart from Databricks into staging.
3. **build_indexes_and_fk** — add PK, indexes, and FK constraints, generated
   from the table's declarative spec.
4. **quality_checks** — gate the swap on generic count / column-contract /
   NULL-probe floors plus optional per-table extras.
5. **swap** — atomic rename swap (`_new` -> live, live -> `_old`) with all
   indexes and constraints renamed to canonical Prisma names, and inbound
   child FKs re-pointed in the same transaction. A leftover `_old` from a
   run that crashed before drop_old is pre-dropped in the same transaction,
   so a crashed run never wedges subsequent ones.
6. **drop_old** — drop the renamed-aside `_old` table.

The shared lifecycle lives in
`include/custom_functions/election_api_utils.py`; each table below is a
declarative `MartSync` entry (spec, columns, gate, FK parents) consumed by a
single task-group factory.

Cross-table FKs order the groups: a child's staging FK references the LIVE
parent table, so the constraint must be created only after the parent's swap
cycle has fully completed (otherwise it would follow the old parent through
the rename and wedge the parent's drop_old). The wiring at the bottom adds
two edges per FK: `parent.drop_old >> child.build_indexes_and_fk`, and
`child.build_staging >> parent.swap` — the latter because a staging table
left over from a prior run (rehearsal mode, or a run that died before its
swap) still carries that FK, and the parent's swap would wedge on it unless
build_staging has dropped it first. Loads still run in parallel.

The eight tables migrated off the legacy dbt writer
(`dbt/project/models/write/write__election_api_db.py`) — Place, District,
Issue, Person, Position, Candidacy, OfficeHolder, Stance — are gated behind
the `election_api_writer_tables_swap_enabled` Variable (rehearsal mode until
it is exactly "true"): every night while disabled is a full dress rehearsal
(staging built, loaded, indexed, gated; only the swap is withheld). Flip the
Variable only after the legacy writer job is paused — the two paths write
the same tables. Upsert-by-id delivery can never delete, so superseded rows
strand in the API (DATA-2015); the swap replaces each table wholesale so the
API always matches the Databricks mart.

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
- `election_api_writer_tables_swap_enabled` — cutover switch for the eight
  tables migrated from the legacy dbt writer. Anything but "true" is
  rehearsal mode.

The source schema is hardcoded to `dbt` (not `databricks_dbt_schema`, which
points at `dbt_staging` for in-flight dbt build artifacts). The election-api
sync reads the production-quality version of the marts in both dev and prod.

### Failure alerting (Astro-side, one-time setup):
Nothing in this repo sends failure notifications — alerting for this DAG is
configured in the Astro UI (Workspace → Alerts): a DAG Failure alert scoped
to `sync_election_api` on the prod deployment, attached to the team's Slack
channel. Without it, the only failure signal is
`max_consecutive_failed_dag_runs=5` eventually pausing the DAG, which
freezes all synced tables silently.

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
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from airflow.sdk import Variable, dag, task, task_group
from include.custom_functions.election_api_utils import (
    ForeignKey,
    InboundForeignKey,
    Index,
    QualityGate,
    TableSyncSpec,
    apply_ddl,
    bulk_insert_from_databricks,
    create_staging_table,
    drop_old_table,
    run_quality_checks,
    swap_staging_into_target,
)
from include.custom_functions.postgres_utils import get_postgres_via_ssh
from pendulum import datetime as pendulum_datetime
from pendulum import duration

t_log = logging.getLogger("airflow.task")

PG_CONN_ID = "election_api_db"
DATABRICKS_SCHEMA = "dbt"  # canonical mart location (not dbt_staging)
SWAP_GATE_VARIABLE = "election_api_writer_tables_swap_enabled"

# Memory note: load_staging streams a mart into Postgres, but
# bulk_insert_from_databricks reads one partition at a time over a single
# connection, so each task's peak memory is bounded to ~one partition (tens of
# MB on top of the worker's shared base).


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


def _swap_enabled(raw: str) -> bool:
    """Cutover switch: only the exact word "true" (case and surrounding
    whitespace insensitive) enables the swap; anything else is rehearsal
    mode. Pure; unit-tested."""
    return raw.strip().lower() == "true"


@dataclass(frozen=True)
class MartSync:
    """One mart-to-table sync: everything the task-group factory needs."""

    group_id: str
    spec: TableSyncSpec
    source_model: str
    # Mart SELECT order; doubles as the insert order when target_columns is
    # empty and no transform reshapes the row.
    source_columns: tuple[str, ...]
    gate: QualityGate
    target_columns: tuple[str, ...] = ()
    transform_row: Callable[[tuple], tuple] | None = None
    partition_column: str | None = None
    # Extra per-table checks run after the generic gate: (conn, spec, loaded).
    extra_checks: Callable[..., None] | None = None
    # group_ids of swapped parents whose live tables this table's staging FKs
    # reference; wired as parent.drop_old >> this.build_indexes_and_fk.
    parents: tuple[str, ...] = ()
    # Rehearsal-gate the swap behind SWAP_GATE_VARIABLE (writer-migrated tables).
    swap_gated: bool = False

    @property
    def insert_columns(self) -> tuple[str, ...]:
        return self.target_columns or self.source_columns


# ---------------------------------------------------------------------------
# Row transforms (pure; unit-tested)
# ---------------------------------------------------------------------------

# Stable namespace so uuid5 produces the same ZipToPosition id across runs.
_UUID_NAMESPACE = uuid.UUID("0a3f9b2c-7d1e-4c5a-9e8d-1f7e6a4c2b30")


def _ztp_transform_row(row: tuple) -> tuple:
    """Prepend a deterministic id (uuid5 of the natural key) and updated_at."""
    zip_code, position_id, election_date = row[3], row[0], row[5]
    row_id = uuid.uuid5(_UUID_NAMESPACE, f"{zip_code}|{position_id}|{election_date}")
    return (str(row_id), datetime.now(UTC), *row)


def _prepend_timestamps(row: tuple) -> tuple:
    """id stays first; created_at/updated_at are synthesized. The Person and
    OfficeHolder marts carry no timestamps (the legacy writer stamped now()
    on every upsert, so daily wall-clock stamps preserve its semantics)."""
    now = datetime.now(UTC)
    return (row[0], now, now, *row[1:])


def _position_transform_row(row: tuple) -> tuple:
    """br_database_id is bigint in the mart but text in Postgres."""
    return (row[0], str(row[1]), *row[2:])


# ---------------------------------------------------------------------------
# Per-table extra quality checks (beyond the generic gate)
# ---------------------------------------------------------------------------


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


def check_race_window(min_election_date, max_election_date, today) -> None:
    """Pure; unit-tested. Bounds mirror m_election_api__race's WHERE window
    (the source of truth); the pads absorb calendar drift and build-vs-gate
    date skew."""
    if min_election_date is None or max_election_date is None:
        raise ValueError("staged Race is empty; refusing to swap")
    if min_election_date < today - timedelta(days=6 * 365 + 32):
        raise ValueError(f"staged min election_date {min_election_date} outside window")
    if max_election_date > today + timedelta(days=2 * 365 + 32):
        raise ValueError(f"staged max election_date {max_election_date} outside window")


def _race_extra_checks(conn, spec: TableSyncSpec, loaded_count: int) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT MIN(election_date)::date, MAX(election_date)::date "
            f'FROM "{spec.staging_schema}"."{spec.new_table}"'
        )
        min_d, max_d = cur.fetchone()
    finally:
        cur.close()
    check_race_window(min_d, max_d, date.today())


# ---------------------------------------------------------------------------
# Table declarations
# ---------------------------------------------------------------------------

ZTP_SOURCE_COLUMNS = (
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
)
ZTP_TARGET_COLUMNS = ("id", "updated_at", *ZTP_SOURCE_COLUMNS)

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
            inbound_fkeys=(
                InboundForeignKey(
                    child_table="Race",
                    constraint_name="Race_place_id_fkey",
                    child_column="place_id",
                ),
            ),
        ),
        source_model="m_election_api__place",
        source_columns=(
            "id",
            "created_at",
            "updated_at",
            "br_database_id",
            "name",
            "slug",
            "geoid",
            "mtfcc",
            "state",
            "city_largest",
            "county_name",
            "population",
            "density",
            "income_household_median",
            "unemployment_rate",
            "home_value",
            "parent_id",
        ),
        gate=QualityGate(cold_start_floor=40_000),
        swap_gated=True,
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
            inbound_fkeys=(
                InboundForeignKey(
                    child_table="Position",
                    constraint_name="Position_district_id_fkey",
                    child_column="district_id",
                ),
                InboundForeignKey(
                    child_table="Projected_Turnout",
                    constraint_name="Projected_Turnout_district_id_fkey",
                    child_column="district_id",
                    on_clause="ON UPDATE CASCADE ON DELETE RESTRICT",
                ),
                InboundForeignKey(
                    child_table="DistrictTopIssue",
                    constraint_name="DistrictTopIssue_district_id_fkey",
                    child_column="district_id",
                    on_clause="ON UPDATE CASCADE ON DELETE RESTRICT",
                ),
            ),
        ),
        source_model="m_election_api__district",
        source_columns=(
            "id",
            "created_at",
            "updated_at",
            "state",
            "l2_district_type",
            "l2_district_name",
            "registered_voters",
            "unique_cellphones",
            "unique_landlines",
        ),
        gate=QualityGate(cold_start_floor=100_000),
        partition_column="state",
        swap_gated=True,
    ),
    MartSync(
        group_id="issue",
        spec=TableSyncSpec(
            target_table="Issue",
            fkeys=(ForeignKey("Issue_parent_id_fkey", "parent_id", "Issue"),),
            inbound_fkeys=(
                InboundForeignKey(
                    child_table="Stance",
                    constraint_name="Stance_issue_id_fkey",
                    child_column="issue_id",
                    on_clause="ON UPDATE CASCADE ON DELETE RESTRICT",
                ),
            ),
        ),
        source_model="m_election_api__issue",
        source_columns=(
            "id",
            "created_at",
            "updated_at",
            "br_database_id",
            "expanded_text",
            "key",
            "name",
            "parent_id",
        ),
        # The BR issue taxonomy is ~20 rows; the ratio gate does the real work.
        gate=QualityGate(cold_start_floor=10),
        swap_gated=True,
    ),
    MartSync(
        group_id="person",
        spec=TableSyncSpec(
            target_table="Person",
            indexes=(
                Index("Person_slug_idx", "(slug)"),
                Index("Person_gp_api_user_id_idx", "(gp_api_user_id)"),
            ),
            inbound_fkeys=(
                InboundForeignKey(
                    child_table="Candidacy",
                    constraint_name="Candidacy_person_id_fkey",
                    child_column="person_id",
                ),
                # OfficeHolder.person_id is NOT NULL: orphans are deleted (the
                # FK's own ON DELETE action), not nulled.
                InboundForeignKey(
                    child_table="OfficeHolder",
                    constraint_name="OfficeHolder_person_id_fkey",
                    child_column="person_id",
                    on_clause="ON UPDATE CASCADE ON DELETE CASCADE",
                ),
            ),
        ),
        source_model="m_election_api__person",
        source_columns=(
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
        ),
        target_columns=(
            "id",
            "created_at",
            "updated_at",
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
        ),
        transform_row=_prepend_timestamps,
        partition_column="state",
        # Prisma-owned columns the loader does not supply: is_pledged keeps its
        # DEFAULT false, gp_api_user_id stays NULL. Both are unpopulated today;
        # when their ETL lands, the mart must start supplying them.
        gate=QualityGate(
            cold_start_floor=200_000,
            db_owned_columns=frozenset({"is_pledged", "gp_api_user_id"}),
        ),
        swap_gated=True,
    ),
    MartSync(
        group_id="position",
        spec=TableSyncSpec(
            target_table="Position",
            indexes=(Index("Position_br_position_id_key", "(br_position_id)", unique=True),),
            fkeys=(ForeignKey("Position_district_id_fkey", "district_id", "District"),),
            inbound_fkeys=(
                InboundForeignKey(
                    child_table="Race",
                    constraint_name="Race_position_id_fkey",
                    child_column="position_id",
                ),
                InboundForeignKey(
                    child_table="ZipToPosition",
                    constraint_name="ZipToPosition_position_id_fkey",
                    child_column="position_id",
                    on_clause="ON UPDATE CASCADE ON DELETE RESTRICT",
                ),
                InboundForeignKey(
                    child_table="OfficeHolder",
                    constraint_name="OfficeHolder_position_id_fkey",
                    child_column="position_id",
                ),
            ),
        ),
        source_model="m_election_api__position",
        # The mart also emits created_at/updated_at; live Position carries no
        # timestamp columns, so they are not selected.
        source_columns=(
            "id",
            "br_database_id",
            "br_position_id",
            "name",
            "state",
            "level",
            "district_id",
            "is_win_icp",
            "is_serve_icp",
            "salary",
        ),
        transform_row=_position_transform_row,
        partition_column="state",
        gate=QualityGate(cold_start_floor=200_000),
        parents=("district",),
        swap_gated=True,
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
            inbound_fkeys=(
                InboundForeignKey(
                    child_table="Candidacy",
                    constraint_name="Candidacy_race_id_fkey",
                    child_column="race_id",
                ),
            ),
        ),
        source_model="m_election_api__race",
        source_columns=(
            "id",
            "created_at",
            "updated_at",
            "br_hash_id",
            "br_database_id",
            "election_date",
            "state",
            "position_level",
            "normalized_position_name",
            "position_description",
            "filing_office_address",
            "filing_phone_number",
            "paperwork_instructions",
            "filing_requirements",
            "is_runoff",
            "is_primary",
            "partisan_type",
            "filing_date_start",
            "filing_date_end",
            "employment_type",
            "eligibility_requirements",
            "salary",
            "sub_area_name",
            "sub_area_value",
            "frequency",
            "place_id",
            "slug",
            "position_names",
            "position_geoid",
            "number_of_seats",
            "win_number",
            "is_partisan",
            "office_type",
            "official_office_name",
            "position_id",
            "office_level",
        ),
        # ~1M rows at the six-years-back retention; one state at a time bounds
        # worker memory. The int[]/text[] array columns arrive as numpy arrays
        # from the arrow-backed connector; the loader normalizes them to
        # Python lists for psycopg2.
        partition_column="state",
        gate=QualityGate(cold_start_floor=100_000),
        extra_checks=_race_extra_checks,
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
            inbound_fkeys=(
                InboundForeignKey(
                    child_table="Stance",
                    constraint_name="Stance_candidacy_id_fkey",
                    child_column="candidacy_id",
                ),
            ),
        ),
        source_model="m_election_api__candidacy",
        source_columns=(
            "id",
            "created_at",
            "updated_at",
            "br_database_id",
            "slug",
            "first_name",
            "last_name",
            "party",
            "place_name",
            "state",
            "image",
            "about",
            "urls",
            "election_frequency",
            "salary",
            "normalized_position_name",
            "position_name",
            "position_description",
            "gp_candidate_id",
            "email",
            "website_url",
            "is_incumbent",
            "race_id",
            "person_id",
        ),
        partition_column="state",
        gate=QualityGate(cold_start_floor=150_000),
        parents=("person", "race"),
        swap_gated=True,
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
        source_columns=(
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
        ),
        target_columns=(
            "id",
            "created_at",
            "updated_at",
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
        ),
        transform_row=_prepend_timestamps,
        partition_column="state",
        gate=QualityGate(cold_start_floor=100_000),
        parents=("person", "position"),
        swap_gated=True,
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
        source_columns=(
            "id",
            "created_at",
            "updated_at",
            "br_database_id",
            "stance_locale",
            "stance_reference_url",
            "stance_statement",
            "issue_id",
            "candidacy_id",
        ),
        partition_column="issue_id",
        gate=QualityGate(cold_start_floor=50_000),
        parents=("issue", "candidacy"),
        swap_gated=True,
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
        source_columns=ZTP_SOURCE_COLUMNS,
        target_columns=ZTP_TARGET_COLUMNS,
        transform_row=_ztp_transform_row,
        # Statewide coverage (DATA-1986) added ~260k rows; read one state at a
        # time so the worker's peak memory stays bounded as the mart grows.
        partition_column="state",
        gate=QualityGate(cold_start_floor=1_000, db_owned_columns=frozenset({"created_at"})),
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
        source_columns=(
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
        ),
        # ~5.1M rows; read one issue at a time (~68 partitions) to keep the
        # combined peak memory bounded when running alongside other loads.
        partition_column="issue",
        gate=QualityGate(
            cold_start_floor=100_000,
            db_owned_columns=frozenset({"created_at"}),
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
            pk_column="elected_office_id",
        ),
        source_model="m_election_api__elected_official_support",
        source_columns=(
            "elected_office_id",
            "support_constituents",
            "total_constituents",
            "created_at",
            "updated_at",
        ),
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
        # election_code arrives as the enum label text; the staging table is a
        # LIKE-clone of the live table, so the column keeps the "ElectionCode"
        # enum type and Postgres rejects any unknown label at insert.
        source_columns=(
            "id",
            "created_at",
            "updated_at",
            "election_year",
            "election_code",
            "projected_turnout",
            "inference_at",
            "model_version",
            "district_id",
        ),
        partition_column="election_year",
        gate=QualityGate(
            cold_start_floor=100_000,
            not_null_columns=("district_id", "election_year", "election_code"),
        ),
        extra_checks=_pt_extra_checks,
        parents=("district",),
    ),
)


# ---------------------------------------------------------------------------
# Task-group factory
# ---------------------------------------------------------------------------


def _build_group(table: MartSync) -> dict:
    """One build->load->index->gate[->cutover]->swap->drop_old task group.

    Returns handles to the tasks that participate in cross-group FK wiring.
    """
    spec = table.spec
    handles: dict = {}

    @task_group(group_id=table.group_id)
    def group():
        @task
        def build_staging() -> None:
            with _open_pg() as conn:
                create_staging_table(conn, spec)

        @task
        def load_staging() -> int:
            catalog = Variable.get("databricks_catalog")
            query = (
                f"SELECT {', '.join(table.source_columns)} "
                f"FROM `{catalog}`.`{DATABRICKS_SCHEMA}`.`{table.source_model}`"
            )
            with _open_pg() as conn:
                return bulk_insert_from_databricks(
                    conn,
                    spec,
                    source_query=query,
                    target_columns=table.insert_columns,
                    transform_row=table.transform_row,
                    partition_column=table.partition_column,
                )

        @task
        def build_indexes_and_fk() -> None:
            with _open_pg() as conn:
                apply_ddl(conn, spec.constraint_ddl())

        @task
        def quality_checks(loaded_count: int) -> None:
            with _open_pg() as conn:
                run_quality_checks(conn, spec, table.gate, loaded_count, table.insert_columns)
                if table.extra_checks:
                    table.extra_checks(conn, spec, loaded_count)

        @task.short_circuit
        def cutover_enabled() -> bool:
            # Placed AFTER quality_checks so every night while disabled is a
            # full dress rehearsal: staging built, loaded, indexed, gated;
            # only the swap is withheld.
            enabled = _swap_enabled(Variable.get(SWAP_GATE_VARIABLE, default="false"))
            if not enabled:
                t_log.info(
                    "%s swap disabled (rehearsal mode); staging left for parity checks",
                    spec.target_table,
                )
            return enabled

        @task
        def swap() -> None:
            with _open_pg() as conn:
                swap_staging_into_target(conn, spec)

        @task
        def drop_old() -> None:
            with _open_pg() as conn:
                drop_old_table(conn, spec)

        s = build_staging()
        loaded = load_staging()
        idx = build_indexes_and_fk()
        qc = quality_checks(loaded)
        sw = swap()
        do = drop_old()
        if table.swap_gated:
            s >> loaded >> idx >> qc >> cutover_enabled() >> sw >> do
        else:
            s >> loaded >> idx >> qc >> sw >> do
        handles["build_staging"] = s
        handles["build_indexes_and_fk"] = idx
        handles["swap"] = sw
        handles["drop_old"] = do

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
        "retries": 3,
        "retry_delay": duration(seconds=30),
    },
    tags=["election_api", "postgres"],
    is_paused_upon_creation=True,
)
def sync_election_api():
    handles = {table.group_id: _build_group(table) for table in TABLES}
    for table in TABLES:
        for parent in table.parents:
            # The child's staging FK must be created against the FRESH live
            # parent, after the parent's full swap cycle.
            handles[parent]["drop_old"] >> handles[table.group_id]["build_indexes_and_fk"]
            # A staging table left over from a prior run (rehearsal mode, or a
            # run that died before its swap) still carries an FK to the live
            # parent; the parent's swap would wedge on it, so it must wait for
            # build_staging to have dropped and recreated the child's staging.
            handles[table.group_id]["build_staging"] >> handles[parent]["swap"]


sync_election_api()
