"""
## Sync ALL election-api marts from Databricks to PostgreSQL

Rebuilds the entire election-api Postgres graph from its dbt marts in Databricks
each run and swaps it in atomically. This DAG is the sole delivery path for the
election-api database; it replaces the legacy dbt python writer
`write__election_api_db.py` (removed).

Every table follows the shared build-and-swap lifecycle (see
`include/custom_functions/election_api_utils.py`):

1. **build_staging** — drop & recreate `staging."<Table>_new"` LIKE the live
   target (no indexes — fast bulk-insert).
2. **load_staging** — stream the source mart from Databricks into staging.
3. **build_indexes** — add the PK, unique, and secondary indexes.
4. **build_fks** — add FK constraints. Each FK references its parent's
   `staging."<Parent>_new"` (NOT the live parent), so the whole staged graph is
   internally consistent before the swap. Runs only after EVERY table's indexes
   are built (the `all_indexes_built` barrier), so each referenced parent has its
   PK and full row set — making FK creation itself a hard referential gate.
5. **quality_checks** — per-table row-count / coverage floors.
6. **swap_all** — ONE transaction renames the whole set (`_new` -> live, live ->
   `_old`) in dependency order (Person and the other parents first). Readers see
   either the entire prior graph or the entire new graph, never a mix. Sibling FKs
   follow their parent into the live schema by OID.
7. **drop_all_old** — drop the archived `_old` set, children-first.

### FK dependency order (ORDERED_SPECS, parents first)
Person, District, Place, Issue -> Position -> Race, OfficeHolder -> Candidacy ->
Stance, plus the leaves ZipToPosition/DistrictTopIssue/Projected_Turnout (-> Position
or District) and the standalone Elected_Office_Support.

### Connections (set in Astro Environment Manager):
- `databricks` / `databricks_dev` (Generic) — Databricks OAuth M2M.
- `gp_bastion_host` (SSH) — bastion for tunneling to PostgreSQL.
- `election_api_db` (Postgres) — election-api database credentials.

### Variables (set in Astro Environment Manager):
- `databricks_conn_id` — selects the Databricks connection.
- `databricks_catalog` — Databricks catalog name.
- `election_api_bastion_conn_id` (optional) — SSH bastion to tunnel through.
  Defaults to `gp_bastion_host`. Set to an empty string for local dev on VPN.

The source schema is hardcoded to `dbt` (the production-quality marts), not
`databricks_dbt_schema` (which points at in-flight dbt build artifacts).

### Deploy dependency:
The election-api Postgres schema is owned by the election-api repo; Prisma
migrations apply on election-api deploy, not on PR merge. `build_staging` clones
each `_new` via `CREATE TABLE ... (LIKE public."<T>")`, so the live table must
already exist. Confirm the person-spine migration (Person / OfficeHolder /
Candidacy.person_id / Position.salary) is applied on the target `_prisma_migrations`
before running.
"""

import logging
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from airflow.sdk import Variable, dag, task, task_group
from include.custom_functions.election_api_utils import (
    TableSyncSpec,
    apply_ddl,
    bulk_insert_from_databricks,
    create_staging_table,
    drop_old_graph,
    swap_graph_into_target,
)
from include.custom_functions.postgres_utils import get_postgres_via_ssh
from pendulum import datetime as pendulum_datetime
from pendulum import duration

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
# Staging DDL builders — every FK references the SIBLING `_new` table so the
# whole staged graph is internally consistent before the atomic swap. A
# `DROP ... IF EXISTS` precedes each `ADD`/`CREATE` so a post-commit Airflow
# retry of a build task is idempotent (create_staging_table drops the table
# fresh each run, so on a normal run the DROP is a no-op).
# ---------------------------------------------------------------------------


def _staged(spec: TableSyncSpec) -> str:
    return f'"{spec.staging_schema}"."{spec.new_table}"'


def _pk_ddl(spec: TableSyncSpec, columns: str = "(id)") -> list[str]:
    name = spec.stage_name(spec.pk_name)
    return [
        f'ALTER TABLE {_staged(spec)} DROP CONSTRAINT IF EXISTS "{name}"',
        f'ALTER TABLE {_staged(spec)} ADD CONSTRAINT "{name}" PRIMARY KEY {columns}',
    ]


def _index_ddl(spec: TableSyncSpec, canonical: str, columns: str, unique: bool = False) -> list[str]:
    name = spec.stage_name(canonical)
    kind = "UNIQUE INDEX" if unique else "INDEX"
    return [
        f'DROP INDEX IF EXISTS "{spec.staging_schema}"."{name}"',
        f'CREATE {kind} "{name}" ON {_staged(spec)} {columns}',
    ]


def _fk_ddl(
    spec: TableSyncSpec,
    canonical: str,
    columns: str,
    parent: TableSyncSpec,
    on_delete: str,
) -> list[str]:
    name = spec.stage_name(canonical)
    ref = f'"{parent.staging_schema}"."{parent.new_table}" (id)'
    return [
        f'ALTER TABLE {_staged(spec)} DROP CONSTRAINT IF EXISTS "{name}"',
        f'ALTER TABLE {_staged(spec)} ADD CONSTRAINT "{name}" FOREIGN KEY {columns} '
        f"REFERENCES {ref} ON UPDATE CASCADE ON DELETE {on_delete}",
    ]


# ---------------------------------------------------------------------------
# Quality gates (pure decision logic where possible; a task gathers the inputs)
# ---------------------------------------------------------------------------


def _prior_live_count(conn, spec: TableSyncSpec) -> int:
    """DISTINCT-safe count of the current live table; 0 if it doesn't exist yet."""
    cur = conn.cursor()
    try:
        cur.execute("SELECT to_regclass(%s)", (f'"{spec.target_schema}"."{spec.target_table}"',))
        if cur.fetchone()[0] is None:
            return 0
        cur.execute(f'SELECT COUNT(*) FROM "{spec.target_schema}"."{spec.target_table}"')
        return cur.fetchone()[0]
    finally:
        cur.close()


def _ratio_gate(
    loaded_count: int,
    prior_count: int,
    cold_floor: int,
    table: str,
    min_ratio: float = 0.5,
) -> None:
    """Refuse the swap on a coverage collapse vs. the prior live table; on a
    true cold start (no prior), refuse an implausibly small load. Pure and
    unit-tested; the quality task gathers `prior_count` from Postgres."""
    if prior_count > 0:
        ratio = loaded_count / prior_count
        if ratio < min_ratio:
            raise ValueError(
                f"{table}: loaded {loaded_count} rows, prior live had "
                f"{prior_count} (ratio {ratio:.2f}) — refusing to swap"
            )
    elif loaded_count < cold_floor:
        raise ValueError(
            f"{table}: cold-start load of {loaded_count} rows (<{cold_floor}) — refusing to swap"
        )


def _pt_quality_gate(loaded_count: int, dup_keys: int, prior_key_count: int, null_keys: int) -> None:
    """Decide whether staged Projected_Turnout data may swap into place.

    Pure decision logic (unit-tested); the quality task gathers the inputs from
    Postgres. Raises ValueError (task fails, no swap) when:

    - ``null_keys`` > 0 — staged rows with a NULL district_id, election_year, or
      election_code.
    - ``dup_keys`` > 0 — duplicate (district_id, election_year, election_code)
      keys: the election-api consumer does not disambiguate model_version, so a
      duplicate key would make it serve an arbitrary row.
    - loaded rows fall below half of ``prior_key_count`` (the prior live table's
      DISTINCT natural-key count; 0 on a cold start). Keys-vs-keys lets a
      legitimate dedupe cutover pass while refusing a coverage collapse.
    - cold start (no prior) with an implausibly small load.
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


def _make_ratio_quality(cold_floor: int) -> Callable[[Any, TableSyncSpec, int], None]:
    """Standard quality gate: prior-live-ratio with a cold-start floor. Row-level
    referential integrity is enforced separately by FK validation at build_fks."""

    def _fn(conn, spec: TableSyncSpec, loaded_count: int) -> None:
        _ratio_gate(loaded_count, _prior_live_count(conn, spec), cold_floor, spec.target_table)

    return _fn


def _ztp_quality(conn, spec: TableSyncSpec, loaded_count: int) -> None:
    if loaded_count < 1_000:
        raise ValueError(f"Only {loaded_count} rows loaded (<1000) — refusing to swap")
    cur = conn.cursor()
    try:
        cur.execute(f'SELECT COUNT(DISTINCT state) FROM "{spec.staging_schema}"."{spec.new_table}"')
        distinct_states = cur.fetchone()[0]
        if distinct_states < 30:
            raise ValueError(f"Only {distinct_states} distinct states — refusing to swap")
    finally:
        cur.close()


def _dti_quality(conn, spec: TableSyncSpec, loaded_count: int) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT COUNT(DISTINCT issue), "
            f"COUNT(*) FILTER (WHERE is_local IS NULL OR is_regional IS NULL "
            f"OR is_state IS NULL OR is_federal IS NULL) "
            f'FROM "{spec.staging_schema}"."{spec.new_table}"'
        )
        distinct_issues, null_flag_rows = cur.fetchone()
        cur.execute("SELECT to_regclass(%s)", (f'"{spec.target_schema}"."{spec.target_table}"',))
        if cur.fetchone()[0] is not None:
            cur.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT issue) "
                f'FROM "{spec.target_schema}"."{spec.target_table}"'
            )
            prior_count, prior_issues = cur.fetchone()
        else:
            prior_count, prior_issues = 0, 0
    finally:
        cur.close()

    _ratio_gate(loaded_count, prior_count, 100_000, spec.target_table)
    if distinct_issues < prior_issues:
        raise ValueError(
            f"Staging has {distinct_issues} distinct issues, prior live had "
            f"{prior_issues} — refusing to swap"
        )
    if null_flag_rows > 0:
        raise ValueError(f"{null_flag_rows} staging rows have a NULL jurisdictional flag — refusing to swap")


def _eos_quality(conn, spec: TableSyncSpec, loaded_count: int) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT elected_office_id), "
            f"COUNT(*) FILTER (WHERE elected_office_id IS NULL) "
            f'FROM "{spec.staging_schema}"."{spec.new_table}"'
        )
        n, distinct_pos, null_pos = cur.fetchone()
    finally:
        cur.close()
    _ratio_gate(loaded_count, _prior_live_count(conn, spec), 500, spec.target_table)
    if null_pos > 0:
        raise ValueError(f"{null_pos} staging rows have a NULL elected_office_id — refusing to swap")
    if distinct_pos != n:
        raise ValueError(f"elected_office_id not unique ({distinct_pos} distinct of {n}) — refusing to swap")


def _pt_quality(conn, spec: TableSyncSpec, loaded_count: int) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT COUNT(*) FROM ("
            f"SELECT district_id, election_year, election_code "
            f'FROM "{spec.staging_schema}"."{spec.new_table}" '
            f"GROUP BY district_id, election_year, election_code HAVING COUNT(*) > 1"
            f") AS dupe_keys"
        )
        dup_keys = cur.fetchone()[0]
        cur.execute(
            f'SELECT COUNT(*) FROM "{spec.staging_schema}"."{spec.new_table}" '
            f"WHERE district_id IS NULL OR election_year IS NULL OR election_code IS NULL"
        )
        null_keys = cur.fetchone()[0]
        cur.execute("SELECT to_regclass(%s)", (f'"{spec.target_schema}"."{spec.target_table}"',))
        if cur.fetchone()[0] is not None:
            cur.execute(
                f"SELECT COUNT(*) FROM ("
                f"SELECT DISTINCT district_id, election_year, election_code "
                f'FROM "{spec.target_schema}"."{spec.target_table}") AS prior_keys'
            )
            prior_key_count = cur.fetchone()[0]
        else:
            prior_key_count = 0
    finally:
        cur.close()
    _pt_quality_gate(loaded_count, dup_keys, prior_key_count, null_keys)


# ---------------------------------------------------------------------------
# ZipToPosition — transform builds a deterministic uuid5 id + updated_at.
# ---------------------------------------------------------------------------

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
    row_id = uuid.uuid5(_UUID_NAMESPACE, f"{zip_code}|{position_id}|{election_date}")
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

EOS_COLUMNS = [
    "elected_office_id",
    "support_constituents",
    "total_constituents",
    "created_at",
    "updated_at",
]

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


# ---------------------------------------------------------------------------
# Table specs (constraint / index / FK names are the canonical Prisma names as
# they exist on the live election-api Postgres — verified against the deployed
# schema). spec.indexes lists every non-PK index (incl. unique-key indexes) so
# the swap renames them; spec.fkeys lists every FK constraint name.
# ---------------------------------------------------------------------------

PERSON = TableSyncSpec(target_table="Person", indexes=("Person_slug_idx",))
OFFICEHOLDER = TableSyncSpec(
    target_table="OfficeHolder",
    indexes=("OfficeHolder_person_id_idx", "OfficeHolder_position_id_idx"),
    fkeys=("OfficeHolder_person_id_fkey", "OfficeHolder_position_id_fkey"),
)
PLACE = TableSyncSpec(
    target_table="Place",
    indexes=("Place_geoid_key", "Place_slug_key"),
    fkeys=("Place_parent_id_fkey",),
)
DISTRICT = TableSyncSpec(
    target_table="District",
    indexes=("District_state_l2_district_type_l2_district_name_key",),
)
ISSUE = TableSyncSpec(target_table="Issue", fkeys=("Issue_parent_id_fkey",))
POSITION = TableSyncSpec(
    target_table="Position",
    indexes=("Position_br_position_id_key", "Position_place_id_idx"),
    fkeys=("Position_district_id_fkey", "Position_place_id_fkey"),
)
RACE = TableSyncSpec(
    target_table="Race",
    indexes=("Race_br_hash_id_idx", "Race_position_id_idx", "Race_slug_idx"),
    fkeys=("Race_place_id_fkey", "Race_position_id_fkey"),
)
CANDIDACY = TableSyncSpec(
    target_table="Candidacy",
    indexes=("Candidacy_person_id_idx", "Candidacy_slug_key"),
    fkeys=("Candidacy_person_id_fkey", "Candidacy_race_id_fkey"),
)
STANCE = TableSyncSpec(
    target_table="Stance",
    fkeys=("Stance_candidacy_id_fkey", "Stance_issue_id_fkey"),
)
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
DTI = TableSyncSpec(
    target_table="DistrictTopIssue",
    indexes=("DistrictTopIssue_district_id_issue_key",),
    fkeys=("DistrictTopIssue_district_id_fkey",),
)
PT = TableSyncSpec(
    target_table="Projected_Turnout",
    indexes=("Projected_Turnout_district_id_election_year_idx",),
    fkeys=("Projected_Turnout_district_id_fkey",),
)
EOS = TableSyncSpec(target_table="Elected_Office_Support")


# ---------------------------------------------------------------------------
# Column lists — for tables with no row transform, the source SELECT order and
# the Postgres insert order are the same list (mart column names == DB columns).
# ---------------------------------------------------------------------------

PERSON_COLUMNS = [
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
]

OFFICEHOLDER_COLUMNS = [
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
]

CANDIDACY_COLUMNS = [
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
    "person_id",
    "email",
    "website_url",
    "is_incumbent",
    "race_id",
]

POSITION_COLUMNS = [
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
]

RACE_COLUMNS = [
    "id",
    "created_at",
    "updated_at",
    "br_hash_id",
    "br_database_id",
    "election_date",
    "state",
    "position_geoid",
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
    "position_id",
    "number_of_seats",
    "win_number",
    "is_partisan",
    "office_type",
    "official_office_name",
    "office_level",
]

PLACE_COLUMNS = [
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
]

DISTRICT_COLUMNS = [
    "id",
    "created_at",
    "updated_at",
    "state",
    "l2_district_type",
    "l2_district_name",
    "registered_voters",
    "unique_cellphones",
    "unique_landlines",
]

ISSUE_COLUMNS = [
    "id",
    "created_at",
    "updated_at",
    "br_database_id",
    "expanded_text",
    "key",
    "name",
    "parent_id",
]

STANCE_COLUMNS = [
    "id",
    "created_at",
    "updated_at",
    "br_database_id",
    "stance_locale",
    "stance_reference_url",
    "stance_statement",
    "issue_id",
    "candidacy_id",
]


# ---------------------------------------------------------------------------
# Pipeline descriptors
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TablePipeline:
    spec: TableSyncSpec
    group_id: str
    source_relation: str
    source_columns: Sequence[str]
    index_ddl: Callable[[], list[str]]
    fk_ddl: Callable[[], list[str]] | None = None
    transform_row: Callable[[tuple], tuple] | None = None
    target_columns: Sequence[str] | None = None
    partition_column: str | None = None
    quality_fn: Callable[[Any, TableSyncSpec, int], None] | None = None

    @property
    def insert_columns(self) -> Sequence[str]:
        return self.target_columns or self.source_columns


PIPELINES: list[TablePipeline] = [
    # ---- L0 parents -------------------------------------------------------
    TablePipeline(
        spec=PERSON,
        group_id="person",
        source_relation="m_election_api__person",
        source_columns=PERSON_COLUMNS,
        index_ddl=lambda: [*_pk_ddl(PERSON), *_index_ddl(PERSON, "Person_slug_idx", "(slug)")],
        partition_column="state",
        quality_fn=_make_ratio_quality(1_000),
    ),
    TablePipeline(
        spec=DISTRICT,
        group_id="district",
        source_relation="m_election_api__district",
        source_columns=DISTRICT_COLUMNS,
        index_ddl=lambda: [
            *_pk_ddl(DISTRICT),
            *_index_ddl(
                DISTRICT,
                "District_state_l2_district_type_l2_district_name_key",
                "(state, l2_district_type, l2_district_name)",
                unique=True,
            ),
        ],
        partition_column="state",
        quality_fn=_make_ratio_quality(100),
    ),
    TablePipeline(
        spec=PLACE,
        group_id="place",
        source_relation="m_election_api__place",
        source_columns=PLACE_COLUMNS,
        index_ddl=lambda: [
            *_pk_ddl(PLACE),
            *_index_ddl(PLACE, "Place_geoid_key", "(geoid)", unique=True),
            *_index_ddl(PLACE, "Place_slug_key", "(slug)", unique=True),
        ],
        fk_ddl=lambda: _fk_ddl(PLACE, "Place_parent_id_fkey", "(parent_id)", PLACE, "SET NULL"),
        partition_column="state",
        quality_fn=_make_ratio_quality(100),
    ),
    TablePipeline(
        spec=ISSUE,
        group_id="issue",
        source_relation="m_election_api__issue",
        source_columns=ISSUE_COLUMNS,
        index_ddl=lambda: _pk_ddl(ISSUE),
        fk_ddl=lambda: _fk_ddl(ISSUE, "Issue_parent_id_fkey", "(parent_id)", ISSUE, "SET NULL"),
        quality_fn=_make_ratio_quality(10),
    ),
    # ---- L1 ---------------------------------------------------------------
    TablePipeline(
        spec=POSITION,
        group_id="position",
        source_relation="m_election_api__position",
        source_columns=POSITION_COLUMNS,
        index_ddl=lambda: [
            *_pk_ddl(POSITION),
            *_index_ddl(POSITION, "Position_br_position_id_key", "(br_position_id)", unique=True),
            *_index_ddl(POSITION, "Position_place_id_idx", "(place_id)"),
        ],
        fk_ddl=lambda: [
            *_fk_ddl(POSITION, "Position_district_id_fkey", "(district_id)", DISTRICT, "SET NULL"),
            *_fk_ddl(POSITION, "Position_place_id_fkey", "(place_id)", PLACE, "SET NULL"),
        ],
        partition_column="state",
        quality_fn=_make_ratio_quality(1_000),
    ),
    # ---- L2 ---------------------------------------------------------------
    TablePipeline(
        spec=RACE,
        group_id="race",
        source_relation="m_election_api__race",
        source_columns=RACE_COLUMNS,
        index_ddl=lambda: [
            *_pk_ddl(RACE),
            *_index_ddl(RACE, "Race_br_hash_id_idx", "(br_hash_id)"),
            *_index_ddl(RACE, "Race_position_id_idx", "(position_id)"),
            *_index_ddl(RACE, "Race_slug_idx", "(slug)"),
        ],
        fk_ddl=lambda: [
            *_fk_ddl(RACE, "Race_place_id_fkey", "(place_id)", PLACE, "SET NULL"),
            *_fk_ddl(RACE, "Race_position_id_fkey", "(position_id)", POSITION, "SET NULL"),
        ],
        partition_column="state",
        quality_fn=_make_ratio_quality(100),
    ),
    TablePipeline(
        spec=OFFICEHOLDER,
        group_id="office_holder",
        source_relation="m_election_api__office_holder",
        source_columns=OFFICEHOLDER_COLUMNS,
        index_ddl=lambda: [
            *_pk_ddl(OFFICEHOLDER),
            *_index_ddl(OFFICEHOLDER, "OfficeHolder_person_id_idx", "(person_id)"),
            *_index_ddl(OFFICEHOLDER, "OfficeHolder_position_id_idx", "(position_id)"),
        ],
        fk_ddl=lambda: [
            *_fk_ddl(OFFICEHOLDER, "OfficeHolder_person_id_fkey", "(person_id)", PERSON, "CASCADE"),
            *_fk_ddl(OFFICEHOLDER, "OfficeHolder_position_id_fkey", "(position_id)", POSITION, "SET NULL"),
        ],
        partition_column="state",
        quality_fn=_make_ratio_quality(1_000),
    ),
    # ---- L3 ---------------------------------------------------------------
    TablePipeline(
        spec=CANDIDACY,
        group_id="candidacy",
        source_relation="m_election_api__candidacy",
        source_columns=CANDIDACY_COLUMNS,
        index_ddl=lambda: [
            *_pk_ddl(CANDIDACY),
            *_index_ddl(CANDIDACY, "Candidacy_person_id_idx", "(person_id)"),
            *_index_ddl(CANDIDACY, "Candidacy_slug_key", "(slug)", unique=True),
        ],
        fk_ddl=lambda: [
            *_fk_ddl(CANDIDACY, "Candidacy_person_id_fkey", "(person_id)", PERSON, "SET NULL"),
            *_fk_ddl(CANDIDACY, "Candidacy_race_id_fkey", "(race_id)", RACE, "SET NULL"),
        ],
        partition_column="state",
        quality_fn=_make_ratio_quality(1_000),
    ),
    # ---- L4 ---------------------------------------------------------------
    TablePipeline(
        spec=STANCE,
        group_id="stance",
        source_relation="m_election_api__stance",
        source_columns=STANCE_COLUMNS,
        index_ddl=lambda: _pk_ddl(STANCE),
        fk_ddl=lambda: [
            *_fk_ddl(STANCE, "Stance_candidacy_id_fkey", "(candidacy_id)", CANDIDACY, "SET NULL"),
            *_fk_ddl(STANCE, "Stance_issue_id_fkey", "(issue_id)", ISSUE, "RESTRICT"),
        ],
        quality_fn=_make_ratio_quality(10),
    ),
    # ---- leaves (parents Position / District are now swapped too) --------
    TablePipeline(
        spec=ZTP,
        group_id="zip_to_position",
        source_relation="m_election_api__zip_to_position",
        source_columns=ZTP_SOURCE_COLUMNS,
        target_columns=ZTP_TARGET_COLUMNS,
        transform_row=_ztp_transform_row,
        index_ddl=lambda: [
            *_pk_ddl(ZTP),
            *_index_ddl(ZTP, "ZipToPosition_zip_code_idx", "(zip_code)"),
            *_index_ddl(ZTP, "ZipToPosition_position_id_idx", "(position_id)"),
            *_index_ddl(
                ZTP, "ZipToPosition_zip_code_pct_districtzip_to_zip_idx", "(zip_code, pct_districtzip_to_zip)"
            ),
            *_index_ddl(
                ZTP,
                "ZipToPosition_zip_code_position_id_election_date_key",
                "(zip_code, position_id, election_date) NULLS NOT DISTINCT",
                unique=True,
            ),
        ],
        fk_ddl=lambda: _fk_ddl(ZTP, "ZipToPosition_position_id_fkey", "(position_id)", POSITION, "RESTRICT"),
        partition_column="state",
        quality_fn=_ztp_quality,
    ),
    TablePipeline(
        spec=DTI,
        group_id="district_top_issues",
        source_relation="m_election_api__district_top_issues",
        source_columns=DTI_COLUMNS,
        index_ddl=lambda: [
            *_pk_ddl(DTI),
            *_index_ddl(DTI, "DistrictTopIssue_district_id_issue_key", "(district_id, issue)", unique=True),
        ],
        fk_ddl=lambda: _fk_ddl(
            DTI, "DistrictTopIssue_district_id_fkey", "(district_id)", DISTRICT, "RESTRICT"
        ),
        partition_column="issue",
        quality_fn=_dti_quality,
    ),
    TablePipeline(
        spec=PT,
        group_id="projected_turnout",
        source_relation="m_election_api__projected_turnout",
        source_columns=PT_COLUMNS,
        index_ddl=lambda: [
            *_pk_ddl(PT),
            *_index_ddl(
                PT, "Projected_Turnout_district_id_election_year_idx", "(district_id, election_year)"
            ),
        ],
        fk_ddl=lambda: _fk_ddl(
            PT, "Projected_Turnout_district_id_fkey", "(district_id)", DISTRICT, "RESTRICT"
        ),
        partition_column="election_year",
        quality_fn=_pt_quality,
    ),
    TablePipeline(
        spec=EOS,
        group_id="elected_official_support",
        source_relation="m_election_api__elected_official_support",
        source_columns=EOS_COLUMNS,
        index_ddl=lambda: _pk_ddl(EOS, "(elected_office_id)"),
        quality_fn=_eos_quality,
    ),
]

# Dependency order (parents first). Drives the single-transaction swap and the
# children-first drop of the archived `_old` set.
ORDERED_SPECS = [p.spec for p in PIPELINES]


def _make_table_group(p: TablePipeline):
    @task_group(group_id=p.group_id)
    def _group():
        @task
        def build_staging() -> None:
            with _open_pg() as conn:
                create_staging_table(conn, p.spec)

        @task
        def load_staging() -> int:
            catalog = Variable.get("databricks_catalog")
            col_list = ", ".join(p.source_columns)
            query = f"SELECT {col_list} FROM `{catalog}`.`{DATABRICKS_SCHEMA}`.`{p.source_relation}`"
            with _open_pg() as conn:
                return bulk_insert_from_databricks(
                    conn,
                    p.spec,
                    source_query=query,
                    target_columns=p.insert_columns,
                    transform_row=p.transform_row,
                    partition_column=p.partition_column,
                )

        @task
        def build_indexes() -> None:
            with _open_pg() as conn:
                apply_ddl(conn, p.index_ddl())

        @task
        def build_fks() -> None:
            if p.fk_ddl is not None:
                with _open_pg() as conn:
                    apply_ddl(conn, p.fk_ddl())

        @task
        def quality_checks(loaded_count: int) -> None:
            if p.quality_fn is not None:
                with _open_pg() as conn:
                    p.quality_fn(conn, p.spec, loaded_count)

        s = build_staging()
        n = load_staging()
        i = build_indexes()
        f = build_fks()
        q = quality_checks(n)
        s >> n >> i
        f >> q
        return {"indexes": i, "fks": f, "quality": q}

    return _group()


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
    groups = [_make_table_group(p) for p in PIPELINES]

    @task
    def all_indexes_built() -> None:
        """Barrier: every table's PK/indexes exist and every table is fully
        loaded before any FK validates against a sibling `_new` parent."""

    @task
    def all_quality_passed() -> None:
        """Barrier: all per-table gates cleared before the graph swap."""

    @task
    def swap_all() -> None:
        with _open_pg() as conn:
            swap_graph_into_target(conn, ORDERED_SPECS)

    @task
    def drop_all_old() -> None:
        with _open_pg() as conn:
            drop_old_graph(conn, ORDERED_SPECS)

    idx_done = all_indexes_built()
    qc_done = all_quality_passed()

    for g in groups:
        g["indexes"] >> idx_done
        idx_done >> g["fks"]
        g["quality"] >> qc_done

    qc_done >> swap_all() >> drop_all_old()


sync_election_api()
