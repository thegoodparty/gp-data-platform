"""
## Sync the voter-density marts from Databricks to the election-api PostgreSQL

The voter-density heat map is served by direct `(district_id, resolution)`
lookups at roughly 300k requests/day, so the data lives in election-api
Postgres rather than being queried from Databricks. This DAG loads it, using
the same build -> load -> index -> gate -> swap lifecycle as
`sync_election_api` (documented there, wired in
`include/custom_functions/election_api_sync.py`).

Separate from `sync_election_api` for two reasons:

- **Cadence.** The source marts are tagged `monthly` in dbt and only rebuild
  then, so a nightly or weekly pass would reload byte-identical data. This DAG
  is `@monthly`.
- **Size.** `DistrictVoterDensity` is ~55M rows (~2 GB compressed in
  Databricks) against the nightly set's largest at ~5M. A load that long has
  no business gating the nightly 13-table swap, and the nightly swap has no
  business waiting on it.

### These tables carry no foreign key, deliberately

`district_id` references `District.id`, and the ids do match: the density
marts are keyed off `m_people_api__district`, which is a plain view over
`m_election_api__district`, so the salted uuids are the same.

An actual FK constraint does not survive here, which was verified against real
Postgres rather than reasoned about. `sync_election_api` renames `District`
aside every night; that leaves a density FK pointing at the stale
`District_old`, and `drop_old` then drops it `CASCADE`, emitting a NOTICE and
no error on either DAG. Rows survive, the constraint does not, and orphans
become insertable from then on. Re-adding it each run would only paper over
the gap between runs, and would leave permanent Prisma drift.

`_district_reference_checks` delivers the same guarantee at the moment the FK
was actually doing work: every staged district_id is matched against live
District before the swap, and a miss fails closed. Between runs nothing writes
these tables (the API reads them), so ongoing enforcement protects nothing.

### Privacy

Cells are K-anonymized in dbt (`voter_density_k`, currently 10) and carry the
deterministic H3 cell centroid, never a voter position. `_k_anonymity_checks`
re-asserts the floor against the staged rows before the swap: a mart regression
that dropped the suppression filter would load *more* rows and pass every
generic gate.

### Connections, Variables, worker queue, deploy model:
As `sync_election_api`, with one addition:
- `election_api_density_swap_enabled` — this DAG's own cutover switch, separate
  from `election_api_swap_enabled` so the two sets can be rehearsed and cut
  over independently. Anything but "true" is rehearsal mode.

### Prerequisite:
`District_Voter_Density` and `District_Voter_Density_Meta` must exist in the
target Postgres before the first run — `build_staging` clones the live table's
shape. They are owned by the election-api repo's Prisma schema (omni #1581,
migration `20260831000000_add_district_voter_density`) and land when
election-api is deployed, not on PR merge. Note the Postgres names are
underscore-separated via `@@map`; the Prisma model names are not.

The PK and index declared below are the complete set the live table has after
a swap, because the staging clone copies none. They must stay in step with the
Prisma migration or the first sync silently drops what it does not declare.
"""

import logging

from airflow.sdk import dag
from include.custom_functions.election_api_sync import MartSync, wire_sync_dag
from include.custom_functions.election_api_utils import Index, QualityGate, TableSyncSpec
from pendulum import datetime as pendulum_datetime
from pendulum import duration

t_log = logging.getLogger("airflow.task")

SWAP_GATE_VARIABLE = "election_api_density_swap_enabled"

# Mirrors `voter_density_k` in dbt/project/dbt_project.yml, the value the marts
# suppress at. Duplicated rather than derived because the DAG has no dbt
# context; a change there without a change here fails the swap closed, which is
# the right direction for a privacy floor.
VOTER_DENSITY_K = 10


def _k_anonymity_checks(conn, spec: TableSyncSpec, loaded_count: int) -> None:
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


# Share of staged districts that may be missing a live District before the load
# is treated as a broken key rather than a timing gap. A handful of districts
# can reach the density mart before the nightly sync lands their District row;
# a fifth of them cannot.
MAX_ORPHAN_DISTRICT_SHARE = 0.01


def _district_reference_checks(conn, spec: TableSyncSpec, loaded_count: int) -> None:
    """Drop staged rows whose district_id has no live District, or fail if there
    are too many of them.

    This is what the absent FK bought, with the rule the election-api handoff
    doc asks for: skip a district the nightly sync has not landed yet, because
    the next run re-offers it, and do not fail a monthly load over it. Past
    MAX_ORPHAN_DISTRICT_SHARE the rows are not late — the mart has started
    minting its own ids — and pruning would hide that.
    """
    cur = conn.cursor()
    try:
        cur.execute(f'SELECT COUNT(DISTINCT district_id) FROM "{spec.staging_schema}"."{spec.new_table}"')
        staged_districts = cur.fetchone()[0]
        cur.execute(
            f"SELECT COUNT(*) FROM ("
            f"SELECT DISTINCT stg.district_id "
            f'FROM "{spec.staging_schema}"."{spec.new_table}" stg '
            f'LEFT JOIN "{spec.target_schema}"."District" d ON stg.district_id = d.id '
            f"WHERE d.id IS NULL"
            f") AS orphans"
        )
        orphan_districts = cur.fetchone()[0]

        share = orphan_districts / staged_districts if staged_districts else 0
        if share > MAX_ORPHAN_DISTRICT_SHARE:
            raise ValueError(
                f"{orphan_districts} of {staged_districts} staged districts have "
                f"no matching District ({share:.1%}) — refusing to swap"
            )
        if orphan_districts:
            cur.execute(
                f'DELETE FROM "{spec.staging_schema}"."{spec.new_table}" stg '
                f'WHERE NOT EXISTS (SELECT 1 FROM "{spec.target_schema}"."District" d '
                f"WHERE d.id = stg.district_id)"
            )
            conn.commit()
            t_log.info(
                "Pruned %d rows across %d districts not yet landed by sync_election_api",
                cur.rowcount,
                orphan_districts,
            )
    finally:
        cur.close()


def _cell_extra_checks(conn, spec: TableSyncSpec, loaded_count: int) -> None:
    """The cells table carries both guards; neither displaces the other."""
    _k_anonymity_checks(conn, spec, loaded_count)
    _district_reference_checks(conn, spec, loaded_count)


TABLES: tuple[MartSync, ...] = (
    MartSync(
        group_id="district_voter_density",
        spec=TableSyncSpec(
            # Prisma model DistrictVoterDensity, @@map'd to this table name.
            target_table="District_Voter_Density",
            # The mart grain.
            pk_column=("district_id", "resolution", "h3_index"),
            # The serving lookup. The PK's leading columns would satisfy it,
            # but this index is narrower by the h3_index string, so the same
            # lookup touches fewer pages and holds its place in cache on RDS.
            # Declared by the Prisma migration; the staging clone copies no
            # indexes, so the sync has to rebuild it or the first swap drops it.
            indexes=(
                Index(
                    "District_Voter_Density_district_id_resolution_idx",
                    "(district_id, resolution)",
                ),
            ),
        ),
        source_model="m_people_api__district_voter_density",
        # ~55M rows; read one state at a time so no single server-side result
        # set holds the whole mart. Unpartitioned is what OOM-kills these tasks.
        partition_column="state",
        # No id-overlap floor: the key is composite and natural, with no minted
        # id an external consumer could hold.
        gate=QualityGate(
            cold_start_floor=40_000_000,
            not_null_columns=("lat", "lng", "voter_count"),
        ),
        extra_checks=_cell_extra_checks,
    ),
    MartSync(
        group_id="district_voter_density_meta",
        spec=TableSyncSpec(
            # Prisma model DistrictVoterDensityMeta, @@map'd to this table name.
            target_table="District_Voter_Density_Meta",
            # One row per district per published resolution.
            pk_column=("district_id", "resolution"),
        ),
        source_model="m_people_api__district_voter_density_meta",
        # ~500k rows across four resolutions; small enough to read in one pass.
        gate=QualityGate(
            cold_start_floor=400_000,
            # coverage drives the app's resolution choice; a NULL there hides
            # the map for that district.
            not_null_columns=("coverage", "total_voters"),
        ),
        extra_checks=_district_reference_checks,
    ),
)


@dag(
    start_date=pendulum_datetime(2026, 9, 1, tz="UTC"),
    # Matches the `monthly` tag on the source marts; a shorter schedule would
    # reload identical data.
    schedule="@monthly",
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
    tags=["election_api", "postgres", "voter_density"],
    is_paused_upon_creation=True,
)
def sync_election_api_density():
    wire_sync_dag(TABLES, SWAP_GATE_VARIABLE)


sync_election_api_density()
