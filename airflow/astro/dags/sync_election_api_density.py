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

`DistrictVoterDensity.district_id` references `District.id` in spirit, and the
ids do match (`m_people_api__district` is a view over
`m_election_api__district`, so the salted uuids are the same). But an actual FK
would not survive: `sync_election_api` renames `District` aside every night and
`drop_old` drops it `CASCADE`, which takes any FK pointing at it with no error
raised on either side. A PK-only table is the only safe shape for a table the
nightly set does not swap alongside District.

The cost is that a monthly density vintage can hold district_ids the nightly
District vintage has since dropped. That surfaces as a lookup returning
nothing, which the app already handles as an unavailable map.

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
`DistrictVoterDensity` and `DistrictVoterDensityMeta` must exist in the target
Postgres before the first run — `build_staging` clones the live table's shape.
They are owned by the election-api repo's Prisma schema and land when
election-api is deployed, not on PR merge. The column contract they must match
is the one in `people-api-loader/src/loader/people_api/schema/schema_spec.py`.
"""

from airflow.sdk import dag
from include.custom_functions.election_api_sync import MartSync, wire_sync_dag
from include.custom_functions.election_api_utils import QualityGate, TableSyncSpec
from pendulum import datetime as pendulum_datetime
from pendulum import duration

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


TABLES: tuple[MartSync, ...] = (
    MartSync(
        group_id="district_voter_density",
        spec=TableSyncSpec(
            target_table="DistrictVoterDensity",
            # The mart grain. The app reads by (district_id, resolution), which
            # the PK's leading columns cover, so no separate index is needed.
            pk_column=("district_id", "resolution", "h3_index"),
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
        extra_checks=_k_anonymity_checks,
    ),
    MartSync(
        group_id="district_voter_density_meta",
        spec=TableSyncSpec(
            target_table="DistrictVoterDensityMeta",
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
