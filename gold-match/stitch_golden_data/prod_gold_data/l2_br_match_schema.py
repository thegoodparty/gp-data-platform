"""Schema of record for the L2-to-BallotReady match results table.

The matcher owns this table; dbt reads it as a source and never creates it,
which is already the convention here -- the expired-voters loader does the
same. A row carrying a district is a match, a row carrying none is an attempt
that found nothing, and a technical error fails the run rather than being
persisted. `attempted_at` doubles as the run key, so rolling a run back is a
delete on that column.
"""

from shared.databricks_client import DatabricksClient

CATALOG = "goodparty_data_catalog"
SCHEMA = "model_predictions"
RESULTS_TABLE = "llm_l2_br_match_results"
RESULTS_TABLE_PATH = f"{CATALOG}.{SCHEMA}.{RESULTS_TABLE}"

RESULTS_DDL = f"""
create table if not exists {RESULTS_TABLE_PATH} (
    br_database_id int not null comment 'BallotReady office database id. int, matching the cast in stg_airbyte_source__ballotready_api_position.',
    l2_state string comment 'State of the matched district. Completes the district key to (state, type, name), the grain the universe, the district mart and the overrides seed all use. Null when the attempt found nothing.',
    l2_district_type string comment 'Null when the attempt found nothing.',
    l2_district_name string comment 'Null when the attempt found nothing. Whether this is populated is what says the office matched.',
    confidence bigint comment 'Integer score, observed 0-100, not a 0-1 float. Read by the position mart confidence gates.',
    attempted_at timestamp not null comment 'When the attempt was made. Doubles as the run key: one run stamps one value across every row it writes, so a rollback is a delete on this column.'
)
"""


def ensure_results_table(databricks: DatabricksClient | None = None) -> None:
    """Create the table if absent. Run once, by hand, when provisioning.

    Needs `USE SCHEMA` and `CREATE TABLE`; the write path afterwards needs only
    `INSERT`. Changing the schema of an existing table is a migration, not a
    rerun of this.
    """
    (databricks or DatabricksClient()).execute_query(RESULTS_DDL)


RUN_LOG_TABLE = "llm_l2_br_match_run_log"
RUN_LOG_TABLE_PATH = f"{CATALOG}.{SCHEMA}.{RUN_LOG_TABLE}"

RUN_LOG_DDL = f"""
create table if not exists {RUN_LOG_TABLE_PATH} (
    run_key timestamp not null comment 'This run key. Matches attempted_at in llm_l2_br_match_results for the rows it wrote.',
    policy_version string not null comment 'Identity of the whole cohort semantics: the pending-selector rule and the outcome write policy together. Bumped when either changes.',
    cohort_size int not null comment 'Pending offices after the quarantine-suppression and pre-cutover-boundary filters, before the ceiling check.',
    backlog_boundary_dropped int not null comment 'Offices dropped because the latest attempt predates the pre-cutover boundary; disposition belongs to the supervised tuning-era rerun, not this loop.',
    quarantine_dropped int not null comment 'Offices dropped because an active quarantine row currently suppresses them.',
    attempted int not null comment 'Offices actually sent to match_office this run: matched, abstained, and quarantined.',
    matched_written int not null comment 'Rows written this run carrying a district: a new match or a healed dead label.',
    abstains_written int not null comment 'Abstains written this run: a first abstain or a re-abstain, never a withdrawal.',
    withdrawals_held int not null comment 'Abstains NOT written because the prior serving answer was a match, held per the v1-hold-withdrawals policy until the rename-normalization lever lands.',
    quarantined_this_run int not null comment 'Offices whose match_office call raised the client typed response-shape error this run.',
    embedding_config string comment 'JSON: matcher.embedding_client.resolved_config().',
    llm_config string comment 'JSON: matcher.llm.resolved_config().',
    prompt_provenance string comment 'JSON: the pinned Braintrust prompt resolved provenance.',
    git_sha string not null comment 'From the GIT_SHA env baked in at image build. The entry point refuses to run without it.',
    created_at timestamp not null comment 'Wall-clock time this row was inserted, distinct from run_key (the run own logical time).'
)
comment 'One row per successfully written daily-loop run. Bookkeeping only: nothing on the serving path reads it, and a pre-write abort never reaches this insert, showing instead as a failed DAG run.'
"""


def ensure_run_log_table(databricks: DatabricksClient | None = None) -> None:
    """Create the table if absent. Run once, by hand, at activation."""
    (databricks or DatabricksClient()).execute_query(RUN_LOG_DDL)


QUARANTINE_TABLE = "llm_l2_br_match_quarantine"
QUARANTINE_TABLE_PATH = f"{CATALOG}.{SCHEMA}.{QUARANTINE_TABLE}"

QUARANTINE_DDL = f"""
create table if not exists {QUARANTINE_TABLE_PATH} (
    br_database_id int not null comment 'BallotReady office database id, matching the cast used across the matcher tables.',
    reason_code string not null comment 'Bounded enum naming the failure class, never raw exception text.',
    retry_class string not null comment 'auto retries after 30 days with no action; held releases only through a client fix landing or a manual UPDATE, both governed by warehouse ACLs.',
    first_failed_at timestamp not null comment 'When this office first entered quarantine under its current, unreleased episode.',
    last_failed_at timestamp not null comment 'Most recent failure timestamp. The 30-day auto-retry clock counts from this value.',
    released_at timestamp comment 'Null while suppression is active. Set when a retry succeeds or a manual release clears the row.',
    release_note string comment 'Free text recording how the row was released, for example an auto-retry success note.'
)
comment 'Per-office technical-failure quarantine for the daily matcher loop. Bookkeeping only, and one eligibility predicate defines suppression: released_at is null AND (retry_class = held OR last_failed_at is within 30 days).'
"""


def ensure_quarantine_table(databricks: DatabricksClient | None = None) -> None:
    """Create the table if absent. Run once, by hand, at activation."""
    (databricks or DatabricksClient()).execute_query(QUARANTINE_DDL)


if __name__ == "__main__":
    ensure_results_table()
    ensure_run_log_table()
    ensure_quarantine_table()
