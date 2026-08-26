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


if __name__ == "__main__":
    ensure_results_table()
