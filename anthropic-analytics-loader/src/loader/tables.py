"""Delta table specs for the Claude Enterprise Analytics tables.

Table names/columns are ours (not a literal copy of Anthropic's own export format) -- Sigma's
"Claude spend" template maps source tables by hand in its Change sources step regardless, so exact
name matching isn't required. Each table also carries a `raw_json` column with the untouched API
row, so a new metric Anthropic adds later doesn't require a loader change to query it.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[tuple[str, str], ...]  # (column_name, sql_type)
    range_column: str  # column DELETE-then-INSERT range-clears on before a resync

    @property
    def column_names(self) -> list[str]:
        return [c for c, _ in self.columns]

    def ddl(self, catalog: str, schema: str) -> str:
        cols = ",\n    ".join(f"{c} {t}" for c, t in self.columns)
        return f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{self.name} (\n    {cols}\n) USING DELTA"


ORG_USAGE_REPORT = TableSpec(
    name="anthropic_analytics_org_usage_report",
    range_column="bucket_start",
    columns=(
        ("bucket_start", "TIMESTAMP"),
        ("bucket_end", "TIMESTAMP"),
        ("product", "STRING"),
        ("model", "STRING"),
        ("uncached_input_tokens", "BIGINT"),
        ("cache_read_input_tokens", "BIGINT"),
        ("cache_creation_1h_input_tokens", "BIGINT"),
        ("cache_creation_5m_input_tokens", "BIGINT"),
        ("output_tokens", "BIGINT"),
        ("requests", "BIGINT"),
        ("web_search_requests", "BIGINT"),
        ("ingested_at", "TIMESTAMP"),
        ("raw_json", "STRING"),
    ),
)

ORG_COST_REPORT = TableSpec(
    name="anthropic_analytics_org_cost_report",
    range_column="bucket_start",
    columns=(
        ("bucket_start", "TIMESTAMP"),
        ("bucket_end", "TIMESTAMP"),
        ("product", "STRING"),
        ("model", "STRING"),
        ("cost_type", "STRING"),
        ("token_type", "STRING"),
        ("amount", "DECIMAL(20,6)"),
        ("list_amount", "DECIMAL(20,6)"),
        ("currency", "STRING"),
        ("requests", "BIGINT"),
        ("ingested_at", "TIMESTAMP"),
        ("raw_json", "STRING"),
    ),
)

USER_COST_REPORT = TableSpec(
    name="anthropic_analytics_user_cost_report",
    range_column="period_start",
    columns=(
        ("period_start", "TIMESTAMP"),
        ("period_end", "TIMESTAMP"),
        ("user_id", "STRING"),
        ("user_email", "STRING"),
        ("user_name", "STRING"),
        ("user_deleted", "BOOLEAN"),
        ("amount", "DECIMAL(20,6)"),
        ("list_amount", "DECIMAL(20,6)"),
        ("currency", "STRING"),
        ("requests", "BIGINT"),
        ("ingested_at", "TIMESTAMP"),
        ("raw_json", "STRING"),
    ),
)

USERS_DAILY = TableSpec(
    name="anthropic_analytics_users_daily",
    range_column="activity_date",
    columns=(
        ("activity_date", "DATE"),
        ("user_id", "STRING"),
        ("user_email", "STRING"),
        ("chat_message_count", "BIGINT"),
        ("chat_distinct_skills_used_count", "BIGINT"),
        ("chat_distinct_connectors_used_count", "BIGINT"),
        ("claude_code_commit_count", "BIGINT"),
        ("claude_code_pull_request_count", "BIGINT"),
        ("claude_code_lines_added", "BIGINT"),
        ("claude_code_lines_removed", "BIGINT"),
        ("cowork_message_count", "BIGINT"),
        ("cowork_skills_used_count", "BIGINT"),
        ("cowork_connectors_used_count", "BIGINT"),
        ("web_search_count", "BIGINT"),
        ("ingested_at", "TIMESTAMP"),
        ("raw_json", "STRING"),
    ),
)

SUMMARIES = TableSpec(
    name="anthropic_analytics_summaries",
    range_column="starting_at",
    columns=(
        ("starting_at", "TIMESTAMP"),
        ("ending_at", "TIMESTAMP"),
        ("assigned_seat_count", "BIGINT"),
        ("pending_invite_count", "BIGINT"),
        ("daily_active_user_count", "BIGINT"),
        ("weekly_active_user_count", "BIGINT"),
        ("monthly_active_user_count", "BIGINT"),
        ("daily_adoption_rate", "DOUBLE"),
        ("weekly_adoption_rate", "DOUBLE"),
        ("monthly_adoption_rate", "DOUBLE"),
        ("chat_daily_active_user_count", "BIGINT"),
        ("chat_weekly_active_user_count", "BIGINT"),
        ("chat_monthly_active_user_count", "BIGINT"),
        ("claude_code_daily_active_user_count", "BIGINT"),
        ("claude_code_weekly_active_user_count", "BIGINT"),
        ("claude_code_monthly_active_user_count", "BIGINT"),
        ("cowork_daily_active_user_count", "BIGINT"),
        ("cowork_weekly_active_user_count", "BIGINT"),
        ("cowork_monthly_active_user_count", "BIGINT"),
        ("claude_design_daily_active_user_count", "BIGINT"),
        ("claude_design_weekly_active_user_count", "BIGINT"),
        ("claude_design_monthly_active_user_count", "BIGINT"),
        ("office_agent_daily_active_user_count", "BIGINT"),
        ("office_agent_weekly_active_user_count", "BIGINT"),
        ("office_agent_monthly_active_user_count", "BIGINT"),
        ("science_daily_active_user_count", "BIGINT"),
        ("science_weekly_active_user_count", "BIGINT"),
        ("science_monthly_active_user_count", "BIGINT"),
        ("science_entitled_user_count", "BIGINT"),
        ("ingested_at", "TIMESTAMP"),
        ("raw_json", "STRING"),
    ),
)

ALL_TABLES = {
    "org_usage_report": ORG_USAGE_REPORT,
    "org_cost_report": ORG_COST_REPORT,
    "user_cost_report": USER_COST_REPORT,
    "users_daily": USERS_DAILY,
    "summaries": SUMMARIES,
}
