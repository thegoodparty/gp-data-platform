"""Post-sync data-quality gates for the cost tables.

Each of these queries the tables as written (not the in-memory `rows` from this run), so they
catch corruption regardless of whether it came from this loader or a manual edit. Raising
`AssertionFailure` is meant to fail the sync (non-zero exit / failed Airflow task) -- these are
gates, not warnings.
"""

from __future__ import annotations

from decimal import Decimal

import structlog

from loader import databricks_writer as writer
from loader.config import Config
from loader.pricing import MAX_PUBLISHED_OUTPUT_PRICE_PER_TOKEN

log = structlog.get_logger()

# "agree within a few percent" per the investigation that motivated these checks.
CROSS_TABLE_TOLERANCE = Decimal("0.05")
# Usage revisions land within ~24h of the underlying event (see anthropic_client module docs);
# a full-range resync should reproduce the same totals to within noise, not swing wildly.
USAGE_NON_REGRESSION_TOLERANCE = Decimal("0.01")

_ORG_COST_REPORT = "anthropic_analytics_org_cost_report"
_USER_COST_REPORT = "anthropic_analytics_user_cost_report"
_ORG_USAGE_REPORT = "anthropic_analytics_org_usage_report"

_USAGE_TOKEN_COLUMNS = (
    "uncached_input_tokens",
    "cache_read_input_tokens",
    "cache_creation_1h_input_tokens",
    "cache_creation_5m_input_tokens",
    "output_tokens",
)


class AssertionFailure(RuntimeError):
    """A post-sync data-quality gate failed; the sync should be treated as failed."""


def _table(cfg: Config, name: str) -> str:
    return f"{cfg.catalog}.{cfg.schema}.{name}"


def _decimal(value: str | int | float | Decimal | None) -> Decimal:
    return Decimal(value) if value is not None else Decimal(0)


def assert_org_cost_report_token_type_grain(cfg: Config, range_start, range_end) -> None:
    """Under cost_type='tokens', every (bucket_start, product, model) must have exactly one row
    per token_type (5 total) -- anything else means group_by stopped requesting token_type, or
    something collapsed/duplicated rows after ingest.
    """
    sql = f"""
        SELECT bucket_start, product, model, COUNT(DISTINCT token_type) AS n_token_types
        FROM {_table(cfg, _ORG_COST_REPORT)}
        WHERE cost_type = 'tokens'
          AND bucket_start >= {writer.sql_literal(range_start)}
          AND bucket_start < {writer.sql_literal(range_end)}
        GROUP BY bucket_start, product, model
        HAVING COUNT(DISTINCT token_type) <> 5
        LIMIT 20
    """
    bad = writer.query_rows(cfg.databricks_warehouse_id, sql)
    if bad:
        raise AssertionFailure(
            f"{len(bad)}+ (bucket_start, product, model) groups under cost_type='tokens' don't "
            f"have exactly 5 token_type rows; examples: {bad}"
        )


def assert_amount_within_list_amount(
    cfg: Config, table_name: str, range_column: str, range_start, range_end
) -> None:
    """amount (post-discount) must never exceed list_amount (pre-discount)."""
    sql = f"""
        SELECT COUNT(*)
        FROM {_table(cfg, table_name)}
        WHERE {range_column} >= {writer.sql_literal(range_start)}
          AND {range_column} < {writer.sql_literal(range_end)}
          AND amount > list_amount
    """
    rows = writer.query_rows(cfg.databricks_warehouse_id, sql)
    bad_count = int(rows[0][0]) if rows else 0
    if bad_count:
        raise AssertionFailure(f"{table_name}: {bad_count} rows have amount > list_amount")


def assert_cross_table_cost_agreement(cfg: Config, range_start, range_end) -> None:
    """org_cost_report and user_cost_report total spend for the same window must roughly agree --
    a real divergence usually means one side is mis-scaled or the two aren't covering the same
    range.
    """
    sql = f"""
        SELECT
            (SELECT SUM(amount) FROM {_table(cfg, _ORG_COST_REPORT)}
             WHERE bucket_start >= {writer.sql_literal(range_start)}
               AND bucket_start < {writer.sql_literal(range_end)}) AS org_total,
            (SELECT SUM(amount) FROM {_table(cfg, _USER_COST_REPORT)}
             WHERE period_start >= {writer.sql_literal(range_start)}
               AND period_start < {writer.sql_literal(range_end)}) AS user_total
    """
    rows = writer.query_rows(cfg.databricks_warehouse_id, sql)
    org_total = _decimal(rows[0][0])
    user_total = _decimal(rows[0][1])
    if org_total == 0 and user_total == 0:
        return
    denom = max(org_total, user_total)
    diff_pct = abs(org_total - user_total) / denom if denom else Decimal(0)
    if diff_pct > CROSS_TABLE_TOLERANCE:
        raise AssertionFailure(
            f"org_cost_report total ({org_total}) and user_cost_report total ({user_total}) "
            f"diverge by {diff_pct:.1%}, more than the {CROSS_TABLE_TOLERANCE:.0%} tolerance"
        )


def assert_cost_per_token_ceiling(cfg: Config, range_start, range_end) -> None:
    """Total token cost / total tokens must not exceed the highest published output-token price
    among the models we use, as a ceiling: no single token can legitimately cost more than that,
    regardless of our actual contract/discount.
    """
    sql = f"""
        SELECT
            (SELECT SUM(amount) FROM {_table(cfg, _ORG_COST_REPORT)}
             WHERE cost_type = 'tokens'
               AND bucket_start >= {writer.sql_literal(range_start)}
               AND bucket_start < {writer.sql_literal(range_end)}) AS total_cost,
            (SELECT SUM({" + ".join(_USAGE_TOKEN_COLUMNS)}) FROM {_table(cfg, _ORG_USAGE_REPORT)}
             WHERE bucket_start >= {writer.sql_literal(range_start)}
               AND bucket_start < {writer.sql_literal(range_end)}) AS total_tokens
    """
    rows = writer.query_rows(cfg.databricks_warehouse_id, sql)
    total_cost = _decimal(rows[0][0])
    total_tokens = _decimal(rows[0][1])
    if total_tokens == 0:
        return
    cost_per_token = total_cost / total_tokens
    if cost_per_token > MAX_PUBLISHED_OUTPUT_PRICE_PER_TOKEN:
        raise AssertionFailure(
            f"cost/token ({cost_per_token:.8f}) exceeds the published output-token price ceiling "
            f"({MAX_PUBLISHED_OUTPUT_PRICE_PER_TOKEN:.8f}); total_cost={total_cost} "
            f"total_tokens={total_tokens}"
        )


def snapshot_usage_totals(cfg: Config, range_start, range_end) -> tuple[Decimal, Decimal]:
    """Tokens/requests currently in org_usage_report for [range_start, range_end) -- call before
    `replace_range` deletes that window, then compare against `assert_usage_non_regression`.
    """
    sql = f"""
        SELECT SUM({" + ".join(_USAGE_TOKEN_COLUMNS)}), SUM(requests)
        FROM {_table(cfg, _ORG_USAGE_REPORT)}
        WHERE bucket_start >= {writer.sql_literal(range_start)}
          AND bucket_start < {writer.sql_literal(range_end)}
    """
    rows = writer.query_rows(cfg.databricks_warehouse_id, sql)
    if not rows:
        return Decimal(0), Decimal(0)
    return _decimal(rows[0][0]), _decimal(rows[0][1])


def assert_usage_non_regression(cfg: Config, before: tuple[Decimal, Decimal], range_start, range_end) -> None:
    """org_usage_report token/request totals for the just-resynced range must be unchanged
    (within noise from Anthropic's own late-arriving revisions) by whatever else this sync did.
    """
    before_tokens, before_requests = before
    after_tokens, after_requests = snapshot_usage_totals(cfg, range_start, range_end)
    if before_tokens == 0 and before_requests == 0:
        log.info("usage_non_regression_skipped", reason="no prior data for range")
        return
    for label, before_val, after_val in (
        ("tokens", before_tokens, after_tokens),
        ("requests", before_requests, after_requests),
    ):
        diff_pct = abs(after_val - before_val) / before_val if before_val else Decimal(0)
        if diff_pct > USAGE_NON_REGRESSION_TOLERANCE:
            raise AssertionFailure(
                f"org_usage_report {label} total moved from {before_val} to {after_val} "
                f"({diff_pct:.1%}), more than the {USAGE_NON_REGRESSION_TOLERANCE:.0%} tolerance"
            )
