"""CLI: `anthropic-analytics-loader sync [--start-date ...] [--end-date ...]`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated

import structlog
import typer

from loader import anthropic_client as client
from loader import assertions
from loader import databricks_writer as writer
from loader.config import Config
from loader.tables import ALL_TABLES

log = structlog.get_logger()
app = typer.Typer()

# Enterprise Analytics API has no data before this date.
DATA_FLOOR = datetime(2026, 1, 1, tzinfo=UTC)

_TABLE_HELP = f"Subset of tables to sync (repeatable). Default: all of {', '.join(ALL_TABLES)}."


@app.command()
def sync(
    start_date: Annotated[
        str | None,
        typer.Option(help="UTC date YYYY-MM-DD, inclusive. Default: --lookback-days before end-date."),
    ] = None,
    end_date: Annotated[
        str | None, typer.Option(help="UTC date YYYY-MM-DD, exclusive. Default: today (UTC).")
    ] = None,
    lookback_days: Annotated[
        int, typer.Option(help="Used when --start-date is omitted (daily/incremental mode).")
    ] = 3,
    table: Annotated[list[str] | None, typer.Option(help=_TABLE_HELP)] = None,
) -> None:
    cfg = Config.from_env()
    now = client.utcnow()
    end = (
        datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=UTC)
        if end_date
        else now.replace(hour=0, minute=0, second=0, microsecond=0)
    )
    start = (
        datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC)
        if start_date
        else end - timedelta(days=lookback_days)
    )
    start = max(start, DATA_FLOOR)

    if start >= end:
        typer.echo(f"start ({start.date()}) must be before end ({end.date()})", err=True)
        raise typer.Exit(1)

    selected = table or list(ALL_TABLES)
    for key in selected:
        if key not in ALL_TABLES:
            typer.echo(f"unknown table {key!r}; choose from {', '.join(ALL_TABLES)}", err=True)
            raise typer.Exit(1)

    fetchers = {
        "org_usage_report": lambda: client.fetch_org_usage_report(
            cfg.anthropic_analytics_api_key, start, end, now
        ),
        "org_cost_report": lambda: client.fetch_org_cost_report(
            cfg.anthropic_analytics_api_key, start, end, now
        ),
        "user_cost_report": lambda: client.fetch_user_cost_report(
            cfg.anthropic_analytics_api_key, start, end, now
        ),
        "users_daily": lambda: client.fetch_users_daily(
            cfg.anthropic_analytics_api_key, start.date(), end.date(), now
        ),
        "summaries": lambda: client.fetch_summaries(cfg.anthropic_analytics_api_key, start, end, now),
    }

    typer.echo(f"syncing {', '.join(selected)} for {start.date()} .. {end.date()} (UTC)")
    writer.ensure_schema(cfg)

    usage_before = None
    if "org_usage_report" in selected:
        writer.ensure_table(cfg, ALL_TABLES["org_usage_report"])
        usage_before = assertions.snapshot_usage_totals(cfg, start, end)

    for key in selected:
        spec = ALL_TABLES[key]
        typer.echo(f"fetching {key}...")
        rows = fetchers[key]()
        typer.echo(f"  -> {len(rows)} rows")
        writer.ensure_table(cfg, spec)
        range_start = start.date() if spec.range_column == "activity_date" else start
        range_end = end.date() if spec.range_column == "activity_date" else end
        n = writer.replace_range(cfg, spec, rows, range_start, range_end)
        typer.echo(f"  wrote {n} rows to {cfg.catalog}.{cfg.schema}.{spec.name}")

    typer.echo("running post-sync assertions...")
    if "org_cost_report" in selected:
        assertions.assert_org_cost_report_token_type_grain(cfg, start, end)
        assertions.assert_amount_within_list_amount(
            cfg, "anthropic_analytics_org_cost_report", "bucket_start", start, end
        )
    if "user_cost_report" in selected:
        assertions.assert_amount_within_list_amount(
            cfg, "anthropic_analytics_user_cost_report", "period_start", start, end
        )
    if "org_cost_report" in selected and "user_cost_report" in selected:
        assertions.assert_cross_table_cost_agreement(cfg, start, end)
    if "org_cost_report" in selected and "org_usage_report" in selected:
        assertions.assert_cost_per_token_ceiling(cfg, start, end)
    if usage_before is not None:
        assertions.assert_usage_non_regression(cfg, usage_before, start, end)

    typer.echo("done.")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
