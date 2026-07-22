"""Typer CLI for the people-API loader.

Each subcommand loads `LoaderConfig.from_env()`, configures logging, and
delegates to the matching `loader.people_api.steps.*.run(...)`. Re-running
any subcommand against a complete manifest is a no-op.
"""

from __future__ import annotations

import sys
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from loader.core.cli import RunDateArg, setup_environment
from loader.core.log import get_logger
from loader.people_api.config import LoaderConfig, require_run_date
from loader.people_api.steps.build_indexes import _DEFAULT_BUILDERS

app = typer.Typer(
    help="GoodParty voter-DB refresh pipeline.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)
log = get_logger(__name__)
console = Console()


def _setup(run_date: str | None = None, *, verify_aws: bool = True) -> LoaderConfig:
    cfg = LoaderConfig.from_env()
    if run_date is not None:
        require_run_date(run_date)
    setup_environment(cfg, run_date, verify_aws=verify_aws)
    return cfg


StateArg = Annotated[
    str | None,
    typer.Option(
        "--state",
        help="Restrict to a single two-letter state (uppercase). Use for the FL-first milestone.",
    ),
]


@app.command(name="inspect-prod")
def inspect_prod(
    run_date: RunDateArg = "",
) -> None:
    """Step 0 — capture prod cluster shape for later diffs."""
    from loader.people_api.steps import inspect_prod as step

    # inspect has no strict dependency on a run_date, but the manifest
    # lives under voter_export_{date} to keep artifacts grouped.
    if not run_date:
        from datetime import UTC, datetime

        run_date = datetime.now(UTC).strftime("%Y%m%d")
    cfg = _setup(run_date)
    step.run(cfg, run_date)


@app.command(name="extract-serving-structure")
def extract_serving_structure() -> None:
    """Bootstrap: capture prod PK/indexes/FKs into schema/_serving_seed.py (committed)."""
    from pathlib import Path

    from loader.people_api.db import connect_prod
    from loader.people_api.schema import serving_structure as ss
    from loader.people_api.schema.serving_seed_writer import render_seed_module

    cfg = _setup()
    tables = ["Voter", "District", "DistrictStats", "DistrictVoter"]
    with connect_prod(cfg) as conn, conn.cursor() as cur:
        pks = ss.extract_primary_keys(cur, tables)
        idxs = ss.extract_indexes(cur, tables)
        fks = ss.extract_foreign_keys(cur, tables)
    out = Path(__file__).parent / "schema" / "_serving_seed.py"
    out.write_text(render_seed_module(pks, idxs, fks), encoding="utf-8")
    typer.echo(f"wrote {out} ({len(pks)} PKs, {len(idxs)} indexes, {len(fks)} FKs)")


@app.command(name="emit-ddl")
def emit_ddl() -> None:
    """Render schema/data/target_schema.sql from the people_api marts (committed artifact)."""
    from pathlib import Path

    from loader.people_api.schema.emit_ddl import render_target_schema

    cfg = _setup(verify_aws=False)
    out = Path(__file__).parent / "schema" / "data" / "target_schema.sql"
    out.write_text(render_target_schema(cfg), encoding="utf-8")
    typer.echo(f"wrote {out}")


@app.command()
def unload(
    run_date: RunDateArg,
    state: StateArg = None,
    skip_submit: Annotated[
        bool,
        typer.Option(
            "--skip-submit",
            help="Build manifest from existing S3 files without re-running SQL.",
        ),
    ] = False,
) -> None:
    """Step 1 — issue Databricks SQL to unload source -> S3."""
    from loader.people_api.steps import unload as step

    cfg = _setup(run_date)
    step.run(cfg, run_date, state_filter=state, skip_submit=skip_submit)


@app.command()
def provision(run_date: RunDateArg) -> None:
    """Step 2 — provision Aurora cluster + IAM role + VPCE + param groups."""
    from loader.people_api.steps import provision as step

    cfg = _setup(run_date)
    step.run(cfg, run_date)


@app.command(name="create-schema")
def create_schema(run_date: RunDateArg) -> None:
    """Step 3 — apply merged DDL to the new cluster."""
    from loader.people_api.steps import create_schema as step

    cfg = _setup(run_date)
    step.run(cfg, run_date)


@app.command()
def copy(
    run_date: RunDateArg,
    state: StateArg = None,
    parallelism: Annotated[
        int,
        typer.Option("--parallelism", help="Concurrent COPY statements."),
    ] = 128,
) -> None:
    """Step 4 — parallel `aws_s3.table_import_from_s3` per file."""
    from loader.people_api.steps import copy_s3 as step

    cfg = _setup(run_date)
    step.run(cfg, run_date, state_filter=state, parallelism=parallelism)


@app.command(name="build-indexes")
def build_indexes(
    run_date: RunDateArg,
    parallelism: Annotated[
        int,
        typer.Option("--parallelism", help="Concurrent CREATE INDEX builds (peak memory scales with this)."),
    ] = _DEFAULT_BUILDERS,
) -> None:
    """Step 5 — PKs, non-unique indexes, FKs, ANALYZE."""
    from loader.people_api.steps import build_indexes as step

    cfg = _setup(run_date)
    step.run(cfg, run_date, parallelism=parallelism)


@app.command()
def resize(run_date: RunDateArg) -> None:
    """Step 6 — swap writer to the serving instance class + serve-tuned params."""
    from loader.people_api.steps import resize as step

    cfg = _setup(run_date)
    step.run(cfg, run_date)


@app.command(name="scale-down")
def scale_down(run_date: RunDateArg) -> None:
    """Failure cost guard — flip the writer to db.serverless (keeps the cluster + data)."""
    from loader.people_api.steps import scale_down as step

    cfg = _setup(run_date)
    step.run(cfg, run_date)


@app.command()
def validate(run_date: RunDateArg) -> None:
    """Step 7 — six validation checks. Exits non-zero if any fail."""
    from loader.people_api.steps import validate as step

    cfg = _setup(run_date)
    manifest = step.run(cfg, run_date)
    _print_validate_report(manifest)
    if not manifest.all_passed:
        raise typer.Exit(code=1)


def _print_validate_report(manifest) -> None:
    tbl = Table(title=f"Validation — {manifest.run_date}")
    tbl.add_column("Check")
    tbl.add_column("Status")
    for c in manifest.checks:
        tbl.add_row(c.name, "[green]PASS[/green]" if c.passed else "[red]FAIL[/red]")
    console.print(tbl)


@app.command()
def teardown(
    run_date: RunDateArg,
    confirm: Annotated[
        bool,
        typer.Option(
            "--confirm",
            help="Actually delete. Without this flag runs a dry-run listing what would be deleted.",
        ),
    ] = False,
    delete_s3: Annotated[
        bool,
        typer.Option(
            "--delete-s3",
            help="Also delete the voter_export_{date}/ S3 prefix (including "
            "manifests). Default keeps artifacts for forensics.",
        ),
    ] = False,
    delete_vpce: Annotated[
        bool,
        typer.Option(
            "--delete-vpce",
            help="Also delete the S3 gateway VPC endpoint. Default keeps it in place for future refreshes.",
        ),
    ] = False,
) -> None:
    """Delete loader-created resources for a run_date. Dry-run by default."""
    from loader.people_api.steps import teardown as step

    cfg = _setup(run_date)
    step.run(
        cfg,
        run_date,
        confirm=confirm,
        delete_s3=delete_s3,
        delete_vpce=delete_vpce,
    )


@app.command()
def status(run_date: RunDateArg) -> None:
    """Print which steps have completed manifests for a given run_date.

    Read-only: skips the AWS caller-identity check so the command runs
    offline. Absent / unreadable manifests are reported as `—`.
    """
    cfg = _setup(run_date, verify_aws=False)
    from loader.core.manifest.io import read_manifest
    from loader.people_api.manifests import (
        CopyManifest,
        IndexManifest,
        InspectManifest,
        ProvisionManifest,
        ResizeManifest,
        SchemaManifest,
        UnloadManifest,
        ValidateManifest,
    )

    steps = [
        ("inspect", InspectManifest),
        ("unload", UnloadManifest),
        ("provision", ProvisionManifest),
        ("schema", SchemaManifest),
        ("copy", CopyManifest),
        ("indexes", IndexManifest),
        ("resize", ResizeManifest),
        ("validate", ValidateManifest),
    ]
    tbl = Table(title=f"Run {run_date} — step status")
    tbl.add_column("Step")
    tbl.add_column("Status")
    tbl.add_column("URI")
    for name, model in steps:
        try:
            m = read_manifest(cfg, run_date, name, model)
        except Exception as exc:
            log.warning("manifest unreadable for step %s: %s", name, exc)
            tbl.add_row(name, "[red]error[/red]", str(exc))
            continue
        if m is None:
            tbl.add_row(name, "—", "")
        else:
            if m.status == "complete":
                status_text = "[green]complete[/green]"
            elif m.status == "failed":
                status_text = "[red]failed[/red]"
            else:
                status_text = "[yellow]in_progress[/yellow]"
            tbl.add_row(
                name,
                status_text,
                f"s3://{cfg.s3_bucket}/{cfg.manifest_key(run_date, name)}",
            )
    console.print(tbl)


def main() -> None:
    try:
        app()
    except KeyboardInterrupt:
        console.print("[yellow]interrupted[/yellow]")
        sys.exit(130)


if __name__ == "__main__":
    main()
