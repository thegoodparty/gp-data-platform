"""Shared CLI helpers.

Consumer-specific CLIs (e.g. `loader.people_api.cli`) compose these into
their own Typer app. Keeps the pieces every loader CLI repeats — logging
setup, AWS caller-identity check, the `--date` argument shape — in one
place.
"""

from __future__ import annotations

from typing import Annotated

import typer
from rich.console import Console

from loader.core.aws import verify_caller
from loader.core.config import BaseLoaderConfig
from loader.core.log import bind, configure_logging, get_logger

console = Console()
log = get_logger(__name__)

RunDateArg = Annotated[str, typer.Option("--date", help="Refresh stamp in YYYYMMDD form.")]


def setup_environment(
    cfg: BaseLoaderConfig,
    run_date: str | None = None,
    *,
    verify_aws: bool = True,
) -> None:
    """Configure logging, bind run_date, and verify AWS caller identity.

    Call at the top of every command body. Exits with code 2 on AWS caller
    check failure — better to fail at startup than mid-provision.

    Pass `verify_aws=False` for read-only / reporting commands that should
    still run when AWS credentials are not configured (e.g. `status`).
    """
    configure_logging()
    if run_date is not None:
        bind(run_date=run_date)
    if not verify_aws:
        return
    try:
        identity = verify_caller(cfg)
        log.info(
            "cli.caller",
            account=identity["Account"],
            arn=identity["Arn"],
            assume_role_arn=cfg.assume_role_arn or "(ambient)",
        )
    except Exception as e:
        console.print(
            f"[red]AWS caller check failed:[/red] {e}\n"
            "Is your AWS profile (`AWS_PROFILE`) authenticated and in-region?"
        )
        raise typer.Exit(code=2) from e
