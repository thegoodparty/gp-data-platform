"""Structured logging setup.

Every log line is JSON on stdout. Airflow scrapes stdout; the orchestrator
mirrors the same lines to S3 at step end (see `mirror_logs_to_s3`).

Call `configure_logging()` once at process start (the Typer entrypoint does
this). Modules obtain a bound logger via `get_logger(__name__)`.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.EventRenamer("msg"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level.upper(), logging.INFO)),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)


def bind(**kwargs: Any) -> None:
    """Bind context-local fields (run_date, step, state) onto every log line."""
    structlog.contextvars.bind_contextvars(**kwargs)


def unbind(*keys: str) -> None:
    structlog.contextvars.unbind_contextvars(*keys)
