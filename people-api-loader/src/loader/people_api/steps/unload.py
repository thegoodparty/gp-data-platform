"""Step 1 — issue Databricks SQL to unload source to S3.

Stub: scaffolding only. Per-step implementation tracked under ClickUp DATA-1908.
"""

from __future__ import annotations

from loader.people_api.config import LoaderConfig


def run(
    cfg: LoaderConfig,
    run_date: str,
    *,
    state_filter: str | None = None,
    skip_submit: bool = False,
) -> None:
    raise NotImplementedError("loader.people_api.steps.unload is a stub; implement per ClickUp DATA-1908.")
