"""Step 4 — parallel `aws_s3.table_import_from_s3` per file.

Stub: scaffolding only. Per-step implementation tracked under ClickUp DATA-1851.
"""

from __future__ import annotations

from loader.people_api.config import LoaderConfig


def run(
    cfg: LoaderConfig,
    run_date: str,
    *,
    state_filter: str | None = None,
    parallelism: int = 128,
) -> None:
    raise NotImplementedError("loader.people_api.steps.copy_s3 is a stub; implement per ClickUp DATA-1851.")
