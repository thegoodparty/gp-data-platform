"""Delete loader-created resources for a run_date.

Stub: scaffolding only. Per-step implementation tracked under ClickUp DATA-1912.
"""

from __future__ import annotations

from loader.people_api.config import LoaderConfig


def run(
    cfg: LoaderConfig,
    run_date: str,
    *,
    confirm: bool = False,
    delete_s3: bool = False,
    delete_vpce: bool = False,
) -> None:
    raise NotImplementedError("loader.people_api.steps.teardown is a stub; implement per ClickUp DATA-1912.")
