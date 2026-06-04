"""Step 5 — PKs, non-unique indexes, FKs, ANALYZE.

Stub: scaffolding only. Per-step implementation tracked under ClickUp DATA-1853.
"""

from __future__ import annotations

from loader.people_api.config import LoaderConfig
from loader.people_api.manifests import IndexManifest


def run(cfg: LoaderConfig, run_date: str) -> IndexManifest:
    raise NotImplementedError(
        "loader.people_api.steps.build_indexes is a stub; implement per ClickUp DATA-1853."
    )
