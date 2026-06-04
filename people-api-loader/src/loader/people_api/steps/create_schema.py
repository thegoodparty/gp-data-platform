"""Step 3 — apply merged DDL to the new cluster.

Stub: scaffolding only. Per-step implementation tracked under ClickUp DATA-1910.
"""

from __future__ import annotations

from loader.people_api.config import LoaderConfig
from loader.people_api.manifests import SchemaManifest


def run(cfg: LoaderConfig, run_date: str) -> SchemaManifest:
    raise NotImplementedError(
        "loader.people_api.steps.create_schema is a stub; implement per ClickUp DATA-1910."
    )
