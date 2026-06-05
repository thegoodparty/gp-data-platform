"""Step 7 — validation checks against the new cluster.

Stub: scaffolding only. Per-step implementation tracked under ClickUp DATA-1911.
"""

from __future__ import annotations

from loader.people_api.config import LoaderConfig
from loader.people_api.manifests import ValidateManifest


def run(cfg: LoaderConfig, run_date: str) -> ValidateManifest:
    raise NotImplementedError("loader.people_api.steps.validate is a stub; implement per ClickUp DATA-1911.")
