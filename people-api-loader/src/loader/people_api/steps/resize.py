"""Step 6 — swap writer to db.serverless plus serve-tuned params.

Stub: scaffolding only. Per-step implementation tracked under ClickUp DATA-1854.
"""

from __future__ import annotations

from loader.people_api.config import LoaderConfig
from loader.people_api.manifests import ResizeManifest


def run(cfg: LoaderConfig, run_date: str) -> ResizeManifest:
    raise NotImplementedError("loader.people_api.steps.resize is a stub; implement per ClickUp DATA-1854.")
