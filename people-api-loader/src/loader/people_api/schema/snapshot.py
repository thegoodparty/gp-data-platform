"""Committed target schema for the new cluster.

`create-schema`, `copy`, and `build-indexes` read the Voter table DDL from the
committed `schema/data/target_schema.sql`, generated from the people-api mart by
`loader emit-ddl` (DATA-1904). Override the path with `LOADER_TARGET_SCHEMA_PATH`
(used by tests and ad-hoc runs).
"""

from __future__ import annotations

import os
from pathlib import Path

from loader.people_api.config import LoaderConfig

DATA_DIR = Path(__file__).parent / "data"


def load_target_schema(cfg: LoaderConfig, run_date: str) -> str:
    del cfg, run_date  # reserved for a future inspect-manifest path
    override = os.environ.get("LOADER_TARGET_SCHEMA_PATH")
    path = Path(override) if override else DATA_DIR / "target_schema.sql"
    return path.read_text(encoding="utf-8")
