"""Committed static snapshot of the prod schema (pg_dump --schema-only).

`create-schema` and `build-indexes` read the prod schema DDL from this
versioned file under `schema/data/` rather than generating it live. Override
the path with `LOADER_PROD_DUMP_PATH` (used by tests and ad-hoc runs). A
dedicated Prisma-to-DDL emitter (DATA-1904) would eventually replace this
committed snapshot.
"""

from __future__ import annotations

import os
from pathlib import Path

from loader.people_api.config import LoaderConfig

DATA_DIR = Path(__file__).parent / "data"


def load_prod_dump(cfg: LoaderConfig, run_date: str) -> str:
    del cfg, run_date  # reserved for a future inspect-manifest path
    override = os.environ.get("LOADER_PROD_DUMP_PATH")
    path = Path(override) if override else DATA_DIR / "prod_dump.sql"
    return path.read_text(encoding="utf-8")
