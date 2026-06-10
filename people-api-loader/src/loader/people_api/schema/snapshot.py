"""Committed static snapshot of the prod schema.

`inspect-prod` (DATA-1907) is out of scope, so `create-schema` and
`build-indexes` read the prod `pg_dump` from a versioned snapshot committed
under `schema/data/` instead of from S3. Override with `LOADER_PROD_DUMP_PATH`
(used by tests and ad-hoc runs).
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
