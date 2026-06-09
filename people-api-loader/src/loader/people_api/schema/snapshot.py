"""Committed static snapshot of the prod schema + Databricks column list.

`inspect-prod` (DATA-1907) is out of scope for this PR, so `create-schema`
and `build-indexes` read their prod-dump / databricks-columns inputs from a
versioned snapshot committed under `schema/data/` instead of from S3.

Override either input with an env var pointing at a file — used by tests and
by ad-hoc runs against a freshly captured dump:
- `LOADER_PROD_DUMP_PATH`
- `LOADER_DATABRICKS_COLUMNS_PATH`

When `inspect-prod` later lands, it can repopulate these files (or this
module can be extended to prefer a present inspect manifest's S3 artifacts).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from loader.people_api.config import LoaderConfig

DATA_DIR = Path(__file__).parent / "data"


def load_prod_dump(cfg: LoaderConfig, run_date: str) -> str:
    del cfg, run_date  # reserved for a future inspect-manifest path
    override = os.environ.get("LOADER_PROD_DUMP_PATH")
    path = Path(override) if override else DATA_DIR / "prod_dump.sql"
    return path.read_text(encoding="utf-8")


def load_databricks_columns(cfg: LoaderConfig, run_date: str) -> dict[str, str]:
    del cfg, run_date
    override = os.environ.get("LOADER_DATABRICKS_COLUMNS_PATH")
    path = Path(override) if override else DATA_DIR / "databricks_columns.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return {c["name"]: c.get("type", "") for c in raw}
    return raw
