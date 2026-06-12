"""Postgres connection helpers for the people-API loader.

Two clusters exist in the loader's life:
- `connect_prod(cfg)`: the existing `gp-people-db-prod` Present cluster (read-only)
  for step 0 (inspect) and step 7 (validate). Auth via `~/.pg_service.conf`
  entry named by `LOADER_PROD_PG_SERVICE` (default `voters`) —
  passwordless because that's how the team reaches this cluster today.
- `connect_new(cfg, run_date, endpoint)`: the cluster provisioned by step 2.
  Auth via the master password stored in Secrets Manager at provision time.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

import psycopg

from loader.core.db import open_conn, password_from_secret
from loader.people_api.config import LoaderConfig
from loader.people_api.manifests import ProvisionManifest, read_manifest

if TYPE_CHECKING:
    from psycopg import Connection


def _prod_service_name() -> str:
    return os.environ.get("LOADER_PROD_PG_SERVICE", "voters")


@contextmanager
def connect_prod(cfg: LoaderConfig, *, autocommit: bool = True) -> Iterator[Connection]:
    """Connect to the existing prod cluster via pg_service (passwordless).

    Uses libpq's `service=<name>` lookup — the `[voters]` section of
    `~/.pg_service.conf` provides host, port, dbname, user, sslmode, and
    the cert bundle. The `cfg` parameter is still accepted so callers don't
    need to care which auth path is in play.
    """
    del cfg  # reserved for future use; pg_service carries all connection info
    with psycopg.connect(
        service=_prod_service_name(),
        autocommit=autocommit,
        connect_timeout=30,
    ) as conn:
        yield conn


@contextmanager
def connect_new(
    cfg: LoaderConfig,
    run_date: str,
    writer_endpoint: str,
    *,
    dbname: str | None = None,
    autocommit: bool = True,
) -> Iterator[Connection]:
    password = password_from_secret(cfg, cfg.new_master_secret_id(run_date))
    with open_conn(
        writer_endpoint,
        user=cfg.prod_db_user,
        password=password,
        dbname=dbname or cfg.prod_db_name,
        port=cfg.prod_db_port,
        autocommit=autocommit,
    ) as conn:
        yield conn


def resolve_writer_endpoint(cfg: LoaderConfig, run_date: str) -> str:
    """Resolve the new cluster's writer endpoint.

    `provision` (DATA-1909) is out of scope for this PR. Resolution order:
    1. `LOADER_NEW_WRITER_ENDPOINT` env override (point at an existing cluster).
    2. A completed `provision` manifest, if one exists for this run.
    3. Otherwise raise.
    """
    override = os.environ.get("LOADER_NEW_WRITER_ENDPOINT")
    if override:
        return override
    prov = read_manifest(cfg, run_date, "provision", ProvisionManifest)
    if prov is not None and prov.status == "complete":
        return prov.writer_endpoint
    raise RuntimeError(
        "No new-cluster writer endpoint available. Set "
        "$LOADER_NEW_WRITER_ENDPOINT to the target cluster, or run "
        "`loader provision --date <run_date>` first."
    )
