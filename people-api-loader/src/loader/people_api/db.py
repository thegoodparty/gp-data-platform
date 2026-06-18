"""Postgres connection helpers for the people-API loader.

Two clusters exist in the loader's life:
- `connect_prod(cfg)`: the existing Present cluster (read-only) for step 0 (inspect)
  and step 7 (validate). The full connection string is an SSM Parameter Store
  SecureString (`cfg.db_conn_param`, e.g. `people-db-connection-string-{env}`), fetched
  and decrypted at connect time — nothing connection-related lives in this repo.
- `connect_new(cfg, run_date, endpoint)`: the cluster provisioned by step 2.
  Auth via the master password stored in Secrets Manager at provision time.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

import psycopg

from loader.core.aws import get_ssm_parameter
from loader.core.db import open_conn, password_from_secret
from loader.people_api.config import LoaderConfig
from loader.people_api.manifests import ProvisionManifest, read_manifest

if TYPE_CHECKING:
    from psycopg import Connection


@contextmanager
def connect_prod(cfg: LoaderConfig, *, autocommit: bool = True) -> Iterator[Connection]:
    """Connect to the existing Present cluster using the SSM connection string.

    `cfg.db_conn_param` names the SecureString parameter (e.g.
    `people-db-connection-string-{env}`); its decrypted value is a full libpq
    connection string (or `postgresql://` URL) handed straight to psycopg.
    """
    conninfo = get_ssm_parameter(cfg, cfg.db_conn_param)
    with psycopg.connect(conninfo, autocommit=autocommit, connect_timeout=30) as conn:
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
    # The new cluster mirrors prod's user/dbname. With no env vars set these are empty
    # placeholders — fail with an actionable error rather than an opaque libpq
    # `role "" does not exist`. Checked before the Secrets Manager call so misconfig is fast.
    effective_dbname = dbname or cfg.prod_db_name
    if not cfg.prod_db_user:
        raise RuntimeError("prod_db_user is not configured — set LOADER_PROD_DB_USER.")
    if not effective_dbname:
        raise RuntimeError("prod_db_name is not configured — set LOADER_PROD_DB_NAME.")
    password = password_from_secret(cfg, cfg.new_master_secret_id(run_date))
    with open_conn(
        writer_endpoint,
        user=cfg.prod_db_user,
        password=password,
        dbname=effective_dbname,
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
