"""Postgres connection helpers for the people-API loader.

Two clusters exist in the loader's life, both reached the same way — through an
SSM Parameter Store SecureString holding a full `postgresql://` connection string,
fetched and decrypted at connect time. Nothing connection-related lives in this repo.
- `connect_prod(cfg)`: the existing Present cluster (read-only) for step 0 (inspect)
  and step 7 (validate). Param name `cfg.db_conn_param` (e.g.
  `people-db-connection-string-{env}`).
- `connect_new(cfg, run_date)`: the cluster provisioned by step 2. Param name
  `cfg.new_conn_param(run_date)`, written by provision with the generated master
  password embedded in the URL.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from loader.core.aws import get_ssm_parameter
from loader.people_api.bastion import open_tunnel
from loader.people_api.config import LoaderConfig

if TYPE_CHECKING:
    from psycopg import Connection


@contextmanager
def _connect(cfg: LoaderConfig, param_name: str, *, autocommit: bool) -> Iterator[Connection]:
    """Open a psycopg connection from an SSM connection string, via the bastion if configured."""
    conninfo = get_ssm_parameter(cfg, param_name)
    parts = conninfo_to_dict(conninfo)
    target_host = str(parts.get("host") or "")
    target_port = int(parts.get("port") or 5432)
    with open_tunnel(cfg, target_host, target_port) as (host, port):
        tunneled = make_conninfo(conninfo, host=host, port=str(port))
        with psycopg.connect(tunneled, autocommit=autocommit, connect_timeout=30) as conn:
            yield conn


@contextmanager
def connect_prod(cfg: LoaderConfig, *, autocommit: bool = True) -> Iterator[Connection]:
    """Connect to the existing Present cluster using its SSM connection string.

    `cfg.db_conn_param` names the SecureString parameter (e.g.
    `people-db-connection-string-{env}`); its decrypted value is a full libpq
    connection string (or `postgresql://` URL) handed straight to psycopg.
    """
    with _connect(cfg, cfg.db_conn_param, autocommit=autocommit) as conn:
        yield conn


@contextmanager
def connect_new(cfg: LoaderConfig, run_date: str, *, autocommit: bool = True) -> Iterator[Connection]:
    """Connect to the cluster provisioned for `run_date` via its SSM connection string.

    `cfg.new_conn_param(run_date)` names the SecureString parameter that provision
    wrote (`people-db-connection-string-{env}-{run_date}`); the decrypted value is the
    full `postgresql://` URL, password and all, handed straight to psycopg.
    """
    with _connect(cfg, cfg.new_conn_param(run_date), autocommit=autocommit) as conn:
        yield conn
