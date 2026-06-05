"""Shared Postgres connection helpers.

Consumer packages build the actual `connect_*` context managers for their
specific clusters; this module supplies the primitives those wrappers reuse.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

import psycopg

from loader.core.aws import get_secret
from loader.core.config import BaseLoaderConfig

if TYPE_CHECKING:
    from psycopg import Connection


def password_from_secret(cfg: BaseLoaderConfig, secret_id: str) -> str:
    """Supports both raw-string secrets and JSON-shaped RDS-managed secrets."""
    raw = get_secret(cfg, secret_id)
    raw_stripped = raw.strip()
    if raw_stripped.startswith("{"):
        data = json.loads(raw_stripped)
        for key in ("password", "Password", "MasterUserPassword"):
            if key in data:
                return data[key]
        raise RuntimeError(
            f"Secret {secret_id!r} is JSON but has no 'password' key (keys: {sorted(data.keys())})"
        )
    return raw_stripped


@contextmanager
def open_conn(
    host: str,
    *,
    user: str,
    password: str,
    dbname: str,
    port: int,
    autocommit: bool,
    sslmode: str = "require",
    connect_timeout: int = 30,
) -> Iterator[Connection]:
    with psycopg.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        sslmode=sslmode,
        connect_timeout=connect_timeout,
        autocommit=autocommit,
    ) as conn:
        yield conn
