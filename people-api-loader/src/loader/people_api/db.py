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
from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from loader.core.aws import get_ssm_parameter
from loader.people_api.bastion import open_tunnel
from loader.people_api.config import LoaderConfig

if TYPE_CHECKING:
    from psycopg import Connection

# TCP keepalives so a dropped connection (e.g. a severed bastion tunnel) surfaces as an error
# in ~1 min rather than hanging indefinitely on a response that never arrives. connect_timeout
# only bounds establishing the connection; these bound a silently-dead established one. libpq
# parameters are string-valued, and we bake them into the conninfo so the connect() call stays
# a plain (conninfo, autocommit, connect_timeout) form.
_KEEPALIVE_KW = {
    "keepalives": "1",
    "keepalives_idle": "30",
    "keepalives_interval": "10",
    "keepalives_count": "5",
}


def _apply_session_settings(conn: Connection, cfg: LoaderConfig) -> None:
    """Bound server-side query time when configured (LOADER_DB_STATEMENT_TIMEOUT_MS > 0) so a
    runaway query fails loudly instead of running unbounded. 0 (the default) leaves it unset."""
    if cfg.db_statement_timeout_ms > 0:
        # statement_timeout is a session GUC; Postgres SET takes no bind params, so inline the
        # already-int-coerced value (never user input).
        set_timeout = f"SET statement_timeout = {int(cfg.db_statement_timeout_ms)}"
        conn.execute(set_timeout)  # ty: ignore[no-matching-overload]


@contextmanager
def _connect(
    cfg: LoaderConfig,
    param_name: str,
    *,
    autocommit: bool,
    forward: tuple[str, int] | None = None,
) -> Iterator[Connection]:
    """Open a psycopg connection from an SSM connection string, via the bastion if configured.

    `forward` reuses a tunnel already opened for this target (see `open_new_tunnel`) instead of
    opening a fresh SSH session for this one connection. Many concurrent connections behind the
    bastion would otherwise each open their own tunnel and flood sshd MaxStartups; sharing one
    forward multiplexes them as channels on a single SSH transport. Ignored when no bastion.
    """
    conninfo = make_conninfo(get_ssm_parameter(cfg, param_name), **_KEEPALIVE_KW)
    if not cfg.bastion_enabled:
        # Direct: the SSM connection string, plus the keepalive params baked in above.
        with psycopg.connect(conninfo, autocommit=autocommit, connect_timeout=30) as conn:
            _apply_session_settings(conn, cfg)
            yield conn
        return
    # Tunneled: forward to the real host/port, then dial the local forward via `hostaddr`
    # while keeping the original `host` so TLS SNI / cert verification still validates against
    # the RDS hostname (not 127.0.0.1). This keeps sslmode=verify-* connection strings working.
    parts = conninfo_to_dict(conninfo)
    target_host = str(parts.get("host") or "")
    target_port = int(parts.get("port") or 5432)
    with ExitStack() as stack:
        if forward is None:
            local_host, local_port = stack.enter_context(open_tunnel(cfg, target_host, target_port))
        else:
            local_host, local_port = forward
        tunneled = make_conninfo(conninfo, hostaddr=local_host, port=str(local_port))
        with psycopg.connect(tunneled, autocommit=autocommit, connect_timeout=30) as conn:
            _apply_session_settings(conn, cfg)
            yield conn


@contextmanager
def open_new_tunnel(cfg: LoaderConfig, run_date: str) -> Iterator[tuple[str, int] | None]:
    """Open ONE bastion tunnel to the run's provisioned cluster; yield its (host, port) forward.

    Yields `None` when no bastion is configured (direct connections). Pass the yielded value as
    `forward=` to `connect_new` so a step that opens many concurrent connections shares a single
    SSH session instead of one handshake per connection (which floods sshd MaxStartups).
    """
    if not cfg.bastion_enabled:
        yield None
        return
    parts = conninfo_to_dict(make_conninfo(get_ssm_parameter(cfg, cfg.new_conn_param(run_date))))
    target_host = str(parts.get("host") or "")
    target_port = int(parts.get("port") or 5432)
    with open_tunnel(cfg, target_host, target_port) as fwd:
        yield fwd


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
def connect_new(
    cfg: LoaderConfig,
    run_date: str,
    *,
    autocommit: bool = True,
    forward: tuple[str, int] | None = None,
) -> Iterator[Connection]:
    """Connect to the cluster provisioned for `run_date` via its SSM connection string.

    `cfg.new_conn_param(run_date)` names the SecureString parameter that provision
    wrote (`people-db-connection-string-{env}-{run_date}`); the decrypted value is the
    full `postgresql://` URL, password and all, handed straight to psycopg.

    `forward` shares a tunnel opened by `open_new_tunnel` (see `_connect`); omit it for a
    one-off connection that opens (and closes) its own tunnel.
    """
    with _connect(cfg, cfg.new_conn_param(run_date), autocommit=autocommit, forward=forward) as conn:
        yield conn
