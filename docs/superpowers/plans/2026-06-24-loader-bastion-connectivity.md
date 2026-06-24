# Loader Bastion Connectivity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the People-API loader reach Postgres through the `gp_bastion_host` SSH tunnel when running on the Astro worker, while keeping direct psycopg connections for local/VPN runs.

**Architecture:** Add optional bastion config to `LoaderConfig`. A new `people_api/bastion.py` opens an `SSHTunnelForwarder` to the bastion and yields a local `(host, port)`; when no bastion is configured it yields the original target unchanged. `db.py._connect` parses the SSM connection string, runs it through the tunnel helper, and rewrites the conninfo's host/port before handing it to psycopg. This is the DATA-1913 prerequisite (Plan A of three: A loader, B DAG, C S3 VPC endpoint).

**Tech Stack:** Python 3.14, uv, psycopg v3 (`psycopg.conninfo`), `sshtunnel`, `paramiko`, pytest, ruff (line length 110), ty.

**Working directory for all commands:** `people-api-loader/` (its own uv env — `cd people-api-loader` first; `uv run …`).

---

### Task 1: Add `sshtunnel` + `paramiko` dependencies

**Files:**
- Modify: `people-api-loader/pyproject.toml`

- [ ] **Step 1: Add the deps**

In `pyproject.toml`, add to the `dependencies` array (next to `psycopg`):

```toml
    "sshtunnel>=0.4.0",
    "paramiko>=3.4.0",
```

- [ ] **Step 2: Lock + sync**

Run: `cd people-api-loader && uv lock && uv sync`
Expected: `uv.lock` updates; `sshtunnel` and `paramiko` install with no resolution error.

- [ ] **Step 3: Commit**

```bash
git add people-api-loader/pyproject.toml people-api-loader/uv.lock
git commit -m "feat(loader): add sshtunnel + paramiko for bastion connectivity"
```

---

### Task 2: Add bastion config to `LoaderConfig`

**Files:**
- Modify: `people-api-loader/src/loader/people_api/config.py`
- Test: `people-api-loader/tests/test_config.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_config.py`:

```python
def test_bastion_defaults_empty(monkeypatch):
    from loader.people_api.config import LoaderConfig

    for var in ("LOADER_BASTION_HOST", "LOADER_BASTION_USER",
                "LOADER_BASTION_PRIVATE_KEY", "LOADER_BASTION_PORT"):
        monkeypatch.delenv(var, raising=False)
    cfg = LoaderConfig.from_env()
    assert cfg.bastion_host == ""
    assert cfg.bastion_port == 22
    assert cfg.bastion_enabled is False


def test_bastion_enabled_when_host_set(monkeypatch):
    from loader.people_api.config import LoaderConfig

    monkeypatch.setenv("LOADER_BASTION_HOST", "bastion.example.com")
    monkeypatch.setenv("LOADER_BASTION_USER", "ec2-user")
    monkeypatch.setenv("LOADER_BASTION_PRIVATE_KEY", "PEM")
    cfg = LoaderConfig.from_env()
    assert cfg.bastion_host == "bastion.example.com"
    assert cfg.bastion_user == "ec2-user"
    assert cfg.bastion_private_key == "PEM"
    assert cfg.bastion_enabled is True
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd people-api-loader && uv run pytest tests/test_config.py -k bastion -v`
Expected: FAIL — `LoaderConfig` has no field `bastion_host`.

- [ ] **Step 3: Add the fields, env wiring, and `bastion_enabled`**

In `config.py`, add four fields to the `LoaderConfig` dataclass (after the `s3_import_role_arn` field):

```python
    # Bastion (SSH) for reaching Postgres from outside the VPC (Astro worker).
    # Empty host = connect directly (local/VPN runs).
    bastion_host: str
    bastion_port: int
    bastion_user: str
    bastion_private_key: str
```

In `from_env()`, add to the `cls(...)` call (after `s3_import_role_arn=...`):

```python
            bastion_host=os.environ.get("LOADER_BASTION_HOST", ""),
            bastion_port=int(os.environ.get("LOADER_BASTION_PORT", "22")),
            bastion_user=os.environ.get("LOADER_BASTION_USER", ""),
            bastion_private_key=os.environ.get("LOADER_BASTION_PRIVATE_KEY", ""),
```

Add a property to the `LoaderConfig` class (after `from_env`):

```python
    @property
    def bastion_enabled(self) -> bool:
        return bool(self.bastion_host)
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd people-api-loader && uv run pytest tests/test_config.py -k bastion -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add people-api-loader/src/loader/people_api/config.py people-api-loader/tests/test_config.py
git commit -m "feat(loader): add optional bastion config to LoaderConfig"
```

---

### Task 3: Add the SSH tunnel helper

**Files:**
- Create: `people-api-loader/src/loader/people_api/bastion.py`
- Test: `people-api-loader/tests/test_bastion.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_bastion.py`:

```python
"""open_tunnel: yields the target unchanged when no bastion; localhost+local port when set."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

from loader.people_api import bastion
from loader.people_api.config import LoaderConfig


def _cfg(**kw) -> LoaderConfig:
    base = dict(bastion_host="", bastion_port=22, bastion_user="", bastion_private_key="",
                bastion_enabled=False)
    base.update(kw)
    return cast(LoaderConfig, SimpleNamespace(**base))


def test_no_bastion_yields_target_unchanged():
    cfg = _cfg()
    with bastion.open_tunnel(cfg, "rds.internal", 5432) as (host, port):
        assert (host, port) == ("rds.internal", 5432)


def test_bastion_yields_localhost(monkeypatch):
    started = {}

    class _FakeTunnel:
        local_bind_port = 54321

        def __init__(self, *a, **k):
            started["remote"] = k["remote_bind_address"]

        def start(self):
            started["started"] = True

        def stop(self):
            started["stopped"] = True

    monkeypatch.setattr(bastion, "SSHTunnelForwarder", _FakeTunnel)
    monkeypatch.setattr(bastion, "_load_key", lambda pem: "PKEY")

    cfg = _cfg(bastion_host="b", bastion_user="u", bastion_private_key="PEM",
               bastion_enabled=True)
    with bastion.open_tunnel(cfg, "rds.internal", 5432) as (host, port):
        assert (host, port) == ("127.0.0.1", 54321)
    assert started == {"remote": ("rds.internal", 5432), "started": True, "stopped": True}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd people-api-loader && uv run pytest tests/test_bastion.py -v`
Expected: FAIL — module `loader.people_api.bastion` does not exist.

- [ ] **Step 3: Implement `bastion.py`**

Create `src/loader/people_api/bastion.py`:

```python
"""Open an SSH tunnel to the bastion when configured; otherwise a no-op passthrough.

The loader connects to private RDS through `gp_bastion_host` when running on the
Astro worker (outside the VPC). When no bastion is configured (local/VPN runs),
`open_tunnel` yields the target host/port unchanged so psycopg connects directly.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from io import StringIO
from typing import TYPE_CHECKING

import paramiko
from sshtunnel import SSHTunnelForwarder

if TYPE_CHECKING:
    from loader.people_api.config import LoaderConfig

# paramiko >=3 dropped DSSKey (DSA deprecated), but sshtunnel still references it
# during host-key discovery. Match the legacy postgres_utils shim.
if not hasattr(paramiko, "DSSKey"):

    class _DSSKeyStub:
        @staticmethod
        def from_private_key_file(*a: object, **kw: object) -> None:
            raise paramiko.SSHException("DSA not supported")

    paramiko.DSSKey = _DSSKeyStub  # type: ignore[attr-defined]


def _load_key(pem: str) -> paramiko.PKey:
    """Load a private key from PEM material, trying each supported key type."""
    for cls in (paramiko.Ed25519Key, paramiko.RSAKey, paramiko.ECDSAKey):
        try:
            return cls.from_private_key(StringIO(pem))  # type: ignore[attr-defined]
        except (paramiko.SSHException, ValueError):
            continue
    raise ValueError("Could not load bastion SSH key with any supported key type")


@contextmanager
def open_tunnel(cfg: LoaderConfig, target_host: str, target_port: int) -> Iterator[tuple[str, int]]:
    """Yield the (host, port) psycopg should connect to.

    With a bastion configured, opens an SSH forward to (target_host, target_port)
    and yields ("127.0.0.1", local_bind_port). Otherwise yields the target unchanged.
    """
    if not cfg.bastion_enabled:
        yield (target_host, target_port)
        return

    tunnel = SSHTunnelForwarder(
        (cfg.bastion_host, cfg.bastion_port),
        ssh_username=cfg.bastion_user,
        ssh_pkey=_load_key(cfg.bastion_private_key),
        remote_bind_address=(target_host, target_port),
        set_keepalive=30,
    )
    tunnel.start()
    try:
        yield ("127.0.0.1", tunnel.local_bind_port)
    finally:
        tunnel.stop()
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd people-api-loader && uv run pytest tests/test_bastion.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add people-api-loader/src/loader/people_api/bastion.py people-api-loader/tests/test_bastion.py
git commit -m "feat(loader): add bastion SSH tunnel helper"
```

---

### Task 4: Route `db.py` connections through the tunnel

**Files:**
- Modify: `people-api-loader/src/loader/people_api/db.py`
- Test: `people-api-loader/tests/test_connections.py`

- [ ] **Step 1: Update the existing tests + add a bastion test**

The existing tests assert the exact conninfo string. After this change `_connect` rewrites
host/port, so update `_patch_ssm_and_psycopg` to also stub the tunnel, and assert on parsed
host/port instead of an exact string. Replace the body of `tests/test_connections.py` with:

```python
"""connect_prod / connect_new: fetch an SSM SecureString, tunnel if configured, hand to psycopg."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import cast

import pytest
from psycopg.conninfo import conninfo_to_dict

from loader.people_api import db
from loader.people_api.config import LoaderConfig


def _patch(monkeypatch: pytest.MonkeyPatch, captured: dict, *, bastion: bool) -> None:
    def _fake_ssm(cfg: object, name: str, **k: object) -> str:
        captured["name"] = name
        return "host=rds.internal dbname=d user=u port=5432"

    monkeypatch.setattr(db, "get_ssm_parameter", _fake_ssm)

    @contextmanager
    def _fake_tunnel(cfg: object, host: str, port: int):
        captured["target"] = (host, port)
        yield ("127.0.0.1", 54321) if bastion else (host, port)

    monkeypatch.setattr(db, "open_tunnel", _fake_tunnel)

    @contextmanager
    def _fake_connect(conninfo: str, **k: object):
        captured["conninfo"] = conninfo
        yield "CONN"

    monkeypatch.setattr(db.psycopg, "connect", _fake_connect)


def test_connect_prod_direct(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    _patch(monkeypatch, captured, bastion=False)
    cfg = cast(LoaderConfig, SimpleNamespace(db_conn_param="people-db-connection-string-dev"))
    with db.connect_prod(cfg) as conn:
        assert conn == "CONN"
    assert captured["name"] == "people-db-connection-string-dev"
    parts = conninfo_to_dict(captured["conninfo"])
    assert parts["host"] == "rds.internal"
    assert parts["port"] == "5432"


def test_connect_prod_tunneled(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    _patch(monkeypatch, captured, bastion=True)
    cfg = cast(LoaderConfig, SimpleNamespace(db_conn_param="people-db-connection-string-dev"))
    with db.connect_prod(cfg) as conn:
        assert conn == "CONN"
    assert captured["target"] == ("rds.internal", 5432)
    parts = conninfo_to_dict(captured["conninfo"])
    assert parts["host"] == "127.0.0.1"
    assert parts["port"] == "54321"


def test_connect_new_uses_run_dated_conn_param(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    _patch(monkeypatch, captured, bastion=False)
    cfg = cast(
        LoaderConfig,
        SimpleNamespace(new_conn_param=lambda rd: f"people-db-connection-string-dev-{rd}"),
    )
    with db.connect_new(cfg, "20260616") as conn:
        assert conn == "CONN"
    assert captured["name"] == "people-db-connection-string-dev-20260616"
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd people-api-loader && uv run pytest tests/test_connections.py -v`
Expected: FAIL — `db` has no attribute `open_tunnel`.

- [ ] **Step 3: Rewrite `_connect` to tunnel + rewrite host/port**

In `db.py`, update the imports and `_connect`. Add near the top imports:

```python
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from loader.people_api.bastion import open_tunnel
```

Replace `_connect` with:

```python
@contextmanager
def _connect(cfg: LoaderConfig, param_name: str, *, autocommit: bool) -> Iterator[Connection]:
    """Open a psycopg connection from an SSM connection string, via the bastion if configured."""
    conninfo = get_ssm_parameter(cfg, param_name)
    parts = conninfo_to_dict(conninfo)
    target_host = str(parts.get("host", ""))
    target_port = int(parts.get("port", 5432))
    with open_tunnel(cfg, target_host, target_port) as (host, port):
        tunneled = make_conninfo(conninfo, host=host, port=str(port))
        with psycopg.connect(tunneled, autocommit=autocommit, connect_timeout=30) as conn:
            yield conn
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd people-api-loader && uv run pytest tests/test_connections.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add people-api-loader/src/loader/people_api/db.py people-api-loader/tests/test_connections.py
git commit -m "feat(loader): route db connections through the bastion tunnel when configured"
```

---

### Task 5: Full lint + type + test sweep

**Files:** none (verification only)

- [ ] **Step 1: Lint, format, type-check, test**

Run:
```bash
cd people-api-loader && uv run ruff check && uv run ruff format --check && uv run ty check && uv run pytest -q
```
Expected: ruff clean, format clean, ty clean, all tests pass.

- [ ] **Step 2: Fix anything that fails, then re-run until green.**

- [ ] **Step 3: Commit any fixes**

```bash
git add -A people-api-loader
git commit -m "chore(loader): lint/type fixes for bastion connectivity"
```

---

## Notes for the implementer

- `LOADER_BASTION_*` env vars are set on the Astro deployment (Task in Plan B). Local/VPN runs leave them unset and connect directly — same behavior as today.
- The bastion private key is delivered as PEM material via `LOADER_BASTION_PRIVATE_KEY` (mirrors the legacy `gp_bastion_host` Airflow connection's `private_key` extra). The plan does not commit any key.
- This plan changes only the `people-api-loader` package. The DAG that sets these env vars and invokes the CLI is Plan B (DATA-1913 DAG); the S3 VPC endpoint is Plan C.
