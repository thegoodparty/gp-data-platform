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

    paramiko.DSSKey = _DSSKeyStub  # ty: ignore[unresolved-attribute]


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
