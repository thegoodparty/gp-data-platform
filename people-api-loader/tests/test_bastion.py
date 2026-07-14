"""open_tunnel: yields the target unchanged when no bastion; localhost+local port when set."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api import bastion
from loader.people_api.config import LoaderConfig


def _cfg(**kw) -> LoaderConfig:
    fields = {
        "bastion_host": "",
        "bastion_port": 22,
        "bastion_user": "",
        "bastion_private_key": "",
        "bastion_private_key_passphrase": "",
    }
    fields.update(kw)
    ns = SimpleNamespace(**fields)
    ns.bastion_enabled = bool(ns.bastion_host)  # mirror the real LoaderConfig.bastion_enabled property
    return cast(LoaderConfig, ns)


def test_no_bastion_yields_target_unchanged():
    cfg = _cfg()
    with bastion.open_tunnel(cfg, "rds.internal", 5432) as (host, port):
        assert (host, port) == ("rds.internal", 5432)


def test_load_key_raises_value_error_on_malformed_pem():
    # Garbage material (no key markers at all — deliberately not a real-looking PEM block, so it
    # doesn't trip secret-scanning) that none of the key types _load_key tries (Ed25519/RSA/ECDSA)
    # can parse — must raise ValueError with a clear message, not an unhandled SSHException.
    with pytest.raises(ValueError, match="Could not load bastion SSH key"):
        bastion._load_key("this is not PEM material of any kind, just plain garbage text")


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
    monkeypatch.setattr(bastion, "_load_key", lambda pem, passphrase="": "PKEY")

    cfg = _cfg(bastion_host="b", bastion_user="u", bastion_private_key="PEM")
    with bastion.open_tunnel(cfg, "rds.internal", 5432) as (host, port):
        assert (host, port) == ("127.0.0.1", 54321)
    assert started == {"remote": ("rds.internal", 5432), "started": True, "stopped": True}
