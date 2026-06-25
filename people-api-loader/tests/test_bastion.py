"""open_tunnel: yields the target unchanged when no bastion; localhost+local port when set."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

from loader.people_api import bastion
from loader.people_api.config import LoaderConfig


def _cfg(**kw) -> LoaderConfig:
    base = dict(
        bastion_host="",
        bastion_port=22,
        bastion_user="",
        bastion_private_key="",
        bastion_enabled=False,
    )
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

    cfg = _cfg(
        bastion_host="b",
        bastion_user="u",
        bastion_private_key="PEM",
        bastion_enabled=True,
    )
    with bastion.open_tunnel(cfg, "rds.internal", 5432) as (host, port):
        assert (host, port) == ("127.0.0.1", 54321)
    assert started == {"remote": ("rds.internal", 5432), "started": True, "stopped": True}
