"""Tests for the generic SFTP helpers.

Only the retry loop is covered. L2's server drops connections often enough that the retry is
what keeps a nightly sync from failing on a blip, and a transport left open on a failed attempt
leaks a socket per retry.
"""

import pytest
from include.custom_functions import sftp_utils
from include.custom_functions.sftp_utils import create_sftp_connection


class FakeTransport:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.closed = False

    def set_keepalive(self, interval):
        pass

    def connect(self, username, password):
        if self.fail:
            raise OSError("connection refused")

    def close(self):
        self.closed = True


class FakeSFTPClient:
    @staticmethod
    def from_transport(transport):
        return "client"


def _connect(monkeypatch, transports):
    queue = list(transports)
    monkeypatch.setattr(sftp_utils, "Transport", lambda address: queue.pop(0))
    monkeypatch.setattr(sftp_utils, "SFTPClient", FakeSFTPClient)
    monkeypatch.setattr(sftp_utils.time, "sleep", lambda _: None)
    return create_sftp_connection(host="h", port=22, username="u", password="p", retry_delay=0)


def test_returns_the_transport_and_client(monkeypatch):
    transport = FakeTransport()
    assert _connect(monkeypatch, [transport]) == (transport, "client")
    assert not transport.closed


def test_a_failed_attempt_is_closed_before_the_next_one(monkeypatch):
    """Leaving it open leaks a socket per retry."""
    failed, good = FakeTransport(fail=True), FakeTransport()

    assert _connect(monkeypatch, [failed, good]) == (good, "client")
    assert failed.closed
    assert not good.closed


def test_exhausting_the_retries_reraises(monkeypatch):
    """The last failure surfaces, rather than a generic post-loop error."""
    transports = [FakeTransport(fail=True) for _ in range(3)]

    with pytest.raises(OSError, match="connection refused"):
        _connect(monkeypatch, transports)
    assert all(transport.closed for transport in transports)
