"""The Bedrock session builder: on Astro the pod's own identity is Astronomer's
and must never be what calls Bedrock. No network: static env credentials head
the chain and the STS fetcher is a fake."""

import pytest
from botocore.config import Config

from bedrock_clients import aws, embedding, structured


@pytest.fixture(autouse=True)
def _fresh_cache():
    aws.bedrock_session.cache_clear()
    yield
    # A test may have swapped the builder for a plain fake; only the real one caches.
    if hasattr(aws.bedrock_session, "cache_clear"):
        aws.bedrock_session.cache_clear()


def test_without_a_role_the_ambient_chain_is_used(monkeypatch):
    """Failure this catches: a local supervised run trying to assume a role it
    was never given, instead of using the operator's own credentials."""
    monkeypatch.delenv(aws.ROLE_ARN_ENV, raising=False)
    # Static env credentials resolve first in the chain, so nothing here can
    # reach a metadata endpoint.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AMBIENT")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ambient-secret")
    monkeypatch.setattr(aws, "AssumeRoleCredentialFetcher", lambda **kw: pytest.fail("no role was named"))

    s = aws.bedrock_session("us-east-1")

    assert s.region_name == "us-east-1"
    assert s.get_credentials().access_key == "AMBIENT"


def test_with_a_role_the_session_carries_the_assumed_credentials(monkeypatch):
    """Failure this catches: the target role or the ExternalId not reaching STS
    (the trust policy then refuses the pod), the assumed credentials not being
    what the session resolves (ambient ones silently used instead), or the
    assumption happening at construction rather than on first use."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AMBIENT")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ambient-secret")
    monkeypatch.setenv(aws.ROLE_ARN_ENV, "arn:aws:iam::333:role/gold-match-bedrock-dev")
    monkeypatch.setenv(aws.EXTERNAL_ID_ENV, "ext-123")
    captured: dict = {}
    fetches: list[int] = []

    class _Fetcher:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def fetch_credentials(self):
            fetches.append(1)
            return {
                "access_key": "ASSUMED",
                "secret_key": "assumed-secret",
                "token": "t",
                "expiry_time": "2099-01-01T00:00:00Z",
            }

    monkeypatch.setattr(aws, "AssumeRoleCredentialFetcher", _Fetcher)

    s = aws.bedrock_session("us-east-1")

    assert s.region_name == "us-east-1"
    assert captured["role_arn"] == "arn:aws:iam::333:role/gold-match-bedrock-dev"
    assert captured["extra_args"] == {"RoleSessionName": "gold-match", "ExternalId": "ext-123"}
    assert fetches == []  # nothing assumed until a call needs credentials
    assert s.get_credentials().access_key == "ASSUMED"
    assert fetches == [1]


def test_both_clients_build_their_runtime_from_the_shared_session(monkeypatch):
    """Failure this catches: a client constructing a bare boto3 client again,
    which on Astro calls Bedrock as Astronomer's identity: refused by IAM at
    best, billed to the wrong account at worst."""
    calls: list[tuple[str, Config | None]] = []

    class _Session:
        def client(self, service, **kwargs):
            calls.append((service, kwargs.get("config")))
            return object()

    monkeypatch.setattr(aws, "bedrock_session", lambda region: _Session())

    embedding.BedrockEmbeddingClient(max_concurrency=7)
    structured.BedrockStructuredContentClient(max_concurrency=9)

    assert [c[0] for c in calls] == ["bedrock-runtime", "bedrock-runtime"]
    assert [c[1].max_pool_connections for c in calls] == [7, 9]
    assert all(c[1].retries == {"max_attempts": 1, "mode": "standard"} for c in calls)
