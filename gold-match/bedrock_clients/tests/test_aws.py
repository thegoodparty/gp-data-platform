"""The Bedrock session builder: on Astro the pod's own identity is Astronomer's
and must never be what calls Bedrock, and the assumed credentials must refresh
across a multi-hour run. No network: the fetcher is a fake, never invoked."""

import boto3
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
    s = aws.bedrock_session("us-east-1")
    assert isinstance(s, boto3.Session)
    assert s.region_name == "us-east-1"


def test_with_a_role_the_session_assumes_it_lazily_with_the_external_id(monkeypatch):
    """Failure this catches: the target role or the ExternalId not reaching STS
    (the trust policy then refuses the pod), or credentials fetched eagerly at
    construction (one fixed hour of validity, expiring mid-wave)."""
    captured: dict = {}

    class _Fetcher:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def fetch_credentials(self):
            raise AssertionError("credentials must be fetched lazily, not at session build")

    monkeypatch.setattr(aws, "AssumeRoleCredentialFetcher", _Fetcher)
    monkeypatch.setenv(aws.ROLE_ARN_ENV, "arn:aws:iam::333:role/gold-match-bedrock-dev")
    monkeypatch.setenv(aws.EXTERNAL_ID_ENV, "ext-123")

    s = aws.bedrock_session("us-east-1")

    assert isinstance(s, boto3.Session)
    assert s.region_name == "us-east-1"
    assert captured["role_arn"] == "arn:aws:iam::333:role/gold-match-bedrock-dev"
    assert captured["extra_args"] == {"RoleSessionName": "gold-match", "ExternalId": "ext-123"}


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
