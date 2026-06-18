"""Thin boto3 helpers shared across steps."""

from __future__ import annotations

from functools import cache
from typing import TYPE_CHECKING, Any

import boto3

if TYPE_CHECKING:
    from botocore.client import BaseClient

from loader.core.config import BaseLoaderConfig


@cache
def _session(profile: str | None, region: str) -> boto3.Session:
    return boto3.Session(profile_name=profile, region_name=region)


def session(cfg: BaseLoaderConfig) -> boto3.Session:
    return _session(cfg.aws_profile, cfg.aws_region)


def s3(cfg: BaseLoaderConfig) -> BaseClient:
    return session(cfg).client("s3")


def rds(cfg: BaseLoaderConfig) -> BaseClient:
    return session(cfg).client("rds")


def iam(cfg: BaseLoaderConfig) -> BaseClient:
    return session(cfg).client("iam")


def ec2(cfg: BaseLoaderConfig) -> BaseClient:
    return session(cfg).client("ec2")


def secrets(cfg: BaseLoaderConfig) -> BaseClient:
    return session(cfg).client("secretsmanager")


def ssm(cfg: BaseLoaderConfig) -> BaseClient:
    return session(cfg).client("ssm")


def get_ssm_parameter(cfg: BaseLoaderConfig, name: str, *, decrypt: bool = True) -> str:
    """Fetch an SSM Parameter Store value (SecureString decrypted by default)."""
    resp = ssm(cfg).get_parameter(Name=name, WithDecryption=decrypt)
    return resp["Parameter"]["Value"]


def sts(cfg: BaseLoaderConfig) -> BaseClient:
    return session(cfg).client("sts")


def get_secret(cfg: BaseLoaderConfig, secret_id: str) -> str:
    """Fetch a secret string. Raises if the secret is missing/non-string."""
    resp = secrets(cfg).get_secret_value(SecretId=secret_id)
    if "SecretString" not in resp:
        raise RuntimeError(f"Secret {secret_id!r} has no SecretString")
    return resp["SecretString"]


def put_secret(cfg: BaseLoaderConfig, secret_id: str, value: str, description: str = "") -> None:
    """Create a secret or overwrite its value idempotently.

    Tags are applied at create time so that IAM policies scoped by
    `aws:ResourceTag/Environment` (the pattern on this workspace's SSO
    role) allow subsequent Get/Describe on the secret.
    """
    client = secrets(cfg)
    try:
        client.create_secret(
            Name=secret_id,
            Description=description,
            SecretString=value,
            Tags=cfg.tags_as_aws(),
        )
    except client.exceptions.ResourceExistsException:
        # Re-tag first: a pre-existing secret (manual, older loader version, or
        # one whose tag was removed) may be untagged, and the value update below
        # is pointless if `aws:ResourceTag/Environment` IAM conditions then deny
        # GetSecretValue. tag_resource is idempotent on an already-tagged secret.
        client.tag_resource(SecretId=secret_id, Tags=cfg.tags_as_aws())
        client.put_secret_value(SecretId=secret_id, SecretString=value)


def verify_caller(cfg: BaseLoaderConfig) -> dict[str, Any]:
    """sts:GetCallerIdentity sanity check. Call once at CLI startup."""
    return sts(cfg).get_caller_identity()
