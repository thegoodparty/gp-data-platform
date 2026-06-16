"""Thin boto3 helpers shared across steps."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from functools import cache
from typing import TYPE_CHECKING, Any

import boto3
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from botocore.client import BaseClient

from loader.core.config import BaseLoaderConfig


@contextmanager
def ignore_client_errors(*codes: str) -> Iterator[None]:
    """Swallow the given AWS error codes, re-raising anything else.

    The idempotency primitive for create/delete steps: e.g. wrap a create call with
    `ignore_client_errors("DBParameterGroupAlreadyExists")`, or a delete with the
    matching not-found code. To branch on whether the call was swallowed, put a `return`
    after the call inside the `with` block — code below the block runs only on swallow.
    """
    try:
        yield
    except ClientError as e:
        if e.response["Error"]["Code"] not in codes:
            raise


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


def ssm(cfg: BaseLoaderConfig) -> BaseClient:
    return session(cfg).client("ssm")


def get_ssm_parameter(cfg: BaseLoaderConfig, name: str, *, decrypt: bool = True) -> str:
    """Fetch an SSM Parameter Store value (SecureString decrypted by default)."""
    resp = ssm(cfg).get_parameter(Name=name, WithDecryption=decrypt)
    return resp["Parameter"]["Value"]


def put_ssm_parameter(cfg: BaseLoaderConfig, name: str, value: str, *, secure: bool = True) -> None:
    """Write (overwrite) an SSM Parameter Store value; SecureString by default."""
    ssm(cfg).put_parameter(
        Name=name, Value=value, Type="SecureString" if secure else "String", Overwrite=True
    )


def sts(cfg: BaseLoaderConfig) -> BaseClient:
    return session(cfg).client("sts")


def verify_caller(cfg: BaseLoaderConfig) -> dict[str, Any]:
    """sts:GetCallerIdentity sanity check. Call once at CLI startup."""
    return sts(cfg).get_caller_identity()
