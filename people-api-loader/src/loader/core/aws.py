"""Thin boto3 helpers shared across steps."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from functools import cache
from threading import Lock
from typing import TYPE_CHECKING, Any

import boto3
import botocore.session
from botocore.credentials import AssumeRoleCredentialFetcher, DeferredRefreshableCredentials
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
def _session(
    profile: str | None,
    region: str,
    assume_role_arn: str | None = None,
    external_id: str | None = None,
) -> boto3.Session:
    base = boto3.Session(profile_name=profile, region_name=region)
    if not assume_role_arn:
        return base
    # Assume the target role with auto-refreshing credentials. Loader runs (provision + the
    # parallel copy/index fan-out) can outlast the default 1h assume-role TTL, so static creds
    # would expire mid-run; DeferredRefreshableCredentials re-assumes on expiry transparently.
    botocore_base = base._session
    extra_args: dict[str, str] = {"RoleSessionName": "people-api-loader"}
    if external_id:
        extra_args["ExternalId"] = external_id
    fetcher = AssumeRoleCredentialFetcher(
        client_creator=botocore_base.create_client,
        source_credentials=botocore_base.get_credentials(),
        role_arn=assume_role_arn,
        extra_args=extra_args,
    )
    creds = DeferredRefreshableCredentials(method="assume-role", refresh_using=fetcher.fetch_credentials)
    assumed = botocore.session.Session()
    assumed._credentials = creds
    assumed.set_config_variable("region", region)
    return boto3.Session(botocore_session=assumed)


def session(cfg: BaseLoaderConfig) -> boto3.Session:
    return _session(cfg.aws_profile, cfg.aws_region, cfg.assume_role_arn, cfg.assume_role_external_id)


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


_ssm_cache: dict[tuple[str | None, str, str, bool], str] = {}
_ssm_cache_lock = Lock()


def get_ssm_parameter(cfg: BaseLoaderConfig, name: str, *, decrypt: bool = True) -> str:
    """Fetch an SSM Parameter Store value (SecureString decrypted by default).

    Cached for the process lifetime, keyed on session identity (profile, region) + name +
    decrypt. Loader-written params (connection strings) are immutable within a run, and the
    parallel fan-out in copy_s3 (up to 128 workers) and build_indexes (32) opens many
    connections at once — uncached, the repeated GetParameter calls on the same name would
    exceed SSM's 40 TPS standard-tier throttle. The lock makes a cold concurrent burst do
    exactly one live call: contenders block, then read the populated value.
    """
    key = (cfg.aws_profile, cfg.aws_region, name, decrypt)
    cached = _ssm_cache.get(key)
    if cached is not None:
        return cached
    with _ssm_cache_lock:
        if key not in _ssm_cache:  # double-checked: only the first cold caller hits the API
            resp = ssm(cfg).get_parameter(Name=name, WithDecryption=decrypt)
            _ssm_cache[key] = resp["Parameter"]["Value"]
        return _ssm_cache[key]


def put_ssm_parameter(cfg: BaseLoaderConfig, name: str, value: str, *, secure: bool = True) -> None:
    """Write (create or overwrite) an SSM Parameter Store value; SecureString by default.

    The parameter is tagged with the loader's Environment tags so IAM policies scoped by
    `aws:ResourceTag/Environment` (the loader's permissions boundary) allow subsequent
    Get/Describe. SSM forbids combining `Tags` with `Overwrite` in one call, so we
    create-with-tags first and fall back to overwrite + re-tag if it already exists.
    """
    client = ssm(cfg)
    param_type = "SecureString" if secure else "String"
    try:
        client.put_parameter(Name=name, Value=value, Type=param_type, Tags=cfg.tags_as_aws())
    except client.exceptions.ParameterAlreadyExists:
        client.put_parameter(Name=name, Value=value, Type=param_type, Overwrite=True)
        client.add_tags_to_resource(ResourceType="Parameter", ResourceId=name, Tags=cfg.tags_as_aws())


def sts(cfg: BaseLoaderConfig) -> BaseClient:
    return session(cfg).client("sts")


def verify_caller(cfg: BaseLoaderConfig) -> dict[str, Any]:
    """sts:GetCallerIdentity sanity check. Call once at CLI startup."""
    return sts(cfg).get_caller_identity()
