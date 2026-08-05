"""Thin boto3 helpers shared across steps."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
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
from loader.core.log import get_logger

log = get_logger(__name__)


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


def retry_after_settle(call: Callable[[], None], *, fault_code: str, settle: Callable[[], None]) -> None:
    """Run an RDS control-plane `call`; if it raises `fault_code` (a still-in-progress state on the
    same resource from a partial re-run), run `settle` (a waiter) and re-issue `call` once. Any other
    ClientError propagates, and a second `fault_code` on the retry propagates too — we never
    swallow-and-skip, because the same fault covers creating/deleting/failed states and a skipped
    call would leave the resource misconfigured.
    """
    try:
        call()
    except ClientError as e:
        if e.response["Error"]["Code"] != fault_code:
            raise
        log.warning("aws.retry_after_settle", fault=fault_code)
        settle()
        call()


_CLASS_APPLY_POLL_SECONDS = 30
_CLASS_APPLY_MAX_POLLS = 80  # ~40 min, generous for a class-change reboot


def wait_instance_class_applied(
    rds_client: BaseClient,
    instance_id: str,
    target: str,
    *,
    poll_seconds: int = _CLASS_APPLY_POLL_SECONDS,
    max_polls: int = _CLASS_APPLY_MAX_POLLS,
) -> None:
    """Poll until `instance_id` reports `DBInstanceClass == target`, status 'available', and no
    `DBInstanceClass` left in `PendingModifiedValues`. A single `db_instance_available` waiter is
    insufficient: Aurora keeps reporting 'available' for a few seconds after a class-change modify
    before it flips to 'modifying' and reboots, so the waiter returns on the stale state and any
    connection/reboot issued next races the delayed reboot.
    """
    for _ in range(max_polls):
        inst = rds_client.describe_db_instances(DBInstanceIdentifier=instance_id)["DBInstances"][0]
        pending = inst.get("PendingModifiedValues") or {}
        if (
            inst["DBInstanceClass"] == target
            and inst["DBInstanceStatus"] == "available"
            and "DBInstanceClass" not in pending
        ):
            return
        time.sleep(poll_seconds)
    raise RuntimeError(f"instance {instance_id} did not reach class {target} in time")


_SERVERLESS_CLASS = "db.serverless"


def flip_writer_to_serverless(
    rds_client: BaseClient, cluster_id: str, instance_id: str, *, min_acu: float, max_acu: float
) -> None:
    """Set the cluster's Serverless v2 scaling config, then flip the writer instance to
    db.serverless, tolerating in-progress modifies and waiting until the class change actually
    applies. Callers layer any extra lockdown (serve param group, backup, deletion protection,
    reboot) on top; this is only the class conversion.

    Generic RDS helper: no people-api knowledge (ACUs and ids are passed in, not read off a
    consumer's config). Used by both `resize` (which layers additional lockdown on top) and
    `scale_down` (which only needs this conversion).
    """
    cluster_waiter = rds_client.get_waiter("db_cluster_available")
    instance_waiter = rds_client.get_waiter("db_instance_available")
    waiter_cfg = {"Delay": 30, "MaxAttempts": 40}

    def _wait_cluster() -> None:
        cluster_waiter.wait(DBClusterIdentifier=cluster_id, WaiterConfig=waiter_cfg)

    def _wait_instance() -> None:
        instance_waiter.wait(DBInstanceIdentifier=instance_id, WaiterConfig=waiter_cfg)

    retry_after_settle(
        lambda: rds_client.modify_db_cluster(
            DBClusterIdentifier=cluster_id,
            ServerlessV2ScalingConfiguration={"MinCapacity": min_acu, "MaxCapacity": max_acu},
            ApplyImmediately=True,
        ),
        fault_code="InvalidDBClusterStateFault",
        settle=_wait_cluster,
    )
    # Instance can read available while the cluster is still modifying; unconditional wait gates
    # the modify_db_instance below (which only tolerates InvalidDBInstanceStateFault, not a
    # cluster-level fault).
    _wait_cluster()
    retry_after_settle(
        lambda: rds_client.modify_db_instance(
            DBInstanceIdentifier=instance_id, DBInstanceClass=_SERVERLESS_CLASS, ApplyImmediately=True
        ),
        fault_code="InvalidDBInstanceStateFault",
        settle=_wait_instance,
    )
    wait_instance_class_applied(rds_client, instance_id, _SERVERLESS_CLASS)


def flip_writer_to_provisioned(
    rds_client: BaseClient, cluster_id: str, instance_id: str, *, instance_class: str
) -> None:
    """Flip the writer instance to a provisioned `instance_class`, tolerating in-progress
    modifies and waiting until the class change actually applies. Callers layer any extra
    lockdown (serve param group, backup, deletion protection, reboot) on top; this is only the
    class conversion.

    Generic RDS helper: no people-api knowledge (the class and ids are passed in, not read off a
    consumer's config). Unlike `flip_writer_to_serverless`, a provisioned class needs no
    cluster-level `ServerlessV2ScalingConfiguration` — only the instance-level modify. `cluster_id`
    is accepted for call-site symmetry with `flip_writer_to_serverless`; it is not used to
    reconfigure the cluster here.
    """
    instance_waiter = rds_client.get_waiter("db_instance_available")

    def _wait_instance() -> None:
        instance_waiter.wait(DBInstanceIdentifier=instance_id, WaiterConfig={"Delay": 30, "MaxAttempts": 40})

    retry_after_settle(
        lambda: rds_client.modify_db_instance(
            DBInstanceIdentifier=instance_id, DBInstanceClass=instance_class, ApplyImmediately=True
        ),
        fault_code="InvalidDBInstanceStateFault",
        settle=_wait_instance,
    )
    wait_instance_class_applied(rds_client, instance_id, target=instance_class)


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


def put_ssm_parameter(
    cfg: BaseLoaderConfig,
    name: str,
    value: str,
    *,
    secure: bool = True,
    tags: list[dict[str, str]] | None = None,
) -> None:
    """Write (create or overwrite) an SSM Parameter Store value; SecureString by default.

    The parameter is tagged with the loader's resource tags for consistency. The loader's own
    Get/Put on this parameter is authorized by parameter NAME (the `loader-s3-ssm` role policy
    scopes `people-db-connection-string-{env}[-date]` by ARN, with no tag condition and no
    permissions boundary), NOT by the `Environment` tag — so omitting `Environment` via `tags`
    does not affect the loader's access; it only removes the tag a human role's
    `ResourceTag/Environment` deny keys on. Pass `tags` to override the set; defaults to
    `cfg.tags_as_aws()`.

    SSM forbids combining `Tags` with `Overwrite`, so a create-with-tags is tried first. If the
    parameter already exists it is deleted and recreated with `tags` — NOT overwrite + re-tag,
    because `add_tags_to_resource` only upserts keys and never removes them, so a tag intentionally
    dropped from `tags` (e.g. `Environment`) would linger. Delete + recreate enforces the exact set,
    and needs only the `ssm:DeleteParameter`/`PutParameter` the loader role already has (it has no
    `ssm:RemoveTagsFromResource`). The existing value is captured before the delete and restored
    (value only) if the recreate fails, so a transient error never leaves the parameter missing —
    which would otherwise strand provision's reuse branch on `ParameterNotFound`.

    NOTE: delete + recreate resets the parameter's version numbering and drops all version labels,
    so this must NOT be used on the serving parameter `promote` maintains — use
    `overwrite_ssm_parameter` there, which adds a version and preserves history + labels.
    """
    client = ssm(cfg)
    param_type = "SecureString" if secure else "String"
    param_tags = cfg.tags_as_aws() if tags is None else tags
    try:
        client.put_parameter(Name=name, Value=value, Type=param_type, Tags=param_tags)
    except client.exceptions.ParameterAlreadyExists:
        old_value = client.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
        client.delete_parameter(Name=name)
        try:
            client.put_parameter(Name=name, Value=value, Type=param_type, Tags=param_tags)
        except Exception:
            # Best-effort restore so the param is never left missing. Value only (Overwrite forbids
            # Tags); the caller can retry put_ssm_parameter to reach the intended tag state.
            client.put_parameter(Name=name, Value=old_value, Type=param_type, Overwrite=True)
            raise


def overwrite_ssm_parameter(cfg: BaseLoaderConfig, name: str, value: str) -> int:
    """Overwrite an existing SecureString parameter's value, creating a NEW version and preserving
    the parameter's version history and labels — unlike `put_ssm_parameter`, which delete+recreates
    an existing parameter (resetting version numbering and dropping labels) to enforce an exact tag
    set.

    This is the serving-cutover write: `promote` overwrites the single serving parameter so each
    refresh is a new version it can label `build-{date}` and move `live` onto, and rollback can
    move `live` back to a prior version's label. Tags are left untouched (Overwrite forbids Tags,
    and the serving parameter's access is by ARN, not tag). Returns the new version.
    """
    resp = ssm(cfg).put_parameter(Name=name, Value=value, Type="SecureString", Overwrite=True)
    return int(resp["Version"])


def label_ssm_parameter_version(cfg: BaseLoaderConfig, name: str, version: int, labels: list[str]) -> None:
    """Attach version label(s) to a specific SSM parameter version.

    A version label is the human-readable anchor for which refresh a serving-parameter version
    came from (traceability + rollback), or a moving pointer to the version being served. A label
    lives on exactly one version at a time, so re-applying an existing label moves it (this is how
    the serving `live` pointer is advanced). SSM rejects labels that begin with a digit or with
    `aws`/`ssm`, so a bare date stamp is invalid — callers prefix it (e.g. `build-<date>`).
    """
    ssm(cfg).label_parameter_version(Name=name, ParameterVersion=version, Labels=labels)


def sts(cfg: BaseLoaderConfig) -> BaseClient:
    return session(cfg).client("sts")


def verify_caller(cfg: BaseLoaderConfig) -> dict[str, Any]:
    """sts:GetCallerIdentity sanity check. Call once at CLI startup."""
    return sts(cfg).get_caller_identity()
