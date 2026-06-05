"""Manifest read/write against S3.

Every step starts with `read_manifest(...)` — if present and
`status == "complete"`, the step logs and returns early. Every step ends
(on success) with `write_manifest(...)` setting `status = "complete"`.

Manifest JSON is pretty-printed so diffs in S3 versioning or in the
reviewer's eyeball are readable.
"""

from __future__ import annotations

import json

from botocore.exceptions import ClientError

from loader.core.aws import s3
from loader.core.config import BaseLoaderConfig
from loader.core.manifest.models import ManifestBase


def manifest_uri(cfg: BaseLoaderConfig, run_date: str, step: str) -> str:
    return f"s3://{cfg.s3_bucket}/{cfg.manifest_key(run_date, step)}"


def read_manifest[M: ManifestBase](
    cfg: BaseLoaderConfig, run_date: str, step: str, model: type[M]
) -> M | None:
    """Return the parsed manifest for (run_date, step), or None if absent."""
    client = s3(cfg)
    key = cfg.manifest_key(run_date, step)
    try:
        resp = client.get_object(Bucket=cfg.s3_bucket, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404", "NotFound"):
            return None
        raise
    body = resp["Body"].read().decode("utf-8")
    return model.model_validate_json(body)


def write_manifest(cfg: BaseLoaderConfig, manifest: ManifestBase) -> str:
    """Write the manifest to S3 and return its s3:// URI."""
    client = s3(cfg)
    key = cfg.manifest_key(manifest.run_date, manifest.step)
    body = manifest.model_dump_json(indent=2).encode("utf-8")
    client.put_object(
        Bucket=cfg.s3_bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    return f"s3://{cfg.s3_bucket}/{key}"


def put_artifact(cfg: BaseLoaderConfig, run_date: str, subpath: str, body: bytes | str) -> str:
    """Store an auxiliary artifact under {export_prefix}/{subpath}."""
    client = s3(cfg)
    key = f"{cfg.export_prefix(run_date)}/{subpath}"
    if isinstance(body, str):
        body = body.encode("utf-8")
    client.put_object(Bucket=cfg.s3_bucket, Key=key, Body=body)
    return f"s3://{cfg.s3_bucket}/{key}"


def load_artifact_json(cfg: BaseLoaderConfig, uri: str) -> dict:
    assert uri.startswith(f"s3://{cfg.s3_bucket}/"), f"unexpected bucket: {uri}"
    key = uri.removeprefix(f"s3://{cfg.s3_bucket}/")
    resp = s3(cfg).get_object(Bucket=cfg.s3_bucket, Key=key)
    return json.loads(resp["Body"].read())
