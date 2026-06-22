# Loader S3 bucket + Databricks UC external location — DATA-1905

**Status:** approved design, pre-implementation
**Date:** 2026-06-22
**Relates to:** DATA-1640, DATA-1905, DATA-1908 (unload — writes here), DATA-1851 (copy — Aurora reads
here), DATA-1856 (the rds-s3-import role whose S3 read must cover this bucket).
**Delivery split:** the Terraform + apply live in `gp-terraform-dataplatform` and are ops/admin
(like DATA-1856). This repo's deliverable is (a) this requirements spec and (b) loader **config wiring**
to consume the bucket name.

## Problem

`unload` (DATA-1908) writes voter export files to S3 from Databricks; `copy` (DATA-1851) reads them
into Aurora via `aws_s3.table_import_from_s3`. That requires a dedicated S3 bucket that **Databricks can
write to** (a Unity Catalog external location + storage credential) and **Aurora can read from** (the
rds-s3-import role from DATA-1856). This is the one-time infra prerequisite for an end-to-end run.

## Requirements (for the ops/Terraform ticket in gp-terraform-dataplatform)

1. **Dedicated S3 bucket** (the ticket calls for a dedicated bucket, not reuse):
   - Region `us-west-2` (same as the loader / Aurora, so `aws_s3` import is in-region).
   - Block all public access; default encryption SSE-KMS with the loader KMS key (or SSE-S3 if KMS
     scoping is deferred) — consistent with the cluster's `StorageEncrypted`.
   - Lifecycle rule expiring `voter_export_*/` prefixes after N days (exports are per-run, disposable
     once cutover completes) — keeps cost down; N is an ops choice (suggest 30).
   - Layout the loader assumes: `s3://{bucket}/voter_export_{run_date}/state=XX/<part files>` plus
     `voter_export_{run_date}/_manifest/<step>.json`.
2. **Databricks Unity Catalog external location + storage credential** covering the bucket, with
   **WRITE** for the principal the loader's SQL warehouse runs as — so `unload`'s
   `INSERT OVERWRITE DIRECTORY 's3://{bucket}/...'` is authorized by UC. (The loader writes to the
   `s3://` URI directly; UC checks it against the external location — the location is not named in SQL.)
3. **Aurora read access:** extend the DATA-1856 `rds-s3-import` role's S3 read policy to include this
   bucket's ARN (`arn:aws:s3:::{bucket}` + `/*`), so `aws_s3.table_import_from_s3` can read the files.
   If the bucket is SSE-KMS, the role also needs `kms:Decrypt` on the key.

## This repo's deliverable: config wiring

- The loader already has `s3_bucket` (`LOADER_S3_BUCKET`, default `gp-voter-loader`) and
  `export_prefix(run_date) -> voter_export_{run_date}`. Point `LOADER_S3_BUCKET` at the dedicated
  bucket's name (no code change beyond confirming the default / `.env.example` documents it).
- No bucket/credential identifiers are committed (public repo): the name comes from `LOADER_S3_BUCKET`,
  consistent with the other infra identifiers being empty/placeholder defaults filled at runtime.
- `.env.example`: document `LOADER_S3_BUCKET` as the dedicated loader bucket and that it must be covered
  by a UC external location (Databricks WRITE) + the rds-s3-import role (Aurora READ).

## Verification (operator, once applied)

- `unload --state FL --skip-submit` prints the `INSERT OVERWRITE DIRECTORY` targeting the bucket; a real
  `--state FL` run writes `state=FL/` part files (confirms Databricks WRITE via UC).
- `copy --state FL` imports them (confirms Aurora READ via the rds-s3-import role) — the FL-first
  milestone already in the plan exercises both ends.

## Non-goals

- No application of Terraform from this repo; no creation of AWS/Databricks resources here.
- Bucket/credential rotation, cross-region replication: out of scope.
