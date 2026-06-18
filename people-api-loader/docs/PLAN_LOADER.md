# Voter Data Loader — Refresh + Redesign Plan

> **Superseded — read this first.** This plan predates the current design. The
> loader now builds the **unified, partitioned `public."Voter"` table** that prod
> actually uses: a single table `PARTITION BY LIST ("State")` with per-state child
> partitions (`Voter_TX`, …), **not** the 51 standalone `Voter{ST}` tables
> described below. The per-state-table model here is stale; see the implemented
> loader steps and PR #477 for the current design. Also note: `people-api` and
> `gp-api` are no longer standalone repos — they now live in the **omni monorepo**
> at `omni/packages/people-api` and `omni/packages/gp-api` (the old standalone
> repos are archived). Paths below that read `people-api/...` mean
> `omni/packages/people-api/...`.
>
> **Cluster identity:** the Present (prod) cluster is **`gp-people-db-prod`**, the
> renamed `gp-voter-db-20250728` (same physical cluster — shared
> `cluster-<hash>` endpoint hash). It holds the unified `public."Voter"` shape
> (single Prisma-managed table), not the legacy per-state `Voter{ST}` tables or the
> separate "schema `green`" people-api DB some prose below describes. New clusters the
> loader provisions are `gp-people-db-{date}`.
>
> **Connection method:** `connect_prod` reads the Present cluster's connection string
> from an SSM Parameter Store SecureString, `people-db-connection-string-{env}` (env from
> `LOADER_ENV`, dev/qa/prod; dev and qa share a value, prod is separate). Mentions of
> `~/.pg_service.conf [people]` / `service=people` / `LOADER_PROD_CONFIG_SECRET_ID` below
> are superseded by this. The new (provisioned) cluster is reached the same way:
> `connect_new` reads `people-db-connection-string-{env}-{date}`, an SSM SecureString that
> `provision` writes with the generated master password embedded in the `postgresql://`
> URL. The loader no longer uses AWS Secrets Manager at all; prose below mentioning
> `gp-people-db/{date}/master` secrets, `_ensure_master_secret`, or `secretsmanager:*`
> grants is superseded.
>
> **Schema source (DATA-1904):** the committed `schema/data/prod_dump.sql` is retired.
> The Voter table DDL is generated from the `m_people_api__voter` mart by `loader emit-ddl`
> into `schema/data/target_schema.sql` (columns/types from Databricks introspection + a
> declared Prisma layer in `schema_spec`); the PK + ~266 indexes come from `_serving_seed.py`,
> captured from the serving cluster's `pg_catalog` by `loader extract-serving-structure`.
> `create-schema`/`copy`/`build-indexes` read those generated artifacts. Scope is Voter-only;
> the District family is built by the dbt write path. See
> `docs/superpowers/specs/2026-06-18-people-api-ddl-generation-design.md`.

## Context

GoodParty.org exports US voter data to political-candidate users. Current pipeline:

- L2 voter data lands in Databricks.
- dbt compiles it into a single nationwide voter table (`int__l2_nationwide_uniform` — ~218M rows as of 2026-04-16).
- A dbt Python model copies that table to an RDS PostgreSQL (Aurora 16) instance, partitioned into 51 state tables (`public."VoterTX"`, `public."VoterFL"`, … — note the mixed-case quoted identifiers, not snake_case).
- Indexes/constraints are built on those tables.
- The app queries the RDS instance to produce per-user exports.

**Problem:** full refresh currently takes >2 weeks (slow INSERT-style loads, in-place UPSERT into indexed tables). Data has gone stale.

**Goal:**
1. Do a one-off manual refresh into a *new* RDS cluster to get fresh data to users immediately.
2. Materialize each step in Python so the same code can eventually be wrapped in a CLI, containerized, and run from Airflow (we recently adopted Astro/Airflow).

**Design stance:** treat the serving RDS cluster as an **immutable artifact** — built once per refresh from scratch, single-purpose, tuned for read serving. Never mutated in place.

---

## Reference Materials (for anyone picking this up)

- **Legacy loader code:** `gp-data-platform/dbt/project/models/write/write__l2_databricks_to_gp_api.py`. Current dbt Python model copying from Databricks to PostgreSQL. Contains the canonical `VOTER_COLUMN_LIST` (348 columns), `REMOVED_COLUMNS` (15 columns L2 stopped delivering — kept as NULL placeholders for schema compatibility), `INTEGER_COLUMNS` (6 columns cast to `INT`), and the full `UPSERT_QUERY` template. **Note:** L2 has added ~470 more columns on the Databricks side since this list was frozen (807 total), but most of those are consumer-enrichment data no app reads. The new loader scopes by *referenced columns* (see "Column Scoping" below), not by source schema.
- **Current app-side query construction:** `gp-api/src/voters/voterFile/util/voterFile.util.ts` (NOTE: file is `voterFile.util.ts`, not `voterFile.utils.ts`). Builds raw SQL against `public."Voter{STATE}"` tables — this is the serving layer we're refreshing.
- **Forward-looking app-side query construction:** `omni/packages/people-api` (alongside `omni/packages/gp-api` in the omni monorepo). The `voterFile.util.ts` file has a standing TODO pointing at ticket ENG-5032 that says the gp-api should stop hitting the raw voter DB and delegate to the people-api instead. The people-api has its own Voter table (in schema `green`, not `public`) and its own dedicated DB, populated by its own loader — so a gp-people-db refresh does not touch it directly. *But* the people-api's Voter schema and filter builder are the authoritative answer to "what columns might be referenced by any current or near-term consumer." Key files (under `omni/packages/people-api/`):
  - `prisma/schema/Voter.prisma` — the Voter model (names are a rename layer over L2-native; see mapping table in "Column Scoping" below).
  - `src/people/schemas/filters.schema.ts` — the enum of exposed filter keys.
  - `src/people/utils/filters.sql.utils.ts` — maps filter keys to L2-native column names + value mappers.
  - `src/people/people.select.ts` — default `SELECT` column list for list/download endpoints.
- **Source dbt model:** `int__l2_nationwide_uniform` at `gp-data-platform/dbt/project/models/intermediate/l2/int__l2_nationwide_uniform.py`. This is the table to unload from — it's the union of all per-state staging tables. Inspected row count: **218,278,803** (51 states incl. DC; see per-state counts in "Data Volume" below).
- **CLI access available:**
  - `aws` — authenticated to account `<aws-account-id>`, default region should be set to `us-west-2` for all loader operations.
  - `databricks` — authenticated; use for workspace/cluster/secret inspection. For querying the voter table itself, `dbt show --inline` from `gp-data-platform/dbt/project/` is faster (handles catalog/schema resolution).
  - `dbt` — dbt cloud CLI; run from `gp-data-platform/dbt/project/`.
  - `psql` - Passwordless login available with `psql service=people`

---

## Inspected Prod Environment (2026-04-16)

Captured from the live `gp-people-db-prod` cluster and Databricks source. Use these as the concrete defaults driving the new cluster's config.

### Cluster
- **Engine:** `aurora-postgresql` **16.8**.
- **Cluster parameter group:** `default.aurora-postgresql16` (the AWS default — **no custom tuning in prod today**). `shared_preload_libraries = pg_stat_statements`. `max_connections = LEAST({DBInstanceClassMemory/9531392},5000)` (auto-sized). `maintenance_work_mem` and `work_mem` at engine defaults.
- **Serverless v2:** MinCapacity = 0.5 ACU, MaxCapacity = 128 ACU. Single writer instance (`db.serverless`), no reader.
- **Backup retention:** 14 days. **Deletion protection:** on. **Storage encrypted:** yes.
- **KMS key:** `arn:aws:kms:us-west-2:<aws-account-id>:key/<kms-key-id>` (reuse for new cluster).
- **Tags:** `Project=gp-api`, `Environment=prod` — match on the new cluster so the app team's IaC filters work.
- **Writer endpoint:** `gp-people-db-prod.cluster-<hash>.us-west-2.rds.amazonaws.com:5432`. The app's connection string will change on cutover — flag in manifest.

### Network
- **VPC:** `<vpc-id>` (us-west-2). Use the same for the new cluster.
- **DB subnet group:** `<db-subnet-group>` — subnets `<subnet-id>` (us-west-2a, 10.0.4.0/22) and `<subnet-id>` (us-west-2b, 10.0.12.0/22). Private. Reuse.
- **Security group:** `<security-group-id>` (name `api-master-rds-security-group`). Inbound 5432 from:
  - `10.0.0.0/16` (VPC internal, covers the app fleet).
  - `172.16.0.0/16` (comment: "databricks via vpc peering" — the legacy loader connects over this).
  - Peer SG `<security-group-id>` (app servers / bastion).
  - Reuse the same SG for the new cluster; add a temporary rule for the Airflow worker / bastion used to run the loader orchestrator.
- **S3 VPC endpoint:** **NOT PRESENT** in `<vpc-id>`. Must be created before step 4 — without it, `aws_s3.table_import_from_s3` round-trips S3 over the NAT gateway (both bandwidth-limited and costly).

### Database / App Contract
- **Database name / master user:** not committed — supplied via the loader-config secret (`LOADER_PROD_CONFIG_SECRET_ID`) or `~/.pg_service.conf`. **Schema:** `public`.
- **Table naming:** `"Voter{STATE_ABBR_UPPER}"` (quoted, capital V). E.g. `public."VoterTX"`, `public."VoterFL"`. 51 tables (50 states + DC). **Not** `voter_tx` / `voter_fl` as originally drafted — update any DDL helpers accordingly.
- **Log/tracking table:** `public."VoterFile"` — load orchestrator writes rows here (`Filename`, `State`, `Lines`, `Loaded`, `updatedAt`) after each state finishes. Keep this contract so a legacy loader rerun would still be recognized.
- **Primary key:** `"LALVOTERID"` on every `"Voter{STATE}"` table. Legacy loader uses `ON CONFLICT ("LALVOTERID") DO UPDATE` — the new loader drops upsert semantics (clean-build model), but the PK must still exist.
- **Column count (today, in prod PG):** 348 (see `VOTER_COLUMN_LIST` in `write__l2_databricks_to_gp_api.py`). All column identifiers are mixed-case, quoted. Order in `VOTER_COLUMN_LIST` is the canonical order for the *legacy* schema. The *new* target schema is a curated superset (~360 base + derived) — see "Column Scoping" below.
- **Type coercions (happen today during upsert, must be pushed up into the unload):**
  - `INT`: `Residence_Addresses_CheckDigit`, `Residence_Addresses_PrefixDirection`, `Residence_Addresses_SuffixDirection`, `Mailing_Addresses_CheckDigit`, `Mailing_Addresses_PrefixDirection`, `Mailing_Addresses_SuffixDirection`. Source is string; Spark must `try_cast("int")` before writing.
  - `DATE`: `Voters_CalculatedRegDate`, `Voters_MovedFrom_Date`.
  - `REMOVED_COLUMNS` (15 of them — GeoHash, HHCount, DateConfidence_Description, etc.): the Databricks source no longer has these, but the PG table still does. Write them out as `\N` (null) in the exported file so column order stays stable.
- **Extensions:** must still capture via `SELECT extname FROM pg_extension` on prod before step 3. Defaults on Aurora 16 include `plpgsql` and typically `pg_stat_statements`. Whatever is there, mirror on the new cluster before schema apply. Add `aws_s3` and `aws_commons` (required for `table_import_from_s3`) regardless.
- **Indexes / constraints:** inspect via `\d+ public."VoterTX"` on prod and dump with `pg_dump --schema-only --table='public.\"Voter*\"' --no-owner --no-acl`. The app-side (`voterFile.util.ts`) reveals the columns that must be indexed for serving performance — see step 5.

### Data Volume (from `int__l2_nationwide_uniform`, 2026-04-16)

Total: **218,278,803 rows** across 51 tables. Top 10 states (those that will dominate straggler time on COPY + CREATE INDEX):

| State | Rows     | Approx raw .tab size |
|-------|----------|---------------------:|
| CA    | 23.2 M   | ~63 GB               |
| TX    | 18.0 M   | ~30 GB               |
| FL    | 15.0 M   | ~36 GB               |
| NY    | 12.6 M   | ~25 GB               |
| PA    |  8.6 M   | —                    |
| IL    |  8.4 M   | —                    |
| MI    |  7.9 M   | —                    |
| GA    |  7.7 M   | —                    |
| OH    |  7.6 M   | —                    |
| NC    |  7.2 M   | —                    |

Smallest states (WY, ND, VT, DC, AK) each under 600 K rows. Size estimates are from the raw L2 `VM2Uniform--XX--*.tab` files in `s3://normalized-voter-files/VM2Uniform/`. The exported-for-load files will be similar order of magnitude (same text format, same columns).

**Recommended state-ordering for scheduling:** big states start first (CA, TX, FL, NY, …). The overall wall-clock is gated by whichever state finishes last.

### Column Scoping: reference-driven, not "dump the whole source"

The legacy PG `"Voter{STATE}"` tables have **348 columns**. The Databricks source `int__l2_nationwide_uniform` exposes **807 columns**. A naive "load everything L2 delivers" would mean ~470 additional columns — most of them consumer-enrichment data (lifestyle, purchasing behaviors, MAIDs, etc.) that no current *or planned* consumer of the voter DB references.

**Decision:** scope the new schema to the **union of columns referenced by the gp-api today and by the people-api's schema/filter builder** (which is the forward-looking API per the ENG-5032 TODO in `voterFile.util.ts`). That's a much smaller, deliberate set — roughly the current 348 plus a targeted ~20–30 additions, not 470.

**Sources of truth for "referenced":**
1. **gp-api `voterFile.util.ts` (static refs)** — every string that appears inside `"..."` in a SELECT, WHERE, or subquery.
2. **gp-api `voterFile.util.ts` (dynamic refs)** — `whereClause += \`("${l2ColumnName}" = ...)\`` treats **any** value of `campaign.pathToVictory.l2Type` as a column name. This means every L2-native district-type column that could appear in `org_districts.l2Type` is implicitly in scope. See "District columns" below for how to enumerate.
3. **people-api `Voter.prisma`** — every non-derived field maps to an L2-native column (explicit rename; the comment next to each field gives the L2 name).
4. **people-api `filters.sql.utils.ts`** — every `buildXxxFilter(fieldName, ...)` call names an L2-native column that the API filters on.
5. **people-api `people.select.ts`** — `VOTER_SELECT_COLUMNS` is the default projection for list/download endpoints.

**District columns — add every L2-native district column not already in prod.**

The legacy PG schema has ~250 district columns (`State_House_District`, `County`, `City`, `Park_District`, …) carried over from an older L2 snapshot. L2 has since added more, and because `voterFile.util.ts` resolves the column name from runtime campaign metadata, *any* district column that a campaign's organization has configured in `org_districts.l2Type` will be queried directly. If it's not in the PG table, the query errors out with "Error counting Records" — this is the class of bug filed against `Judicial_Justice_of_the_Peace` for a Bell County TX campaign.

The concrete net-new district columns to add to the new cluster (derived from the Databricks source minus the legacy 348 minus demographic/address/phone/election columns):

| L2-native column                                   | Notes                                                         |
|----------------------------------------------------|---------------------------------------------------------------|
| `Judicial_Chancery_Court`                          | New (e.g. TN, MS use these)                                   |
| `Judicial_Justice_of_the_Peace`                    | **New — this is the Bell County TX bug**                      |
| `Judicial_Municipal_Court_District`                | New — supersedes legacy `Municipal_Court_District`            |
| `Community_College`                                | New (distinct from legacy `Community_College_At_Large` etc.)  |
| `Hospital_District`                                | New (distinct from legacy `Community_Hospital_District` / `County_Hospital_District`) |
| `State_Board_of_Equalization`                      | New (CA, AL use it)                                           |
| `4H_Livestock_District`                            | New (rural states)                                            |
| `2024_Proposed_Congressional_District`             | New — redistricting-in-progress overlays                      |
| `2024_Proposed_State_Senate_District`              | New                                                           |
| `2024_Proposed_State_House_District`               | New                                                           |
| `2024_Proposed_State_Legislative_District`         | New                                                           |
| `2001_US_Congressional_District` (+ 3 sibling 2001 cols) | New — historical redistricting, L2 re-added for long-horizon candidate research |
| `2010_US_Congressional_District` (+ 3 sibling 2010 cols) | New — historical redistricting                                |

The three legacy typo/rename fixes (`Water_Control__Water_Conservation`, `Water_Control__Water_Conservation_SubDistrict`, `Landscaping_And_Lighting_Assessment_Distric`) are already covered in the rename table below.

**How to enumerate at build time (don't eyeball the list):** the loader's `emit_ddl` step takes a "district column" predicate (regex on column name, or explicit allowlist) over the Databricks source column set, unions that with the prod-dump district columns, and emits all of them. Recommended predicate: any L2-native column that is neither (a) in a maintained exclusion list of non-district columns nor (b) matched by the demographic/address/phone/election patterns. Err on the side of *including* — wasted disk on a string column with 80% nulls is cheaper than another refresh.

**Cross-check for in-use dynamic references (validation, not scoping):** before cutover, run a prod query against `gp-api`'s Postgres: `SELECT DISTINCT "l2Type" FROM org_districts WHERE "l2Type" IS NOT NULL;` and diff against the new cluster's `"VoterTX"` `information_schema.columns`. Any `l2Type` value that isn't a column in the new schema is a latent "Error counting Records" bug. Ship nothing with holes in that diff.

**Demographic/election additions vs. legacy (derived from the people-api Voter.prisma — column name comments map back to L2-native source):**

| L2-native source column (new target)              | Why it's in scope                                                   |
|---------------------------------------------------|---------------------------------------------------------------------|
| `Voters_BirthDate`                                | people-api comment: *"format 99/99/9999; calculated reg date…"* via BirthDateConfidence |
| `BirthDateConfidence_Description`                 | people-api uses as dateConfidence                                  |
| `ConsumerData_Business_Owner`                     | people-api filter: `businessOwner` → `"Business_Owner"`             |
| `ConsumerData_Education_of_Person`                | people-api filter: `educationLevel` → `"Education_Of_Person"`       |
| `ConsumerData_Estimated_Income_Amount`            | people-api filter: `estimatedIncomeAmountInt` (raw + `_Int`)        |
| `ConsumerData_Homeowner_Probability_Model`        | people-api filter: `homeowner` → `"Homeowner_Probability_Model"`    |
| `ConsumerData_Language_Code`                      | people-api filter: `language` → `"Language_Code"`                   |
| `ConsumerData_Marital_Status`                     | people-api filter: `maritalStatus` → `"Marital_Status"`             |
| `ConsumerData_Presence_Of_Children_in_HH`         | people-api filter: `presenceOfChildren` → `"Presence_Of_Children"`  |
| `ConsumerDataLL_Veteran`                          | people-api filter: `veteranStatus` → `"Veteran_Status"`             |
| `Mailing_HHGender_Description`                    | people-api Voter.prisma (currently a `REMOVED_COLUMNS` NULL placeholder — restore) |
| `Residence_Families_FamilyID`                     | people-api Voter.prisma                                             |
| `Phone_Number_Available` / `Cell_Phone_Number_Available` / `Landline_Phone_Number_Available` | people-api's `hasCellPhone`/`hasLandline` filters currently test the `_Formatted` cols, but these availability booleans are lighter and indexable |

**Derived/computed columns (not from L2, do NOT attempt to load from source):**
- `Age_Int` — `CAST("Voters_Age" AS INTEGER)` if numeric, else NULL. Produce during unload/load, not from a Databricks source column.
- `Estimated_Income_Amount_Int` — `CAST("ConsumerData_Estimated_Income_Amount" AS INTEGER)` with NULL on parse failure.
- `Voter_Status` / `Voter_Status_UpdatedAt` — computed in-product (people-api owns the derivation). Not a loader concern.

**Implication for the target schema:** the loader source of truth is a curated list in `loader/schema/voter_columns.py` — not the full 807 Databricks columns. Start with the legacy 348 as the floor, add the ~15 net-new district columns (above) plus the ~13 demographic additions (below), plus the derived `_Int` casts done during unload, and flag the known legacy-schema renames. Total: ~375 base columns + derived. Any ad-hoc column request from a future PR should update `voter_columns.py` first so the schema stays documented.

**Legacy-schema renames the new cluster should fix (watch in DDL diffs):**

| Legacy PG name                                   | L2-native (new target)                           |
|--------------------------------------------------|--------------------------------------------------|
| `Water_Control__Water_Conservation`              | `Water_Control_Water_Conservation`               |
| `Water_Control__Water_Conservation_SubDistrict`  | `Water_Control_Water_Conservation_SubDistrict`   |
| `Landscaping_And_Lighting_Assessment_Distric`    | `Landscaping_and_Lighting_Assessment_District`   |

**Out of scope (explicitly exclude):** the ~300 `ConsumerData_*` interest/lifestyle columns (Cooking, Arts, Sports, Gaming, MAID IDs, etc.), the `FECDonors_*` block, and the full `PRI_BLT_*` block. None of these are referenced by current gp-api or people-api code, nor are they referenced by `org_districts.l2Type` (since none are districts). Adding them later is cheap — they can be backfilled in a narrow partial refresh without a full cluster rebuild — so we don't pay the size/index cost up front. (Note: 2001/2010/2024_Proposed districting columns *are* in scope — they pattern-match `l2Type` values.)

**Loader-internal columns to EXCLUDE from the target schema** (Databricks source artifacts, not voter data): `SEQUENCE`, `loaded_at`, `state_postal_code` (keep as partition key only), `state_from_lalvoterid`.

**Process impact per step:**
- **Step 1 (unload):** project onto the curated `VOTER_TARGET_COLUMNS` list. Derive `state_postal_code` as partition key only.
- **Step 3 (create tables):** start from `pg_dump` of prod, then apply: (a) drop the three typo'd legacy columns, (b) add the ~13 new columns with Databricks-derived PG types, (c) carry PKs and all prod indexes forward. Emit the final DDL deterministically from `voter_columns.py` so reruns produce the same schema.
- **App side:** additions are truly additive for the gp-api — `voterFile.util.ts` uses L2-native column names, so nothing breaks. The people-api runs against its own cluster and is unaffected. When ENG-5032 finally delegates gp-api to people-api, the voter-DB columns will already exist.
- **Data types:** read from `databricks tables get goodparty_data_catalog.dbt.int__l2_nationwide_uniform`. Use int/date/boolean/double in the DDL where the Databricks source uses them (and only where L2 is stable — e.g. `Voters_Age` is stored as STRING in the source, so keep it `TEXT` in PG and rely on the derived `Age_Int` sidecar for numeric queries).

### S3
- **Existing bucket with raw L2 uniform files:** `s3://normalized-voter-files/` (us-west-2). Do not repurpose — it holds inputs to the Databricks side, not loader output.
- **Recommended new prefix:** create a bucket e.g. `s3://gp-voter-loader-us-west-2/voter_export_{date}/` (must be us-west-2, same account). Manifests live at `voter_export_{date}/_manifest/`.

### IAM
- **Existing relevant roles:** `databricks-access-s3`, `databricks-default-storage`, `rds-monitoring-role`. **No** `rds-s3-import` style role exists yet — create one in step 2 with `s3:GetObject`/`s3:ListBucket` on the loader bucket and a trust policy for `rds.amazonaws.com`, then attach to the new cluster (`aws rds add-role-to-db-cluster --feature-name s3Import`).
- **Account ID:** `<aws-account-id>`.

---

## Cross-Cutting Architectural Decisions

These apply across all steps. Lock them in before writing code.

### 1. Server-side COPY from S3 via the `aws_s3` extension
**Decision:** use Aurora PostgreSQL's `aws_s3.table_import_from_s3` for the load phase, not client-side COPY from a bastion host.

**Rationale:** runs server-side directly from S3 to Aurora storage. Bandwidth is bounded by the RDS instance's network (~25–50 Gbps on large r6g/r7g), not by a client's pipe. Eliminates an entire hop.

**Requires:** IAM role attached to the cluster with `s3:GetObject` on the export bucket, plus a VPC endpoint for S3 in the cluster's VPC.

**Fallback:** if `aws_s3` misbehaves on real data, fall back to `psql \copy` from a large EC2 instance streaming S3. Simpler error handling, slower.

### 2. Region-pin everything
**Decision:** S3 export bucket, Databricks workspace, and RDS cluster all in the same region (**`us-west-2`**, matching existing prod).

**Rationale:** cross-region transfer destroys both throughput and budget. Single-region keeps the fast path fast.

### 3. Export format: PostgreSQL text (tab-delimited, `\N` nulls)
**Decision:** export to text format (`\t` delimiter, `\N` null marker) rather than CSV or Parquet.

**Rationale:**
- Text format maps 1:1 to PG's native COPY wire format — minimal escaping overhead.
- CSV adds quoting complexity and is slower to parse for no benefit here.
- Parquet is faster to produce from Spark, but `aws_s3.table_import_from_s3` only accepts text/CSV. Would force a conversion step.

**Use CSV only if** data contains raw tabs/newlines that force heavy escaping anyway.

### 4. Chunk by state AND by size
**Decision:** one S3 "folder" per state, multiple ~2 GB files per folder. Target 30+ files for CA (largest, ~63 GB raw) down to 1–2 files for the smallest states (WY, DC, VT, AK — all under ~2 GB).

**Rationale:**
- Largest states: CA 23.2M voters (~63 GB raw), TX 18M (~30 GB), FL 15M (~36 GB), NY 12.6M (~25 GB). One massive file makes them straggler tasks.
- Multiple files per state enable parallel COPY *within* a state (each file → one concurrent COPY).
- PG's COPY is single-threaded per statement, so file-level parallelism is the only lever.

**Concrete file-count plan** (rounding up on ~2 GB targets from raw-file sizing):
- CA: ~32 files | TX: ~15 | FL: ~18 | NY: ~13 | PA/IL/MI/GA/OH/NC: ~4–10 each | remaining: 1–4 each. Total ≈ 200 files across 51 states.

### 5. Manifest-driven idempotency
**Decision:** every step writes a small JSON manifest to S3 (e.g. `s3://bucket/voter_export_YYYYMMDD/_manifest/unload.json`) recording what it produced. Subsequent steps read the manifest. Re-running a step validates the manifest and no-ops if already complete.

**Rationale:** more robust than "check if the file exists." Captures expected row counts, column order, file → table mapping. Lets validation compare actuals against documented intent.

**Canonical schemas** (pydantic, `loader/manifest/models.py`). All manifests share a common envelope with `run_date`, `step`, `status`, `started_at`, `finished_at`, `schema_version`. Bodies vary:

```python
from datetime import datetime
from typing import Literal
from pydantic import BaseModel

class ManifestBase(BaseModel):
    schema_version: Literal[1] = 1
    run_date: str                    # "20260417"
    step: str                        # "unload" | "provision" | ...
    status: Literal["in_progress", "complete", "failed"]
    started_at: datetime
    finished_at: datetime | None = None

class InspectManifest(ManifestBase):
    prod_cluster_id: str
    engine_version: str
    extensions: list[str]            # from pg_extension
    roles: list[str]                 # from pg_roles (non-superuser)
    prod_row_counts: dict[str, int]  # {"VoterTX": 18_036_204, ...}
    prod_ddl_s3_uri: str             # s3://.../prod_dump.sql

class UnloadFile(BaseModel):
    state: str                       # "TX"
    s3_key: str                      # voter_export_20260417/state_id=TX/part-00007.txt
    size_bytes: int
    row_count: int

class UnloadManifest(ManifestBase):
    databricks_table: str            # "goodparty_data_catalog.dbt.int__l2_nationwide_uniform"
    columns: list[str]               # ordered list, matches file column order
    column_types_pg: dict[str, str]  # {"LALVOTERID": "TEXT", "Age_Int": "INTEGER", ...}
    per_state_row_counts: dict[str, int]
    files: list[UnloadFile]

class ProvisionManifest(ManifestBase):
    cluster_id: str                  # "gp-people-db-20260417"
    writer_instance_id: str
    writer_endpoint: str
    iam_role_arn: str                # the rds-s3-import role attached
    vpc_endpoint_id: str             # S3 gateway endpoint
    load_parameter_group: str
    serve_parameter_group: str

class CopyTableResult(BaseModel):
    table: str                       # "VoterTX"
    expected_rows: int               # from UnloadManifest
    actual_rows: int                 # after COPY
    files_loaded: int
    seconds_elapsed: float

class CopyManifest(ManifestBase):
    results: list[CopyTableResult]

class IndexSpec(BaseModel):
    table: str
    index_name: str
    columns: list[str]
    unique: bool
    where: str | None = None         # partial index predicate

class IndexManifest(ManifestBase):
    indexes: list[IndexSpec]
    constraints_added: list[str]     # PK/FK names
    analyzed_tables: list[str]

class ValidationCheck(BaseModel):
    name: str                        # "row_counts_match_databricks"
    passed: bool
    details: dict                    # freeform per-check

class ValidateManifest(ManifestBase):
    checks: list[ValidationCheck]
    all_passed: bool
```

**Paths:** `s3://gp-voter-loader-us-west-2/voter_export_{date}/_manifest/{step}.json`. Each step's first action is `manifest = read_manifest(step)` → if `status=="complete"`, log and exit 0. Each step's last action (on success) is `write_manifest(step, status="complete", ...)`.

### 6. Date-stamped artifact versioning
**Decision:** a single date stamp (e.g. `20260416`) flows through everything: S3 prefix, RDS cluster identifier, parameter group names, manifest paths.

**Rationale:** rollback and retry become trivial — just bump the date. No mutable state anywhere. Matches the existing convention (`gp-people-db-prod`).

---

## Step 0 — Inspect Existing Environment

> **Implemented scope (DATA-1907):** the `inspect-prod` CLI step is narrower than this
> section's original plan. It captures per-state row counts + L2 snapshot dates
> (`max(updated_at)`) for `Voter` (+ best-effort `DistrictVoter`/`District`/`DistrictStats`)
> into `InspectManifest`, which `validate` diffs the new cluster against. It does NOT
> produce the schema dump / extension / role inventory — the committed
> `schema/data/prod_dump.sql` is the schema source of truth, and the AWS-side inventory
> is the "Inspected Prod Environment" block above.

**Goal:** capture the config of `gp-people-db-prod` so the new cluster mimics what the app already expects, without surprises at cutover.

**Status:** AWS-side inspection is already done and lives in the "Inspected Prod Environment" section above. The inside-the-database inspection still needs to run on first loader invocation (requires credentials this plan doesn't want to hardcode).

**Two inspections needed:**

### AWS-side (via boto3 / `aws rds describe-*`) — DONE, see above
Key values to reuse (full table in the top-of-doc "Inspected Prod Environment" block):
- VPC `<vpc-id>`, subnet group `<db-subnet-group>`, SG `<security-group-id>`.
- KMS key `arn:aws:kms:us-west-2:<aws-account-id>:key/<kms-key-id>`.
- Engine `aurora-postgresql` 16.8. Cluster param group `default.aurora-postgresql16` (no custom tuning today — on the new cluster we'll create a load-tuned one and revert at cutover).
- Serverless v2 MinCapacity=0.5, MaxCapacity=128.
- Backup retention 14 days. Deletion protection on. Tags `Project=gp-api`, `Environment=prod`.
- **No S3 VPC endpoint in the VPC yet** — must be added as part of step 2.
- **No `rds-s3-import` IAM role yet** — must be created as part of step 2.

### Inside-the-database (connect and query) — TODO on first loader run
Run these against the prod cluster (connection from `~/.pg_service.conf [people]`) and write results into `target_environment.json`:
- `SELECT extname, extversion FROM pg_extension` — minimum expected: `plpgsql`, `pg_stat_statements`. The new cluster also needs `aws_s3` + `aws_commons` for `table_import_from_s3`.
- `SELECT rolname FROM pg_roles` + grants on `public."Voter*"` tables — the app connects as a specific role; reuse credentials on the new cluster.
- Per-table DDL: `pg_dump --schema-only --no-owner --no-acl --dbname=... -t 'public."Voter*"'`. Split into (a) CREATE TABLE + PK, (b) non-unique indexes, (c) FKs — steps 3 and 5 consume each subset.
- Per-state row counts: `SELECT 'VoterXX' AS t, count(*) FROM public."VoterXX"` union-all across all 51 tables. These will be *stale* vs Databricks (expected) — record anyway for validation's "old vs new" diff (step 7).

**Output:** a single JSON `target_environment.json` checked into the loader repo. Step 2 reads from it.

**Rationale:** doing this up front turns "what does prod look like" from tribal knowledge into a versioned artifact. One surprise discovered at cutover can cost a day.

**Gotcha:** the app's connection string will change (new cluster → new endpoint, e.g. `gp-people-db-{date}.cluster-<suffix>.us-west-2.rds.amazonaws.com`). Flag in the manifest so the cutover task isn't forgotten.

**Done when:**
- `target_environment.json` exists in the loader repo with: engine version, `pg_extension` list, `pg_roles` list + grants on `public."Voter*"`, `prod_row_counts` dict, writer endpoint, SG/VPC/subnet/KMS IDs.
- `schema/prod_dump.sql` committed — `pg_dump --schema-only` output for all 51 `Voter*` tables + `VoterFile`.
- `InspectManifest` written to `s3://.../voter_export_{date}/_manifest/inspect.json` with `status=complete`.
- Re-running `loader inspect-prod` is a no-op (reads manifest, exits 0).

---

## Step 1 — Unload Databricks → S3

**Architecture:** a PySpark job (either a standalone script submitted via `databricks` CLI / the Databricks Jobs API, or a new dbt Python model placed alongside `write__l2_databricks_to_gp_api.py`) that reads `int__l2_nationwide_uniform`, projects onto the **curated target column list** (see "Column Scoping" above — ~360 columns, not the full 807 source and not just the legacy 348), partitions by `state_postal_code` (which comes from the Databricks source), repartitions by size, and writes text-format files to S3. The unload step is also where derived columns (`Age_Int`, `Estimated_Income_Amount_Int`) are computed via `try_cast`, since they aren't physically present in the source.

```python
# VOTER_TARGET_COLUMNS is the new schema column list — checked into the loader
# repo (e.g. loader/schema/voter_columns.py), with (name, pg_type, spark_cast)
# per column. Derived from `databricks tables get ...int__l2_nationwide_uniform`
# filtered to exclude loader-internal columns (SEQUENCE, loaded_at,
# state_postal_code, state_from_lalvoterid).
from loader.schema import VOTER_TARGET_COLUMNS

df = spark.table("goodparty_data_catalog.dbt.int__l2_nationwide_uniform")

projected = []
for name, _pg_type, spark_cast in VOTER_TARGET_COLUMNS:
    if spark_cast:
        projected.append(col(name).cast(spark_cast).alias(name))
    else:
        projected.append(col(name).alias(name))
df = df.select(col("state_postal_code"), *projected)

(df.repartition("state_postal_code")
   .write
   .partitionBy("state_postal_code")
   .option("sep", "\t")
   .option("nullValue", "\\N")
   .option("emptyValue", "")
   .option("quote", "\u0000")   # effectively disable quoting
   .option("escape", "\\")
   .option("header", "false")   # aws_s3 COPY uses FORMAT text; no header
   .mode("overwrite")
   .csv(f"s3://gp-voter-loader-us-west-2/voter_export_{date}/"))
```

After writing, the orchestrator runs `df.groupBy("state_postal_code").count()` and writes counts into `_manifest/unload.json` alongside the ordered column list. Step 4 validates PG row count equals this manifest per-state number.

Use `coalesce`/`repartition` to hit the ~2 GB file target per state (see "Chunk by state AND by size" above for concrete file counts).

**Rationale:** Spark-native write is the only thing that scales to the full dataset. State partitioning gives a clean `state=TX/part-*.txt` layout that maps 1:1 to target tables.

**Pitfalls:**
- **Column order must exactly match the target PG table.** Pin column ordering from the new-schema `VOTER_TARGET_COLUMNS` list checked into the loader repo — don't trust `SELECT *`. Write the ordered column list into the manifest so step 4 can verify.
- **Boolean columns (the election columns: `General_2026`, `Primary_2024`, etc.):** Inspect shows they're native Spark booleans. PG COPY text format accepts `t`/`f` or `true`/`false` — Spark's default `boolean.toString()` emits `true`/`false`, which is fine, but confirm with a sample of FL before full unload.
- **Quoting traps:** strings containing `\t`, `\n`, `\r`, or `\\` must be escaped. Spark's CSV writer with `escape="\\"` handles this — verify on a sample before running full unload.
- **Small-file explosion:** Spark's default partitioning may produce thousands of tiny files. `coalesce`/`repartition` to target file size deliberately.
- **Removed-column nulls:** the 15 `REMOVED_COLUMNS` (e.g. `Residence_Addresses_GeoHash`, `MaritalStatus_Description`, `Religions_Description`) have no source data but must appear in the file as `\N` so column positions line up with the target PG table. Do NOT skip them.

**Alternative considered:** Databricks `COPY INTO` producing Parquet. Rejected — `aws_s3.table_import_from_s3` doesn't accept Parquet, would require an extra conversion step.

**Reference:** `write__l2_databricks_to_gp_api.py` defines only the *legacy* (stale) column list — don't use it as-is. Authoritative schema for the new loader comes from `databricks tables get goodparty_data_catalog.dbt.int__l2_nationwide_uniform` minus loader-internal columns. The dbt source ref is `int__l2_nationwide_uniform` (Databricks catalog `goodparty_data_catalog`, schema `dbt`). No per-state Databricks tables are used directly — the nationwide table is already the union.

**Done when:**
- All files present under `s3://gp-voter-loader-us-west-2/voter_export_{date}/state_id=XX/part-*.txt` for all 51 states.
- `UnloadManifest` at `_manifest/unload.json` includes: `columns` (ordered, matches file column order), `column_types_pg`, `per_state_row_counts` for all 51 states, `files[]` with row counts per file, `status=complete`.
- Sum of `files[].row_count` per state equals `per_state_row_counts[state]`.
- A sample file decoded with PG COPY format rules (`\t` delim, `\N` null) against the target DDL round-trips (test on FL).
- Re-running `loader unload --date X` is a no-op.

---

## Step 2 — Provision New Aurora Cluster

**Architecture:** Terraform or boto3 script that creates:
- Aurora PostgreSQL cluster: engine `aurora-postgresql`, version `16.8` (matches prod).
- Cluster identifier: `gp-people-db-{YYYYMMDD}`.
- Database name + master user match prod (from the loader-config secret) so app secrets carry over.
- Single writer instance, **provisioned `db.r7g.16xlarge`** for the load/index phase.
- Custom cluster + instance parameter groups tuned for bulk load (`gp-people-db-{date}-load`), values in step 4/5. Serving group (`gp-people-db-{date}-serve`) kept separate.
- IAM role (new — none exists today) with `s3:GetObject` + `s3:ListBucket` on the loader bucket (enables `aws_s3`); attach via `aws rds add-role-to-db-cluster --feature-name s3Import`.
- **VPC endpoint for S3** in `<vpc-id>` — **does not exist today**, create it (Gateway endpoint type, attach to both route tables for <subnet-id> and <subnet-id>).
- VPC: `<vpc-id>`. Subnet group: `<db-subnet-group>` (reuse). Security group: start by reusing `<security-group-id>` so the app's existing SG allowlist carries over at cutover.
- Backup retention = 1 day during load (bump to 14 at cutover to match prod).
- `storage_encrypted = true`, KMS key `arn:aws:kms:us-west-2:<aws-account-id>:key/<kms-key-id>` (reuse prod's key).
- Deletion protection off during POC; on before cutover.
- IO-optimized storage (`aurora-iopt1`) for the one-shot bulk-load + heavy-index-build workload.
- Tags `Project=gp-api`, `Environment=prod` to match prod's tagging convention.

**Rationale:** provisioned (not serverless) for load/index even though prod is serverless. See *Provisioned-vs-Serverless* section below for full reasoning. r7g is Graviton — cheaper and excellent per-core throughput for PG. 16xlarge = 64 vCPU / 512 GB RAM, which is the room needed for parallel COPY and parallel CREATE INDEX.

**Pitfalls:**
- **Static parameters require reboots.** Set all of them at creation so mid-load reboots aren't needed.
- **"Available" ≠ "ready."** Poll with `SELECT 1` after the API says available.
- **IAM role must be attached to the cluster**, not just created. Double-check in console after Terraform apply.

**Idempotency:** script checks for existing cluster with the date-stamped identifier; skips creation if present.

**Done when:**
- `aws rds describe-db-clusters --db-cluster-identifier gp-people-db-{date}` returns `Status=available` and `EngineVersion=16.8`.
- Writer instance `DBInstanceClass=db.r7g.16xlarge`, `DBInstanceStatus=available`.
- Cluster has the `rds-s3-import` IAM role attached with `FeatureName=s3Import` (check `AssociatedRoles` on the cluster, not the instance).
- S3 gateway VPC endpoint exists in `<vpc-id>` and is attached to both private route tables.
- `CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE` succeeds; `SELECT aws_s3.table_import_from_s3('pg_catalog.pg_class', '', '(FORMAT text)', aws_commons.create_s3_uri('bucket','missing-key','us-west-2'))` returns an "object not found" error (not a permissions error) — proves the role is working.
- Load parameter group `gp-people-db-{date}-load` attached to the cluster; serving group `gp-people-db-{date}-serve` exists but NOT attached yet.
- `ProvisionManifest` at `_manifest/provision.json` with cluster/instance IDs, writer endpoint, IAM role ARN, VPCE ID, param group names. `status=complete`.

---

## Step 3 — Create Target Tables

**Architecture:** two-part. (a) Extract the current prod DDL as the *floor* — preserves every column the app currently relies on. (b) Apply the curated additions from `loader/schema/voter_columns.py` (see "Column Scoping" above) to produce the final DDL. Check both the extracted prod DDL and the emitted final DDL into git.

```bash
# (a) Starting point: current prod schema.
pg_dump --schema-only --no-owner --no-acl \
        --schema=public \
        -t 'public."Voter"' \
        "$(aws ssm get-parameter --name people-db-connection-string-prod \
            --with-decryption --query Parameter.Value --output text)" \
        > schema/prod_dump.sql

# (b) Emit the merged DDL. Loader code reads prod_dump.sql, merges with the
# curated VOTER_TARGET_COLUMNS list (gp-api + people-api referenced), and
# writes CREATE TABLE statements with:
#   - every prod column NOT in the drop list (preserves app-facing contract)
#   - the ~13 curated additions (Voters_BirthDate, ConsumerData_* referenced by
#     people-api filters, Mailing_HHGender_Description, etc.) with
#     Databricks-derived types where safe (int/date/bool/double), text otherwise
#   - the three rename fixups (Water_Control__*, Landscaping_*_Distric) so the
#     new schema has the corrected name and the legacy typo is retired
#   - do NOT carry forward the ~300 ConsumerData_* interest columns or the
#     FECDonors_* block; those are explicitly out of scope
python -m loader.schema.emit_ddl \
    --prod-dump schema/prod_dump.sql \
    --target-columns loader/schema/voter_columns.py \
    --databricks-columns schema/databricks_columns.json \
    --out schema/target_schema.sql
```

Note the quoted mixed-case table names (`"VoterAL"` through `"VoterWY"` plus `"VoterDC"`). Expect 51 `Voter*` tables plus the `VoterFile` tracking table.

**Rename handling (see "Column Scoping"):** when building the merged DDL, collapse the three known rename pairs down to a single column using the Databricks-side (corrected) name. This means the new cluster will *not* have e.g. `"Landscaping_And_Lighting_Assessment_Distric"` (the typo'd legacy name). The app-facing impact is small — `voterFile.util.ts` only hits these via dynamic l2Type lookups, and the campaign metadata should be updated to match — but call this out in the cutover checklist.

**Rationale:** mimicking prod schema exactly is the only defensible approach — the app depends on it. Extracting from a live instance is more reliable than hand-writing DDL.

**Pitfalls:**
- **Don't create indexes yet** — indexes during COPY cause 10–50× slowdown.
- **Don't create FKs yet** — same reason.
- **Don't create PKs yet either.** A PK is just a unique index + NOT NULL. Load first, add PK in the index phase.
- **`fillfactor=100`** — the default. Correct here; these tables are read-only between refreshes so no HOT-update slack is needed.
- **`UNLOGGED` during load?** On vanilla PG this skips WAL and is a big win. On Aurora, UNLOGGED has different semantics (still persisted to shared storage), so WAL savings are smaller. Worth benchmarking on one state before committing.

**Idempotency:** `CREATE TABLE IF NOT EXISTS`. For a full reset, drop and recreate — cheap when empty.

**Done when:**
- 51 tables `public."Voter{STATE}"` + `public."VoterFile"` exist in the new cluster.
- Each `Voter{STATE}` column set equals `UnloadManifest.columns` exactly (same names, same order — verify via `information_schema.columns` diff).
- NO indexes, NO primary keys, NO foreign keys on any `Voter{STATE}` table (`SELECT count(*) FROM pg_indexes WHERE tablename LIKE 'Voter%' AND indexname NOT LIKE '%_pkey'` returns 0; PK step is Step 5).
- The three legacy typo columns are absent (`"Water_Control__Water_Conservation"`, `"Water_Control__Water_Conservation_SubDistrict"`, `"Landscaping_And_Lighting_Assessment_Distric"` must not exist).
- `schema/target_schema.sql` committed with the emitted DDL.

---

## Step 4 — COPY S3 → Aurora

**Architecture:** Python orchestrator (thread pool, one thread per in-flight COPY) that issues one `aws_s3.table_import_from_s3` call per file, many calls in parallel.

```sql
SELECT aws_s3.table_import_from_s3(
    'public."VoterTX"',            -- note: schema-qualified, quoted, mixed-case
    '',                            -- columns (empty = all, in table order)
    '(FORMAT text, DELIMITER E''\t'', NULL ''\N'', ENCODING ''UTF8'')',
    aws_commons.create_s3_uri(
        'gp-voter-loader-us-west-2',
        'voter_export_20260416/state_id=TX/part-00007.txt',
        'us-west-2'
    )
);
```

**Target concurrency:** ~2× vCPU count → ~128 parallel COPYs on a 64-vCPU instance. Tune from there; too much saturates WAL flushing, too little leaves throughput unused.

**Parameter group tuned for load (apply before starting):**
- `synchronous_commit = off` — huge win, safe to reset post-load.
- `maintenance_work_mem = 4GB`
- `work_mem = 256MB`
- `max_wal_size = 64GB` — reduce checkpoint frequency.
- `checkpoint_timeout = 60min`
- `checkpoint_completion_target = 0.9`
- `autovacuum = off` — re-enable after load + ANALYZE.
- `wal_compression = on`

**Rationale:** server-side COPY is the only way to hit instance-network speeds. File-level parallelism is where the real wins come from since PG's COPY is single-threaded per statement.

**Pitfalls:**
- **`aws_s3` handles row-level errors poorly.** Validate one file per state first with a strict row-count check before firing the rest. Recommended first run: FL (~15 M rows, mid-sized, representative distribution).
- **Partial failure = dirty table.** Mitigations:
  - *Safe option:* load into per-file staging tables (e.g. `loader_staging."VoterTX_part_00007"`) and then `INSERT INTO public."VoterTX" SELECT * FROM loader_staging."VoterTX_part_*"`. Slower but idempotent/debuggable. (Matches the `staging_schema` pattern used by legacy `write__l2_databricks_to_gp_api.py`.)
  - *Fast option:* on failure, `TRUNCATE public."VoterTX"` and reload. Track per-state completion in the manifest.
- **Connection limits.** A 16xlarge's default `max_connections` is generous but check it. 128 parallel COPYs + orchestrator overhead can bite.
- **`aws_s3` uses the cluster's IAM role, not the session.** Verify role trust + bucket policy before starting, not during.

**Idempotency:** compare target table row count against expected (from Databricks, persisted in manifest). Equal → skip. Zero → load. Partial → TRUNCATE + reload.

**Reference for expected row counts/columns:** column list is the new-schema `VOTER_TARGET_COLUMNS` (loader repo) — NOT the stale 348-column `VOTER_COLUMN_LIST` from `write__l2_databricks_to_gp_api.py`. Per-state row counts already captured in the top-of-doc "Data Volume" section; re-run `dbt show --inline "select state_postal_code, count(*) from {{ ref('int__l2_nationwide_uniform') }} group by 1"` at the start of each refresh to repopulate the manifest with current numbers.

**Done when:**
- For every state, `SELECT count(*) FROM public."Voter{STATE}"` equals `UnloadManifest.per_state_row_counts[state]` exactly.
- No rows exist in `loader_staging` schema (if the safe-option staging pattern is used, staging tables must be dropped after INSERT).
- No indexes exist on `Voter{STATE}` tables (still — indexes come in step 5).
- `CopyManifest` at `_manifest/copy.json` has one `CopyTableResult` per state with `actual_rows == expected_rows`, `status=complete`.
- Re-running `loader copy --date X` (or `--state TX`) is a no-op per state that's already complete.

---

## Step 5 — Build Indexes

**Architecture:** orchestrator reads the index manifest (extracted from prod in step 0's DDL extraction) and issues `CREATE INDEX` statements with controlled parallelism.

```sql
SET maintenance_work_mem = '16GB';
SET max_parallel_maintenance_workers = 8;
CREATE INDEX "VoterTX_Voters_LastName_idx"
    ON public."VoterTX" ("Voters_LastName");
```

**Parallelism strategy:** two levels.
- `max_parallel_maintenance_workers` parallelizes a *single* index build across workers.
- Orchestrator runs multiple `CREATE INDEX` statements concurrently across different tables.
- Sweet spot: ~4–8 workers per index × ~8–16 concurrent builds. Saturates a 64-vCPU box.

**Order:**
1. Primary keys first.
2. Non-unique indexes.
3. FK constraints.

Within non-unique, build largest states first so straggler tail isn't dominated by them.

**Rationale:** `CREATE INDEX CONCURRENTLY` is for live tables. Not needed here — no traffic. Regular `CREATE INDEX` is 2–3× faster, and the ACCESS EXCLUSIVE lock is irrelevant on an idle cluster.

**Pitfalls:**
- **Memory math.** `maintenance_work_mem × workers × concurrent_builds` = total RAM. 16 GB × 8 × 16 = 2 TB → OOM on a 512 GB box. Calculate before setting.
- **Don't forget:** unique constraints, FK constraints, PK constraints, sequences / `DEFAULT nextval(...)`.
- **`ANALYZE` every table** after indexes land. Planner stats matter for serve-time latency.

**Idempotency:** `CREATE INDEX IF NOT EXISTS`; check `pg_constraint` before adding constraints.

**Reference:** `voterFile.util.ts` (note `.util.ts`, not `.utils.ts`) shows the exact query shapes. Columns that get filtered/grouped and therefore need indexes (derived from inspecting the file):
- **District/geographic filters** (dynamic — the WHERE clause is `"<l2ColumnName>" = '<value>'`): could be any of ~250 district columns. Prod likely has a composite or per-column indexes on the common ones (`US_Congressional_District`, `State_Senate_District`, `State_House_District`, `County`, `City`, `Precinct`). Extract exact set from `pg_indexes` on prod in step 0.
- **Contact-method NOT NULL filters:** `"VoterTelephones_CellPhoneFormatted"`, `"VoterTelephones_LandlineFormatted"` — partial indexes likely.
- **`EXISTS` subquery for direct-mail de-dup:** joins `a."Mailing_Families_FamilyID" = b."Mailing_Families_FamilyID"` with a `GROUP BY ... HAVING COUNT(*) = 1` — so a btree on `"Mailing_Families_FamilyID"` per state table is load-bearing.
- **Custom filter expressions** (`customFiltersToQuery`): `"Parties_Description"`, `"Voters_Gender"`, `"Voters_Age"::integer`, `"Voters_VotingPerformanceEvenYearGeneral"` (regex-match string). None of these look like they benefit from single-column indexes given the OR-heavy combinations, but prod may have partial or expression indexes — confirm from the pg_dump.

A missing index only surfaces as a slow serve-time query. Confirm every index from prod's `pg_indexes` is in the manifest before step 5 runs.

**Done when:**
- Every `Voter{STATE}` table has a PK on `"LALVOTERID"` (verify via `pg_constraint`).
- Every non-unique index present on prod's `Voter*` tables exists on the new cluster's `Voter*` tables (diff `pg_indexes` new vs. old, excluding the three legacy rename columns).
- Every FK constraint present on prod is re-added (`pg_constraint` match).
- `ANALYZE` run on every `Voter*` table (`pg_stat_user_tables.last_analyze IS NOT NULL`).
- For the dynamic-l2Type concern: `SELECT DISTINCT "l2Type" FROM org_districts WHERE "l2Type" IS NOT NULL` on gp-api prod has ZERO values missing as columns in `information_schema.columns WHERE table_name='VoterTX'` (or any state). Ship nothing with holes.
- `IndexManifest` at `_manifest/indexes.json` lists every index + constraint created. `status=complete`.

---

## Step 6 — Resize (Provisioned → Serverless v2) + Lock Down

**Architecture:**
1. Attach the serving-tuned parameter group (autovacuum on, `synchronous_commit=on`, normal `maintenance_work_mem`) — takes effect on reboot.
2. `modify-db-instance --db-instance-class db.serverless --serverless-v2-scaling-configuration MinCapacity=X,MaxCapacity=128 --apply-immediately`.
3. Reboot lands (~5 min); verify parameter values stuck via `SHOW`.
4. Bump backup retention to match prod.
5. Enable deletion protection.
6. Hand the new endpoint to the app team for cutover.

**Matching the existing config:**
- **Max ACU = 128** (matches current prod: `Serverless v2 (0.5 – 128 ACUs)`).
- **Min ACU:** existing prod runs at 0.5. 0.5 ACU is tiny (~1 GB RAM) — first query after idle will be cold. If that's acceptable in prod, match it. Otherwise bump to 2–4 for latency stability.

**Rationale:** Aurora's compute/storage separation makes the instance-class switch a ~5-min reboot with zero storage copy. The cluster keeps its data, endpoint, and security config. Piggyback the parameter-group swap on the same reboot.

**Pitfalls:**
- **Serverless v2 manages some parameters itself** — `max_connections` auto-scales with ACU, `shared_buffers` is managed. If the load-tuned parameter group hard-codes values that serverless overrides, they'll reset. **Keep load-tuned and serving-tuned values in separate parameter groups.**
- **Don't forget to change parameter groups back.** Leaving `synchronous_commit=off` in production is a durability hazard.
- **Cost check:** `db.r7g.16xlarge` ≈ $5/hr. 12 hours of load+index ≈ $60. Don't over-optimize this.

**Alternative for zero-downtime cutover:** add a serverless reader, failover to it, delete the provisioned writer. Overkill for a one-shot refresh but on the table if cutover windows are tight.

**Done when:**
- Writer instance class = `db.serverless`. `ServerlessV2ScalingConfiguration` = `{MinCapacity: 0.5, MaxCapacity: 128}` (matching prod).
- Serving parameter group `gp-people-db-{date}-serve` attached; `SHOW synchronous_commit` returns `on`; `SHOW autovacuum` returns `on`.
- Backup retention = 14 days.
- Deletion protection = enabled.
- Cluster still has the `rds-s3-import` IAM role attached (don't detach — future incremental loads may reuse).
- `modify_db_cluster`'s pending-modifications queue is empty (all changes applied, no lingering reboots needed).

---

## Step 7 — Validation

**Architecture:** a read-only validator that compares new cluster vs old (`gp-people-db-prod`) and against the Databricks source.

**Checks:**
1. **Row counts per state** — new cluster vs Databricks manifest. Must match exactly. (Old RDS is stale, so it's not the source of truth for counts.)
2. **Schema diff** — `information_schema.columns` new vs old. Column names, types, nullability, defaults.
3. **Index/constraint diff** — `pg_indexes` and `pg_constraint` new vs old.
4. **Sample query parity** — run ~20 representative app queries against both instances and diff results. *Derive the queries from `voterFile.utils.ts`* to match real usage patterns.
5. **Distribution sanity checks** — null rates per column, min/max/median on key numeric columns, distinct counts on low-cardinality columns. Compare against Databricks.

**Rationale:** row counts catch bulk-loading mistakes but miss silent corruption (e.g. columns loaded in the wrong order). Distribution checks catch that class. Query-parity against prod proves functional equivalence at the API layer.

**Pitfalls:**
- **Old RDS is stale by assumption.** Source of truth for "correct data" is Databricks, not prod. Use prod only for schema/index/query parity.
- **Query-parity tolerances.** Some queries (counts, filters) should match between new and Databricks, but prod will legitimately differ due to staleness. Flag differences by category — schema/index/query-shape mismatches are bugs; count mismatches are expected.

**Done when:**
- `ValidateManifest.all_passed == true`. All five `ValidationCheck` entries present with `passed=true`:
  1. `row_counts_match_databricks` — per-state counts equal Databricks.
  2. `schema_diff_clean` — new `information_schema.columns` is a superset of prod's, minus the three retired typo columns, plus the ~15 new districts + ~13 demographic additions.
  3. `index_constraint_diff_clean` — all prod indexes/constraints present on new (via name or column-set match, allowing for renames).
  4. `sample_queries_pass` — ~20 `voterFile.util.ts`-derived queries return non-error results with row counts within tolerance.
  5. `l2Type_coverage` — every `DISTINCT "l2Type"` in `org_districts` maps to a real column.
- Validation report dumped to `s3://.../voter_export_{date}/_manifest/validate.json` AND a human-readable `validate.md` for a sign-off review.
- Nothing is handed to the app team before this step passes.

---

## Provisioned vs Serverless Rationale (for the record)

**For load + index, serverless is actively risky.**
- Serverless v2 scales on observed load with ~15–30s lag. A COPY storm spikes CPU + WAL throughput instantly; the first minutes of every batch run under-provisioned while ACUs ramp.
- 256 ACU max ≈ provisioned r7g.16xlarge, but costs ~40–50% more per ACU-hour than equivalent provisioned.
- Downscaling mid-load (during brief lulls) removes capacity right before the next file hits.

**For serving, match prod (serverless v2 0.5–128 ACU).**
- Prod is already serverless. Matching minimizes cutover risk.
- Mid-refresh swap from provisioned → serverless is a single `ModifyDBInstance` call + one reboot.

---

## Implementation: Python project with `uv`

The loader lives in its own new Python project (sibling to the existing `gp-data-platform/` — e.g. `loader/`), managed with `uv`. It does **not** live inside the dbt project: the goal is for the same code to run from a local CLI for the one-off refresh, then be containerized and wrapped in an Astro/Airflow DAG without being tangled up in dbt's Spark-submission machinery.

**Stack:**
- **Dependency / env management:** `uv` (pyproject.toml, `uv sync`, `uv run`).
- **AWS:** `boto3` for all RDS / IAM / S3 / VPC-endpoint provisioning, cluster describe, parameter-group CRUD, `add-role-to-db-cluster` calls. Prefer `boto3` over Terraform for this project — the whole point is a date-stamped immutable artifact and boto3 keeps that flow self-contained in Python.
- **PostgreSQL:** `psycopg` (v3, the modern successor to `psycopg2` — preferred unless a dependency pins v2) for orchestrator-issued `aws_s3.table_import_from_s3`, `CREATE INDEX`, `SET` statements, and manifest validation queries. Use explicit cursor-level `autocommit` + `SET synchronous_commit=off` rather than a heavier SQLAlchemy layer. Skip SQLAlchemy — there's no ORM value here; every statement is procedural DDL or admin SQL.
- **Databricks:** `databricks-sdk` (Python) for workspace-side operations (listing schemas, running SQL via SQL Warehouses, submitting Spark jobs for the unload step). The unload step itself runs as a Python script on a Databricks cluster (submitted via the Jobs API); the orchestrator stays Python.
- **Concurrency:** `concurrent.futures.ThreadPoolExecutor` for the 128-way parallel COPY in step 4 and the 8-to-16-way parallel `CREATE INDEX` in step 5. Threads (not processes) because the work is I/O-bound — PG is doing the heavy lifting server-side.
- **CLI:** `typer` or `click` for the subcommand surface in the CLI sketch below.
- **Manifests:** plain JSON via `json` + `pydantic` models. Schema in `loader/manifest/models.py`; persisted to S3 under `voter_export_{date}/_manifest/`.
- **Logging:** `structlog` or stdlib `logging` with JSON output — Airflow will scrape these.

**Project layout sketch:**
```
loader/
├── pyproject.toml
├── uv.lock
├── loader/
│   ├── __init__.py
│   ├── cli.py                      # typer entrypoint (see "CLI shape" below)
│   ├── schema/
│   │   ├── voter_columns.py        # VOTER_TARGET_COLUMNS, authoritative
│   │   ├── emit_ddl.py             # merges prod_dump.sql + databricks cols
│   │   └── databricks_columns.json # captured via `databricks tables get`
│   ├── manifest/models.py          # pydantic manifest schemas
│   ├── steps/
│   │   ├── inspect_prod.py         # step 0
│   │   ├── unload.py               # step 1 — submits Databricks job
│   │   ├── provision.py            # step 2 — boto3 RDS/IAM/VPCE
│   │   ├── create_schema.py        # step 3
│   │   ├── copy_s3.py              # step 4 — ThreadPoolExecutor of COPYs
│   │   ├── build_indexes.py        # step 5
│   │   ├── resize.py               # step 6
│   │   └── validate.py             # step 7
│   └── spark/
│       └── unload_job.py           # runs on Databricks cluster; packaged
│                                   # as a wheel and submitted via Jobs API
└── tests/
```

**Airflow integration later:** each step's entrypoint is a function accepting `(date: str, config: LoaderConfig)` and returning a manifest path. Airflow DAG imports and wraps them in `PythonOperator`s — no separate CLI shim needed for the DAG path. The `typer` CLI is for humans and for the one-off first refresh.

### Project Bootstrap

Do these once, at the start of the project, before writing any step code.

**1. Initialize the project:**
```bash
cd /Users/danball/projects/gp-data-parent
uv init loader --package
cd loader
uv python pin 3.12
```

**2. Pin dependencies (`pyproject.toml`):**
```toml
[project]
name = "loader"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "boto3>=1.35",
    "psycopg[binary,pool]>=3.2",       # v3, not psycopg2
    "databricks-sdk>=0.40",
    "pydantic>=2.8",
    "typer>=0.15",
    "structlog>=24.4",
    "rich>=13.9",                      # for nice CLI output
    "tenacity>=9.0",                   # retries around AWS/PG flaky calls
]

[project.optional-dependencies]
dev = [
    "pytest>=8.3",
    "pytest-mock>=3.14",
    "ruff>=0.7",
    "pyright>=1.1",
]

[project.scripts]
loader = "loader.cli:app"
```

Run `uv sync` and commit `uv.lock`.

**3. Environment variables (`.env.example` checked into repo):**
```
# AWS
AWS_PROFILE=goodparty-prod             # or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
AWS_REGION=us-west-2

# Target cluster (supplied per-run; default read from manifest/provision.json if present)
LOADER_S3_BUCKET=gp-voter-loader-us-west-2
LOADER_RUN_DATE=20260417

# Databricks
DATABRICKS_HOST=https://dbc-xxxxxxxx.cloud.databricks.com
DATABRICKS_TOKEN=                      # pulled from ~/.databrickscfg if using `databricks auth`
DATABRICKS_WAREHOUSE_ID=               # for SQL Warehouse-based queries
DATABRICKS_CLUSTER_ID=                 # for Jobs API-submitted Spark jobs

# Source-of-truth for the one secret we can't avoid
VOTER_DB_MASTER_USER=postgres
# VOTER_DB_MASTER_PASSWORD intentionally NOT here — pulled at runtime from
# AWS Secrets Manager (see below).
```

**4. Secrets strategy:**
- **RDS master password** for the new cluster: generated at provision time via `secrets.token_urlsafe(32)`, immediately stored in AWS Secrets Manager under `gp-people-db/{date}/master`, then referenced from there for the load. The master password is NEVER written to disk, env, or manifest.
- **RDS master password for prod** (step 0 inspection, step 7 validation): already in Secrets Manager — check `gp-people-db-prod/master` or the equivalent. If absent, ask the person doing the refresh for a short-lived credential rather than baking one in.
- **Databricks token:** use `databricks auth login` (already done — user's CLI is authenticated). SDK picks up `~/.databrickscfg` automatically.
- **AWS:** use SSO / profile (`AWS_PROFILE=goodparty-prod`). Don't bake access keys into `.env`.
- **Enforcement:** the loader's config loader (`loader/config.py`) should error hard if `VOTER_DB_MASTER_PASSWORD` appears in the process env — forces everyone through Secrets Manager.

**5. Logging + observability:**
- `structlog` configured in `loader/log.py`, JSON to stdout.
- Every step log line includes `run_date`, `step`, `state` (where applicable).
- Mirror the structlog JSON to `s3://.../voter_export_{date}/_logs/{step}-{timestamp}.jsonl` at the end of each step so Airflow's ephemeral log retention isn't the only copy.

**6. Local sanity before writing any code:**
```bash
uv run python -c "import boto3; print(boto3.client('sts').get_caller_identity())"
uv run python -c "from databricks.sdk import WorkspaceClient; print(WorkspaceClient().current_user.me().user_name)"
```
Both must work before step 0. If either fails, stop — don't paper over auth with try/except.

**7. First working slice (matches "Recommended First Milestone" below):**
Build `steps/inspect_prod.py` → `steps/unload.py` (for FL only) → `steps/provision.py` → `steps/create_schema.py` (FL only) → `steps/copy_s3.py` (FL only) → `steps/build_indexes.py` (FL only) → `steps/validate.py` (FL only). Get a working end-to-end for one state before generalizing. Only then fan out to 51 states + all files.

---

## Airflow / CLI Structure (for when this graduates from POC)

**Shape:**
- One DAG, one task per numbered step above.
- Steps 4 and 5 fan out dynamically — one task per state (or per file for step 4).
- Manifests in S3 are the coordination primitive — every task can re-read them, enabling retry from any point.
- **"Provision cluster" and "destroy cluster" belong in separate DAGs (or manual operators).** A glitchy retry should not be able to destroy a serving cluster.

**CLI shape:**
- `loader unload --date 20260416`
- `loader provision --date 20260416`
- `loader inspect-prod` (step 0 — pure read, no date needed)
- `loader create-schema --date 20260416`
- `loader copy --date 20260416 --state TX`  (or `--all`)
- `loader build-indexes --date 20260416`
- `loader resize --date 20260416`
- `loader validate --date 20260416`

Every command reads/writes its manifest; re-running is safe.

---

## Recommended First Milestone

Before building the whole orchestrator, do **one state end-to-end manually.** Pick **FL** — big enough to be realistic, not so big it dominates.

Unload FL → load FL → index FL → validate FL. Measure throughput. This tells you:
- Whether `aws_s3` behaves on your actual data.
- Real throughput numbers so you can project full-load duration.
- Which parameter-tuning knobs actually matter.

Generalize *after* the single-state path works. Building the full pipeline before validating the load primitive wastes a week on abstractions that turn out to be wrong.

---

## Environment Summary

- **AWS account:** `<aws-account-id>`. **Region:** `us-west-2` (everything).
- **Prod RDS:** `gp-people-db-prod`. Aurora PG 16.x, Serverless v2 (0.5–128 ACU), default param group. DB name / master user / writer endpoint are not committed (loader-config secret + `~/.pg_service.conf`). Holds the unified `public."Voter"` table (single Prisma-managed table, schema `public`) — not the legacy 51 `Voter{STATE}` tables. Use for inspection (step 0) and validation (step 7). Do not mutate.
- **New RDS:** `gp-people-db-{YYYYMMDD}`. Same VPC (`<vpc-id>`), subnet group (`<db-subnet-group>`), SG (`<security-group-id>`), KMS key (`arn:aws:kms:us-west-2:<aws-account-id>:key/<kms-key-id>`). Starts provisioned `db.r7g.16xlarge`, ends serverless v2 (0.5–128 ACU matching prod). Requires an S3 gateway VPC endpoint and a new `rds-s3-import` IAM role — neither exists today.
- **S3:** loader output at `s3://gp-voter-loader-us-west-2/voter_export_{YYYYMMDD}/` (bucket to be created). Existing `s3://normalized-voter-files/` holds raw L2 `VM2Uniform--XX--*.tab` inputs, unrelated to loader output.
- **Source data:** `int__l2_nationwide_uniform` (Databricks, catalog `goodparty_data_catalog`, schema `dbt`, ~218 M rows as of 2026-04-16, 807 columns in the source, keyed on `LALVOTERID`). State comes from the source column `state_postal_code`. Canonical target-column list for the *new* loader: `loader/schema/voter_columns.py` — a curated ~375-column superset of the legacy 348, scoped to gp-api + people-api references **including every L2-native district column that could appear in `org_districts.l2Type` (closes the `Judicial_Justice_of_the_Peace`-class bugs)**. See "Column Scoping".
- **Consumer:** app code in `gp-api/src/voters/voterFile/util/voterFile.util.ts` constructs per-user export SQL — `SELECT ... FROM public."Voter{state}" WHERE ...` — dynamically built from campaign metadata. Serving-side indexes must cover district filters, contact-method NOT NULL filters, and `Mailing_Families_FamilyID` for direct-mail de-dup.

---

## Test Run Log — FL Milestone (2026-04-17)

This section captures where the FL-only end-to-end test got to, decisions we
made under time pressure, and the exact next steps to resume. Hand this
section to whoever picks the work back up; the rest of the plan still applies.

### What's built and working

The entire code base in `loader/` is in place: bootstrap, config, logging,
manifest models, schema module (`VOTER_TARGET_COLUMNS` = 383 cols: 369 source
+ 12 removed + 2 derived), index-spec parser, `emit_ddl`, and all 8 step
modules with a Typer CLI. Deps pinned via `uv sync`; lint clean.

**Major shape changes vs. plan as originally written:**

1. **Databricks SQL, not PySpark, for the unload.** Team runs SQL everywhere,
   so `loader/sql/unload.py` emits `INSERT OVERWRITE DIRECTORY ... USING csv`
   per state, submitted via `databricks-sdk`'s StatementExecutionAPI to a SQL
   Warehouse. No `loader/spark/` module remains.

2. **Prod connection via SSM Parameter Store.** `connect_prod` reads the connection
   string from the SecureString `people-db-connection-string-{env}` (`LOADER_ENV`
   selects dev/qa/prod; dev and qa share a value, prod is separate) and hands it to
   psycopg. Secrets Manager is still the path for the *new* cluster's master password.
   The loader role needs `ssm:GetParameter` + `kms:Decrypt` on that parameter's key.

3. **Loader-created resources tagged `Environment=dev`, not `prod`.** The
   engineer's SSO role has an `aws:ResourceTag/Environment=dev` conditional
   Allow; everything the loader creates is tagged dev. Ops re-tags to prod at
   cutover. Configurable via `LOADER_TAG_ENVIRONMENT`.

4. **S3 bucket → `goodparty-warehouse-databricks`, not a dedicated bucket.**
   A dedicated `gp-voter-loader` bucket was created (and left empty and unused),
   but Databricks Serverless SQL can only write to S3 paths registered as
   Unity Catalog external locations. Creating a new UC external location needed
   IAM role creation (`databricks-*` prefix) which the engineer's SSO role
   can't do. Instead, an admin granted `READ_FILES` + `WRITE_FILES` on the
   existing external location `db_s3_external_databricks-s3-ingest-dbca4` →
   `s3://goodparty-warehouse-databricks/`. Loader writes under
   `voter_export_{date}/` there.

5. **Cluster-param tuning reduced to 2 params.** Aurora PG16 only exposes
   `synchronous_commit` and `autovacuum` as cluster-modifiable. The rest from
   the plan's `_LOAD_PARAMS` (max_wal_size, checkpoint_timeout, wal_compression)
   are Aurora-managed; `work_mem`/`maintenance_work_mem` are instance-level
   (DB parameter group, not cluster). Session-level `SET` in step 4 covers
   what matters during COPY.

6. **Per-file row counts dropped.** `aws s3api select-object-content` returns
   `MethodNotAllowed` on this bucket. Row counts now come from a second
   `SELECT COUNT(*)` SQL on the warehouse, one per state. Files in the
   manifest carry `size_bytes` but `row_count=0`.

7. **Bug fixes discovered mid-test:**
   - `pg_dump -t 'public."Voter*"'` matches nothing — the `*` inside the
     quoted identifier is literal. Correct pattern: `public."Voter"*` with
     wildcard *outside* the quotes.
   - `Mailing_HHGender_Description` was in `NEW_DEMOGRAPHIC_COLUMNS` under
     the assumption L2 restored it. It's not in the Databricks source
     (only `Residence_HHGender_Description` exists; different concept —
     mailing vs residence household). Moved to `REMOVED_COLUMNS` so it
     stays as a NULL placeholder for schema stability.
   - `_ensure_cluster_param_group` and `_ensure_master_secret` both now
     log-and-continue (or fall through to create) on `AccessDeniedException`,
     since tag-conditional IAM policies can return AccessDenied on
     not-yet-created resources.

### FL milestone retrospective (2026-04-17)

Ran end-to-end through index-building before tearing down on the same
day. All artifacts from the FL run were removed by `loader teardown`
(cluster, writer, param groups, IAM role, secret, `voter_export_20260417/`
S3 prefix). Nothing persistent remains from the FL attempt — the next
run starts from a genuinely clean slate.

What got validated:

| Step | Command | Outcome |
|---|---|---|
| 0 | `loader inspect-prod --date 20260417` | ✅ prod_dump.sql + databricks_columns.json + inspect.json all written |
| 1 | `loader unload --date 20260417 --state FL` | ✅ 15,017,471 rows → 39 `part-*.csv`, ~28 GB, ≈ 43s on Serverless Medium |
| 2 | `loader provision --date 20260417` | ✅ completed after admin ran one-shot `add-role-to-db-cluster` (see Tradeoff #18) — cluster+writer in ~4m45s |
| 3 | `loader create-schema --date 20260417` | ✅ 52 tables in ~18s |
| 4 | `loader copy --date 20260417 --state FL --parallelism 16` | ✅ 15,017,471 rows in 122s (~123k rows/sec) via server-side `aws_s3.table_import_from_s3` |
| 5 | `loader build-indexes --date 20260417` | ⚠️ got through ~6,300 indexes across ~30 states before we chose to abandon and teardown (orthogonal to any bug — the step was healthy, time-boxed by the operator) |
| 6 | `loader resize --date 20260417` | not reached |
| 7 | `loader validate --date 20260417` | not reached |

Code changes that landed mid-test and are now part of the loader:

- `_attach_iam_role` in `steps/provision.py` is now **describe-first**.
  AWS authorizes `iam:PassRole` before returning
  `DBClusterRoleAlreadyExistsFault`, so the old catch-and-skip pattern
  failed when admin had already attached the role but the caller still
  lacks PassRole. The new code checks `AssociatedRoles` status=ACTIVE
  before calling `add_role_to_db_cluster`.
- `loader teardown` CLI command added (see below) — replaces the
  previous manual AWS-CLI block for per-run cleanup.

### Teardown (loader teardown CLI)

Built-in command for deleting loader-owned resources for one `run_date`.
Safe on partial runs: describes-first, idempotent, walks
cfg-derived resource names only, enforces `Environment=dev` tag guards
per resource, refuses anything tagged `Environment=prod`.

```bash
# Dry-run first — prints every resource that would be deleted.
uv run --env-file .env loader teardown --date 20260417

# When the dry-run looks right, commit:
uv run --env-file .env loader teardown --date 20260417 --confirm

# Add --delete-s3 to also drop voter_export_20260417/ artifacts;
# add --delete-vpce to retire the S3 gateway endpoint (default keeps it).
```

Deletion order: writer instance → `gp-people-db-{date}` cluster (auto
disables deletion-protection first) → `-load`/`-serve` param groups →
`rds-s3-import-{date}` IAM role (inline policies + role) →
`gp-people-db/{date}/master` secret → (opt-in) `voter_export_{date}/`
S3 prefix → (opt-in, only when the VPCE carries the loader's full tag
signature) the S3 VPC endpoint.

Explicitly never touched, by code audit: the reused dev subnet group
`api-rds-subnet-group`, the reused dev SG `<security-group-id>`, the
VPC and its subnets/route tables, the KMS key, the S3 bucket itself,
and the shared `vpce-0869178526f7e608c` VPCE (tag signature doesn't
match loader ownership). Also the lingering unused empty bucket
`gp-voter-loader` is not touched — delete it manually if desired
(`aws s3api delete-bucket --bucket gp-voter-loader --region us-west-2`).

Safety model: the tag-guard refuses any resource tagged
`Environment=prod`, so a fat-fingered date that now names a cluster
ops re-tagged to prod at cutover aborts before anything is deleted.
Run the dry-run before `--confirm` regardless.

### Tradeoffs / decisions to revisit before productionizing

1. **Shared output bucket.** `goodparty-warehouse-databricks` is also used by
   other ingest workloads. For prod, create a dedicated `gp-voter-loader` UC
   external location with its own IAM storage-credential role. Needs an AWS
   admin who can create the `databricks-voter-loader-uc` IAM role (the
   Engineer SSO role can't create IAM roles outside specific prefixes).

2. **Instance sizing.** `db.r7g.2xlarge` (8 vCPU / 64 GB) was picked for the
   15 M-row FL test. For the full 218 M-row refresh the plan's
   `db.r7g.16xlarge` (64 vCPU / 512 GB) is still right — tune
   `LOADER_LOAD_INSTANCE_CLASS` and `--parallelism` up.

3. **Only 2 cluster-level params tuned.** `synchronous_commit=off`,
   `autovacuum=off` during load. Other suggested params
   (`max_wal_size=64GB`, `checkpoint_timeout=60min`, `wal_compression=on`) are
   Aurora-managed and can't be set. If load throughput is insufficient,
   consider adding an **instance**-level `DBParameterGroup` to tune
   `work_mem`/`maintenance_work_mem` at the daemon level instead of per
   session.

4. **Removed columns (12 NULL placeholders).** Every `Voter{STATE}` table
   carries 12 columns the L2 source doesn't populate. Schema-stable, but
   consider auditing the gp-api and people-api for references and dropping
   the truly unused ones (likely `Religions_Description`, `Languages_Description`,
   `MilitaryStatus_Description`, `MaritalStatus_Description`).

5. **Row counting via extra `COUNT(*)` SQL.** One extra warehouse query per
   state (51 queries for full refresh) just to populate
   `UnloadManifest.per_state_row_counts`. Negligible cost but could be folded
   into the unload SQL itself by reading `operationMetrics` from the statement
   result (Databricks exposes `numOutputRows`). Not implemented yet.

6. **`pg_dump` tooling.** Requires Postgres client binaries on PATH. Local
   dev uses `/Applications/Postgres.app/Contents/Versions/18/bin/pg_dump`.
   When this moves into Airflow/Astro, the image needs `postgresql-client`
   installed.

7. **Validator queries hard-coded to `VoterTX`.** Step 7's `_SAMPLE_QUERIES`
   all target TX. Parameterize by sample state so FL-only runs validate
   against the right table.

8. **No dedicated Databricks UC external location for loader output.** We
   write into a shared ingest location. The plan's intent was a dedicated
   bucket. Blocked on IAM role creation for `databricks-voter-loader-uc`
   (see #1). Consider asking an AWS admin to create that role + storage
   credential as a one-time setup, then the loader can own its own location.

9. **S3 bucket region suffix.** Plan originally named the bucket
   `gp-voter-loader-us-west-2`. Changed to `gp-voter-loader` (no region
   suffix) — the region is bucket metadata and we region-pin everything
   already. The bucket exists but is unused since we ended up writing to
   `goodparty-warehouse-databricks`. Either delete the empty bucket or repurpose
   it for the dedicated-location follow-up (#1/#8).

10. **Loader-level and session-level tuning split.** Load parameter group
    covers daemon-level tuning (`synchronous_commit`, `autovacuum`); step 4's
    `_SESSION_SQL` covers per-backend tuning (`work_mem`, `maintenance_work_mem`,
    `statement_timeout=0`). If we ever add a DB-instance parameter group,
    decide which layer owns which setting to avoid confusion.

11. **Index build parallelism = 8 concurrent builds.** In step 5. For a
    2xlarge (8 vCPU) that's already saturating; for a 16xlarge bump to
    16–24.

12. **Validate's `l2Type_coverage` check reads from prod's `org_districts`** —
    but `org_districts` is in the **gp-api app DB**, not the voter DB we're
    connecting to. Currently the check is wrapped in try/except and logs a
    warning. For real coverage validation, add a separate pg_service entry
    for the app DB and query there.

13. **Fresh master password each provision.** If the cluster is re-created
    with the same date stamp, the secret is reused; if date changes, a new
    secret is generated. Consider secrets-rotation policy for prod.

14. **IAM grant scope for resume.** The minimum ask is narrow and tag-scoped,
    but the `StringEqualsIfExists` variant is permissive (allows untagged
    resources through). Tighter alternative: `StringEquals` on both RequestTag
    (for creates) and ResourceTag (for mutates/deletes) to force Environment=dev
    on every action. Will fail any create call that forgets to include the tag
    in the request.

15. **Reused dev-tagged subnet group and security group** (FL test, 2026-04-17).
    Config's defaults (`<db-subnet-group>`, `<security-group-id>`) are
    prod-tagged, and the Engineer SSO role's `DevResourceCreation` statement
    excludes `Environment=prod` via `StringNotEquals`. Rather than ask an admin
    to create dedicated loader-owned dev resources, we overrode via env vars to
    reuse the existing dev-tagged `api-rds-subnet-group` (same two subnets) and
    `<security-group-id>` / `api-rds-security-group` (already permits prod app
    SG, bastion, VPC CIDR, and Databricks peering on 5432). For prod refresh:
    either (a) ops creates dedicated `gp-voter-loader-*` subnet group + SG both
    tagged `dev` for the loader to own, or (b) the loader runs under a role
    that can touch prod-tagged VPC resources directly. Option (a) is cleaner.

16. **Env-var override pattern for VPC infra.** `.env` now carries
    `LOADER_DB_SUBNET_GROUP` and `LOADER_SECURITY_GROUP_ID` pointing at the
    reused dev resources. The `DEFAULT_*` constants in `config.py` still point
    at the prod-tagged equivalents (the "real" cutover targets). Consider
    inverting: make `config.py` defaults point at dev resources and require
    explicit env-var overrides for the prod cutover, so a bare run from a
    fresh checkout doesn't fail on IAM before it fails on anything useful.

17. **KMS key is prod-tagged but unblocked by RequestTag.** The cluster uses
    `arn:aws:kms:us-west-2:<aws-account-id>:key/<kms-key-id>` (the single account-wide
    RDS KMS key). It has no Environment tag; `gp-people-db-develop` already
    uses the same key, which proves IAM authz passes for CreateDBCluster
    when the cluster request carries `Environment=dev`. No action needed,
    but flagged because the "dev-tagged everything" mental model of the SSO
    policy has this exception: KMS keys aren't partitioned by environment.

18. **`iam:PassRole` gap in the SSO policy** (hit mid-provision on 2026-04-17).
    `rds:AddRoleToDBCluster` requires `iam:PassRole` on the target role. The
    SSO policy's `DevResourceOperations` statement grants `*` with an
    `aws:ResourceTag/Environment=dev` condition, but **`iam:PassRole` does
    not honor `aws:ResourceTag`**, and AWS explicitly discourages using
    `iam:ResourceTag` on PassRole ("does not have reliable results" — IAM
    docs). The canonical fix is a naming-convention + service-pin grant.
    The simplest safe statement for the admin to add to the SSO permission
    set:
    ```json
    {
      "Sid": "LoaderPassRoleForRDS",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::<aws-account-id>:role/rds-s3-import-*",
      "Condition": {
        "StringEquals": {
          "iam:PassedToService": "rds.amazonaws.com"
        }
      }
    }
    ```
    Optional tightening: add `StringLike: iam:AssociatedResourceARN =
    arn:aws:rds:us-west-2:<aws-account-id>:cluster:gp-people-db-*` to pin the
    target cluster prefix. For the FL milestone specifically, a one-shot
    admin-run `aws rds add-role-to-db-cluster --db-cluster-identifier
    gp-people-db-20260417 --role-arn
    arn:aws:iam::<aws-account-id>:role/rds-s3-import-20260417 --feature-name
    s3Import` unblocks without a policy edit. Document either path as a
    prerequisite for future refreshes.

19. **Tunnelblick VPN route overlap with RDS subnets.** The engineer's
    home LAN uses `10.0.0.0/20` on `en0`, which *overlaps* the VPC's
    RDS subnet CIDRs (`10.0.4.0/22`, `10.0.12.0/22`). The tunnel pushes
    a `10.0.0.0/16` route via `10.8.0.9` on `utun4`, but the link-local
    /20 shadows it — meaning any attempt to reach an RDS IP fails ARP
    on en0, gets cached as a `UHLWI ... !` blackhole, and further
    shadows the tunnel route. Workaround is two more-specific static
    routes *through the tunnel gateway*:
    ```bash
    sudo route add -net 10.0.4.0/22 10.8.0.9
    sudo route add -net 10.0.12.0/22 10.8.0.9
    ```
    These cover every current and future RDS IP in those two subnets
    via `utun4`. Add to the operator's shell profile or a pre-run
    checklist. If the VPN provider ever supports `redirect-gateway
    def1-bypass-dhcp` or pushing more-specific routes, that's the
    cleaner fix.

20. **Stray S3 object survived teardown.** On 2026-04-17 teardown,
    `loader teardown --confirm --delete-s3` logged `object_count=1189`
    and exited cleanly, but a single 364 MB `state_id=MO/part-00009-*`
    CSV (from an earlier nationwide unload attempt that pre-dated the
    run's date stamp) remained. Unclear whether it was a pagination
    edge case in `list_objects_v2`, an S3 eventual-consistency quirk,
    or the object was written by an unrelated process between list
    and delete. Bucket versioning is disabled, so it's not a
    delete-marker artifact. Action: either add a post-delete
    re-listing + retry loop in `_delete_s3_prefix`, or stop sharing
    the bucket (§4 of the AWS Policy Changes section) which moots the
    class of problem. For now, a manual `aws s3 rm --recursive
    s3://<bucket>/voter_export_<date>/` after `loader teardown` is
    the safety net.

## Starting a Nationwide Refresh — Cold-Start Checklist

Use this as a hand-off for anyone picking up the work from scratch for
the first nationwide refresh (or any refresh, FL or otherwise). Assumes
`loader/` is checked out and `uv sync` has run.

### Prerequisites (once per operator machine)

1. **SSO authenticated**:
   ```bash
   aws sso login --profile gp-engineer
   ```
   The token lasts ~8h. Re-run if `cli.caller` fails with
   `SSOTokenLoadError`.
2. **Tunnelblick connected** AND the two subnet routes in place (see
   Tradeoff #19):
   ```bash
   sudo route add -net 10.0.4.0/22 10.8.0.9
   sudo route add -net 10.0.12.0/22 10.8.0.9
   ```
   Verify with `nc -zvw 5
   gp-people-db-prod.cluster-<hash>.us-west-2.rds.amazonaws.com
   5432` — should say `succeeded`.
3. **`pg_service.conf` [people] entry**: host =
   `gp-people-db-prod.cluster-<hash>.us-west-2.rds.amazonaws.com`,
   SSL required, no password (IAM auth or passwordless pre-arranged).
4. **Postgres client binaries on PATH** (for `pg_dump` during
   `inspect-prod`). Local dev uses
   `/Applications/Postgres.app/Contents/Versions/18/bin/`.
5. **AWS policy §1 grant applied** (see "AWS Policy Changes §1" below).
   Without it, `provision` will stop on `iam:PassRole` denied and need
   a one-shot admin `aws rds add-role-to-db-cluster`. Check by trying
   any refresh — if you can't add the role to the cluster as part of
   provision, the grant isn't in place.

### Environment configuration (`.env`)

```
AWS_REGION=us-west-2
LOADER_S3_BUCKET=goodparty-warehouse-databricks
LOADER_DATABRICKS_TABLE=goodparty_data_catalog.dbt.int__l2_nationwide_uniform
DATABRICKS_WAREHOUSE_ID=<databricks-warehouse-id>
LOADER_ENV=dev
LOADER_TAG_PROJECT=gp-api
LOADER_TAG_ENVIRONMENT=dev

# Reuse existing dev-tagged network infra (see Tradeoffs #15, #16):
LOADER_DB_SUBNET_GROUP=api-rds-subnet-group
LOADER_SECURITY_GROUP_ID=<security-group-id>

# For a NATIONWIDE refresh, remove or unset LOADER_LOAD_INSTANCE_CLASS
# so the config default (db.r7g.16xlarge) takes over. The FL test used
# db.r7g.2xlarge, which is undersized for 218M rows.
# LOADER_LOAD_INSTANCE_CLASS=db.r7g.2xlarge
```

### Run sequence

Pick a `run_date` stamp in `YYYYMMDD` form, e.g. `20260601`. That's the
one required argument throughout.

```bash
DATE=20260601

# Step 0 — capture prod shape (prod_dump.sql, databricks_columns.json,
# prod_row_counts per state). Idempotent; takes ~1 min.
uv run --env-file .env loader inspect-prod --date $DATE

# Step 1 — Databricks SQL unload, all 51 states. ~5-10 min on Serverless
# Medium warehouse for the full 218M rows. Omit --state for nationwide.
uv run --env-file .env loader unload --date $DATE

# Step 2 — provision Aurora cluster + writer. ~4-8 min for the
# db.r7g.16xlarge default. Idempotent on re-run.
uv run --env-file .env loader provision --date $DATE

# Step 3 — apply DDL (52 tables, aws_s3 + aws_commons extensions).
# ~20 sec.
uv run --env-file .env loader create-schema --date $DATE

# Step 4 — server-side COPY from S3 via aws_s3.table_import_from_s3.
# NATIONWIDE: use default parallelism (128, tuned for 16xlarge). FL-only:
# was --parallelism 16 for the 2xlarge.
uv run --env-file .env loader copy --date $DATE

# Step 5 — PKs, non-unique indexes, FKs, ANALYZE. Longest step
# (~1-3 hours for full 218M row set depending on instance class).
uv run --env-file .env loader build-indexes --date $DATE

# Step 6 — resize writer to db.serverless, swap to serve param group,
# bump backup retention, turn on deletion protection.
uv run --env-file .env loader resize --date $DATE

# Step 7 — 5 validation checks. Non-zero exit on any failure. Known:
# l2Type_coverage will fail/warn because org_districts lives in the
# app DB, not the voter DB (Tradeoff #12).
uv run --env-file .env loader validate --date $DATE
```

Each step writes a manifest to
`s3://<bucket>/voter_export_$DATE/_manifest/<step>.json`. Re-running a
step with a complete manifest is a no-op, so you can retry mid-pipeline
without worry about duplicate state.

### Status + teardown

```bash
# Print per-step status table for a run_date.
uv run --env-file .env loader status --date $DATE

# Dry-run teardown (safe; lists what would be deleted).
uv run --env-file .env loader teardown --date $DATE

# Commit. Add --delete-s3 to also drop the 20-40 GB of CSVs; add
# --delete-vpce only if you created your own endpoint (we currently
# reuse a shared one, so it's a no-op).
uv run --env-file .env loader teardown --date $DATE --confirm --delete-s3
```

### Known things to expect

- **PassRole gate** (Tradeoff #18, AWS Policy §1): without the permanent
  grant, provision stops after creating the cluster+writer+role with
  `iam:PassRole` denied. Admin runs one-shot `add-role-to-db-cluster`;
  then `loader provision` continues on re-run (describe-first path picks
  up the attached role).
- **`validate`'s `l2Type_coverage` check** will fail/warn (Tradeoff #12).
  Expected for nationwide too until we give the loader access to the
  app DB.
- **`validate`'s sample queries** are hardcoded to `VoterTX` (Tradeoff
  #7). For nationwide, TX *is* loaded, so queries run against real
  data. For state-specific tests, parameterize.
- **Build-indexes runtime** scales roughly with data size: FL (15M
  rows) took ~15 min per-state just for the indexes on the loaded
  table; nationwide (218M across 51 tables) will be dominated by the
  large-state tables (TX, CA, NY, FL). Plan for 2-4 hours. Empty
  tables are fast (~10-30 sec each).
- **Index build SSO expiry risk**: if the run starts near session
  expiry, boto3 refresh fires per-client but long-running psycopg
  connections don't refresh. Start index-building with at least 2h
  of SSO session remaining.

## AWS Policy Changes for Long-Term Loader Autonomy

This section enumerates the IAM permission changes that would let the
loader run end-to-end without any of the workarounds collected above.
Ordered by necessity. Apply to the Engineer SSO permission set
(`AWSReservedSSO_EngineerAccess_*`'s inline policy) — the Engineer role
is what the loader assumes.

All statements below target the `<aws-account-id>` (prod) account,
`us-west-2` region. Expand or tighten scope based on account policy.

### 1. MUST-HAVE — `iam:PassRole` for loader-created RDS roles

**Problem**: Every `provision` run attaches the new `rds-s3-import-{date}`
IAM role to the cluster via `rds:AddRoleToDBCluster`, which requires
`iam:PassRole` on the role. The SSO policy's existing `DevResourceOperations`
grants `*` on `ResourceTag/Environment=dev`, but AWS explicitly documents
that `aws:ResourceTag` is not reliable for `iam:PassRole` (per IAM user
guide). Without this addition, every refresh blocks on an admin manually
running `aws rds add-role-to-db-cluster` per new cluster — permanent
async friction.

**Statement to add** (simplest safe form, from tradeoff #18):

```json
{
  "Sid": "LoaderPassRoleForRDS",
  "Effect": "Allow",
  "Action": "iam:PassRole",
  "Resource": "arn:aws:iam::<aws-account-id>:role/rds-s3-import-*",
  "Condition": {
    "StringEquals": {
      "iam:PassedToService": "rds.amazonaws.com"
    }
  }
}
```

What this buys:
- `Resource` prefix: only loader-convention-named roles can be passed.
- `iam:PassedToService`: the role can only be handed to RDS, not Lambda /
  EC2 / etc. A role with a different trust policy couldn't assume for a
  different service anyway, but this is defense-in-depth.

Optional tightening (defer unless audit asks):

```json
"StringLike": {
  "iam:AssociatedResourceARN":
    "arn:aws:rds:us-west-2:<aws-account-id>:cluster:gp-people-db-*"
}
```

Pins which RDS cluster the role can be attached to. Narrower but costs
flexibility if the cluster-naming convention ever changes.

### 2. MUST-HAVE — `rds:AddRoleToDBCluster` / `rds:RemoveRoleFromDBCluster`

**Problem**: Same `provision` step. The caller needs both `iam:PassRole`
(§1) and `rds:AddRoleToDBCluster` permission on the cluster. Today this
is *already covered* by the broad `DevResourceOperations` statement since
the target cluster is tagged `Environment=dev` at creation via
`RequestTag`. Adding a specific statement would be belt-and-suspenders but
not strictly required.

If you want to make the loader's IAM asks legible as one cohesive block
(recommended when the reviewer will scan the permission set):

```json
{
  "Sid": "LoaderAddRoleToVoterDBCluster",
  "Effect": "Allow",
  "Action": [
    "rds:AddRoleToDBCluster",
    "rds:RemoveRoleFromDBCluster"
  ],
  "Resource": "arn:aws:rds:us-west-2:<aws-account-id>:cluster:gp-people-db-*",
  "Condition": {
    "StringEquals": {
      "aws:ResourceTag/Environment": "dev"
    }
  }
}
```

### 3. OPTIONAL — Let loader own dedicated subnet group + security group

**Status today**: Config defaults in `src/loader/config.py` point at prod
resources (`<db-subnet-group>`, `<security-group-id>`), and
`.env` overrides to the shared dev-tagged alternatives
(`api-rds-subnet-group`, `<security-group-id>`). The reuse works but
couples loader-created clusters to infrastructure managed by other teams.

**Decision to make**: do we want the loader to provision and own its own
subnet group + SG per refresh? If yes, these statements unlock that path.
If no (accept the reuse pattern indefinitely), skip this section.

**3a. Create a dev-tagged SG inside the prod-tagged VPC:**

The existing `DevResourceCreation` statement blocks SG creation because
`CreateSecurityGroup` authzs against the VPC resource (which is
`Environment=prod`) and the statement's `StringNotEquals: ResourceTag/
Environment=prod` rejects it. Add a carve-out:

```json
{
  "Sid": "LoaderCreateSecurityGroupInProdVPC",
  "Effect": "Allow",
  "Action": "ec2:CreateSecurityGroup",
  "Resource": [
    "arn:aws:ec2:us-west-2:<aws-account-id>:security-group/*",
    "arn:aws:ec2:us-west-2:<aws-account-id>:vpc/<vpc-id>"
  ],
  "Condition": {
    "StringEquals": {
      "aws:RequestTag/Environment": "dev"
    }
  }
}
```

**3b. Authorize ingress rules on the new SG that reference prod-tagged
source SGs** (e.g., the app's client SG `<security-group-id>`):

AWS authz for `ec2:AuthorizeSecurityGroupIngress` applies to the SG
*being modified*. Referenced source SGs aren't separately authz'd in
most documented cases — **test this empirically** before relying on it.
If testing shows a deny:

```json
{
  "Sid": "LoaderAuthorizeSecurityGroupRules",
  "Effect": "Allow",
  "Action": [
    "ec2:AuthorizeSecurityGroupIngress",
    "ec2:AuthorizeSecurityGroupEgress",
    "ec2:RevokeSecurityGroupIngress",
    "ec2:RevokeSecurityGroupEgress"
  ],
  "Resource": "arn:aws:ec2:us-west-2:<aws-account-id>:security-group/*",
  "Condition": {
    "StringEqualsIfExists": {
      "ec2:ResourceTag/Environment": "dev"
    }
  }
}
```

Caveat: this lets the dev-tagged SG reference *any* other SG (including
prod-tagged ones) in its rules. That mirrors what we actually want —
the loader's new SG needs to allow ingress from the prod-tagged app SG
for cutover — but reviewers will want the `ec2:ResourceTag/Environment=dev`
on the target SG, which the condition enforces.

**3c. Create a dev-tagged DB subnet group referencing prod subnets:**

Per AWS docs, `rds:CreateDBSubnetGroup` doesn't authz against the
referenced `ec2:subnet` ARNs — only against the subnet-group resource
being created. Already covered by `DevResourceCreation` since the new
subnet group is tagged `Environment=dev` at creation. **No new statement
needed.**

### 4. OPTIONAL — Dedicated S3 output bucket + Databricks external location

**Status today**: loader writes to shared `goodparty-warehouse-databricks`
under `voter_export_*/` (tradeoff #1). For prod-cadence refreshes, we'd
want a dedicated bucket.

**Two asks from the newly-empowered admin role** (these are Databricks /
S3 operations, not IAM-on-Engineer-SSO-role edits):

1. Create IAM role `databricks-voter-loader-uc` with a Databricks-compatible
   trust policy and S3 read/write on the new loader bucket. This is the
   role Databricks Unity Catalog assumes to read/write on behalf of the
   loader SQL job.
2. Create a Unity Catalog storage credential (referencing that role) and
   an external location pointing at `s3://gp-voter-loader/voter_export_*/`.
3. Grant `READ_FILES` + `WRITE_FILES` on the external location to the
   `dan@` service principal (or whichever runs the loader).

The loader's own IAM permissions (Engineer SSO) already cover `s3:*`
unconditionally, so creating the bucket and writing to it is already
allowed.

### 5. OPTIONAL — Narrower alternative to the broad `DevResourceOperations`

**Status today**: The SSO policy's `DevResourceOperations` statement
grants `*` action on `*` resource with `aws:ResourceTag/Environment=dev`.
This is convenient but broad — any engineer can delete or modify any
dev-tagged resource anywhere.

If an audit pushes for tighter-scoped permissions, the loader's actual
need boils down to these action families:

| Service | Actions |
|---|---|
| `rds:*` (on voter-db resources) | Create/Modify/Delete cluster + instance + param group; AddRoleToDBCluster |
| `iam:*` (on rds-s3-import-* roles) | CreateRole, PutRolePolicy, DeleteRole, DeleteRolePolicy, GetRole, ListRolePolicies, ListRoleTags |
| `secretsmanager:*` (on gp-people-db/* secrets) | CreateSecret, GetSecretValue, DescribeSecret, DeleteSecret, PutSecretValue |
| `kms:*` (on the RDS KMS key) | GenerateDataKey, Decrypt, DescribeKey, CreateGrant |
| `ec2:Describe*` | VPC/subnet/SG/VPCE lookups during provision |
| `s3:*` (on the loader bucket + prefixes) | already covered by the unconditional `s3:*` statement |

Producing the full scoped-down policy is a ~1 hour exercise; the above
is the shopping list. Leave the broad statement in place for now unless
compliance requires narrowing.

### Apply order + verification

1. Apply §1 (PassRole). Verify by running
   `uv run --env-file .env loader provision --date $(date +%Y%m%d)`;
   should complete without admin intervention, ending with
   `provision.iam.attached` in the log.
2. If pursuing §3 (dedicated subnet group / SG), apply 3a + 3b together
   and run a test provision pointing `LOADER_DB_SUBNET_GROUP` /
   `LOADER_SECURITY_GROUP_ID` at fresh loader-created names. Probably
   easiest to remove the `.env` overrides and restore config defaults to
   a new `gp-voter-loader-*` pair.
3. §4 (dedicated bucket) is a larger refactor — wait until nationwide
   refresh cadence is proven before untangling the shared-bucket
   arrangement.
4. §5 is a "when an audit asks" exercise; defer.
