"""One-time, manifest-verified loader for the Census ACS 2020-2024 5-year table-based Summary File.

This loader is deliberately specific to the 2020-2024 vintage (the 2024
summary-file release): the URL template, table names, manifest, dbt source,
staging model, and anchor tests all encode it together. A future vintage is a
reviewed change to all of those, not a flag; the loader refuses any manifest
whose recorded vintage is not the pinned one.

Downloads the locked table inventory (plus the Connecticut block-to-planning-region
crosswalk) from the public bulk directories, verifies every file's sha256 and data
row count against the committed manifest BEFORE touching any warehouse table, then
uploads the raw files to a Unity Catalog volume (re-hashing each file at upload
time against the verified manifest hash) and creates one all-string table per file
via read_files. Casting, renaming, and jam-code handling happen downstream in dbt
staging, per repo convention. One deliberate load-time exception: the crosswalk
CSV's "block number" header is renamed block_number in the CTAS select list
because Delta rejects spaces in column names, and its UTF-8 BOM is stripped before
hashing so the manifest hash covers exactly the bytes read_files parses.

Publication semantics: a first load issues plain CREATE TABLE, so the warehouse
itself enforces fail-if-exists. --allow-replace permits an explicit re-run over
existing targets: the preflight records every existing target's current Delta
version as the concrete rollback target, then tables are replaced one at a time.
These are one-time, operator-owned sandbox tables; the safeguards here are
operational (verify-first ordering, truthful reporting, documented rollback),
not concurrency control. Default downloads live in a temporary directory that
is cleaned up on exit; pass --download-dir to keep them.

Failure handling: publication proceeds one table at a time (upload with re-hash,
create, immediate count check) and stops at the first problem. Bookkeeping is
truthful about mutation: a table counts as mutated the moment its CREATE
statement succeeds, so a count-check problem reports it as MUTATED BUT
UNVERIFIED rather than pretending nothing happened, and recovery guidance
distinguishes a table this run CREATED (drop it once inspected) from one it
REPLACED (RESTORE TABLE ... TO VERSION AS OF the recorded pre-replacement
version). A communication problem after a statement was submitted is an UNKNOWN
outcome, never a plain failure: wait for the statement to reach a terminal state
and re-check the table before re-running.

Credentials: the bulk download path needs none (public HTTPS). Databricks access
uses the CLI OAuth profile; the script strips DATABRICKS_TOKEN from subprocess
environments so the profile always wins. No Census API key is used, logged, or
stored by this script; keep it that way.

Usage (from the dbt/ directory):
    uv run python scripts/load_acs_summary_file.py --write-manifest --verify-only
        # bootstrap: record download facts; NEVER writes to the warehouse
    uv run python scripts/load_acs_summary_file.py
        # first load: verify every download against the COMMITTED manifest,
        # then publish one table at a time with plain CREATE TABLE
    uv run python scripts/load_acs_summary_file.py --verify-only
        # re-verify downloads against the committed manifest; no warehouse writes
    uv run python scripts/load_acs_summary_file.py --tables b19001 --allow-replace
        # subset re-run over existing tables (explicit replacement)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

_SYSTEM_GETADDRINFO = socket.getaddrinfo


def _ipv4_first_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    """Prefer IPv4 for downloads: the Census bulk host publishes AAAA records
    whose route is unreachable from some networks, and an unspecified-family
    lookup can come back IPv6-only there, leaving urllib nothing usable. Ask
    for A records first; fall back to the system default so IPv6-only
    environments still work."""
    if family == 0:
        try:
            return _SYSTEM_GETADDRINFO(host, port, socket.AF_INET, type, proto, flags)
        except socket.gaierror:
            pass
    return _SYSTEM_GETADDRINFO(host, port, family, type, proto, flags)


# The pinned vintage. Everything downstream (table names, dbt source, staging
# model, anchor constants) encodes 2020-2024; a new vintage is a reviewed
# change to all of them together, never a runtime flag.
VINTAGE = "2020-2024 ACS 5-year"

ACS_TABLES = (
    "b01001",
    "b03002",
    "b15002",
    "b19001",
    "b19013",
    "b19025",
    "b23025",
    "b25003",
    "b25008",
    "b25077",
    "b28002",
    "c17002",
)
ACS_URL_TEMPLATE = (
    "https://www2.census.gov/programs-surveys/acs/summary_file/2024"
    "/table-based-SF/data/5YRData/acsdt5y2024-{table}.dat"
)
CT_CROSSWALK_URL = (
    "https://raw.githubusercontent.com/CT-Data-Collaborative/2022-block-crosswalk"
    "/main/2022blockcrosswalk.csv"
)
CT_CROSSWALK_FILENAME = "2022blockcrosswalk.csv"
CT_CROSSWALK_TABLE = "census_ct_block_to_planning_region_2022"
UTF8_BOM = b"\xef\xbb\xbf"
STATEMENT_POLL_SECONDS = 5
STATEMENT_TIMEOUT_SECONDS = 900


@dataclass(frozen=True)
class FileFacts:
    """What the manifest records (and verification recomputes) per downloaded file."""

    filename: str
    url: str
    sha256: str
    size_bytes: int
    data_rows: int


class StatementOutcomeUnknownError(RuntimeError):
    """The statement's server-side outcome is unknown: the client timed out or
    lost contact after submission. It may still complete; re-check actual table
    state before classifying or re-running."""

    def __init__(self, statement_id: str, message: str):
        super().__init__(f"{message} (statement_id {statement_id})")
        self.statement_id = statement_id


@dataclass(frozen=True)
class PublishOutcome:
    """Publication report, truthful about mutation. `verified` tables were
    created/replaced AND count-verified. `mutated_unverified` names a table
    whose create statement succeeded but whose count check failed or could not
    complete: the table WAS changed and must be inspected before it is trusted.
    `failed` names a table where nothing was mutated (upload or re-hash error,
    or a terminal FAILED statement). `unknown` names a table whose create
    statement outcome itself is unknown (contact lost mid-flight)."""

    verified: tuple[str, ...]
    failed: str | None = None
    mutated_unverified: str | None = None
    unknown: str | None = None
    reason: str | None = None
    remaining: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return self.failed is None and self.unknown is None and self.mutated_unverified is None


def acs_url(table: str) -> str:
    return ACS_URL_TEMPLATE.format(table=table)


def acs_filename(table: str) -> str:
    return f"acsdt5y2024-{table}.dat"


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def stream_download(url: str, dest: Path, strip_bom: bool = False) -> FileFacts:
    """Stream url to dest; return sha256/size/data-row facts over the STORED bytes.

    data_rows excludes the header line. A missing trailing newline still counts
    the final line. strip_bom removes a leading UTF-8 BOM so the stored file is
    exactly what read_files parses (and what the recorded sha256 covers).
    """
    digest = hashlib.sha256()
    size = 0
    newlines = 0
    last_byte = b"\n"
    first_chunk = True
    with urllib.request.urlopen(url) as response, open(dest, "wb") as out:
        while chunk := response.read(1024 * 1024):
            if first_chunk:
                if strip_bom and chunk.startswith(UTF8_BOM):
                    chunk = chunk[len(UTF8_BOM) :]
                first_chunk = False
            digest.update(chunk)
            out.write(chunk)
            size += len(chunk)
            newlines += chunk.count(b"\n")
            last_byte = chunk[-1:]
    lines = newlines + (0 if last_byte == b"\n" else 1)
    return FileFacts(
        filename=dest.name,
        url=url,
        sha256=digest.hexdigest(),
        size_bytes=size,
        data_rows=max(lines - 1, 0),
    )


def build_manifest(facts: list[FileFacts], retrieved_on: str) -> dict:
    return {
        "vintage": VINTAGE,
        "retrieved_on": retrieved_on,
        "generated_by": "dbt/scripts/load_acs_summary_file.py --write-manifest --verify-only",
        "files": {f.filename: {k: v for k, v in asdict(f).items() if k != "filename"} for f in facts},
    }


def verify_against_manifest(manifest: dict, facts: list[FileFacts]) -> list[str]:
    """Return human-readable mismatch strings; empty list means verified. The
    manifest must carry the pinned vintage: this loader publishes only the
    2020-2024 extract, so a foreign or mixed manifest is rejected outright."""
    problems: list[str] = []
    if manifest.get("vintage") != VINTAGE:
        problems.append(f"manifest vintage {manifest.get('vintage')!r} is not the pinned {VINTAGE!r}")
    files = manifest.get("files", {})
    for f in facts:
        expected = files.get(f.filename)
        if expected is None:
            problems.append(f"{f.filename}: not in the manifest")
            continue
        if expected["sha256"] != f.sha256:
            problems.append(f"{f.filename}: sha256 mismatch (manifest {expected['sha256']}, got {f.sha256})")
        if expected["data_rows"] != f.data_rows:
            problems.append(
                f"{f.filename}: rows mismatch (manifest {expected['data_rows']}, got {f.data_rows})"
            )
    return problems


def decide_action(write_manifest: bool, verify_only: bool, manifest_exists: bool, problems: list[str]) -> str:
    """The one gate deciding what a run may do. Bootstrap (--write-manifest)
    records facts and can never publish; nothing publishes without a committed
    manifest and a clean verification against it."""
    if write_manifest:
        return "write_manifest"
    if not manifest_exists:
        return "abort_no_manifest"
    if problems:
        return "abort_mismatch"
    if verify_only:
        return "verified_no_publish"
    return "publish"


def create_acs_table_sql(
    catalog: str, schema: str, table: str, volume: str, filename: str, replace: bool = False
) -> str:
    # plain CREATE TABLE by default so the warehouse itself enforces
    # fail-if-exists; OR REPLACE only for an explicit --allow-replace run
    verb = "create or replace table" if replace else "create table"
    path = f"/Volumes/{catalog}/{schema}/{volume}/{filename}"
    return (
        f"{verb} {catalog}.{schema}.{table}"
        f" comment 'Raw all-string load of {filename} from the Census ACS table-based 5-year"
        f" summary file; loaded by dbt/scripts/load_acs_summary_file.py; casting and jam-code"
        f" handling happen in dbt staging.'"
        f" as select * except (_rescued_data)"
        f" from read_files('{path}',"
        f" format => 'csv', sep => '|', header => true, inferSchema => false, mode => 'FAILFAST')"
    )


def create_crosswalk_sql(
    catalog: str, schema: str, table: str, volume: str, filename: str, replace: bool = False
) -> str:
    # Same all-string pattern as the ACS tables, with one load-time exception:
    # the source header's "block number" column is renamed because Delta
    # rejects spaces in column names.
    verb = "create or replace table" if replace else "create table"
    path = f"/Volumes/{catalog}/{schema}/{volume}/{filename}"
    return (
        f"{verb} {catalog}.{schema}.{table}"
        f" comment 'State-republished (CTData) copy of the Census 2022 Connecticut"
        f" county-equivalent-change block-level equivalency mapping (2020 block to"
        f" planning-region-coded block); one row per 2020 CT block; loaded by"
        f" dbt/scripts/load_acs_summary_file.py.'"
        f" as select * except (`block number`, _rescued_data), `block number` as block_number"
        f" from read_files('{path}',"
        f" format => 'csv', sep => ',', header => true, inferSchema => false, mode => 'FAILFAST')"
    )


def _clean_env() -> dict[str, str]:
    env = dict(os.environ)
    env.pop("DATABRICKS_TOKEN", None)
    return env


def run_databricks(args: list[str], profile: str) -> str:
    cmd = ["databricks", *args, "-p", profile]
    result = subprocess.run(cmd, capture_output=True, text=True, env=_clean_env(), check=False)
    if result.returncode != 0:
        raise RuntimeError(f"databricks {' '.join(args[:2])} failed: {result.stderr.strip()}")
    return result.stdout


def execute_sql(statement: str, warehouse_id: str, profile: str) -> list[list[str]]:
    """Submit a statement and poll to a terminal state. Only a terminal FAILED
    state is a plain failure; ANY communication problem once submission has
    been attempted (submit response unreadable, polling error, CLI failure,
    network drop, poll timeout) raises StatementOutcomeUnknownError, because
    the server-side statement may still be running or may have completed."""
    payload = json.dumps(
        {"statement": statement, "warehouse_id": warehouse_id, "wait_timeout": "50s", "disposition": "INLINE"}
    )
    statement_id = "unknown"
    try:
        response = json.loads(
            run_databricks(["api", "post", "/api/2.0/sql/statements", "--json", payload], profile)
        )
        statement_id = response["statement_id"]
        deadline = time.monotonic() + STATEMENT_TIMEOUT_SECONDS
        while response["status"]["state"] in ("PENDING", "RUNNING"):
            if time.monotonic() > deadline:
                raise StatementOutcomeUnknownError(
                    statement_id,
                    f"client stopped polling after {STATEMENT_TIMEOUT_SECONDS}s: {statement[:120]}",
                )
            time.sleep(STATEMENT_POLL_SECONDS)
            response = json.loads(
                run_databricks(["api", "get", f"/api/2.0/sql/statements/{statement_id}"], profile)
            )
    except StatementOutcomeUnknownError:
        raise
    except Exception as exc:
        raise StatementOutcomeUnknownError(
            statement_id, f"lost contact after submission was attempted: {exc}"
        ) from exc
    if response["status"]["state"] != "SUCCEEDED":
        error = response["status"].get("error", {}).get("message", response["status"]["state"])
        raise RuntimeError(f"statement failed ({error}): {statement[:120]}")
    return (response.get("result") or {}).get("data_array") or []


def preflight_existing_targets(
    tables: list[str], catalog: str, schema: str, run_sql: Callable[[str], list[list[str]]]
) -> dict[str, int]:
    """Return {table: current Delta version} for publish targets that already
    exist. Default load mode refuses to touch them; --allow-replace records
    these versions first as the concrete RESTORE targets for rollback."""
    names = ", ".join(f"'{t}'" for t in tables)
    rows = run_sql(
        f"select table_name from {catalog}.information_schema.tables"
        f" where table_schema = '{schema}' and table_name in ({names})"
    )
    existing: dict[str, int] = {}
    for row in rows:
        table = row[0]
        history = run_sql(f"describe history {catalog}.{schema}.{table} limit 1")
        existing[table] = int(history[0][0])
    return existing


def upload_file(local: Path, catalog: str, schema: str, volume: str, profile: str) -> None:
    target = f"dbfs:/Volumes/{catalog}/{schema}/{volume}/{local.name}"
    run_databricks(["fs", "cp", str(local), target, "--overwrite"], profile)


def publish_tables(
    facts: list[FileFacts],
    targets: dict[str, str],
    catalog: str,
    schema: str,
    volume: str,
    run_sql: Callable[[str], list[list[str]]],
    upload: Callable[[FileFacts], None],
    replace_versions: dict[str, int] | None = None,
) -> PublishOutcome:
    """Publish one table at a time: upload its file, CREATE it (CREATE OR
    REPLACE for a table in replace_versions), then count-verify immediately.
    Stops at the first problem so tables that verification has not cleared are
    never touched. Bookkeeping is truthful about mutation: a table counts as
    mutated the moment its create statement succeeds, so a count-check problem
    reports MUTATED BUT UNVERIFIED rather than pretending nothing happened; a
    post-submission communication problem during the create itself is an
    UNKNOWN outcome, never a plain failure."""
    replace_versions = replace_versions or {}
    verified: list[str] = []
    order = [targets[f.filename] for f in facts]
    for i, f in enumerate(facts):
        table = targets[f.filename]
        remaining = tuple(order[i + 1 :])
        # phase 1, pre-mutation: upload (with the caller's re-hash guard)
        try:
            upload(f)
        except Exception as exc:
            return PublishOutcome(
                verified=tuple(verified),
                failed=table,
                reason=f"failed before any mutation: {exc}",
                remaining=remaining,
            )
        # phase 2: the mutation
        replace = table in replace_versions
        sql = (
            create_crosswalk_sql(catalog, schema, table, volume, f.filename, replace=replace)
            if table == CT_CROSSWALK_TABLE
            else create_acs_table_sql(catalog, schema, table, volume, f.filename, replace=replace)
        )
        try:
            run_sql(sql)
        except StatementOutcomeUnknownError as exc:
            return PublishOutcome(
                verified=tuple(verified), unknown=table, reason=str(exc), remaining=remaining
            )
        except Exception as exc:
            return PublishOutcome(
                verified=tuple(verified),
                failed=table,
                reason=f"create statement failed; nothing was mutated: {exc}",
                remaining=remaining,
            )
        # phase 3, verification: the table IS mutated from here on
        try:
            rows = int(run_sql(f"select count(*) from {catalog}.{schema}.{table}")[0][0])
        except Exception as exc:
            return PublishOutcome(
                verified=tuple(verified),
                mutated_unverified=table,
                reason=f"the table was mutated but its count check did not complete: {exc}",
                remaining=remaining,
            )
        if rows != f.data_rows:
            return PublishOutcome(
                verified=tuple(verified),
                mutated_unverified=table,
                reason=(
                    f"the table was mutated but is unverified: staged {rows:,} rows"
                    f" vs downloaded {f.data_rows:,}"
                ),
                remaining=remaining,
            )
        verified.append(table)
    return PublishOutcome(verified=tuple(verified))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--profile", default="dbc-3d8ca484-79f3")
    parser.add_argument("--warehouse-id", default="18583d8b081c6486")
    parser.add_argument("--catalog", default="goodparty_data_catalog")
    parser.add_argument("--schema", default="sandbox")
    parser.add_argument("--volume", default="census_acs_raw")
    parser.add_argument("--manifest", type=Path, default=Path(__file__).parent / "acs5y2024_manifest.json")
    parser.add_argument(
        "--write-manifest",
        action="store_true",
        help="bootstrap: record download facts; NEVER writes to the warehouse",
    )
    parser.add_argument("--verify-only", action="store_true", help="download and verify; no warehouse writes")
    parser.add_argument(
        "--allow-replace",
        action="store_true",
        help="permit replacing publish targets that already exist (their current Delta versions are recorded first)",
    )
    parser.add_argument(
        "--tables", default="", help="comma-separated subset: ACS table tokens and/or 'crosswalk'"
    )
    parser.add_argument("--download-dir", type=Path, default=None)
    args = parser.parse_args(argv)

    # process-local; subprocesses (the databricks CLI) are unaffected
    socket.getaddrinfo = _ipv4_first_getaddrinfo

    if args.download_dir:
        args.download_dir.mkdir(parents=True, exist_ok=True)
        return _run(args, args.download_dir)
    # default downloads are cleaned up on exit; pass --download-dir to keep them
    with tempfile.TemporaryDirectory(prefix="acs_summary_file_") as tmp:
        return _run(args, Path(tmp))


def _run(args: argparse.Namespace, download_dir: Path) -> int:
    subset = {t.strip().lower() for t in args.tables.split(",") if t.strip()}
    unknown_tokens = subset - set(ACS_TABLES) - {"crosswalk"}
    if unknown_tokens:
        print(f"FATAL: unknown --tables entries: {sorted(unknown_tokens)}", file=sys.stderr)
        return 2
    tables = [t for t in ACS_TABLES if not subset or t in subset]
    include_crosswalk = not subset or "crosswalk" in subset

    print(f"downloading {len(tables)} {VINTAGE} file(s) + crosswalk={include_crosswalk} -> {download_dir}")

    plan: list[tuple[str, str, bool, str]] = [
        (acs_url(t), acs_filename(t), False, f"acs5y2024_{t}") for t in tables
    ]
    if include_crosswalk:
        plan.append((CT_CROSSWALK_URL, CT_CROSSWALK_FILENAME, True, CT_CROSSWALK_TABLE))

    facts: list[FileFacts] = []
    targets: dict[str, str] = {}
    for url, filename, strip_bom, table in plan:
        print(f"  {filename} ...", flush=True)
        f = stream_download(url, download_dir / filename, strip_bom=strip_bom)
        facts.append(f)
        targets[f.filename] = table
        print(f"    {f.size_bytes:,} bytes, {f.data_rows:,} data rows, sha256 {f.sha256[:12]}...")

    manifest_exists = args.manifest.exists()
    problems: list[str] = []
    if not args.write_manifest and manifest_exists:
        problems = verify_against_manifest(json.loads(args.manifest.read_text()), facts)
    action = decide_action(args.write_manifest, args.verify_only, manifest_exists, problems)

    if action == "write_manifest":
        manifest = build_manifest(facts, date.today().isoformat())
        if manifest_exists:  # additive subset re-run keeps other entries, same vintage only
            existing_manifest = json.loads(args.manifest.read_text())
            if existing_manifest.get("vintage") != VINTAGE:
                print(
                    f"FATAL: existing manifest vintage {existing_manifest.get('vintage')!r} is not"
                    f" the pinned {VINTAGE!r}; refusing to merge",
                    file=sys.stderr,
                )
                return 2
            merged = dict(existing_manifest["files"])
            merged.update(manifest["files"])
            manifest["files"] = dict(sorted(merged.items()))
        args.manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        print(f"manifest written: {args.manifest}")
        print("bootstrap never writes to the warehouse: inspect the manifest, commit it, then run a load")
        return 0
    if action == "abort_no_manifest":
        print(
            "FATAL: no committed manifest; bootstrap first with --write-manifest --verify-only",
            file=sys.stderr,
        )
        return 2
    if action == "abort_mismatch":
        print("FATAL: manifest verification failed; NO table was touched:", file=sys.stderr)
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        return 2
    if action == "verified_no_publish":
        print("verify-only: all downloads match the committed manifest; no warehouse writes")
        return 0

    # action == "publish": every downloaded file above verified against the COMMITTED manifest
    def run_sql(statement: str) -> list[list[str]]:
        return execute_sql(statement, args.warehouse_id, args.profile)

    def upload_after_rehash(ff: FileFacts) -> None:
        # bind the uploaded bytes to the verification: the manifest-verified
        # hash must still describe the on-disk file at the moment of upload
        local = download_dir / ff.filename
        digest = sha256_of(local)
        if digest != ff.sha256:
            raise RuntimeError(
                f"{ff.filename}: on-disk bytes changed since verification"
                f" (sha256 {digest[:12]}... != verified {ff.sha256[:12]}...)"
            )
        upload_file(local, args.catalog, args.schema, args.volume, args.profile)

    fq_volume = f"{args.catalog}.{args.schema}.{args.volume}"
    run_sql(
        f"create volume if not exists {fq_volume}"
        f" comment 'Raw Census ACS table-based summary-file downloads; see"
        f" dbt/scripts/load_acs_summary_file.py'"
    )
    existing = preflight_existing_targets(
        [targets[f.filename] for f in facts], args.catalog, args.schema, run_sql
    )
    if existing and not args.allow_replace:
        print("FATAL: publish targets already exist and --allow-replace was not given:", file=sys.stderr)
        for table, version in sorted(existing.items()):
            print(
                f"  {args.catalog}.{args.schema}.{table} (current Delta version {version})", file=sys.stderr
            )
        print(
            "  re-run with --allow-replace to replace them; the versions above are the RESTORE targets",
            file=sys.stderr,
        )
        return 5
    for table, version in sorted(existing.items()):
        print(
            f"  will replace {args.catalog}.{args.schema}.{table}: pre-replacement Delta version {version}"
            f" (rollback: RESTORE TABLE {args.catalog}.{args.schema}.{table} TO VERSION AS OF {version})"
        )
    outcome = publish_tables(
        facts,
        targets,
        args.catalog,
        args.schema,
        args.volume,
        run_sql=run_sql,
        upload=upload_after_rehash,
        replace_versions=existing,
    )
    for table in outcome.verified:
        mode = "replaced" if table in existing else "created"
        print(f"  {args.catalog}.{args.schema}.{table}: {mode} and count-verified")
    if not outcome.ok:
        print("FATAL: publication stopped early", file=sys.stderr)
        print(f"  mutated and verified: {list(outcome.verified) or 'none'}", file=sys.stderr)
        if outcome.failed:
            print(f"  failed with nothing mutated: {outcome.failed} ({outcome.reason})", file=sys.stderr)
        if outcome.mutated_unverified:
            print(
                f"  MUTATED BUT UNVERIFIED: {outcome.mutated_unverified} ({outcome.reason})",
                file=sys.stderr,
            )
        if outcome.unknown:
            print(
                f"  unknown outcome: {outcome.unknown} ({outcome.reason}); the server-side statement"
                " may still complete -- wait for it to reach a terminal state and re-check the table"
                " (count and DESCRIBE HISTORY) before re-running",
                file=sys.stderr,
            )
        print(f"  never attempted: {list(outcome.remaining) or 'none'}", file=sys.stderr)
        for table in [t for t in (outcome.mutated_unverified, outcome.unknown) if t]:
            fq_table = f"{args.catalog}.{args.schema}.{table}"
            if table in existing:
                print(
                    f"  recovery for {fq_table} (was REPLACED): RESTORE TABLE {fq_table}"
                    f" TO VERSION AS OF {existing[table]}",
                    file=sys.stderr,
                )
            else:
                print(
                    f"  recovery for {fq_table} (was CREATED by this run): inspect it, then"
                    " DROP TABLE it if it should not exist",
                    file=sys.stderr,
                )
        return 4 if outcome.unknown else 3
    print("load complete: all tables published and count-verified against the committed manifest")
    return 0


if __name__ == "__main__":
    sys.exit(main())
