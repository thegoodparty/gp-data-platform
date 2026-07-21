"""One-time, manifest-verified loader for the Census ACS 2020-2024 5-year table-based Summary File.

Downloads the locked table inventory (plus the Connecticut block-to-planning-region
crosswalk) from the public bulk directories, verifies every file's sha256 and data
row count against the committed manifest BEFORE touching any warehouse table, then
uploads the raw files to a Unity Catalog volume and CREATE OR REPLACEs one
all-string table per file via read_files. Casting, renaming, and jam-code handling
happen downstream in dbt staging, per repo convention. One deliberate load-time
exception: the crosswalk CSV's "block number" header is renamed block_number in the
CTAS select list because Delta rejects spaces in column names, and its UTF-8 BOM is
stripped before hashing so the manifest hash covers exactly the bytes read_files
parses.

Refresh story: this is a one-time documented load for the 2020-2024 vintage, not a
recurring job. For a future vintage, bootstrap with --year <YYYY> --write-manifest
--verify-only, review and commit the regenerated manifest beside this script, then
run the load. Adding a table to ACS_TABLES and re-running with --tables <table>
(bootstrap first, then load) is an idempotent, additive re-run.

Failure handling: publication replaces ONE table at a time and count-verifies it
immediately; the first failure stops the run and reports which tables were
replaced and verified, which one failed or has an unknown outcome (client lost
contact after submission: the server-side statement may still complete, so wait
for it to reach a terminal state and re-check the table before re-running), and
which were never attempted. Rollback for any replaced table is Delta time travel:
RESTORE TABLE <catalog>.<schema>.<table> TO VERSION AS OF <version>
(find the prior version with DESCRIBE HISTORY). Publication preflights all
target names first: the default load mode FAILS if any target already exists;
--allow-replace permits a re-run and records every existing table's current
Delta version up front as the concrete RESTORE target. These are new sandbox
tables with no consumers, so verify-first ordering plus the documented restore
path is the deliberate, proportionate publication story.

Credentials: the bulk download path needs none (public HTTPS). Databricks access
uses the CLI OAuth profile; the script strips DATABRICKS_TOKEN from subprocess
environments so the profile always wins. No Census API key is used, logged, or
stored by this script; keep it that way.

Usage (from the dbt/ directory):
    uv run python scripts/load_acs_summary_file.py --write-manifest --verify-only
        # bootstrap or new vintage: record facts; NEVER writes to the warehouse
    uv run python scripts/load_acs_summary_file.py
        # load: verify every download against the COMMITTED manifest, then
        # publish tables one at a time (upload, replace, count-verify)
    uv run python scripts/load_acs_summary_file.py --verify-only
        # re-verify downloads against the committed manifest; no warehouse writes
    uv run python scripts/load_acs_summary_file.py --tables b19001
        # subset re-run (same verify-before-replace rules)
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
    "https://www2.census.gov/programs-surveys/acs/summary_file/{year}"
    "/table-based-SF/data/5YRData/acsdt5y{year}-{table}.dat"
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
    """Publication report: what was replaced and count-verified, what failed or
    has an unknown outcome, and what was never attempted."""

    replaced: tuple[str, ...]
    failed: str | None = None
    reason: str | None = None
    unknown: str | None = None
    remaining: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return self.failed is None and self.unknown is None


def acs_url(year: int, table: str) -> str:
    return ACS_URL_TEMPLATE.format(year=year, table=table)


def acs_filename(year: int, table: str) -> str:
    return f"acsdt5y{year}-{table}.dat"


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


def build_manifest(facts: list[FileFacts], vintage: str, retrieved_on: str) -> dict:
    return {
        "vintage": vintage,
        "retrieved_on": retrieved_on,
        "generated_by": "dbt/scripts/load_acs_summary_file.py --write-manifest --verify-only",
        "files": {f.filename: {k: v for k, v in asdict(f).items() if k != "filename"} for f in facts},
    }


def verify_against_manifest(manifest: dict, facts: list[FileFacts]) -> list[str]:
    """Return human-readable mismatch strings; empty list means verified."""
    problems: list[str] = []
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


def create_acs_table_sql(catalog: str, schema: str, table: str, volume: str, filename: str) -> str:
    path = f"/Volumes/{catalog}/{schema}/{volume}/{filename}"
    return (
        f"create or replace table {catalog}.{schema}.{table}"
        f" comment 'Raw all-string load of {filename} from the Census ACS table-based 5-year"
        f" summary file; loaded by dbt/scripts/load_acs_summary_file.py; casting and jam-code"
        f" handling happen in dbt staging.'"
        f" as select * except (_rescued_data)"
        f" from read_files('{path}',"
        f" format => 'csv', sep => '|', header => true, inferSchema => false, mode => 'FAILFAST')"
    )


def create_crosswalk_sql(catalog: str, schema: str, table: str, volume: str, filename: str) -> str:
    # Same all-string pattern as the ACS tables, with one load-time exception:
    # the source header's "block number" column is renamed because Delta
    # rejects spaces in column names.
    path = f"/Volumes/{catalog}/{schema}/{volume}/{filename}"
    return (
        f"create or replace table {catalog}.{schema}.{table}"
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
    these versions first as the concrete RESTORE targets."""
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
) -> PublishOutcome:
    """Publish one table at a time: upload its file, CREATE OR REPLACE, then
    count-verify immediately. Stops at the first failure so tables that
    verification has not cleared are never touched. Any post-submission
    communication problem is an UNKNOWN outcome (the server-side statement may
    still complete), never a plain failure."""
    replaced: list[str] = []
    order = [targets[f.filename] for f in facts]
    for i, f in enumerate(facts):
        table = targets[f.filename]
        remaining = tuple(order[i + 1 :])
        sql = (
            create_crosswalk_sql(catalog, schema, table, volume, f.filename)
            if table == CT_CROSSWALK_TABLE
            else create_acs_table_sql(catalog, schema, table, volume, f.filename)
        )
        try:
            upload(f)
            run_sql(sql)
            rows = int(run_sql(f"select count(*) from {catalog}.{schema}.{table}")[0][0])
        except StatementOutcomeUnknownError as exc:
            return PublishOutcome(
                replaced=tuple(replaced), unknown=table, reason=str(exc), remaining=remaining
            )
        except Exception as exc:
            return PublishOutcome(
                replaced=tuple(replaced),
                failed=table,
                reason=f"replacement failed before verification: {exc}",
                remaining=remaining,
            )
        if rows != f.data_rows:
            return PublishOutcome(
                replaced=tuple(replaced),
                failed=table,
                reason=(
                    f"staged {rows:,} rows vs downloaded {f.data_rows:,}: "
                    "the table was replaced with unverified data"
                ),
                remaining=remaining,
            )
        replaced.append(table)
    return PublishOutcome(replaced=tuple(replaced))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--year", type=int, default=2024, help="final year of the 5-year vintage")
    parser.add_argument("--profile", default="dbc-3d8ca484-79f3")
    parser.add_argument("--warehouse-id", default="18583d8b081c6486")
    parser.add_argument("--catalog", default="goodparty_data_catalog")
    parser.add_argument("--schema", default="sandbox")
    parser.add_argument("--volume", default="census_acs_raw")
    parser.add_argument("--manifest", type=Path, default=Path(__file__).parent / "acs5y2024_manifest.json")
    parser.add_argument(
        "--write-manifest",
        action="store_true",
        help="bootstrap/new vintage: record download facts; NEVER writes to the warehouse",
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

    subset = {t.strip().lower() for t in args.tables.split(",") if t.strip()}
    unknown_tokens = subset - set(ACS_TABLES) - {"crosswalk"}
    if unknown_tokens:
        parser.error(f"unknown --tables entries: {sorted(unknown_tokens)}")
    tables = [t for t in ACS_TABLES if not subset or t in subset]
    include_crosswalk = not subset or "crosswalk" in subset

    download_dir = args.download_dir or Path(tempfile.mkdtemp(prefix="acs_summary_file_"))
    download_dir.mkdir(parents=True, exist_ok=True)
    print(f"downloading {len(tables)} ACS file(s) + crosswalk={include_crosswalk} -> {download_dir}")

    plan: list[tuple[str, str, bool, str]] = [
        (acs_url(args.year, t), acs_filename(args.year, t), False, f"acs5y{args.year}_{t}") for t in tables
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
        manifest = build_manifest(facts, f"{args.year - 4}-{args.year} ACS 5-year", date.today().isoformat())
        if manifest_exists:  # additive subset re-run keeps other entries
            merged = dict(json.loads(args.manifest.read_text())["files"])
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
        upload=lambda ff: upload_file(
            download_dir / ff.filename, args.catalog, args.schema, args.volume, args.profile
        ),
    )
    for table in outcome.replaced:
        print(f"  {args.catalog}.{args.schema}.{table}: replaced and count-verified")
    if not outcome.ok:
        print("FATAL: publication stopped early", file=sys.stderr)
        print(f"  replaced and verified: {list(outcome.replaced) or 'none'}", file=sys.stderr)
        if outcome.failed:
            print(f"  failed: {outcome.failed} ({outcome.reason})", file=sys.stderr)
        if outcome.unknown:
            print(
                f"  unknown outcome: {outcome.unknown} ({outcome.reason}); the server-side statement"
                " may still complete -- wait for it to reach a terminal state and re-check the table"
                " (count and DESCRIBE HISTORY) before re-running",
                file=sys.stderr,
            )
        print(f"  never attempted: {list(outcome.remaining) or 'none'}", file=sys.stderr)
        print(
            "  rollback for any replaced table: RESTORE TABLE"
            f" {args.catalog}.{args.schema}.<table> TO VERSION AS OF <prior version>"
            " (find it with DESCRIBE HISTORY)",
            file=sys.stderr,
        )
        return 4 if outcome.unknown else 3
    print("load complete: all tables replaced and count-verified against the committed manifest")
    return 0


if __name__ == "__main__":
    sys.exit(main())
