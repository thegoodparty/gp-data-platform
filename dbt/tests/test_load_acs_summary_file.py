"""Unit tests for the ACS summary-file loader's pure functions.

Network- and warehouse-touching paths are exercised by the documented live run,
not here; these tests pin the verify-before-replace contract, the SQL the
loader emits, and the publication-orchestration guarantees (bootstrap never
publishes; manifest mismatch, unknown statement outcomes, and mid-sequence
failures can never touch tables that verification has not cleared).
"""

import json
from pathlib import Path

from dbt.scripts.load_acs_summary_file import (
    ACS_TABLES,
    CT_CROSSWALK_FILENAME,
    FileFacts,
    StatementOutcomeUnknownError,
    acs_url,
    build_manifest,
    create_acs_table_sql,
    create_crosswalk_sql,
    decide_action,
    preflight_existing_targets,
    publish_tables,
    sha256_of,
    stream_download,
    verify_against_manifest,
)


def test_acs_table_inventory_is_locked():
    assert ACS_TABLES == (
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


def test_acs_url_template():
    assert acs_url(2024, "b01001") == (
        "https://www2.census.gov/programs-surveys/acs/summary_file/2024"
        "/table-based-SF/data/5YRData/acsdt5y2024-b01001.dat"
    )


def _download(tmp_path: Path, payload: bytes, name: str, strip_bom: bool = False) -> FileFacts:
    src = tmp_path / name
    src.write_bytes(payload)
    dest = tmp_path / f"out-{name}"
    return stream_download(src.as_uri(), dest, strip_bom=strip_bom)


def test_stream_download_counts_data_rows_with_trailing_newline(tmp_path):
    facts = _download(tmp_path, b"GEO_ID|X_E001|X_M001\na|1|2\nb|3|4\n", "t.dat")
    assert facts.data_rows == 2
    assert facts.size_bytes == 33


def test_stream_download_counts_data_rows_without_trailing_newline(tmp_path):
    facts = _download(tmp_path, b"GEO_ID|X_E001|X_M001\na|1|2\nb|3|4", "t.dat")
    assert facts.data_rows == 2


def test_stream_download_sha256_matches_stored_bytes(tmp_path):
    import hashlib

    payload = b"h1,h2\r\n1,2\n"
    facts = _download(tmp_path, payload, "t.csv")
    stored = (tmp_path / "out-t.csv").read_bytes()
    assert stored == payload
    assert facts.sha256 == hashlib.sha256(payload).hexdigest()


def test_stream_download_strips_utf8_bom_when_asked(tmp_path):
    facts = _download(tmp_path, b"\xef\xbb\xbfh1,h2\n1,2\n", "x.csv", strip_bom=True)
    stored = (tmp_path / "out-x.csv").read_bytes()
    assert stored.startswith(b"h1,h2")
    assert facts.data_rows == 1
    # the hash is over the stored (post-strip) bytes: what read_files will parse
    import hashlib

    assert facts.sha256 == hashlib.sha256(stored).hexdigest()


def _facts(name: str, sha: str = "abc", rows: int = 10) -> FileFacts:
    return FileFacts(
        filename=name, url=f"https://example.test/{name}", sha256=sha, size_bytes=1, data_rows=rows
    )


def test_verify_against_manifest_passes_on_exact_match():
    manifest = build_manifest([_facts("a.dat"), _facts("b.dat")], vintage="v", retrieved_on="2026-01-01")
    assert verify_against_manifest(manifest, [_facts("a.dat"), _facts("b.dat")]) == []


def test_verify_against_manifest_flags_sha_row_and_missing_mismatches():
    manifest = build_manifest([_facts("a.dat"), _facts("b.dat")], vintage="v", retrieved_on="2026-01-01")
    problems = verify_against_manifest(
        manifest, [_facts("a.dat", sha="different"), _facts("b.dat", rows=11), _facts("c.dat")]
    )
    assert len(problems) == 3
    assert any("a.dat" in p and "sha256" in p for p in problems)
    assert any("b.dat" in p and "rows" in p for p in problems)
    assert any("c.dat" in p and "not in the manifest" in p for p in problems)


def test_manifest_round_trips_through_json(tmp_path):
    manifest = build_manifest([_facts("a.dat")], vintage="v", retrieved_on="2026-01-01")
    p = tmp_path / "m.json"
    p.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    assert json.loads(p.read_text()) == manifest


def test_create_acs_table_sql_shape():
    sql = create_acs_table_sql(
        "cat", "sandbox", "acs5y2024_b01001", "census_acs_raw", "acsdt5y2024-b01001.dat"
    )
    assert sql.lower().startswith("create table cat.sandbox.acs5y2024_b01001")
    assert "or replace" not in sql.lower()  # first loads let the warehouse enforce fail-if-exists
    assert "'/Volumes/cat/sandbox/census_acs_raw/acsdt5y2024-b01001.dat'" in sql
    assert "sep => '|'" in sql
    assert "header => true" in sql
    assert "inferSchema => false" in sql
    assert "mode => 'FAILFAST'" in sql
    assert "except (_rescued_data)" in sql.lower()


def test_create_table_sql_replace_mode_uses_or_replace():
    acs = create_acs_table_sql("cat", "sandbox", "t", "v", "f.dat", replace=True)
    crosswalk = create_crosswalk_sql("cat", "sandbox", "x", "v", "x.csv", replace=True)
    assert acs.lower().startswith("create or replace table cat.sandbox.t")
    assert crosswalk.lower().startswith("create or replace table cat.sandbox.x")


def test_create_crosswalk_sql_renames_space_column_and_uses_commas():
    sql = create_crosswalk_sql(
        "cat", "sandbox", "census_ct_block_to_planning_region_2022", "census_acs_raw", CT_CROSSWALK_FILENAME
    )
    assert sql.lower().startswith("create table cat.sandbox.census_ct_block_to_planning_region_2022")
    assert "sep => ','" in sql
    assert "except (`block number`, _rescued_data)" in sql.lower()
    assert "`block number` as block_number" in sql.lower()
    assert "mode => 'FAILFAST'" in sql


def test_sha256_of_matches_hashlib(tmp_path):
    import hashlib

    payload = b"some bytes\nacross lines\n"
    f = tmp_path / "x.dat"
    f.write_bytes(payload)
    assert sha256_of(f) == hashlib.sha256(payload).hexdigest()


# --- orchestration guarantees: nothing verification has not cleared is ever touched ---


def test_decide_action_bootstrap_never_publishes():
    # bootstrap ignores manifest state entirely and can never reach "publish"
    assert decide_action(True, False, False, []) == "write_manifest"
    assert decide_action(True, True, True, ["sha mismatch"]) == "write_manifest"


def test_decide_action_requires_a_committed_manifest():
    assert decide_action(False, False, False, []) == "abort_no_manifest"


def test_decide_action_mismatch_blocks_everything():
    assert decide_action(False, False, True, ["sha mismatch"]) == "abort_mismatch"
    assert decide_action(False, True, True, ["sha mismatch"]) == "abort_mismatch"


def test_decide_action_verify_only_never_publishes():
    assert decide_action(False, True, True, []) == "verified_no_publish"


def test_decide_action_publishes_only_after_clean_verification():
    assert decide_action(False, False, True, []) == "publish"


class _FakeSql:
    """Stands in for execute_sql: records statements, counts creates, and can
    fail, lose contact (during create or count), return a wrong staged count,
    or report pre-existing targets with a current Delta version."""

    def __init__(
        self,
        fail_on: int | None = None,
        unknown_on: int | None = None,
        bad_count_for: str | None = None,
        unknown_on_count_for: str | None = None,
        existing: dict[str, int] | None = None,
    ):
        self.fail_on = fail_on
        self.unknown_on = unknown_on
        self.bad_count_for = bad_count_for
        self.unknown_on_count_for = unknown_on_count_for
        self.existing = existing or {}
        self.ctas_seen = 0
        self.statements: list[str] = []

    def __call__(self, sql: str) -> list[list[str]]:
        self.statements.append(sql)
        lowered = sql.lstrip().lower()
        if "information_schema.tables" in lowered:
            return [[t] for t in sorted(self.existing)]
        if lowered.startswith("describe history"):
            table = sql.rsplit(".", 1)[-1].split()[0]
            return [[str(self.existing[table]), "irrelevant"]]
        if lowered.startswith(("create table", "create or replace table")):
            self.ctas_seen += 1
            if self.ctas_seen == self.fail_on:
                raise RuntimeError("boom")
            if self.ctas_seen == self.unknown_on:
                raise StatementOutcomeUnknownError("stmt-1", "lost contact after submission was attempted")
            return []
        table = sql.rsplit(".", 1)[-1]  # "select count(*) from cat.sandbox.<table>"
        if table == self.unknown_on_count_for:
            raise StatementOutcomeUnknownError("stmt-2", "lost contact during count verification")
        return [["7" if table == self.bad_count_for else "10"]]


def test_preflight_reports_existing_tables_with_their_delta_versions():
    fake = _FakeSql(existing={"t_b": 7})
    assert preflight_existing_targets(["t_a", "t_b", "t_c"], "cat", "sandbox", fake) == {"t_b": 7}


def test_preflight_is_empty_when_no_targets_exist():
    assert preflight_existing_targets(["t_a", "t_b"], "cat", "sandbox", _FakeSql()) == {}


def _publish(fake: _FakeSql, replace_versions: dict[str, int] | None = None):
    facts = [_facts("a.dat"), _facts("b.dat"), _facts("c.dat")]
    targets = {"a.dat": "t_a", "b.dat": "t_b", "c.dat": "t_c"}
    uploaded: list[FileFacts] = []
    outcome = publish_tables(
        facts, targets, "cat", "sandbox", "vol", fake, uploaded.append, replace_versions=replace_versions
    )
    return outcome, uploaded


def test_publish_happy_path_creates_and_verifies_all():
    fake = _FakeSql()
    outcome, uploaded = _publish(fake)
    assert outcome.verified == ("t_a", "t_b", "t_c")
    assert outcome.ok
    assert len(uploaded) == 3
    creates = [s for s in fake.statements if s.lstrip().lower().startswith("create")]
    assert all(not s.lstrip().lower().startswith("create or replace") for s in creates)


def test_publish_uses_or_replace_only_for_recorded_replacements():
    fake = _FakeSql(existing={"t_b": 7})
    outcome, _ = _publish(fake, replace_versions={"t_b": 7})
    assert outcome.ok
    creates = [s.lstrip().lower() for s in fake.statements if s.lstrip().lower().startswith("create")]
    assert sum(s.startswith("create or replace table") for s in creates) == 1
    assert any("create or replace table cat.sandbox.t_b" in s for s in creates)


def test_publish_aborts_before_mutation_on_version_drift():
    # recorded version 7 at preflight; the table moved to 9 before publish
    fake = _FakeSql(existing={"t_b": 9})
    outcome, uploaded = _publish(fake, replace_versions={"t_b": 7})
    assert outcome.verified == ("t_a",)
    assert outcome.failed == "t_b"
    assert "drifted" in (outcome.reason or "")
    assert outcome.remaining == ("t_c",)
    assert fake.ctas_seen == 1  # only t_a's create ever ran
    assert len(uploaded) == 2  # t_b uploaded, then aborted pre-create; t_c untouched


def test_publish_nth_table_failure_stops_before_later_tables():
    fake = _FakeSql(fail_on=2)
    outcome, uploaded = _publish(fake)
    assert outcome.verified == ("t_a",)
    assert outcome.failed == "t_b"
    assert outcome.remaining == ("t_c",)
    assert fake.ctas_seen == 2  # t_c's create was never attempted
    assert len(uploaded) == 2  # and its file was never uploaded


def test_publish_post_submission_loss_is_unknown_not_failed_and_stops():
    fake = _FakeSql(unknown_on=1)
    outcome, uploaded = _publish(fake)
    assert outcome.verified == ()
    assert outcome.unknown == "t_a"
    assert outcome.failed is None
    assert outcome.remaining == ("t_b", "t_c")
    assert fake.ctas_seen == 1
    assert len(uploaded) == 1


def test_publish_count_mismatch_reports_mutated_but_unverified():
    fake = _FakeSql(bad_count_for="t_b")
    outcome, _ = _publish(fake)
    assert outcome.verified == ("t_a",)
    assert outcome.mutated_unverified == "t_b"
    assert outcome.failed is None
    assert "mutated but is unverified" in (outcome.reason or "")
    assert outcome.remaining == ("t_c",)


def test_publish_unknown_during_count_verification_is_mutated_but_unverified():
    # the create succeeded, so the mutation is certain even though the count
    # check lost contact: never classify this as if nothing happened
    fake = _FakeSql(unknown_on_count_for="t_b")
    outcome, _ = _publish(fake)
    assert outcome.verified == ("t_a",)
    assert outcome.mutated_unverified == "t_b"
    assert outcome.unknown is None
    assert "count check did not complete" in (outcome.reason or "")
    assert outcome.remaining == ("t_c",)


def test_ipv4_first_getaddrinfo_prefers_a_records(monkeypatch):
    import socket as socket_module

    import dbt.scripts.load_acs_summary_file as loader

    calls: list[int] = []

    def fake(host, port, family=0, *args, **kwargs):
        calls.append(family)
        return [("v4-result",)] if family == socket_module.AF_INET else [("unspec-result",)]

    monkeypatch.setattr(loader, "_SYSTEM_GETADDRINFO", fake)
    assert loader._ipv4_first_getaddrinfo("h", 443) == [("v4-result",)]
    assert calls == [socket_module.AF_INET]


def test_ipv4_first_getaddrinfo_falls_back_to_system_default(monkeypatch):
    import socket as socket_module

    import dbt.scripts.load_acs_summary_file as loader

    calls: list[int] = []

    def fake(host, port, family=0, *args, **kwargs):
        calls.append(family)
        if family == socket_module.AF_INET:
            raise socket_module.gaierror("no A records")
        return [("unspec-result",)]

    monkeypatch.setattr(loader, "_SYSTEM_GETADDRINFO", fake)
    assert loader._ipv4_first_getaddrinfo("h", 443) == [("unspec-result",)]
    assert calls == [socket_module.AF_INET, 0]
