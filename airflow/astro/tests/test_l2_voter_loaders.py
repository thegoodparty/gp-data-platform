"""Tests for the L2 SFTP -> S3 -> Databricks loader."""

import io
import zipfile
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from include.custom_functions import l2_voter_loaders
from include.custom_functions.l2_voter_loaders import (
    EXPIRED_DIR,
    EXPIRED_FOLDER,
    SOURCE_GROUPS,
    build_load_statement,
    get_table_loaded_at,
    list_remote_sources,
    load_table,
    plan_loads,
    plan_transfers,
    source_file_type,
    sync_source,
)

BASE = "VM2--MO--2026-08-03"
MODIFIED = datetime(2026, 8, 3, 20, 0, tzinfo=UTC)
MODIFIED_TS = MODIFIED.timestamp()
STAGED = MODIFIED + timedelta(minutes=30)

DEMOGRAPHIC_ROWS = b"LALVOTERID\tZip\nLALMO1\t01854\nLALMO2\t07001\n"
MEMBERS = [
    f"{BASE}-DEMOGRAPHIC.tab",
    f"{BASE}-DEMOGRAPHIC_DataDictionary.csv",
    f"{BASE}-VOTEHISTORY.tab",
    f"{BASE}-VOTEHISTORY_DataDictionary.csv",
]

# One real archive name per group. A group added without one fails the round-trip test below.
ARCHIVE_SAMPLES = {
    "VM2": f"{BASE}.zip",
    "VM2Uniform": "VM2Uniform--MO--2026-08-03.zip",
    "HaystaqFlags": "mo_haystaqdnaflags_20260520.tab.zip",
    "HaystaqScores": "mo_haystaqdnascores_20260520.tab.zip",
}


def _data_dictionary(footer_rows: int) -> bytes:
    preamble = "".join(f"preamble {n}\n" for n in range(15))
    body = "Field,Description\nLALVOTERID,Voter id\nZip,ZIP code\n"
    footer = "".join(f"legend {n}\n" for n in range(footer_rows))
    return (preamble + body + footer).encode()


@pytest.fixture
def vm2_archive() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(MEMBERS[0], DEMOGRAPHIC_ROWS)
        archive.writestr(MEMBERS[1], _data_dictionary(24))
        archive.writestr(f"{BASE}-DEMOGRAPHIC-FillRate.tab", b"ignored\n")
        archive.writestr(MEMBERS[2], b"LALVOTERID\tGeneral_2024\nLALMO1\tY\n")
        archive.writestr(MEMBERS[3], _data_dictionary(4))
    return buffer.getvalue()


class FakeAttributes:
    def __init__(self, filename, st_size=1024, st_mtime=MODIFIED_TS):
        self.filename = filename
        self.st_size = st_size
        self.st_mtime = st_mtime


class FakeSFTPClient:
    """Stands in for paramiko: download() copies the payload to the local path."""

    def __init__(
        self,
        payload: bytes = b"",
        listings: dict[str, list] | None = None,
        missing: set[str] | None = None,
    ):
        self.payload = payload
        self.listings = listings or {}
        self.missing = missing or set()

    def listdir_attr(self, remote_dir):
        if remote_dir in self.missing:
            # What paramiko raises for SFTP_NO_SUCH_FILE.
            raise FileNotFoundError(2, "No such file")
        return self.listings.get(remote_dir, [])

    def get(self, remotepath, localpath, **kwargs):
        with open(localpath, "wb") as handle:
            handle.write(self.payload)


class FakeS3Client:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body):
        self.objects[Key] = Body

    def upload_fileobj(self, fileobj, Bucket, Key, Config=None):
        self.objects[Key] = fileobj.read()


def _source(members=None, file_name=f"{BASE}.zip", folder="MO", size=1024, group="VM2"):
    return {
        "group": group,
        "folder": folder,
        "remote_path": f"/VMFiles/{file_name}",
        "members": members,
        "size_bytes": size,
        "modified_at": MODIFIED.isoformat(),
    }


class TestListRemoteSources:
    def _list(self, listings, missing=None):
        return list_remote_sources(FakeSFTPClient(listings=listings, missing=missing))

    def test_missing_expired_dir_does_not_block_the_archives(self):
        """The expired feed is auxiliary, so losing its directory must not cost us 51 states."""
        sources = self._list({"/VMFiles": [FakeAttributes("VM2--MO--2026-08-03.zip")]}, missing={EXPIRED_DIR})
        assert [s["folder"] for s in sources] == ["MO"]

    def test_missing_archive_dir_raises(self):
        """A fixed L2 directory disappearing is an alarm, not a quiet zero-work success."""
        with pytest.raises(FileNotFoundError):
            self._list({}, missing={"/VMFiles"})

    def test_both_archive_groups_and_the_expired_file(self):
        sources = self._list(
            {
                "/VM2Uniform": [FakeAttributes("VM2Uniform--MO--2026-08-03.zip")],
                "/VMFiles": [FakeAttributes("VM2--MO--2026-08-03.zip")],
                EXPIRED_DIR: [FakeAttributes("Manual_ID_Omits.tab")],
            }
        )

        assert sorted((s["folder"], len(s["members"])) for s in sources) == [
            ("EXPIRED", 1),
            ("MO", 2),
            ("MO", 4),
        ]

    def test_non_matching_names_are_ignored(self):
        """A state L2 has pulled, and its scratch files, simply do not appear."""
        assert (
            self._list(
                {
                    "/VM2Uniform": [FakeAttributes("VM2Uniform--MO--2026-08-03.zip.tmp")],
                    "/VMFiles": [FakeAttributes("readme.txt")],
                }
            )
            == []
        )

    def test_only_the_newest_publication_per_state_is_listed(self):
        """L2 leaves superseded archives in place, and only the newest ever becomes a table."""
        sources = self._list(
            {
                "/VMFiles": [
                    FakeAttributes("VM2--MO--2026-08-03.zip"),
                    FakeAttributes("VM2--MO--2026-07-27.zip"),
                ]
            }
        )
        assert [s["remote_path"] for s in sources] == ["/VMFiles/VM2--MO--2026-08-03.zip"]

    def test_missing_mtime_forces_a_transfer(self):
        sources = self._list({EXPIRED_DIR: [FakeAttributes("Manual_ID_Omits.tab", st_mtime=None)]})

        assert sources[0]["modified_at"] is None
        assert plan_transfers(sources, {f"{EXPIRED_FOLDER}/Manual_ID_Omits.tab": STAGED}) == sources


class TestArchiveGrammar:
    """SOURCE_GROUPS names 300-odd tables, so its patterns are a contract."""

    def test_members_derived_from_an_archive_classify_to_their_declared_type(self):
        """A member we stage but cannot classify is uploaded and then never loaded."""
        listings: dict[str, list] = {}
        for group, file_name in ARCHIVE_SAMPLES.items():
            listings.setdefault(SOURCE_GROUPS[group]["remote_dir"], []).append(FakeAttributes(file_name))
        sources = list_remote_sources(FakeSFTPClient(listings=listings))

        assert {source["group"] for source in sources} == set(SOURCE_GROUPS)
        for source in sources:
            # Haystaq spells the state lowercase; S3 and the table name want one spelling.
            assert source["folder"] == "MO"
            declared = list(SOURCE_GROUPS[source["group"]]["members"].values())
            assert [source_file_type(member) for member in source["members"]] == declared

    def test_fillrate_is_not_a_voter_file(self):
        assert source_file_type("VM2--AL--2025-05-10-DEMOGRAPHIC-FillRate.tab") is None


class TestPlanTransfers:
    """The diff is the whole idempotency story: no manifest, just S3 versus the SFTP listing."""

    def test_fully_staged_source_is_a_noop(self):
        staged = {f"MO/{member}": STAGED for member in MEMBERS}
        assert plan_transfers([_source(MEMBERS)], staged) == []

    def test_partially_staged_source_is_pending(self):
        staged = {f"MO/{member}": STAGED for member in MEMBERS[:-1]}
        assert plan_transfers([_source(MEMBERS)], staged) == [_source(MEMBERS)]
        assert plan_transfers([_source(MEMBERS)], {}) == [_source(MEMBERS)]

    def test_plain_file_is_its_own_member(self):
        """The expired file stages under its own name, so it diffs like any other source."""
        expired = _source(
            members=["Manual_ID_Omits.tab"], file_name="Manual_ID_Omits.tab", folder=EXPIRED_FOLDER
        )
        assert plan_transfers([expired], {f"{EXPIRED_FOLDER}/Manual_ID_Omits.tab": STAGED}) == []
        assert plan_transfers([expired], {}) == [expired]


class TestSyncSource:
    def _sync(self, payload, source, tmp_path):
        s3_client = FakeS3Client()
        return s3_client, sync_source(
            sftp_client=FakeSFTPClient(payload),
            s3_client=s3_client,
            bucket="bucket",
            prefix="staging/prod",
            source=source,
            staging_dir=str(tmp_path),
        )

    def test_uploads_expected_members_and_trims_the_dictionaries(self, vm2_archive, tmp_path):
        """FillRate is in the zip and must not be staged; the dictionaries carry L2's legend."""
        s3_client, keys = self._sync(vm2_archive, _source(MEMBERS), tmp_path)

        assert keys == [f"staging/prod/MO/{member}" for member in MEMBERS]
        assert s3_client.objects[f"staging/prod/MO/{MEMBERS[0]}"] == DEMOGRAPHIC_ROWS
        for member in (MEMBERS[1], MEMBERS[3]):
            written = s3_client.objects[f"staging/prod/MO/{member}"].decode()
            assert written.splitlines() == ["Field,Description", "LALVOTERID,Voter id", "Zip,ZIP code"]

    def test_refuses_an_archive_larger_than_the_free_space(self, monkeypatch, tmp_path):
        """The precheck is what makes download-to-disk viable on a fixed 10 GiB worker."""
        monkeypatch.setattr(l2_voter_loaders.shutil, "disk_usage", lambda _: SimpleNamespace(free=1_000_000))
        with pytest.raises(ValueError, match="GB is free"):
            self._sync(b"", _source(MEMBERS, size=9_000_000_000), tmp_path)
        assert not list(tmp_path.iterdir())

    def test_unknown_size_skips_the_check_rather_than_blocking(self, vm2_archive, tmp_path):
        _, keys = self._sync(vm2_archive, _source(MEMBERS, size=0), tmp_path)
        assert len(keys) == len(MEMBERS)

    def test_plain_file_is_copied_as_is(self, tmp_path):
        source = _source(
            members=["Manual_ID_Omits.tab"], file_name="Manual_ID_Omits.tab", folder=EXPIRED_FOLDER
        )
        s3_client, keys = self._sync(DEMOGRAPHIC_ROWS, source, tmp_path)

        assert keys == [f"staging/prod/{EXPIRED_FOLDER}/Manual_ID_Omits.tab"]
        assert s3_client.objects[keys[0]] == DEMOGRAPHIC_ROWS


class TestPlanLoads:
    UNIFORM = "VM2Uniform--MO--2026-08-03.tab"

    def test_current_table_is_a_noop(self):
        assert (
            plan_loads({f"MO/{self.UNIFORM}": STAGED}, {"l2_s3_mo_uniform": STAGED + timedelta(hours=8)})
            == []
        )

    def test_table_older_than_the_staged_file_is_pending(self):
        assert plan_loads({f"MO/{self.UNIFORM}": STAGED}, {}) == [
            {"folder": "MO", "source_file_name": self.UNIFORM, "table_name": "l2_s3_mo_uniform"}
        ]

    def test_only_the_newest_snapshot_per_table_is_loaded(self):
        """S3 retains every dated snapshot; loading an older one would regress a table."""
        staged = {
            "MO/VM2Uniform--MO--2026-07-27.tab": STAGED - timedelta(days=7),
            f"MO/{self.UNIFORM}": STAGED,
        }
        assert [load["source_file_name"] for load in plan_loads(staged, {})] == [self.UNIFORM]

    def test_haystaq_and_expired_files_map_to_their_own_tables(self):
        """These table names are the contract with the dbt staging models that read them."""
        staged = {
            "MO/mo_haystaqdnascores_20260520.tab": STAGED,
            f"{EXPIRED_FOLDER}/Manual_ID_Omits.tab": STAGED,
        }
        assert [load["table_name"] for load in plan_loads(staged, {})] == [
            "l2_s3_expired_voters",
            "l2_s3_mo_haystaq_dna_scores",
        ]


class TestBuildLoadStatement:
    def _statement(self, source_file_name, table_name="t"):
        return build_load_statement(
            "cat",
            "schema",
            "bucket",
            "staging/prod",
            {"folder": "MO", "source_file_name": source_file_name, "table_name": table_name},
        )

    def test_tab_and_csv_get_their_own_delimiter(self):
        assert self._statement(MEMBERS[0])[1]["delimiter"] == "\t"
        assert self._statement(MEMBERS[1])[1]["delimiter"] == ","

    def test_reads_every_column_as_a_string(self):
        """Leading zeros in ZIPs and similar codes only survive with inference off."""
        sql, _ = self._statement(MEMBERS[0])
        assert "inferColumnTypes => false" in sql
        # read_files appends a rescued-data column the source tables have never carried.
        assert "EXCEPT (_rescued_data)" in sql

    def test_path_is_bound_not_interpolated(self):
        """A staged name reaches Databricks as data, so it cannot alter the statement."""
        sql, parameters = self._statement("evil'; drop table x; --.tab")
        assert "drop table" not in sql
        assert parameters["path"] == "s3://bucket/staging/prod/MO/evil'; drop table x; --.tab"

    def test_rejects_an_unsafe_identifier(self):
        """Identifiers cannot be bound in DDL, so they are still validated."""
        with pytest.raises(ValueError, match="Unsafe table name"):
            self._statement(MEMBERS[0], table_name="t`; drop table x; --")


class FakeCursor:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.executed = []

    def execute(self, query, parameters=None):
        self.executed.append((query, parameters))

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class FakeConnection:
    def __init__(self, rows=()):
        self.cursor_obj = FakeCursor(rows)

    def cursor(self):
        return self.cursor_obj


class TestGetTableLoadedAt:
    def test_naive_timestamps_become_utc(self):
        """The connector returns naive datetimes; comparing them to S3's aware ones would raise."""
        connection = FakeConnection([("l2_s3_mo_uniform", datetime(2026, 8, 3, 20, 0))])
        loaded_at = get_table_loaded_at(connection, "cat", "schema")

        assert loaded_at["l2_s3_mo_uniform"].tzinfo is UTC
        # The value must survive a comparison against an aware S3 timestamp.
        assert loaded_at["l2_s3_mo_uniform"] < STAGED


class TestLoadTable:
    def test_returns_the_fully_qualified_name_and_binds_the_path(self):
        connection = FakeConnection()
        load = {"folder": "MO", "source_file_name": MEMBERS[0], "table_name": "l2_s3_mo_demographic"}

        name = load_table(connection, "cat", "schema", "bucket", "staging/prod", load)

        assert name == "cat.schema.l2_s3_mo_demographic"
        query, parameters = connection.cursor_obj.executed[0]
        assert "CREATE OR REPLACE TABLE `cat`.`schema`.`l2_s3_mo_demographic`" in query
        assert parameters["path"].endswith(MEMBERS[0])
