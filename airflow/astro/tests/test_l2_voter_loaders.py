"""Tests for the L2 SFTP -> S3 -> Databricks loader."""

import io
import zipfile
from datetime import UTC, datetime, timedelta

import pytest
from include.custom_functions.l2_voter_loaders import (
    ARCHIVE_GROUPS,
    EXPIRED_FOLDER,
    build_load_statement,
    get_table_loaded_at,
    load_table,
    plan_loads,
    plan_transfers,
    source_file_type,
    sync_source,
)

BASE = "VM2--MO--2026-08-03"
MODIFIED = datetime(2026, 8, 3, 20, 0, tzinfo=UTC)
STAGED = MODIFIED + timedelta(minutes=30)

DEMOGRAPHIC_ROWS = b"LALVOTERID\tZip\nLALMO1\t01854\nLALMO2\t07001\n"
MEMBERS = [
    f"{BASE}-DEMOGRAPHIC.tab",
    f"{BASE}-DEMOGRAPHIC_DataDictionary.csv",
    f"{BASE}-VOTEHISTORY.tab",
    f"{BASE}-VOTEHISTORY_DataDictionary.csv",
]


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


class FakeSFTPClient:
    """Stands in for paramiko: download() copies the payload to the local path."""

    def __init__(self, payload: bytes):
        self.payload = payload

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


def _source(members=None, file_name=f"{BASE}.zip", folder="MO", size=1024):
    return {
        "folder": folder,
        "remote_path": f"/VMFiles/{file_name}",
        "members": members,
        "size_bytes": size,
        "modified_at": MODIFIED.isoformat(),
    }


class TestPlanTransfers:
    """The diff is the whole idempotency story: no manifest, just S3 versus the SFTP listing."""

    def test_fully_staged_source_is_a_noop(self):
        staged = {f"MO/{member}": STAGED for member in MEMBERS}
        assert plan_transfers([_source(MEMBERS)], staged) == []

    def test_unseen_source_is_pending(self):
        assert plan_transfers([_source(MEMBERS)], {}) == [_source(MEMBERS)]

    def test_partially_staged_source_is_pending(self):
        staged = {f"MO/{member}": STAGED for member in MEMBERS[:-1]}
        assert plan_transfers([_source(MEMBERS)], staged) == [_source(MEMBERS)]

    def test_plain_file_is_its_own_member(self):
        """The expired file stages under its own name, so it diffs like any other source."""
        expired = _source(
            members=["Manual_ID_Omits.tab"], file_name="Manual_ID_Omits.tab", folder=EXPIRED_FOLDER
        )
        assert plan_transfers([expired], {f"{EXPIRED_FOLDER}/Manual_ID_Omits.tab": STAGED}) == []
        assert plan_transfers([expired], {}) == [expired]

    def test_unknown_members_always_resync(self):
        """A zip whose contents we cannot name is re-synced rather than guessed from the folder."""
        unknown = _source(members=None, file_name="omits.zip", folder=EXPIRED_FOLDER)
        assert plan_transfers([unknown], {f"{EXPIRED_FOLDER}/anything.tab": STAGED}) == [unknown]


class TestSyncSource:
    def test_uploads_expected_members_and_skips_the_rest(self, vm2_archive, tmp_path):
        s3_client = FakeS3Client()
        keys = sync_source(
            sftp_client=FakeSFTPClient(vm2_archive),
            s3_client=s3_client,
            bucket="bucket",
            prefix="staging/prod",
            source=_source(MEMBERS),
            staging_dir=str(tmp_path),
        )

        assert keys == [f"staging/prod/MO/{member}" for member in MEMBERS]
        assert s3_client.objects[f"staging/prod/MO/{MEMBERS[0]}"] == DEMOGRAPHIC_ROWS

    def test_strips_dictionary_preamble_and_per_type_footer(self, vm2_archive, tmp_path):
        s3_client = FakeS3Client()
        sync_source(
            sftp_client=FakeSFTPClient(vm2_archive),
            s3_client=s3_client,
            bucket="bucket",
            prefix="staging/prod",
            source=_source(MEMBERS),
            staging_dir=str(tmp_path),
        )

        for member in (MEMBERS[1], MEMBERS[3]):
            written = s3_client.objects[f"staging/prod/MO/{member}"].decode()
            assert written.splitlines() == ["Field,Description", "LALVOTERID,Voter id", "Zip,ZIP code"]

    def test_archive_with_underivable_members_fails_before_downloading(self, tmp_path):
        """Each staged file becomes one table, so a source that expands to several must not load.

        The refusal precedes the download, so the daily retry costs nothing.
        """
        sftp_client = FakeSFTPClient(b"never read")
        source = _source(members=None, file_name="omits.zip", folder=EXPIRED_FOLDER)
        with pytest.raises(ValueError, match="cannot be derived from its name"):
            sync_source(
                sftp_client=sftp_client,
                s3_client=FakeS3Client(),
                bucket="bucket",
                prefix="staging/prod",
                source=source,
                staging_dir=str(tmp_path),
            )
        assert not list(tmp_path.iterdir())

    def test_plain_file_is_copied_as_is(self, tmp_path):
        s3_client = FakeS3Client()
        source = _source(
            members=["Manual_ID_Omits.tab"], file_name="Manual_ID_Omits.tab", folder=EXPIRED_FOLDER
        )
        keys = sync_source(
            sftp_client=FakeSFTPClient(DEMOGRAPHIC_ROWS),
            s3_client=s3_client,
            bucket="bucket",
            prefix="staging/prod",
            source=source,
            staging_dir=str(tmp_path),
        )

        assert keys == [f"staging/prod/{EXPIRED_FOLDER}/Manual_ID_Omits.tab"]
        assert s3_client.objects[keys[0]] == DEMOGRAPHIC_ROWS


class TestSourceFileType:
    @pytest.mark.parametrize(
        "source_file_name,expected",
        [
            ("VM2Uniform--AK--2025-05-10.tab", "uniform"),
            ("VM2Uniform--AK--2025-05-10_DataDictionary.csv", "uniform_data_dictionary"),
            ("VM2--NY--2025-05-10-DEMOGRAPHIC.tab", "demographic"),
            ("VM2--TX--2025-05-10-DEMOGRAPHIC_DataDictionary.csv", "demographic_data_dictionary"),
            ("VM2--AL--2025-05-10-VOTEHISTORY.tab", "vote_history"),
            ("VM2--CA--2025-05-10-VOTEHISTORY_DataDictionary.csv", "vote_history_data_dictionary"),
        ],
    )
    def test_type_drives_the_table_suffix(self, source_file_name, expected):
        """These six strings name 306 tables, so they are a contract."""
        assert source_file_type(source_file_name) == expected

    def test_fillrate_is_not_a_voter_file(self):
        assert source_file_type("VM2--AL--2025-05-10-DEMOGRAPHIC-FillRate.tab") is None

    def test_every_archive_member_is_classifiable(self):
        """A member we stage but cannot classify would be uploaded and then never loaded."""
        for name, spec in ARCHIVE_GROUPS.items():
            for suffix in spec["members"]:
                assert source_file_type(f"{name}--MO--2026-08-03{suffix}") is not None


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

    def test_expired_folder_maps_to_its_own_table(self):
        staged = {f"{EXPIRED_FOLDER}/Manual_ID_Omits.tab": STAGED}
        assert [load["table_name"] for load in plan_loads(staged, {})] == ["l2_s3_expired_voters"]


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

    def test_schema_is_bound(self):
        connection = FakeConnection()
        get_table_loaded_at(connection, "cat", "schema")

        query, parameters = connection.cursor_obj.executed[0]
        assert ":schema" in query
        assert parameters == {"schema": "schema"}


class TestLoadTable:
    def test_returns_the_fully_qualified_name_and_binds_the_path(self):
        connection = FakeConnection()
        load = {"folder": "MO", "source_file_name": MEMBERS[0], "table_name": "l2_s3_mo_demographic"}

        name = load_table(connection, "cat", "schema", "bucket", "staging/prod", load)

        assert name == "cat.schema.l2_s3_mo_demographic"
        query, parameters = connection.cursor_obj.executed[0]
        assert "CREATE OR REPLACE TABLE `cat`.`schema`.`l2_s3_mo_demographic`" in query
        assert parameters["path"].endswith(MEMBERS[0])
