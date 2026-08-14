"""Tests for the L2 SFTP -> S3 -> Databricks loader."""

import io
import zipfile
from datetime import UTC, datetime, timedelta

import pytest
from include.custom_functions.l2_voter_loaders import (
    EXPIRED_FOLDER,
    build_load_sql,
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
        "file_name": file_name,
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

    def test_source_without_known_members_diffs_on_its_folder(self):
        """The expired file's members aren't derivable from its name."""
        expired = _source(members=None, file_name="Manual_ID_Omits.tab", folder=EXPIRED_FOLDER)
        assert plan_transfers([expired], {f"{EXPIRED_FOLDER}/Manual_ID_Omits.tab": STAGED}) == []
        assert plan_transfers([expired], {}) == [expired]


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

    def test_plain_file_is_copied_as_is(self, tmp_path):
        s3_client = FakeS3Client()
        source = _source(members=None, file_name="Manual_ID_Omits.tab", folder=EXPIRED_FOLDER)
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


class TestBuildLoadSql:
    def _sql(self, source_file_name):
        return build_load_sql(
            "cat",
            "schema",
            "bucket",
            "staging/prod",
            {"folder": "MO", "source_file_name": source_file_name, "table_name": "t"},
        )

    def test_tab_and_csv_get_their_own_delimiter(self):
        assert "delimiter => '\\t'" in self._sql(MEMBERS[0])
        assert "delimiter => ','" in self._sql(MEMBERS[1])

    def test_reads_every_column_as_a_string(self):
        """Leading zeros in ZIPs and similar codes only survive with inference off."""
        sql = self._sql(MEMBERS[0])
        assert "inferColumnTypes => false" in sql
        # read_files appends a rescued-data column the source tables have never carried.
        assert "EXCEPT (_rescued_data)" in sql

    def test_refuses_a_name_it_cannot_vouch_for(self):
        with pytest.raises(ValueError, match="Unsafe source file name"):
            self._sql("evil'; drop table x; --.tab")
