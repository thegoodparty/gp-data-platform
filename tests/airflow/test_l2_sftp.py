"""Tests for L2 SFTP expired voter file parsing."""

import pytest
from include.custom_functions.l2_sftp import (
    parse_expired_voter_ids,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MANUAL_ID_OMITS_CONTENT = (
    "LALVoterID\tIndividual_ID_Commercial\n"
    "LALMD1207645\t44539080\n"
    "LALCO607260009\t870564358\n"
    "LALCA22155264\t892402169\n"
    "LALCA4390800\t\n"
    "\t901731746\n"
    "\t670233769\n"
    "LALNJ3456751\t\n"
    "\t805519227\n"
    "LALCA22790772\t210418251\n"
)

UPPERCASE_HEADER_CONTENT = (
    "LALVOTERID\tstate_postal_code\n"
    "LALWY0001\tWY\n"
    "LALNC0002\tNC\n"
    "LALCA0003\tCA\n"
)


@pytest.fixture
def manual_id_omits_file(tmp_path):
    """Create a temp file mimicking L2's Manual_ID_Omits.tab format."""
    f = tmp_path / "Manual_ID_Omits.tab"
    f.write_text(MANUAL_ID_OMITS_CONTENT)
    return str(f)


@pytest.fixture
def uppercase_header_file(tmp_path):
    """Create a temp .tab file with uppercase LALVOTERID header."""
    f = tmp_path / "voters.tab"
    f.write_text(UPPERCASE_HEADER_CONTENT)
    return str(f)


@pytest.fixture
def csv_file(tmp_path):
    """Create a temp .csv file with lowercase lalvoterid header."""
    f = tmp_path / "voters.csv"
    f.write_text("lalvoterid,state_postal_code\nLALFL0001,FL\nLALTX0002,TX\n")
    return str(f)


# ---------------------------------------------------------------------------
# parse_expired_voter_ids — column name handling
# ---------------------------------------------------------------------------


class TestParseColumnNames:
    """Tests for case-insensitive column name matching."""

    def test_camelcase_column(self, manual_id_omits_file):
        """Recognize camelCase column name LALVoterID."""
        ids = parse_expired_voter_ids([manual_id_omits_file])
        assert len(ids) == 6
        assert "LALMD1207645" in ids
        assert "LALCA22790772" in ids

    def test_uppercase_column(self, uppercase_header_file):
        """LALVOTERID (all caps) should work."""
        ids = parse_expired_voter_ids([uppercase_header_file])
        assert len(ids) == 3

    def test_lowercase_column(self, csv_file):
        """Lowercase lalvoterid should work."""
        ids = parse_expired_voter_ids([csv_file])
        assert len(ids) == 2
        assert "LALFL0001" in ids


# ---------------------------------------------------------------------------
# parse_expired_voter_ids — empty/blank row handling
# ---------------------------------------------------------------------------


class TestParseEmptyRows:
    """Tests for handling empty and blank LALVOTERID rows."""

    def test_empty_lalvoterids_dropped(self, manual_id_omits_file):
        """Rows with blank LALVoterID should be excluded."""
        ids = parse_expired_voter_ids([manual_id_omits_file])
        assert "" not in ids
        # 9 data rows - 3 with empty LALVoterID = 6 IDs
        assert len(ids) == 6

    def test_whitespace_only_ids_dropped(self, tmp_path):
        """Whitespace-only LALVOTERIDs should be excluded."""
        f = tmp_path / "whitespace.csv"
        f.write_text("LALVOTERID\n  \nLALCA0001\n   \n")
        ids = parse_expired_voter_ids([str(f)])
        assert ids == ["LALCA0001"]


# ---------------------------------------------------------------------------
# parse_expired_voter_ids — file format handling
# ---------------------------------------------------------------------------


class TestParseFileFormats:
    """Tests for file format handling (.tab, .csv, missing columns, etc.)."""

    def test_csv_file(self, csv_file):
        """Parse comma-delimited CSV files."""
        ids = parse_expired_voter_ids([csv_file])
        assert len(ids) == 2

    def test_missing_column_skipped(self, tmp_path):
        """Files without LALVOTERID column are skipped."""
        f = tmp_path / "bad.tab"
        f.write_text("SomeOtherColumn\nvalue1\nvalue2\n")
        ids = parse_expired_voter_ids([str(f)])
        assert ids == []

    def test_nonexistent_file_skipped(self):
        """Non-existent file paths are skipped."""
        ids = parse_expired_voter_ids(["/nonexistent/path.tab"])
        assert ids == []

    def test_multiple_files(self, manual_id_omits_file, csv_file):
        """Parse and combine IDs from multiple files."""
        ids = parse_expired_voter_ids([manual_id_omits_file, csv_file])
        assert len(ids) == 8  # 6 from .tab + 2 from .csv

    def test_deduplication(self, tmp_path):
        """Same ID in two files should be deduplicated."""
        f1 = tmp_path / "a.tab"
        f2 = tmp_path / "b.tab"
        f1.write_text("LALVOTERID\nLALCA0001\nLALCA0002\n")
        f2.write_text("LALVOTERID\nLALCA0001\nLALCA0003\n")
        ids = parse_expired_voter_ids([str(f1), str(f2)])
        assert len(ids) == 3

    def test_empty_file(self, tmp_path):
        """File with header but no data rows returns empty list."""
        f = tmp_path / "empty.tab"
        f.write_text("LALVOTERID\tstate_postal_code\n")
        ids = parse_expired_voter_ids([str(f)])
        assert ids == []
