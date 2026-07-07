"""Tests for the BallotReady GraphQL extraction helpers."""

from base64 import b64decode
from datetime import UTC, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from include.custom_functions import ballotready_graphql
from include.custom_functions.ballotready_graphql import (
    _is_retryable_status,
    _retry_wait_seconds,
    base64_encode_person_id,
    build_person_cursor_query,
    chunked,
    fetch_person_batch,
    format_cursor_ts,
    get_person_ids_to_fetch,
    persons_to_ndjson,
    read_cursor,
    write_cursor,
    write_persons_ndjson,
)


@pytest.fixture
def mock_connection():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


def _response(status_code=200, json_body=None, headers=None):
    """A minimal stand-in for a requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.json.return_value = json_body or {}
    resp.raise_for_status.side_effect = None if status_code < 400 else RuntimeError(f"HTTP {status_code}")
    return resp


# ---------------------------------------------------------------------------
# Encoding / chunking
# ---------------------------------------------------------------------------


class TestBase64EncodePersonId:
    def test_encodes_candidate_global_id(self):
        encoded = base64_encode_person_id(12345)
        assert b64decode(encoded).decode() == "gid://ballot-factory/Candidate/12345"


class TestChunked:
    def test_splits_into_batches(self):
        assert list(chunked([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]

    def test_empty(self):
        assert list(chunked([], 2)) == []

    def test_rejects_bad_size(self):
        with pytest.raises(ValueError, match="chunk size"):
            list(chunked([1], 0))


# ---------------------------------------------------------------------------
# Rate-limit handling
# ---------------------------------------------------------------------------


class TestRetryPolicy:
    @pytest.mark.parametrize(
        "status,expected", [(200, False), (404, False), (429, True), (500, True), (503, True)]
    )
    def test_is_retryable_status(self, status, expected):
        assert _is_retryable_status(status) is expected

    def test_honors_retry_after_header(self):
        assert _retry_wait_seconds({"Retry-After": "5"}, attempt=0, base_backoff=1.0) == 5.0

    def test_retry_after_capped(self):
        assert (
            _retry_wait_seconds({"Retry-After": "9999"}, attempt=0, base_backoff=1.0, max_backoff=60.0)
            == 60.0
        )

    def test_exponential_backoff_bounded_by_jitter(self):
        # Full jitter: result is within [0, min(base*2**attempt, max)].
        wait = _retry_wait_seconds({}, attempt=3, base_backoff=1.0, max_backoff=60.0)
        assert 0.0 <= wait <= 8.0

    def test_bad_retry_after_falls_back_to_backoff(self):
        wait = _retry_wait_seconds({"Retry-After": "not-a-number"}, attempt=0, base_backoff=2.0)
        assert 0.0 <= wait <= 2.0


# ---------------------------------------------------------------------------
# fetch_person_batch
# ---------------------------------------------------------------------------


class TestFetchPersonBatch:
    def test_happy_path_filters_nulls(self):
        session = MagicMock()
        session.post.return_value = _response(
            200, {"data": {"nodes": [{"databaseId": 1}, None, {"databaseId": 2}]}}
        )
        result = fetch_person_batch([1, 99, 2], "tok", session=session)
        assert result == [{"databaseId": 1}, {"databaseId": 2}]
        # ids are base64-encoded into the GraphQL variables
        sent = session.post.call_args.kwargs["json"]["variables"]["ids"]
        assert all(b64decode(i).decode().startswith("gid://ballot-factory/Candidate/") for i in sent)

    def test_retries_429_then_succeeds(self):
        session = MagicMock()
        session.post.side_effect = [
            _response(429, headers={"Retry-After": "0"}),
            _response(200, {"data": {"nodes": [{"databaseId": 7}]}}),
        ]
        with patch.object(ballotready_graphql.time, "sleep") as sleep:
            result = fetch_person_batch([7], "tok", session=session)
        assert result == [{"databaseId": 7}]
        assert session.post.call_count == 2
        sleep.assert_called_once()

    def test_raises_after_exhausting_retries(self):
        session = MagicMock()
        session.post.return_value = _response(503)
        with patch.object(ballotready_graphql.time, "sleep"), pytest.raises(RuntimeError, match="HTTP 503"):
            fetch_person_batch([1], "tok", session=session, max_retries=2)
        assert session.post.call_count == 3  # initial + 2 retries

    def test_graphql_errors_raise(self):
        session = MagicMock()
        session.post.return_value = _response(200, {"errors": [{"message": "boom"}]})
        with pytest.raises(RuntimeError, match="GraphQL errors"):
            fetch_person_batch([1], "tok", session=session)

    def test_null_data_returns_empty(self):
        # A valid GraphQL response with data: null must not raise AttributeError.
        session = MagicMock()
        session.post.return_value = _response(200, {"data": None})
        assert fetch_person_batch([1], "tok", session=session) == []


# ---------------------------------------------------------------------------
# Cursor timestamp formatting
# ---------------------------------------------------------------------------


class TestFormatCursorTs:
    def test_tz_naive_passthrough(self):
        assert format_cursor_ts(datetime(2026, 1, 15, 10, 0, 0)) == "2026-01-15 10:00:00"

    def test_utc_aware_drops_tzinfo(self):
        assert format_cursor_ts(datetime(2026, 1, 15, 10, 0, 0, tzinfo=UTC)) == "2026-01-15 10:00:00"

    def test_non_utc_aware_normalized_to_utc(self):
        est = timezone(timedelta(hours=-5))
        assert format_cursor_ts(datetime(2026, 1, 15, 10, 0, 0, tzinfo=est)) == "2026-01-15 15:00:00"


# ---------------------------------------------------------------------------
# Cursor query
# ---------------------------------------------------------------------------


class TestBuildPersonCursorQuery:
    def test_no_cursor_has_no_keyset_predicate(self):
        sql = build_person_cursor_query("cat", "sch", after_changed_at=None, after_person_id=None, limit=None)
        assert "source_changed_at IS NOT NULL" in sql
        assert "TIMESTAMP" not in sql
        assert "LIMIT" not in sql
        assert "stg_airbyte_source__ballotready_s3_candidacies_v3" in sql
        assert "stg_airbyte_source__ballotready_s3_office_holders_v3" in sql
        assert "greatest(" in sql

    def test_keyset_predicate_and_limit(self):
        # Sub-second precision is preserved so the keyset tiebreak stays exact.
        sql = build_person_cursor_query(
            "cat", "sch", after_changed_at="2026-01-15 10:00:00.453000", after_person_id=42, limit=1000
        )
        assert "source_changed_at > TIMESTAMP '2026-01-15 10:00:00.453000'" in sql
        assert "source_changed_at = TIMESTAMP '2026-01-15 10:00:00.453000' AND br_person_id > 42" in sql
        assert "LIMIT 1000" in sql
        assert "ORDER BY source_changed_at ASC, br_person_id ASC" in sql

    def test_normalizes_seconds_to_microseconds(self):
        sql = build_person_cursor_query(
            "cat", "sch", after_changed_at="2026-01-15 10:00:00", after_person_id=1, limit=None
        )
        assert "TIMESTAMP '2026-01-15 10:00:00.000000'" in sql

    def test_rejects_invalid_identifier(self):
        with pytest.raises(ValueError, match="valid SQL identifier"):
            build_person_cursor_query(
                "cat", "sch; DROP", after_changed_at=None, after_person_id=None, limit=None
            )

    def test_rejects_non_timestamp_cursor(self):
        with pytest.raises(ValueError, match="ISO timestamp"):
            build_person_cursor_query("c", "s", after_changed_at="2026'; DROP", after_person_id=1, limit=None)


class TestGetPersonIdsToFetch:
    def test_returns_ordered_rows(self, mock_connection):
        conn, cursor = mock_connection
        cursor.fetchall.return_value = [(1, datetime(2026, 1, 1)), (2, datetime(2026, 1, 2))]
        rows = get_person_ids_to_fetch(conn, "cat", "sch", limit=10)
        assert rows == [(1, datetime(2026, 1, 1)), (2, datetime(2026, 1, 2))]
        cursor.execute.assert_called_once()
        cursor.close.assert_called_once()

    def test_drops_null_ids(self, mock_connection):
        conn, cursor = mock_connection
        cursor.fetchall.return_value = [(None, datetime(2026, 1, 1)), (5, datetime(2026, 1, 2))]
        rows = get_person_ids_to_fetch(conn, "cat", "sch")
        assert rows == [(5, datetime(2026, 1, 2))]


# ---------------------------------------------------------------------------
# S3 serialization + IO
# ---------------------------------------------------------------------------


class TestPersonsToNdjson:
    def test_one_line_per_person_with_metadata(self):
        out = persons_to_ndjson([{"databaseId": 1}, {"databaseId": 2}], "2026-07-06T00:00:00Z", "run_1")
        lines = out.split("\n")
        assert len(lines) == 2
        assert '"_extracted_at": "2026-07-06T00:00:00Z"' in lines[0]
        assert '"_dag_run_id": "run_1"' in lines[0]

    def test_handles_apostrophes_and_unicode(self):
        out = persons_to_ndjson([{"fullName": "O'Brien é"}], "t", "r")
        assert "O'Brien" in out


class TestS3Io:
    def test_read_cursor_absent(self):
        hook = MagicMock()
        hook.check_for_key.return_value = False
        with patch.object(ballotready_graphql, "_s3_hook", return_value=hook):
            assert read_cursor("bucket", "k", "aws") == (None, None)

    def test_read_cursor_present(self):
        hook = MagicMock()
        hook.check_for_key.return_value = True
        hook.read_key.return_value = '{"source_changed_at": "2026-01-01 00:00:00", "br_person_id": 9}'
        with patch.object(ballotready_graphql, "_s3_hook", return_value=hook):
            assert read_cursor("bucket", "k", "aws") == ("2026-01-01 00:00:00", 9)

    def test_read_cursor_partial_state_raises(self):
        hook = MagicMock()
        hook.check_for_key.return_value = True
        hook.read_key.return_value = '{"source_changed_at": "2026-01-01 00:00:00"}'
        with (
            patch.object(ballotready_graphql, "_s3_hook", return_value=hook),
            pytest.raises(ValueError, match="partial state"),
        ):
            read_cursor("bucket", "k", "aws")

    def test_write_cursor_serializes_state(self):
        hook = MagicMock()
        with patch.object(ballotready_graphql, "_s3_hook", return_value=hook):
            write_cursor(
                "b", "k", "aws", source_changed_at="2026-01-01 00:00:00", br_person_id=9, dag_run_id="r"
            )
        payload = hook.load_string.call_args.args[0]
        assert '"br_person_id": 9' in payload
        assert hook.load_string.call_args.kwargs["replace"] is True

    def test_write_persons_ndjson_skips_empty(self):
        hook = MagicMock()
        with patch.object(ballotready_graphql, "_s3_hook", return_value=hook):
            assert write_persons_ndjson("b", "k", "aws", [], "t", "r") == 0
        hook.load_string.assert_not_called()

    def test_write_persons_ndjson_writes(self):
        hook = MagicMock()
        with patch.object(ballotready_graphql, "_s3_hook", return_value=hook):
            n = write_persons_ndjson("b", "k", "aws", [{"databaseId": 1}], "t", "r")
        assert n == 1
        hook.load_string.assert_called_once()
