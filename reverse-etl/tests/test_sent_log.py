from __future__ import annotations

from datetime import UTC, datetime

from retl.sent_log import append_sent_log, latest_sent_sql, read_latest_sent
from tests._fakes import FakeConnection

LOG_TABLE = "goodparty_data_catalog.reverse_etl.sent_log"


def test_latest_sent_sql_filters_by_flow_id_and_keeps_only_the_latest_row() -> None:
    """Catches: a refactor dropping the flow_id filter or the qualify row_number() clause."""
    sql = latest_sent_sql(LOG_TABLE)
    assert "where flow_id = :flow_id" in sql
    assert "qualify row_number() over (partition by tracking_key order by sent_at desc) = 1" in sql
    assert LOG_TABLE in sql


def test_append_then_read_round_trips_the_confirmed_payload() -> None:
    """Catches: an appended row not being readable back as that key's latest payload."""
    connection = FakeConnection()
    append_sent_log(connection, log_table=LOG_TABLE, flow_id="hubspot_leads", confirmed={"p1": '{"a":1}'})

    latest = read_latest_sent(connection, log_table=LOG_TABLE, flow_id="hubspot_leads")
    assert latest == {"p1": '{"a":1}'}


def test_read_latest_sent_keeps_only_the_most_recent_row_per_key() -> None:
    """Catches: subtracting against all history instead of the latest row per key."""
    connection = FakeConnection()
    append_sent_log(
        connection,
        log_table=LOG_TABLE,
        flow_id="hubspot_leads",
        confirmed={"p1": '{"score":3}'},
        sent_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    append_sent_log(
        connection,
        log_table=LOG_TABLE,
        flow_id="hubspot_leads",
        confirmed={"p1": '{"score":4}'},
        sent_at=datetime(2026, 1, 2, tzinfo=UTC),
    )

    latest = read_latest_sent(connection, log_table=LOG_TABLE, flow_id="hubspot_leads")
    assert latest == {"p1": '{"score":4}'}


def test_two_flows_sharing_one_tracking_key_keep_independent_histories() -> None:
    """Catches: one flow's sent_log read or write suppressing or leaking into another flow's rows.

    Log identity is (flow_id, tracking_key) everywhere -- required test.
    """
    connection = FakeConnection()
    append_sent_log(connection, log_table=LOG_TABLE, flow_id="hubspot_leads", confirmed={"p1": '{"a":1}'})
    append_sent_log(connection, log_table=LOG_TABLE, flow_id="techspeed_leads", confirmed={"p1": '{"a":2}'})

    assert read_latest_sent(connection, log_table=LOG_TABLE, flow_id="hubspot_leads") == {"p1": '{"a":1}'}
    assert read_latest_sent(connection, log_table=LOG_TABLE, flow_id="techspeed_leads") == {"p1": '{"a":2}'}


def test_append_sent_log_is_a_no_op_for_an_empty_confirmed_mapping() -> None:
    """Catches: an unconditional insert being issued with zero rows, e.g. on an all-rejected batch."""
    connection = FakeConnection()
    append_sent_log(connection, log_table=LOG_TABLE, flow_id="hubspot_leads", confirmed={})
    assert connection.log_table == []
    assert connection.execute_call_count == 0


def test_append_sent_log_issues_one_execute_per_chunk_of_50_never_one_per_row() -> None:
    """Catches: reverting to one execute (or executemany) call per row, which the connector's
    own docs say is one sequential round trip per row -- ~74k of them on a first convergence."""
    connection = FakeConnection()
    confirmed = {f"p{i}": f'{{"n":{i}}}' for i in range(120)}

    append_sent_log(connection, log_table=LOG_TABLE, flow_id="hubspot_leads", confirmed=confirmed)

    assert connection.execute_call_count == 3  # ceil(120 / 50)
    assert len(connection.log_table) == 120
    latest = read_latest_sent(connection, log_table=LOG_TABLE, flow_id="hubspot_leads")
    assert len(latest) == 120
    assert latest["p119"] == '{"n":119}'
