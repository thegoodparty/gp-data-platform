from __future__ import annotations

from datetime import UTC, datetime

import pytest

from retl.sent_log import (
    FLOW_ID_PROPERTY,
    WrongLogTableError,
    append_sent_log,
    create_log_table_sql,
    init_log_table,
    latest_sent_sql,
    read_latest_sent,
    verify_log_table_identity,
)
from tests._fakes import FakeConnection, FakeTable, stamped_table

LOG_TABLE = "goodparty_data_catalog.reverse_etl.sent_log_hubspot_leads"
OTHER_LOG_TABLE = "goodparty_data_catalog.reverse_etl.sent_log_techspeed_leads"


def test_create_log_table_sql_has_three_columns_and_the_flow_id_stamp() -> None:
    """Catches: a refactor dropping a column, the cluster key, or the identity stamp."""
    sql = create_log_table_sql(LOG_TABLE, "hubspot_leads")
    assert LOG_TABLE in sql
    assert "tracking_key string not null" in sql
    assert "payload string not null" in sql
    assert "sent_at timestamp not null" in sql
    assert "cluster by (tracking_key)" in sql
    assert "tblproperties ('retl.flow_id' = 'hubspot_leads')" in sql


def test_latest_sent_sql_has_no_flow_filter() -> None:
    """Catches: a leftover WHERE clause now that identity is the table itself, not a column."""
    sql = latest_sent_sql(LOG_TABLE)
    assert "where" not in sql.lower()
    assert "qualify row_number() over (partition by tracking_key order by sent_at desc) = 1" in sql
    assert LOG_TABLE in sql


def test_init_log_table_creates_an_empty_stamped_table() -> None:
    """Catches: init failing to create the table, or creating it without the identity stamp."""
    connection = FakeConnection()

    init_log_table(connection, LOG_TABLE, "hubspot_leads")

    assert connection.tables[LOG_TABLE].properties == {FLOW_ID_PROPERTY: "hubspot_leads"}
    assert read_latest_sent(connection, log_table=LOG_TABLE) == {}


def test_init_log_table_is_idempotent_and_keeps_existing_rows() -> None:
    """Catches: IF NOT EXISTS being reissued in a way that wipes an existing table's history."""
    connection = FakeConnection(
        tables={
            LOG_TABLE: stamped_table(
                "hubspot_leads",
                rows=[{"tracking_key": "p1", "payload": "{}", "sent_at": datetime(2026, 1, 1, tzinfo=UTC)}],
            )
        }
    )

    init_log_table(connection, LOG_TABLE, "hubspot_leads")

    assert read_latest_sent(connection, log_table=LOG_TABLE) == {"p1": "{}"}


def test_init_log_table_raises_when_an_existing_table_is_stamped_for_another_flow() -> None:
    """Catches: re-pointing init at another flow's already-stamped table silently succeeding."""
    connection = FakeConnection(tables={LOG_TABLE: stamped_table("techspeed_leads")})

    with pytest.raises(WrongLogTableError):
        init_log_table(connection, LOG_TABLE, "hubspot_leads")


def test_verify_log_table_identity_raises_when_the_stamp_is_absent() -> None:
    """Catches: a table that exists but was never stamped (e.g. created outside init)
    being trusted as this flow's log."""
    connection = FakeConnection(tables={LOG_TABLE: FakeTable()})

    with pytest.raises(WrongLogTableError):
        verify_log_table_identity(connection, LOG_TABLE, "hubspot_leads")


def test_verify_log_table_identity_passes_for_a_correctly_stamped_table() -> None:
    """Catches: the identity check false-failing on the ordinary, correctly-stamped case."""
    connection = FakeConnection(tables={LOG_TABLE: stamped_table("hubspot_leads")})
    verify_log_table_identity(connection, LOG_TABLE, "hubspot_leads")  # does not raise


def test_append_then_read_round_trips_the_confirmed_payload() -> None:
    """Catches: an appended row not being readable back as that key's latest payload."""
    connection = FakeConnection(tables={LOG_TABLE: stamped_table("hubspot_leads")})
    append_sent_log(connection, log_table=LOG_TABLE, confirmed={"p1": '{"a":1}'})

    latest = read_latest_sent(connection, log_table=LOG_TABLE)
    assert latest == {"p1": '{"a":1}'}


def test_read_latest_sent_keeps_only_the_most_recent_row_per_key() -> None:
    """Catches: subtracting against all history instead of the latest row per key."""
    connection = FakeConnection(tables={LOG_TABLE: stamped_table("hubspot_leads")})
    append_sent_log(
        connection,
        log_table=LOG_TABLE,
        confirmed={"p1": '{"score":3}'},
        sent_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    append_sent_log(
        connection,
        log_table=LOG_TABLE,
        confirmed={"p1": '{"score":4}'},
        sent_at=datetime(2026, 1, 2, tzinfo=UTC),
    )

    latest = read_latest_sent(connection, log_table=LOG_TABLE)
    assert latest == {"p1": '{"score":4}'}


def test_two_flows_with_different_log_tables_keep_independent_histories() -> None:
    """Catches: one flow's reads or writes landing in another flow's table.

    Prod isolation is now structural (one table per flow); this proves the fake's
    (and by extension the real DDL/read/write's) per-table routing actually holds
    two tables apart rather than accidentally sharing storage.
    """
    connection = FakeConnection(
        tables={
            LOG_TABLE: stamped_table("hubspot_leads"),
            OTHER_LOG_TABLE: stamped_table("techspeed_leads"),
        }
    )
    append_sent_log(connection, log_table=LOG_TABLE, confirmed={"p1": '{"a":1}'})
    append_sent_log(connection, log_table=OTHER_LOG_TABLE, confirmed={"p1": '{"a":2}'})

    assert read_latest_sent(connection, log_table=LOG_TABLE) == {"p1": '{"a":1}'}
    assert read_latest_sent(connection, log_table=OTHER_LOG_TABLE) == {"p1": '{"a":2}'}


def test_append_sent_log_is_a_no_op_for_an_empty_confirmed_mapping() -> None:
    """Catches: an unconditional insert being issued with zero rows, e.g. on an all-rejected batch."""
    connection = FakeConnection(tables={LOG_TABLE: stamped_table("hubspot_leads")})
    append_sent_log(connection, log_table=LOG_TABLE, confirmed={})
    assert connection.tables[LOG_TABLE].rows == []
    assert connection.execute_call_count == 0


def test_append_sent_log_issues_one_execute_per_chunk_of_50_never_one_per_row() -> None:
    """Catches: reverting to one execute (or executemany) call per row, which the connector's
    own docs say is one sequential round trip per row -- ~74k of them on a first convergence."""
    connection = FakeConnection(tables={LOG_TABLE: stamped_table("hubspot_leads")})
    confirmed = {f"p{i}": f'{{"n":{i}}}' for i in range(120)}

    append_sent_log(connection, log_table=LOG_TABLE, confirmed=confirmed)

    assert connection.execute_call_count == 3  # ceil(120 / 50)
    assert len(connection.tables[LOG_TABLE].rows) == 120
    latest = read_latest_sent(connection, log_table=LOG_TABLE)
    assert len(latest) == 120
    assert latest["p119"] == '{"n":119}'
