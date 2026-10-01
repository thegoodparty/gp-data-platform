"""A flow's orphans table: an append-only record of keys that left the source model.

retl never deletes from a destination. Instead, a key that was sent before and is no
longer in the source gets a `missing` event (with its last-sent payload, for lookup),
and a `returned` event if it comes back. Keys whose latest event is `missing` are the
ones to clean up by hand.

A returned key is resent once even if its payload is unchanged, because the contact may
have been deleted by hand while it was missing; its `returned` event is written only
after the destination confirms it, so a failed resend is retried next run.

The table is `<log_table>_orphans`, so a scratch log table gets a scratch orphans table
with no extra config. Like the log table, it is created only by `retl --init-log` and
carries the same `retl.flow_id` stamp.
"""

from __future__ import annotations

from typing import Any

from .databricks_io import fetch_all_rows
from .sent_log import FLOW_ID_PROPERTY, JsonRowsWriter, json_rows_insert_sql, verify_log_table_identity

MISSING = "missing"
RETURNED = "returned"


def orphans_table(log_table: str) -> str:
    return f"{log_table}_orphans"


def create_orphans_table_sql(table: str, flow_id: str) -> str:
    return (
        f"create table if not exists {table} ("
        "tracking_key string not null, "
        "event string not null, "
        "last_payload string not null, "
        "detected_at timestamp not null"
        f") using delta cluster by (tracking_key) tblproperties ('{FLOW_ID_PROPERTY}' = '{flow_id}')"
    )


def init_orphans_table(connection: Any, table: str, flow_id: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(create_orphans_table_sql(table, flow_id))
    verify_log_table_identity(connection, table, flow_id)


def open_orphans_sql(table: str) -> str:
    return (
        f"select tracking_key from {table} "
        "qualify row_number() over (partition by tracking_key order by detected_at desc) = 1 "
        f"and event = '{MISSING}'"
    )


def read_open_orphans(connection: Any, table: str) -> set[str]:
    """Keys whose latest event is `missing`."""
    with connection.cursor() as cursor:
        cursor.execute(open_orphans_sql(table))
        return {row["tracking_key"] for row in fetch_all_rows(cursor)}


def orphans_writer(connection: Any, table: str) -> JsonRowsWriter:
    return JsonRowsWriter(
        connection, json_rows_insert_sql(table, ("tracking_key", "event", "last_payload"), "detected_at")
    )
