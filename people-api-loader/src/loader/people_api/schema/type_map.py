"""Map Databricks/Spark type strings (as returned by Unity Catalog `type_text`)
to Postgres column types. Fail loud on anything unrecognised — never guess a type."""

from __future__ import annotations

import re

_DECIMAL_RE = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$")

_SCALAR: dict[str, str] = {
    "string": "TEXT",
    "int": "INTEGER",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "long": "BIGINT",
    "smallint": "SMALLINT",
    "short": "SMALLINT",
    "tinyint": "SMALLINT",
    "byte": "SMALLINT",
    "double": "DOUBLE PRECISION",
    "float": "REAL",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMPTZ",
    "timestamp_ntz": "TIMESTAMP",
    "binary": "BYTEA",
}


class UnknownSparkType(RuntimeError):
    """Raised for a Spark type with no Postgres mapping (so we never emit a wrong type)."""


def to_pg_type(spark_type: str) -> str:
    t = spark_type.strip().lower()
    if t in _SCALAR:
        return _SCALAR[t]
    m = _DECIMAL_RE.match(t)
    if m:
        return f"NUMERIC({m.group(1)},{m.group(2)})"
    raise UnknownSparkType(
        f"no Postgres mapping for Spark type {spark_type!r}; add it to type_map._SCALAR "
        "or handle it as a per-column override in schema_spec"
    )
