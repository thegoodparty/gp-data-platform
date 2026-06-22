"""Render target_schema.sql from mart columns + the schema spec.

Output is pg_dump-shaped CREATE TABLE blocks (no indexes/PK — those live in schema_spec
and are applied by build_indexes), so the existing table_ddl.extract_create_tables parser
reads it unchanged. Deterministic: same marts + same spec -> identical bytes."""

from __future__ import annotations

from loader.people_api.config import LoaderConfig
from loader.people_api.schema.mart_introspect import MartColumn, introspect_mart
from loader.people_api.schema.schema_spec import TABLE_SPECS, TableSpec
from loader.people_api.schema.type_map import to_pg_type


def _column_type(spec: TableSpec, col: MartColumn) -> str:
    return spec.type_overrides.get(col.name) or to_pg_type(col.spark_type)


def render_create_table(spec: TableSpec, columns: list[MartColumn]) -> str:
    header = f'CREATE TABLE public."{spec.pg_table}" ('
    rendered = []
    for col in columns:
        nn = "" if col.nullable else " NOT NULL"
        rendered.append(f'    "{col.name}" {_column_type(spec, col)}{nn}')
    # App/Prisma-managed columns the mart omits, appended verbatim from the spec.
    for name, pg_type, nullable in spec.extra_columns:
        nn = "" if nullable else " NOT NULL"
        rendered.append(f'    "{name}" {pg_type}{nn}')
    return "\n".join([header, ",\n".join(rendered) + "\n);"])


def render_target_schema(cfg: LoaderConfig) -> str:
    """Introspect every mart and render the full target_schema.sql contents."""
    blocks = []
    for pg_table, spec in TABLE_SPECS.items():
        columns = introspect_mart(cfg.mart_fqns[pg_table])
        blocks.append(render_create_table(spec, columns))
    return "\n\n".join(blocks) + "\n"
