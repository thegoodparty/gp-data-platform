"""The unload (Spark CSV write) and copy (PG `FORMAT csv` import) options are two literals in
two dialects that MUST stay paired, or the load silently corrupts. Pin both so a one-sided edit
(e.g. changing one delimiter) fails CI."""

from __future__ import annotations

from loader.people_api.schema.unload_sql import _CSV_OPTIONS
from loader.people_api.steps.copy_s3 import _IMPORT_OPTIONS


def test_unload_and_copy_csv_options_are_paired() -> None:
    # tab delimiter on both sides
    assert "'sep' = '\\t'" in _CSV_OPTIONS
    assert "DELIMITER E'\\t'" in _IMPORT_OPTIONS
    # empty string round-trips to NULL on both sides
    assert "'nullValue' = ''" in _CSV_OPTIONS
    assert "NULL ''" in _IMPORT_OPTIONS
    # double-quote for both quote and escape on both sides
    assert "'quote' = '\"'" in _CSV_OPTIONS and "'escape' = '\"'" in _CSV_OPTIONS
    assert "QUOTE '\"'" in _IMPORT_OPTIONS and "ESCAPE '\"'" in _IMPORT_OPTIONS
    # copy reads CSV (not text)
    assert "FORMAT csv" in _IMPORT_OPTIONS
