"""Unit tests for the identifier-type gate in the L2 nationwide union.

A state whose ZIP, precinct or district code arrives as a number has already lost its
leading zeros, so the model must fail rather than cast it back to a string. These run
on DataFrame.dtypes tuples, so they need no Spark session.
"""

from dbt.project.models.intermediate.l2.int__l2_nationwide_uniform_raw_districts import (
    IDENTIFIER_STRING_COLUMNS,
    non_string_identifier_columns,
)


def test_all_string_identifiers_pass():
    dtypes = [(name, "string") for name in IDENTIFIER_STRING_COLUMNS]
    assert non_string_identifier_columns(dtypes) == []


def test_numeric_identifiers_are_reported_with_their_type():
    dtypes = [
        ("Residence_Addresses_Zip", "string"),
        ("Residence_Addresses_ZipPlus4", "int"),
        ("Precinct", "bigint"),
    ]
    assert non_string_identifier_columns(dtypes) == [
        "Residence_Addresses_ZipPlus4 (int)",
        "Precinct (bigint)",
    ]


def test_missing_identifier_columns_are_not_an_error():
    # unionByName(allowMissingColumns) fills a column a state does not deliver.
    assert non_string_identifier_columns([("LALVOTERID", "string")]) == []


def test_non_identifier_columns_are_ignored():
    dtypes = [("Voters_Age", "int"), ("General_2024", "boolean")]
    assert non_string_identifier_columns(dtypes) == []
