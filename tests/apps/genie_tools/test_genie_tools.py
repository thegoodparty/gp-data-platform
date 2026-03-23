import json
from unittest.mock import patch

import pytest
from genie_tools import cli
from genie_tools.export_space import FetchedSpace, export_space_bundle
from genie_tools.normalize_space import normalize_space_config
from genie_tools.redact_space import redact_obj
from genie_tools.validate_space import (
    validate_space_file,
    validate_space_payload,
)


def test_normalize_space_config_sorts_dict_keys_and_preserves_list_order():
    payload = {
        "z": {"beta": 2, "alpha": 1},
        "a": [
            {"name": "second", "id": 2},
            {"name": "first", "id": 1},
        ],
    }

    normalized = normalize_space_config(payload)

    assert list(normalized) == ["a", "z"]
    assert list(normalized["z"]) == ["alpha", "beta"]
    assert normalized["a"] == [
        {"id": 2, "name": "second"},
        {"id": 1, "name": "first"},
    ]


def test_redact_obj_replaces_obvious_environment_specific_values(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://dbc-test.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-123")
    monkeypatch.setenv(
        "GENIE_TOOLS_REDACTIONS_JSON",
        json.dumps({"catalog_prod": "<CATALOG>"}),
    )

    payload = {
        "workspace_id": 123456789,
        "warehouse_id": "wh-123",
        "empty_space_id": "",
        "empty_warehouse_id": "",
        "empty_workspace_id": "",
        "url": "https://dbc-test.cloud.databricks.com/sql/warehouses/wh-123",
        "path": "/Users/example/team/notebook.py",
        "notes": "Use catalog_prod via https://dbc-test.cloud.databricks.com and /Workspace/Users/team",
        "id": "01f0f7977d991cbfabc174b6b68107e0",
        "sql": "SELECT * FROM goodparty_data_catalog.mart_civics.users",
        "quoted_sql": "FROM `goodparty_data_catalog`.`mart_civics`.`campaigns`",
        "benchmark_question_id": "keep-this-id",
    }

    redacted = redact_obj(payload)

    assert redacted["workspace_id"] == "<WORKSPACE_ID>"
    assert redacted["warehouse_id"] == "<WAREHOUSE_ID>"
    assert redacted["empty_space_id"] == ""
    assert redacted["empty_warehouse_id"] == ""
    assert redacted["empty_workspace_id"] == ""
    assert redacted["url"] == "<URL>"
    assert redacted["path"] == "<ABSOLUTE_PATH>"
    assert redacted["notes"] == "Use <CATALOG> via <URL> and <ABSOLUTE_PATH>"
    assert redacted["id"] == "<GENIE_ID_01>"
    assert redacted["sql"] == "SELECT * FROM <CATALOG>.<SCHEMA>.users"
    assert redacted["quoted_sql"] == "FROM `<CATALOG>`.`<SCHEMA>`.`campaigns`"
    assert redacted["benchmark_question_id"] == "keep-this-id"


def test_redact_obj_redacts_urls_before_plain_fqn_replacement():
    payload = {
        "notes": "Connect to https://myworkspace.cloud.databricks.com/path for data",
        "identifier": "goodparty_data_catalog.mart_civics.users",
        "free_text": "Reference genie.civics.election in prose",
        "sql": "FROM goodparty_data_catalog.mart_civics.users",
    }

    redacted = redact_obj(payload)

    assert redacted["notes"] == "Connect to <URL> for data"
    assert redacted["identifier"] == "<CATALOG>.<SCHEMA>.users"
    assert redacted["free_text"] == "Reference genie.civics.election in prose"
    assert redacted["sql"] == "FROM <CATALOG>.<SCHEMA>.users"


def test_redact_obj_applies_custom_replacements_after_builtin_fqn_redaction(
    monkeypatch,
):
    monkeypatch.setenv(
        "GENIE_TOOLS_REDACTIONS_JSON",
        json.dumps({"goodparty_data_catalog": "<CATALOG>"}),
    )

    payload = {
        "sql": "SELECT * FROM goodparty_data_catalog.mart_civics.users",
    }

    redacted = redact_obj(payload)

    assert redacted["sql"] == "SELECT * FROM <CATALOG>.<SCHEMA>.users"


def test_validate_space_payload_rejects_unstructured_payload():
    with pytest.raises(ValueError, match="structured Genie config sections"):
        validate_space_payload({"title": "Only metadata"})


def test_validate_space_file_accepts_structured_payload(tmp_path):
    payload = {
        "title": "Civics Genie",
        "tables": [{"name": "elections"}],
    }
    file_path = tmp_path / "space.json"
    file_path.write_text(json.dumps(payload), encoding="utf-8")

    validate_space_file(file_path)


def test_validate_space_file_includes_json_error_location(tmp_path):
    file_path = tmp_path / "bad.json"
    file_path.write_text('{"key": }', encoding="utf-8")

    with pytest.raises(ValueError, match=r"line 1 column 9"):
        validate_space_file(file_path)


def test_export_space_bundle_writes_expected_files(tmp_path):
    payload = {
        "tables": [{"name": "elections", "warehouse_id": "wh-123"}],
        "version": 1,
    }
    metadata = {"requested_space_id": "space-123", "databricks_host": "https://dbc"}

    with patch(
        "genie_tools.export_space.fetch_space_record",
        return_value=FetchedSpace(payload=payload, metadata=metadata),
    ):
        artifacts = export_space_bundle(
            space_id="space-123",
            out_dir=tmp_path,
            write_redacted=True,
            write_metadata=True,
        )

    raw_payload = json.loads(artifacts.raw_path.read_text(encoding="utf-8"))
    normalized_payload = json.loads(
        artifacts.normalized_path.read_text(encoding="utf-8")
    )
    redacted_payload = json.loads(artifacts.redacted_path.read_text(encoding="utf-8"))
    metadata_payload = json.loads(artifacts.metadata_path.read_text(encoding="utf-8"))

    assert raw_payload == payload
    assert list(normalized_payload) == ["tables", "version"]
    assert redacted_payload["tables"][0]["warehouse_id"] == "<WAREHOUSE_ID>"
    assert metadata_payload == metadata


def test_cli_export_command_writes_files_and_reports_paths(tmp_path, capsys):
    payload = {
        "tables": [{"name": "elections"}],
        "version": 1,
    }

    with patch(
        "genie_tools.export_space.fetch_space_record",
        return_value=FetchedSpace(
            payload=payload, metadata={"requested_space_id": "s"}
        ),
    ):
        exit_code = cli.main(
            [
                "export",
                "--space-id",
                "space-123",
                "--out-dir",
                str(tmp_path),
                "--write-metadata",
            ]
        )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "WROTE raw:" in captured.out
    assert "WROTE normalized:" in captured.out
    assert (tmp_path / "raw" / "space-123.json").exists()
    assert (tmp_path / "metadata" / "space-123.json").exists()
