from typing import ClassVar

import pytest
from dbt.project.models.load.load__l2_haystaq_sftp_to_s3 import (
    EMPTY_LOAD_DETAILS,
    _collect_load_details,
    _finalize_load_details,
    _locate_extracted_tab,
    _resolve_pickup_action,
)


class TestResolvePickupAction:
    PREFIX = "l2_data/from_sftp_server/Haystaq/prod/AZ/flags/"
    S3_KEYS: ClassVar[list[str]] = [
        "l2_data/from_sftp_server/Haystaq/prod/AZ/flags/az_haystaqdnaflags_20260520.tab",
    ]

    def test_downloads_when_tab_not_in_s3(self):
        action, matched = _resolve_pickup_action(
            tab_file_name="az_haystaqdnaflags_20260601.tab",
            s3_state_prefix=self.PREFIX,
            s3_keys=self.S3_KEYS,
            logged_file_names=set(),
        )
        assert action == "download"
        assert matched is None

    def test_skips_when_tab_in_s3_and_logged(self):
        action, matched = _resolve_pickup_action(
            tab_file_name="az_haystaqdnaflags_20260520.tab",
            s3_state_prefix=self.PREFIX,
            s3_keys=self.S3_KEYS,
            logged_file_names={"az_haystaqdnaflags_20260520.tab"},
        )
        assert action == "skip"
        assert matched == "az_haystaqdnaflags_20260520.tab"

    def test_self_heals_when_tab_in_s3_but_never_logged(self):
        # The May 2026 incident: a run died after uploading to S3 but before
        # appending its sync-log rows, so the file looked "done" forever.
        action, matched = _resolve_pickup_action(
            tab_file_name="az_haystaqdnaflags_20260520.tab",
            s3_state_prefix=self.PREFIX,
            s3_keys=self.S3_KEYS,
            logged_file_names=set(),
        )
        assert action == "self_heal"
        assert matched == "az_haystaqdnaflags_20260520.tab"

    def test_matching_is_case_insensitive_but_returns_exact_s3_spelling(self):
        # S3 GETs are case-sensitive: the caller must log the spelling that
        # actually exists in S3, not the SFTP-derived one it searched with.
        action, matched = _resolve_pickup_action(
            tab_file_name="AZ_HaystaqDnaFlags_20260520.TAB",
            s3_state_prefix=self.PREFIX,
            s3_keys=self.S3_KEYS,
            logged_file_names=set(),
        )
        assert action == "self_heal"
        assert matched == "az_haystaqdnaflags_20260520.tab"

    def test_matched_name_preserves_nesting_under_the_state_prefix(self):
        # Downstream builds its read path as prefix + logged name, so the
        # matched name must be the key's suffix after the prefix, not its
        # basename.
        action, matched = _resolve_pickup_action(
            tab_file_name="az_haystaqdnaflags_20260520.tab",
            s3_state_prefix=self.PREFIX,
            s3_keys=[f"{self.PREFIX}nested/az_haystaqdnaflags_20260520.tab"],
            logged_file_names=set(),
        )
        assert action == "self_heal"
        assert matched == "nested/az_haystaqdnaflags_20260520.tab"

    def test_does_not_match_tab_name_suffix_of_other_file(self):
        # "…/az_haystaqdnaflags_20260520.tab" must not satisfy a lookup for
        # "flags_20260520.tab"; only a full path segment counts.
        action, matched = _resolve_pickup_action(
            tab_file_name="flags_20260520.tab",
            s3_state_prefix=self.PREFIX,
            s3_keys=self.S3_KEYS,
            logged_file_names=set(),
        )
        assert action == "download"
        assert matched is None


class TestLocateExtractedTab:
    def test_returns_path_when_single_tab_present(self, tmp_path):
        tab = tmp_path / "az_haystaqdnaflags_20260520.tab"
        tab.write_text("LALVOTERID\th1\n")
        found = _locate_extracted_tab(
            extracted_names=["az_haystaqdnaflags_20260520.tab"],
            temp_dir=str(tmp_path),
        )
        assert found == str(tab)

    def test_returns_path_for_tab_nested_in_zip_subdirectory(self, tmp_path):
        nested = tmp_path / "inner"
        nested.mkdir()
        tab = nested / "az_haystaqdnaflags_20260520.tab"
        tab.write_text("LALVOTERID\th1\n")
        found = _locate_extracted_tab(
            extracted_names=["inner/az_haystaqdnaflags_20260520.tab"],
            temp_dir=str(tmp_path),
        )
        assert found == str(tab)

    def test_returns_none_when_tab_file_missing_on_disk(self, tmp_path):
        # The exact May 2026 failure: the zip listed a tab member but the file
        # never materialized (vendor was mid-upload). Must be a retryable skip,
        # not an exception that kills the whole run.
        found = _locate_extracted_tab(
            extracted_names=["az_haystaqdnaflags_20260520.tab"],
            temp_dir=str(tmp_path),
        )
        assert found is None

    def test_raises_when_zip_has_no_tab(self, tmp_path):
        with pytest.raises(ValueError, match="Expected 1 .tab"):
            _locate_extracted_tab(extracted_names=["readme.txt"], temp_dir=str(tmp_path))

    def test_raises_when_zip_has_multiple_tabs(self, tmp_path):
        with pytest.raises(ValueError, match="Expected 1 .tab"):
            _locate_extracted_tab(
                extracted_names=["a_20260520.tab", "b_20260520.tab"],
                temp_dir=str(tmp_path),
            )


def _details_for(state_id: str, kind: str) -> dict:
    return {
        "state_id": state_id,
        "source_file_names": [f"{state_id.lower()}_haystaqdna{kind}_20260520.tab"],
        "source_zip_file": f"{state_id.lower()}_haystaqdna{kind}_20260520.tab.zip",
        "loaded_at": None,
        "s3_state_prefix": f"prefix/{state_id}/{kind}/",
    }


class TestCollectLoadDetails:
    def test_collects_details_for_every_state_and_kind(self):
        details, failures = _collect_load_details(
            state_ids=["AZ", "MI"],
            extract_fns={
                "flags": lambda st: _details_for(st, "flags"),
                "scores": lambda st: _details_for(st, "scores"),
            },
        )
        assert failures == []
        assert [(d["state_id"], d["load_details"]["source_zip_file"]) for d in details] == [
            ("AZ", "az_haystaqdnaflags_20260520.tab.zip"),
            ("AZ", "az_haystaqdnascores_20260520.tab.zip"),
            ("MI", "mi_haystaqdnaflags_20260520.tab.zip"),
            ("MI", "mi_haystaqdnascores_20260520.tab.zip"),
        ]

    def test_skipped_states_are_excluded_from_details(self):
        details, failures = _collect_load_details(
            state_ids=["AZ"],
            extract_fns={
                "flags": lambda st: dict(EMPTY_LOAD_DETAILS),
                "scores": lambda st: _details_for(st, "scores"),
            },
        )
        assert failures == []
        assert [d["load_details"]["source_zip_file"] for d in details] == [
            "az_haystaqdnascores_20260520.tab.zip"
        ]

    def test_one_failing_state_does_not_stop_later_states(self):
        # Regression for the May 2026 incident: MI's failure must not prevent
        # the remaining states from being processed.
        def flags(st):
            if st == "MI":
                raise FileNotFoundError("extracted tab not found")
            return _details_for(st, "flags")

        details, failures = _collect_load_details(
            state_ids=["AZ", "MI", "NV"],
            extract_fns={"flags": flags, "scores": lambda st: _details_for(st, "scores")},
        )
        processed = [(d["state_id"], d["load_details"]["source_zip_file"]) for d in details]
        assert ("NV", "nv_haystaqdnaflags_20260520.tab.zip") in processed
        assert ("MI", "mi_haystaqdnascores_20260520.tab.zip") in processed
        assert failures == ["MI (flags): extracted tab not found"]


class TestFinalizeLoadDetails:
    def test_returns_details_when_no_failures(self):
        details = [{"state_id": "AZ", "load_details": _details_for("AZ", "flags")}]
        assert _finalize_load_details(details, failures=[]) is details

    def test_partial_failure_still_returns_successful_details(self):
        # A persistently failing state must not starve the others: successful
        # states' rows persist this run instead of being discarded by a raise.
        details = [{"state_id": "AZ", "load_details": _details_for("AZ", "flags")}]
        assert _finalize_load_details(details, failures=["MI (flags): extracted tab not found"]) is details

    def test_raises_only_when_every_extraction_failed(self):
        # Nothing succeeded means nothing would be lost by failing the run, and
        # a total wipeout (e.g. SFTP outage) should be loud, not a green no-op.
        with pytest.raises(RuntimeError, match=r"all 2 extraction\(s\) failed.*MI \(flags\).*WI \(scores\)"):
            _finalize_load_details(
                [],
                failures=["MI (flags): extracted tab not found", "WI (scores): boom"],
            )
