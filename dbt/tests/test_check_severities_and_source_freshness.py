"""Contracts for which checks are allowed to stay quiet, and which must not.

Vendor-value checks fire on a value the vendor has added, not on a defect: a new
HubSpot verification reason or a new BallotReady office level passes through the
models unchanged, so failing the build on it only skips downstream nodes. Those
sit at warn. The checks here pin that boundary in both directions, because the
dangerous edit is a severity lowered on a check whose silence loses real data --
an unknown `election_result` becomes a null winner flag, so it has to stay an
error.

The freshness half pins the other quiet-check decision: a source table that has
never landed a row is permanently stale by construction, so its freshness is
disabled rather than left to error on every run. Tables that do land rows must
keep the freshness they inherit from their source block.
"""

from pathlib import Path

import yaml

PROJECT = Path(__file__).resolve().parents[1] / "project"
SOURCES = PROJECT / "models" / "staging" / "__sources.yaml"
INT_CIVICS = PROJECT / "models" / "intermediate" / "civics" / "int__civics.yaml"
M_CIVICS = PROJECT / "models" / "marts" / "civics" / "m_civics.yaml"
M_ELECTION_API = PROJECT / "models" / "marts" / "election_api" / "m_election_api.yaml"
BR_S3_STAGING = (
    PROJECT
    / "models"
    / "staging"
    / "airbyte_source"
    / "ballotready_s3"
    / "stg_airbyte_source__ballotready_s3.yaml"
)
TESTS_DIR = PROJECT / "tests"

# Tables under airbyte_source_finance that have never landed a row: the staging
# and mart descriptions all say the stream synced empty, and the finance
# cutover's post-merge check names exactly these three as the empty QuickBooks
# streams erroring on freshness every run. A fourth stream that starts landing
# rows keeps the freshness it inherits, so this list is the whole opt-out.
NEVER_LANDED = ("quickbooks_credit_memos", "quickbooks_time_activities", "quickbooks_transfers")


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def _model(doc: dict, name: str) -> dict:
    return next(m for m in doc["models"] if m["name"] == name)


def _column(node: dict, column: str) -> dict:
    """The column, with the deprecated `tests:` key normalized to `data_tests:`."""
    found = next(c for c in node["columns"] if c["name"] == column)
    if "tests" in found:
        found = {**found, "data_tests": found["tests"]}
    return found


def _severity(column: dict, test_name: str) -> str:
    """Effective severity of one data test on a column; unset means error."""
    test = next(t for t in column.get("data_tests") or [] if test_name in t)
    config = test[test_name].get("config") or {}
    return config.get("severity", "error")


def _source_table(doc: dict, source: str, table: str) -> dict:
    src = next(s for s in doc["sources"] if s["name"] == source)
    return next(t for t in src["tables"] if t["name"] == table)


def test_vendor_value_checks_warn_rather_than_fail_the_build():
    civics = _load(INT_CIVICS)
    marts = _load(M_CIVICS)
    sources = _load(SOURCES)

    cases = [
        (
            _column(_model(civics, "int__civics_candidacy_2025"), "verification_status_reason"),
            "accepted_values",
        ),
        (
            _column(_model(civics, "int__hubspot_contacts_2025"), "verified_candidate_status"),
            "accepted_values",
        ),
        (
            _column(_model(civics, "int__civics_elected_official_ballotready"), "office_level"),
            "accepted_values",
        ),
        (_column(_model(marts, "candidacy"), "candidacy_result"), "accepted_values"),
        (
            _column(
                _source_table(sources, "airbyte_source", "techspeed_gdrive_officeholders"), "is_incumbent"
            ),
            "accepted_values",
        ),
        (
            _column(
                _source_table(sources, "airbyte_source", "techspeed_gdrive_officeholders"), "is_uncontested"
            ),
            "accepted_values",
        ),
    ]

    for column, test_name in cases:
        assert _severity(column, test_name) == "warn", f"{column['name']} must be a warning"


def test_checks_whose_silence_loses_data_stay_errors():
    """A lowered severity here turns a lost election outcome into a quiet warning."""
    staging = _load(BR_S3_STAGING)
    sources = _load(SOURCES)
    election_api = _load(M_ELECTION_API)

    candidacies_v3 = _model(staging, "stg_airbyte_source__ballotready_s3_candidacies_v3")
    assert _severity(_column(candidacies_v3, "election_result"), "accepted_values") == "error"

    for column in ("is_primary", "is_runoff"):
        assert (
            _severity(
                _column(_source_table(sources, "airbyte_source", "ballotready_s3_candidacies_v3"), column),
                "accepted_values",
            )
            == "error"
        )

    assert (
        _severity(_column(_model(election_api, "m_election_api__position"), "level"), "accepted_values")
        == "error"
    )


def test_the_remaining_singular_test_guards_are_still_present():
    """These guard checks carry no severity config, so their presence is the contract."""
    for name in (
        "assert_proposed_district_format",
        "assert_proposed_district_seed_coverage",
        "assert_hubspot_generated_casts_lossless",
    ):
        assert (TESTS_DIR / f"{name}.sql").is_file(), f"{name} is missing"


def test_sources_that_never_land_a_row_opts_out_of_freshness():
    sources = _load(SOURCES)

    for table in NEVER_LANDED:
        config = _source_table(sources, "airbyte_source_finance", table).get("config") or {}
        assert "freshness" in config, f"{table} inherits freshness and would error on every run"
        assert config["freshness"] is None, f"{table} must disable freshness, not retune it"


def test_sources_that_do_land_rows_keep_their_inherited_freshness():
    """The opt-out is per table; the block default still covers everything else."""
    sources = _load(SOURCES)
    finance = next(s for s in sources["sources"] if s["name"] == "airbyte_source_finance")
    assert finance["config"]["freshness"]["error_after"]["count"] == 31

    for table in ("quickbooks_accounts", "quickbooks_journal_entries", "quickbooks_payments"):
        assert "freshness" not in (
            _source_table(sources, "airbyte_source_finance", table).get("config") or {}
        )
