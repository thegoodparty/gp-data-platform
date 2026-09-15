from decimal import Decimal

import pytest

from loader import assertions
from loader.config import Config


@pytest.fixture
def cfg():
    return Config(
        anthropic_analytics_api_key="unused",
        databricks_warehouse_id="fake-warehouse",
        schema="dbt_test",
    )


def _patch_query_rows(monkeypatch, rows):
    monkeypatch.setattr(assertions.writer, "query_rows", lambda warehouse_id, sql: rows)


def test_decimal_none_defaults_to_zero():
    assert assertions._decimal(None) == Decimal(0)
    assert assertions._decimal("1.50") == Decimal("1.50")


def test_token_type_grain_passes_when_no_bad_groups(monkeypatch, cfg):
    _patch_query_rows(monkeypatch, [])
    assertions.assert_org_cost_report_token_type_grain(cfg, "2026-01-01", "2026-01-02")


def test_token_type_grain_fails_when_group_missing_rows(monkeypatch, cfg):
    _patch_query_rows(monkeypatch, [["2026-01-01", "chat", "claude-opus-5", 1]])
    with pytest.raises(assertions.AssertionFailure, match="exactly 5"):
        assertions.assert_org_cost_report_token_type_grain(cfg, "2026-01-01", "2026-01-02")


def test_amount_within_list_amount_passes_when_zero_violations(monkeypatch, cfg):
    _patch_query_rows(monkeypatch, [[0]])
    assertions.assert_amount_within_list_amount(cfg, "some_table", "bucket_start", "a", "b")


def test_amount_within_list_amount_fails_when_violations_found(monkeypatch, cfg):
    _patch_query_rows(monkeypatch, [[3]])
    with pytest.raises(assertions.AssertionFailure, match="3 rows"):
        assertions.assert_amount_within_list_amount(cfg, "some_table", "bucket_start", "a", "b")


def test_cross_table_agreement_passes_when_user_within_org(monkeypatch, cfg):
    _patch_query_rows(monkeypatch, [["100.00", "90.00"]])
    assertions.assert_cross_table_cost_agreement(cfg, "a", "b")


def test_cross_table_agreement_passes_when_totals_equal(monkeypatch, cfg):
    _patch_query_rows(monkeypatch, [["100.00", "100.00"]])
    assertions.assert_cross_table_cost_agreement(cfg, "a", "b")


def test_cross_table_agreement_fails_when_user_exceeds_org(monkeypatch, cfg):
    _patch_query_rows(monkeypatch, [["90.00", "100.00"]])
    with pytest.raises(assertions.AssertionFailure, match="exceeds"):
        assertions.assert_cross_table_cost_agreement(cfg, "a", "b")


def test_cross_table_agreement_passes_when_both_zero(monkeypatch, cfg):
    _patch_query_rows(monkeypatch, [[None, None]])
    assertions.assert_cross_table_cost_agreement(cfg, "a", "b")


def test_cost_per_token_ceiling_passes_under_ceiling(monkeypatch, cfg):
    # $10 for 1,000,000 tokens = $0.00001/token, well under the $50/MTok ceiling.
    _patch_query_rows(monkeypatch, [["10.00", "1000000"]])
    assertions.assert_cost_per_token_ceiling(cfg, "a", "b")


def test_cost_per_token_ceiling_fails_over_ceiling(monkeypatch, cfg):
    # $100 for 1,000,000 tokens = $0.0001/token, over the $50/MTok ($0.00005/token) ceiling.
    _patch_query_rows(monkeypatch, [["100.00", "1000000"]])
    with pytest.raises(assertions.AssertionFailure, match="ceiling"):
        assertions.assert_cost_per_token_ceiling(cfg, "a", "b")


def test_cost_per_token_ceiling_skips_when_no_tokens(monkeypatch, cfg):
    _patch_query_rows(monkeypatch, [["0", 0]])
    assertions.assert_cost_per_token_ceiling(cfg, "a", "b")


def test_usage_non_regression_skips_when_no_prior_data(monkeypatch, cfg):
    assertions.assert_usage_non_regression(cfg, (Decimal(0), Decimal(0)), "a", "b")


def test_usage_non_regression_passes_within_tolerance(monkeypatch, cfg):
    _patch_query_rows(monkeypatch, [["1000", "50"]])
    assertions.assert_usage_non_regression(cfg, (Decimal(1000), Decimal(50)), "a", "b")


def test_usage_non_regression_fails_on_large_swing(monkeypatch, cfg):
    _patch_query_rows(monkeypatch, [["1", "50"]])
    with pytest.raises(assertions.AssertionFailure, match="tokens total moved"):
        assertions.assert_usage_non_regression(cfg, (Decimal(1000), Decimal(50)), "a", "b")


def test_usage_non_regression_fails_on_large_request_swing(monkeypatch, cfg):
    _patch_query_rows(monkeypatch, [["1000", "1"]])
    with pytest.raises(assertions.AssertionFailure, match="requests total moved"):
        assertions.assert_usage_non_regression(cfg, (Decimal(1000), Decimal(50)), "a", "b")


def test_usage_non_regression_passes_when_one_leg_stays_zero(monkeypatch, cfg):
    # before_requests is legitimately 0 (e.g. NULL requests for that range) while tokens are
    # nonzero; as long as requests stays 0 after resync too, that leg is not a regression.
    _patch_query_rows(monkeypatch, [["1000", "0"]])
    assertions.assert_usage_non_regression(cfg, (Decimal(1000), Decimal(0)), "a", "b")


def test_usage_non_regression_fails_when_value_appears_from_zero_baseline(monkeypatch, cfg):
    # before_requests == 0 must not let a real jump (0 -> 500) divide out to a false "0% change".
    _patch_query_rows(monkeypatch, [["1000", "500"]])
    with pytest.raises(assertions.AssertionFailure, match="new data appeared"):
        assertions.assert_usage_non_regression(cfg, (Decimal(1000), Decimal(0)), "a", "b")
