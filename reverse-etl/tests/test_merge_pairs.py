import argparse

import pytest

from probes.merge_pairs import _parse_pair, predict


def test_predict_keeps_primary_values_and_fills_blanks():
    primary = {"win_stage": "Stage 5", "hubspot_owner_id": "1", "phone": "", "lifecyclestage": "lead"}
    secondary = {
        "win_stage": "Stage 0",
        "hubspot_owner_id": "2",
        "phone": "555",
        "lifecyclestage": "customer",
    }
    expected = predict(primary, secondary)
    assert expected["win_stage"] == "Stage 5"
    assert expected["hubspot_owner_id"] == "1"
    assert expected["phone"] == "555"
    assert expected["lifecyclestage"] == "customer"


def test_predict_keeps_oldest_createdate():
    expected = predict({"createdate": "2026-01-01T00:00:00Z"}, {"createdate": "2024-05-09T00:00:00Z"})
    assert expected["createdate"] == "2024-05-09T00:00:00Z"


def test_parse_pair():
    assert _parse_pair("111:222") == ("111", "222")
    for bad in ("111", "111:111", "a:2", "1:"):
        with pytest.raises(argparse.ArgumentTypeError):
            _parse_pair(bad)
