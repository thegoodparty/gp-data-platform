from datetime import UTC, datetime
from decimal import Decimal

from loader import anthropic_client as client


class _FakeResponse:
    def __init__(self, body):
        self._body = body
        self.status_code = 200
        self.text = ""

    def raise_for_status(self):
        pass

    def json(self):
        return self._body


def test_cents_to_dollars():
    assert client.cents_to_dollars(None) is None
    assert client.cents_to_dollars("0.000000") == Decimal("0")
    # This is the exact value observed in production: the API's "amount" is fractional cents,
    # so "2799.187450" is $27.9918745, not $2799.19.
    assert client.cents_to_dollars("2799.187450") == Decimal("27.9918745")


def test_fetch_org_cost_report_requests_token_type_and_converts_to_dollars(monkeypatch):
    captured_params = {}

    def fake_get(url, headers, params, timeout):
        captured_params.update(params)
        return _FakeResponse(
            {
                "data": [
                    {
                        "starting_at": "2026-09-08T00:00:00Z",
                        "ending_at": "2026-09-09T00:00:00Z",
                        "results": [
                            {
                                "product": "chat",
                                "model": "claude-fable-5",
                                "cost_type": "tokens",
                                "token_type": "output_tokens",
                                "currency": "USD",
                                "amount": "2799.187450",
                                "list_amount": "2799.187450",
                                "requests": None,
                            }
                        ],
                    }
                ],
                "has_more": False,
                "next_page": None,
            }
        )

    monkeypatch.setattr(client.requests, "get", fake_get)

    start = datetime(2026, 9, 8, tzinfo=UTC)
    end = datetime(2026, 9, 9, tzinfo=UTC)
    now = datetime(2026, 9, 9, 1, tzinfo=UTC)
    rows = client.fetch_org_cost_report("fake-key", start, end, now)

    assert captured_params["group_by[]"] == ["product", "model", "cost_type", "token_type"]
    assert len(rows) == 1
    row = rows[0]
    assert row["token_type"] == "output_tokens"
    # The regression this guards: storing "2799.187450" verbatim as dollars is a 100x overstatement.
    assert row["amount"] == Decimal("27.9918745")
    assert row["list_amount"] == Decimal("27.9918745")
