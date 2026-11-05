"""The response-shape parsing check 13 does, tested without HubSpot."""

from probes.checks import _answered, _answered_in_order, _merge_pair
from probes.client import Response


def _response(status_code: int, body: dict) -> Response:
    return Response(status_code=status_code, body=body, headers={})


def test_answered_splits_results_from_not_found_ids():
    response = _response(
        207,
        {
            "results": [{"id": "1"}],
            "errors": [
                {
                    "category": "OBJECT_NOT_FOUND",
                    "context": {"ids": ["7"]},
                }
            ],
        },
    )
    answered = _answered(response)
    assert answered["returned_ids"] == ["1"]
    assert answered["error_categories"] == ["OBJECT_NOT_FOUND"]
    assert answered["not_found_ids"] == ["7"]


def test_answered_reports_an_unordered_read_as_unordered():
    response = _response(200, {"results": [{"id": "2"}, {"id": "1"}]})
    assert _answered_in_order(response, ["1", "2"])["order_preserved"] is False
    assert _answered_in_order(_response(200, {"results": [{"id": "1"}]}), ["1"])["order_preserved"] is True


class _FakeClient:
    def __init__(self, body: dict):
        self.body = body
        self.created_contact_ids: list[str] = []

    def request(self, method: str, path: str, *, json: dict) -> Response:
        return _response(200, self.body)


def test_merge_pair_registers_a_survivor_that_is_neither_input():
    client = _FakeClient({"id": "3"})
    response, survivor = _merge_pair(client, "1", "2")
    assert response.status_code == 200
    assert survivor == "3"
    assert client.created_contact_ids == ["3"]


def test_merge_pair_registers_nothing_when_the_survivor_is_the_primary():
    client = _FakeClient({"id": "1"})
    assert _merge_pair(client, "1", "2") == (client.request("POST", "", json={}), "1")
    assert client.created_contact_ids == []
