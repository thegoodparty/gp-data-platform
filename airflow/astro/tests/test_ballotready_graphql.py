import base64
from dataclasses import FrozenInstanceError

import pytest
from include.custom_functions.ballotready_graphql import (
    CANDIDACY_SELECTION,
    ENDORSEMENT_SELECTION,
    FILING_PERIOD_SELECTION,
    GEOFENCE_SELECTION,
    ISSUE_SELECTION,
    NORMALIZED_POSITION_SELECTION,
    PARTY_SELECTION,
    POSITION_ELECTION_FREQUENCY_SELECTION,
    STANCE_SELECTION,
    EntitySpec,
    FetchedNode,
    RateLimiter,
    chunked,
    encode_node_id,
    fetch_nodes,
    is_retryable_status,
    landing_table,
    retry_wait_seconds,
)


def test_encode_node_id_uses_the_ballot_factory_global_id_format():
    encoded = encode_node_id("Candidacy", 12345)
    assert base64.b64decode(encoded).decode() == "gid://ballot-factory/Candidacy/12345"


def test_encode_node_id_varies_by_node_type():
    assert encode_node_id("Issue", 7) != encode_node_id("Geofence", 7)


def test_chunked_splits_into_fixed_size_batches():
    assert list(chunked([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]


def test_chunked_rejects_a_size_below_one():
    with pytest.raises(ValueError, match="chunk size"):
        list(chunked([1, 2], 0))


class FakeClock:
    """A monotonic clock that only advances when sleep() is called."""

    def __init__(self):
        self.now = 0.0
        self.slept = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


def test_rate_limiter_spaces_calls_by_the_configured_interval():
    clock = FakeClock()
    limiter = RateLimiter(requests_per_second=2.0, sleep=clock.sleep, clock=clock.time)

    limiter.acquire()  # first call is free
    limiter.acquire()  # must wait 0.5s

    assert clock.slept == [pytest.approx(0.5)]


def test_rate_limiter_does_not_sleep_when_enough_time_has_passed():
    clock = FakeClock()
    limiter = RateLimiter(requests_per_second=2.0, sleep=clock.sleep, clock=clock.time)

    limiter.acquire()
    clock.now += 10.0
    limiter.acquire()

    assert clock.slept == []


def test_pause_for_holds_every_subsequent_acquire():
    """A 429 seen by one worker must stop all of them, not just the one that saw it."""
    clock = FakeClock()
    limiter = RateLimiter(requests_per_second=1000.0, sleep=clock.sleep, clock=clock.time)

    limiter.pause_for(30.0)
    limiter.acquire()

    assert clock.slept and clock.slept[0] == pytest.approx(30.0)


def test_rate_limiter_rejects_a_non_positive_rate():
    with pytest.raises(ValueError, match="requests_per_second"):
        RateLimiter(requests_per_second=0)


@pytest.mark.parametrize("status", [429, 500, 502, 503])
def test_retryable_statuses(status):
    assert is_retryable_status(status)


@pytest.mark.parametrize("status", [200, 400, 401, 403, 404, 422])
def test_non_retryable_statuses(status):
    assert not is_retryable_status(status)


def test_retry_wait_honors_a_well_formed_retry_after_header():
    assert retry_wait_seconds({"Retry-After": "12"}, attempt=0) == 12.0


def test_retry_wait_reads_retry_after_case_insensitively():
    assert retry_wait_seconds({"retry-after": "5"}, attempt=0) == 5.0


def test_retry_wait_caps_retry_after_at_max_backoff():
    assert retry_wait_seconds({"Retry-After": "9999"}, attempt=0, max_backoff=60.0) == 60.0


def test_retry_wait_falls_back_to_jittered_backoff_when_retry_after_is_garbage():
    # rng returns its upper bound so the growth is assertable.
    wait = retry_wait_seconds({"Retry-After": "soon"}, attempt=2, base_backoff=1.0, rng=lambda lo, hi: hi)
    assert wait == 4.0


def test_retry_wait_backoff_grows_with_the_attempt_number():
    rng = lambda lo, hi: hi  # noqa: E731
    waits = [retry_wait_seconds({}, attempt=n, base_backoff=1.0, rng=rng) for n in range(4)]
    assert waits == [1.0, 2.0, 4.0, 8.0]


def test_retry_wait_never_returns_a_negative_wait_for_a_negative_retry_after():
    wait = retry_wait_seconds({"Retry-After": "-5"}, attempt=0)
    assert wait >= 0.0


SELECTION = "... on Candidacy { databaseId id }"


class FakeResponse:
    def __init__(self, status_code=200, body=None, headers=None):
        self.status_code = status_code
        self._body = body or {}
        self.headers = headers or {}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    """Returns queued responses in order and records the ids each call requested."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.requested_id_counts = []

    def post(self, url, json, headers, timeout):
        self.requested_id_counts.append(len(json["variables"]["ids"]))
        return self._responses.pop(0)


def _ok(nodes):
    return FakeResponse(body={"data": {"nodes": nodes}})


def _limiter():
    return RateLimiter(requests_per_second=1000.0, sleep=lambda s: None, clock=lambda: 0.0)


def test_fetch_nodes_maps_results_positionally():
    session = FakeSession([_ok([{"databaseId": 11}, None, {"databaseId": 33}])])

    result = fetch_nodes([1, 2, 3], "Candidacy", SELECTION, "tok", _limiter(), session)

    assert result == [
        FetchedNode(requested_id=1, node={"databaseId": 11}),
        FetchedNode(requested_id=2, node=None),
        FetchedNode(requested_id=3, node={"databaseId": 33}),
    ]


def test_fetch_nodes_does_not_key_results_by_database_id():
    """databaseId need not equal the requested id, so position is the only safe mapping."""
    session = FakeSession([_ok([{"databaseId": 999}])])

    result = fetch_nodes([1], "Candidacy", SELECTION, "tok", _limiter(), session)

    assert result == [FetchedNode(requested_id=1, node={"databaseId": 999})]


def test_fetch_nodes_bisects_when_the_response_is_short():
    """A short array means silent truncation, so split and retry rather than lose rows."""
    session = FakeSession(
        [
            _ok([{"databaseId": 1}, {"databaseId": 2}]),  # 4 requested, 2 returned
            _ok([{"databaseId": 1}, {"databaseId": 2}]),  # first half
            _ok([{"databaseId": 3}, {"databaseId": 4}]),  # second half
        ]
    )

    result = fetch_nodes([1, 2, 3, 4], "Candidacy", SELECTION, "tok", _limiter(), session)

    assert [r.requested_id for r in result] == [1, 2, 3, 4]
    assert session.requested_id_counts == [4, 2, 2]


def test_fetch_nodes_raises_when_a_single_id_request_is_still_short():
    session = FakeSession([_ok([])])

    with pytest.raises(RuntimeError, match="returned 0 nodes for 1 id"):
        fetch_nodes([1], "Candidacy", SELECTION, "tok", _limiter(), session)


def test_fetch_nodes_retries_a_429_and_pauses_every_worker():
    limiter = _limiter()
    paused = []
    limiter.pause_for = lambda s: paused.append(s)
    session = FakeSession([FakeResponse(429, headers={"Retry-After": "7"}), _ok([{"databaseId": 1}])])

    result = fetch_nodes([1], "Candidacy", SELECTION, "tok", limiter, session, sleep=lambda s: None)

    assert paused == [7.0]
    assert result == [FetchedNode(requested_id=1, node={"databaseId": 1})]


def test_fetch_nodes_raises_on_graphql_errors():
    session = FakeSession([FakeResponse(body={"errors": [{"message": "nope"}]})])

    with pytest.raises(RuntimeError, match="CivicEngine GraphQL errors"):
        fetch_nodes([1], "Candidacy", SELECTION, "tok", _limiter(), session)


def test_fetch_nodes_sends_encoded_ids_and_a_bearer_token():
    captured = {}

    class CapturingSession(FakeSession):
        def post(self, url, json, headers, timeout):
            captured["url"] = url
            captured["ids"] = json["variables"]["ids"]
            captured["auth"] = headers["Authorization"]
            return super().post(url, json, headers, timeout)

    session = CapturingSession([_ok([{"databaseId": 1}])])
    fetch_nodes([42], "Issue", SELECTION, "tok", _limiter(), session)

    assert captured["url"] == "https://bpi.civicengine.com/graphql"
    assert captured["ids"] == [encode_node_id("Issue", 42)]
    assert captured["auth"] == "Bearer tok"


ALL_SELECTIONS = {
    "Candidacy": CANDIDACY_SELECTION,
    "Endorsement": ENDORSEMENT_SELECTION,
    "FilingPeriod": FILING_PERIOD_SELECTION,
    "Geofence": GEOFENCE_SELECTION,
    "Issue": ISSUE_SELECTION,
    "NormalizedPosition": NORMALIZED_POSITION_SELECTION,
    "Party": PARTY_SELECTION,
    "PositionElectionFrequency": POSITION_ELECTION_FREQUENCY_SELECTION,
    "Stance": STANCE_SELECTION,
}


@pytest.mark.parametrize("name,selection", sorted(ALL_SELECTIONS.items()))
def test_every_selection_is_an_inline_fragment_with_balanced_braces(name, selection):
    assert selection.strip().startswith("... on "), f"{name} is not an inline fragment"
    assert selection.count("{") == selection.count("}"), f"{name} has unbalanced braces"


@pytest.mark.parametrize("name,selection", sorted(ALL_SELECTIONS.items()))
def test_no_selection_carries_the_outer_query_wrapper(name, selection):
    # _build_query supplies `query GetNodesBatch(...) { nodes(ids: $ids) { ... } }`.
    assert "query Get" not in selection, f"{name} still has the query wrapper"
    assert "nodes(ids:" not in selection, f"{name} still has the nodes() wrapper"


def test_candidacy_selection_keeps_the_fields_the_replaced_model_reads():
    for field in ("databaseId", "id", "candidate", "election", "position", "race"):
        assert field in CANDIDACY_SELECTION


def test_stance_selection_keeps_the_nested_issue_reference():
    # The issue worklist reads issue ids back out of landed stance payloads.
    assert "stances" in STANCE_SELECTION
    assert "issue" in STANCE_SELECTION


def test_entity_spec_is_frozen():
    spec = EntitySpec("issue", "Issue", ISSUE_SELECTION, 100, lambda **kwargs: "")
    with pytest.raises(FrozenInstanceError):
        spec.name = "other"


def test_entity_spec_carries_its_fields():
    builder = lambda **kwargs: "sql"  # noqa: E731
    spec = EntitySpec("issue", "Issue", ISSUE_SELECTION, 100, builder)
    assert (spec.name, spec.node_type, spec.batch_size) == ("issue", "Issue", 100)
    assert spec.worklist_sql is builder


def test_landing_table_is_backtick_qualified():
    assert landing_table("cat", "sch", "issue") == "`cat`.`sch`.`ballotready_issue_raw`"


def test_landing_table_names_follow_the_entity():
    assert landing_table("c", "s", "position_election_frequency").endswith(
        "`ballotready_position_election_frequency_raw`"
    )
