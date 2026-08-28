import time
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError, ConnectionClosedError, ResponseStreamingError

from bedrock_clients._retry import _is_retryable, call_with_retries


def client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "x"}}, "InvokeModel")


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (client_error("ThrottlingException"), True),
        (client_error("ModelTimeoutException"), True),
        (client_error("ServiceUnavailableException"), True),
        (client_error("ValidationException"), False),
        (client_error("AccessDeniedException"), False),
        # HTTPClientError family: NOT botocore ConnectionError subclasses.
        (ConnectionClosedError(endpoint_url="https://x"), True),
        (ResponseStreamingError(error="mid-body drop"), True),
    ],
)
def test_classification(exc, expected):
    assert _is_retryable(exc) is expected


def test_sleep_never_crosses_the_deadline():
    calls = []

    def fn():
        calls.append(1)
        raise client_error("ThrottlingException")

    start = time.monotonic()
    with patch("bedrock_clients._retry.random.uniform", return_value=10.0):
        with pytest.raises(ClientError):
            call_with_retries(fn, max_retries=5, max_elapsed_seconds=0.5)
    # The 10s jitter would land past the 0.5s deadline: no sleep, no retry.
    assert time.monotonic() - start < 2.0
    assert len(calls) == 1
