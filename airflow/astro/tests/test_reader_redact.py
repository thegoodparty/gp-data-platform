"""Redaction of a tool's error line.

Every case here is a shape that has appeared in a real failure message; the
fixture text is invented, never a production message. The last test is the guard
against over-redaction, because a record whose error is all placeholders tells a
ticket nothing.
"""

import include.reader.redact as redact
import pytest
from include.reader.redact import redact_error


def test_email_is_removed():
    out = redact_error("Contact someone@example.com bounced with a permanent failure")
    assert "someone@example.com" not in out
    assert "bounced" in out


def test_phone_is_removed():
    out = redact_error("SMS to (555) 010-0199 was rejected by the carrier")
    assert "010-0199" not in out
    assert "rejected by the carrier" in out


def test_long_token_is_removed():
    out = redact_error("Cannot cast value aB3dE5gH7jK9mN1pQ3sT5v to integer")
    assert "aB3dE5gH7jK9mN1pQ3sT5v" not in out
    assert "Cannot cast value" in out


def test_url_credentials_are_removed():
    out = redact_error("Connection refused for postgres://loader:hunter2@db.internal:5432/orders")
    assert "hunter2" not in out
    # The host has to survive: `user:pass@host` otherwise reads as an email and
    # takes the host with it.
    assert "db.internal" in out


def test_quoted_literal_is_removed():
    out = redact_error("Cast failed: input 'O'Brien' is not valid for INT64")
    assert "O'Brien" not in out
    assert "not valid for INT64" in out


def test_digit_run_is_removed():
    out = redact_error("No row found for voter reference 482019377 during the join")
    assert "482019377" not in out
    assert "No row found for voter reference" in out


def test_truncation_marker():
    out = redact_error("word " * 300, limit=100)
    assert len(out) <= 100
    assert out.endswith("...")


def test_synthetic_cast_error_fixture():
    """The shape the first rule set missed: a cast error echoing the offending
    value inside quotes. A name, a street address or an id is none of the other
    shapes, so only stripping the quoted literal removes it."""
    fixture = (
        "Database Error in test assert_no_duplicate_paid_p2p_sends (tests/assert_no_duplicate_paid_p2p_sends.sql)\n"
        "  Cannot cast column recipient_address to type INTEGER: "
        "'1187 Sunset Terrace Apt 4B' is not a valid INTEGER"
    )
    out = redact_error(fixture)
    assert "1187 Sunset Terrace Apt 4B" not in out
    assert "Cannot cast column recipient_address to type INTEGER" in out
    # A single line, whatever the input's shape.
    assert "\n" not in out


def test_ordinary_message_survives():
    """No quoted text and no run of five digits, so nothing here should be eaten."""
    raw = "Got 1 result, configured to fail if != 0"
    assert redact_error(raw) == raw


def test_empty_input_gives_an_empty_line():
    assert redact_error(None) == ""
    assert redact_error("") == ""


@pytest.mark.parametrize(
    "pattern, replacement, sample",
    [
        (redact._EMAIL, "<email>", "someone@example.com"),
        (redact._PHONE, "<phone>", "(555) 010-0199"),
        (redact._LONG_TOKEN, "<token>", "aB3dE5gH7jK9mN1pQ3sT5v"),
        (redact._URL_CREDENTIALS, "***@", "postgres://loader:hunter2@db.internal"),
        (redact._QUOTED_LITERAL, "<quoted>", "value 'Sunset Terrace' rejected"),
        (redact._DIGIT_RUN, "<number>", "reference 482019377"),
    ],
    ids=["email", "phone", "token", "credentials", "quoted", "digits"],
)
def test_every_rule_matches_its_own_shape(pattern, replacement, sample):
    """A rule with no shape it matches is dead code that reads as protection, and
    the per-shape tests above would still pass on a neighbouring rule."""
    assert pattern.search(sample), sample
    assert pattern.sub(replacement, sample) != sample, sample
    # Every rule is in the applied set, so none is defined and then forgotten.
    assert pattern in [p for p, _ in redact._REPLACEMENTS], sample
