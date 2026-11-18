"""Redaction of a tool's error line before it leaves the reader.

Everything this module emits can land in Loki for 30 days and in a ticket, and
`gp-data-platform` is public, so the default is to strip anything that could be a
value from a row: an email, a phone number, a vendor or row key, credentials in a
URL, a quoted literal, a long number. A database cast error echoes the offending
value inside quotes, which is why quoted literals are stripped even though they
are the part of such a message that reads most like prose.

The risk runs both ways: over-redaction would make records useless, so
`test_ordinary_message_survives` pins that a normal dbt message keeps its text.
"""

import re

# Anything shaped like an email address.
_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
# A US phone number, with or without a country code and separators.
_PHONE = re.compile(r"(?<!\d)(?:\+?1[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}(?!\d)")
# 20+ alphanumeric characters in one run: row keys, vendor ids, hashes. dbt's own
# identifiers carry underscores or dots, so they do not match as one run.
_LONG_TOKEN = re.compile(r"\b[A-Za-z0-9]{20,}\b")
# `user:pass@` inside a URL, before the host.
_URL_CREDENTIALS = re.compile(r"(?<=//)[^/\s:@]+:[^/\s@]+@")
# A quoted literal, either quote style, on one line.
_QUOTED_LITERAL = re.compile(r"'[^'\n]*'|\"[^\"\n]*\"")
# Five or more digits: a voter id, a report id, a stripped-then-echoed count.
_DIGIT_RUN = re.compile(r"\b\d{5,}\b")

_REPLACEMENTS: tuple[tuple[re.Pattern[str], str], ...] = (
    # Credentials first: `user:pass@host` otherwise reads as an email address and
    # the host is lost with it.
    (_URL_CREDENTIALS, "***@"),
    (_EMAIL, "<email>"),
    (_PHONE, "<phone>"),
    (_LONG_TOKEN, "<token>"),
    (_QUOTED_LITERAL, "<quoted>"),
    (_DIGIT_RUN, "<number>"),
)


def redact_error(raw: str | None, limit: int = 500) -> str:
    """One redacted line, at most `limit` characters, ending in an ellipsis when cut."""
    if not raw:
        return ""
    text = " ".join(str(raw).split())
    for pattern, replacement in _REPLACEMENTS:
        text = pattern.sub(replacement, text)
    if len(text) > limit:
        text = text[: max(limit - 3, 0)] + "..."
    return text
