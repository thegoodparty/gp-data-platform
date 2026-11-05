"""Fakes built from the response shapes the sandbox probe recorded (check 13).

`tests/test_client.py` reads them as HubSpot's answers; nothing here reaches the network.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from hsmerge.client import HttpResponse

# A contact as `batch/read` returns it: the record carries its own id, and the merged ids
# of anything absorbed into it. Trimmed to the keys the client reads.
SURVIVOR_RECORD: dict[str, Any] = {
    "id": "260560930060",
    "properties": {
        "email": "sandbox-a@example.com",
        "gp_person_id": None,
        "hs_merged_object_ids": "260560930060;260546473623",
        "hs_object_id": "260560930060",
        "firstname": "sandbox-a",
        "lastname": None,
    },
    "createdAt": "2026-11-05T04:34:08.765Z",
    "updatedAt": "2026-11-05T04:34:11.946Z",
    "archived": False,
    "url": "https://app.hubspot.com/contacts/51780263/record/0-1/260560930060",
}

# The same contact before any merge: HubSpot leaves hs_merged_object_ids null, so this
# record speaks only for itself. SURVIVOR_RECORD is what it looks like after absorbing
# LIVE_RECORD, and a read cannot return both.
PRE_MERGE_RECORD: dict[str, Any] = {
    **SURVIVOR_RECORD,
    "properties": {**SURVIVOR_RECORD["properties"], "hs_merged_object_ids": None},
    "updatedAt": "2026-11-05T04:34:08.765Z",
}

LIVE_RECORD: dict[str, Any] = {
    "id": "260546473623",
    "properties": {
        "email": "sandbox-b@example.com",
        "gp_person_id": None,
        "hs_merged_object_ids": None,
        "hs_object_id": "260546473623",
        "firstname": "sandbox-b",
        "lastname": None,
    },
    "createdAt": "2026-11-05T04:34:09.280Z",
    "updatedAt": "2026-11-05T04:34:09.280Z",
    "archived": False,
    "url": "https://app.hubspot.com/contacts/51780263/record/0-1/260546473623",
}

READ_ENVELOPE: dict[str, Any] = {
    "completedAt": "2026-11-05T04:34:10.735Z",
    "status": "COMPLETE",
    "startedAt": "2026-11-05T04:34:10.695Z",
}

ABSENT_ID = "999999999999"

# A single absent id answers 207 with an empty result set and the id named in the error.
ABSENT_READ: dict[str, Any] = {
    **READ_ENVELOPE,
    "results": [],
    "errors": [
        {
            "status": "error",
            "category": "OBJECT_NOT_FOUND",
            "message": (
                "Could not get some CONTACT objects, they may be deleted or not exist. "
                "Check that ids are valid."
            ),
            "context": {"ids": [ABSENT_ID]},
        }
    ],
    "numErrors": 1,
}

ACCOUNT_INFO: dict[str, Any] = {
    "accountType": "SANDBOX",
    "companyCurrency": "USD",
    "dataHostingLocation": "na1",
    "portalId": 51780263,
    "timeZone": "US/Eastern",
    "uiDomain": "app.hubspot.com",
    "utcOffset": "-05:00",
    "utcOffsetMilliseconds": -18000000,
}

# A successful merge answers the survivor's whole record, with the absorbed id named in
# hs_merged_object_ids and hs_calculated_merged_vids.
MERGED_RECORD: dict[str, Any] = {
    "id": "260560930060",
    "properties": {
        **SURVIVOR_RECORD["properties"],
        "hs_additional_emails": "sandbox-b@example.com",
        "hs_calculated_merged_vids": "260560930060:1793853251946;260546473623:1793853251946",
    },
    "createdAt": "2026-11-05T04:34:08.765Z",
    "updatedAt": "2026-11-05T04:34:11.946Z",
    "archived": False,
    "url": "https://app.hubspot.com/contacts/51780263/record/0-1/260560930060",
}

# HubSpot refuses any merge on a secondary that is already merged away, either side.
FORWARD_REFERENCE_400: dict[str, Any] = {
    "status": "error",
    "message": (
        "Cannot merge ObjectCoordinates{portalId=51780263, "
        "objectTypeId=ObjectTypeId{legacyObjectType=CONTACT}, objectId=260546473623} "
        "because it has a forward reference to 260560930060. Only canonical objects can be merged."
    ),
    "correlationId": "01a1a9fc-68a4-71ea-bcce-1fa18b789288",
    "category": "VALIDATION_ERROR",
}


def json_headers(**extra: str) -> dict[str, str]:
    return {"Content-Type": "application/json", **extra}


@dataclass
class FakeTransport:
    """Replays queued responses in order, or one response forever, and records every call."""

    responses: Sequence[HttpResponse]
    calls: list[dict[str, Any]] = field(default_factory=list)

    def request(
        self,
        method: str,
        url: str,
        *,
        body: Mapping[str, Any] | None,
        headers: Mapping[str, str],
        timeout: float,
    ) -> HttpResponse:
        self.calls.append({"method": method, "url": url, "body": body, "headers": dict(headers)})
        index = min(len(self.calls) - 1, len(self.responses) - 1)
        return self.responses[index]


class FakeClock:
    """A monotonic clock that only moves when something sleeps."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds
