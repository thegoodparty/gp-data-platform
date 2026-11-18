"""End to end against the HubSpot sandbox: `uv run pytest -m sandbox`.

Needs HUBSPOT_SERVICE_KEY set to a sandbox service key. The portal check refuses any other
portal. Every contact is created here and archived at the end.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import pytest
import requests

from hsmerge.client import HubSpotClient
from hsmerge.config import Config
from hsmerge.execute import execute, verify
from hsmerge.types import Pair, Status
from tests._fakes import FakeLog

pytestmark = pytest.mark.sandbox

SANDBOX_PORTAL_ID = "51780263"
CONTACTS_URL = "https://api.hubapi.com/crm/v3/objects/contacts"


class Sandbox:
    def __init__(self, token: str):
        self._headers = {"Authorization": f"Bearer {token}"}
        self._tag = f"hsm-test-{uuid.uuid4().hex[:8]}"
        self.created: list[str] = []

    def contact(self, label: str) -> str:
        email = f"{self._tag}-{label}@example.com"
        response = requests.post(
            CONTACTS_URL,
            json={"properties": {"email": email, "firstname": self._tag, "lastname": label}},
            headers=self._headers,
            timeout=30,
        )
        response.raise_for_status()
        contact_id = str(response.json()["id"])
        self.created.append(contact_id)
        return contact_id

    def archive(self, contact_id: str) -> None:
        requests.delete(f"{CONTACTS_URL}/{contact_id}", headers=self._headers, timeout=30)

    def archive_all(self) -> None:
        for contact_id in self.created:
            self.archive(contact_id)


@pytest.fixture
def sandbox() -> Iterator[tuple[Sandbox, HubSpotClient]]:
    token = os.environ.get("HUBSPOT_SERVICE_KEY")
    if not token:
        pytest.skip("HUBSPOT_SERVICE_KEY is not set")
    config = Config(
        hubspot_token=token,
        hubspot_base_url="https://api.hubapi.com",
        expected_portal_id=SANDBOX_PORTAL_ID,
        object_type="contacts",
        log_table="unused.unused.unused",
        max_rps=5,
        databricks_host="unused",
        databricks_http_path="unused",
    )
    client = HubSpotClient(config)
    client.verify_portal()
    contacts = Sandbox(token)
    try:
        yield contacts, client
    finally:
        contacts.archive_all()


def test_every_status_the_read_can_reach(sandbox: tuple[Sandbox, HubSpotClient]) -> None:
    contacts, client = sandbox
    c = {label: contacts.contact(label) for label in ["p1", "s1", "p2", "s2", "p3", "s3", "x3", "p4", "s4"]}
    c |= {label: contacts.contact(label) for label in ["p5", "s5", "x5", "p6", "s6"]}
    client.merge(c["p2"], c["s2"])
    client.merge(c["x3"], c["s3"])
    client.merge(c["x5"], c["p5"])
    contacts.archive(c["s4"])
    contacts.archive(c["p6"])
    pairs = [Pair(c[f"p{n}"], c[f"s{n}"]) for n in range(1, 7)]
    log = FakeLog()

    execute(pairs, client, log)

    outcomes = {entry.pair.secondary_id: entry.outcome for entry in log.rows}
    assert {label: outcomes[c[label]].status for label in ["s1", "s2", "s3", "s4", "s5", "s6"]} == {
        "s1": Status.MERGED,
        "s2": Status.ALREADY_MERGED,
        "s3": Status.MERGED_ELSEWHERE,
        "s4": Status.SECONDARY_MISSING,
        "s5": Status.PRIMARY_MERGED_AWAY,
        "s6": Status.PRIMARY_MISSING,
    }
    assert outcomes[c["s1"]].survivor_id == c["p1"]
    assert outcomes[c["s3"]].survivor_id == c["x3"]
    assert list(verify([Pair(c["p1"], c["s1"])], client)) == []
