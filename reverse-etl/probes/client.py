"""HubSpot client for the sandbox checks, with a hard guard against the wrong portal.

Every check writes to HubSpot, so the first call this client makes is always
`/account-info/v3/details` to confirm which portal the credential actually opens.
A service key carries no portal in its text, so asking the API is the only way to
know; without it a production key pasted into the wrong env var would run the whole
destructive suite against real contacts.

Contacts the checks create are tagged with a run id in `firstname` and tracked for
cleanup, so a crashed run leaves findable debris rather than anonymous junk.
"""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import requests

CLAUDE_TEST_ZONE_PORTAL_ID = 51780263
DEFAULT_BASE_URL = "https://api.hubapi.com"
TOKEN_ENV = "RETL_PROBE_TOKEN"
PORTAL_ENV = "RETL_PROBE_EXPECTED_PORTAL_ID"
REQUEST_TIMEOUT = 30.0


class ProbeConfigError(RuntimeError):
    pass


class WrongPortalError(RuntimeError):
    """Refuses to run rather than write to a portal nobody sanctioned."""

    def __init__(self, expected: int, actual: int):
        super().__init__(
            f"credential opens portal {actual}, expected {expected}. "
            f"Refusing to run: set {PORTAL_ENV} deliberately if this is intended."
        )


@dataclass
class Response:
    status_code: int
    body: dict[str, Any]
    headers: dict[str, str]


@dataclass
class SandboxClient:
    token: str
    base_url: str = DEFAULT_BASE_URL
    expected_portal_id: int = CLAUDE_TEST_ZONE_PORTAL_ID
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    created_contact_ids: list[str] = field(default_factory=list)

    @classmethod
    def from_env(cls) -> SandboxClient:
        token = os.environ.get(TOKEN_ENV, "")
        if not token:
            raise ProbeConfigError(f"{TOKEN_ENV} is not set")
        portal = int(os.environ.get(PORTAL_ENV) or CLAUDE_TEST_ZONE_PORTAL_ID)
        client = cls(
            token=token,
            base_url=os.environ.get("RETL_PROBE_BASE_URL", DEFAULT_BASE_URL),
            expected_portal_id=portal,
        )
        client.verify_portal()
        return client

    def verify_portal(self) -> int:
        response = self.request("GET", "/account-info/v3/details")
        actual = int(response.body.get("portalId", -1))
        if actual != self.expected_portal_id:
            raise WrongPortalError(self.expected_portal_id, actual)
        return actual

    def request(self, method: str, path: str, *, json: dict[str, Any] | None = None) -> Response:
        response = requests.request(
            method,
            f"{self.base_url}{path}",
            json=json,
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=REQUEST_TIMEOUT,
        )
        try:
            body = response.json()
        except ValueError:
            body = {}
        return Response(response.status_code, body, dict(response.headers))

    # --- fixtures -------------------------------------------------------

    def tag(self, label: str) -> str:
        """A value unique to this run, so leftovers are traceable to one invocation."""
        return f"probe-{self.run_id}-{label}"

    def create_contact(self, properties: dict[str, Any], *, label: str = "x") -> str:
        """Create a contact and remember it for cleanup. Returns its HubSpot id."""
        props = {"firstname": self.tag(label), **properties}
        response = self.request("POST", "/crm/v3/objects/contacts", json={"properties": props})
        if response.status_code >= 300:
            raise RuntimeError(f"fixture create failed ({response.status_code}): {response.body}")
        contact_id = str(response.body["id"])
        self.created_contact_ids.append(contact_id)
        return contact_id

    def get_contact(self, contact_id: str, properties: list[str]) -> dict[str, Any]:
        query = ",".join(properties)
        response = self.request("GET", f"/crm/v3/objects/contacts/{contact_id}?properties={query}")
        return response.body.get("properties", {})

    def property_history(self, contact_id: str, prop: str) -> list[dict[str, Any]]:
        response = self.request("GET", f"/crm/v3/objects/contacts/{contact_id}?propertiesWithHistory={prop}")
        return response.body.get("propertiesWithHistory", {}).get(prop, [])

    def cleanup(self) -> int:
        """Archive every contact this run created. Best effort: a failure here must not
        mask a check's finding, and the run tag makes survivors findable by hand."""
        removed = 0
        for contact_id in reversed(self.created_contact_ids):
            response = self.request("DELETE", f"/crm/v3/objects/contacts/{contact_id}")
            if response.status_code < 300:
                removed += 1
        self.created_contact_ids.clear()
        return removed


def settle(seconds: float = 1.0) -> None:
    """HubSpot reads are not immediately consistent with writes; several checks compare
    written state against a read-back and would otherwise report a false negative."""
    time.sleep(seconds)
