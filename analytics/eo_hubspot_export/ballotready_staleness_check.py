"""QC: is a 'not currently serving' case a BallotReady gap or our ingest staleness?

For a set of person ids (br_candidate_id) flagged by web verification as former/deceased, query
the live CivicEngine (BallotReady) GraphQL API and print each person's officeholder terms
(isCurrent / endAt / parties). If BallotReady's live API still reports them isCurrent=True with a
future endAt, the miss is a BallotReady data-quality gap, not our ingest -- our mart faithfully
reflects BR. Reads CIVICENGINE_API_TOKEN from ~/.secrets.

Pass person ids as CLI args, e.g.:
    uv run --with requests python eo_hubspot_export/ballotready_staleness_check.py 901253 384116
"""

import base64
import os
import re
import sys

import requests

URL = "https://bpi.civicengine.com/graphql"


def _token() -> str:
    with open(os.path.expanduser("~/.secrets")) as fh:
        for line in fh:
            m = re.match(r"\s*(export\s+)?CIVICENGINE_API_TOKEN\s*=\s*[\"']?([^\"'\n]+)", line)
            if m:
                return m.group(2).strip()
    raise SystemExit("CIVICENGINE_API_TOKEN not found in ~/.secrets")


def _encode(person_id: int) -> str:
    return base64.b64encode(f"gid://ballot-factory/Candidate/{person_id}".encode()).decode()


QUERY = """
query($ids:[ID!]!){
  nodes(ids:$ids){
    ... on Person {
      databaseId fullName updatedAt
      officeHolders { nodes {
        officeTitle isCurrent isVacant startAt endAt updatedAt
        parties { name } position { name state }
      } }
    }
  }
}
"""


def main() -> None:
    ids = [int(a) for a in sys.argv[1:]]
    if not ids:
        raise SystemExit("pass one or more person ids (br_candidate_id) as args")
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {_token()}"}
    res = requests.post(
        URL,
        json={"query": QUERY, "variables": {"ids": [_encode(i) for i in ids]}},
        headers=headers,
        timeout=60,
    ).json()
    if "errors" in res:
        raise SystemExit(f"API errors: {res['errors']}")

    nodes = {n["databaseId"]: n for n in res.get("data", {}).get("nodes", []) if n}
    for pid in ids:
        n = nodes.get(pid)
        print(f"\n=== person_id={pid} ===")
        if not n:
            print("  no person node returned")
            continue
        for oh in n.get("officeHolders", {}).get("nodes", []):
            pos = oh.get("position") or {}
            parties = [p.get("name") for p in (oh.get("parties") or [])]
            print(
                f"  - {oh.get('officeTitle') or pos.get('name')} [{pos.get('state')}] "
                f"isCurrent={oh.get('isCurrent')} start={oh.get('startAt')} "
                f"end={oh.get('endAt')} parties={parties}"
            )


if __name__ == "__main__":
    main()
