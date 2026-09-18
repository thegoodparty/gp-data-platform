"""Trigger the DSAR delete operation as a dbt Cloud job and wait for it.

The dbt Cloud CLI runs in your development environment with your own Databricks
credentials, which can count but not delete from the raw sources. A deployment job
runs as the dbt Cloud service principal, which can. This script starts that job over
the Admin API with the operation's arguments as a step override, follows it to
completion, and prints the operation's per-table log lines.

    python .claude/skills/dsar-deletion/run_deletes.py                        # dry run, whole register
    python .claude/skills/dsar-deletion/run_deletes.py --request-id DATA-XXXX # dry run, one request
    python .claude/skills/dsar-deletion/run_deletes.py --request-id DATA-XXXX --apply

Reads host, account and token from ~/.dbt/dbt_cloud.yml, the same file the CLI uses,
so there is nothing new to configure. Stdlib only.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

JOB_NAME = "DSAR apply deletes"
TERMINAL = {10: "success", 20: "error", 30: "cancelled"}
STEP_TERMINAL = {10, 20, 30}


def cli_context() -> tuple[str, str, str]:
    """Return (host, account_id, token) from the dbt Cloud CLI config."""
    text = Path("~/.dbt/dbt_cloud.yml").expanduser().read_text()
    fields: dict[str, str] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, _, value = line.strip().partition(":")
        fields.setdefault(key.strip(), value.strip().strip('"'))
    try:
        return fields["account-host"], fields["account-id"], fields["token-value"]
    except KeyError as exc:
        sys.exit(f"~/.dbt/dbt_cloud.yml is missing {exc}; run `dbt cloud login` first")


class Client:
    def __init__(self, host: str, account_id: str, token: str) -> None:
        self.base = f"https://{host}/api/v2/accounts/{account_id}"
        self.headers = {"Authorization": f"Token {token}", "Content-Type": "application/json"}

    def get(self, path: str) -> dict:
        return self._call("GET", path, None)

    def post(self, path: str, body: dict) -> dict:
        return self._call("POST", path, body)

    def _call(self, method: str, path: str, body: dict | None) -> dict:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(f"{self.base}{path}", data=data, headers=self.headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.load(resp)["data"]
        except urllib.error.HTTPError as exc:
            sys.exit(f"dbt Cloud API {method} {path} failed: {exc.code} {exc.read().decode()[:500]}")


def find_job(client: Client, name: str) -> dict:
    # The UI keeps stray whitespace in a saved name; do not let that hide the job.
    jobs = [j for j in client.get("/jobs/") if j["name"].strip() == name.strip()]
    if not jobs:
        sys.exit(f"No dbt Cloud job named {name!r}. Create it in the Prod deployment environment first.")
    if len(jobs) > 1:
        sys.exit(f"{len(jobs)} jobs named {name!r}; rename so the name is unique.")
    return jobs[0]


def operation_step(dry_run: bool, request_id: str | None) -> str:
    args: dict[str, bool | str] = {"dry_run": dry_run}
    if request_id:
        args["request_id"] = request_id
    return f"dbt run-operation dsar_apply_deletes --args '{json.dumps(args)}'"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--request-id", help="Scope to one register request, e.g. DATA-1234")
    parser.add_argument("--apply", action="store_true", help="Delete. Without this the job only counts.")
    parser.add_argument("--job-name", default=JOB_NAME, help=f"dbt Cloud job to run (default: {JOB_NAME!r})")
    parser.add_argument("--poll", type=int, default=20, help="Seconds between status checks")
    parser.add_argument(
        "--git-branch",
        help="Run the job against this branch instead of the job's default. For trying a branch before merge.",
    )
    args = parser.parse_args()

    host, account_id, token = cli_context()
    client = Client(host, account_id, token)
    job = find_job(client, args.job_name)
    step = operation_step(dry_run=not args.apply, request_id=args.request_id)
    cause = f"DSAR {'delete' if args.apply else 'dry run'}" + (
        f" for {args.request_id}" if args.request_id else ""
    )

    body: dict[str, object] = {"cause": cause, "steps_override": [step]}
    if args.git_branch:
        body["git_branch"] = args.git_branch
        cause += f" ({args.git_branch})"
    run = client.post(f"/jobs/{job['id']}/run/", body)
    run_url = f"https://{host}/deploy/{account_id}/projects/{job['project_id']}/runs/{run['id']}"
    print(f"{cause}: job {job['id']} run {run['id']}\n  {step}\n  {run_url}", flush=True)

    while run["status"] not in TERMINAL:
        time.sleep(args.poll)
        run = client.get(f"/runs/{run['id']}/?include_related=[run_steps]")
        print(f"  ... {run.get('status_humanized', run['status'])}", flush=True)

    print(f"\nrun {run['id']} finished: {TERMINAL[run['status']]}")
    for s in run.get("run_steps", []):
        if "dsar_apply_deletes" not in s.get("name", "") or s.get("status") not in STEP_TERMINAL:
            continue
        # Keep the operation's own lines and anything that explains a failure; drop
        # dbt's parse-time deprecation chatter, which also uses backticks.
        for line in (s.get("logs") or "").splitlines():
            text = line.split("  ", 1)[-1] if line[:2].isdigit() else line
            is_count = text.lstrip()[:1].isdigit() and "`" in text
            is_error = any(
                k in text for k in ("Error", "error:", "Encountered", "not found", "does not exist")
            )
            if "dsar_apply_deletes:" in text or is_count or is_error:
                print(text)
    return 0 if run["status"] == 10 else 1


if __name__ == "__main__":
    sys.exit(main())
