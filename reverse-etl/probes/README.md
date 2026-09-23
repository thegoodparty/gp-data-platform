# Sandbox checks

One-off probes that settle what HubSpot actually does for the behaviors the reverse-ETL design
assumes. They are run by hand against the sandbox portal before enabling the flow against
production, and again whenever HubSpot changes something that touches the batch upsert.

These are not tests. They make real writes, they answer questions rather than assert, and nothing
runs them on a schedule.

## Running

```bash
cd reverse-etl
uv sync
export RETL_PROBE_TOKEN=pat-na1-...   # a service key on the sandbox portal
uv run python -m probes.checks --all
```

`RETL_HUBSPOT_TOKEN`, retl's own variable, is accepted as a fallback, so a `.env` already
configured to run the app against the sandbox runs the checks too. The portal guard below, not
the variable name, is what keeps a production credential out.

One check at a time: `uv run python -m probes.checks --check 4`.

Output is markdown, ready to paste into the findings doc. `--json` gives the raw findings
instead. `--no-cleanup` leaves the created contacts in place when you want to look at them in
the HubSpot UI.

## Safety

The client's first call is always `/account-info/v3/details`, and it refuses to run if the
credential does not open the expected portal. A service key carries no portal id in its text, so
asking the API is the only way to know which one a token opens, and every check writes.

The expected portal defaults to the sandbox. Overriding it is deliberate:

```bash
RETL_PROBE_EXPECTED_PORTAL_ID=<id> uv run python -m probes.checks --all
```

Every contact a check creates is tagged `probe-<run id>-<label>` in `firstname` and archived at
the end of the run, so a crashed run leaves debris you can find and remove by hand.

## Credential

Use a service key dedicated to this work, not a shared one. HubSpot's per-key API log is the only
record of what the checks did, and a shared key interleaves another integration's traffic into it.
Scopes needed: `crm.objects.contacts.read`, `crm.objects.contacts.write`,
`crm.schemas.contacts.read`, `crm.schemas.contacts.write`.

Service keys replace legacy private apps, which can no longer be created after 26 Oct 2026 and
lose support in Sept 2027. The auth header is identical, so retl itself needs no change.

## What is here, and what is not

Checks 1 to 8 and 10 are API-only and live in `checks.py`. Check 9 (sales-owned omission end to
end) is not here: it needs the sandbox contact mirror and the desired-state model, so it is run
through `retl` itself rather than as a probe.

Check 3 covers `lastmodifieddate` and property history but not workflow re-triggering. Sandboxes
do not inherit production workflows, so that part needs a workflow built in the sandbox by hand,
or an explicit note that the sandbox had none.
