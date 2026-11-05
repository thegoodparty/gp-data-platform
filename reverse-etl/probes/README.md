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

`RETL_HUBSPOT_TOKEN`, retl's own variable, is accepted as a fallback. Nothing here loads a
`.env` file, so a sandbox `.env` already configured for the app needs `uv run --env-file .env
python -m probes.checks --all` rather than a bare `uv run`. The portal guard below, not the
variable name, is what keeps a production credential out.

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

Every contact a check creates is tagged `probe-<run id>-<label>` in `firstname`, every company
carries the same tag in `name`, and both are archived at the end of the run, so a crashed run
leaves debris you can find and remove by hand.

## Credential

Use a service key dedicated to this work, not a shared one. HubSpot's per-key API log is the only
record of what the checks did, and a shared key interleaves another integration's traffic into it.
Scopes needed: `crm.objects.contacts.read`, `crm.objects.contacts.write`,
`crm.schemas.contacts.read`, `crm.schemas.contacts.write`, plus
`crm.objects.companies.read` and `crm.objects.companies.write` for check 11's association trial.

Service keys replace legacy private apps, which can no longer be created after 26 Oct 2026 and
lose support in Sept 2027. The auth header is identical, so retl itself needs no change.

## What is here, and what is not

Checks 1 to 8 and 10 to 13 are API-only and live in `checks.py`. Check 9 (sales-owned omission
end to end) is not here: it needs the sandbox contact mirror and the desired-state model, so it is
run through `retl` itself rather than as a probe.

Check 3 covers `lastmodifieddate` and property history. Its workflow half was answered by hand and
is not automated here, because it needs a throwaway contact workflow built in the sandbox. Build
one rather than enabling an inherited workflow: the sandbox carries 50+ contact workflows cloned
from production, disabled, and the set includes SMS sends and Slack notifications that could fire
for real.

## Merging reviewed pairs

`merge_pairs.py` is not a check. It merges contact pairs named on the command line, taken from
`mart_sales_reverse_etl.hubspot_contact_merge_candidates` as `primary_hs_contact_id:hs_contact_id`,
and compares the survivor with what the primary-wins rule predicts. It is for the handful of test
merges that confirm the ranking before a bulk run.

```bash
uv run python -m probes.merge_pairs --pair 111:222                # dry run: reads, checks, predicts
RETL_PROBE_EXPECTED_PORTAL_ID=<id> uv run python -m probes.merge_pairs --pair 111:222 --execute
```

A dry run writes nothing. It skips a pair when either id no longer answers as itself, which is what
a contact already merged away does. A merge cannot be undone, and the same portal guard applies,
so a production run needs that portal named deliberately. Check 12 settles the precedence rule
itself on throwaway sandbox contacts.

## What the merge endpoint and a merged-away id answer

Check 13 builds throwaway contacts and records the five responses the contact merge job's
classification is built on. The design document says these confirm or correct its status table.
All five confirm it. The read's shape adds two details the classification has to be written
around.

**A merged-away id reads as its survivor, not as missing.** `batch/read` on the absorbed id
returns the survivor's record with status 200 and no error. It does not return an empty result
with `OBJECT_NOT_FOUND`. That is what makes `already_merged`, `merged_elsewhere` and
`secondary_missing` separable at all: the secondary resolving to the primary is `already_merged`,
to any other contact is `merged_elsewhere`, and `secondary_missing` is reserved for a contact that
is archived or never existed, which does read as `OBJECT_NOT_FOUND` in a 207 envelope, with the id
named in `errors[].context.ids`.

**A batch read answers per resolved object, not per id asked for.** The read of both ids of a
merged pair returns one record. The records carry no echo of the ids that were requested, and they
do not come back in the order they were asked for. A record is attributed to a pair through its
own id: a record whose id equals the id asked for means the contact is still itself, and the
merged-id list on a record names the ids that resolved to it.

**The merge call refuses a secondary that is merged away, with 400.** Already merged into the
primary, already merged into a third contact, and a primary that was itself merged away all answer
`400 VALIDATION_ERROR` and `Cannot merge ... because it has a forward reference to <id>. Only
canonical objects can be merged.` So none of the four resolved statuses can be read from the merge
response; the pre-merge read is the only thing that can name them, and a merge call attempted
anyway logs `failed` for a pair that earned something better.

**The merge call answers 404 for an absent id, without saying which side.** A missing primary and
a missing secondary produce the same `404 OBJECT_NOT_FOUND` and `Failed to find objects with
provided ids`. `primary_missing` and `secondary_missing` are therefore also the read's job.

**A successful merge returns the primary, and its whole record.** Status 200 with `id`,
`createdAt`, `updatedAt`, `archived`, `url` and the full property set, the id being the primary's.
The absorbed id appears in `hs_merged_object_ids` and `hs_calculated_merged_vids` on the returned
record. That is the primary ID preservation the design relies on, observed again rather than taken
on trust.
