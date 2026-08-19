# Elected officials -> HubSpot contacts export (one-off, review-only)

Ad-hoc workflow that produces a CSV of current, non-major-party elected officials for the Sales
team to upload to HubSpot Contacts, with an existing `hubspot_contact_id` attached wherever the
person is already a HubSpot contact (so Sales updates rather than duplicates).

This directory is a one-off. It is NOT wired into Airflow, dbt production runs, or the matcha ER
pipeline. It is committed for review / quality-control of the method, not to be merged or
scheduled. The CSV output is PII-bearing and is never committed.

## The pieces

1. `dbt/project/analyses/elected_officials_hubspot_export.sql` (in the dbt project, not here)
   The core. A dbt *analysis* (compiled, never materialized). Applies scope, does the
   deterministic entity resolution (four match tiers), enforces a strict 1:1 EO<->contact link,
   and shapes the HubSpot-facing columns. This is what generated the final list.

2. `run_export.py` — runs the compiled analysis against the SQL warehouse and writes the CSV.

3. `fuzzy_er.py` — an EXPERIMENTAL Splink (DuckDB) fuzzy pass over the records the deterministic
   tiers miss. Evaluated, then DROPPED from the final deliverable after the audit found it leaked
   false positives (same name + state, different person/locality). Kept here so a reviewer can
   see the approach and why it was rejected.

4. `audit_sample.py` — QC. Stratified random sample of matched records; pulls the HubSpot side and
   runs a data-side identity check (does the attached contact's name/state match our person?).

5. `ballotready_staleness_check.py` — QC. Cross-checks flagged former/deceased holders against the
   live CivicEngine (BallotReady) API to determine whether "not currently serving" cases are a
   BallotReady data-quality gap vs. our ingest staleness.

## Running

All scripts resolve Databricks auth via the `analytics/lib/databricks_conn.py` helper
(`~/.databrickscfg` profile or M2M service principal). From the `analytics/` directory:

```
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<id>
uv run --with pandas --with databricks-sql-connector --with databricks-sdk \
        --with splink --with rapidfuzz --with requests python eo_hubspot_export/<script>.py
```

Output CSVs go to `$EO_EXPORT_DIR` (default: a local `output/` dir, gitignored). The CivicEngine
check reads `CIVICENGINE_API_TOKEN` from `~/.secrets`.

## Method summary (for reviewers)

See the PR description for the full walkthrough. In brief:
- Source of truth for the officeholder universe: `mart_civics.elected_officials` (one row per
  person, latest term). Scope: current term (not ended) + contactable + non-major-party.
- HubSpot side: `airbyte_source.hubspot_api_contacts`. Contacts carry no BallotReady person id,
  so matching leans on email/phone/name plus two id bridges (`br_candidacy_id`, gp_api user id).
- Final link uses deterministic tiers only. Audit confirmed 0 false positives in those tiers;
  the two lower-confidence approaches (exact name+state, Splink fuzzy) were dropped.
</content>
