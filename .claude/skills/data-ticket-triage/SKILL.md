---
name: data-ticket-triage
description: Investigate a ClickUp data ticket end to end and ship the fix — read the ticket, trace the symptom through the dbt DAG in Databricks, post a root-cause note on the ticket, implement the smallest fix that reuses an existing repair pattern, verify in dev, open the PR, and iterate with the delegate reviewer until it approves. Use when given a ClickUp ticket ID or URL (DATA-XXXX) and asked to investigate, diagnose, or resolve it — production data bugs, missing or wrong records in a mart, a value the product should show but does not.
---

# Data ticket triage

End-to-end workflow for a ClickUp data ticket. The seven steps below are the whole job; do all of them unless told to stop early.

The hard part is almost never the fix. It is finding the exact model where the data stops existing, and resisting the urge to generalize before you know how isolated the case is.

## 1. Read the ticket

```
mcp__clickup__clickup_get_task(task_id="DATA-XXXX", include=["description"])
mcp__clickup__clickup_get_task_comments(task_id="DATA-XXXX")
```

Read the reporter's hypothesis but do not inherit it. Tickets routinely name the wrong layer ("the district is missing") when the break is somewhere else entirely. Treat the described symptom as fact and the diagnosis as a guess.

Note the concrete identifiers: a customer email, a place, an office name, a race. You need at least one to anchor the trace.

## 2. Trace the symptom through the DAG

Find the **last model that has the record and the first that does not**. That boundary is the root cause; everything upstream of it is a distraction.

Work backwards from the surface the user complained about, not forwards from the source. Read the model that serves the symptom, list its inputs, and check each. Then recurse into whichever one is empty. Usually 3-6 queries.

Query prod relations directly with the dbt Cloud CLI from `dbt/project/`:

```bash
dbt show --inline "select ... from {{ ref('some_model') }} where ..." --output json --limit 20
```

`--output json` is much easier to read than the default table, which truncates columns to `...`.

At each hop ask: is the record absent, or present with a null join key? A null FK that a downstream inner join drops is the most common shape of these bugs, and it looks identical to "missing data" from the outside.

Once you find the boundary, **quantify how isolated it is** before choosing a fix. This decides everything in step 4:

```sql
-- how many rows have this defect, and does the defect cluster?
select <suspect_dimension>, count(*) as n
from {{ ref('the_model_where_it_breaks') }}
where <the_null_or_missing_condition>
group by <suspect_dimension>
```

One row nationwide is a data-entry gap and takes a seeded override. Thousands clustered on one dimension is a modeling bug and takes a code change. Do not guess which you have.

## 3. Read the consuming application when the mart looks fine

If the mart has the data and the product still does not show it, the filter is in the app. The product monorepo is the `omni` repository: https://github.com/thegoodparty/omni

| Package | What it tells you |
|---|---|
| `election-api` | Serves races, positions, places, districts, the office picker. `prisma/schema/*.prisma` shows required FKs and unique constraints — this is *why* a mart inner-joins and drops rows. `src/<domain>/*.service.ts` shows the real query, including thresholds and date windows the mart knows nothing about. |
| `gp-api` | Win product backend — campaigns, onboarding, path-to-victory, voters. |
| `gp-webapp` | Frontend. Useful for what the user literally saw. |
| `gp-admin`, `candidate-sites` | Other surfaces. |
| `contracts`, `gp-sdk`, `nest-common` | Shared types and clients. |
| `runbooks` | Operational procedures. |

Read-only. Never edit these from a data-platform ticket without user confirmation first.

Two things worth checking every time: the service may read a *different* table than the ticket names (e.g. it uses one table only to collect IDs, then reads the real rows from another), and it may apply an env-var threshold or a `>= today` window that silently excludes the record. Both change the diagnosis.

The dbt marts are the source of truth for what the writer will publish, so a mart query is usually sufficient evidence without touching Postgres at all.

## 4. Choose the smallest fix

**Reuse an existing repair pattern if one exists. Do not build a generalized solution when we have no generalized pattern for the problem.** See `references/repair-patterns.md` for the catalog of override seeds and the chains they repair.

Order of preference:

1. A row in an existing override seed. Zero new machinery.
2. A new override seed following the established shape (sparse match tuple or key + corrected value + a documented `reason` column), plus a `coalesce` at the point of use.
3. A code change to the model logic — only when step 2's quantification showed the defect is systemic, not a single data gap.

Key the override at the grain the defect actually lives at, which is often coarser than the reporting grain. Keying at geography rather than position, for instance, fixes every position in that geography and any the vendor adds later, from one row.

If the fix touches an incremental model, a seed edit will not propagate: the source row's `updated_at` predates the watermark. Either re-emit override-matched rows on every run (`or <override>.<col> is not null` in the incremental filter — the established pattern) or document the required `--full-refresh`. Re-emitting is preferable when the model is large.

Add a test only for a real failure mode, not for coverage. The two that matter for an override seed: a `relationships` test so a typo'd key fails loudly, and an `expression_is_true` asserting the override actually reached the target grain — otherwise a stale watermark or a bad key leaves it a silent no-op that looks fixed.

## 5. Post the root cause on the ticket

Do this **before** implementing, so the finding is recorded even if the fix takes another pass.

```
mcp__clickup__clickup_create_comment(task_id="DATA-XXXX", entity_id="<id>", entity_type="task", comment_text="...")
```

Write it as an engineer's note. Bullets are fine. Plain prose, no headers beyond a `Root cause` line, no bold-labeled sections, no emoji.

What it must contain:

- What is **not** wrong, when the ticket guessed wrong. Say so explicitly with the evidence that clears it.
- The actual boundary: which model, which column, which join.
- Why that boundary drops the row (the FK, the inner join, the filter).
- How isolated it is, with the count.
- One line on the intended fix.

Skip the narrative of how you found it.

## 6. Implement and verify in dev

Branch and PR conventions are in `dbt/project/CLAUDE.md` — read it. Summary: branch `data-XXXX/short-slug`, PR title `[DATA-XXXX] Short title`, and the ticket ID never appears in committed code, only in the branch name and PR title. No real colleague names anywhere in the repo.

Build only what you modified. Never use the `+` upstream selector — dbt Cloud defers to prod artifacts for everything you did not touch.

```bash
cd dbt/project
dbt seed --select <new_seed>
dbt build --select "<seed> <modified_model> <affected_mart>"
```

Then prove the change is surgical by diffing dev against prod:

```sql
select count(*) as differing_rows
from goodparty_data_catalog.dbt_<user>.<model> as dev
join goodparty_data_catalog.dbt.<model> as prod using (id)
where not (dev.<changed_column> <=> prod.<changed_column>)
```

Expect the count to equal the number of records you meant to fix. If it is larger, the fix is too broad.

Also list the rows present in dev but not prod (and vice versa) and account for every one. Unexplained rows usually mean a **stale relation in your dev schema** — dbt Cloud uses an existing dev table instead of deferring to prod, so an old build silently skews the diff. Check before concluding your change caused it.

Run the affected mart too, not just the model you edited, and confirm the record now appears where the user would see it.

## 7. Open the PR and clear review

Use the **`pull-request` skill** for this step — it covers the pre-commit gate, triggering the delegate reviewer, judging its findings, and reporting approval with CI state.

Two things specific to a ticket PR: the body leads with the root cause, the fix and why it is shaped that way, and a verification section with the actual numbers from step 6. And if a generalized process would help future tickets of this class, propose it in the PR description. Otherwise keep it short.

## dbt Cloud CLI gotchas

- **One invocation at a time.** A second concurrent call fails with `Session occupied`. Retry until the first finishes.
- **No `limit` inside inline SQL.** dbt appends its own; use `qualify row_number() over (order by ...) <= n`.
- **Long runs can drop the log stream** on a network blip. The invocation usually keeps running — check the dev relation before re-running.
- **Verify a column exists before selecting it.** Mart column names drift from what the docs and ticket call them.
