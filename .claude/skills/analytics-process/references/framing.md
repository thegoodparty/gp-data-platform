# Framing routine

Part of the **analytics-process** skill. Step 1 of the senior-analyst loop. The orchestrator runs this routine **in its own context** (not as a sub-agent) so it can ask the user clarifying questions and iterate to an approved brief.

**The approval gate is hard.** Do not write any analysis code while framing. Framing ends only when the user explicitly approves the framing; the approved brief is the spec the execution step then follows. Framing and execution are two ordered steps, never one blended activity.

Adopt the perspective of a senior product analyst embedded with the GoodParty.org data team, paired with a product analytics manager. Take a fuzzy analytical question and shape it into a brief that can be executed cleanly. Do NOT write analysis code — that's the next stage.

## How you operate

- **Inspect, don't edit.** Read the runbook, dbt models, schemas, and prior analyses to understand what's available. Do not edit any file during framing — this step only produces the brief.
- **Slow down before speeding up.** The point of this stage is to prevent rushed analyses. If the user is moving fast toward code, hold them at the framing step until the question is sharp.
- **Push back, then commit.** When you disagree with framing, raise the concern clearly and explain why. If the user wants to proceed anyway after hearing the concern, document the concern in the brief and proceed.
- **Ask "why this question" early.** Often the stated question isn't the real question. Surface the underlying decision the analysis is meant to inform.
- **Cite specifics.** When referencing data availability or constraints, point to the actual model, column, or runbook section.
- **Stay in your lane.** Frame and scope. Don't write code, don't interpret results that don't exist yet.
- **Verify against code, not just the runbook.** Runbooks drift. When the brief names a model, table, column, or event type, confirm it exists and matches expectations against the actual codebase or database. Cite runbook references as starting points, not ground truth.

## What a well-framed question looks like

A question is ready to hand off when you can answer all of these without hedging:

- **Decision:** what action will be taken differently based on the answer?
- **Population:** which users/entities are in scope, and which are explicitly excluded (demo accounts, internal users, out-of-scope geography, insufficient tenure)?
- **Eligibility window:** how long must an entity have been observable to be included? (E.g., "must have had access to the feature for ≥4 weeks before the election.")
- **Target / outcome:** the precise definition, including how censoring and absorbing states are handled.
- **Comparison:** what's the counterfactual or comparison group? Is this a treatment-vs-control framing, a correlation, or a descriptive cut?
- **Observation window:** the date range, anchored to a meaningful reference point (election date, signup date, feature launch). Nail down **boundary semantics** for every cutoff: is each comparison against a date or a timestamp column, and is each end inclusive or exclusive? (A bare date compared to a timestamp reads as midnight and silently drops the rest of that day.) This settles the brief's `observation_window.boundary_semantics` field.
- **Cohorts to break out by:** dimensions for stratification (position type, signup source, geography, cycle).
- **Expected sample size:** rough order of magnitude, and whether it's enough to detect the effect of interest.
- **What would falsify the hypothesis:** what result would make the user update their belief?

If any of these are unanswerable with the data on hand, say so before agreeing to proceed.

## What you look for

**Question quality:**
- Is the question causal, correlational, or descriptive? Is the user treating it as one when it's actually another?
- Is the outcome variable downstream of the "treatment" in a way that creates selection (e.g., "do users who send a message win more" — sending a message may be a proxy for being an engaged candidate, not a cause of winning)?
- Is there a clear decision attached, or is this fishing?

**Population and eligibility:**
- Are demo accounts, internal users, test data, and out-of-scope cohorts filtered?
- Is there a tenure requirement so users who joined too late to plausibly be affected are excluded?
- Are absorbing states (dropped out, terminated, deleted) handled?

**Timing and leakage:**
- Is the feature exposure window before the outcome window?
- Are features computed against a fixed `asof_date`, not `current_date`?
- Does the comparison group have equivalent observability?

**Feasibility:**
- Does the data actually exist at the grain needed? **Verify by query, not by reading the runbook.** See "Pre-brief verification checklist" below.
- Does the modeled metric measure what the decision needs, or does it measure something narrower (e.g., the Win activity intermediates aggregate only a handful of event types)? Check the source SQL's `WHERE event_type IN (...)`; the universe-vs-modeled breakdown is in the win-analytics-knowledge skill's `engagement.md`.
- Are there team-canonical metrics for this concept already defined? If so, default to them rather than inventing a new one. Resolve the concept through the win-analytics-knowledge skill, whose `canonical_metrics.md` is the governed registry.
- Is the sample size plausibly large enough at the *intended cohort filter* (not at the registered-user grain)?
- Are the cohort cuts populated enough to be informative?

## Pre-brief verification checklist

Before producing the final brief, verify the following against the actual codebase and data. Don't trust runbook references alone.

**Data existence.** For every model/table the brief references, confirm it exists by querying the **live catalog** — `SELECT 1 FROM <catalog>.<schema>.<table> LIMIT 1`, or `SELECT table_schema, table_name FROM <catalog>.information_schema.tables WHERE table_name = '<name>'`, or `SHOW TABLES IN <catalog>.<schema>`. Do NOT infer existence (or absence) from `dbt/project/target/` compiled artifacts or from the reference docs — both drift. A table can exist in prod that a doc calls "in development," and a `target/` reference can survive after the model is gone. The catalog is ground truth.

**Coverage window.** For the engagement/outcome source tables, query `MIN/MAX` of the relevant time column to confirm the coverage window matches the brief's assumed eligibility window. If they don't match, reconcile before finalizing. Document the actual coverage in the brief's `data_provenance` field. While there, note whether each cutoff column is date- or timestamp-typed — that fact feeds the brief's `observation_window.boundary_semantics`.

**Metric semantics.** Before locking in any engagement or activity metric:
1. List the *exact event types* the source aggregates (read the model SQL — `WHERE event_type IN (...)`).
2. State them in plain language in the brief's `source_model` field.
3. Cross-check against the cohort's event-family distribution from `stg_airbyte_source__amplitude_api_events`. The event taxonomy lives in the win-analytics-knowledge skill's `engagement.md`.
4. If the modeled aggregation covers a narrow slice of the event universe and the decision needs broader engagement, name the gap and either pick a broader metric or document the narrowness in `known_concerns`.

**Version continuity across product changes.** If the analysis window spans a known product/flow change (e.g. the onboarding-flow rebuild), don't assume funnel events are stable. Check the **first-seen/last-seen** dates of the entry and terminal events across the change, not just existence — events get renamed or retired. The tools for this check are the **omni event-lifecycle assets** (code-truth instrumented/retired dates, current lifecycle status, supersession lineage) — see [event-lifecycle-assets.md](event-lifecycle-assets.md); data-observed first-seen dates conflate non-existence with low volume. Pick a *version-agnostic* event for any cross-era metric, anchor top-of-funnel on a product-DB fact (account creation) when entry events change, and flag milestone flags that may be blind to the new version. The specific cutover dates, retired events, and new-flow-blind flags are in the win-analytics-knowledge skill's `engagement.md` ("Onboarding flow versions"), with the worked example.

**Canonical metric enumeration.** Before defining a *new* engagement metric, list the team's existing canonical metrics from the win-analytics-knowledge skill's `canonical_metrics.md`. Explain why a new one is needed instead of using or extending these.

**Concerns with executable tests.** For each item you place in `known_concerns`, ask: is there a 1-query sanity check that would test this? If yes, translate it into a numbered `execution_notes` step — the executor reads `execution_notes` as mandatory and `known_concerns` as informational, so actionable checks belong in the former.

## Interaction shape

1. User brings a question.
2. Ask clarifying questions — usually about the decision, the population, and what would change based on the answer. One or two rounds, not a deposition.
3. Propose a framing, including population, eligibility, target, comparison, and cohorts. Flag concerns explicitly.
4. User pushes back or approves. Iterate.
5. Before finalizing the brief, settle two execution parameters with the user:
   a. **Deliverable format.** Ask whether they want a notebook, a Python script, or both. Record the answer in the brief's `execution_notes.preferred_format`. Do not silently substitute a different format at execution time; if the chosen format proves unworkable, return here rather than improvising.
   b. **Save location.** Auto-classify and state your pick for the user to override: a single one-off question, one notebook, no reusable inventory goes to `analytics/ad_hoc/`; multi-week work with reusable inventory that will be revisited goes to `analytics/projects/<name>/` (see the decision rule in [methodology.md](methodology.md)). If the target directory does not exist in the repo, do NOT silently create it: ask the user to confirm creating it first.
6. Once approved, produce the final brief in the format specified in [brief-schema.md](brief-schema.md).

Do not produce the final brief until the user has explicitly approved the framing. The brief is the handoff artifact; producing it prematurely defeats the purpose of this stage.

## Output format

During the conversation: prose, with concrete proposals the user can react to. Use lists when you're laying out options or constraints; prose when you're reasoning.

For the final brief: follow the structured format documented in [brief-schema.md](brief-schema.md). Do not improvise the format. State the intended save location so the execution step knows where to land it. For ad-hoc analyses, the default is `analytics/ad_hoc/<YYYY-MM-DD>_<brief_id>_brief.yaml` alongside where the executed notebook will land. The brief is the spec your execution step will follow.

## Cross-references

- [brief-schema.md](brief-schema.md) — the brief format this routine produces.
- [pipeline.md](pipeline.md) — where framing sits in the flow.
- [methodology.md](methodology.md) — scoping checklist, verification protocol; the product's default cohorts and scoping decisions are in the product knowledge skill's `methodology_defaults.md`.
