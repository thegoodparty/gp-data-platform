# L2-to-BallotReady district matcher

Matches BallotReady offices to L2 districts. Reads its worklist
(`int__l2_br_match_pending_offices`) and its menu source
(`int__l2_district_universe`) from Databricks, holds embeddings in memory for
the run, and returns terminal results. The matcher itself writes nothing --
`l2_br_match_writer.py` holds the Databricks write path.

## Directory structure

```
prod_gold_data/
├── l2_br_matcher.py           # The matcher: class L2BrMatcher
├── l2_br_match_schema.py      # Schema of record for llm_l2_br_match_results
├── l2_br_match_writer.py      # The write path: class MatchResultWriter
└── vector_store_generator.py  # Unrelated laptop tool, out of scope here --
                                # still feeds bronze_data's pickle-based path
```

## Running

```bash
uv run stitch_golden_data/prod_gold_data/l2_br_matcher.py --states DE --limit 100
```

`--states` limits to specific state codes; omit it to process every state
present in the pending worklist. `--limit` caps how many pending offices are
read (must be positive). `--batch-size` controls how many offices are matched
concurrently per group (default 100); `--embedding-batch-size` controls how
many district texts go into one `create_embeddings` call when building the
universe (default 100) -- the two are unrelated knobs for unrelated
workloads, not one shared setting.

Before either query embedding or the LLM runs, each office is classified on
its BallotReady geography fields (`mtfcc`, `geo_id`, the `sub_area` pair,
`is_judicial`, `has_unknown_boundaries`): a party-committee seat or a
judicial office the state's universe cannot serve at the right level
abstains outright, and everything else has its candidate pool narrowed to
the types its geography can actually be, before the frozen top-13/slot-11
mechanics rank it. `SCHOOL_WHOLE_ASSERTION_ENABLED` gates only the school
family's whole-jurisdiction denial off by default; the holdout adjudicates
flipping it. No field asserts a match on its own -- narrowing the pool and
adding prompt context is the whole extent of it.

## Terminal-outcome contract

A run produces two outcomes and no third -- this module writes nothing, so
"persists" belongs to the write path. A `MatchResult` whose
`l2_state` / `l2_district_type` / `l2_district_name` are populated is a match;
one where all three are `None` is an attempt that found nothing. There is no
status column on the result or in `llm_l2_br_match_results`, so a populated
district name is the whole signal. A technical error (an LLM or embedding call
raising, or a malformed LLM response) fails the run instead of being recorded
as a match or coerced into an abstention.

## The write path

`MatchResultWriter` (`l2_br_match_writer.py`) is the only writer of
`model_predictions.llm_l2_br_match_results`. Two operations, driven by hand
during the supervised cutover: append a batch of results under a run key, or
delete a run.

`attempted_at` is the run key, it must be **timezone-aware** (enforced, not
just documented -- a naive value silently splits one run into two), and the caller
passes it in so a sharded or resumed run keeps one key. `append_results` skips
offices already written under that key, validates what is left, inserts it in
chunks, then counts what the table holds for the key. Short means the run is
incomplete and `delete_run` is the repair; a *surplus* means another writer
touched the same key concurrently, which is a different problem and is
reported differently, because deleting there would destroy the other writer's
rows. One run key must have a single writer.

That count is what replaces a transaction: the connector has none, so a
failure part-way leaves an *incomplete* run rather than a corrupt one -- every
row stands on its own, and an office whose row never arrived is still on the
pending list. `delete_run(attempted_at)` returns how many rows it removed,
counted rather than asserted, since this connector hardcodes
`Cursor.rowcount = -1` and a delete that matched nothing would otherwise be
indistinguishable from one that worked.

`MatchResultWriter` has no CLI and no production caller in this repo. It is
driven by hand, or by the cutover runbook, which lives with the ticket rather
than here.

The publication-time label check -- re-testing every matched label
against a freshly rebuilt universe just before publication -- is deliberately
**not** here. It has to run after the warehouse rebuild in cutover step 5,
which this module cannot do, and dbt already ships the generic test for it
(`l2_district_tuple_exists`). It belongs to the serving PR's undated staging
model, pointed at `int__l2_district_universe`.

## Operations

Both Gemini clients retry every exception blindly (`max_retries=11`,
`base_delay=1.0`), so a run that hits a 429 wall stalls in backoff for up to
roughly 1,023s per call before surfacing anything. At `--batch-size 100`
against `max_connections=1200` that is usually per-minute throttling rather
than an exhausted daily quota: **lower `--batch-size` and rerun.** Making the
clients themselves fail fast means changing `shared/`, which every other
service in this package imports, and belongs with the containerization work.

## What is frozen

The matcher core -- the district and query embedding text, the menu
construction (top 13 by cosine, plus the bare "state" query inserted at
index 10), the LLM prompt, its response schema, and the Braintrust project
and prompt identifiers -- is an owner-decided constraint and is not touched
here.
