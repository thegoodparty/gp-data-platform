# Calibration logs

Part of the **win-analytics-process** skill. The closing step that makes the pipeline self-correct.

After a substantive analysis run, write findings that should update the docs or agents into a
dated calibration log at `analytics/runbook/CALIBRATION_<YYYY-MM-DD>.md`. Hand-process the log
into doc/agent edits before treating it as resolved. This is a **required closing step** of
every substantive analysis (see the verification protocol in [methodology.md](methodology.md)):
either produce the log and distribute it, or explicitly record that no calibration was needed.
It's what makes the process self-correct rather than relying on someone remembering.

Calibration logs are personal working documents — they are gitignored
(`analytics/runbook/CALIBRATION_*.md`) so each analyst's working tree can carry them without
affecting the shared repo. The durable team-shared output is the doc + agent edits the log drives.

## Distribution mapping

Each finding lands in the file that *owns* it; the log itself is disposable. Use this routing:

| Finding type | Lands in |
|---|---|
| How framing scopes / verifies (data-existence checks, metric-semantics steps) | `.claude/skills/win-analytics-process/references/framing.md` |
| How the executor builds notebooks (working-set pattern, mandatory checks) | executor instructions / `analytics/lib` |
| How the DS reviews / interprets (leakage classes, calibration self-checks) | `.claude/agents/product-data-scientist.md` |
| Data facts, joins, gotchas, metric definitions, coverage | the owning doc in the **win-analytics-knowledge** skill (`.claude/skills/win-analytics-knowledge/references/`) |

Tag each finding **universal** (codify freely) vs **data-state** (hedge, or wait 2-3 cycles),
and prefer sharpening an existing rule over adding a new one — per the cautions below.

## Beware over-calibration

A single-analysis log surfaces failure modes specific to that run's data state alongside
general principles. When promoting findings into doc or agent edits, distinguish between the two:
universal hygiene (data-existence checks, CIs, documentation) can be codified freely;
data-state-dependent defaults (cohort filters, metric choices, funnel decompositions) should
carry hedges noting current state, or wait for confirmation across two or three calibration
cycles before being treated as settled rules. Tightening a rule "so this exact failure can't
happen again" often encodes the failure mode rather than the underlying principle.

## Beware doc bloat

Even genuinely general principles compete for the reader's attention budget. A reference doc
is useful only if a framer can hold the relevant subset in working memory; past some point,
adding rules makes following the doc harder, not better — the analog of an over-parameterized
model losing generalization. When applying a calibration finding, prefer sharpening or
generalizing an existing rule over adding a new one. If a doc starts to feel exhaustive rather
than usable, that's a signal to consolidate, not extend.

## Cross-references

- [methodology.md](methodology.md) — the verification protocol that requires this closing step.
- [pipeline.md](pipeline.md) — which stage owns which calibration finding.
