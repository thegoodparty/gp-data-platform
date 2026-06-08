# Calibration logs

Part of the **win-analytics-process** skill. The closing step that makes the pipeline self-correct.

After a substantive analysis run, write findings that should update the docs or agents into a
dated calibration log at `analytics/runbook/CALIBRATION_<YYYY-MM-DD>.md`, then handle each finding
per its track (below). This is a **required closing step** of every substantive analysis (see the
verification protocol in [methodology.md](methodology.md)): either produce the log and process it,
or explicitly record that no calibration was needed. It's what makes the process self-correct
rather than relying on someone remembering.

Calibration logs are personal working documents: they are gitignored
(`analytics/runbook/CALIBRATION_*.md`) so each analyst's working tree can carry them without
affecting the shared repo. The durable team-shared output is the doc + agent edits the log drives.

**No core or shared file is ever edited without explicit human approval.** Approved edits land via
the branch/PR convention below, never on `main` directly and never mixed into an analysis branch.

## Two tracks

Calibration findings split into two tracks with different defaults:

### Track 1: data calibration (default, ON)

Reusable **data facts**: joins, gotchas, metric definitions, coverage, additions to the canonical
metrics list. These land in the owning doc of the **win-analytics-knowledge** skill
(`.claude/skills/win-analytics-knowledge/references/`). Runs on every substantive analysis.
Propose the exact edit; apply **only after explicit human approval**.

### Track 2: process calibration (default, OFF)

Findings that change the **process itself**: the framing routine ([framing.md](framing.md)), the
executor instructions / `analytics/lib`, the reviewer agents (e.g.
`.claude/agents/product-data-scientist.md`), or the process skill. On a standard run, do **not**
propose or apply these. Park them in the log under a **"Process-design candidates"** heading, and
in the closing summary state plainly that process calibration was **OFF (not run)** and offer to
rerun in process-design mode. Act on Track 2 only if the user has **explicitly opted into
process-design mode** this session.

| Finding | Track |
|---|---|
| Data facts, joins, gotchas, metric definitions, coverage, canonical-list additions | Track 1 (data) |
| How framing scopes / verifies; how the executor builds; how a reviewer agent reviews; process-skill wording | Track 2 (process) |

Tag each finding **universal** (codify freely) vs **data-state** (hedge, or wait 2-3 cycles),
and prefer sharpening an existing rule over adding a new one, per the cautions below.

## Branch / PR convention for approved edits

Once edits are approved, the orchestrator lands them without the user having to ask for a branch:

- **Branch off `main`** (never `main` directly, never an analysis branch):
  `calib/<track>/<YYYY-MM-DD>-<slug>`, where `<track>` is `data` or `process`, the date is the
  calibration-log date, and `<slug>` is a short kebab description. Prefix `<slug>` with a
  `DATA-####` ticket where one exists, matching the repo's branch convention.
- **One PR per calibration batch.** Title `calib(<track>): <slug>`. Body links the calibration-log
  date, lists each finding and the file it touched, and tags the track. Process-track PRs note
  that they change shared process behavior and request review.
- The orchestrator creates the branch, commits, and opens the PR automatically when edits are
  approved. **Merging stays a human action.** This convention is the standing authorization to
  open the PR, not to merge it.

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
