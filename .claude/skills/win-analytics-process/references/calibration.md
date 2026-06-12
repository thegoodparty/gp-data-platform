# Calibration logs

Part of the **win-analytics-process** skill. The closing step that makes the pipeline self-correct.

After a substantive analysis run, write findings that pass the promotion test (below) into a
dated calibration log at `analytics/runbook/CALIBRATION_<YYYY-MM-DD>.md`, then handle each finding
per its track (below). This is a **required closing step** of every substantive analysis (see the
verification protocol in [methodology.md](methodology.md)): either produce the log and process it,
or explicitly record that no calibration was needed. **"No calibration needed" is the expected,
healthy outcome of most runs** — a finding is the exception, not a deliverable, and a run that
produces none has not underperformed. What's required is the closing step itself; it's what makes
the process self-correct rather than relying on someone remembering.

Two files, two sharing rules. Calibration logs are personal working documents: they are gitignored
(`analytics/runbook/CALIBRATION_*.md`) so each analyst's working tree can carry them without
affecting the shared repo; the durable team-shared output is the doc + agent edits the log drives.
The candidates ledger `analytics/runbook/CANDIDATES.md` (below) is **shared and committed** —
everything below the promotion bar goes there.

**No core or shared file is ever edited without explicit human approval.** Approved edits land via
the branch/PR convention below, never on `main` directly and never mixed into an analysis branch.

## Two tracks

Calibration findings split into two tracks with different defaults:

### Track 1: data calibration (default, ON)

Reusable **data facts**: joins, gotchas, metric definitions, coverage, additions to the canonical
metrics list. These land in the owning doc of the **win-analytics-knowledge** skill
(`.claude/skills/win-analytics-knowledge/references/`). Runs on every substantive analysis.
Propose the exact edit; apply **only after explicit human approval**.

**Promotion test.** A candidate finding may enter the calibration log only if all three hold:

- (a) it would have changed this run's outcome, or saved meaningful time, had it been known in
  advance;
- (b) it is not already stated in or derivable from the existing knowledge docs;
- (c) it is likely to recur in future analyses — not specific to this run's question or cut.

The log entry for each finding must answer (a), (b), and (c) explicitly, one line each. A finding
that cannot answer all three is not logged as a finding (it may still be appended to the
candidates ledger, below).

### Track 2: process calibration (default, OFF)

Findings that change the **process itself**: the framing routine ([framing.md](framing.md)), the
executor instructions / `analytics/lib`, the reviewer agents (e.g.
`.claude/agents/product-data-scientist.md`), or the process skill. On a standard run, do **not**
propose or apply these. Park them as `process`-track lines in the candidates ledger (below), so
OFF does not silently become never, and in the closing summary state plainly that process
calibration was **OFF (not run)** and offer to rerun in process-design mode. Act on Track 2 only if the user has **explicitly opted into
process-design mode** this session.

| Finding | Track |
|---|---|
| Data facts, joins, gotchas, metric definitions, coverage, canonical-list additions | Track 1 (data) |
| How framing scopes / verifies; how the executor builds; how a reviewer agent reviews; process-skill wording | Track 2 (process) |

Things that **never qualify** as findings, on either track:

- parameter or filter variations of an already-documented query or metric;
- one-off cohort or date-range choices made for this run's question;
- restatements of how this run was performed ("this analysis used X") with no forward-looking rule;
- anything already covered by an existing rule, unless the proposal sharpens or generalizes it.

Tag each finding **universal** (codify freely) vs **data-state** (hedge, or track in the ledger
until confirmed), and prefer sharpening an existing rule over adding a new one, per the cautions
below.

## Candidates ledger

`analytics/runbook/CANDIDATES.md` is a committed, append-only ledger that decouples observation
from proposal. Instead of proposing a doc edit, a run appends one line (format documented in the
file) for:

- an observation that fails the promotion test but seems worth remembering;
- a data-state finding awaiting confirmation — the ledger is the tracking mechanism for the
  wait-for-confirmation rule below;
- a parked Track 2 process-design candidate.

**Promotion:** when essentially the same observation has appeared in **2 separate runs** (**3**
for data-state-tagged items), it becomes eligible for the next calibration batch as a real
proposal — still subject to the promotion test and the approval gate. Mark promoted lines with
the PR reference rather than deleting them.

## Branch / PR convention for approved edits

Once edits are approved, the orchestrator lands them without the user having to ask for a branch:

- **Branch off `main`** (never `main` directly, never an analysis branch):
  `calib/<track>/<YYYY-MM-DD>-<slug>`, where `<track>` is `data` or `process`, the date is the
  calibration-log date, and `<slug>` is a short kebab description. Prefix `<slug>` with a
  `DATA-####` ticket where one exists, matching the repo's branch convention.
- **One PR per calibration batch.** Title `calib(<track>): <slug>`. Body links the calibration-log
  date, lists each finding and the file it touched, and tags the track. Process-track PRs note
  that they change shared process behavior and request review.
- **Ledger appends follow this same convention — no exemptions.** If the run produces a
  calibration batch PR, ledger appends ride in that PR. A run that produces only candidate
  observations and no promotable findings gets a small ledger-only PR on a `calib/data` branch
  with slug suffix `-candidates`. These PRs are append-only and should be quick to approve, but
  approval is still required and merging stays a human action.
- The orchestrator creates the branch, commits, and opens the PR automatically when edits are
  approved. **Merging stays a human action.** This convention is the standing authorization to
  open the PR, not to merge it.

## Beware over-calibration

A single-analysis log surfaces failure modes specific to that run's data state alongside
general principles. When promoting findings into doc or agent edits, distinguish between the two:
universal hygiene (data-existence checks, CIs, documentation) can be codified freely;
data-state-dependent defaults (cohort filters, metric choices, funnel decompositions) should
carry hedges noting current state, or sit in the candidates ledger until they reach the
data-state promotion threshold before being treated as settled rules. Tightening a rule "so this exact failure can't
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
- `analytics/runbook/CANDIDATES.md` — the shared candidates ledger.
