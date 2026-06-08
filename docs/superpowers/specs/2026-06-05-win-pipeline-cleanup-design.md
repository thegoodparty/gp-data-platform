# Design: lean the product-analytics pipeline (Phase 1 topology + entry point)

Status: approved design, pending implementation plan
Date: 2026-06-05
Supersedes parts of: `~/Downloads/pipeline_leanup_plan.md` (the working plan this was brainstormed from)
Builds on: DATA-1959 / PR #441 (runbook split into the two `win-analytics-*` skills)
Conventions from: the org `runbooks` repo (books / commands / experiments / scripts model); see
"Conventions adopted from the org runbooks repo" below

## Background

PR #441 replaced the 784-line `analytics/runbook/win.md` monolith with two Claude Code
skills: `win-analytics-knowledge` (the data facts) and `win-analytics-process` (the analyst
workflow). That landed the content split. It did not fix the pipeline topology, and it did not
add a session entry point. This is the Phase 1 cleanup that closes both, plus one related
re-home of the data-matching reference.

Validation, evals, the repeatable benchmark, and per-directory CI are out of scope here; see
"Deferred and related work" for where each lands.

The working plan this was brainstormed from assumed two things that the repo and current usage
overruled, both captured below: that the data-match runbook should be folded into `joins.md`
(it should become its own skill instead), and the relative weighting of where the entry point
lives (resolved as a runbook for org-convention consistency).

## Mental model

In this harness a skill and a runbook are the same kind of artifact, a passive markdown
document the orchestrator reads on demand, with one difference: a skill carries frontmatter
that makes it auto-surfaced at session start and invocable as a slash command, while a runbook
is a plain file the user points Claude at. A sub-agent is an isolated, fire-and-forget worker:
one prompt in, one result out, with no ability to converse with the user mid-task. Only the
top-level orchestrator talks to the human.

Design rule: a step that needs back-and-forth with the human lives in the orchestrator, driven
by a skill or runbook. A well-bounded non-interactive step (review) is a good sub-agent.

## Goals

1. Fix the framing topology so the framing step can actually converse with the user.
2. Give every session one supported, consistent entry point.
3. Re-home the data-matching reference so its standalone (non-Win) uses can find it.
4. Keep the "one fact, one file" and anti-bloat discipline PR #441 established.

## Component 1: promote the data-matching reference to its own skill

`.claude/data-match-runbook.md` is a general Databricks reference for matching arbitrary ad-hoc
tables (HubSpot exports, lead lists, P2V samples) to ICP flags and other reference data. It is
used as a standalone activity, frequently outside any Win analysis. Folding it into the
Win-scoped `joins.md` (the working plan's item 3) would hide its entry paths from exactly the
non-Win tasks that need them. So it becomes a first-class, discoverable skill instead.

New skill: `.claude/skills/data-matching/`

- `SKILL.md`: a thin router. Description triggers on "matching an external or ad-hoc table to
  ICP flags or other reference data in Databricks." Routes by which ID the source table
  carries: email to Path 1 (via `campaigns`), race ID to Path 2 (via the ballotready race
  staging struct), position ID to Path 3 (direct join to `int__icp_offices`).
- The single file carries the three join paths, the Key Reference Tables block, and the five
  durable gotchas: Delta column aliasing to snake_case, `TRY_CAST` for non-numeric race IDs,
  `position.databaseId` struct access, `LOWER()` on both sides of an email join, and the
  one-to-many duplicate check before saving. It is small enough not to need a `references/`
  subdirectory.

Dropped on extraction:

- The self-append instruction ("after completing a matching task, update this runbook").
  A governed skill is human-owned reference, not a self-growing log.
- The "Output Tables Created" log (the `private_tristan.*` rows). This is point-in-time
  provenance, not reference. Drop it.
- The per-path match-rate "Used in" notes stay, as one-line evidence on each path.

Retire `.claude/data-match-runbook.md` after extraction.

### Dedup boundary with the Win skills

- `joins.md` keeps its Win-entity ICP line
  (`candidacy.br_position_database_id = int__icp_offices.br_database_position_id`, the path the
  Win working set already has position IDs for) and adds a one-line pointer to the
  `data-matching` skill for external-table entry paths.
- `segmentation.md` keeps owning what `icp_office_win` means as a slice dimension.
- The `data-matching` skill owns the matching recipes and the `int__icp_offices` reference-table
  description.

Each fact has one home.

### Long-term home

The data-matching reference is general, not Win-specific, and matches the kind of standalone
Databricks reference the org runbooks repo already hosts as a book (cf. `books/query-voter-data.md`).
Building it as a skill here now is the pragmatic step. Migrating it to the runbooks repo `books/`
is a later cross-repo step once this pipeline moves toward that process. Out of scope for this spec;
see "Deferred and related work."

## Component 2: the entry-point runbook

There is no entry point today, which is why a cold session diverges from an already-primed one
and takes the shortest path, skipping framing. The fix is a single supported front door. For
consistency with the org's runbook convention, the entry point is a runbook (not a skill); the
reference content stays as the two `win-analytics-*` skills. This is a deliberate mixed model.

New runbook: `analytics/runbook/run-product-analysis.md`. (Action-led name per the org procedure
convention. The session is started by pointing Claude at this runbook; it is not a slash command.)

- Product-agnostic front door. Step 1 determines the product. Only the Win branch is populated
  today; Serve and others are added later. No routing machinery beyond a simple branch (YAGNI).
- Lightweight: alignment, pointers, and a staged to-do only. No data facts and no methodology,
  which live in the skills.
- It primes the session: names the data sources and the credentials a cold session lacks, and
  instructs the orchestrator to load the `win-analytics-process` and `win-analytics-knowledge`
  skills.
- It seeds the corrected stages as a to-do (agents follow a to-do far more faithfully than
  prose): refine the question in the orchestrator, get human approval of the brief, execute,
  spawn the reviewers, capture calibration.
- It follows the org book proc shape: one-line summary, prerequisites, steps, then a short
  troubleshooting block; kebab-case; written for an agent first, a human second. See the
  conventions section below.

`analytics/runbook/README.md` is rebuilt as a routing index in the org's `INDEX.md` style: a small
table (type, trigger keywords, path, one-line description) that routes to this entry runbook and
names the two skills. Today it only points at the two skills. The routing table is how a session
finds the entry point by keyword rather than by remembering a filename.

Invocation is explicit and human-driven, consistent with the chosen "user explicitly invokes
it" model. The team convention becomes "start a product analysis from this runbook." It is not
a slash command and not auto-surfaced.

### Known property (documented, not a blocker)

Because the runbook is passive and explicitly invoked, the acceptance criterion "a cold session
reaches the framing step" holds only when the user actually starts from the runbook. A cold
session that skips it can still shortcut to an answer. The routing index in
`analytics/runbook/README.md` (keyword triggers) partially mitigates this, matching how the org
repo handles discoverability, but it is not the autonomous guarantee a skill front door would have
given. This trade was chosen deliberately for org-convention consistency.

## Component 3: topology fix, framer agent to framing routine

`analytics-question-framer` is currently a sub-agent, but its body defines a conversational
interaction shape (ask clarifying questions, iterate, wait for explicit human approval before
producing the brief). A sub-agent cannot converse with the user, so this only works today via a
wasteful relay pattern (the framer emits questions, the orchestrator relays them, each reply
re-spawns the framer with fresh context). This is the highest-leverage fix.

Changes:

- Move the framer's persona, its "what a well-framed question looks like" set, the pre-brief
  verification checklist, and the interaction shape into a new
  `.claude/skills/win-analytics-process/references/framing.md`, phrased imperatively as a
  routine the orchestrator runs in its own context. The voice and the sharp questions carry over
  as content.
- Retire `.claude/agents/analytics-question-framer.md` (delete the spawnable agent; its content
  now lives in the skill).
- Framing and execution become two distinct, ordered steps owned by the orchestrator, with the
  human-approval gate as the hard separator: no analysis code until the brief is approved. The
  orchestrator must not frame and write code at the same time. This is already the shape of the
  `win-analytics-process` senior-analyst loop; this change gives step 1 its real content and
  makes the gate explicit.
- The reviewers (`product-data-scientist`, `product-manager`) and `code-critic` stay sub-agents.
  Review needs no human interaction, so isolation is correct for them.

What the sub-agent gave for free was triggering framing as a discrete step in an isolated
context. We re-create that with the staged to-do (Component 2) and the human-approval gate, not
with isolation, since in the orchestrator's shared context the persona is a softer prior
competing with the main task. The staged to-do is what keeps framing from being skipped.

## Component 4: pipeline.md rewrite and dedup cleanup

- Rewrite `pipeline.md` so the documented flow matches the new topology. Stage 1 (Frame) and
  stage 2 (Execute) become orchestrator-owned, driven by the process skill, not separate
  agents. The reviewer rows stay as sub-agents. The `run-product-analysis.md` runbook is documented
  as the start of the flow.
- Repoint the references to the retired framer agent found across the repo. References that name
  "the framer" as a role or the "framer-to-executor contract" can stay (it is still a step).
  References that treat it as a spawnable agent file must change:
  - `win-analytics-process/references/brief-schema.md`: the `author` field default and the
    "kick back to `analytics-question-framer`" and "re-frame" language point to the framing step
    / re-running framing, not the agent.
  - `win-analytics-process/references/calibration.md`: the triage row that points to
    `.claude/agents/analytics-question-framer.md` points to `references/framing.md` instead.
  - `win-analytics-process/references/pipeline.md`: stage 1 agent column and the calibration
    triage line.
  - `win-analytics-process/SKILL.md`: the "the framer produces a structured brief" line.
  - `.claude/agents/product-data-scientist.md`: "from an `analytics-question-framer` brief"
    becomes "from a framing brief."
  - `win-analytics-process/references/methodology.md`: the "framer-to-executor brief contract"
    reference (name only, low priority).
- Trim the "where you sit in the workflow" paragraph in each of the three agents
  (`analytics-question-framer` being deleted, so the two reviewers) to a single pointer to
  `pipeline.md`. This satisfies the criterion that the flow is described in exactly one place.
- Clean the stale line in `.claude/settings.local.json` (a pre-commit permission that references
  both the deleted `analytics/runbook/win.md` and the framer agent file).

## Conventions adopted from the org runbooks repo (item 3)

The org `runbooks` repo defines an AI-first convention set. We adopt the parts that fit this repo
now, so the product-analytics artifacts are portable to that repo later:

- **Book shape for the entry runbook.** One-line summary first, then prerequisites (data sources,
  credentials, the skills to load), then the staged steps, then a short troubleshooting block.
  Written for an AI agent first, a human second.
- **A routing index for discoverability.** `analytics/runbook/README.md` becomes a routing table in
  the org's `INDEX.md` style: a row per artifact with a type tag (`proc` / `ref`), trigger
  keywords, path, and one-line description, routing to the entry runbook and naming the two skills.
  Keyword routing is the org's answer to discoverability and partially mitigates the Component 2
  known property.
- **Naming and portability.** Kebab-case, action-led names; standalone docs with no hardcoded
  machine paths. Same-repo skill links are fine now; full standalone portability is only required
  if an artifact migrates to the runbooks repo later.
- **Delete, do not deprecate.** Retiring the framer agent and the old `win.md` follows the org rule
  to delete stale docs rather than keep dated snapshots; git history is the archive.

Not adopted now. Each maps to a different destination in the deferred work below, not all to one
track:

- The runbook-to-experiment lifecycle and the manifest schema belong to the long-term port to the
  experiment process (neither Track A nor Track B).
- The `qa_validate` verdict harness maps to the repeatable benchmark (former Track B).
- The path-filtered publish CI maps to per-directory CI, built with the Track A linter (DATA-1960).

## Acceptance criteria

- Framing and execution both run in the orchestrator as two distinct ordered steps with the
  approval gate between them. No step that needs to talk to the user is a sub-agent.
- There is one supported entry point, the `analytics/runbook/run-product-analysis.md` runbook, that
  primes the session and seeds the staged to-do, and `pipeline.md` matches the topology it
  drives.
- A concept resolves to one governed definition via `canonical_metrics.md`, not several
  candidates (unchanged from PR #441; verify still true after edits).
- `.claude/data-match-runbook.md` is extracted into the `data-matching` skill and retired. No
  data fact appears verbatim in more than one file across the skill docs and agent files, except
  as an explicit pointer.
- The pipeline flow is described in exactly one place (`pipeline.md`); no agent file re-describes
  it.
- A session started from the entry-point runbook reaches the framing step rather than jumping
  straight to an answer. (Conditional on starting from the runbook, per the known property
  above.)
- At least one existing analysis in `analytics/ad_hoc/` still runs end to end after the changes.

## Out of scope

- Validation, evals, the repeatable benchmark, and per-directory CI. See "Deferred and related
  work" for where each lands.
- Auto-generating metric definitions with an LLM. Humans own definitions; Claude drafts docs.
- Any rework of the `win-analytics-process` or `win-analytics-knowledge` reference content beyond
  the framing-routine addition and the repointing above.
- A Serve branch in the entry runbook. Stub the product branch only; do not build it.

## Deferred and related work

These came up during design and are deliberately not in this spec:

- **Repeatable benchmark (former Track B).** A fixed-question benchmark that measures a few
  auto-measurable metrics (cost, runtime, turns, schema or verdict pass) plus human feedback, run
  repeatedly to tell whether pipeline changes move in the right direction. Modeled on the org repo's
  `qa-validate` verdict harness (`ok` / `warn` / `block`) and `analyze-experiment-costs` metrics.
  Gets its own spec, built after Phase 1. Noted tradeoff: building it after Phase 1 forgoes a clean
  pre-change baseline for the Phase 1 changes themselves.
- **Per-directory CI (item 2).** Path-filtered CI scoped to the product-analytics directory,
  following the org repo's `paths:`-filtered workflow plus `CODEOWNERS` model. Designed and built
  with the Track A linter on the `data-1960` branch (DATA-1960), not here, so CI lands with the
  checks it runs.
- **Migrating the `data-matching` skill to the runbooks repo `books/`.** It is a general,
  standalone Databricks reference and a natural fit for that repo. A later cross-repo step.
- **Porting the pipeline to the runbook-to-experiment process.** The long-term arc: prove the
  workflow as runbooks here, then port to schema-contracted, autonomously runnable experiments in
  the org repo. Out of scope for Phase 1.

## Verification before and during implementation

- Confirm the harness instantiates the framer and reviewer agents as isolated sub-agents (this
  is the premise of Component 3). Already confirmed from the agent registry; reconfirm if the
  harness changes.
- Confirm the five `data-match-runbook.md` gotchas are still accurate before extraction. They
  appear durable and version-agnostic (last updated 2026-02-13); spot-check the table and column
  names against the live catalog.
- Identify where local Claude Code memory does priming work that a cold machine will not have
  (Databricks credential wiring, ground-truth table locations), so the entry runbook names what
  to load rather than assuming it.
- Re-run the cross-doc link check (PR #441 verified 120 links) after the edits.
