# Product-analytics pipeline cleanup (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the product-analytics pipeline topology (framing can converse), add one supported entry-point runbook, and re-home the data-matching reference as its own skill, following the org runbooks-repo conventions.

**Architecture:** This is a documentation and agent-config refactor. No production SQL or application logic changes. We extract a general reference into a new skill, add a runbook entry point plus a routing index, convert the framer sub-agent into an orchestrator-run framing routine inside the `win-analytics-process` skill, and repoint every reference so each fact and the pipeline flow each live in exactly one place.

**Tech Stack:** Markdown skills/agents/runbooks under `.claude/` and `analytics/`; pre-commit (black/isort/flake8/mypy/sqlfmt/pytest) gates every commit; git.

**Spec:** `docs/superpowers/specs/2026-06-05-win-pipeline-cleanup-design.md`

**Branch:** `data-1959/pipeline-topology-cleanup` (already created off `main`; the spec is already committed here).

---

## Conventions for this plan

**Commit convention.** This repo's pre-commit pytest hook needs the `dbt/` poetry venv on PATH, and an active analytics venv shadows it. Every commit step uses this form (run from anywhere; paths are absolute):

```bash
git -C /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform add <paths>
cd /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform/dbt \
  && env -u VIRTUAL_ENV poetry run git -C /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform commit -m "<msg>"
```

Append to every commit message body:

```
Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
```

**Link-check helper.** There is no automated link checker in the repo. Use this from the repo root to verify a markdown file's relative `.md` links resolve. Expected output: nothing (no `BROKEN:` lines).

```bash
check_links() {
  f="$1"; d=$(dirname "$f")
  grep -oE '\]\([^)]+\.md[^)]*\)' "$f" \
    | sed -E 's/^\]\(([^)# ]+).*/\1/' \
    | while read -r t; do
        case "$t" in /*|http*) continue;; esac
        [ -f "$d/$t" ] || echo "BROKEN: $f -> $t"
      done
}
```

**Repo root.** All relative paths below are under `/Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform`.

---

## File structure (what changes and why)

**Created:**
- `.claude/skills/data-matching/SKILL.md` — the general ICP/data-matching reference, extracted from the retired runbook, as a discoverable router skill.
- `.claude/skills/win-analytics-process/references/framing.md` — the framing routine the orchestrator runs (the framer agent's content, made imperative).
- `analytics/runbook/run-product-analysis.md` — the single entry-point runbook that primes a session and seeds the staged to-do.

**Modified:**
- `.claude/skills/win-analytics-knowledge/references/joins.md` — add a pointer to the `data-matching` skill (dedup boundary).
- `analytics/runbook/README.md` — rebuild as a routing index (org `INDEX.md` style).
- `.claude/skills/win-analytics-process/SKILL.md` — step 1/3 and the reference list point at `framing.md`.
- `.claude/skills/win-analytics-process/references/pipeline.md` — stages 1–2 become orchestrator-owned; runbook documented as the start; triage line repointed.
- `.claude/skills/win-analytics-process/references/brief-schema.md` — agent self-references become the framing step.
- `.claude/skills/win-analytics-process/references/calibration.md` — triage row repointed to `framing.md`.
- `.claude/skills/win-analytics-process/references/methodology.md` — "framer→executor" wording (name only).
- `.claude/agents/product-data-scientist.md` — "from an analytics-question-framer brief" wording.
- `.claude/settings.local.json` — remove the stale pre-commit permission line.

**Deleted:**
- `.claude/data-match-runbook.md` — content moved into the new skill.
- `.claude/agents/analytics-question-framer.md` — content moved into `framing.md`; the spawnable agent is retired.

---

## Task 1: Extract the data-matching skill (Component 1)

**Files:**
- Create: `.claude/skills/data-matching/SKILL.md`
- Modify: `.claude/skills/win-analytics-knowledge/references/joins.md`
- Delete: `.claude/data-match-runbook.md`

- [ ] **Step 1: Create the skill file**

Create `.claude/skills/data-matching/SKILL.md`. Start with this frontmatter and intro:

```markdown
---
name: data-matching
description: Match an external or ad-hoc table (HubSpot export, lead list, P2V sample) to ICP flags or other reference data in Databricks. Use when you have a table of people or races and need to attach int__icp_offices flags (icp_office_win / icp_office_serve, voter_count, etc.) but the table only has emails, BallotReady race IDs, or position IDs, not the ICP join. Routes to the right join path by which ID you have. General Databricks matching reference; not Win-pipeline specific.
---

# Data matching

Match an arbitrary table to ICP flags (and other reference data) in Databricks using established join paths. Route by which ID your source table carries:

- It has **emails** but no BallotReady IDs → **Path 1** (via `campaigns`).
- It has a **BallotReady race ID** but no position ID → **Path 2** (via the race staging struct).
- It already has a **position ID** → **Path 3** (direct join to `int__icp_offices`).

Verify any named table/column against the live `goodparty_data_catalog` before relying on it; these recipes drift.
```

Then copy the body of `.claude/data-match-runbook.md` from its `## Key Reference Tables` heading through the end of the `## Gotchas` section **verbatim** (the Key Reference Tables block, all three Join Paths including their `**Used in**:` evidence lines, and the five Gotchas). Apply exactly these two removals while copying:
- Do **not** copy the `> **For Claude**: ...update this runbook...` blockquote (the self-append instruction).
- Do **not** copy the `## Output Tables Created` section or its table (the `private_tristan.*` rows).

- [ ] **Step 2: Delete the retired runbook**

Run:

```bash
git -C /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform rm .claude/data-match-runbook.md
```

- [ ] **Step 3: Add the dedup pointer in joins.md**

In `.claude/skills/win-analytics-knowledge/references/joins.md`, under `## Routing triggers`, add this as the final bullet of that list (immediately after the `IF the question is about what a metric *means*...` bullet):

```markdown
- IF you have an external or ad-hoc table (emails, BallotReady race IDs) that needs ICP flags attached → use the **data-matching** skill, which owns the email→campaigns and race-id→position entry paths. This doc covers only the Win-entity ICP path (`candidacy.br_position_database_id`, below).
```

Then in the same file under `## Cross-references`, add this bullet to the list:

```markdown
- **data-matching** skill (`.claude/skills/data-matching/SKILL.md`) — matching external tables to ICP flags by email / race ID / position ID.
```

- [ ] **Step 4: Verify the extraction and links**

Run (from repo root):

```bash
test -f .claude/skills/data-matching/SKILL.md && echo "SKILL created"
test ! -e .claude/data-match-runbook.md && echo "runbook removed"
grep -rn "data-match-runbook" .claude/ analytics/ docs/ ; echo "exit:$?"
grep -c "Output Tables Created\|update this runbook" .claude/skills/data-matching/SKILL.md
```

Expected: `SKILL created`, then `runbook removed`, then no lines from the grep with `exit:1` (no surviving references), then `0` (neither dropped block was carried over).

Then run the link helper on the two touched docs:

```bash
check_links .claude/skills/win-analytics-knowledge/references/joins.md
check_links .claude/skills/data-matching/SKILL.md
```

Expected: no `BROKEN:` lines.

- [ ] **Step 5: Commit**

```bash
git -C /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform add .claude/skills/data-matching/SKILL.md .claude/skills/win-analytics-knowledge/references/joins.md .claude/data-match-runbook.md
cd /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform/dbt \
  && env -u VIRTUAL_ENV poetry run git -C /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform commit -m "feat(analytics): extract data-matching skill; retire data-match-runbook

Move the general ICP/data-matching reference into a discoverable
data-matching skill; drop the self-append instruction and the
point-in-time Output Tables log. joins.md points to the skill for
external-table entry paths and keeps only the Win-entity ICP path.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

Expected: pre-commit hooks pass; one commit created.

---

## Task 2: Add the framing routine; retire the framer agent (Component 3)

**Files:**
- Create: `.claude/skills/win-analytics-process/references/framing.md`
- Modify: `.claude/skills/win-analytics-process/SKILL.md`
- Delete: `.claude/agents/analytics-question-framer.md`

- [ ] **Step 1: Create framing.md from the framer agent content**

Create `.claude/skills/win-analytics-process/references/framing.md`. Begin with this exact intro:

```markdown
# Framing routine

Part of the **win-analytics-process** skill. Step 1 of the senior-analyst loop. The orchestrator runs this routine **in its own context** (not as a sub-agent) so it can ask the user clarifying questions and iterate to an approved brief.

**The approval gate is hard.** Do not write any analysis code while framing. Framing ends only when the user explicitly approves the framing; the approved brief is the spec the execution step then follows. Framing and execution are two ordered steps, never one blended activity.
```

Then port the body from `.claude/agents/analytics-question-framer.md`, carrying these sections in this order and applying the transformation rules below:
1. The persona paragraph (the agent's opening "You are a senior product analyst..." paragraph).
2. `## How you operate`
3. `## What a well-framed question looks like`
4. `## What you look for`
5. `## Pre-brief verification checklist`
6. `## Interaction shape`
7. `## Output format`

Transformation rules while porting:
- Rephrase second-person descriptions of the agent into imperative instructions to the orchestrator ("You frame and scope" → "Frame and scope"; "You ask clarifying questions" → "Ask clarifying questions").
- **Do not** carry the `## Where you sit in the workflow` section — the pipeline flow lives only in `pipeline.md`.
- **Do not** carry the `## Context supplied at invocation` section — skill loading is handled by the entry runbook and the process SKILL.md.
- Replace any phrase that frames the output as handing off to a separate executor (e.g. "Your output will be read by Claude Code as input") with language that the same orchestrator continues to the execution step: "The brief is the spec your execution step will follow."
- Keep the cross-references to the knowledge skill (`engagement.md`, `canonical_metrics.md`, etc.) and to `brief-schema.md` exactly as they resolve from this file's location (`brief-schema.md` is a sibling in the same `references/` directory, so links are bare `brief-schema.md`; knowledge-skill links are `../../win-analytics-knowledge/references/<doc>.md`).

End the file with this cross-references block:

```markdown
## Cross-references

- [brief-schema.md](brief-schema.md) — the brief format this routine produces.
- [pipeline.md](pipeline.md) — where framing sits in the flow.
- [methodology.md](methodology.md) — scoping checklist, default cohorts, verification protocol.
```

- [ ] **Step 2: Wire framing.md into the process SKILL.md**

In `.claude/skills/win-analytics-process/SKILL.md`, replace step 1 of the senior-analyst loop:

Find:
```markdown
1. **Clarify and scope.** Sharpen the question to a one-sentence hypothesis with a decision attached. Resolve scoping forks before writing code (don't defer them to the reader). See [`references/methodology.md`](references/methodology.md) for the scoping checklist, the resolved defaults, and how to pick the right cohort for the analysis type.
```
Replace with:
```markdown
1. **Clarify and scope (framing).** Sharpen the question to a one-sentence hypothesis with a decision attached, and resolve scoping forks before writing any code. Run the framing routine in [`references/framing.md`](references/framing.md) in your own context (ask the user, iterate); see [`references/methodology.md`](references/methodology.md) for the scoping checklist, resolved defaults, and cohort selection. Framing ends at the human-approval gate; no execution code until the brief is approved.
```

Then find step 3:
```markdown
3. **Frame the brief.** The framer produces a structured brief; the format is the framer→executor contract in [`references/brief-schema.md`](references/brief-schema.md).
```
Replace with:
```markdown
3. **Produce the brief.** The framing routine produces a structured brief; the format is the framing→execution contract in [`references/brief-schema.md`](references/brief-schema.md).
```

Then in the `## Reference docs` list at the bottom, add this entry as the first bullet:
```markdown
- [`references/framing.md`](references/framing.md) — the framing routine (persona, question set, pre-brief verification) the orchestrator runs in step 1.
```

- [ ] **Step 3: Delete the framer agent**

Run:

```bash
git -C /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform rm .claude/agents/analytics-question-framer.md
```

- [ ] **Step 4: Verify**

Run (from repo root):

```bash
test -f .claude/skills/win-analytics-process/references/framing.md && echo "framing.md created"
test ! -e .claude/agents/analytics-question-framer.md && echo "agent removed"
grep -n "Where you sit\|Context supplied at invocation" .claude/skills/win-analytics-process/references/framing.md ; echo "exit:$?"
check_links .claude/skills/win-analytics-process/references/framing.md
check_links .claude/skills/win-analytics-process/SKILL.md
```

Expected: `framing.md created`, `agent removed`, the grep prints nothing with `exit:1` (neither dropped section was carried over), and no `BROKEN:` lines.

- [ ] **Step 5: Commit**

```bash
git -C /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform add .claude/skills/win-analytics-process/references/framing.md .claude/skills/win-analytics-process/SKILL.md .claude/agents/analytics-question-framer.md
cd /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform/dbt \
  && env -u VIRTUAL_ENV poetry run git -C /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform commit -m "refactor(analytics): convert framer sub-agent to an orchestrator framing routine

The framer could not converse as a sub-agent. Move its persona, question
set, and pre-brief verification into win-analytics-process/references/framing.md
as an imperative routine the orchestrator runs in step 1, gated by human
approval before execution. Retire the spawnable agent.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

Expected: pre-commit hooks pass; one commit created.

---

## Task 3: Add the entry-point runbook and routing index (Component 2)

**Files:**
- Create: `analytics/runbook/run-product-analysis.md`
- Modify: `analytics/runbook/README.md`

- [ ] **Step 1: Create the entry-point runbook**

Create `analytics/runbook/run-product-analysis.md` with exactly this content:

```markdown
Start a product-analytics session on the right path: refine the question to an approved brief before writing any analysis code, then execute, then review.

## Prerequisites

- **Databricks access.** `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_API_KEY` must be set (the key resolves from 1Password; unlock it if a call errors with "Missing required Databricks connection parameters"). The live `goodparty_data_catalog` is ground truth.
- **Skills to load.** Read the `win-analytics-process` skill (how to work) and resolve every data concept through the `win-analytics-knowledge` skill (what is true). Do not restate the pipeline flow; it lives in `win-analytics-process/references/pipeline.md`.
- **Working-set helper.** `analytics/lib/win_analysis.py` builds the per-user working set once; slice it in pandas.

## Steps

1. **Determine the product.** Today only the **Win** product is supported; route to the Win skills above. (Serve and others will be added here later.)
2. **Frame (refine the question).** Run the framing routine in `win-analytics-process/references/framing.md` in this conversation: ask the user clarifying questions, resolve scoping forks, and produce a brief per `win-analytics-process/references/brief-schema.md`. Do not write analysis code yet.
3. **Get human approval of the brief.** This is the hard gate between framing and execution. Do not proceed until the user approves.
4. **Execute.** Build the working set once with `analytics/lib/win_analysis.py`, then slice every cut in pandas. Save the brief alongside the executed notebook (ad-hoc: `analytics/ad_hoc/<YYYY-MM-DD>_<brief_id>_brief.yaml`).
5. **Review.** Spawn the `product-data-scientist` (methodology + interpretation) and `product-manager` (usefulness) reviewers as sub-agents on the executed analysis.
6. **Capture calibration.** Run the closing calibration pass (`win-analytics-process/references/calibration.md`): route findings into the file that owns them, or record that none were needed.

Seed steps 2–6 as a TodoWrite list at the start so the staged flow is not skipped.

## Troubleshooting

- "Missing required Databricks connection parameters" → 1Password is locked; unlock it and open a fresh shell.
- The orchestrator jumps straight to an answer → it skipped framing; restart from step 2 and seed the to-do.
- A concept resolves to several candidate metrics → resolve it through `win-analytics-knowledge`'s `canonical_metrics.md`, which is the governed registry.
```

- [ ] **Step 2: Rebuild README.md as a routing index**

Replace the entire contents of `analytics/runbook/README.md` with:

```markdown
# analytics/runbook

Routing index for product-analytics work. Match your task to a row, then open that artifact. Start at the entry-point runbook.

| Type | Trigger keywords | Path | Description |
|---|---|---|---|
| proc | start analysis, product analysis, new analysis, frame question, kick off, where do I start | `analytics/runbook/run-product-analysis.md` | Entry point. Primes the session and seeds the staged to-do: frame → approve → execute → review → calibrate. |
| ref | how to work, scoping, methodology, brief, pipeline, calibration, reviewers | `.claude/skills/win-analytics-process/` (`SKILL.md`) | The analyst workflow: framing routine, methodology, brief contract, pipeline topology, calibration. |
| ref | data facts, which table, which metric, joins, engagement, outcomes, viability, segmentation, gotchas | `.claude/skills/win-analytics-knowledge/` (`SKILL.md`) | The data facts behind Win analyses, behind a router over the canonical metrics registry. |

A Serve entry will be added here when it exists.

## What still lives here

- **`CALIBRATION_<YYYY-MM-DD>.md`** — per-session calibration logs. Gitignored working documents; durable lessons are promoted into the two skills above (and the agent files). See `.claude/skills/win-analytics-process/references/calibration.md` for the convention.
```

- [ ] **Step 3: Verify**

Run (from repo root):

```bash
test -f analytics/runbook/run-product-analysis.md && echo "runbook created"
grep -c "run-product-analysis.md" analytics/runbook/README.md
grep -n "## Steps\|## Prerequisites\|## Troubleshooting" analytics/runbook/run-product-analysis.md
```

Expected: `runbook created`; `1` (the index links the runbook once); and all three section headings present (the book proc shape).

- [ ] **Step 4: Commit**

```bash
git -C /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform add analytics/runbook/run-product-analysis.md analytics/runbook/README.md
cd /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform/dbt \
  && env -u VIRTUAL_ENV poetry run git -C /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform commit -m "feat(analytics): add run-product-analysis entry runbook + routing index

Single supported front door that primes a session and seeds the staged
to-do (frame, approve, execute, review, calibrate). README.md becomes a
keyword routing index in the org runbooks-repo INDEX.md style.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

Expected: pre-commit hooks pass; one commit created.

---

## Task 4: Rewrite pipeline.md and repoint stale references (Component 4)

**Files:**
- Modify: `.claude/skills/win-analytics-process/references/pipeline.md`
- Modify: `.claude/skills/win-analytics-process/references/brief-schema.md`
- Modify: `.claude/skills/win-analytics-process/references/calibration.md`
- Modify: `.claude/skills/win-analytics-process/references/methodology.md`
- Modify: `.claude/agents/product-data-scientist.md`
- Modify: `.claude/settings.local.json`

- [ ] **Step 1: Rewrite the pipeline.md stage table and start**

In `.claude/skills/win-analytics-process/references/pipeline.md`, replace the stage-1 and stage-2 rows of the `## Stages and handoffs` table.

Find the stage-1 row (the line beginning `| 1 | **Frame** | `):
```markdown
| 1 | **Frame** | `analytics-question-framer` | a vague question + the human's intent | a structured analysis **brief** (YAML per [brief-schema.md](brief-schema.md)) | Read-only and advisory. Shapes and scopes; verifies data exists against the live catalog. Does **not** write analysis code. Does not produce the brief until the human approves the framing. |
```
Replace with:
```markdown
| 1 | **Frame** | Orchestrator (framing routine, [framing.md](framing.md)) | a vague question + the human's intent | a structured analysis **brief** (YAML per [brief-schema.md](brief-schema.md)) | Runs in the orchestrator's own context so it can converse with the human. Shapes and scopes; verifies data exists against the live catalog. Does **not** write analysis code, and does not produce the brief until the human approves the framing. |
```

Find the stage-2 row (the line beginning `| 2 | **Execute** | `):
```markdown
| 2 | **Execute** | Executor (general-purpose Claude Code) | the approved brief | an executed notebook + the brief saved alongside it | Treats the brief as a spec. Builds the working set once, slices in pandas. If the brief is unworkable on inspection of the data, kicks it back to stage 1 rather than improvising. |
```
Replace with:
```markdown
| 2 | **Execute** | Orchestrator (same session, after the approval gate) | the approved brief | an executed notebook + the brief saved alongside it | A distinct ordered step after framing, separated by the human-approval gate. Treats the brief as a spec. Builds the working set once, slices in pandas. If the brief is unworkable on inspection of the data, returns to framing rather than improvising. |
```

Then update the "Descriptive, not active" paragraph to name the entry point. Find:
```markdown
**Descriptive, not active.** This pipeline runs as a conversation in which the human shapes the
framing and approves the brief. There is no automated driver that runs the stages end to end —
doing so would skip those human inputs. This doc documents the flow; a human (or the process
skill stepping through it) drives it.
```
Replace with:
```markdown
**Descriptive, not active.** This pipeline runs as a conversation in which the human shapes the
framing and approves the brief. The supported entry point is the
`analytics/runbook/run-product-analysis.md` runbook, which seeds the staged to-do. There is no
automated driver that runs the stages end to end; doing so would skip those human inputs. This
doc documents the flow; a human (or the process skill stepping through it) drives it.
```

Then in the `## Closing the loop` paragraph, repoint the triage target. Find:
```markdown
are triaged into the file that owns them — the framer agent, the executor instructions /
`analytics/lib`, the data-scientist agent, or a knowledge-skill domain doc — so the pipeline
```
Replace with:
```markdown
are triaged into the file that owns them — the framing routine ([framing.md](framing.md)), the
executor instructions / `analytics/lib`, the data-scientist agent, or a knowledge-skill domain
doc — so the pipeline
```

- [ ] **Step 2: Repoint brief-schema.md**

In `.claude/skills/win-analytics-process/references/brief-schema.md`:

Find (line 3):
```markdown
Part of the **win-analytics-process** skill. The framer→executor handoff contract.
```
Replace with:
```markdown
Part of the **win-analytics-process** skill. The framing→execution handoff contract.
```

Find (line 5):
```markdown
When the `analytics-question-framer` agent finishes shaping a question, it produces a
```
Replace with:
```markdown
When the framing routine (see [framing.md](framing.md)) finishes shaping a question, it produces a
```

Find (line 16, the YAML `author` field):
```markdown
author: analytics-question-framer (refined with <username>)
```
Replace with:
```markdown
author: framing routine (refined with <username>)
```

Find (line 102):
```markdown
    Concerns raised by analytics-question-framer that the user chose to proceed past.
```
Replace with:
```markdown
    Concerns raised during framing that the user chose to proceed past.
```

Find (line 116):
```markdown
- The executor should treat the brief as a spec. If something in the brief is ambiguous or unworkable on inspection of the actual data, kick it back to `analytics-question-framer` rather than improvising.
```
Replace with:
```markdown
- The executor should treat the brief as a spec. If something in the brief is ambiguous or unworkable on inspection of the actual data, return to the framing step (re-run [framing.md](framing.md)) rather than improvising.
```

Find (line 125, inside the "Re-frame" bullet):
```markdown
- **Re-frame (new framer round + brief revision).** The change touches **population, eligibility, target metric, or comparison** — e.g. "filter to ICP only" (not slice), switch the outcome variable, or change the cohort window. These are exactly what the framer owns, so send it back through `analytics-question-framer` for a revised brief.
```
Replace with:
```markdown
- **Re-frame (new framing round + brief revision).** The change touches **population, eligibility, target metric, or comparison** — e.g. "filter to ICP only" (not slice), switch the outcome variable, or change the cohort window. These are exactly what framing owns, so re-run [framing.md](framing.md) for a revised brief.
```

- [ ] **Step 3: Repoint calibration.md triage row**

In `.claude/skills/win-analytics-process/references/calibration.md`, find this table row:
```markdown
| How the framer scopes / verifies (data-existence checks, metric-semantics steps) | `.claude/agents/analytics-question-framer.md` |
```
Replace with:
```markdown
| How framing scopes / verifies (data-existence checks, metric-semantics steps) | `.claude/skills/win-analytics-process/references/framing.md` |
```

- [ ] **Step 4: Repoint methodology.md cross-reference (name only)**

In `.claude/skills/win-analytics-process/references/methodology.md`, find:
```markdown
- [brief-schema.md](brief-schema.md) — the framer→executor brief contract.
```
Replace with:
```markdown
- [brief-schema.md](brief-schema.md) — the framing→execution brief contract.
```

- [ ] **Step 5: Repoint product-data-scientist.md**

In `.claude/agents/product-data-scientist.md`, find:
```markdown
In addition to methodology review, you also interpret results from executed analyses (typically produced by Claude Code from an `analytics-question-framer` brief). Interpretation is a different mode from methodology review — sense-making rather than adversarial — but the same rigor applies.
```
Replace with:
```markdown
In addition to methodology review, you also interpret results from executed analyses (typically produced by Claude Code from an approved framing brief). Interpretation is a different mode from methodology review — sense-making rather than adversarial — but the same rigor applies.
```

- [ ] **Step 6: Remove the stale settings.local.json permission**

In `.claude/settings.local.json`, delete the entire array element line:
```json
      "Bash(env -u VIRTUAL_ENV pre-commit run --files analytics/lib/win_analysis.py analytics/lib/README.md .claude/agents/analytics-question-framer.md analytics/runbook/win.md)",
```
(Remove the whole line including its trailing comma. The lines above and below it remain valid JSON array elements, each already ending in a comma.)

- [ ] **Step 7: Verify no stale references remain and JSON is valid**

Run (from repo root):

```bash
grep -rn "analytics-question-framer" .claude/skills .claude/agents ; echo "skills/agents exit:$?"
grep -rn "analytics/runbook/win.md" .claude/ ; echo "win.md exit:$?"
python3 -c "import json; json.load(open('.claude/settings.local.json')); print('settings.local.json valid')"
check_links .claude/skills/win-analytics-process/references/pipeline.md
check_links .claude/skills/win-analytics-process/references/brief-schema.md
check_links .claude/skills/win-analytics-process/references/calibration.md
check_links .claude/skills/win-analytics-process/references/methodology.md
```

Expected: both greps print nothing with `exit:1` (no surviving references to the retired agent or the deleted `win.md`), `settings.local.json valid`, and no `BROKEN:` lines.

- [ ] **Step 8: Commit**

```bash
git -C /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform add .claude/skills/win-analytics-process/references/pipeline.md .claude/skills/win-analytics-process/references/brief-schema.md .claude/skills/win-analytics-process/references/calibration.md .claude/skills/win-analytics-process/references/methodology.md .claude/agents/product-data-scientist.md .claude/settings.local.json
cd /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform/dbt \
  && env -u VIRTUAL_ENV poetry run git -C /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform commit -m "refactor(analytics): pipeline.md topology + repoint retired framer references

Frame and Execute are now orchestrator-owned ordered steps split by the
approval gate; the entry runbook is the documented start. Repoint every
reference to the retired framer agent to the framing routine, and remove
the stale settings.local.json permission referencing win.md.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

Expected: pre-commit hooks pass; one commit created.

---

## Task 5: Whole-pipeline verification

**Files:** none (verification only).

- [ ] **Step 1: Confirm the two reviewers describe the flow only by pointer**

The spec requires the flow to be described in exactly one place (`pipeline.md`). The two reviewer agents should only point to it, not restate it. Run (from repo root):

```bash
grep -n "pipeline.md" .claude/agents/product-data-scientist.md .claude/agents/product-manager.md
grep -in "stage 1\|stage 2\|framing stage\|where you sit in the workflow" .claude/agents/product-data-scientist.md .claude/agents/product-manager.md ; echo "exit:$?"
```

Expected: each reviewer has a `pipeline.md` pointer; the second grep prints nothing with `exit:1` (neither reviewer re-describes the flow). If a reviewer does re-describe the flow, trim that sentence to a single pointer to `pipeline.md` and re-commit it with Task 4's message style.

- [ ] **Step 2: Full link sweep across changed docs**

Run (from repo root):

```bash
for f in \
  .claude/skills/data-matching/SKILL.md \
  .claude/skills/win-analytics-knowledge/references/joins.md \
  .claude/skills/win-analytics-process/SKILL.md \
  .claude/skills/win-analytics-process/references/framing.md \
  .claude/skills/win-analytics-process/references/pipeline.md \
  .claude/skills/win-analytics-process/references/brief-schema.md \
  .claude/skills/win-analytics-process/references/calibration.md \
  .claude/skills/win-analytics-process/references/methodology.md \
  analytics/runbook/README.md \
  analytics/runbook/run-product-analysis.md ; do
    check_links "$f"
done
echo "link sweep done"
```

Expected: `link sweep done` with no `BROKEN:` lines.

- [ ] **Step 3: Confirm pre-commit passes on all changed files**

Run (from repo root):

```bash
cd /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform/dbt \
  && env -u VIRTUAL_ENV poetry run pre-commit run --all-files
```

Expected: all hooks Pass or Skip (no Fail). This is the same gate CI runs.

- [ ] **Step 4 (optional, requires live Databricks + unlocked 1Password): end-to-end smoke**

The spec's acceptance criterion is that an existing analysis still runs end to end. This plan does not touch `analytics/lib` or any data, so the working set is unchanged; this step confirms nothing regressed. Open one existing notebook in `analytics/ad_hoc/` (e.g. `2026-06-02_win_retention_curve.ipynb`) and re-run its working-set build cell, or run the helper's entrypoint:

```bash
cd /Users/tristan/Documents/0_goodparty/0_repos/gp-data-platform
env -u VIRTUAL_ENV poetry run python -c "from analytics.lib.win_analysis import build_win_working_set; df = build_win_working_set(); print(df.shape)"
```

Expected: a non-empty working set (the Nov-2025 set was ~12,760 rows in PR #441). If the import path differs, follow the notebook's own build cell instead. Skip this step if Databricks credentials are not available in the session; note it as skipped.

- [ ] **Step 5: Final review against the spec**

Re-read `docs/superpowers/specs/2026-06-05-win-pipeline-cleanup-design.md` acceptance criteria and confirm each is met:
- Framing and execution are two ordered orchestrator steps with the approval gate (Task 2 + Task 4).
- One entry point (`run-product-analysis.md`) primes the session and seeds the to-do; `pipeline.md` matches (Task 3 + Task 4).
- A concept resolves to one governed definition via `canonical_metrics.md` (unchanged; confirm no edit broke it).
- `data-match-runbook.md` extracted into the skill and retired; no data fact verbatim in two files except as a pointer (Task 1).
- The flow is described in exactly one place; no agent re-describes it (Task 5 Step 1).
- A session from the entry runbook reaches framing (manual read-through of `run-product-analysis.md` + `framing.md`).

There is no commit in this task unless Step 1 required a reviewer trim.

---

## Self-review notes (for the executor)

- **Spec coverage:** Component 1 → Task 1; Component 2 → Task 3; Component 3 → Task 2; Component 4 → Task 4; acceptance + smoke → Task 5. The "conventions adopted" section is realized by the book-shaped runbook (Task 3 Step 1) and the routing-index README (Task 3 Step 2). Deferred work (benchmark, per-directory CI, data-matching migration, experiment port) is intentionally not in this plan.
- **Naming consistency:** the entry runbook is `run-product-analysis.md` everywhere; the framing doc is `framing.md`; the skill is `data-matching`. These names are used identically across tasks.
- **Ordering:** Task 2 must run before Task 4 (Task 4 repoints references to `framing.md`, which Task 2 creates). Task 1 and Task 3 are independent and may run in any order relative to the others.
