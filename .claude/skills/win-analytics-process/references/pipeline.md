# Pipeline topology

Part of the **win-analytics-process** skill. The single source of truth for the analytics
pipeline's stages and handoff contracts. Agents reference this doc instead of describing the
flow themselves.

**Descriptive, not active.** This pipeline runs as a conversation in which the human shapes the
framing and approves the brief. The supported entry point is the
`analytics/runbook/run-product-analysis.md` runbook, which seeds the staged to-do list (frame →
approve → execute → results-checkpoint → review → calibrate). There is no
automated driver that runs the stages end to end; doing so would skip those human inputs. This
doc documents the flow; a human (or the process skill stepping through it) drives it.

## Stages and handoffs

| # | Stage | Actor | Consumes | Produces | May / may not |
|---|---|---|---|---|---|
| 1 | **Frame** | Orchestrator (framing routine, [framing.md](framing.md)) | a vague question + the human's intent | a structured analysis **brief** (YAML per [brief-schema.md](brief-schema.md)) | Runs in the orchestrator's own context so it can converse with the human. Shapes and scopes; verifies data exists against the live catalog. Does **not** write analysis code, and does not produce the brief until the human approves the framing. Settles the deliverable format and save location with the human before the brief (framing.md step 5). |
| 2 | **Execute** | Orchestrator (same session, after the approval gate) | the approved brief | the deliverable in the brief's `preferred_format` (notebook, script, or both) + the brief saved alongside it | A distinct ordered step after framing, separated by the human-approval gate. Treats the brief as a spec. Builds the working set once, slices in pandas. If the brief is unworkable on inspection of the data, returns to framing rather than improvising. |
| G | **Results checkpoint** | Orchestrator → human (hard gate) | the executed notebook + robustness checks | a human go / no-go | **Hard stop after execution.** Present results and robustness checks; do **not** dispatch any reviewer until the human approves. The human may redirect scope here, which is cheaper than a wasted review cycle. A clean result or an eager requester does not waive the gate. |
| 3 | **Review (methodology + interpretation)** | `product-data-scientist` | the executed notebook, read against the brief | a methodology review + an interpretation of results | Read-only and advisory. Surfaces leakage / survivorship / calibration concerns and interprets effect sizes. Does not edit code or open PRs. |
| + | **Review (usefulness)** | `product-manager` | the plan or the deliverable | a framing / actionability review | Read-only and advisory. Asks whether this answers the team's real question and whether names/segments/thresholds match how consumers think. Invoked proactively at plan checkpoints and pre-PR — a checkpoint, not a strict sequence position. |

## Reviewer dispatch template

Build each reviewer's invocation prompt from this template — and nothing more:

- **Artifacts by path:** the brief YAML and the executed notebook/script (plus any figures directory). The reviewer reads these on the merits.
- **Product context:** the product, its knowledge skill, the decision cadence, and the intended audience/consumer of the deliverable.
- **Docs to load:** the reviewer doc pointers from the product knowledge skill's `methodology_defaults.md` (which names the two or three docs each reviewer needs, per role).
- **The ask:** review per your role (methodology + interpretation, or usefulness + actionability).

**The dispatch must not lead the witness.** Do not summarize the analysis's conclusions, characterize the result's quality ("clean", "strong", "confirms X"), or include the orchestrator's interpretation. The orchestrator that produced the analysis writes this prompt; anything beyond paths, product context, and doc pointers contaminates a review whose value is its fresh context.

**Model note.** The reviewers inherit the session model (their frontmatter deliberately carries no pin — intent recorded 2026-07: "strong model" was the goal and a literal pin lags model generations). Run substantive reviews in a session on a strong model tier.

**On reviewer conflict:** when the two reviews disagree, present both to the user verbatim — do not arbitrate, average, or synthesize away the disagreement. The two lenses (rigor, usefulness) are allowed to pull in different directions; picking a winner is the human's call.

## Artifacts and where they land

- **Brief:** YAML, format in [brief-schema.md](brief-schema.md). For ad-hoc work it lands at `analytics/ad_hoc/<YYYY-MM-DD>_<brief_id>_brief.yaml`; for scout projects, alongside the project notebook. The brief is durable — it travels with the executed notebook so the framing is retrievable later.
- **Executed notebook:** lands next to its brief (ad-hoc) or under `analytics/projects/<project>/notebooks/` (scout). Per-contributor working artifacts are gitignored by default; promote a specific analysis with `git add -f`.
- **Review notes:** delivered in-conversation; durable lessons are routed via the calibration log (see [calibration.md](calibration.md)).

## Closing the loop

Every substantive run ends with a calibration pass ([calibration.md](calibration.md)), which has
**two tracks**. Data calibration (reusable data facts, routed to the owning knowledge-skill doc)
runs by default: findings that pass the promotion test are proposed as edits to their owning doc
and applied only after human approval — most runs correctly produce none. Observations below that
bar land in the shared `analytics/runbook/CANDIDATES.md` ledger until they recur. Process
calibration (changes to framing, the executor / `analytics/lib`, the reviewer agents, or the
process skill itself) is **off by default**: on a standard run its findings are parked as
`process`-track lines in the candidates ledger rather than routed into any doc, and are acted on
only when the user opts into process-design mode. The log itself is disposable; the durable
output is the doc and agent edits, landed via the calibration branch/PR convention in
[calibration.md](calibration.md).

## Cross-references

- [methodology.md](methodology.md) — how the executor scopes and verifies within a stage.
- [brief-schema.md](brief-schema.md) — the stage-1→stage-2 contract.
- [calibration.md](calibration.md) — the closing step that feeds edits back into the pipeline.
