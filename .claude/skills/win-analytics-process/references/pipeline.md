# Pipeline topology

Part of the **win-analytics-process** skill. The single source of truth for the analytics
pipeline's stages and handoff contracts. Agents reference this doc instead of describing the
flow themselves.

**Descriptive, not active.** This pipeline runs as a conversation in which the human shapes the
framing and approves the brief. There is no automated driver that runs the stages end to end —
doing so would skip those human inputs. This doc documents the flow; a human (or the process
skill stepping through it) drives it.

## Stages and handoffs

| # | Stage | Agent | Consumes | Produces | May / may not |
|---|---|---|---|---|---|
| 1 | **Frame** | `analytics-question-framer` | a vague question + the human's intent | a structured analysis **brief** (YAML per [brief-schema.md](brief-schema.md)) | Read-only and advisory. Shapes and scopes; verifies data exists against the live catalog. Does **not** write analysis code. Does not produce the brief until the human approves the framing. |
| 2 | **Execute** | Executor (general-purpose Claude Code) | the approved brief | an executed notebook + the brief saved alongside it | Treats the brief as a spec. Builds the working set once, slices in pandas. If the brief is unworkable on inspection of the data, kicks it back to stage 1 rather than improvising. |
| 3 | **Review (methodology + interpretation)** | `product-data-scientist` | the executed notebook, read against the brief | a methodology review + an interpretation of results | Read-only and advisory. Surfaces leakage / survivorship / calibration concerns and interprets effect sizes. Does not edit code or open PRs. |
| + | **Review (usefulness)** | `product-manager` | the plan or the deliverable | a framing / actionability review | Read-only and advisory. Asks whether this answers the team's real question and whether names/segments/thresholds match how consumers think. Invoked proactively at plan checkpoints and pre-PR — a checkpoint, not a strict sequence position. |

## Artifacts and where they land

- **Brief:** YAML, format in [brief-schema.md](brief-schema.md). For ad-hoc work it lands at `analytics/ad_hoc/<YYYY-MM-DD>_<brief_id>_brief.yaml`; for scout projects, alongside the project notebook. The brief is durable — it travels with the executed notebook so the framing is retrievable later.
- **Executed notebook:** lands next to its brief (ad-hoc) or under `analytics/projects/<project>/notebooks/` (scout). Per-contributor working artifacts are gitignored by default; promote a specific analysis with `git add -f`.
- **Review notes:** delivered in-conversation; durable lessons are routed via the calibration log (see [calibration.md](calibration.md)).

## Closing the loop

Every substantive run ends with a calibration pass ([calibration.md](calibration.md)): findings
are triaged into the file that owns them — the framer agent, the executor instructions /
`analytics/lib`, the data-scientist agent, or a knowledge-skill domain doc — so the pipeline
self-corrects across runs. The log itself is disposable; the doc and agent edits are the durable output.

## Cross-references

- [methodology.md](methodology.md) — how the executor scopes and verifies within a stage.
- [brief-schema.md](brief-schema.md) — the stage-1→stage-2 contract.
- [calibration.md](calibration.md) — the closing step that feeds edits back into the pipeline.
